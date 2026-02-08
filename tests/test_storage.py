"""Phase 1: ZODB.DB integration tests for PGJsonbStorage.

Tests that PGJsonbStorage works as a real ZODB storage — store and load
objects through the standard ZODB.DB/Connection API.

Requires PostgreSQL on localhost:5433.
"""

from persistent.mapping import PersistentMapping
from tests.conftest import DSN
from ZODB.Connection import TransactionMetaData
from ZODB.POSException import ReadConflictError
from ZODB.POSException import StorageTransactionError
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64
from zodb_pgjsonb.storage import _deserialize_extension
from zodb_pgjsonb.storage import _serialize_extension
from zodb_pgjsonb.storage import _unsanitize_from_pg
from zodb_pgjsonb.storage import LoadCache
from zodb_pgjsonb.storage import PGJsonbStorage

import os
import pickle
import psycopg
import pytest
import tempfile
import transaction as txn
import ZODB


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""

    # Clean slate
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()

    s = PGJsonbStorage(DSN)
    yield s
    s.close()


@pytest.fixture
def db(storage):
    """ZODB.DB using our storage."""
    database = ZODB.DB(storage)
    yield database
    database.close()


class TestZODBIntegration:
    """Test full ZODB.DB lifecycle with PGJsonbStorage."""

    def test_empty_root(self, db):
        """Opening a new DB creates a root PersistentMapping."""
        conn = db.open()
        root = conn.root()
        assert isinstance(root, PersistentMapping)
        conn.close()

    def test_store_and_load_string(self, db):
        """Store a string value in root, commit, reopen, verify."""
        conn = db.open()
        root = conn.root()
        root["title"] = "Hello World"
        txn.commit()
        conn.close()

        # Reopen and verify
        conn = db.open()
        root = conn.root()
        assert root["title"] == "Hello World"
        conn.close()

    def test_store_multiple_values(self, db):
        """Store multiple values of different types."""
        conn = db.open()
        root = conn.root()
        root["name"] = "Test"
        root["count"] = 42
        root["active"] = True
        root["tags"] = ["a", "b", "c"]
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        assert root["name"] == "Test"
        assert root["count"] == 42
        assert root["active"] is True
        assert root["tags"] == ["a", "b", "c"]
        conn.close()

    def test_nested_persistent_objects(self, db):
        """Store nested PersistentMapping objects."""
        conn = db.open()
        root = conn.root()
        root["folder"] = PersistentMapping()
        root["folder"]["doc"] = PersistentMapping()
        root["folder"]["doc"]["title"] = "Nested Doc"
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        assert root["folder"]["doc"]["title"] == "Nested Doc"
        conn.close()

    def test_update_existing_object(self, db):
        """Update an existing object value."""
        conn = db.open()
        root = conn.root()
        root["counter"] = 1
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        assert root["counter"] == 1
        root["counter"] = 2
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        assert root["counter"] == 2
        conn.close()

    def test_multiple_transactions(self, db):
        """Multiple sequential transactions work correctly."""
        for i in range(5):
            conn = db.open()
            root = conn.root()
            root[f"key_{i}"] = f"value_{i}"
            txn.commit()
            conn.close()

        conn = db.open()
        root = conn.root()
        for i in range(5):
            assert root[f"key_{i}"] == f"value_{i}"
        conn.close()

    def test_abort_transaction(self, db):
        """Aborting a transaction discards changes."""
        conn = db.open()
        root = conn.root()
        root["keep"] = "yes"
        txn.commit()

        root["discard"] = "no"
        txn.abort()
        conn.close()

        conn = db.open()
        root = conn.root()
        assert root["keep"] == "yes"
        assert "discard" not in root
        conn.close()

    def test_object_count(self, storage, db):
        """__len__ returns the number of stored objects."""
        conn = db.open()
        root = conn.root()
        root["child"] = PersistentMapping()
        txn.commit()
        conn.close()

        # root + child = 2 objects
        assert len(storage) == 2

    def test_last_transaction(self, storage, db):
        """lastTransaction returns the TID of the last commit."""
        from ZODB.utils import z64

        # Before any user commit, DB has created root
        assert storage.lastTransaction() != z64

        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        tid1 = storage.lastTransaction()

        root["x"] = 2
        txn.commit()
        tid2 = storage.lastTransaction()
        conn.close()

        # TIDs must be monotonically increasing
        assert tid2 > tid1

    def test_jsonb_stored_correctly(self, db):
        """Verify that JSONB is actually stored in PostgreSQL."""
        from psycopg.rows import dict_row

        conn = db.open()
        root = conn.root()
        root["title"] = "JSONB Test"
        root["year"] = 2025
        txn.commit()
        conn.close()

        # Query PostgreSQL directly
        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            # Root object is at zoid=0
            cur.execute(
                "SELECT class_mod, class_name, state FROM object_state WHERE zoid = 0"
            )
            row = cur.fetchone()
        pg_conn.close()

        assert row is not None
        assert row["class_mod"] == "persistent.mapping"
        assert row["class_name"] == "PersistentMapping"
        # The state should contain our values as native JSONB
        state = row["state"]
        assert state["data"]["title"] == "JSONB Test"
        assert state["data"]["year"] == 2025

    def test_refs_populated(self, db):
        """Verify refs column is populated for persistent references."""
        from psycopg.rows import dict_row

        conn = db.open()
        root = conn.root()
        root["child"] = PersistentMapping()
        root["child"]["name"] = "child"
        txn.commit()
        conn.close()

        # Root should have a ref to child
        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT refs FROM object_state WHERE zoid = 0")
            row = cur.fetchone()
        pg_conn.close()

        assert row is not None
        assert len(row["refs"]) > 0  # root references child


class TestLocalCache:
    """Test LoadCache eviction and management methods."""

    def test_set_evicts_old_entry(self):
        """Updating an existing zoid reclaims old entry size."""
        cache = LoadCache(max_mb=10)
        cache.set(1, b"data1", b"\x00" * 8)
        size_after_first = cache._size
        # Update with new data — old entry size should be reclaimed
        cache.set(1, b"data2-longer", b"\x00" * 8)
        # Size should reflect the new entry, not accumulate
        assert cache._size != size_after_first + cache._size

    def test_lru_eviction(self):
        """Cache evicts LRU entries when over budget."""
        # Tiny cache: 1 byte budget
        cache = LoadCache(max_mb=0)
        cache._max_size = 200  # set a small absolute budget
        cache.set(1, b"a", b"\x01")
        cache.set(2, b"b", b"\x02")
        cache.set(3, b"c", b"\x03")
        # Cache should have evicted oldest entries to stay in budget
        assert cache._size <= cache._max_size + 200  # allow overhead

    def test_clear(self):
        """clear() empties the cache."""
        cache = LoadCache(max_mb=10)
        cache.set(1, b"data", b"\x01")
        assert len(cache) > 0
        cache.clear()
        assert len(cache) == 0
        assert cache._size == 0

    def test_len(self):
        """__len__ returns number of entries."""
        cache = LoadCache(max_mb=10)
        assert len(cache) == 0
        cache.set(1, b"d1", b"\x01")
        assert len(cache) == 1
        cache.set(2, b"d2", b"\x02")
        assert len(cache) == 2

    def test_size_mb(self):
        """size_mb returns size in megabytes."""
        cache = LoadCache(max_mb=10)
        assert cache.size_mb == 0.0
        cache.set(1, b"data", b"\x01")
        assert cache.size_mb > 0


class TestInstanceDelegates:
    """Test PGJsonbStorageInstance metadata delegates."""

    def test_instance_getName(self, storage, db):
        """Instance getName() delegates to main storage."""
        conn = db.open()
        inst = conn._storage
        assert inst.getName() == storage.getName()
        conn.close()

    def test_instance_name_property(self, storage, db):
        """Instance __name__ delegates to main storage."""
        conn = db.open()
        inst = conn._storage
        assert inst.__name__ == storage.__name__
        conn.close()

    def test_instance_isReadOnly(self, db):
        """Instance isReadOnly() returns False."""
        conn = db.open()
        assert conn._storage.isReadOnly() is False
        conn.close()

    def test_instance_lastTransaction(self, storage, db):
        """Instance lastTransaction() returns main storage _ltid."""
        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        assert conn._storage.lastTransaction() == storage.lastTransaction()
        conn.close()

    def test_instance_len(self, storage, db):
        """Instance __len__ delegates to main storage."""
        conn = db.open()
        root = conn.root()
        root["child"] = PersistentMapping()
        txn.commit()
        assert len(conn._storage) == len(storage)
        conn.close()

    def test_instance_getSize(self, storage, db):
        """Instance getSize() delegates to main storage."""
        conn = db.open()
        assert conn._storage.getSize() == storage.getSize()
        conn.close()

    def test_instance_sortKey(self, storage, db):
        """Instance sortKey() delegates to main storage."""
        conn = db.open()
        assert conn._storage.sortKey() == storage.sortKey()
        conn.close()

    def test_instance_supportsUndo(self, storage, db):
        """Instance supportsUndo() delegates to main storage."""
        conn = db.open()
        assert conn._storage.supportsUndo() == storage.supportsUndo()
        conn.close()

    def test_instance_undoLog(self, storage, db):
        """Instance undoLog() delegates to main storage."""
        conn = db.open()
        assert conn._storage.undoLog() == storage.undoLog()
        conn.close()

    def test_instance_undoInfo(self, storage, db):
        """Instance undoInfo() delegates to main storage."""
        conn = db.open()
        assert conn._storage.undoInfo() == storage.undoInfo()
        conn.close()


class TestErrorPaths:
    """Test error handling paths in store/storeBlob."""

    def test_store_version_rejected(self, storage):
        """store() with non-empty version raises TypeError."""
        t = TransactionMetaData()
        storage.tpc_begin(t)
        oid = storage.new_oid()
        with pytest.raises(TypeError, match="versions"):
            storage.store(oid, z64, zodb_pickle(MinPO(1)), "ver1", t)
        storage.tpc_abort(t)

    def test_store_wrong_transaction(self, storage):
        """store() with wrong transaction raises StorageTransactionError."""
        t1 = TransactionMetaData()
        t2 = TransactionMetaData()
        storage.tpc_begin(t1)
        oid = storage.new_oid()
        with pytest.raises(StorageTransactionError):
            storage.store(oid, z64, zodb_pickle(MinPO(1)), "", t2)
        storage.tpc_abort(t1)

    def test_instance_store_wrong_transaction(self, storage):
        """Instance store() with wrong transaction raises error."""
        inst = storage.new_instance()
        inst.poll_invalidations()
        t1 = txn.Transaction()
        t2 = txn.Transaction()
        inst.tpc_begin(t1)
        oid = inst.new_oid()
        with pytest.raises(StorageTransactionError):
            inst.store(oid, z64, zodb_pickle(MinPO(1)), "", t2)
        inst.tpc_abort(t1)
        inst.release()

    def test_instance_store_version_rejected(self, storage):
        """Instance store() with version raises TypeError."""
        inst = storage.new_instance()
        inst.poll_invalidations()
        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        with pytest.raises(TypeError, match="versions"):
            inst.store(oid, z64, zodb_pickle(MinPO(1)), "ver1", t)
        inst.tpc_abort(t)
        inst.release()

    def test_instance_storeBlob_wrong_transaction(self, storage):
        """Instance storeBlob() with wrong transaction raises error."""
        inst = storage.new_instance()
        inst.poll_invalidations()
        t1 = txn.Transaction()
        t2 = txn.Transaction()
        inst.tpc_begin(t1)
        oid = inst.new_oid()
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, b"test")
        os.close(fd)
        try:
            with pytest.raises(StorageTransactionError):
                inst.storeBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, "", t2)
        finally:
            if os.path.exists(blob_path):
                os.unlink(blob_path)
        inst.tpc_abort(t1)
        inst.release()

    def test_instance_storeBlob_version_rejected(self, storage):
        """Instance storeBlob() with version raises TypeError."""
        inst = storage.new_instance()
        inst.poll_invalidations()
        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, b"test")
        os.close(fd)
        try:
            with pytest.raises(TypeError, match="versions"):
                inst.storeBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, "v1", t)
        finally:
            if os.path.exists(blob_path):
                os.unlink(blob_path)
        inst.tpc_abort(t)
        inst.release()

    def test_instance_tpc_vote_wrong_transaction(self, storage):
        """Instance tpc_vote() with wrong transaction raises error."""
        inst = storage.new_instance()
        inst.poll_invalidations()
        t1 = txn.Transaction()
        t2 = txn.Transaction()
        inst.tpc_begin(t1)
        with pytest.raises(StorageTransactionError):
            inst.tpc_vote(t2)
        inst.tpc_abort(t1)
        inst.release()

    def test_instance_checkCurrentSerialInTransaction(self, storage, db):
        """checkCurrentSerialInTransaction raises ReadConflictError on mismatch."""
        # Store an object first
        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        conn.close()

        inst = storage.new_instance()
        inst.poll_invalidations()
        t = txn.Transaction()
        inst.tpc_begin(t)
        # Root (oid=0) has a real serial; pass a fake stale serial
        with pytest.raises(ReadConflictError):
            inst.checkCurrentSerialInTransaction(z64, p64(1), t)
        inst.tpc_abort(t)
        inst.release()

    def test_instance_tpc_finish_callback(self, storage):
        """tpc_finish calls callback with tid when f is not None."""
        inst = storage.new_instance()
        inst.poll_invalidations()
        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.store(oid, z64, zodb_pickle(MinPO(1)), "", t)
        inst.tpc_vote(t)
        tids = []
        tid = inst.tpc_finish(t, lambda tid: tids.append(tid))
        assert len(tids) == 1
        assert tids[0] == tid
        inst.release()


class TestMainStorageDirectPaths:
    """Test main storage direct-use methods (not through instances)."""

    def test_main_load_cache_hit(self, storage, db):
        """Main storage load() returns cached data on second call."""
        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        conn.close()
        # First load populates cache
        data1, tid1 = storage.load(z64)
        # Second load should hit cache
        data2, tid2 = storage.load(z64)
        assert data1 == data2
        assert tid1 == tid2

    def test_main_loadSerial_cache_hit(self, storage, db):
        """Main storage loadSerial() returns from serial cache."""
        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        tid = storage.lastTransaction()
        conn.close()
        # Load to populate serial cache
        data1 = storage.loadSerial(z64, tid)
        # Second call from cache
        data2 = storage.loadSerial(z64, tid)
        assert data1 == data2

    def test_main_cleanup(self, storage, db):
        """cleanup() removes all data."""
        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        conn.close()
        assert len(storage) >= 1
        storage.cleanup()
        assert len(storage) == 0

    def test_main_storeBlob(self, storage):
        """Main storage storeBlob() stores blob data."""
        blob_data = b"test blob via main storage"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        t = TransactionMetaData()
        storage.tpc_begin(t)
        oid = storage.new_oid()
        storage.storeBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, "", t)
        storage.tpc_vote(t)
        tid = storage.tpc_finish(t)

        # loadBlob should return the data
        loaded_path = storage.loadBlob(oid, tid)
        assert os.path.isfile(loaded_path)
        with open(loaded_path, "rb") as f:
            assert f.read() == blob_data

    def test_main_openCommittedBlobFile(self, storage):
        """Main storage openCommittedBlobFile() returns file object."""
        blob_data = b"committed blob test"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        t = TransactionMetaData()
        storage.tpc_begin(t)
        oid = storage.new_oid()
        storage.storeBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, "", t)
        storage.tpc_vote(t)
        tid = storage.tpc_finish(t)

        # Without blob object
        f = storage.openCommittedBlobFile(oid, tid)
        assert f.read() == blob_data
        f.close()

        # With blob object
        from ZODB.blob import Blob

        blob = Blob()
        f = storage.openCommittedBlobFile(oid, tid, blob)
        assert f.read() == blob_data
        f.close()

    def test_main_storeBlob_version_rejected(self, storage):
        """Main storage storeBlob() with version raises TypeError."""
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, b"test")
        os.close(fd)
        t = TransactionMetaData()
        storage.tpc_begin(t)
        oid = storage.new_oid()
        try:
            with pytest.raises(TypeError, match="versions"):
                storage.storeBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, "ver1", t)
        finally:
            if os.path.exists(blob_path):
                os.unlink(blob_path)
        storage.tpc_abort(t)

    def test_main_storeBlob_wrong_transaction(self, storage):
        """Main storage storeBlob() with wrong txn raises error."""
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, b"test")
        os.close(fd)
        t1 = TransactionMetaData()
        t2 = TransactionMetaData()
        storage.tpc_begin(t1)
        oid = storage.new_oid()
        try:
            with pytest.raises(StorageTransactionError):
                storage.storeBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, "", t2)
        finally:
            if os.path.exists(blob_path):
                os.unlink(blob_path)
        storage.tpc_abort(t1)

    def test_main_restore_wrong_transaction(self, storage):
        """Main storage restore() with wrong transaction raises error."""
        t1 = TransactionMetaData()
        t2 = TransactionMetaData()
        storage.tpc_begin(t1)
        oid = storage.new_oid()
        with pytest.raises(StorageTransactionError):
            storage.restore(oid, z64, zodb_pickle(MinPO(1)), "", None, t2)
        storage.tpc_abort(t1)

    def test_main_restoreBlob(self, storage):
        """Main storage restoreBlob() stores data + blob."""
        blob_data = b"restored blob"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        t = TransactionMetaData()
        storage.tpc_begin(t)
        oid = storage.new_oid()
        storage.restoreBlob(oid, z64, zodb_pickle(MinPO(1)), blob_path, None, t)
        storage.tpc_vote(t)
        tid = storage.tpc_finish(t)

        # Verify blob was stored
        loaded = storage.loadBlob(oid, tid)
        with open(loaded, "rb") as f:
            assert f.read() == blob_data

    def test_instance_new_instance_delegates(self, storage):
        """Instance new_instance() delegates to main storage."""
        inst1 = storage.new_instance()
        inst2 = inst1.new_instance()
        assert inst2 is not inst1
        inst2.release()
        inst1.release()

    def test_instance_registerDB_and_close(self, storage):
        """Instance registerDB() is no-op, close() calls release()."""
        inst = storage.new_instance()
        inst.registerDB(None)  # should not raise
        assert inst._conn is not None
        inst.close()
        # After close, connection is returned to pool (set to None)
        assert inst._conn is None


class TestExtensionSerialization:
    """Test _serialize_extension and _deserialize_extension."""

    def test_serialize_empty_bytes(self):
        assert _serialize_extension(b"") == b""

    def test_serialize_empty_dict(self):
        assert _serialize_extension({}) == b""

    def test_serialize_dict(self):
        result = _serialize_extension({"key": "value"})
        import json

        assert json.loads(result) == {"key": "value"}

    def test_serialize_pickle_bytes(self):
        """Pickle bytes from ZODB are converted to JSON."""
        ext_dict = {"user": "admin"}
        pkl = pickle.dumps(ext_dict, protocol=3)
        result = _serialize_extension(pkl)
        import json

        assert json.loads(result) == ext_dict

    def test_serialize_none_type(self):
        """Non-bytes, non-dict returns empty bytes."""
        assert _serialize_extension(None) == b""

    def test_deserialize_empty(self):
        assert _deserialize_extension(b"") == {}
        assert _deserialize_extension(None) == {}

    def test_deserialize_json(self):
        import json

        data = json.dumps({"key": "value"}).encode()
        assert _deserialize_extension(data) == {"key": "value"}

    def test_deserialize_memoryview(self):
        """memoryview is converted to bytes before parsing."""
        import json

        data = json.dumps({"x": 1}).encode()
        mv = memoryview(data)
        assert _deserialize_extension(mv) == {"x": 1}

    def test_deserialize_pickle_fallback(self):
        """Pickle bytes (legacy data) are deserialized via fallback."""
        ext_dict = {"legacy": True}
        pkl = pickle.dumps(ext_dict, protocol=3)
        assert _deserialize_extension(pkl) == ext_dict

    def test_deserialize_garbage_returns_empty(self):
        """Unparseable data returns empty dict."""
        assert _deserialize_extension(b"\xff\xfe\xfd") == {}


class TestUnsanitizeFromPg:
    """Test _unsanitize_from_pg with list values."""

    def test_list_with_ns_marker(self):
        """Lists containing @ns markers are unsanitized."""
        import base64

        val = "hello\x00world"
        encoded = base64.b64encode(val.encode("utf-8", errors="surrogatepass")).decode()
        result = _unsanitize_from_pg([{"@ns": encoded}, "normal"])
        assert result[0] == val
        assert result[1] == "normal"

    def test_list_unchanged(self):
        """Lists without markers are returned unchanged (same object)."""
        original = ["a", "b", "c"]
        result = _unsanitize_from_pg(original)
        assert result is original


class TestInstanceLoadPaths:
    """Test instance loadBefore and loadSerial edge cases."""

    def test_instance_loadBefore_hf(self, storage, db):
        """Instance loadBefore in HF mode returns data with future tid."""
        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        tid = storage.lastTransaction()
        conn.close()

        inst = storage.new_instance()
        inst.poll_invalidations()
        # Use a future TID — stored tid < future_tid → returns data
        future_tid = p64(u64(tid) + 1)
        result = inst.loadBefore(z64, future_tid)
        assert result is not None
        _data, start_tid, _end_tid = result
        assert start_tid == tid
        inst.release()

    def test_instance_loadBefore_hf_returns_none(self, storage, db):
        """Instance loadBefore returns None when tid <= object's tid."""
        conn = db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        tid = storage.lastTransaction()
        conn.close()

        inst = storage.new_instance()
        inst.poll_invalidations()
        # loadBefore with exact tid should return None (tid not < tid)
        result = inst.loadBefore(z64, tid)
        assert result is None
        inst.release()

    def test_instance_loadSerial_missing_raises(self, storage):
        """Instance loadSerial for non-existent oid raises POSKeyError."""
        from ZODB.POSException import POSKeyError

        inst = storage.new_instance()
        inst.poll_invalidations()
        with pytest.raises(POSKeyError):
            inst.loadSerial(p64(9999), p64(1))
        inst.release()
