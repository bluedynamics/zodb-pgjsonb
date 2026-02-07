"""Phase 4: Blob storage tests for PGJsonbStorage.

Tests that PGJsonbStorage correctly implements IBlobStorage —
storeBlob(), loadBlob(), openCommittedBlobFile(), temporaryDirectory().

Requires PostgreSQL on localhost:5433.
"""

import os
import tempfile

import pytest

import transaction as txn

import ZODB
from ZODB.blob import Blob
from ZODB.interfaces import IBlobStorage
from ZODB.POSException import POSKeyError
from ZODB.utils import z64

from persistent.mapping import PersistentMapping

from zodb_pgjsonb.storage import PGJsonbStorage
from zodb_pgjsonb.storage import PGJsonbStorageInstance


DSN = "dbname=zodb_test user=zodb password=zodb host=localhost port=5433"


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""
    import psycopg
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS "
            "blob_state, object_state, transaction_log CASCADE"
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


class TestBlobStorageInterface:
    """Test that PGJsonbStorage provides IBlobStorage."""

    def test_provides_iblobstorage(self, storage):
        assert IBlobStorage.providedBy(storage)

    def test_temporary_directory_exists(self, storage):
        temp_dir = storage.temporaryDirectory()
        assert os.path.isdir(temp_dir)

    def test_instance_temporary_directory(self, storage):
        inst = storage.new_instance()
        temp_dir = inst.temporaryDirectory()
        assert os.path.isdir(temp_dir)
        # Each instance gets its own temp dir
        assert temp_dir != storage.temporaryDirectory()
        inst.release()

    def test_instance_temp_dir_cleaned_on_release(self, storage):
        inst = storage.new_instance()
        temp_dir = inst.temporaryDirectory()
        assert os.path.isdir(temp_dir)
        inst.release()
        assert not os.path.exists(temp_dir)


class TestBlobStoreAndLoad:
    """Test blob store/load via storage API."""

    def test_store_and_load_blob(self, storage):
        """Store a blob via low-level API and load it back."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        # Create a temp blob file
        blob_data = b"Hello, blob world!"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        # Create a minimal object with blob
        import zodb_json_codec
        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        tid = inst.tpc_finish(t)

        # Load the blob back
        loaded_path = inst.loadBlob(oid, tid)
        assert os.path.isfile(loaded_path)
        with open(loaded_path, 'rb') as f:
            assert f.read() == blob_data

        inst.release()

    def test_store_blob_takes_ownership(self, storage):
        """storeBlob should remove the original blob file during vote."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"ownership test"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)
        assert os.path.exists(blob_path)

        import zodb_json_codec
        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        # After vote, the original file should be gone
        assert not os.path.exists(blob_path)
        inst.tpc_finish(t)

        inst.release()

    def test_load_nonexistent_blob_raises(self, storage):
        """loadBlob should raise POSKeyError for missing blobs."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        from ZODB.utils import p64
        with pytest.raises(POSKeyError):
            inst.loadBlob(p64(999), p64(999))

        inst.release()

    def test_open_committed_blob_file(self, storage):
        """openCommittedBlobFile returns a readable file object."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"committed blob data"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec
        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        inst.tpc_vote(t)
        tid = inst.tpc_finish(t)

        # Open without blob object
        f = inst.openCommittedBlobFile(oid, tid)
        assert f.read() == blob_data
        f.close()

        inst.release()

    def test_blob_abort_cleans_up(self, storage):
        """tpc_abort should clean up queued blob temp files."""
        inst = storage.new_instance()
        inst.poll_invalidations()

        blob_data = b"abort test"
        fd, blob_path = tempfile.mkstemp()
        os.write(fd, blob_data)
        os.close(fd)

        import zodb_json_codec
        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)

        t = txn.Transaction()
        inst.tpc_begin(t)
        oid = inst.new_oid()
        inst.storeBlob(oid, z64, data, blob_path, "", t)
        # Abort before vote — blob file should be cleaned up
        inst.tpc_abort(t)
        assert not os.path.exists(blob_path)

        inst.release()


class TestBlobsWithZODB:
    """Test blob objects through ZODB.DB."""

    def test_store_and_load_blob_via_zodb(self, db):
        """Store a Blob via ZODB and load it back."""
        conn = db.open()
        root = conn.root()
        root["myblob"] = Blob(b"Hello from ZODB blob!")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        blob = root["myblob"]
        with blob.open("r") as f:
            assert f.read() == b"Hello from ZODB blob!"
        conn.close()

    def test_blob_data_persists_across_connections(self, db):
        """Blob data persists and is readable from new connections."""
        blob_content = b"persistent blob data " * 100
        conn = db.open()
        root = conn.root()
        root["doc"] = PersistentMapping()
        root["doc"]["attachment"] = Blob(blob_content)
        txn.commit()
        conn.close()

        # New connection reads it back
        conn = db.open()
        root = conn.root()
        with root["doc"]["attachment"].open("r") as f:
            assert f.read() == blob_content
        conn.close()

    def test_large_blob(self, db):
        """Test with a blob larger than 1MB."""
        large_data = b"X" * (1024 * 1024 + 1)  # 1MB + 1 byte
        conn = db.open()
        root = conn.root()
        root["large"] = Blob(large_data)
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["large"].open("r") as f:
            loaded = f.read()
        assert loaded == large_data
        conn.close()

    def test_multiple_blobs_same_transaction(self, db):
        """Multiple blobs in the same transaction."""
        conn = db.open()
        root = conn.root()
        root["blob1"] = Blob(b"first blob")
        root["blob2"] = Blob(b"second blob")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["blob1"].open("r") as f:
            assert f.read() == b"first blob"
        with root["blob2"].open("r") as f:
            assert f.read() == b"second blob"
        conn.close()

    def test_blob_update(self, db):
        """Update blob data in a subsequent transaction."""
        conn = db.open()
        root = conn.root()
        root["doc"] = Blob(b"version 1")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["doc"].open("w") as f:
            f.write(b"version 2")
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["doc"].open("r") as f:
            assert f.read() == b"version 2"
        conn.close()

    def test_blob_survives_transaction_abort(self, db):
        """Aborting a transaction with blob changes discards them."""
        conn = db.open()
        root = conn.root()
        root["keeper"] = Blob(b"keep me")
        txn.commit()

        root["discard"] = Blob(b"throw me away")
        txn.abort()
        conn.close()

        conn = db.open()
        root = conn.root()
        with root["keeper"].open("r") as f:
            assert f.read() == b"keep me"
        assert "discard" not in root
        conn.close()
