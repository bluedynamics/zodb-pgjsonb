"""Phase 2: MVCC tests for PGJsonbStorage.

Tests that PGJsonbStorage correctly implements IMVCCStorage —
new_instance(), poll_invalidations(), per-connection isolation.

Requires PostgreSQL on localhost:5433.
"""

from persistent.mapping import PersistentMapping
from tests.conftest import DSN
from ZODB.interfaces import IMVCCStorage
from zodb_pgjsonb.storage import PGJsonbStorage
from zodb_pgjsonb.storage import PGJsonbStorageInstance

import pytest
import transaction as txn
import ZODB


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""
    import psycopg

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


class TestIMVCCStorageInterface:
    """Test that PGJsonbStorage provides IMVCCStorage."""

    def test_provides_imvccstorage(self, storage):
        assert IMVCCStorage.providedBy(storage)

    def test_new_instance_returns_instance(self, storage):
        inst = storage.new_instance()
        assert isinstance(inst, PGJsonbStorageInstance)
        assert inst is not storage
        inst.release()

    def test_instance_has_own_connection(self, storage):
        inst = storage.new_instance()
        assert inst._conn is not storage._conn
        inst.release()

    def test_pg_connection_property(self, storage):
        inst = storage.new_instance()
        assert inst.pg_connection is inst._conn
        inst.release()
        assert inst.pg_connection is None

    def test_multiple_instances(self, storage):
        inst1 = storage.new_instance()
        inst2 = storage.new_instance()
        assert inst1 is not inst2
        assert inst1._conn is not inst2._conn
        inst1.release()
        inst2.release()

    def test_release_returns_to_pool(self, storage):
        inst = storage.new_instance()
        pg_conn = inst._conn
        assert not pg_conn.closed
        inst.release()
        # Connection returned to pool, not closed
        assert not pg_conn.closed
        # Instance no longer holds a reference
        assert inst._conn is None

    def test_release_idempotent(self, storage):
        inst = storage.new_instance()
        inst.release()
        inst.release()  # should not raise

    def test_connection_reused_after_release(self, storage):
        """Released connections are reused by new instances."""
        inst1 = storage.new_instance()
        conn1 = inst1._conn
        inst1.release()
        # Pool may reuse the same connection
        inst2 = storage.new_instance()
        assert not conn1.closed
        inst2.release()


class TestPollInvalidations:
    """Test poll_invalidations() behavior."""

    def test_first_poll_returns_empty(self, storage):
        inst = storage.new_instance()
        result = inst.poll_invalidations()
        assert result == []
        inst.release()

    def test_no_changes_returns_empty(self, storage):
        inst = storage.new_instance()
        inst.poll_invalidations()  # initial poll
        result = inst.poll_invalidations()  # second poll, no changes
        assert result == []
        inst.release()

    def test_sees_committed_changes(self, db):
        """An instance sees OIDs changed by other connections."""
        # Write data via connection 1
        conn1 = db.open()
        root = conn1.root()
        root["x"] = 1
        txn.commit()
        conn1.close()

        # Open connection 2 — its poll_invalidations should see root
        conn2 = db.open()
        root2 = conn2.root()
        assert root2["x"] == 1
        conn2.close()

    def test_returns_changed_oids(self, storage):
        """Directly test that poll_invalidations returns changed OIDs."""
        # Create two instances
        writer = storage.new_instance()
        reader = storage.new_instance()

        # Reader does initial poll
        reader.poll_invalidations()

        # Writer commits a change (create root)
        t = txn.Transaction()
        writer.tpc_begin(t)

        # Create a minimal object
        import zodb_json_codec

        record = {
            "@cls": ["persistent.mapping", "PersistentMapping"],
            "@s": {"data": {}},
        }
        data = zodb_json_codec.encode_zodb_record(record)
        from ZODB.utils import z64

        oid = writer.new_oid()
        writer.store(oid, z64, data, "", t)
        writer.tpc_vote(t)
        writer.tpc_finish(t)

        # Reader polls — should see the changed OID
        changed = reader.poll_invalidations()
        assert len(changed) > 0
        assert oid in changed

        writer.release()
        reader.release()


class TestMVCCWithZODB:
    """Test MVCC behavior through ZODB.DB."""

    def test_db_uses_mvcc_directly(self, storage):
        """ZODB.DB should use our storage directly (no MVCCAdapter)."""
        database = ZODB.DB(storage)
        # When IMVCCStorage is provided, DB uses it directly
        assert database._mvcc_storage is storage
        database.close()

    def test_connection_gets_instance(self, db):
        """Each connection should get its own storage instance."""
        conn = db.open()
        # Connection's storage should be a PGJsonbStorageInstance
        storage = conn._storage
        assert isinstance(storage, PGJsonbStorageInstance)
        conn.close()

    def test_two_connections_different_instances(self, db):
        """Two connections should have different storage instances."""
        conn1 = db.open()
        conn2 = db.open()
        assert conn1._storage is not conn2._storage
        conn1.close()
        conn2.close()

    def test_store_and_load_via_mvcc(self, db):
        """Basic store/load through MVCC path."""
        conn = db.open()
        root = conn.root()
        root["title"] = "MVCC Test"
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        assert root["title"] == "MVCC Test"
        conn.close()

    def test_nested_persistent_objects_via_mvcc(self, db):
        """Nested PersistentMapping objects through MVCC path."""
        conn = db.open()
        root = conn.root()
        root["folder"] = PersistentMapping()
        root["folder"]["name"] = "test"
        txn.commit()
        conn.close()

        conn = db.open()
        root = conn.root()
        assert root["folder"]["name"] == "test"
        conn.close()

    def test_multiple_transactions_via_mvcc(self, db):
        """Multiple sequential transactions through MVCC path."""
        for i in range(3):
            conn = db.open()
            root = conn.root()
            root[f"key_{i}"] = f"value_{i}"
            txn.commit()
            conn.close()

        conn = db.open()
        root = conn.root()
        for i in range(3):
            assert root[f"key_{i}"] == f"value_{i}"
        conn.close()

    def test_abort_via_mvcc(self, db):
        """Aborting a transaction discards changes (MVCC path)."""
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

    def test_concurrent_reads(self, db):
        """Two connections reading concurrently see consistent data."""
        conn = db.open()
        root = conn.root()
        root["counter"] = 42
        txn.commit()
        conn.close()

        conn1 = db.open()
        conn2 = db.open()
        assert conn1.root()["counter"] == 42
        assert conn2.root()["counter"] == 42
        conn1.close()
        conn2.close()

    def test_update_visible_after_commit(self, db):
        """Updates are visible to new connections after commit."""
        conn1 = db.open()
        root = conn1.root()
        root["version"] = 1
        txn.commit()
        conn1.close()

        conn1 = db.open()
        root = conn1.root()
        root["version"] = 2
        txn.commit()
        conn1.close()

        conn2 = db.open()
        assert conn2.root()["version"] == 2
        conn2.close()
