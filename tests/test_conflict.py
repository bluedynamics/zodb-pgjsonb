"""Phase 5: Conflict resolution tests.

Tests that PGJsonbStorage correctly handles conflict resolution:
- BTrees auto-resolve non-overlapping concurrent modifications
- PersistentMapping raises ConflictError for concurrent modifications
- Transform hooks are propagated from main storage to instances

Requires PostgreSQL on localhost:5433.
"""

import pytest

import transaction as txn

import ZODB
from ZODB.POSException import ConflictError

from BTrees.OOBTree import OOBTree

from persistent.mapping import PersistentMapping

from zodb_pgjsonb.storage import PGJsonbStorage


DSN = "dbname=zodb_test user=zodb password=zodb host=localhost port=5433"


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""
    import psycopg

    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state, blob_history, object_history, "
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


class TestConflictResolutionBTrees:
    """BTrees have _p_resolveConflict — non-overlapping changes auto-resolve."""

    def test_btree_concurrent_inserts_resolve(self, db):
        """Two connections inserting different keys into the same OOBTree.

        BTrees' _p_resolveConflict should merge non-overlapping inserts.
        """
        # Setup: create an OOBTree at root["tree"]
        conn = db.open()
        root = conn.root()
        root["tree"] = OOBTree()
        txn.commit()
        conn.close()

        # Open two connections with separate TMs to avoid advisory lock deadlock
        tm1 = txn.TransactionManager()
        tm2 = txn.TransactionManager()
        conn1 = db.open(tm1)
        conn2 = db.open(tm2)

        tree1 = conn1.root()["tree"]
        tree2 = conn2.root()["tree"]

        # Both insert different keys
        tree1["key_a"] = "value_a"
        tree2["key_b"] = "value_b"

        # Commit conn1 first — succeeds
        tm1.commit()

        # Commit conn2 — serial mismatch triggers conflict resolution
        # BTrees should auto-resolve non-overlapping inserts
        tm2.commit()

        conn1.close()
        conn2.close()

        # Verify both keys exist
        conn = db.open()
        tree = conn.root()["tree"]
        assert tree["key_a"] == "value_a"
        assert tree["key_b"] == "value_b"
        conn.close()

    def test_btree_resolved_data_persists_correctly(self, db):
        """Verify resolved BTree data survives roundtrip through JSONB."""
        # Setup: OOBTree with initial data
        conn = db.open()
        root = conn.root()
        root["tree"] = OOBTree()
        root["tree"]["existing"] = "data"
        txn.commit()
        conn.close()

        # Two connections add different keys
        tm1 = txn.TransactionManager()
        tm2 = txn.TransactionManager()
        conn1 = db.open(tm1)
        conn2 = db.open(tm2)

        conn1.root()["tree"]["from_conn1"] = "hello"
        conn2.root()["tree"]["from_conn2"] = "world"

        tm1.commit()
        tm2.commit()

        conn1.close()
        conn2.close()

        # Verify all three keys via fresh connection
        conn = db.open()
        tree = conn.root()["tree"]
        assert tree["existing"] == "data"
        assert tree["from_conn1"] == "hello"
        assert tree["from_conn2"] == "world"
        assert len(tree) == 3
        conn.close()

    def test_btree_overlapping_raises_conflict(self, db):
        """Two connections modifying the same BTree key should raise ConflictError."""
        # Setup
        conn = db.open()
        root = conn.root()
        root["tree"] = OOBTree()
        root["tree"]["shared_key"] = "original"
        txn.commit()
        conn.close()

        # Two connections modify the same key
        tm1 = txn.TransactionManager()
        tm2 = txn.TransactionManager()
        conn1 = db.open(tm1)
        conn2 = db.open(tm2)

        conn1.root()["tree"]["shared_key"] = "from_conn1"
        conn2.root()["tree"]["shared_key"] = "from_conn2"

        tm1.commit()

        with pytest.raises(ConflictError):
            tm2.commit()

        conn1.close()
        conn2.close()


class TestConflictResolutionPersistentMapping:
    """PersistentMapping has NO _p_resolveConflict — always raises ConflictError."""

    def test_persistent_mapping_conflict_raises(self, db):
        """Concurrent modifications to PersistentMapping raise ConflictError."""
        # Setup
        conn = db.open()
        root = conn.root()
        root["data"] = PersistentMapping()
        root["data"]["initial"] = True
        txn.commit()
        conn.close()

        # Two connections modify the same PersistentMapping
        tm1 = txn.TransactionManager()
        tm2 = txn.TransactionManager()
        conn1 = db.open(tm1)
        conn2 = db.open(tm2)

        conn1.root()["data"]["key1"] = "from_conn1"
        conn2.root()["data"]["key2"] = "from_conn2"

        tm1.commit()

        with pytest.raises(ConflictError):
            tm2.commit()

        conn1.close()
        conn2.close()


class TestTransformHooks:
    """Verify transform hooks work correctly on both storage and instances."""

    def test_main_storage_hooks_are_identity(self, storage):
        """Main storage transform hooks should be identity functions."""
        test_data = b"test pickle data"
        assert storage._crs_transform_record_data(test_data) == test_data
        assert storage._crs_untransform_record_data(test_data) == test_data

    def test_instance_hooks_are_identity(self, storage):
        """Instance transform hooks should also be identity functions."""
        inst = storage.new_instance()
        test_data = b"test pickle data"
        assert inst._crs_transform_record_data(test_data) == test_data
        assert inst._crs_untransform_record_data(test_data) == test_data
        inst.release()

    def test_instance_hooks_after_registerDB(self, storage):
        """After manual registerDB, new instances get the stored hooks."""
        # Simulate a DB wrapper that could set custom transforms
        class MockDB:
            def transform_record_data(self, data):
                return data
            def untransform_record_data(self, data):
                return data

        storage.registerDB(MockDB())

        inst = storage.new_instance()
        # After registerDB sets instance attrs, they propagate to instances
        assert (
            inst._crs_transform_record_data
            is storage._crs_transform_record_data
        )
        inst.release()
