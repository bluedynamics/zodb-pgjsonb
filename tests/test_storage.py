"""Phase 1: ZODB.DB integration tests for PGJsonbStorage.

Tests that PGJsonbStorage works as a real ZODB storage — store and load
objects through the standard ZODB.DB/Connection API.

Requires PostgreSQL on localhost:5433.
"""

import pytest

import transaction as txn

import ZODB

from persistent.mapping import PersistentMapping

from zodb_pgjsonb.storage import PGJsonbStorage


DSN = "dbname=zodb_test user=zodb password=zodb host=localhost port=5433"


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage with clean database."""
    import psycopg
    # Clean slate
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS blob_state, object_state, transaction_log CASCADE")
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
        import psycopg
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
            cur.execute("SELECT class_mod, class_name, state FROM object_state WHERE zoid = 0")
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
        import psycopg
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
