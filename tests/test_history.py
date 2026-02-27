"""Phase 3: History-preserving mode + undo tests.

Tests that PGJsonbStorage correctly implements history-preserving mode
with dual writes, loadBefore from history table, history() with
transaction metadata, IStorageUndoable (undo, undoLog, undoInfo),
and history-aware pack.

Requires PostgreSQL on localhost:5433.
"""

from persistent.mapping import PersistentMapping
from tests.conftest import DSN
from ZODB.interfaces import IStorageUndoable
from ZODB.POSException import UndoError
from ZODB.utils import p64
from ZODB.utils import u64
from ZODB.utils import z64
from zodb_pgjsonb.storage import PGJsonbStorage

import pytest
import time
import transaction as txn
import ZODB


@pytest.fixture
def hp_storage():
    """Fresh PGJsonbStorage in history-preserving mode."""
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

    s = PGJsonbStorage(DSN, history_preserving=True)
    yield s
    s.close()


@pytest.fixture
def hp_db(hp_storage):
    """ZODB.DB using history-preserving storage."""
    database = ZODB.DB(hp_storage)
    yield database
    database.close()


class TestHistoryPreservingSchema:
    """Test that history tables are created in HP mode."""

    def test_schema_creates_history_tables(self, hp_storage):
        from psycopg.rows import dict_row

        import psycopg

        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' "
                "ORDER BY tablename"
            )
            tables = [r["tablename"] for r in cur.fetchall()]
        pg_conn.close()

        assert "object_history" in tables
        assert "pack_state" in tables
        # blob_history is no longer created for new installations
        # (blob_state keeps all versions via PK (zoid, tid))
        assert "blob_history" not in tables

    def test_history_preserving_flag(self, hp_storage):
        assert hp_storage._history_preserving is True

    def test_instance_inherits_flag(self, hp_storage):
        inst = hp_storage.new_instance()
        assert inst._history_preserving is True
        inst.release()


class TestDualWrite:
    """Test that stores write to both tables in HP mode."""

    def test_store_writes_to_both_tables(self, hp_db):
        from psycopg.rows import dict_row

        import psycopg

        conn = hp_db.open()
        root = conn.root()
        root["title"] = "Test"
        txn.commit()
        conn.close()

        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM object_state")
            state_count = cur.fetchone()["cnt"]
            cur.execute("SELECT COUNT(*) AS cnt FROM object_history")
            history_count = cur.fetchone()["cnt"]
        pg_conn.close()

        # Root was stored once (in the DB open() commit)
        # then updated with title in a second commit
        assert state_count >= 1
        assert history_count >= 1

    def test_multiple_revisions_in_history(self, hp_db):
        from psycopg.rows import dict_row

        import psycopg

        conn = hp_db.open()
        root = conn.root()
        root["counter"] = 1
        txn.commit()

        root["counter"] = 2
        txn.commit()

        root["counter"] = 3
        txn.commit()
        conn.close()

        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            # Root has 1 entry in object_state (current)
            cur.execute("SELECT COUNT(*) AS cnt FROM object_state WHERE zoid = 0")
            assert cur.fetchone()["cnt"] == 1

            # Root has multiple entries in object_history
            cur.execute("SELECT COUNT(*) AS cnt FROM object_history WHERE zoid = 0")
            assert cur.fetchone()["cnt"] >= 3
        pg_conn.close()


class TestLoadBefore:
    """Test loadBefore with history-preserving mode."""

    def test_loadBefore_returns_previous_revision(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["val"] = "first"
        txn.commit()
        tid1 = hp_storage.lastTransaction()

        root["val"] = "second"
        txn.commit()
        tid2 = hp_storage.lastTransaction()
        conn.close()

        # Load state before tid2 → should get tid1's state
        result = hp_storage.loadBefore(z64, tid2)
        assert result is not None
        _data, start_tid, end_tid = result
        assert start_tid == tid1
        assert end_tid == tid2

    def test_loadBefore_returns_none_before_creation(self, hp_db, hp_storage):
        # The root is created at the first commit
        first_tid = hp_storage.lastTransaction()
        # loadBefore with a tid <= first_tid should return None
        result = hp_storage.loadBefore(z64, first_tid)
        assert result is None

    def test_loadBefore_end_tid_none_for_current(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["val"] = "latest"
        txn.commit()
        tid = hp_storage.lastTransaction()
        conn.close()

        # loadBefore with a tid > latest should return latest
        # We need a tid larger than current
        future_tid = p64(u64(tid) + 1)
        result = hp_storage.loadBefore(z64, future_tid)
        assert result is not None
        _data, start_tid, end_tid = result
        assert start_tid == tid
        assert end_tid is None  # no newer revision


class TestLoadSerial:
    """Test loadSerial with history-preserving mode."""

    def test_loadSerial_from_history(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["val"] = "first"
        txn.commit()
        tid1 = hp_storage.lastTransaction()

        root["val"] = "second"
        txn.commit()
        conn.close()

        # Load the old revision by tid1
        import zodb_json_codec

        data = hp_storage.loadSerial(z64, tid1)
        record = zodb_json_codec.decode_zodb_record(data)
        assert record["@s"]["data"]["val"] == "first"


class TestHistory:
    """Test history() returns metadata from transaction_log."""

    def test_history_returns_metadata(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["title"] = "Test"
        txn.get().setUser("admin")
        txn.get().note("Initial setup")
        txn.commit()
        conn.close()

        entries = hp_storage.history(z64, size=10)
        assert len(entries) >= 1
        # Latest entry should have our metadata
        latest = entries[0]
        assert "admin" in latest["user_name"]
        assert latest["description"] == "Initial setup"

    def test_history_multiple_revisions(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["v"] = 1
        txn.commit()

        root["v"] = 2
        txn.commit()

        root["v"] = 3
        txn.commit()
        conn.close()

        entries = hp_storage.history(z64, size=10)
        # Should have multiple entries (root creation + 3 updates)
        assert len(entries) >= 3
        # Entries should be in reverse chronological order
        for i in range(len(entries) - 1):
            assert entries[i]["tid"] > entries[i + 1]["tid"]


class TestUndoInterface:
    """Test IStorageUndoable implementation."""

    def test_provides_istorageundoable(self, hp_storage):
        assert IStorageUndoable.providedBy(hp_storage)

    def test_supports_undo_true(self, hp_storage):
        assert hp_storage.supportsUndo() is True

    def test_supports_undo_false_history_free(self):
        """History-free storage does not support undo."""
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

        s = PGJsonbStorage(DSN, history_preserving=False)
        try:
            assert s.supportsUndo() is False
        finally:
            s.close()

    def test_undo_log_returns_transactions(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["x"] = 1
        txn.get().note("change 1")
        txn.commit()

        root["x"] = 2
        txn.get().note("change 2")
        txn.commit()
        conn.close()

        log = hp_storage.undoLog(0, -20)
        assert len(log) >= 2
        # Latest transaction first
        assert log[0]["description"] == b"change 2"
        assert log[1]["description"] == b"change 1"

    def test_undo_log_with_filter(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["x"] = 1
        txn.get().note("keep")
        txn.commit()

        root["x"] = 2
        txn.get().note("skip")
        txn.commit()
        conn.close()

        log = hp_storage.undoLog(0, -20, filter=lambda d: d["description"] == b"keep")
        assert all(d["description"] == b"keep" for d in log)

    def test_undo_info_works(self, hp_db, hp_storage):
        """UndoLogCompatible mixin provides undoInfo()."""
        conn = hp_db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        conn.close()

        info = hp_storage.undoInfo()
        assert isinstance(info, list)
        assert len(info) >= 1


class TestUndo:
    """Test undo() reverts objects to previous state."""

    def test_undo_reverts_object(self, hp_db, hp_storage):
        conn = hp_db.open()
        root = conn.root()
        root["val"] = "original"
        txn.commit()

        root["val"] = "modified"
        txn.get().note("modify val")
        txn.commit()
        tid_to_undo = hp_storage.lastTransaction()
        conn.close()

        # Undo the modification via DB.undo()
        # (goes through TransactionalUndo → instance.undo())
        hp_db.undo(tid_to_undo)
        txn.commit()

        # Verify the value is back to original
        conn = hp_db.open()
        root = conn.root()
        assert root["val"] == "original"
        conn.close()


class TestHistoryPack:
    """Test pack with history-preserving mode."""

    def test_pack_removes_old_history(self, hp_db, hp_storage):
        from psycopg.rows import dict_row

        import psycopg

        conn = hp_db.open()
        root = conn.root()
        root["v"] = 1
        txn.commit()

        root["v"] = 2
        txn.commit()

        root["v"] = 3
        txn.commit()
        conn.close()

        # Count history rows before pack
        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM object_history WHERE zoid = 0")
            before = cur.fetchone()["cnt"]
        pg_conn.close()

        assert before >= 3

        # Pack up to now
        hp_storage.pack(time.time(), None)

        # Some old history should be removed
        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM object_history WHERE zoid = 0")
            after = cur.fetchone()["cnt"]
        pg_conn.close()

        assert after < before

    def test_pack_removes_unreachable_history(self, hp_db, hp_storage):
        from psycopg.rows import dict_row

        import psycopg

        conn = hp_db.open()
        root = conn.root()
        root["child"] = PersistentMapping()
        root["child"]["name"] = "temp"
        txn.commit()

        # Remove the child — makes it unreachable
        del root["child"]
        txn.commit()
        conn.close()

        # Pack
        hp_storage.pack(time.time(), None)

        # Unreachable child's history should be cleaned up
        pg_conn = psycopg.connect(DSN, row_factory=dict_row)
        with pg_conn.cursor() as cur:
            # The child object should not be in object_state
            cur.execute("SELECT COUNT(*) AS cnt FROM object_state WHERE zoid != 0")
            orphan_state = cur.fetchone()["cnt"]
            # The child should also be gone from object_history
            cur.execute(
                "SELECT COUNT(*) AS cnt FROM object_history "
                "WHERE zoid NOT IN (SELECT zoid FROM object_state)"
            )
            orphan_history = cur.fetchone()["cnt"]
        pg_conn.close()

        assert orphan_state == 0
        assert orphan_history == 0


class TestUndoErrorPaths:
    """Test undo error conditions."""

    def test_undo_nonexistent_transaction(self, hp_db, hp_storage):
        """undo() of non-existent transaction raises UndoError."""
        # Store something first so HP schema is active
        conn = hp_db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        conn.close()

        fake_tid = p64(999999999)
        with pytest.raises(UndoError, match="Transaction not found"):
            hp_storage.undo(fake_tid)

    def test_undo_hf_raises(self):
        """Undo on history-free storage raises UndoError."""
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

        s = PGJsonbStorage(DSN, history_preserving=False)
        try:
            with pytest.raises(UndoError, match="not supported"):
                s.undo(p64(1))
        finally:
            s.close()

    def test_undo_empty_transaction(self, hp_db, hp_storage):
        """undo() of transaction with no object changes raises UndoError."""
        # Create a transaction that has a txn_log entry but no objects
        # (edge case: manually insert a txn_log entry)
        import psycopg

        pg_conn = psycopg.connect(DSN)
        with pg_conn.cursor() as cur:
            cur.execute(
                "INSERT INTO transaction_log (tid, username, description) "
                "VALUES (999999999, '', '')"
            )
        pg_conn.commit()
        pg_conn.close()

        with pytest.raises(UndoError, match="no object changes"):
            hp_storage.undo(p64(999999999))

    def test_undoLog_hf_returns_empty(self):
        """undoLog() on history-free storage returns empty list."""
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

        s = PGJsonbStorage(DSN, history_preserving=False)
        try:
            assert s.undoLog() == []
        finally:
            s.close()

    def test_instance_undo_hf_raises(self):
        """Instance undo on HF storage raises UndoError."""
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

        s = PGJsonbStorage(DSN, history_preserving=False)
        try:
            inst = s.new_instance()
            inst.poll_invalidations()
            with pytest.raises(UndoError, match="not supported"):
                inst.undo(p64(1))
            inst.release()
        finally:
            s.close()


class TestInstanceHistoryDelegates:
    """Test instance history/pack delegates."""

    def test_instance_history(self, hp_db, hp_storage):
        """Instance history() delegates to main."""
        conn = hp_db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        inst = conn._storage
        entries = inst.history(z64, size=10)
        assert len(entries) >= 1
        conn.close()

    def test_instance_pack(self, hp_db, hp_storage):
        """Instance pack() delegates to main."""
        conn = hp_db.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        conn.close()

        inst = hp_storage.new_instance()
        inst.pack(time.time(), None)  # should not raise
        inst.release()


def _clean_db():
    """Drop all tables for a fresh start."""
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


def _table_exists_raw(table_name):
    """Check if a table exists using a raw connection."""
    import psycopg

    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (table_name,))
        result = cur.fetchone()[0]
    conn.close()
    return result


class TestModeConversion:
    """Test switching between HF and HP modes."""

    def test_hp_to_hf_conversion(self):
        """HP storage with data can be converted to HF."""
        _clean_db()
        # Create HP storage and write data with history
        storage = PGJsonbStorage(DSN, history_preserving=True)
        db = ZODB.DB(storage)
        conn = db.open()
        root = conn.root()
        root["key"] = "v1"
        txn.commit()
        root["key"] = "v2"
        txn.commit()
        conn.close()
        db.close()

        # HP tables should exist
        assert _table_exists_raw("object_history")
        assert _table_exists_raw("pack_state")

        # Convert to HF
        storage = PGJsonbStorage(DSN, history_preserving=True)
        counts = storage.convert_to_history_free()
        storage.close()

        # HP tables should be gone
        assert not _table_exists_raw("object_history")
        assert not _table_exists_raw("pack_state")

        # History rows were removed
        assert counts["history_rows"] > 0

        # Data should still be intact — reopen as HF
        storage = PGJsonbStorage(DSN, history_preserving=False)
        db = ZODB.DB(storage)
        conn = db.open()
        root = conn.root()
        assert root["key"] == "v2"
        conn.close()
        db.close()

    def test_hf_to_hp_conversion(self):
        """HF storage can be converted to HP."""
        _clean_db()
        # Create HF storage with data
        storage = PGJsonbStorage(DSN, history_preserving=False)
        db = ZODB.DB(storage)
        conn = db.open()
        root = conn.root()
        root["key"] = "value"
        txn.commit()
        conn.close()
        db.close()

        assert not _table_exists_raw("object_history")

        # Convert to HP
        storage = PGJsonbStorage(DSN, history_preserving=False)
        storage.convert_to_history_preserving()
        storage.close()

        assert _table_exists_raw("object_history")
        assert _table_exists_raw("pack_state")

        # Reopen as HP — data intact, new writes create history
        storage = PGJsonbStorage(DSN, history_preserving=True)
        db = ZODB.DB(storage)
        conn = db.open()
        root = conn.root()
        assert root["key"] == "value"
        root["key"] = "updated"
        txn.commit()
        conn.close()
        db.close()

    def test_convert_to_hf_raises_if_already_hf(self):
        """convert_to_history_free() raises if no HP tables exist."""
        _clean_db()
        storage = PGJsonbStorage(DSN, history_preserving=False)
        with pytest.raises(RuntimeError, match="already history-free"):
            storage.convert_to_history_free()
        storage.close()

    def test_convert_to_hp_raises_if_already_hp(self):
        """convert_to_history_preserving() raises if HP tables exist."""
        _clean_db()
        storage = PGJsonbStorage(DSN, history_preserving=True)
        with pytest.raises(RuntimeError, match="already history-preserving"):
            storage.convert_to_history_preserving()
        storage.close()

    def test_startup_warning_hp_tables_in_hf_mode(self, caplog):
        """Opening HF storage with leftover HP tables logs a warning."""
        import logging

        _clean_db()
        # Create HP storage to establish tables
        storage = PGJsonbStorage(DSN, history_preserving=True)
        storage.close()

        # Reopen as HF — should warn
        with caplog.at_level(logging.WARNING, logger="zodb_pgjsonb.storage"):
            storage = PGJsonbStorage(DSN, history_preserving=False)
        storage.close()

        assert any(
            "convert_to_history_free" in record.message for record in caplog.records
        )

    def test_convert_to_hf_cleans_old_blob_versions(self):
        """convert_to_history_free removes old blob_state versions."""
        from ZODB.blob import Blob

        import psycopg

        _clean_db()
        storage = PGJsonbStorage(DSN, history_preserving=True)
        db = ZODB.DB(storage)

        # Write a blob, then update the same blob object to create
        # two blob_state rows with the same zoid but different tids.
        conn = db.open()
        root = conn.root()
        blob = Blob(b"version1")
        root["blob"] = blob
        txn.commit()

        blob.open("w").write(b"version2")
        txn.commit()
        conn.close()
        db.close()

        # Should have 2 blob_state rows for the same zoid
        raw = psycopg.connect(DSN)
        with raw.cursor() as cur:
            cur.execute(
                "SELECT zoid, count(*) AS cnt FROM blob_state "
                "GROUP BY zoid HAVING count(*) > 1"
            )
            row = cur.fetchone()
            assert row is not None, "expected multiple blob versions"
        raw.close()

        # Convert
        storage = PGJsonbStorage(DSN, history_preserving=True)
        counts = storage.convert_to_history_free()
        storage.close()

        assert counts["old_blob_versions"] >= 1

        # Only latest version remains
        raw = psycopg.connect(DSN)
        with raw.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM blob_state GROUP BY zoid HAVING count(*) > 1"
            )
            assert cur.fetchone() is None  # no zoid with multiple versions
        raw.close()

    def test_convert_to_hf_cleans_orphan_transactions(self):
        """convert_to_history_free removes unreferenced transaction_log rows."""
        import psycopg

        _clean_db()
        storage = PGJsonbStorage(DSN, history_preserving=True)
        db = ZODB.DB(storage)
        conn = db.open()
        root = conn.root()
        root["a"] = 1
        txn.commit()
        root["a"] = 2
        txn.commit()
        root["a"] = 3
        txn.commit()
        conn.close()
        db.close()

        # In HP mode we have multiple transaction_log entries;
        # in HF object_state only points to the latest tid per object.
        raw = psycopg.connect(DSN)
        with raw.cursor() as cur:
            cur.execute("SELECT count(*) FROM transaction_log")
            txn_count_before = cur.fetchone()[0]
        raw.close()
        assert txn_count_before >= 3

        storage = PGJsonbStorage(DSN, history_preserving=True)
        counts = storage.convert_to_history_free()
        storage.close()

        # Some orphaned transactions should have been removed
        assert counts["orphan_transactions"] >= 1
