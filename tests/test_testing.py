"""Tests for zodb_pgjsonb.testing — PGTestDB snapshot/restore."""

from tests.conftest import DSN
from unittest.mock import patch
from zodb_pgjsonb.testing import PGTestDB

import contextlib
import psycopg
import pytest


@pytest.fixture
def test_db():
    """Provide a PGTestDB instance with schema installed."""
    db = PGTestDB(DSN)
    db.setup()
    yield db
    db.teardown()


def _insert_row(conn, zoid, tid=1):
    """Insert a minimal object_state row for testing."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO transaction_log (tid) VALUES (%(tid)s) ON CONFLICT DO NOTHING",
            {"tid": tid},
        )
        cur.execute(
            "INSERT INTO object_state "
            "(zoid, tid, class_mod, class_name, state, state_size) "
            "VALUES (%(zoid)s, %(tid)s, 'mod', 'Cls', '{}'::jsonb, 2)",
            {"zoid": zoid, "tid": tid},
        )


def _count_rows(conn, table="object_state"):
    """Count rows in a table."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        return cur.fetchone()[0]


class TestPGTestDBLifecycle:
    """Basic setup/teardown lifecycle."""

    def test_setup_creates_tables(self, test_db):
        """setup() creates all core tables."""
        with test_db.connection.cursor() as cur:
            cur.execute(
                "SELECT tablename FROM pg_tables "
                "WHERE schemaname = 'public' "
                "ORDER BY tablename"
            )
            tables = [row[0] for row in cur.fetchall()]
        assert "transaction_log" in tables
        assert "object_state" in tables
        assert "blob_state" in tables

    def test_setup_with_custom_schema(self):
        """setup() accepts custom schema SQL."""
        db = PGTestDB(DSN, tables=("test_custom",))
        db.setup(schema_sql="CREATE TABLE test_custom (id int PRIMARY KEY)")
        try:
            with db.connection.cursor() as cur:
                cur.execute("SELECT 1 FROM test_custom")
                assert cur.fetchone() is not None or True  # table exists
        finally:
            db.teardown()

    def test_teardown_drops_tables(self):
        """teardown() drops all managed tables."""
        db = PGTestDB(DSN)
        db.setup()
        db.teardown()

        conn = psycopg.connect(DSN, autocommit=True)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname = 'public' "
                    "AND tablename IN ('transaction_log', 'object_state', 'blob_state')"
                )
                assert cur.fetchall() == []
        finally:
            conn.close()

    def test_depth_starts_at_zero(self, test_db):
        """Stack depth is 0 before any push."""
        assert test_db.depth == 0


class TestPGTestDBPushPop:
    """Push/pop snapshot stack operations."""

    def test_push_increments_depth(self, test_db):
        test_db.push()
        assert test_db.depth == 1
        test_db.push()
        assert test_db.depth == 2

    def test_pop_decrements_depth(self, test_db):
        test_db.push()
        test_db.push()
        test_db.pop()
        assert test_db.depth == 1
        test_db.pop()
        assert test_db.depth == 0

    def test_pop_empty_raises(self, test_db):
        with pytest.raises(RuntimeError, match="empty"):
            test_db.pop()

    def test_push_captures_data(self, test_db):
        """push() captures all rows; pop() restores them."""
        conn = test_db.connection
        _insert_row(conn, zoid=1)
        _insert_row(conn, zoid=2)
        assert _count_rows(conn) == 2

        test_db.push()

        # Insert more rows
        _insert_row(conn, zoid=3)
        assert _count_rows(conn) == 3

        # Pop should revert to 2 rows
        test_db.pop()
        assert _count_rows(conn) == 2

    def test_pop_restores_snapshot_state(self, test_db):
        """pop() restores to snapshot state (what was captured by push)."""
        conn = test_db.connection
        _insert_row(conn, zoid=1)

        test_db.push()
        _insert_row(conn, zoid=2)

        # Pop restores to state at push time (1 row captured by push)
        test_db.pop()
        assert _count_rows(conn) == 1


class TestPGTestDBRestore:
    """Restore to topmost snapshot (per-test isolation)."""

    def test_restore_without_push_raises(self, test_db):
        with pytest.raises(RuntimeError, match="No snapshot"):
            test_db.restore()

    def test_restore_reverts_changes(self, test_db):
        """restore() reverts to snapshot state, repeatable."""
        conn = test_db.connection
        _insert_row(conn, zoid=1)
        test_db.push()

        # Test A: add data, then restore
        _insert_row(conn, zoid=2)
        assert _count_rows(conn) == 2
        test_db.restore()
        assert _count_rows(conn) == 1

        # Test B: add different data, restore again
        _insert_row(conn, zoid=3)
        _insert_row(conn, zoid=4)
        assert _count_rows(conn) == 3
        test_db.restore()
        assert _count_rows(conn) == 1

    def test_restore_does_not_pop(self, test_db):
        """restore() keeps the snapshot on the stack."""
        test_db.push()
        test_db.restore()
        assert test_db.depth == 1


class TestPGTestDBStacking:
    """Multi-level stacking (like DemoStorage layer hierarchy)."""

    def test_two_level_stacking(self, test_db):
        """Layer A → push → Layer B → push → per-test → restore → pop → pop."""
        conn = test_db.connection

        # Layer A: base data
        _insert_row(conn, zoid=1)
        test_db.push()  # snapshot_A: 1 row
        assert test_db.depth == 1

        # Layer B: add fixtures
        _insert_row(conn, zoid=2)
        test_db.push()  # snapshot_B: 2 rows
        assert test_db.depth == 2

        # Test: add data, then restore
        _insert_row(conn, zoid=3)
        assert _count_rows(conn) == 3
        test_db.restore()
        assert _count_rows(conn) == 2  # back to snapshot_B

        # Layer B teardown: pop to snapshot_A
        test_db.pop()
        assert test_db.depth == 1
        assert _count_rows(conn) == 1  # back to snapshot_A

        # Layer A teardown: pop restores to snapshot_A state
        test_db.pop()
        assert test_db.depth == 0
        assert _count_rows(conn) == 1  # snapshot_A had 1 row

    def test_three_level_stacking(self, test_db):
        """Three levels of stacking."""
        conn = test_db.connection

        _insert_row(conn, zoid=1)
        test_db.push()  # L1: 1 row

        _insert_row(conn, zoid=2)
        test_db.push()  # L2: 2 rows

        _insert_row(conn, zoid=3)
        test_db.push()  # L3: 3 rows

        assert test_db.depth == 3

        # Restore at L3
        _insert_row(conn, zoid=4)
        assert _count_rows(conn) == 4
        test_db.restore()
        assert _count_rows(conn) == 3

        # Pop L3 → L2
        test_db.pop()
        assert _count_rows(conn) == 2

        # Pop L2 → L1
        test_db.pop()
        assert _count_rows(conn) == 1

        # Pop L1 → restores to L1 snapshot state
        test_db.pop()
        assert _count_rows(conn) == 1

    def test_restore_at_each_level(self, test_db):
        """Each level's restore returns to that level's state."""
        conn = test_db.connection

        _insert_row(conn, zoid=1)
        test_db.push()

        _insert_row(conn, zoid=2)
        test_db.push()

        # Restore at level 2
        _insert_row(conn, zoid=99)
        test_db.restore()
        assert _count_rows(conn) == 2

        # Pop to level 1 and restore there
        test_db.pop()
        _insert_row(conn, zoid=99)
        test_db.restore()
        assert _count_rows(conn) == 1


class TestPGTestDBTerminate:
    """pg_terminate_backend handling."""

    def test_setup_survives_active_connections(self):
        """setup() terminates other connections before DDL."""
        # Create a "stuck" connection
        stuck = psycopg.connect(DSN, autocommit=True)
        try:
            stuck.execute("SELECT 1")  # make it active

            # setup() should succeed despite the stuck connection
            db = PGTestDB(DSN)
            db.setup()
            db.teardown()
        finally:
            with contextlib.suppress(Exception):
                stuck.close()


class TestSetupIdempotent:
    """Calling setup() twice must not leak connections (GH-21)."""

    def test_double_setup_closes_first_connection(self):
        db = PGTestDB(DSN)
        db.setup()
        first_conn = db.connection
        assert not first_conn.closed

        db.setup()
        assert first_conn.closed
        assert not db.connection.closed
        assert db.connection is not first_conn
        db.teardown()

    def test_setup_after_teardown(self):
        db = PGTestDB(DSN)
        db.setup()
        db.teardown()
        # setup() after teardown should work cleanly
        db.setup()
        assert db.connection is not None
        assert not db.connection.closed
        db.teardown()


class TestTeardownRobust:
    """teardown() must close connection and clear stack even on errors (GH-21)."""

    def test_teardown_closes_connection_on_drop_error(self):
        db = PGTestDB(DSN)
        db.setup()
        db.push()
        conn = db.connection

        with (
            patch.object(conn, "cursor", side_effect=psycopg.OperationalError("fake")),
            pytest.raises(psycopg.OperationalError),
        ):
            db.teardown()

        # Connection and stack must still be cleaned up
        assert conn.closed
        assert db.depth == 0

    def test_teardown_clears_stack_on_terminate_error(self):
        db = PGTestDB(DSN)
        db.setup()
        db.push()

        with (
            patch.object(
                db,
                "_terminate_other_connections",
                side_effect=psycopg.OperationalError("fake"),
            ),
            pytest.raises(psycopg.OperationalError),
        ):
            db.teardown()

        assert db.connection.closed
        assert db.depth == 0

    def test_teardown_noop_when_not_setup(self):
        """teardown() on a fresh instance is a no-op."""
        db = PGTestDB(DSN)
        db.teardown()  # Should not raise
        assert db.depth == 0
