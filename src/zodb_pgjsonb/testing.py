"""Test support for PGJsonbStorage: database setup with stackable snapshot/restore.

Provides ``PGTestDB`` — a helper class that manages a PostgreSQL test database
with push/pop/restore semantics mirroring ZODB's DemoStorage stacking pattern.

Usage (single layer)::

    db = PGTestDB(dsn)
    db.setup()                 # Install schema (tables + indexes)
    # ... populate data ...
    db.push()                  # Snapshot current state
    # ... test mutates data ...
    db.restore()               # Reset to snapshot (repeatable per test)
    db.teardown()              # Drop tables, close connection

Usage (stacked layers — like DemoStorage)::

    db.setup()
    # Layer A: create Plone site
    db.push()                  # snapshot_A (Plone site)
    # Layer B: add test fixtures
    db.push()                  # snapshot_B (Plone site + fixtures)
    # Per test:
    db.restore()               # Reset to snapshot_B
    # ... test runs ...
    # Layer B teardown:
    db.pop()                   # Revert to snapshot_A
    # Layer A teardown:
    db.pop()                   # Revert to empty

Future TODO
-----------
- Extra tables: pass additional table names via ``tables`` param when needed
  (e.g. ``text_extraction_queue`` for pgcatalog async extraction).
- Tempfile spill: for very large test fixtures, spill COPY binary data to
  temporary files instead of holding in memory.
- Extract ``_clean_db()`` pattern from existing zodb-pgjsonb tests to use
  ``PGTestDB.setup()`` instead of duplicated cleanup code.
"""

import logging
import psycopg


__all__ = ["TABLES", "PGTestDB"]

log = logging.getLogger(__name__)

# Core tables managed by zodb-pgjsonb.
# Order matters: FK-safe insertion order (parent → child).
TABLES = ("transaction_log", "object_state", "blob_state")


class PGTestDB:
    """Manages a PostgreSQL test database with stackable snapshot/restore.

    Mirrors DemoStorage's push/pop semantics:

    - ``push()``    — save current state, allow new writes (like stacking)
    - ``pop()``     — discard changes, revert to previous snapshot
    - ``restore()`` — reset to topmost snapshot without popping (per-test)
    """

    def __init__(self, dsn, tables=TABLES):
        self.dsn = dsn
        self._tables = tables
        self._stack: list[dict[str, bytes]] = []
        self._conn: psycopg.Connection | None = None

    # -- Lifecycle -------------------------------------------------------------

    def setup(self, schema_sql=None):
        """Create tables.  Uses ``HISTORY_FREE_SCHEMA`` by default.

        Terminates other connections first to avoid REPEATABLE READ
        transactions blocking DDL operations.

        Calling ``setup()`` twice closes the existing connection first
        to prevent resource leaks.
        """
        if self._conn is not None and not self._conn.closed:
            self._conn.close()
        self._conn = psycopg.connect(self.dsn, autocommit=True)
        self._terminate_other_connections()
        with self._conn.cursor() as cur:
            cur.execute(
                "DROP TABLE IF EXISTS " + ", ".join(reversed(self._tables)) + " CASCADE"
            )
        if schema_sql is None:
            from zodb_pgjsonb.schema import HISTORY_FREE_SCHEMA

            schema_sql = HISTORY_FREE_SCHEMA
        self._conn.execute(schema_sql)
        log.debug("PGTestDB: schema installed (%d tables)", len(self._tables))

    def teardown(self):
        """Drop tables and close admin connection.

        Guarantees connection close and stack cleanup even if
        ``DROP TABLE`` or ``pg_terminate_backend`` raises.
        """
        try:
            if self._conn and not self._conn.closed:
                try:
                    self._terminate_other_connections()
                    with self._conn.cursor() as cur:
                        cur.execute(
                            "DROP TABLE IF EXISTS "
                            + ", ".join(reversed(self._tables))
                            + " CASCADE"
                        )
                finally:
                    self._conn.close()
        finally:
            self._stack.clear()
        log.debug("PGTestDB: teardown complete")

    # -- Snapshot stack --------------------------------------------------------

    def push(self):
        """Save current table state onto the snapshot stack.

        Like DemoStorage's ``push()`` — preserves current state so it can
        be restored later via ``pop()`` or ``restore()``.
        """
        snap = {}
        for table in self._tables:
            with (
                self._conn.cursor() as cur,
                cur.copy(f"COPY {table} TO STDOUT (FORMAT BINARY)") as copy,
            ):
                # Drain all chunks — read() returns one chunk at a time
                snap[table] = b"".join(copy)
        self._stack.append(snap)
        log.debug("PGTestDB: push (depth=%d)", len(self._stack))

    def pop(self):
        """Discard changes since the last push and revert to the parent state.

        Like DemoStorage's ``pop()`` — discards the current overlay and
        returns to the state of the previous level.  If this was the only
        level, restores to the snapshot that was captured (the state at
        push time).
        """
        if not self._stack:
            raise RuntimeError("Nothing to pop — snapshot stack is empty")
        snap = self._stack.pop()
        if self._stack:
            # Revert to parent level's snapshot
            self._restore_from(self._stack[-1])
        else:
            # Last level — restore to the state captured by this push
            self._restore_from(snap)
        log.debug("PGTestDB: pop (depth=%d)", len(self._stack))

    def restore(self):
        """Restore to the topmost snapshot without popping.

        Used for per-test isolation: reset to the current layer's
        base state between tests.
        """
        if not self._stack:
            raise RuntimeError("No snapshot — call push() first")
        self._restore_from(self._stack[-1])
        log.debug("PGTestDB: restore (depth=%d)", len(self._stack))

    # -- Internal --------------------------------------------------------------

    def _restore_from(self, snap):
        """DELETE all rows, then COPY FROM snapshot bytes.

        Uses DELETE instead of TRUNCATE to avoid ACCESS EXCLUSIVE locks.
        TRUNCATE conflicts with ACCESS SHARE held by REPEATABLE READ
        snapshots (e.g. from ZODB Connection pools).  DELETE only needs
        ROW EXCLUSIVE, which is compatible with ACCESS SHARE.
        """
        with self._conn.cursor() as cur:
            # Delete in reverse FK order (child → parent)
            for table in reversed(self._tables):
                cur.execute(f"DELETE FROM {table}")
        for table in self._tables:
            data = snap[table]
            with (
                self._conn.cursor() as cur,
                cur.copy(f"COPY {table} FROM STDIN (FORMAT BINARY)") as copy,
            ):
                copy.write(data)

    def _terminate_other_connections(self):
        """Terminate other connections to this database.

        REPEATABLE READ transactions hold locks that block DDL (DROP TABLE,
        TRUNCATE).  This is needed before setup/teardown.
        """
        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity "
                "WHERE datname = current_database() "
                "  AND pid != pg_backend_pid() "
                "  AND state != 'idle'"
            )

    # -- Properties ------------------------------------------------------------

    @property
    def connection(self):
        """Admin connection for custom DDL (e.g. ALTER TABLE for catalog columns)."""
        return self._conn

    @property
    def depth(self):
        """Current snapshot stack depth."""
        return len(self._stack)
