"""PostgreSQL schema definitions for zodb-pgjsonb."""

SCHEMA_VERSION = 1

# History-free mode: only current object state
HISTORY_FREE_SCHEMA = """\
-- Transaction metadata
CREATE TABLE IF NOT EXISTS transaction_log (
    tid         BIGINT PRIMARY KEY,
    username    TEXT DEFAULT '',
    description TEXT DEFAULT '',
    extension   BYTEA DEFAULT ''
);

-- Current object state (JSONB)
CREATE TABLE IF NOT EXISTS object_state (
    zoid        BIGINT PRIMARY KEY,
    tid         BIGINT NOT NULL REFERENCES transaction_log(tid),
    class_mod   TEXT NOT NULL,
    class_name  TEXT NOT NULL,
    state       JSONB,
    state_size  INTEGER NOT NULL,
    refs        BIGINT[] NOT NULL DEFAULT '{}'
);

-- Blob storage (tiered: inline PG bytea or S3 reference)
CREATE TABLE IF NOT EXISTS blob_state (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    blob_size   BIGINT NOT NULL,
    data        BYTEA,
    s3_key      TEXT,
    PRIMARY KEY (zoid, tid)
);

-- Indexes for queryability
CREATE INDEX IF NOT EXISTS idx_object_class
    ON object_state (class_mod, class_name);
CREATE INDEX IF NOT EXISTS idx_object_refs
    ON object_state USING gin (refs);

-- Invalidation trigger
CREATE OR REPLACE FUNCTION notify_commit() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('zodb_invalidations', NEW.tid::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_notify_commit ON transaction_log;
CREATE TRIGGER trg_notify_commit
    AFTER INSERT ON transaction_log
    FOR EACH ROW EXECUTE FUNCTION notify_commit();
"""

# History-preserving mode: adds object_history for previous revisions.
# Note: blob_history is no longer created — blob_state (PK: zoid, tid)
# already keeps all blob versions.  Old databases with blob_history are
# handled gracefully by the packer and compact_history().
HISTORY_PRESERVING_ADDITIONS = """\
CREATE TABLE IF NOT EXISTS object_history (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    class_mod   TEXT NOT NULL,
    class_name  TEXT NOT NULL,
    state       JSONB,
    state_size  INTEGER NOT NULL,
    refs        BIGINT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (zoid, tid)
);

CREATE INDEX IF NOT EXISTS idx_history_tid
    ON object_history (tid);
CREATE INDEX IF NOT EXISTS idx_history_zoid_tid
    ON object_history (zoid, tid DESC);

CREATE TABLE IF NOT EXISTS pack_state (
    zoid        BIGINT PRIMARY KEY,
    tid         BIGINT NOT NULL
);
"""


def _table_exists(conn, name):
    """Check if a table exists (lightweight, no ACCESS EXCLUSIVE lock)."""
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s) IS NOT NULL AS exists", (name,))
        row = cur.fetchone()
        return row["exists"] if isinstance(row, dict) else row[0]


def install_schema(conn, *, history_preserving=False):
    """Install the database schema.

    Skips DDL when core tables already exist to avoid ACCESS EXCLUSIVE
    locks that would block concurrent instances holding REPEATABLE READ
    snapshots (see #15).

    Args:
        conn: psycopg connection (must not be in autocommit mode)
        history_preserving: if True, also create history tables
    """
    core_exists = _table_exists(conn, "transaction_log")
    if not core_exists:
        conn.execute(HISTORY_FREE_SCHEMA)
    if history_preserving and not _table_exists(conn, "object_history"):
        conn.execute(HISTORY_PRESERVING_ADDITIONS)
    conn.commit()


def drop_history_tables(conn):
    """Drop history-preserving tables and clean up stale data.

    Removes ``object_history``, ``pack_state``, and the deprecated
    ``blob_history`` table.  Also removes old blob versions from
    ``blob_state`` (keeps only the latest tid per zoid) and orphaned
    ``transaction_log`` entries no longer referenced by ``object_state``.

    Args:
        conn: psycopg connection (must not be in autocommit mode)

    Returns:
        Dict with counts: ``history_rows``, ``pack_rows``,
        ``blob_history_rows``, ``old_blob_versions``, ``orphan_transactions``.
    """
    counts = {
        "history_rows": 0,
        "pack_rows": 0,
        "blob_history_rows": 0,
        "old_blob_versions": 0,
        "orphan_transactions": 0,
    }
    with conn.cursor() as cur:
        # Drop object_history
        if _table_exists(conn, "object_history"):
            cur.execute("SELECT count(*) FROM object_history")
            row = cur.fetchone()
            counts["history_rows"] = row["count"] if isinstance(row, dict) else row[0]
            cur.execute("DROP TABLE object_history CASCADE")

        # Drop pack_state
        if _table_exists(conn, "pack_state"):
            cur.execute("SELECT count(*) FROM pack_state")
            row = cur.fetchone()
            counts["pack_rows"] = row["count"] if isinstance(row, dict) else row[0]
            cur.execute("DROP TABLE pack_state CASCADE")

        # Drop deprecated blob_history
        if _table_exists(conn, "blob_history"):
            cur.execute("SELECT count(*) FROM blob_history")
            row = cur.fetchone()
            counts["blob_history_rows"] = (
                row["count"] if isinstance(row, dict) else row[0]
            )
            cur.execute("DROP TABLE blob_history CASCADE")

        # Clean old blob versions (keep only latest tid per zoid)
        cur.execute(
            "DELETE FROM blob_state b "
            "WHERE EXISTS ("
            "  SELECT 1 FROM blob_state b2 "
            "  WHERE b2.zoid = b.zoid AND b2.tid > b.tid"
            ")"
        )
        counts["old_blob_versions"] = cur.rowcount

        # Clean orphaned transaction_log entries
        cur.execute(
            "DELETE FROM transaction_log t "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM object_state os WHERE os.tid = t.tid"
            ")"
        )
        counts["orphan_transactions"] = cur.rowcount

    conn.commit()
    return counts
