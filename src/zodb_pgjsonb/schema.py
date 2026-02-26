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
        cur.execute("SELECT to_regclass(%s) IS NOT NULL", (name,))
        return cur.fetchone()[0]


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
