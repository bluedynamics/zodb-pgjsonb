<!-- diataxis: reference -->

# Database schema

This page documents the PostgreSQL tables, indexes, triggers, and JSONB
conventions used by zodb-pgjsonb.
The schema version is 2.

## Tables

### transaction_log

Stores transaction metadata.
Present in both history-free and history-preserving modes.

| Column | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `tid` | `BIGINT` | -- | `PRIMARY KEY` | Transaction ID (ZODB TID as 64-bit integer). |
| `username` | `TEXT` | `''` | -- | User who committed the transaction. |
| `description` | `TEXT` | `''` | -- | Transaction description text. |
| `extension` | `BYTEA` | `''` | -- | Serialized transaction extension metadata. |

### object_state

Stores the current state of each object as JSONB.
Present in both history-free and history-preserving modes.

| Column | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `zoid` | `BIGINT` | -- | `PRIMARY KEY` | Object ID (ZODB OID as 64-bit integer). |
| `tid` | `BIGINT` | -- | `NOT NULL`, `REFERENCES transaction_log(tid)` | Transaction ID of the current revision. |
| `class_mod` | `TEXT` | -- | `NOT NULL` | Python module path of the object class. |
| `class_name` | `TEXT` | -- | `NOT NULL` | Python class name of the object. |
| `state` | `JSONB` | -- | -- | Object state as JSONB (see [JSONB state structure](#jsonb-state-structure)). |
| `state_size` | `INTEGER` | -- | `NOT NULL` | Size of the original pickle in bytes. |
| `refs` | `BIGINT[]` | `'{}'` | `NOT NULL` | Array of OIDs this object references (used by the packer). |

State processors may add extra columns to this table via `ALTER TABLE`.

### blob_state

Stores blob data with tiered PG/S3 support.
Present in both history-free and history-preserving modes.
The primary key `(zoid, tid)` retains all blob versions across transactions.

| Column | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `zoid` | `BIGINT` | -- | `PRIMARY KEY (zoid, tid)` | Object ID. |
| `tid` | `BIGINT` | -- | `PRIMARY KEY (zoid, tid)` | Transaction ID. |
| `blob_size` | `BIGINT` | -- | `NOT NULL` | Blob size in bytes. |
| `data` | `BYTEA` | -- | -- | Blob data (populated for PG-stored blobs). |
| `s3_key` | `TEXT` | -- | -- | S3 object key (populated for S3-stored blobs). |

A blob row has either `data` populated (PG-stored) or `s3_key` populated
(S3-stored), but not both.

### object_history (history-preserving mode)

Stores previous revisions of objects.
Created only when `history_preserving=True`.

| Column | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `zoid` | `BIGINT` | -- | `PRIMARY KEY (zoid, tid)` | Object ID. |
| `tid` | `BIGINT` | -- | `PRIMARY KEY (zoid, tid)` | Transaction ID of this revision. |
| `class_mod` | `TEXT` | -- | `NOT NULL` | Python module path of the object class. |
| `class_name` | `TEXT` | -- | `NOT NULL` | Python class name. |
| `state` | `JSONB` | -- | -- | Object state as JSONB. |
| `state_size` | `INTEGER` | -- | `NOT NULL` | Size of the original pickle in bytes. |
| `refs` | `BIGINT[]` | `'{}'` | `NOT NULL` | Referenced OIDs. |

The storage uses a copy-before-overwrite model: the previous version of an
object is copied to `object_history` before `object_state` is updated.
The current version lives only in `object_state`.

### pack_state (history-preserving mode)

Tracks pack state for history-preserving databases.
Created only when `history_preserving=True`.

| Column | Type | Default | Constraints | Description |
|---|---|---|---|---|
| `zoid` | `BIGINT` | -- | `PRIMARY KEY` | Object ID. |
| `tid` | `BIGINT` | -- | `NOT NULL` | Transaction ID. |

## Indexes

### Core indexes (always present)

| Index name | Table | Type | Expression | Purpose |
|---|---|---|---|---|
| `idx_object_class` | `object_state` | B-tree | `(class_mod, class_name)` | Class-based queries. |
| `idx_object_refs` | `object_state` | GIN | `refs` | Reference graph traversal (pack). |
| `idx_object_state_tid_zoid` | `object_state` | B-tree | `(tid, zoid)` | `poll_invalidations()` queries. |

### History indexes (history-preserving mode)

| Index name | Table | Type | Expression | Purpose |
|---|---|---|---|---|
| `idx_history_tid` | `object_history` | B-tree | `tid` | Transaction-based lookups. |
| `idx_history_zoid_tid` | `object_history` | B-tree | `(zoid, tid DESC)` | `loadBefore()` queries (most recent first). |

## LISTEN/NOTIFY trigger

A trigger on `transaction_log` fires a PostgreSQL `NOTIFY` on every committed
transaction, sending the TID as the payload on the `zodb_invalidations` channel.

```sql
CREATE OR REPLACE FUNCTION notify_commit() RETURNS trigger AS $$
BEGIN
    PERFORM pg_notify('zodb_invalidations', NEW.tid::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_notify_commit
    AFTER INSERT ON transaction_log
    FOR EACH ROW EXECUTE FUNCTION notify_commit();
```

## JSONB state structure

Object state is stored as a JSONB document in the `state` column of `object_state` (and `object_history`).
The JSONB is produced by [zodb-json-codec](https://bluedynamics.github.io/zodb-json-codec/)'s `decode_zodb_record_for_pg_json()` function, which converts ZODB pickle data to JSON.

The class information (`class_mod`, `class_name`) is stored in separate columns, not inside the JSONB.
The JSONB contains only the object's instance state.

For the full list of JSON marker keys (`@ref`, `@t`, `@b`, `@pkl`, `@dt`, and others), see the [zodb-json-codec JSON format reference](https://bluedynamics.github.io/zodb-json-codec/reference/json-format.html).

The only marker specific to zodb-pgjsonb is `@ns` (null-byte sanitization).
PostgreSQL JSONB cannot store `\u0000` characters, so strings containing null bytes are base64-encoded and wrapped in an `@ns` marker.
The storage sanitizes on write and unsanitizes on read automatically.

## Schema installation

The `install_schema()` function in `schema.py` applies DDL idempotently.
It skips DDL when core tables already exist to avoid `ACCESS EXCLUSIVE`
locks that would block concurrent instances holding `REPEATABLE READ`
snapshots.

The installation sequence:

1. Check whether `transaction_log` exists.
2. If absent, execute the full history-free schema DDL (tables, indexes, trigger).
3. If present, ensure indexes added in later schema versions exist (lightweight `CREATE INDEX IF NOT EXISTS`).
4. If `history_preserving=True` and `object_history` does not exist, execute the history-preserving additions.
5. Commit.

State processor DDL (from `get_schema_sql()`) is applied separately via
`register_state_processor()`, using a dedicated autocommit connection
with a 2-second `lock_timeout`.
If the lock cannot be acquired at startup,
the DDL is deferred to the first `tpc_begin()`.
