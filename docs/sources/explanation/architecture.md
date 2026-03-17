<!-- diataxis: explanation -->

# Architecture

zodb-pgjsonb is a ZODB storage adapter that stores object state as PostgreSQL JSONB instead of opaque pickle bytes.
It implements the full suite of ZODB storage interfaces -- IStorage, IMVCCStorage, IBlobStorage, IStorageUndoable, IStorageIteration, and IStorageRestoreable -- while making all stored data queryable through standard SQL.

This page explains the storage class hierarchy, the write and read data paths, MVCC snapshot isolation, the pack algorithm, blob tiering, and the LISTEN/NOTIFY invalidation mechanism.

## Storage hierarchy

ZODB's IMVCCStorage protocol separates the storage factory from per-connection instances.
The factory creates the schema, manages the connection pool, and handles administrative operations.
Each ZODB Connection receives its own storage instance with an independent PostgreSQL connection for snapshot isolation.

```{mermaid}
flowchart TD
    DB[ZODB.DB] -->|new_instance| Main[PGJsonbStorage]
    Main -->|"per-connection"| I1[PGJsonbStorageInstance]
    Main -->|"per-connection"| I2[PGJsonbStorageInstance]
    Main -->|"per-connection"| I3[PGJsonbStorageInstance]
    Main -->|owns| Pool[psycopg3 ConnectionPool]
    Pool -->|getconn| I1
    Pool -->|getconn| I2
    Pool -->|getconn| I3
    I1 -->|"reads/writes"| PG[(PostgreSQL)]
    I2 -->|"reads/writes"| PG
    I3 -->|"reads/writes"| PG
```

**PGJsonbStorage** (the main storage) extends `ConflictResolvingStorage` and `BaseStorage`.
It owns the psycopg3 `ConnectionPool`, installs the database schema on startup, handles OID allocation, TID generation, pack, and state processor registration.
It also keeps its own direct connection for administrative queries like `__len__` and `getSize`.

**PGJsonbStorageInstance** extends `ConflictResolvingStorage` only.
ZODB.DB calls `new_instance()` on the main storage to create one per ZODB Connection.
Each instance borrows a connection from the pool and manages its own REPEATABLE READ snapshot, load cache, and 2PC transaction state.
The instance implements IBlobStorage so that ZODB's Connection class can detect blob support on the object it actually talks to.

## Write path

The write path transforms Python pickle bytes into queryable JSONB and writes them to PostgreSQL in a single pipelined batch.

```{mermaid}
sequenceDiagram
    participant App as Application
    participant ZODB as ZODB Connection
    participant Inst as PGJsonbStorageInstance
    participant Codec as zodb-json-codec (Rust)
    participant SP as State Processors
    participant PG as PostgreSQL

    App->>ZODB: modify persistent object
    ZODB->>Inst: store(oid, serial, pickle_bytes)
    Inst->>Codec: decode_zodb_record_for_pg_json(pickle_bytes)
    Codec-->>Inst: (class_mod, class_name, json_str, refs[])
    Inst->>SP: process(zoid, class_mod, class_name, state)
    SP-->>Inst: extra column values
    Note over Inst: Queue entry in _tmp list
    ZODB->>Inst: tpc_vote(transaction)
    Inst->>PG: BEGIN; pg_advisory_xact_lock(0)
    Inst->>PG: Batch conflict check (SELECT WHERE zoid = ANY(...))
    Inst->>PG: executemany INSERT...ON CONFLICT (pipelined)
    Inst->>PG: COMMIT
    Inst-->>ZODB: tid_bytes
```

### Step by step

1. **store()** receives pickle bytes from ZODB and calls `zodb_json_codec.decode_zodb_record_for_pg_json()`.
This Rust function decodes the two concatenated pickles (class + state) in a ZODB record and returns a JSON string, the class module and name, and an array of referenced OIDs.
The entire pipeline runs in Rust with the GIL released.
No intermediate Python dicts are created for the store path.

2. **State processors** run after decoding.
Registered processors (such as plone-pgcatalog's `CatalogStateProcessor`) can inspect the decoded state, pop keys, and return `ExtraColumn` values that will be written alongside the object state in the same INSERT statement.

3. **tpc_vote()** acquires a PostgreSQL advisory lock (`pg_advisory_xact_lock(0)`) to serialize all write transactions.
It then runs a single batch conflict check (`SELECT zoid, tid FROM object_state WHERE zoid = ANY(...)`) instead of per-object queries.
Finally, it calls `_batch_write_objects()` which uses psycopg3's `executemany()` in pipeline mode -- all INSERT statements go in a single network round-trip.

4. **tpc_finish()** commits the PostgreSQL transaction, inserts a row into `transaction_log` (which fires the NOTIFY trigger), releases the advisory lock, and returns the TID bytes to ZODB.

### History-preserving writes

In history-preserving mode, `_batch_write_objects()` adds a copy-before-overwrite step: before upserting into `object_state`, it copies the existing row for each modified object into `object_history` using `INSERT INTO object_history SELECT ... FROM object_state WHERE zoid = ANY(...)`.
Only previous versions are archived -- the current version always lives in `object_state`.
This model avoids writing duplicate JSONB data on every commit and is 33% faster for batch writes compared to the original full dual-write approach.

## Read path

The read path converts JSONB back to pickle bytes, with an LRU cache to avoid redundant transcoding.

1. **load()** checks the per-instance `LoadCache` first.
On a hit, it returns cached pickle bytes and TID immediately -- no database round-trip, no transcoding.
The cache is an `OrderedDict`-based LRU bounded by byte size (default 16 MB per instance).

2. On a cache miss, the instance executes `SELECT state, tid, class_mod, class_name FROM object_state WHERE zoid = %s` using a prepared statement (`prepare=True`).

3. The returned JSONB state is passed to `zodb_json_codec.encode_zodb_record()`, which produces pickle bytes in Rust.
The result is stored in the load cache before being returned.

In production, the ZODB object cache sits above the storage cache and handles the vast majority of reads.
The storage LRU cache catches objects evicted from the ZODB object cache but still hot at the storage level, avoiding PostgreSQL round-trips for warm data.

## MVCC and snapshot isolation

ZODB requires storage backends to provide multi-version concurrency control (MVCC) so that each Connection sees a consistent snapshot of the database.
zodb-pgjsonb implements this using PostgreSQL's built-in REPEATABLE READ isolation level.

### poll_invalidations

The critical sequencing in `poll_invalidations()` is:

1. End any previous read snapshot (`_end_read_txn()`).
2. Start a new REPEATABLE READ transaction (`_begin_read_txn()`).
The first query anchors the snapshot.
3. Query `transaction_log` for the current maximum TID.
4. Query `object_state` for OIDs changed since the last polled TID.

Starting the snapshot *before* querying for changes ensures that all subsequent `load()` calls see the exact same database state as the invalidation queries.
If the snapshot started after the query, a concurrent commit could land between the poll and the first load, causing an inconsistent view.

### TID generation

Transaction IDs must be globally unique and monotonically increasing.
zodb-pgjsonb uses a PostgreSQL advisory lock (`pg_advisory_xact_lock(0)`) held during `tpc_vote` and `tpc_finish` to serialize all write transactions.
Within the lock, `_new_tid()` generates a timestamp-based TID that is guaranteed to be later than the previous one via `TimeStamp.laterThan()`.

This is a simplicity trade-off: it limits write throughput to one transaction at a time, but guarantees correct TID ordering without complex distributed coordination.
For most ZODB workloads, write transactions are short and the advisory lock introduces negligible contention.

## Connection pool lifecycle

zodb-pgjsonb uses psycopg3's `ConnectionPool` for all database connections.

```{mermaid}
flowchart LR
    Main[PGJsonbStorage] -->|"creates"| Pool[ConnectionPool<br/>min_size / max_size]
    Pool -->|"getconn()"| I1[Instance 1]
    Pool -->|"getconn()"| I2[Instance 2]
    I1 -->|"putconn()"| Pool
    I2 -->|"putconn()"| Pool
    Main -->|"admin conn"| PG[(PostgreSQL)]
```

The pool is created at storage initialization with configurable `min_size` (default 1) and `max_size` (default 10).
A `pool_timeout` parameter (default 30 seconds) limits how long a connection request blocks before raising an error, preventing unbounded waits under load.
Connections use `autocommit=True` and `dict_row` by default.
Each `PGJsonbStorageInstance` borrows a connection from the pool at creation time and returns it when `release()` is called.

## Pack algorithm

Pack (garbage collection) is the standout performance advantage over pickle-based storage backends.
Traditional ZODB storages must load and unpickle every object to discover its references via `referencesf()`.
zodb-pgjsonb stores references in a pre-extracted `refs` BIGINT[] column at write time, enabling a pure SQL graph traversal that never leaves PostgreSQL.

The reachability query uses a recursive CTE:

```sql
WITH RECURSIVE reachable AS (
    SELECT zoid FROM object_state WHERE zoid = 0
    UNION
    SELECT unnest(o.refs)
    FROM object_state o
    JOIN reachable r ON o.zoid = r.zoid
)
SELECT zoid FROM reachable
```

This starts from the root object (zoid=0) and follows all reference edges, collecting every reachable object.
PostgreSQL's recursive CTE engine terminates naturally when no new rows are produced -- there is no explicit depth limit.

The full pack runs in five phases:

1. **Find reachable objects** using the recursive CTE, materialized into a temp table with an index.
2. **Delete unreachable objects** from `object_state`.
In history-preserving mode with a pack time, objects created or modified after the pack time are preserved for undo.
3. **Delete unreachable blobs** from `blob_state`, collecting S3 keys for external cleanup.
4. **Clean history tables** (history-preserving mode only): remove history rows for unreachable objects and old revisions of reachable objects before the pack time.
5. **Clean transaction log**: remove `transaction_log` entries no longer referenced by any `object_state` row.

The result is 13-28x faster pack compared to RelStorage, which must load and unpickle every object in Python to discover references.
See {doc}`performance` for benchmark numbers.

## Blob tiering

zodb-pgjsonb supports two-tier blob storage: PostgreSQL bytea for small blobs and S3 for large blobs.

```{mermaid}
flowchart TD
    Store[storeBlob] -->|"size < threshold?"| Check{blob_threshold<br/>default 100 KB}
    Check -->|"Yes"| PG["PG bytea<br/>(blob_state.data)"]
    Check -->|"No"| S3["S3 upload<br/>(blob_state.s3_key)"]

    Load[loadBlob] -->|"check cache"| Cache{Local cache?}
    Cache -->|"hit"| File[Return file path]
    Cache -->|"miss"| Source{s3_key set?}
    Source -->|"No"| PGRead["Read PG bytea"]
    Source -->|"Yes"| S3Read["Download from S3"]
    PGRead --> File
    S3Read --> File
```

The `blob_threshold` (default 100 KB) determines the cutoff.
Blobs below the threshold go into PostgreSQL's `blob_state.data` column as bytea, keeping them transactional and included in `pg_dump` backups.
Blobs above the threshold are uploaded to S3 with a deterministic key (`blobs/{zoid:016x}/{tid:016x}.blob`) and only the S3 key is stored in `blob_state.s3_key`.

An optional local blob cache directory caches downloaded blobs with LRU eviction.
Blob files use deterministic filenames (`{oid:016x}-{tid:016x}.blob`) as required by `ZODB.blob.Blob.committed()`.

When S3 tiering is not configured, all blobs go to PostgreSQL bytea regardless of size.

## LISTEN/NOTIFY invalidation

ZODB's MVCC protocol requires the storage to report which objects changed since the last poll.
The standard approach is periodic polling, which introduces 1-5 seconds of latency.
zodb-pgjsonb uses PostgreSQL's LISTEN/NOTIFY for near-instant invalidation.

A trigger on the `transaction_log` table fires on every INSERT:

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

When a transaction commits and inserts its metadata row into `transaction_log`, PostgreSQL asynchronously notifies all listening connections with the new TID.
Storage instances that have subscribed to the `zodb_invalidations` channel receive the notification within milliseconds, allowing them to invalidate cached objects and refresh their snapshots with minimal delay.

This mechanism works across multiple application servers connecting to the same PostgreSQL instance, providing cluster-wide invalidation without any additional infrastructure.

## History modes

zodb-pgjsonb supports two history modes that determine how much historical data is retained.

### History-free mode

In history-free mode (the default), `object_state` holds only the current version of each object.
Each store operation upserts into `object_state` (`INSERT ... ON CONFLICT (zoid) DO UPDATE`), replacing the previous version.
There is no `object_history` table.
This mode offers the lowest storage overhead and simplest query paths but does not support undo operations.

### History-preserving mode

In history-preserving mode, previous versions are archived in `object_history` before being overwritten in `object_state`.
This uses a copy-before-overwrite model: at write time, the existing `object_state` row for each modified object is copied into `object_history`, then `object_state` is upserted with the new version.
The current version always lives in `object_state`; only previous versions go to `object_history`.

This design enables:
- **loadBefore()**: retrieve the state of an object at a specific point in time by querying `object_history` with a TID range.
- **history()**: return the revision history of an object by querying both `object_state` (current) and `object_history` (previous) with a UNION.
- **undo()**: revert an object to a previous state by swapping the current and historical versions.

The copy-before-overwrite model is more efficient than a full dual-write approach (where every version is written to both tables) because it avoids writing duplicate JSONB data.
Batch writes are up to 33% faster and storage overhead is roughly 50% lower.

### Switching between modes

Both directions are supported at runtime.
Switching from history-free to history-preserving creates the `object_history` table and begins tracking changes on the next modification.
Switching from history-preserving to history-free uses `convert_to_history_free()` to drop history tables, clean old blob versions, and remove orphaned transaction log entries.
See {doc}`../how-to/index` for details.
