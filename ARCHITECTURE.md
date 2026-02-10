# Architecture: ZODB PostgreSQL JSONB Storage

## Vision

A modern, PostgreSQL-only ZODB storage that stores object state as **JSONB** (not pickle/bytea), using **zodb-json-codec** for transparent transcoding. Blobs use tiered storage (small in PG bytea, large in S3 via zodb-s3blobs). Enables SQL queryability of ZODB data while remaining fully transparent to ZODB/Zope/Plone.

## Design Decisions

- **Direction C: Informed Independent** — new independent storage, studying proven algorithms from RelStorage
- **Separate repo/package** — https://github.com/bluedynamics/zodb-pgjsonb
- **Both history modes (configurable)** — history-free and history-preserving, like RelStorage
- **Tiered blob storage** — small blobs in PG bytea, large blobs in S3 (configurable threshold)

---

## Architecture

```
ZODB.DB
  └-> PGJsonbStorage (IStorage + IMVCCStorage + IBlobStorage + IStorageUndoable + IStorageIteration + IStorageRestoreable)
       ├-> zodb-json-codec    (pickle <-> JSON transcoding, Rust)
       ├-> psycopg3           (sync, pipelined writes via executemany, ConnectionPool)
       ├-> LISTEN/NOTIFY      (instant invalidation via PG trigger)
       ├-> Tiered blobs       (PG bytea + optional S3 via zodb-s3blobs)
       └-> Pure SQL pack/GC   (recursive CTE on pre-extracted refs column)
```

### What we borrow from RelStorage (algorithms, not code)

- Transaction ID allocation (monotonic, global lock via `pg_advisory_xact_lock`)
- MVCC semantics (snapshot isolation, `new_instance()` per connection)
- Pack algorithm concept (mark reachable, sweep unreachable) — reimplemented as pure SQL graph traversal via `refs` column, no object loading needed
- Conflict resolution via inherited `ConflictResolvingStorage` (data is pickle at boundaries)

### What we design fresh

- Schema optimized for JSONB from day one
- psycopg3 sync API with pipelined `executemany()` batch writes
- LISTEN/NOTIFY for instant invalidation
- Tiered blob storage with configurable threshold
- Configurable history modes (history-free / history-preserving)
- SQL query API as first-class citizen

---

## Schema Design

### History-Free Mode (default, recommended for Plone)

```sql
CREATE TABLE transaction_log (
    tid         BIGINT PRIMARY KEY,
    username    TEXT DEFAULT '',
    description TEXT DEFAULT '',
    extension   BYTEA DEFAULT ''
);

CREATE TABLE object_state (
    zoid        BIGINT PRIMARY KEY,
    tid         BIGINT NOT NULL REFERENCES transaction_log(tid),
    class_mod   TEXT NOT NULL,
    class_name  TEXT NOT NULL,
    state       JSONB,
    state_size  INTEGER NOT NULL,
    refs        BIGINT[] NOT NULL DEFAULT '{}'
);

CREATE TABLE blob_state (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    blob_size   BIGINT NOT NULL,
    data        BYTEA,
    s3_key      TEXT,
    PRIMARY KEY (zoid, tid)
);

CREATE INDEX idx_object_class ON object_state (class_mod, class_name);
CREATE INDEX idx_object_refs ON object_state USING gin (refs);
```

### History-Preserving Mode (adds history tables)

```sql
CREATE TABLE object_history (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    class_mod   TEXT NOT NULL,
    class_name  TEXT NOT NULL,
    state       JSONB,
    state_size  INTEGER NOT NULL,
    refs        BIGINT[] NOT NULL DEFAULT '{}',
    PRIMARY KEY (zoid, tid)
);

CREATE TABLE blob_history (
    zoid        BIGINT NOT NULL,
    tid         BIGINT NOT NULL,
    blob_size   BIGINT NOT NULL,
    data        BYTEA,
    s3_key      TEXT,
    PRIMARY KEY (zoid, tid)
);

CREATE TABLE pack_state (
    zoid        BIGINT PRIMARY KEY,
    tid         BIGINT NOT NULL
);
```

### Schema Notes

- **Class columns inline** (not normalized FK): avoids JOIN on the hot path (`load()`), class strings compress well via TOAST
- **`state` is `@s` only**: the `@cls` marker is extracted into `class_mod`/`class_name` columns, saving JSONB space and enabling efficient class-based queries
- **`state_size`**: original pickle size, useful for monitoring
- **Advisory locks** for commit serialization: `pg_advisory_xact_lock(0)`, released automatically on transaction end
- **GIN index on `refs`**: enables `@>` containment queries for pack/GC
- **`refs` column**: pre-extracted persistent reference OIDs, enables pure SQL pack/GC

### Pack via Pure SQL Graph Traversal

Traditional ZODB pack loads and unpickles every object to extract references. PGJsonbStorage extracts `@ref` OIDs at `store()` time into the `refs` column, enabling pack as a single recursive SQL query:

```sql
WITH RECURSIVE reachable AS (
    SELECT zoid FROM object_state WHERE zoid = 0
    UNION
    SELECT unnest(o.refs)
    FROM object_state o
    JOIN reachable r ON o.zoid = r.zoid
)
DELETE FROM object_state
WHERE zoid NOT IN (SELECT zoid FROM reachable);
```

Result: 15-28x faster than RelStorage for pack operations.

---

## Tiered Blob Storage

| Mode     | Config                     | Where blobs are stored        |
|----------|----------------------------|-------------------------------|
| PG-only  | No S3 keys set             | All blobs in PostgreSQL bytea |
| Tiered   | S3 keys + blob-threshold   | Small in PG, large in S3     |
| S3-only  | S3 keys + blob-threshold=0 | All blobs in S3               |

S3 tiering uses `zodb-s3blobs` (`S3Client` + `S3BlobCache`) as optional dependency.

---

## Transcoding Integration

### Store Path (ZODB -> PostgreSQL)

```
pickle bytes -> zodb_json_codec.decode_zodb_record()
  -> {"@cls": [mod, name], "@s": {state...}}
  -> extract class_mod, class_name, state, refs
  -> INSERT INTO object_state (JSONB)
```

### Load Path (PostgreSQL -> ZODB)

```
SELECT class_mod, class_name, state FROM object_state
  -> reconstruct {"@cls": [mod, name], "@s": {state...}}
  -> zodb_json_codec.encode_zodb_record()
  -> pickle bytes
```

### Conflict Resolution

Data is pickle bytes at the ZODB boundary. `ConflictResolvingStorage.tryToResolveConflict()` receives pickle, unpickles, calls `_p_resolveConflict()`, re-pickles. The default identity transform hooks are correct since all data at the resolution boundary is already pickle.

---

## MVCC Architecture

```
PGJsonbStorage (main, factory)
  ├── implements IMVCCStorage
  ├── owns: schema, OID allocation, DSN, connection pool
  ├── new_instance() -> PGJsonbStorageInstance
  └── close() closes pool + main connection

PGJsonbStorageInstance (per-connection)
  ├── borrows PG connection from pool (autocommit=True)
  ├── REPEATABLE READ snapshots for consistent reads
  ├── poll_invalidations() via SQL query
  ├── store/tpc_begin/vote/finish/abort (write path)
  └── release() returns connection to pool
```

---

## Migration via zodbconvert

PGJsonbStorage implements `IStorageRestoreable` (`restore()`, `restoreBlob()`), making it compatible with `zodbconvert` out of the box:

```ini
<source>
    <relstorage>
        <postgresql>
            dsn dbname='zodb_old'
        </postgresql>
    </relstorage>
</source>
<destination>
    %import zodb_pgjsonb
    <pgjsonb>
        dsn dbname='zodb_new'
    </pgjsonb>
</destination>
```

---

## Package Structure

```
zodb-pgjsonb/
├── src/zodb_pgjsonb/
│   ├── __init__.py           # public API
│   ├── storage.py            # PGJsonbStorage + PGJsonbStorageInstance (core)
│   ├── schema.py             # DDL definitions
│   ├── objects.py            # ObjectStore (JSONB CRUD)
│   ├── invalidation.py       # LISTEN/NOTIFY channel
│   ├── blobs.py              # BlobManager (tiered PG/S3)
│   ├── packer.py             # Pack/GC (pure SQL)
│   ├── interfaces.py         # Zope interface declarations
│   ├── config.py             # ZConfig factory
│   └── component.xml         # ZConfig schema definition
├── tests/
│   ├── test_storage.py       # Core IStorage tests
│   ├── test_mvcc.py          # MVCC / poll_invalidations
│   ├── test_history.py       # History-preserving mode + undo
│   ├── test_blobs.py         # Blob storage (PG + S3)
│   ├── test_conflict.py      # Conflict resolution (BTrees)
│   ├── test_conformance.py   # ZODB conformance test suite
│   ├── test_iteration.py     # IStorageIteration
│   ├── test_roundtrip.py     # Codec roundtrip verification
│   └── test_stress.py        # Concurrent write stress tests
├── benchmarks/
│   └── bench.py              # Performance benchmarks vs RelStorage
├── example/                  # Docker Compose demo setup
├── ARCHITECTURE.md           # This file
├── BENCHMARKS.md             # Performance results
├── pyproject.toml
└── README.md
```

---

## Comparison: RelStorage vs PGJsonbStorage

| Feature | RelStorage | PGJsonbStorage |
|---------|-----------|----------------|
| Storage format | pickle/bytea | JSONB |
| SQL queryable | No | Yes (GIN indexes, jsonpath) |
| Databases | PG, MySQL, Oracle, SQLite | PostgreSQL only |
| Python driver | psycopg2 | psycopg3 (pipelined writes, pool) |
| Invalidation | Polling (1-5s latency) | LISTEN/NOTIFY (~1ms) |
| Blob storage | Filesystem or cloud (separate) | Tiered PG + S3 (integrated) |
| History modes | Both | Both (configurable) |
| Conflict resolution | Built-in | Via inherited ConflictResolvingStorage |
| Pack algorithm | Load+unpickle every object | Pure SQL graph traversal via `refs` column |
| Codec dependency | None | zodb-json-codec (Rust) |
| Maturity | 15+ years | New |
