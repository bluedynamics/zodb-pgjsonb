# Changelog

## 1.10.2

- Fix instance `load()` not using prefetch refs expression (#40).
  `PGJsonbStorageInstance.load()` now uses the registered
  `prefetch_refs_expr` and prefetches referenced objects into the
  load cache — previously this only worked in direct-use mode.

- Fix cache warmer `_flush()` atomicity.  The decay UPDATE, score
  UPSERT, and low-score DELETE are now wrapped in an explicit
  `BEGIN`/`COMMIT` transaction, preventing inconsistent scores if the
  process is killed mid-flush.

- Apply deferred processor DDL in direct-use storage path.  Previously
  `_apply_pending_ddl()` only fired from instance `tpc_begin()`;
  main storage `_begin()` now also applies deferred schema changes.

- `BackgroundBlobSink`: prune completed futures every 256 submits to
  bound memory during large migrations with many blobs.

- Cache warmer: use `threading.Event` instead of bare `bool` for the
  warming-done flag (future-proofs for free-threaded Python / PEP 703).

- Internal: extract `storage.py` (3700→1535 lines) into focused modules
  (`batch.py`, `conflict.py`, `undo.py`, `serialization.py`, `migration.py`,
  `stats.py`, `instance.py`).  Deduplicate `loadSerial` between main
  storage and instance.  Migration pipeline moved to `CopyTransactionsMixin`.
  Reduce `_copyTransactionsFrom_parallel` cyclomatic complexity from 57→17
  by extracting `WatermarkTracker`, `ProgressTracker`, `_create_blob_sink`.

## 1.10.1

- Cache warmer: use actual `AVG(state_size)` from DB instead of
  hardcoded 2KB estimate (#51).  With median object size of 163B,
  the warmer now targets ~40k objects instead of 3200 (at 64MB cache).

## 1.10.0

- Learning cache warmer with L2 warm cache (#48).  Records which objects
  are loaded first after each startup, persists scores to PG with
  exponential decay, and pre-loads the highest-scored objects into a
  shared L2 warm cache on the next startup.  Expected cold-start latency
  improvement: 5-14s → ~1-2s.  Configurable via `cache-warm-pct`
  (default 10%) and `cache-warm-decay` (default 0.8).  Set
  `cache-warm-pct` to 0 to disable.

## 1.9.6

- Fix startup DDL blocking rolling updates (#96).  `ALTER TABLE SET
  COMPRESSION` and `CREATE INDEX IF NOT EXISTS` now use `lock_timeout
  = '5s'`.  If blocked by old pods' REPEATABLE READ connections, the
  DDL is silently skipped and retried on next startup.

## 1.9.5

- Fix `new_oid()` leaving main connection "idle in transaction"
  indefinitely (#45).  The main storage connection now uses
  `autocommit=True` after schema init, so SELECTs (new_oid, load,
  history, stats) never open implicit transactions.  Write paths
  that need transactions continue using explicit `BEGIN`/`COMMIT`.

## 1.9.4

- Enable LZ4 TOAST compression on JSONB and BYTEA columns (PG 14+).
  LZ4 decompresses ~10x faster than the default pglz at similar
  compression ratios.  Applied automatically during schema init.
  Only affects new writes — existing rows keep pglz until rewritten.
  Silently skipped on PG < 14.

## 1.9.3

- Fix ZODB undo nullifying catalog columns (plone-pgcatalog #30).
  `undo()` now calls `_process_state()` on restored entries (same as
  `restore()` does), so catalog columns (path, idx, searchable_text,
  allowed_roles, etc.) are recomputed from the restored state instead
  of being left as NULL.

## 1.9.2

- Fix refs prefetch over-fetching (#40). Replace hardcoded class_mod
  blacklist with a pluggable SQL expression via
  `register_prefetch_refs_expr()`. The expression is included in the
  load() query as a conditional refs column. When None (default), no
  prefetch occurs — same behavior as pre-v1.9.0. Applications register
  their own expression (e.g. pgcatalog uses
  `CASE WHEN idx IS NOT NULL THEN refs END` to prefetch only for
  cataloged content objects).

## 1.9.1

- Fix refs prefetch over-fetching (#40). Only prefetch refs for
  content-type objects, not internal ZODB structures
  (PersistentMapping, OOBTree, etc.) whose refs cascade into massive
  over-fetching. v1.9.0 made cold-start 40-84% slower; this fix
  restores the benefit by limiting prefetch to objects that actually
  have useful sub-objects (annotations, workflows, etc.).

## 1.9.0

- Prefetch referenced objects on `load()` (#38). When an object is
  loaded, its `refs` column (annotations, sub-mappings, OOBTrees) is
  used to prefetch all directly referenced objects in a single
  `load_multiple()` call. Turns N+1 individual loads into 1 batch
  load. Default-on, no configuration needed. Cold-start page loads
  reduced by ~85% fewer roundtrips per object.

## 1.8.1

- Fix packer DELETE using NOT IN anti-join (#35). Replaced all
  `NOT IN (SELECT zoid FROM reachable_oids)` with
  `NOT EXISTS (SELECT 1 FROM reachable_oids r WHERE r.zoid = ...)`.
  NOT IN builds a hash of millions of rows; NOT EXISTS uses an
  indexed anti-join that short-circuits. Pack on 4.4M objects went
  from 48+ minutes (incomplete) to expected minutes.
  Also added progress logging per phase.

## 1.8.0

- Add `load_multiple(oids)` method to `PGJsonbStorageInstance` for
  batch object loading (#34). Loads multiple objects in a single
  `SELECT WHERE zoid = ANY()` query instead of individual roundtrips.
  Checks `_load_cache` first, only queries misses. Caches all results.

## 1.7.3

- Fix: add `lock_timeout = '30s'` to deferred DDL application
  (`_apply_pending_ddl`). Previously, deferred DDL could block
  indefinitely at `tpc_begin()` during rolling deployments when old
  REPEATABLE READ sessions held ACCESS SHARE locks.

## 1.7.2

- Fix OID collisions across multiple application pods (#31). Replace
  BaseStorage's in-memory OID counter with a PostgreSQL sequence (`zoid_seq`).
  `new_oid()` now calls `nextval('zoid_seq')`, guaranteeing uniqueness across
  all processes sharing the same database. The sequence is created automatically
  during schema initialization and synchronized with existing data on restart.

## 1.7.1

- Smooth ETA during parallel migration using exponential moving average instead
  of single-window rate. Eliminates wild jumps caused by S3 retries or variable
  transaction sizes.

## 1.7.0

- Add `blob_mode` parameter to `copyTransactionsFrom()` for decoupled S3 blob
  uploads during parallel migration. Three modes: `"inline"` (default, current
  behavior), `"background"` (S3 uploads in a background thread pool, PG writes
  continue independently), and `"deferred:<manifest_path>"` (write manifest file
  for later upload). Background mode significantly improves throughput when S3
  latency is the bottleneck.

## 1.6.1

- Fix `_stage_blob` `PermissionError` when hard-linking blobs owned by
  another user (e.g. Docker's uid 500). `fs.protected_hardlinks` blocks
  the link even on the same filesystem. Now falls back to using the
  source path directly.

## 1.6.0

- Support incremental parallel imports with watermark-based resume.
  `copyTransactionsFrom(source, workers=N, start_tid=X)` now accepts a
  `start_tid` parameter. A `migration_watermark` table tracks contiguous
  commit progress so interrupted parallel imports can resume safely without
  losing transactions to out-of-order worker commits.
  Fixes #24.

## 1.5.6

- Fix `_stage_blob` crash when source blobs and temp directory are on
  different filesystems. The hard-link fallback tried to unlink an
  already-removed placeholder, causing `FileNotFoundError` during
  `copyTransactionsFrom` with `--workers`.

## 1.5.5

- Parallel S3 blob uploads in `_batch_write_blobs()`. When a transaction
  contains multiple blobs destined for S3, they are now uploaded concurrently
  using a thread pool (up to 8 workers). Single-blob transactions skip the
  thread pool to avoid overhead. This significantly speeds up bulk operations
  like `zodb-convert` imports with many blobs per transaction.

## 1.5.4

- Add `PGTestDB` class in `zodb_pgjsonb.testing` for test database
  management with stackable snapshot/restore (COPY BINARY protocol).
  Mirrors DemoStorage's push/pop semantics for per-test isolation.

- Consolidate test fixtures into shared `conftest.py` (clean_db, storage,
  db, hp_storage, hp_db), removing ~210 lines of duplicated boilerplate.

## 1.5.3

- Add composite index `(tid, zoid)` on `object_state` to speed up
  `poll_invalidations()` queries. Previously required a full sequential
  scan on large tables. Existing databases get the index automatically
  on next startup. Fixes #19.

## 1.5.2

- Add blob storage statistics API: `get_blob_stats()` and
  `get_blob_histogram()` methods on `PGJsonbStorage` for querying
  blob count, size, per-tier (PG/S3) breakdown, and logarithmic
  size distribution.

- Add Zope Control Panel integration: "Blob Storage" tab appears
  in Control Panel -> Database -> [dbname] when the storage is a
  PGJsonbStorage. Shows blob statistics, tier breakdown, and
  size distribution histogram. Previously in plone-pgcatalog.

## 1.5.1

- Parallel copy performance and observability improvements:
  - Backpressure via `BoundedSemaphore(workers * 2)` prevents unbounded blob
    temp file accumulation (critical for large blob databases).
  - Hard-link blob staging on same filesystem (instant, zero extra disk);
    cross-device: read source directly without copying.
  - Progress logging (every 10s) with elapsed time, read/written transaction
    counts, OID and blob counts, percentage, and ETA.
  - Abort immediately on worker errors with clear message (e.g. non-empty
    target database) instead of silently accumulating failures.
  - Log WARNING per missing source blob (with oid/tid) and ERROR summary at end.
  - ETA based on recent-window throughput instead of overall average
    (much more accurate, especially when early transactions are empty after pack).
  - Non-blocking drain loop with periodic "Still waiting" log instead of
    silent hang during `executor.shutdown`.

## 1.5.0

- Add parallel `copyTransactionsFrom(source, workers=N)` for faster migrations.
  Multiple worker threads write to PostgreSQL concurrently, bypassing the
  advisory lock serialization. The main thread reads from the source storage,
  decodes pickles, and orchestrates OID-level dependency tracking to guarantee
  correct write ordering. Default `workers=1` preserves existing sequential
  behavior.

## 1.4.1

- Fix `FileNotFoundError` when `blob-temp-dir` is configured but the directory
  does not exist yet. The storage now auto-creates the directory on startup.

## 1.4.0

- Reduce default `blob-threshold` from 1MB to 100KB to reduce WAL pollution
  when S3 tiering is enabled.

- Add history mode switching support: `convert_to_history_free()` and
  `convert_to_history_preserving()` methods on `PGJsonbStorage` for
  converting between history-free and history-preserving modes.
  HP→HF conversion drops history tables, cleans old blob versions,
  and removes orphaned transaction log entries.
  Startup warning logged when HP tables exist in HF mode.

- Add progress logging to `copyTransactionsFrom()` for `zodbconvert` usage:
  per-transaction TID, record count, throughput (MB/s), and completion summary.
  [mamico] [#16]

- Fix schema init blocking concurrent instances: skip DDL when core tables
  already exist, avoiding `ACCESS EXCLUSIVE` locks that conflict with
  `REPEATABLE READ` snapshots held by other processes. [#15]

## 1.3.0

- **Direct JSON string decode path**: Use `decode_zodb_record_for_pg_json()`
  from zodb-json-codec 1.4.0 — the entire pickle-to-JSON pipeline now runs
  in Rust with the GIL released. No intermediate Python dicts are created
  for the store path. 1.3x faster end-to-end on real-world data. [#14]

- **History-preserving optimization**: Changed `object_history` from full
  dual-write to copy-before-overwrite model — only previous versions are
  archived before overwrite. Eliminated redundant `blob_history` table.
  Batch writes up to 33% faster, loadBefore 15% faster, undo 14% faster,
  ~50% less storage overhead for HP mode. [#13]

- Require `zodb-json-codec>=1.4.0`.

## 1.2.2

- Fix blob migration: override `copyTransactionsFrom` for blob-aware copying.
  `BaseStorage.copyTransactionsFrom` only calls `restore()` and silently drops
  blob data during `zodbconvert` from FileStorage+BlobStorage. The new override
  detects blobs via `is_blob_record()` and uses `restoreBlob()` to migrate them.
  Blob files are copied (not moved) to preserve source storage integrity.
  [mamico, jensens] [#9, #10]

## 1.2.1

Security review fixes (addresses #7):

- **PG-H1:** Strengthen ExtraColumn / register_state_processor docstrings with
  security guidance for SQL expressions.
- **PG-H2:** Replace unrestricted `zodb_loads` with `_RestrictedUnpickler` for
  legacy pickle extension data (blocks arbitrary code execution).
- **PG-H3:** Fix DSN password masking regex to handle quoted passwords.
- **PG-M1:** Add SECURITY NOTE to `_batch_write_objects()` about dynamic SQL
  from `ExtraColumn.value_expr`.
- **PG-M2:** Expand `_new_tid()` docstring about advisory lock serialization.
- **PG-M3:** Add `pool_timeout` parameter (default 30s) to prevent unbounded
  connection waits; exposed in ZConfig as `pool-timeout`.
- **PG-M4:** Remove `errors="surrogatepass"` from `_unsanitize_from_pg()` to
  reject invalid surrogate bytes instead of silently accepting them.
- **PG-L1:** Document PostgreSQL recursive CTE depth behavior in packer.
- **PG-L2:** Add DSN format validation in ZConfig factory (`config.py`).

## 1.2.0

- Add `finalize(cursor)` hook to state processor protocol [#5]
  plone-pgcatalog needs to apply partial JSONB merges (idx || patch) for lightweight reindex operations (e.g. `reindexObjectSecurit`) without full ZODB serialization. The finalize(cursor) hook provides the extension point.

## 1.1.0

- Added `pg_connection` read-only property to `PGJsonbStorageInstance`,
  exposing the underlying psycopg connection for read queries that need
  to share the same REPEATABLE READ snapshot as ZODB loads (e.g. catalog
  queries in plone-pgcatalog).

- Security hardening: validate `ExtraColumn.name` against SQL identifier pattern to prevent injection via state processor plugins.
- Mask credentials in DSN before debug logging (`_mask_dsn()`).
- Restrict blob file permissions to `0o600` (owner-only, was `0o644`).
- Narrow bare `except Exception` blocks to specific exception types.

## 1.0.1

- Fix `FileNotFoundError` when using blobs with `transaction.savepoint()` (e.g. plone.exportimport content import). Blob files are now staged to a stable location before the caller can delete them. [#1]

## 1.0.0

### Added

- **State processor plugin system**: Register processors that extract extra
  column data from object state during writes. This enables downstream
  packages (e.g. plone-pgcatalog) to write supplementary columns alongside
  the object state in a single atomic `INSERT...ON CONFLICT` statement.

  New public API:

  - `ExtraColumn(name, value_expr, update_expr=None)` dataclass — declares
    an extra column for `object_state`.
  - `PGJsonbStorage.register_state_processor(processor)` — registers a
    processor whose `process(zoid, class_mod, class_name, state)` method
    can pop keys from the state dict and return extra column data.

  Processors are called in `store()` after pickle-to-JSON decoding.
  Extra columns are included in the pipelined `executemany()` batch write
  during `tpc_vote()`, keeping everything in the same PostgreSQL
  transaction for full atomicity.

- **State processor DDL via `get_schema_sql()`**: Processors can now
  optionally provide a `get_schema_sql()` method returning DDL statements
  (e.g. `ALTER TABLE`, `CREATE INDEX`). The DDL is applied using the
  storage's own connection during `register_state_processor()`, avoiding
  REPEATABLE READ lock conflicts with pool connections.

### Optimized

- **Batch conflict detection**: Conflict checks are now batched into a
  single `SELECT ... WHERE zoid = ANY(...)` query in `tpc_vote()` instead
  of individual per-object queries in `store()`. Eliminates N-1 SQL
  round trips per transaction while holding the advisory lock.

- **Prepared statements**: Added `prepare=True` to hot-path queries
  (`load`, `loadSerial`, `loadBefore`) for faster repeated execution.

- **Removed GIN index on state JSONB**: The `jsonb_path_ops` GIN index
  indexed every key-path and value, causing significant write amplification.
  With plone-pgcatalog providing dedicated query columns, direct state
  JSONB queries are no longer needed in production.

## 1.0.0a1

Initial feature-complete release.

### Added

- **Core IStorage**: Full two-phase commit (2PC) with ZODB.DB integration.
- **IMVCCStorage**: Per-connection MVCC instances with REPEATABLE READ
  snapshot isolation and advisory lock TID serialization.
- **IBlobStorage**: Blob support using PostgreSQL bytea with deterministic
  filenames for `Blob.committed()` compatibility.
- **S3 tiered blob storage**: Configurable threshold to offload large blobs
  to S3 while keeping small blobs in PostgreSQL.
- **History-preserving mode**: Dual-write to `object_state` and
  `object_history` with full `IStorageUndoable` support (undo, undoLog,
  undoInfo).
- **IStorageIteration / IStorageRestoreable**: Transaction and record
  iteration for backup/restore tooling.
- **Conflict resolution**: Inherited `tryToResolveConflict` with serial
  cache for correct `loadSerial` during conflict resolution.
- **ZConfig integration**: `%import zodb_pgjsonb` with `<pgjsonb>` section
  for Zope/Plone deployments.
- **PG null-byte sanitization**: Automatic `\u0000` handling with `@ns`
  marker for JSONB compatibility.

### Optimized

- **LRU load cache**: Configurable in-memory cache for `load()` calls.
- **Connection pooling**: psycopg3 connection pool with configurable size.
- **Batch SQL writes**: `executemany()` for pipelined store performance
  during `tpc_vote()`.
- **Single-pass store**: `decode_zodb_record_for_pg` for combined class
  extraction and state decoding.
- **Reduced default cache**: 16 MB per instance (down from 64 MB).
