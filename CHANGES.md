# Changelog

## 1.3.1

- Add progress logging to `copyTransactionsFrom()` for `zodbconvert` usage:
  per-transaction TID, record count, throughput (MB/s), and completion summary.
  [mamico] [#16]

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
