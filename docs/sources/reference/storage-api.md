<!-- diataxis: reference -->

# Storage API

This page documents the public API of `PGJsonbStorage` and
`PGJsonbStorageInstance`, the two classes that implement ZODB storage.

`PGJsonbStorage` is the main storage (factory).
It manages schema initialization, OID allocation, shared state, and
creates per-connection instances.

`PGJsonbStorageInstance` is a per-connection MVCC instance created by
`new_instance()`.
Each ZODB Connection receives its own instance with an independent
PostgreSQL connection for snapshot isolation.

## PGJsonbStorage

```python
from zodb_pgjsonb.storage import PGJsonbStorage
```

Declared interfaces: `IPGJsonbStorage`, `IMVCCStorage`, `IBlobStorage`,
`IStorageUndoable`, `IStorageIteration`, `IStorageRestoreable`.

Inherits from: `ConflictResolvingStorage`, `BaseStorage`.

### Constructor

```python
PGJsonbStorage(
    dsn: str,
    name: str = "pgjsonb",
    history_preserving: bool = False,
    blob_temp_dir: str | None = None,
    cache_local_mb: int = 16,
    pool_size: int = 1,
    pool_max_size: int = 10,
    pool_timeout: float = 30.0,
    s3_client: S3Client | None = None,
    blob_cache: S3BlobCache | None = None,
    blob_threshold: int = 102_400,
)
```

See {doc}`configuration` for parameter descriptions.

The constructor connects to PostgreSQL, creates a connection pool,
installs the schema, and restores max OID and last TID from
existing data.

### IMVCCStorage methods

`new_instance() -> PGJsonbStorageInstance`
: Create a per-connection storage instance.
  Each ZODB Connection gets its own instance with an independent
  PostgreSQL connection.

`release() -> None`
: Release resources.
  No-op on the main storage.

`poll_invalidations() -> list[bytes]`
: Poll for invalidations.
  No-op on the main storage; returns `[]`.

`sync(force: bool = True) -> None`
: Sync snapshot.
  No-op on the main storage.

### IStorage read methods

`load(oid: bytes, version: str = "") -> tuple[bytes, bytes]`
: Load the current object state.
  Returns `(pickle_bytes, tid_bytes)`.
  Raises `POSKeyError` if the object does not exist.
  Results are cached in the per-instance LRU cache.

`loadBefore(oid: bytes, tid: bytes) -> tuple[bytes, bytes, bytes] | None`
: Load object data before a given TID.
  Returns `(data, serial, end_serial)` or `None`.
  In history-preserving mode, queries `object_history`.
  In history-free mode, returns the current state if its TID is less
  than the requested TID.

`loadSerial(oid: bytes, serial: bytes) -> bytes`
: Load a specific revision of an object.
  Returns pickle bytes.
  Raises `POSKeyError` if the revision does not exist.
  Checks the serial cache first (needed for conflict resolution in
  history-free mode).

### IStorage write methods

`store(oid: bytes, serial: bytes, data: bytes, version: str, transaction) -> None`
: Queue an object for storage during the current transaction.
  Decodes pickle to JSON via `zodb-json-codec`, runs state processors,
  and defers conflict detection to `_vote()`.

`checkCurrentSerialInTransaction(oid: bytes, serial: bytes, transaction) -> None`
: Queue a read-conflict check for batch verification in `_vote()`.

### IStorage two-phase commit

`tpc_begin(tid, u, d, e) -> None`
: Begin a two-phase commit.
  Starts a PostgreSQL transaction and acquires advisory lock 0.

`tpc_vote(transaction) -> list | None`
: Flush pending stores and blobs to PostgreSQL.
  Performs batch conflict detection, writes transaction log, objects,
  and blobs.
  Calls `finalize()` on each registered state processor.
  Returns a list of resolved conflict OIDs, or `None`.

`tpc_finish(transaction, f: callable | None = None) -> bytes`
: Commit the PostgreSQL transaction and run the optional callback.
  Returns the transaction's TID bytes.

`tpc_abort(transaction) -> None`
: Rollback the PostgreSQL transaction and clean up temp blob files.

### IStorage metadata methods

`lastTransaction() -> bytes`
: Return the TID of the last committed transaction.

`__len__() -> int`
: Return the approximate number of objects in `object_state`.

`getSize() -> int`
: Return the approximate total size in bytes (sum of `state_size`).

`history(oid: bytes, size: int = 1) -> list[dict]`
: Return revision history for an object.
  Each dict contains: `time`, `tid`, `serial`, `user_name`,
  `description`, `size`.
  In history-preserving mode, queries both `object_state` and
  `object_history`.

`pack(t: float, referencesf) -> None`
: Pack the storage by removing unreachable objects and their blobs.
  In history-preserving mode, removes old revisions before the pack time.
  Cleans up S3 blobs for deleted objects.

`close() -> None`
: Close all database connections, the connection pool, and remove
  the blob temp directory.

### IStorageUndoable methods

`supportsUndo() -> bool`
: Returns `True` in history-preserving mode, `False` otherwise.

`undoLog(first: int = 0, last: int = -20, filter: callable | None = None) -> list[dict]`
: Return a list of transaction descriptions for undo.
  Each dict contains: `id` (TID bytes), `time`, `user_name` (bytes),
  `description` (bytes), plus any extension metadata.
  Returns `[]` in history-free mode.
  Excludes transactions at or before the last pack time.

`undo(transaction_id: bytes, transaction=None) -> tuple[bytes, list[bytes]]`
: Undo a transaction by restoring previous object states.
  Returns `(tid, [oid_bytes, ...])` for the new undo transaction.
  Raises `UndoError` in history-free mode.

### IStorageIteration methods

`iterator(start: bytes | None = None, stop: bytes | None = None) -> Iterator[TransactionRecord]`
: Iterate over transactions yielding `TransactionRecord` objects.
  Borrows a connection from the pool for the duration of iteration.
  In history-free mode, each object appears once at its current TID.
  In history-preserving mode, all revisions are included.

### IStorageRestoreable methods

`restore(oid: bytes, serial: bytes, data: bytes, version: str, prev_txn, transaction) -> None`
: Write pre-committed data without conflict checking.
  Used by `copyTransactionsFrom()` and `zodbconvert`.

`restoreBlob(oid: bytes, serial: bytes, data: bytes, blobfilename: str, prev_txn, transaction) -> None`
: Restore object data and a blob file without conflict checking.

`copyTransactionsFrom(other, workers: int = 1) -> None`
: Copy all transactions from another storage, including blobs.
  Blob files are copied (not moved) to preserve source storage integrity.
  When `workers` > 1, uses parallel PostgreSQL connections for concurrent writes.
  The main thread reads from the source, decodes pickles, and tracks OID-level
  dependencies to guarantee correct write ordering.

### IBlobStorage methods

`storeBlob(oid: bytes, oldserial: bytes, data: bytes, blobfilename: str, version: str, transaction) -> None`
: Store object data and a blob file.
  Calls `store()` for the object data and stages the blob file
  to a stable temp location.

`loadBlob(oid: bytes, serial: bytes) -> str`
: Return the path to a file containing the blob data.
  Uses deterministic filenames (`{oid:016x}-{tid:016x}.blob`) so
  repeated calls return the same path.
  Checks pending blobs, the local cache, the S3 blob cache, and then
  the database in that order.

`openCommittedBlobFile(oid: bytes, serial: bytes, blob=None) -> IO`
: Open a committed blob file for reading.
  Returns a file object or `BlobFile`.

`temporaryDirectory() -> str`
: Return the directory path for uncommitted blob data.

### Blob statistics methods

`get_blob_stats() -> dict`
: Return blob storage statistics.
  The returned dict contains:

  | Key | Type | Description |
  |---|---|---|
  | `available` | `bool` | Whether the `blob_state` table exists. |
  | `total_blobs` | `int` | Total number of blob rows. |
  | `unique_objects` | `int` | Number of distinct OIDs with blobs. |
  | `avg_versions` | `float` | Average blob versions per object. |
  | `total_size` | `int` | Total blob size in bytes. |
  | `total_size_display` | `str` | Human-readable total size. |
  | `pg_count` | `int` | Number of PG-stored blobs. |
  | `pg_size` | `int` | Total size of PG-stored blobs in bytes. |
  | `pg_size_display` | `str` | Human-readable PG blob size. |
  | `s3_count` | `int` | Number of S3-stored blobs. |
  | `s3_size` | `int` | Total size of S3-stored blobs in bytes. |
  | `s3_size_display` | `str` | Human-readable S3 blob size. |
  | `largest_blob` | `int` | Size of the largest blob in bytes. |
  | `largest_blob_display` | `str` | Human-readable largest blob size. |
  | `avg_blob_size` | `int` | Average blob size in bytes. |
  | `avg_blob_size_display` | `str` | Human-readable average blob size. |
  | `s3_configured` | `bool` | Whether an S3 client is configured. |
  | `blob_threshold` | `int` | Current blob threshold in bytes. |
  | `blob_threshold_display` | `str` | Human-readable blob threshold. |

  Returns `{"available": False}` if the `blob_state` table does not exist.

`get_blob_histogram() -> list[dict]`
: Return blob size distribution as logarithmic buckets.
  Each dict in the list contains:

  | Key | Type | Description |
  |---|---|---|
  | `label` | `str` | Bucket range label (for example, `"100.0 KB -- 1.0 MB"`). |
  | `count` | `int` | Number of blobs in this bucket. |
  | `pct` | `int` | Percentage relative to the largest bucket (0--100). |
  | `tier` | `str` | `"pg"`, `"s3"`, `"mixed"`, or `""` (no S3 configured). |

  Returns `[]` if no blobs exist.

### History mode conversion methods

`convert_to_history_free() -> dict`
: Convert a history-preserving database to history-free mode.
  Drops `object_history`, `pack_state`, and the deprecated
  `blob_history` table.
  Removes old blob versions from `blob_state` (keeps only the latest
  tid per zoid) and orphaned `transaction_log` entries.
  Returns a dict with counts: `history_rows`, `pack_rows`,
  `blob_history_rows`, `old_blob_versions`, `orphan_transactions`.
  Raises `RuntimeError` if no history tables exist.

`convert_to_history_preserving() -> None`
: Convert a history-free database to history-preserving mode.
  Creates `object_history` and `pack_state` tables.
  Existing objects gain history tracking on their next modification.
  Raises `RuntimeError` if history tables already exist.

`compact_history() -> tuple[int, int]`
: Remove duplicate entries created by the old dual-write mode.
  Returns `(deleted_object_history_rows, deleted_blob_history_rows)`.
  Returns `(0, 0)` in history-free mode.

### State processor registration

`register_state_processor(processor) -> None`
: Register a state processor plugin.
  See {doc}`state-processor-api` for the processor protocol.

## PGJsonbStorageInstance

```python
from zodb_pgjsonb.storage import PGJsonbStorageInstance
```

Declared interfaces: `IBlobStorage`.

Inherits from: `ConflictResolvingStorage`.

### Properties

`pg_connection -> psycopg.Connection`
: The underlying psycopg connection (read-only).
  Shares the same `REPEATABLE READ` snapshot used for ZODB loads, so
  external queries see a consistent point-in-time view.

### IMVCCStorage methods

`new_instance() -> PGJsonbStorageInstance`
: Delegate to the main storage's `new_instance()`.

`release() -> None`
: Return the connection to the pool, end any active read transaction,
  and remove the blob temp directory.

`poll_invalidations() -> list[bytes]`
: Return OIDs changed since the last poll.
  Ends any previous read snapshot, starts a new `REPEATABLE READ`
  snapshot, queries for changes, and invalidates affected cache entries.
  Returns `[]` if nothing changed.

`sync(force: bool = True) -> None`
: Sync snapshot.
  No-op (autocommit mode provides latest committed data).

### IStorage read methods

`load(oid: bytes, version: str = "") -> tuple[bytes, bytes]`
: Load the current object state.
  Returns `(pickle_bytes, tid_bytes)`.
  Results are cached in the per-instance LRU cache.

`loadBefore(oid: bytes, tid: bytes) -> tuple[bytes, bytes, bytes] | None`
: Load object data before a given TID.

`loadSerial(oid: bytes, serial: bytes) -> bytes`
: Load a specific revision of an object.

### IStorage write methods

`new_oid() -> bytes`
: Delegate OID allocation to the main storage.

`store(oid: bytes, serial: bytes, data: bytes, version: str, transaction) -> None`
: Queue an object for storage.
  Conflict detection is deferred to `tpc_vote()`.

`checkCurrentSerialInTransaction(oid: bytes, serial: bytes, transaction) -> None`
: Queue a read-conflict check for batch verification in `tpc_vote()`.

### Two-phase commit

`tpc_begin(transaction, tid: bytes | None = None, status: str = " ") -> None`
: Begin a two-phase commit.
  Ends any active read snapshot, applies deferred DDL, starts a
  PostgreSQL transaction, acquires advisory lock 0, and generates a TID.

`tpc_vote(transaction) -> list | None`
: Flush pending stores and blobs to PostgreSQL.
  Performs batch conflict detection, writes all data, and calls
  `finalize()` on each state processor.

`tpc_finish(transaction, f: callable | None = None) -> bytes`
: Commit the PostgreSQL transaction.
  Updates the main storage's last TID and runs the optional callback.
  Returns TID bytes.

`tpc_abort(transaction) -> None`
: Rollback the PostgreSQL transaction and clean up temp blob files.

### IBlobStorage methods

`storeBlob(oid: bytes, oldserial: bytes, data: bytes, blobfilename: str, version: str, transaction) -> None`
: Store object data and a blob file.

`loadBlob(oid: bytes, serial: bytes) -> str`
: Return the path to a file containing the blob data.

`openCommittedBlobFile(oid: bytes, serial: bytes, blob=None) -> IO`
: Open a committed blob file for reading.

`temporaryDirectory() -> str`
: Return the directory path for uncommitted blob data.

### IStorageRestoreable methods

`restore(oid: bytes, serial: bytes, data: bytes, version: str, prev_txn, transaction) -> None`
: Write pre-committed data without conflict checking.

`restoreBlob(oid: bytes, serial: bytes, data: bytes, blobfilename: str, prev_txn, transaction) -> None`
: Restore object data and a blob file without conflict checking.

### Metadata (delegated to main storage)

`sortKey() -> str`
: Delegate to main storage.

`getName() -> str`
: Delegate to main storage.

`__name__ -> str`
: Property; delegates to main storage.

`isReadOnly() -> bool`
: Returns `False`.

`lastTransaction() -> bytes`
: Return the main storage's last TID.

`__len__() -> int`
: Delegate to main storage.

`getSize() -> int`
: Delegate to main storage.

`history(oid: bytes, size: int = 1) -> list[dict]`
: Delegate to main storage.

`pack(t: float, referencesf) -> None`
: Delegate to main storage.

`supportsUndo() -> bool`
: Delegate to main storage.

`undoLog(first: int = 0, last: int = -20, filter: callable | None = None) -> list[dict]`
: Delegate to main storage.

`undoInfo(first: int = 0, last: int = -20, specification: dict | None = None) -> list[dict]`
: Delegate to main storage.

`undo(transaction_id: bytes, transaction=None) -> tuple[bytes, list[bytes]]`
: Undo a transaction using this instance's connection.

`registerDB(db) -> None`
: No-op.

`close() -> None`
: Calls `release()`.

## LoadCache

```python
from zodb_pgjsonb.storage import LoadCache
```

Bounded LRU cache for `load()` results.
Stores `(pickle_bytes, tid_bytes)` keyed by zoid (int).
Each `PGJsonbStorageInstance` has its own cache; thread safety is not required.

### Constructor

```python
LoadCache(max_mb: int = 16)
```

### Methods

`get(zoid: int) -> tuple[bytes, bytes] | None`
: Look up by zoid.
  Returns `(data, tid)` or `None`.
  Promotes the entry on hit.

`set(zoid: int, data: bytes, tid: bytes) -> None`
: Store `(data, tid)` for a zoid.
  Evicts LRU entries if the cache exceeds the byte budget.

`invalidate(zoid: int) -> None`
: Remove a single zoid from the cache.

`clear() -> None`
: Remove all entries.

### Properties

`size_mb -> float`
: Current cache size in megabytes.

### Attributes

`hits: int`
: Number of cache hits.

`misses: int`
: Number of cache misses.

## Packer

```python
from zodb_pgjsonb.packer import pack
```

`pack(conn, pack_time: bytes | None = None, history_preserving: bool = False) -> tuple[int, int, list[str]]`
: Remove unreachable objects and their blobs.
  Uses a recursive CTE to find all objects reachable from the root (zoid 0).
  Returns `(deleted_objects, deleted_blobs, s3_keys_to_delete)`.
  The caller is responsible for deleting S3 objects in the returned key list.

  In history-preserving mode with `pack_time`, objects created or modified
  after `pack_time` are preserved even if currently unreachable.
  Old revisions in `object_history` and `blob_state` before `pack_time`
  are removed for reachable objects.
  Orphaned `transaction_log` entries at or before `pack_time` are deleted.

## ZMI integration

The `zodb_pgjsonb.zmi` module patches `App.ApplicationManager.AltDatabaseManager`
to add a "Blob Storage" tab in the Zope Management Interface when the database
uses `PGJsonbStorage`.

The patch is applied automatically during ZConfig factory initialization when
Zope (`App` package) is available.

The tab displays blob statistics from `get_blob_stats()` and
`get_blob_histogram()`.
Access requires the `Manager` role.
