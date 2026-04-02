<!-- diataxis: explanation -->

# Performance characteristics

zodb-pgjsonb's performance profile reflects a fundamental trade-off: it pays a transcode cost on every uncached read in exchange for SQL queryability, faster writes, dramatically faster garbage collection, and a richer ecosystem of integrations.

This page presents benchmark data, explains where time is spent on each operation, and traces the optimization history that shaped the current implementation.

## Benchmark environment

All measurements use the following setup:

- Python 3.13, PostgreSQL 17, zodb-json-codec 1.5.0
- 100 iterations, 10 warmup iterations, Docker-containerized PostgreSQL on localhost
- 3 runs per configuration, median of medians reported
- Comparison baseline: RelStorage (PostgreSQL) with the same database server

The benchmark suite covers raw storage API operations (bypassing ZODB's object cache), ZODB.DB-level operations (through the object cache), pack/GC, history-preserving mode, and real Plone workloads.

## Write performance

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| store single | 4.7 ms | 6.6 ms | **1.4x faster** |
| store batch 10 | 5.3 ms | 7.6 ms | **1.4x faster** |
| store batch 100 | 6.8 ms | 10.0 ms | **1.5x faster** |

Single-object and batch writes are consistently faster than RelStorage.
The speedup comes from zodb-pgjsonb's simpler 2PC path -- direct SQL INSERT with advisory lock serialization, no OID/TID tracking tables, no separate commit lock table.
The Rust codec's transcode cost is negligible: processing 100 objects from pickle to JSON takes under 0.2 ms, a small fraction of the total operation time dominated by PostgreSQL I/O and 2PC overhead.

### Where write time goes

The dominant costs on the write path are:

1. **Advisory lock acquisition** -- serializes all write transactions through `pg_advisory_xact_lock(0)`.
2. **Batch conflict check** -- a single `SELECT ... WHERE zoid = ANY(...)` to detect write conflicts.
3. **Pipelined INSERT** -- `executemany()` sends all statements in one network round-trip.
4. **COMMIT** -- PostgreSQL WAL fsync.

The codec transcode and state processor execution together account for less than 5% of write time.

## Read performance

### Cached reads (LRU cache hit)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| load cached | 1 us | 2 us | **3.9x faster** |
| load batch cached (100) | 39 us | 128 us | **3.3x faster** |

Both storages serve hot objects from in-memory LRU caches without hitting PostgreSQL.
zodb-pgjsonb's simpler pure-Python `OrderedDict` cache outperforms RelStorage's Cython-compiled generational LRU for single-key lookups.
This is somewhat surprising -- the explanation is that the `OrderedDict` code path has fewer branches and no generational promotion logic.

### Uncached reads (database round-trip)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| load uncached | 131 us | 84 us | 1.6x slower |

Uncached loads are the expected trade-off for JSONB storage.
After the SQL SELECT, zodb-pgjsonb must transcode JSONB back to pickle via the Rust codec.
RelStorage returns raw bytea bytes with no post-processing.

In production, this trade-off matters less than benchmarks suggest.
The ZODB object cache handles >95% of reads.
Objects evicted from the ZODB object cache but still warm at the storage level hit the storage LRU cache, which returns cached pickle bytes without any database round-trip.
The uncached path fires primarily during cold starts and after large invalidations.

### ZODB.DB-level reads

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| cached read | 2 us | 3 us | **1.2x faster** |

Through ZODB.DB, the object cache dominates.
Read performance is slightly faster due to the simpler storage cache.

## Pack and garbage collection

| Objects | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| 100 | 9.7 ms | 130.1 ms | **13.4x faster** |
| 1,000 | 6.1 ms | 167.6 ms | **27.5x faster** |
| 10,000 | 21.5 ms | 481.5 ms | **22.4x faster** |

Pack is the standout advantage.
zodb-pgjsonb's pure SQL graph traversal via the pre-extracted `refs` column runs entirely inside PostgreSQL -- no objects are loaded, no Python unpickling occurs.
RelStorage must load and unpickle every object to discover references via `referencesf()`.

The performance gap widens with database size because RelStorage's cost scales linearly with the number of objects (each must be loaded and unpickled), while zodb-pgjsonb's recursive CTE operates on integer arrays and benefits from PostgreSQL's index-driven join strategies.

At 1,000 objects, pack is actually faster than at 100 objects (6.1 ms vs 9.7 ms) because the temp table creation and index build -- a fixed overhead -- is amortized over more useful work.

## History-preserving mode

### HP writes

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| store single | 4.8 ms | 6.4 ms | **1.3x faster** |
| store batch 100 | 10.8 ms | 12.5 ms | **1.2x faster** |

History-preserving writes use the copy-before-overwrite model: existing rows are copied to `object_history` before `object_state` is upserted.
This is more efficient than RelStorage's full dual-write path.

### HP reads

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| loadBefore | 262 us | 230 us | 1.1x slower |
| history() | 159 us | 319 us | **2.0x faster** |

`loadBefore` carries the same transcode overhead as regular uncached loads.
`history()` is 2x faster because zodb-pgjsonb uses a UNION query across `object_state` and `object_history` with a direct JOIN on `transaction_log`, while RelStorage requires separate table scans.

### HP undo and pack

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| undo | 7.8 ms | 14.0 ms | **1.8x faster** |
| pack 100 (4 revisions each) | 9.8 ms | 215.3 ms | **22.0x faster** |
| pack 1,000 (4 revisions each) | 14.1 ms | 308.1 ms | **21.8x faster** |

Undo is 1.8x faster thanks to zodb-pgjsonb's simpler state swap path (direct SQL).
HP pack retains the same massive advantage as HF pack -- pure SQL graph traversal versus object loading.

### HP optimization impact

The history-preserving optimization (v1.3.0) changed `object_history` from a full duplicate of every write to a copy-before-overwrite model.
The before/after comparison shows the impact:

| Operation | Before | After | Change |
|---|---|---|---|
| store batch 100 | 18.3 ms | 12.3 ms | **-33%** |
| loadBefore | 316 us | 267 us | **-15%** |
| undo | 8.9 ms | 7.7 ms | **-14%** |
| load uncached | 149 us | 123 us | **-17%** |

The biggest win is batch writes (-33%), where the old dual-write approach wrote the same JSONB data twice.
Storage overhead dropped by roughly 50% because `object_history` now contains only previous versions, not a copy of every current version.

## Plone application workloads

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| site creation | 1.07 s | 1.06 s | on par |
| content create/doc | 27.5 ms | 27.2 ms | on par |
| catalog query | 190 us | 180 us | on par |
| content modify/doc | 6.6 ms | 6.7 ms | on par |

Real Plone workloads show both backends performing identically.
At the application level, ZODB's object cache handles the hot path, and per-object transcoding cost is negligible relative to Plone's own processing (security checks, event handling, catalog indexing, template rendering).

This is the expected result: zodb-pgjsonb's performance differences are visible at the storage API level, but Plone's application overhead dominates at the user-facing level.
The value proposition of zodb-pgjsonb is not raw speed for Plone operations -- it is SQL queryability, faster pack/GC, and the ecosystem of PostgreSQL-native integrations.

## Optimization history

zodb-pgjsonb's performance evolved through several significant optimizations.
Understanding this history explains why the current implementation looks the way it does.

### LRU load cache (1.0.0a1)

The initial release included a per-instance `OrderedDict` LRU cache bounded by byte size.
This eliminated database round-trips for hot objects and made cached reads 3-4x faster than RelStorage.

### Batch conflict detection (1.0.0)

Conflict checks were moved from per-object queries in `store()` to a single batch query in `tpc_vote()`.
This eliminated N-1 SQL round-trips per transaction while holding the advisory lock, reducing write latency proportionally to the number of objects in the transaction.

### Prepared statements (1.0.0)

Adding `prepare=True` to hot-path queries (`load`, `loadSerial`, `loadBefore`) eliminated PostgreSQL parse overhead for repeated query shapes.
The per-query savings are small but accumulate over a connection's lifetime.

### GIN index removal (1.0.0)

The `jsonb_path_ops` GIN index on the `state` column was removed.
It indexed every key-path and value in the JSONB, causing significant write amplification on every INSERT.
With plone-pgcatalog providing dedicated query columns for catalog data, direct state JSONB queries are no longer the primary access pattern.

### Direct JSON string decode path (1.3.0)

The store path was changed to use `decode_zodb_record_for_pg_json()` from zodb-json-codec 1.4.0, which returns a JSON string directly instead of an intermediate Python dict.
The entire pickle-to-JSON pipeline now runs in Rust with the GIL released.
This provided a 1.3x end-to-end improvement on real-world data.

### History-preserving copy-before-overwrite (1.3.0)

Changing `object_history` from full dual-write to copy-before-overwrite reduced HP batch writes by 33%, HP loadBefore by 15%, and HP storage overhead by roughly 50%.

### Packer NOT EXISTS anti-join (1.8.1)

The packer's unreachable object deletion was changed from `NOT IN (SELECT zoid FROM reachable_oids)` to `NOT EXISTS (SELECT 1 FROM reachable_oids r WHERE r.zoid = ...)`.
`NOT IN` builds a hash of the entire reachable set and checks every row.
`NOT EXISTS` uses an indexed anti-join that short-circuits on the first match.
On a 4.4M-object database, pack went from 48+ minutes (incomplete) to approximately 2 minutes.
The same pattern was applied to all deletion phases (objects, blobs, transaction log cleanup).

### Batch object loading (1.8.0)

`load_multiple(oids)` on `PGJsonbStorageInstance` loads multiple objects in a single `SELECT ... WHERE zoid = ANY(...)` query instead of individual roundtrips.
The method checks the per-instance LRU cache first and only queries misses.
This is the building block for the refs prefetch feature (1.9.0+).

### Pluggable refs prefetch (1.9.0--1.9.2)

When an object is loaded, its `refs` column (which lists OIDs of referenced objects such as annotations and sub-mappings) can be used to prefetch all directly referenced objects via `load_multiple()`.
This turns the N+1 individual loads typical of ZODB object traversal into 1+1 batch loads.

The initial implementation (v1.9.0) prefetched unconditionally, which caused severe over-fetching for internal ZODB structures like BTrees and PersistentMappings whose refs cascade into thousands of objects.
Cold-start performance was 40--84% slower than without prefetch.
v1.9.1 added a class-based blacklist, but maintaining a list of "non-content" classes proved fragile.
v1.9.2 replaced the blacklist with a pluggable SQL expression via `register_prefetch_refs_expr()`.
The expression is included in the `load()` query as a conditional `refs` column.
When `None` (the default), no prefetch occurs.
plone-pgcatalog registers `CASE WHEN idx IS NOT NULL THEN refs END`, which limits prefetch to rows that have catalog index data -- a reliable proxy for "is a content object".

### Composite TID/ZOID index (1.5.3)

A composite index on `(tid, zoid)` was added to `object_state` to speed up `poll_invalidations()` queries.
Previously, polling required a sequential scan on large tables.
The index is created automatically on startup for existing databases.

## Scaling characteristics

### Connection pool

The psycopg3 `ConnectionPool` with configurable min/max size allows tuning for different deployment sizes.
Each ZODB Connection gets its own storage instance with its own pool connection, so the pool max_size should match the expected number of concurrent ZODB Connections.

### Prepared statements

Prepared statements amortize PostgreSQL's parse overhead across the lifetime of a connection.
Since pool connections are reused across ZODB Connection cycles, the prepared statement cache persists and benefits subsequent connections.

### Write serialization

The advisory lock serialization model (`pg_advisory_xact_lock(0)`) limits write throughput to one transaction at a time.
This is rarely a bottleneck because ZODB write transactions are typically short (a few milliseconds) and the lock is held only during `tpc_vote` and `tpc_finish`.
For workloads with sustained high write concurrency, OID-range-based advisory locks could partition the lock space, but this has not been necessary in practice.

### Pack scaling

Pack time scales with the size of the reachable object graph, not the total database size.
The recursive CTE operates on integer arrays (the `refs` column), which PostgreSQL can process efficiently in memory.
For very large databases with deep reference chains, adequate `work_mem` and a `statement_timeout` should be configured to bound resource usage.
