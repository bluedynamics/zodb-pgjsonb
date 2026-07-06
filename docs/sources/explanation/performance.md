<!-- diataxis: explanation -->

# Performance characteristics

zodb-pgjsonb's performance profile reflects a fundamental trade-off: it pays a transcode cost on every uncached read in exchange for SQL queryability, faster writes, dramatically faster garbage collection, and a richer ecosystem of integrations.

This page presents benchmark data, explains where time is spent on each operation, and traces the optimization history that shaped the current implementation.

## Benchmark environment

The numbers below were refreshed in 2026-07 on a developer workstation:

- Python 3.14, PostgreSQL 17.9 (Docker on `localhost`), ZODB 6.2, psycopg 3.3
- zodb-json-codec 1.6.1
- Comparison baseline: RelStorage 4.2.0 on the same PostgreSQL server
- Each figure is the median of 100 measured iterations after 10 warmup iterations; pack figures are the mean over 3 runs

```{important}
Absolute numbers are machine-specific -- a workstation and a production server differ by a large factor.
The portable signal is the **PGJsonb-versus-RelStorage ratio** on the same machine.
Reproduce or refresh these numbers on your own hardware with the suite in [`benchmarks/`](https://github.com/bluedynamics/zodb-pgjsonb/tree/main/benchmarks); its `README.md` documents the setup and how to update this page.
```

The benchmark suite covers raw storage API operations (bypassing ZODB's object cache), ZODB.DB-level operations (through the object cache), pack/GC, history-preserving mode, and real Plone workloads.

## Write performance

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| store single | 4.5 ms | 5.4 ms | **1.2x faster** |
| store batch 10 | 4.5 ms | 5.9 ms | **1.3x faster** |
| store batch 100 | 9.2 ms | 9.3 ms | on par |

Single-object and small-batch writes are faster than RelStorage; large batches are on par.
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

### Cached reads (storage cache hit)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| load cached | 1 us | 1 us | **2.5x faster** |
| load batch cached (100) | 31 us | 148 us | **4.8x faster** |

Both storages serve hot objects from in-memory caches without hitting PostgreSQL.
zodb-pgjsonb's `OrderedDict`-based caches keep single-key and batch lookups fast; the batch path is well ahead of RelStorage's generational LRU.
The storage cache is two-tier -- a small per-connection L1 in front of a process-wide L2 shared across all connections; see {ref}`cache-tiers`.

### Uncached reads (database round-trip)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| load uncached | 101 us | 76 us | 1.3x slower |

Uncached loads are the expected trade-off for JSONB storage.
After the SQL SELECT, zodb-pgjsonb must transcode JSONB back to pickle via the Rust codec.
RelStorage returns raw bytea bytes with no post-processing.

In production, this trade-off matters less than benchmarks suggest.
The ZODB object cache handles the large majority of reads.
An object evicted from the ZODB object cache but still warm at the storage level hits the L1 or L2 cache and returns pickle bytes without any database round-trip.
The uncached path fires primarily during cold starts and after large invalidations.

### ZODB.DB-level reads

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| cached read | 3 us | 2 us | 1.1x slower |
| write simple | 5.9 ms | 6.5 ms | **1.1x faster** |
| write btree | 5.3 ms | 6.6 ms | **1.2x faster** |
| connection cycle | 235 us | 162 us | 1.4x slower |

Through ZODB.DB the object cache dominates cached reads, so both storages land within microseconds of each other.
The connection cycle (open a `ZODB.Connection`, poll, close) is slower because each cycle acquires a pooled connection -- validated with a liveness check on checkout since 1.14.2 -- and opens a fresh REPEATABLE READ snapshot.

## Pack and garbage collection

| Objects | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| 100 | 16.0 ms | 138.4 ms | **8.7x faster** |
| 1,000 | 18.4 ms | 192.7 ms | **10.5x faster** |
| 10,000 | 45.2 ms | 650.5 ms | **14.4x faster** |

Pack is the standout advantage.
zodb-pgjsonb's pure SQL graph traversal via the pre-extracted `refs` column runs entirely inside PostgreSQL -- no objects are loaded, no Python unpickling occurs.
RelStorage must load and unpickle every object to discover references via `referencesf()`.

The performance gap widens with database size because RelStorage's cost scales linearly with the number of objects (each must be loaded and unpickled), while zodb-pgjsonb's recursive CTE operates on integer arrays and benefits from PostgreSQL's index-driven join strategies.
zodb-pgjsonb's own pack time grows only slowly with object count -- a fixed temp-table and index-build overhead dominates at small sizes, so 100 and 1,000 objects pack in a similar time.

## History-preserving mode

### HP writes

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| store single | 4.2 ms | 5.8 ms | **1.4x faster** |
| store batch 10 | 5.0 ms | 7.4 ms | **1.5x faster** |
| store batch 100 | 9.3 ms | 10.7 ms | **1.1x faster** |

History-preserving writes use the copy-before-overwrite model: existing rows are copied to `object_history` before `object_state` is upserted.
This is more efficient than RelStorage's full dual-write path.

### HP reads

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| loadBefore | 196 us | 215 us | **1.1x faster** |
| history() | 177 us | 289 us | **1.6x faster** |

`loadBefore` carries the same transcode overhead as regular uncached loads and is now roughly on par with RelStorage.
`history()` is faster because zodb-pgjsonb uses a UNION query across `object_state` and `object_history` with a direct JOIN on `transaction_log`, while RelStorage requires separate table scans.

### HP undo and pack

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| undo | 6.4 ms | 11.0 ms | **1.7x faster** |
| pack 100 (4 revisions each) | 25.7 ms | 186.7 ms | **7.3x faster** |
| pack 1,000 (4 revisions each) | 34.6 ms | 273.7 ms | **7.9x faster** |

Undo is 1.7x faster thanks to zodb-pgjsonb's simpler state swap path (direct SQL).
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

```{note}
The Plone-workload figures below are from an earlier run and were not refreshed in the 2026-07 pass, because they require a full Plone environment.
To refresh them, run `bench.py plone` in an environment with `Products.CMFPlone` installed -- see [`benchmarks/README.md`](https://github.com/bluedynamics/zodb-pgjsonb/tree/main/benchmarks).
The conclusion is unchanged: at the application level both backends are on par.
```

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

When an object is loaded, its `refs` column (which lists OIDs of referenced objects such as annotations and sub-mappings) can be used to prefetch all *directly referenced* objects via `load_multiple()`.
This turns the N+1 loads of an object's own sub-graph into 1+1 batch loads.
(The distinct N+1 of a *result set* -- a collection of sibling objects that are not each other's refs -- is addressed by the ZODB `prefetch` hook in 1.15.0, below.)

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

### Process-wide shared cache (1.12.0)

The per-instance load cache was split into two tiers: a small per-connection L1 in front of a single process-wide L2 (`SharedLoadCache`) shared by every connection.
Before this, N connections each kept their own copy of the same hot objects -- roughly a gigabyte of duplication on a busy pod.
The shared tier is gated by a consensus TID for MVCC correctness; a lagging reader is served an entry only when it is not newer than the reader's snapshot ({ref}`cache-tiers`).

### Cache warmer (1.13.0)

A learning cache warmer records the objects loaded right after startup and pre-loads the highest-scored ones into L2 on the next start, so a fresh pod is not cold.
On rolling deploys the warmer is deliberately un-synchronized (baseline delay, jitter, a cluster-wide concurrency cap) so N replicas do not warm at once and multiply the database's startup load.

### Connection-pool hardening (1.14.1--1.14.2)

Two production pool-slot leaks were fixed: an unguarded `COMMIT` on a server-closed connection, and a strand of the connection during `Connection.open()` when `poll_invalidations` raised.
The pool now validates a connection's liveness on checkout, and the read path replaces a connection found broken.
This adds one lightweight round-trip per connection checkout (visible in the connection-cycle figure above) in exchange for not leaking pool slots under connection recycling.

### Per-entry L2 read gate (1.15.0)

The shared cache previously denied a connection *all* of L2 whenever its snapshot was behind the consensus, so a lagging reader fell through to PostgreSQL for every object during a write burst.
The gate is now per entry: a lagging reader keeps hitting L2 for every object unchanged since its snapshot and misses only genuinely newer ones ({ref}`cache-tiers`).

### ZODB prefetch hook (1.15.0)

`PGJsonbStorageInstance.prefetch(oids)` implements ZODB's `Connection.prefetch` hook, delegating to `load_multiple`.
Without it, `Connection.prefetch()` was a no-op on zodb-pgjsonb, so a result set (a collection listing, a tile) was loaded one object at a time -- N sequential round-trips.
A caller that prefetches its result set turns those N round-trips into one; the micro-benchmark `benchmarks/bench_prefetch.py` shows 151 objects collapsing from 151 round-trips to 1 (about 130x faster at a 20 ms per-query latency).

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
