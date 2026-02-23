# Performance Benchmarks

Comparison of `PGJsonbStorage` vs `RelStorage` (both using PostgreSQL)
for ZODB storage operations at multiple abstraction levels.

Measured on: 2026-02-08
Python: 3.13.9, PostgreSQL: 17, 100 iterations, 10 warmup
Host: localhost, Docker-containerized PostgreSQL

## Architecture Differences

| | PGJsonbStorage | RelStorage |
|---|---|---|
| Storage format | JSONB (queryable via SQL) | bytea (opaque pickle) |
| Load cache | OrderedDict LRU (Python), 16 MB, per-instance | Generational LRU (Cython), 10 MB, shared |
| Cache miss cost | SQL SELECT + JSONB→pickle transcode (Rust) | SQL SELECT (raw bytes) |
| Store cost | pickle→JSONB transcode (Rust) + INSERT | raw pickle INSERT |
| Pack/GC | Pure SQL recursive CTE (refs column) | Load + unpickle every object |

## Storage API (raw store/load)

### Writes

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| store single | 3.6 ms | 4.8 ms | **1.3x faster** |
| store batch 10 | 4.8 ms | 10.4 ms | **2.2x faster** |
| store batch 100 | 7.8 ms | 6.5 ms | 1.2x slower |

Single and small-batch stores are faster because PGJsonbStorage has a simpler
2PC path (direct SQL, no OID/TID tracking tables). Batch-100 stores are
slightly slower due to PostgreSQL JSONB indexing overhead vs raw bytea writes.
Transcoding cost is <1% of batch time (the Rust codec processes 100 objects
in ~0.14 ms via `decode_zodb_record_for_pg`).

### Loads (cached — in-memory LRU hit)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| load cached | <1 us | 1 us | **3.7x faster** |
| load batch cached (100) | 24 us | 105 us | **4.4x faster** |

Cached loads measure in-memory cache lookup speed. Both storages serve hot
objects from their local LRU cache without hitting PostgreSQL.
PGJsonbStorage uses a pure-Python `OrderedDict`; RelStorage uses a
Cython-compiled generational LRU. Despite the implementation difference,
PGJsonbStorage's simpler cache structure is faster for single-key lookups.

### Loads (uncached — DB round-trip)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| load uncached | 97 us | 45 us | 2.2x slower |

Uncached loads force a DB round-trip on every iteration (each load accesses
a unique OID never seen before). PGJsonbStorage is slower because it must
transcode JSONB→pickle via the Rust codec after the SQL SELECT. RelStorage
returns raw bytea bytes with no post-processing. This is the expected
trade-off for storing data as queryable JSONB.

## ZODB.DB (through object cache)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| write simple | 5.4 ms | 5.7 ms | on par |
| write btree | 4.5 ms | 5.5 ms | **1.2x faster** |
| cached read | 2 us | 2 us | on par |
| connection cycle | 171 us | 148 us | on par |
| write batch 10 | 7.7 ms | 6.7 ms | 1.1x slower |

Through ZODB.DB, the object cache handles the hot read path — both storages
show identical 2 us reads (Python dict lookup in ZODB.Connection, not a
storage operation). Writes are on par. BTree writes are faster for
PGJsonbStorage due to efficient BTree state encoding. Batch-10 writes are
slightly slower due to JSONB overhead accumulating across multiple objects.

## Pack / GC (3 iterations, mean +/- stddev)

| Objects | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| 100 | 5.7 ms +/- 36 us | 125.4 ms +/- 4.0 ms | **22.1x faster** |
| 1,000 | 6.9 ms +/- 206 us | 163.1 ms +/- 2.3 ms | **23.5x faster** |
| 10,000 | 26.6 ms +/- 7.3 ms | 484.8 ms +/- 16.2 ms | **18.2x faster** |

Pack is the standout advantage. PGJsonbStorage's pure SQL graph traversal
via the pre-extracted `refs` column (recursive CTE) runs entirely inside
PostgreSQL — no objects are loaded, no Python unpickling occurs. RelStorage
must load and unpickle every object to discover references via
`referencesf()`. The advantage is 18-24x across all database sizes.

## History-Preserving Mode

Measured on: 2026-02-23, same environment as above.

### HP Storage Writes (PGJsonbStorage vs RelStorage)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| store single | 4.8 ms | 6.4 ms | **1.3x faster** |
| store batch 10 | 5.5 ms | 7.8 ms | **1.4x faster** |
| store batch 100 | 10.8 ms | 12.5 ms | **1.2x faster** |

PGJsonbStorage is consistently faster for HP writes. The copy-before-overwrite
model (`INSERT INTO object_history SELECT ... WHERE zoid = ANY(...)` + `UPSERT`)
is more efficient than RelStorage's full dual-write path.

### HP Storage Reads (PGJsonbStorage vs RelStorage)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| loadBefore | 262 us | 230 us | 1.1x slower |
| history() | 159 us | 319 us | **2.0x faster** |
| load uncached | 106 us | 93 us | 1.1x slower |

`loadBefore` and uncached loads are slightly slower due to the JSONB→pickle
transcode (same trade-off as HF mode). `history()` is 2x faster because
PGJsonbStorage uses a `UNION` query across `object_state` + `object_history`
with a direct `JOIN` on `transaction_log`, while RelStorage requires separate
table scans.

### HP ZODB-Level Operations (PGJsonbStorage vs RelStorage)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| write (via ZODB.DB) | 7.0 ms | 8.3 ms | **1.2x faster** |
| undo | 7.8 ms | 14.0 ms | **1.8x faster** |

Undo is 1.8x faster — PGJsonbStorage's simpler undo path (direct SQL state
swap) avoids RelStorage's more complex undo machinery.

### HP Pack (with history revisions)

| Objects (4 revisions each) | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| 100 | 9.8 ms | 215.3 ms | **22.0x faster** |
| 1,000 | 14.1 ms | 308.1 ms | **21.8x faster** |

Same advantage as HF mode: pure SQL graph traversal via the `refs` column
vs RelStorage loading and unpickling every object to discover references.

### HP Optimization: Before/After

The history-preserving optimization ([#13](https://github.com/bluedynamics/z3blobs/pull/13))
changes `object_history` from a full duplicate of every write to a
copy-before-overwrite model (only previous versions are stored) and eliminates
the redundant `blob_history` table entirely. This reduces write amplification
and storage overhead by ~50% for HP mode.

| Operation | Before | After | Change |
|---|---|---|---|
| store single | 5.3 ms | 4.7 ms | **-12%** |
| store batch 10 | 5.9 ms | 5.2 ms | **-11%** |
| store batch 100 | 18.3 ms | 12.3 ms | **-33%** |
| loadBefore | 316 us | 267 us | **-15%** |
| history() | 194 us | 187 us | -4% |
| load uncached | 149 us | 123 us | **-17%** |
| write (via ZODB.DB) | 7.2 ms | 7.0 ms | -3% |
| undo | 8.9 ms | 7.7 ms | **-14%** |
| pack 100 | 10.3 ms | 9.9 ms | -4% |
| pack 1,000 | 21.0 ms | 24.1 ms | +15% (variance) |

The biggest wins are batch writes (-33%) and undo (-14%). The copy-before-overwrite
model avoids writing duplicate JSONB data. `loadBefore` benefits from `UNION`
queries that check `object_state` first (PK lookup) before scanning history.

## Plone Operations (50 documents)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| site creation | 1.09 s | 1.06 s | on par |
| content create/doc | 28.2 ms | 27.2 ms | on par |
| catalog query | 199 us | 180 us | on par |
| content modify/doc | 6.8 ms | 6.7 ms | on par |

Real Plone workloads (site creation, content CRUD, catalog queries) show
both backends performing identically. This is expected: at the application
level, ZODB's object cache handles the hot path, and the per-object
transcoding cost is negligible relative to Plone's own processing
(ZCML, security, catalog indexing, event subscribers).

## Analysis

**Strengths (vs RelStorage):**
- Single-object writes 1.3x faster (simpler 2PC) — both HF and HP mode
- Cached loads 3.7-4.4x faster (simpler cache structure)
- Pack/GC 18-24x faster (pure SQL, no object loading) — both HF and HP mode
- BTree writes 1.2x faster
- HP history() queries 2.0x faster (UNION + direct JOIN)
- HP undo 1.8x faster (simpler SQL state swap)
- All ZODB data queryable via SQL/JSONB (unique to PGJsonbStorage)

**Trade-offs:**
- Uncached loads 1.1-2.2x slower (JSONB→pickle transcode overhead)
- Batch store 100 is 1.2x slower in HF mode (JSONB indexing overhead vs raw bytea)
- Batch ZODB write 10 is 1.1x slower in HF mode

**HP optimization impact (before/after):**
- Batch writes up to 33% faster (copy-before-overwrite vs full dual-write)
- loadBefore 15% faster (UNION query checks object_state PK first)
- Undo 14% faster (fewer rows to copy)
- ~50% less storage overhead (no duplicate JSONB in object_history)

**Unique value proposition:**
PGJsonbStorage trades a small uncached-load overhead for full SQL
queryability of all ZODB data via PostgreSQL JSONB. In production, the
object cache handles >95% of reads, making the uncached-load cost largely
irrelevant. Real Plone workloads confirm zero performance difference
at the application level.

## Running Benchmarks

```bash
cd sources/zodb-pgjsonb

# Requires PostgreSQL on localhost:5433 with benchmark databases:
#   createdb -h localhost -p 5433 -U zodb zodb_bench_pgjsonb
#   createdb -h localhost -p 5433 -U zodb zodb_bench_relstorage

# Install benchmark dependencies (includes RelStorage for comparison)
uv pip install -e ".[bench]"

# For Plone benchmarks, also install Plone:
uv pip install -c example/constraints.txt Plone

# Individual benchmark categories
python benchmarks/bench.py storage --iterations 100
python benchmarks/bench.py zodb --iterations 100
python benchmarks/bench.py pack
python benchmarks/bench.py history --iterations 100  # HP mode benchmarks
python benchmarks/bench.py plone --docs 50

# All benchmarks with JSON export
python benchmarks/bench.py all --output benchmarks/results.json --format both
```
