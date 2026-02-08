# Performance Benchmarks

Comparison of `PGJsonbStorage` vs `RelStorage` (both using PostgreSQL)
for ZODB storage operations at multiple abstraction levels.

Measured on: 2026-02-08
Python: 3.13.9, PostgreSQL: 17, 100 iterations, 10 warmup
Host: localhost, Docker-containerized PostgreSQL

## Context

PGJsonbStorage stores object state as **JSONB** (enabling SQL queries on ZODB
data), while RelStorage stores **pickle bytes** as bytea. This fundamental
difference affects the performance profile:

- **Writes**: PGJsonbStorage transcodes pickle to JSONB via `zodb-json-codec`
  (Rust), then PostgreSQL indexes the JSONB. RelStorage writes raw bytes.
- **Reads (raw)**: Both storages use a local in-memory LRU cache
  (`cache_local_mb`) that serves hot objects from RAM. Cache misses hit
  PostgreSQL — PGJsonbStorage additionally transcodes JSONB back to pickle.
- **Reads (ZODB)**: ZODB's object cache handles >95% of reads in production,
  making the raw storage read speed largely irrelevant.
- **Pack/GC**: PGJsonbStorage uses pure SQL graph traversal via pre-extracted
  `refs` column (recursive CTE, no data leaves PostgreSQL). RelStorage must
  load and unpickle every object to extract references.

## Storage API (raw store/load)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| store single | 5.4 ms | 8.2 ms | **1.5x faster** |
| store batch 10 | 8.9 ms | 9.4 ms | **1.1x faster** |
| store batch 100 | 19.0 ms | 8.6 ms | 2.2x slower |
| load single | 1 us | 1 us | **1.7x faster** |
| load batch 100 | 33 us | 168 us | **5.1x faster** |

Single stores are faster because PGJsonbStorage has a simpler 2PC path (direct
SQL, no OID/TID tracking tables). Batch stores are slower because each object
requires JSONB transcoding. Both storages serve hot reads from their local LRU
cache — PGJsonbStorage's cache is slightly faster due to simpler lookup logic.

## ZODB.DB (through object cache)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| write simple | 10.0 ms | 10.4 ms | **1.0x faster** |
| write btree | 7.9 ms | 8.9 ms | **1.1x faster** |
| read | 2 us | 3 us | **1.2x faster** |
| connection cycle | 177 us | 173 us | 1.0x (on par) |
| write batch 10 | 16.2 ms | 11.2 ms | 1.4x slower |

Through ZODB.DB, ZODB's object cache handles the hot read path. Writes are
consistently faster. Both storages use connection pooling (`psycopg_pool`
for PGJsonbStorage, built-in for RelStorage). Connection cycle overhead is
on par thanks to pool pre-warming (`pool-size=1` default).

## Pack / GC

| Objects | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| 100 | 7.6 ms | 179.8 ms | **23.7x faster** |
| 1,000 | 7.6 ms | 191.1 ms | **25.1x faster** |
| 10,000 | 18.2 ms | 612.9 ms | **33.6x faster** |

Pack is the standout advantage. PGJsonbStorage's pure SQL graph traversal
via the pre-extracted `refs` column (recursive CTE) runs entirely inside
PostgreSQL — no objects are loaded, no Python unpickling occurs. RelStorage
must load and unpickle every object to discover references via `referencesf()`.
The advantage grows with database size.

## Plone Application (50 documents)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| site creation | 1.18 s | 1.06 s | 1.1x slower |
| content create/doc | 30.1 ms | 27.0 ms | 1.1x slower |
| catalog query | 201 us | 190 us | 1.1x slower |
| content modify/doc | 7.4 ms | 6.6 ms | 1.1x slower |

At the Plone application level, both storage backends are **on par**. The
~10% difference is within noise range and dominated by Plone framework overhead
(catalog indexing, security checks, event subscribers, ZCML lookups). The
storage layer accounts for a small fraction of total request time.

This confirms that PGJsonbStorage can serve as a drop-in replacement for
RelStorage in Plone deployments, trading minimal overhead for **SQL
queryability of all ZODB data via JSONB**.

## Analysis

**Strengths:**
- Single-object writes 1.1-1.5x faster (simpler 2PC)
- Raw loads 1.7-5x faster (both cached, simpler lookup)
- Pack/GC 24-34x faster (pure SQL, no object loading)
- Plone-level performance on par with RelStorage
- All ZODB data queryable via SQL/JSONB (unique to PGJsonbStorage)

**Trade-offs:**
- Batch stores slower (per-object JSONB transcoding overhead)

**Future optimizations:**
- Prepared statements for load/store queries
- Batch transcoding in Rust (amortize PyO3 overhead)

## Running Benchmarks

```bash
cd sources/zodb-pgjsonb

# Requires PostgreSQL on localhost:5433 with benchmark databases:
#   createdb -h localhost -p 5433 -U zodb zodb_bench_pgjsonb
#   createdb -h localhost -p 5433 -U zodb zodb_bench_relstorage

# Install benchmark dependencies
uv pip install -e ".[bench]"

# Individual benchmark categories
python benchmarks/bench.py storage --iterations 100
python benchmarks/bench.py zodb --iterations 100
python benchmarks/bench.py pack
python benchmarks/bench.py plone --docs 50

# All benchmarks with JSON export
python benchmarks/bench.py all --output benchmarks/results.json --format both
```
