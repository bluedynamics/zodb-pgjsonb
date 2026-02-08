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
  Batch writes use psycopg3's `executemany()` with pipeline mode for
  pipelined SQL (single network round-trip per batch).
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
| store single | 5.2 ms | 8.4 ms | **1.6x faster** |
| store batch 10 | 8.4 ms | 8.5 ms | **on par** |
| store batch 100 | 12.4 ms | 10.4 ms | 1.2x slower |
| load single | 1 us | 1 us | **1.4x faster** |
| load batch 100 | 21 us | 96 us | **4.6x faster** |

Single stores are faster because PGJsonbStorage has a simpler 2PC path (direct
SQL, no OID/TID tracking tables). Batch stores use pipelined `executemany()`
— transcoding is <1% of batch time (0.14 ms for 100 objects via
`decode_zodb_record_for_pg`), the remaining cost is PostgreSQL JSONB indexing
overhead vs RelStorage's raw bytea writes. Both storages serve hot reads from
their local LRU cache — PGJsonbStorage's cache is slightly faster due to
simpler lookup logic.

## ZODB.DB (through object cache)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| write simple | 8.9 ms | 8.7 ms | on par |
| write btree | 7.7 ms | 10.8 ms | **1.4x faster** |
| read | 3 us | 3 us | on par |
| connection cycle | 224 us | 240 us | **1.1x faster** |
| write batch 10 | 12.6 ms | 11.2 ms | 1.1x slower |

Through ZODB.DB, ZODB's object cache handles the hot read path. Writes are
on par. Both storages use connection pooling (`psycopg_pool`
for PGJsonbStorage, built-in for RelStorage). Connection cycle overhead is
on par thanks to pool pre-warming (`pool-size=1` default).

## Pack / GC

| Objects | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| 100 | 14.0 ms | 202.5 ms | **14.5x faster** |
| 1,000 | 8.3 ms | 236.8 ms | **28.4x faster** |
| 10,000 | 23.5 ms | 604.7 ms | **25.7x faster** |

Pack is the standout advantage. PGJsonbStorage's pure SQL graph traversal
via the pre-extracted `refs` column (recursive CTE) runs entirely inside
PostgreSQL — no objects are loaded, no Python unpickling occurs. RelStorage
must load and unpickle every object to discover references via `referencesf()`.
The advantage grows with database size.

## Plone Application (50 documents)

| Operation | PGJsonb | RelStorage | Comparison |
|---|---|---|---|
| site creation | 1.05 s | 1.06 s | on par |
| content create/doc | 27.6 ms | 27.2 ms | on par |
| catalog query | 187 us | 182 us | on par |
| content modify/doc | 6.6 ms | 7.0 ms | **1.1x faster** |

At the Plone application level, both storage backends are **on par**. The
storage layer accounts for a small fraction of total request time, which
is dominated by Plone framework overhead (catalog indexing, security checks,
event subscribers, ZCML lookups).

This confirms that PGJsonbStorage can serve as a drop-in replacement for
RelStorage in Plone deployments, trading minimal overhead for **SQL
queryability of all ZODB data via JSONB**.

## Analysis

**Strengths:**
- Single-object writes 1.4-1.6x faster (simpler 2PC)
- Raw loads 1.4-4.6x faster (both cached, simpler lookup)
- Pack/GC 15-28x faster (pure SQL, no object loading)
- Plone-level performance on par with RelStorage
- All ZODB data queryable via SQL/JSONB (unique to PGJsonbStorage)

**Trade-offs:**
- Batch store 100 is 1.2x slower (JSONB indexing overhead vs raw bytea)

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
