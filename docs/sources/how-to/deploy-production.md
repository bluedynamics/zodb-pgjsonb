<!-- diataxis: how-to -->

# How to deploy to production

This guide shows you how to configure zodb-pgjsonb for production workloads.

(deploy-production-tune-connection-pool-settings)=

## Tune connection pool settings

Set `pool-size` and `pool-max-size` based on your expected concurrency:

```ini
<pgjsonb>
    dsn dbname=zodb user=zodb host=localhost port=5432
    pool-size 4
    pool-max-size 20
    pool-timeout 30.0
</pgjsonb>
```

- **pool-size**: minimum connections kept open (default: 1).
  Set this to the number of Zope threads for steady-state workloads.
- **pool-max-size**: maximum connections (default: 10).
  Every *open or idle* ZODB connection holds one pool connection, and ZODB keeps up to its own `pool-size` (Zope default: 7) idle connections around.
  Size it to at least ZODB `pool-size` + Zope worker threads + cache-warmer concurrency + 2.
  Multiply by the number of processes when you size PostgreSQL's `max_connections`.
- **pool-timeout**: seconds to wait for a connection before raising `StorageError` (default: 30.0).

Each ZODB connection gets its own `PGJsonbStorageInstance` with a dedicated PostgreSQL connection from the pool.

(size-threads-gil)=

## Size worker threads for GIL headroom

A single process runs its worker threads under one CPython GIL.
When several threads render responses concurrently while others wait on PostgreSQL, a thread returning from a query queues behind the CPU-bound ones for the GIL.
This inflates per-load wall time even when the database is fast.

Prefer more processes with fewer threads each over fewer processes with many threads:

- Keep the thread count per process modest, at a small number such as 2 to 4.
  Scale out with more processes or replicas to serve the same concurrency.
- Match `pool-size` to the thread count per process, as described above.
  Each thread then still has its own pooled connection.
- Cache CPU-heavy per-request work such as theme compilation out of the request path.
  Waiting threads then re-acquire the GIL sooner.

```{seealso}
{ref}`gil-convoy` explains why per-load timings inflate under concurrency even when PostgreSQL is fast.
```

(deploy-production-tune-the-object-cache)=

## Tune the object cache

Set `cache-shared-mb` to size the process-wide load cache (L2) and `cache-per-connection-mb` to size the per-connection cache (L1):

```ini
<pgjsonb>
    dsn dbname=zodb user=zodb host=localhost
    cache-shared-mb 512
    cache-per-connection-mb 32
</pgjsonb>
```

The caches store pickle bytes keyed by object ID, avoiding repeated PostgreSQL round-trips and JSONB transcoding.
The default is 256 MB for `cache-shared-mb` and 16 MB for `cache-per-connection-mb`.
Set either to 0 to disable that tier.
See {ref}`cache-tiers` for the model.

(deploy-production-roll-out-without-a-ddl-stampede)=

## Roll out without a DDL stampede

When you run several replicas and deploy with a rolling update, every pod starts up and wants to apply its deferred schema DDL.
The storage serializes this DDL behind an advisory lock and skips it when the schema is already current, with no configuration needed.
To tune it, use two environment variables:

- Set `ZODB_PGJSONB_DDL_LOCK_TIMEOUT` (default `15min`) to bound how long a pod waits for the lock before requeuing its DDL for the next transaction.
- Set `ZODB_PGJSONB_FORCE_DDL=1` on a single pod to bypass the schema check and re-apply all deferred DDL, for example after you manually dropped an index.
  Remove it again once that pod has started.
  The rest of the fleet then keeps skipping DDL that is already current.

```{seealso}
{ref}`startup-ddl-gate` explains how the gate works and why it trades re-running idempotent DDL for skipping the lock.
For the variables themselves, see {doc}`/reference/configuration`.
```

(deploy-production-schedule-periodic-pack)=

## Schedule periodic pack

Pack removes unreachable objects (history-free mode) or old revisions (history-preserving mode).
Schedule a cron job to run pack periodically:

```python
#!/usr/bin/env python
"""Pack the ZODB database."""

import time
from zodb_pgjsonb.storage import PGJsonbStorage

storage = PGJsonbStorage(dsn="dbname=zodb user=zodb host=localhost")
# Pack to 1 day ago
pack_time = time.time() - 86400
storage.pack(pack_time, None)
storage.close()
```

Save this as `pack_db.py` and add a cron entry:

```text
0 3 * * * /path/to/venv/bin/python /path/to/pack_db.py
```

Pack runs pure SQL (recursive CTE on the pre-extracted `refs` column) and does not load any objects.
It also deletes orphaned S3 blobs when S3 tiering is configured.

(deploy-production-monitor-blob-statistics)=

## Monitor blob statistics

Use `get_blob_stats()` to check blob storage health:

```python
from zodb_pgjsonb.storage import PGJsonbStorage

storage = PGJsonbStorage(dsn="dbname=zodb user=zodb host=localhost")
stats = storage.get_blob_stats()
print(f"Total blobs: {stats['total_blobs']}")
print(f"Total size: {stats['total_size_display']}")
print(f"PG blobs: {stats['pg_count']} ({stats['pg_size_display']})")
print(f"S3 blobs: {stats['s3_count']} ({stats['s3_size_display']})")
print(f"Largest blob: {stats['largest_blob_display']}")
storage.close()
```

When running under Zope, blob statistics are available in the ZMI database management tab.

(deploy-production-tune-postgresql)=

## Tune PostgreSQL

Key `postgresql.conf` settings for a dedicated PostgreSQL instance:

```ini
shared_buffers = 256MB          # 25% of RAM for dedicated PG
work_mem = 32MB                 # For complex queries and sorts
effective_cache_size = 1GB      # Available OS cache
maintenance_work_mem = 256MB    # For VACUUM and REINDEX
max_connections = 100           # Match pool-max-size * instances + overhead
```

Enable slow query logging to identify performance issues:

```ini
log_min_duration_statement = 100   # Log queries slower than 100 ms
```

Run `ANALYZE object_state` after large bulk imports or migrations to update planner statistics.

Configure autovacuum for `object_state` if you have high write throughput:

```sql
ALTER TABLE object_state SET (autovacuum_analyze_threshold = 5000);
```

(deploy-production-back-up-the-database-and-blobs)=

## Back up the database and blobs

### PostgreSQL backup

Use `pg_dump` for logical backups:

```shell
pg_dump -Fc -f zodb_backup.dump "dbname=zodb user=zodb host=localhost"
```

For continuous archiving, use `pg_basebackup` or `pgBackRest`.
All ZODB data (objects, transactions, blobs stored in PG) is captured by standard PostgreSQL backup tools.

### S3 blob backup

Enable S3 bucket versioning to protect against accidental deletion:

```shell
aws s3api put-bucket-versioning \
    --bucket my-zodb-blobs \
    --versioning-configuration Status=Enabled
```

Set up S3 cross-region replication or lifecycle rules for additional durability.
Pack deletes orphaned S3 blobs.
Versioning therefore preserves a safety net.

(deploy-production-monitor-the-database)=

## Monitor the database

Key PostgreSQL views for monitoring:

```sql
-- Active connections and queries
SELECT pid, state, query, now() - query_start AS duration
FROM pg_stat_activity
WHERE datname = 'zodb'
ORDER BY duration DESC;

-- Table sizes
SELECT relname, pg_size_pretty(pg_total_relation_size(relid)) AS size
FROM pg_stat_user_tables
ORDER BY pg_total_relation_size(relid) DESC;

-- Index usage
SELECT indexrelname, idx_scan, pg_size_pretty(pg_relation_size(indexrelid)) AS size
FROM pg_stat_user_indexes
ORDER BY idx_scan DESC;
```
