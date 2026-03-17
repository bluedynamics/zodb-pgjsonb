<!-- diataxis: how-to -->

# How to deploy to production

This guide shows you how to configure zodb-pgjsonb for production workloads.

## Tune connection pool settings

Set `pool-size` and `pool-max-size` based on your expected concurrency:

```xml
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
  Set this to at least the number of concurrent Zope threads plus a margin for background tasks like pack.
- **pool-timeout**: seconds to wait for a connection before raising `StorageError` (default: 30.0).

Each ZODB connection gets its own `PGJsonbStorageInstance` with a dedicated PostgreSQL connection from the pool.

## Tune the object cache

Set `cache-local-mb` to control the per-instance LRU cache for `load()` results:

```xml
<pgjsonb>
    dsn dbname=zodb user=zodb host=localhost
    cache-local-mb 64
</pgjsonb>
```

The cache stores pickle bytes keyed by object ID, avoiding repeated PostgreSQL round-trips and JSONB transcoding.
The default is 16 MB.
Set to 0 to disable caching entirely.

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

```
0 3 * * * /path/to/venv/bin/python /path/to/pack_db.py
```

Pack runs pure SQL (recursive CTE on the pre-extracted `refs` column) and does not load any objects.
It also deletes orphaned S3 blobs when S3 tiering is configured.

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

## PostgreSQL tuning

Key `postgresql.conf` settings for a dedicated PostgreSQL instance:

```
shared_buffers = 256MB          # 25% of RAM for dedicated PG
work_mem = 32MB                 # For complex queries and sorts
effective_cache_size = 1GB      # Available OS cache
maintenance_work_mem = 256MB    # For VACUUM and REINDEX
max_connections = 100           # Match pool-max-size * instances + overhead
```

Enable slow query logging to identify performance issues:

```
log_min_duration_statement = 100   # Log queries slower than 100 ms
```

Run `ANALYZE object_state` after large bulk imports or migrations to update planner statistics.

Configure autovacuum for `object_state` if you have high write throughput:

```sql
ALTER TABLE object_state SET (autovacuum_analyze_threshold = 5000);
```

## Backup strategy

### PostgreSQL backup

Use `pg_dump` for logical backups:

```bash
pg_dump -Fc -f zodb_backup.dump "dbname=zodb user=zodb host=localhost"
```

For continuous archiving, use `pg_basebackup` or `pgBackRest`.
All ZODB data (objects, transactions, blobs stored in PG) is captured by standard PostgreSQL backup tools.

### S3 blob backup

Enable S3 bucket versioning to protect against accidental deletion:

```bash
aws s3api put-bucket-versioning \
    --bucket my-zodb-blobs \
    --versioning-configuration Status=Enabled
```

Set up S3 cross-region replication or lifecycle rules for additional durability.
Pack deletes orphaned S3 blobs, so versioning preserves a safety net.

## Monitoring queries

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
