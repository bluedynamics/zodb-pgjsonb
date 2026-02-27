# zodb-pgjsonb

ZODB storage adapter for PostgreSQL using JSONB, powered by [zodb-json-codec](https://github.com/bluedynamics/zodb-json-codec).

## Overview

`zodb-pgjsonb` is a modern, PostgreSQL-only ZODB storage that stores object state as **JSONB** instead of pickle/bytea. This enables:

- **SQL Queryability**: Query ZODB objects directly with PostgreSQL JSONB operators and GIN indexes
- **Transparency**: ZODB sees pickle bytes; PostgreSQL sees queryable JSON
- **Performance**: Rust-based codec (2x faster than CPython pickle), psycopg3 with pipelined writes
- **Tiered Blobs**: Small blobs in PG bytea, large blobs in S3 (configurable threshold)
- **Security**: JSON has no code execution attack surface (unlike pickle deserialization)
- **Full ZODB Compatibility**: IStorage, IMVCCStorage, IBlobStorage, IStorageUndoable, IStorageIteration, IStorageRestoreable

## Installation

```bash
pip install zodb-pgjsonb

# With S3 blob tiering support
pip install zodb-pgjsonb[s3]
```

Requires PostgreSQL 14+ and [zodb-json-codec](https://pypi.org/project/zodb-json-codec/) (installed automatically).

## Quick Start

### ZConfig (Zope/Plone)

```xml
%import zodb_pgjsonb

<zodb_db main>
    mount-point /
    cache-size 30000
    <pgjsonb>
        dsn dbname=zodb user=zodb password=zodb host=localhost port=5432
    </pgjsonb>
</zodb_db>
```

### Python API

```python
from zodb_pgjsonb import PGJsonbStorage
import ZODB

storage = PGJsonbStorage(dsn="dbname=zodb user=zodb host=localhost")
db = ZODB.DB(storage)
conn = db.open()
root = conn.root()
root["hello"] = "world"
import transaction
transaction.commit()
```

## SQL Queryability (the killer feature)

Your ZODB data is stored as queryable JSONB, not opaque pickle blobs:

```sql
-- List all object types
SELECT class_mod || '.' || class_name AS class, count(*)
FROM object_state GROUP BY 1 ORDER BY 2 DESC;

-- Find Plone content items
SELECT zoid, state->>'title' AS title, state->>'portal_type' AS type
FROM object_state
WHERE class_mod LIKE 'plone.app.contenttypes.content%';

-- Full-text search across all objects
SELECT zoid, state->>'title' AS title
FROM object_state
WHERE state::text ILIKE '%search term%';

-- Connect Grafana, Metabase, or any SQL tool directly
```

## Architecture

```
ZODB.DB
  └-> PGJsonbStorage (IStorage + IMVCCStorage + IBlobStorage + IStorageUndoable)
       ├-> zodb-json-codec    (pickle <-> JSONB transcoding, Rust)
       ├-> psycopg3           (sync, pipelined writes, connection pool)
       ├-> LISTEN/NOTIFY      (instant invalidation via PG trigger)
       ├-> Tiered blobs       (PG bytea + optional S3 via zodb-s3blobs)
       └-> Pure SQL pack/GC   (recursive CTE on pre-extracted refs column)
```

## Key Features

- **JSONB-native schema** with extracted class columns and GIN indexes
- **Pure SQL pack/GC** via pre-extracted `refs` column (no object loading needed, 15-28x faster than RelStorage)
- **Configurable history modes** (history-free or history-preserving with undo)
- **Connection pooling** via psycopg3 ConnectionPool
- **Pipelined batch writes** via `executemany()` (single network round-trip)
- **State processor plugins** for writing extra columns atomically alongside object state (with optional DDL via `get_schema_sql()`)
- **zodbconvert compatible** for migration from RelStorage or FileStorage
- **ZConfig integration** for standard Zope/Plone deployment

## Configuration Reference

| Key                  | Default    | Description                                     |
|----------------------|------------|-------------------------------------------------|
| `dsn`                | *required* | PostgreSQL connection string (libpq format)     |
| `name`               | pgjsonb    | Storage name                                    |
| `history-preserving` | false      | Enable history-preserving mode (undo support)   |
| `blob-temp-dir`      | auto       | Directory for temporary blob files              |
| `cache-local-mb`     | 64         | Per-instance object cache size in MB            |
| `pool-size`          | 1          | Minimum connections in pool                     |
| `pool-max-size`      | 10         | Maximum connections in pool                     |
| `blob-threshold`     | 1MB        | Blobs larger than this go to S3 (if configured) |
| `s3-bucket-name`     | *none*     | S3 bucket name (enables S3 tiering)             |
| `s3-endpoint-url`    | *none*     | S3 endpoint (for MinIO, Ceph, etc.)             |
| `s3-region`          | *none*     | AWS region name                                 |
| `s3-access-key`      | *none*     | AWS access key (uses boto3 chain if omitted)    |
| `s3-secret-key`      | *none*     | AWS secret key (uses boto3 chain if omitted)    |
| `s3-use-ssl`         | true       | Enable SSL for S3 connections                   |
| `s3-prefix`          |            | S3 key prefix for namespace isolation           |
| `blob-cache-dir`     | *none*     | Local cache directory for S3 blobs              |
| `blob-cache-size`    | 1GB        | Maximum size of local blob cache                |

### Switching History Modes

Both directions are supported:

**History-free to history-preserving** (enabling history): Change
`history-preserving` to `true` in your ZConfig and restart. The history
tables (`object_history`, `pack_state`) are created automatically on
startup. Existing objects gain history tracking on their next
modification.

**History-preserving to history-free** (disabling history):

1. Stop all application instances.
2. Run the conversion utility:
   ```python
   from zodb_pgjsonb.storage import PGJsonbStorage
   storage = PGJsonbStorage(dsn="...", history_preserving=True)
   storage.convert_to_history_free()
   storage.close()
   ```
3. Change `history-preserving` to `false` in your ZConfig.
4. Restart.

The conversion drops `object_history` and `pack_state`, removes old blob
versions (keeps only the latest per object), and cleans orphaned
transaction log entries. **This is irreversible** — all undo history is
permanently deleted.

If you start in history-free mode with leftover history tables, the
storage logs a warning pointing to `convert_to_history_free()`.

## Performance

See [BENCHMARKS.md](https://github.com/bluedynamics/zodb-pgjsonb/blob/main/BENCHMARKS.md) for detailed comparison with RelStorage.

Summary (100 iterations, PostgreSQL 17):

| Category | vs RelStorage |
|----------|---------------|
| Single store | 1.6x faster |
| Pack/GC (10K objects) | 25x faster |
| Plone application | on par |
| Batch store 100 | 1.2x slower (JSONB indexing overhead) |

## Try It Out

The [example/](https://github.com/bluedynamics/zodb-pgjsonb/tree/main/example) directory contains a Docker Compose setup with PostgreSQL, MinIO, and pgAdmin to try zodb-pgjsonb with a full Plone 6 site. See [example/README.md](https://github.com/bluedynamics/zodb-pgjsonb/blob/main/example/README.md) for instructions.

## Development

```bash
cd sources/zodb-pgjsonb
uv pip install -e ".[test]"

# Run tests (requires PostgreSQL on localhost:5433)
pytest
```

## Source Code and Contributions

The source code is managed in a Git repository, with its main branches hosted on GitHub.
Issues can be reported there too.

We'd be happy to see many forks and pull requests to make this package even better.
We welcome AI-assisted contributions, but expect every contributor to fully understand and be able to explain the code they submit.
Please don't send bulk auto-generated pull requests.

Maintainers are Jens Klein and the BlueDynamics Alliance developer team.
We appreciate any contribution and if a release on PyPI is needed, please just contact one of us.
We also offer commercial support if any training, coaching, integration or adaptations are needed.

## License

ZPL-2.1 (Zope Public License)
