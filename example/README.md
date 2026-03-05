# zodb-pgjsonb Example Setup

Try out **zodb-pgjsonb** with a full Plone 6 site backed by PostgreSQL JSONB
and MinIO for S3 blob tiering.

## Prerequisites

- **Docker** and **Docker Compose** (v2+)
- **Python 3.12+**
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Quick Start

### 1. Start infrastructure services

```bash
cd example/
docker compose up -d
```

This starts:

| Service    | Port  | Purpose                              | Credentials                        |
|------------|-------|--------------------------------------|------------------------------------|
| PostgreSQL | 5433  | ZODB object storage (JSONB)          | user=zodb password=zodb db=zodb    |
| MinIO API  | 9000  | S3-compatible blob storage           | minioadmin / minioadmin            |
| MinIO UI   | 9001  | Web console for browsing blobs       | minioadmin / minioadmin            |

The `zodb-blobs` bucket is created automatically.

### 2. Create a Python virtual environment

```bash
cd ..  # back to zodb-pgjsonb root (or any working directory)
uv venv -p 3.13
source .venv/bin/activate
uv pip install -r example/requirements.txt
```

### 3. Generate a Zope instance

```bash
uvx cookiecutter -f --no-input --config-file /dev/null \
    gh:plone/cookiecutter-zope-instance \
    target=instance \
    wsgi_listen=0.0.0.0:8081 \
    initial_user_name=admin \
    initial_user_password=admin
```

### 4. Copy the example configuration

```bash
cp example/zope.conf instance/etc/zope.conf
cp example/zope.ini instance/etc/zope.ini
cp example/site.zcml instance/etc/site.zcml
mkdir -p instance/var/blobtemp instance/var/blobcache
```

### 5. Start Zope

```bash
.venv/bin/runwsgi instance/etc/zope.ini
```

### 6. Create a Plone site

Open http://localhost:8081 in your browser and create a new Plone site
using the admin/admin credentials.

## Exploring the JSONB Data (the killer feature)

This is the main reason zodb-pgjsonb exists: your ZODB data is stored as
**queryable JSONB** in PostgreSQL, not as opaque pickle blobs.

### Start pgAdmin

```bash
docker compose --profile tools up -d
```

Open http://localhost:5050 (login: admin@example.com / admin).

Add a server connection:
- **Host**: postgres (Docker service name, both containers share the same network)
- **Port**: 5432
- **Username**: zodb
- **Password**: zodb
- **Database**: zodb

Or connect directly with psql:

```bash
psql -h localhost -p 5433 -U zodb -d zodb
```

### Example SQL queries

**List all object types and counts:**

```sql
SELECT class_mod || '.' || class_name AS class,
       count(*) AS count
FROM object_state
GROUP BY 1
ORDER BY 2 DESC;
```

**Find all Plone content items:**

```sql
SELECT zoid,
       class_mod || '.' || class_name AS class,
       state->>'title' AS title,
       state->>'portal_type' AS type
FROM object_state
WHERE class_mod LIKE 'plone.app.contenttypes.content%'
ORDER BY zoid;
```

**Full-text search across all objects:**

```sql
SELECT zoid,
       class_mod || '.' || class_name AS class,
       state->>'title' AS title
FROM object_state
WHERE state::text ILIKE '%search term%'
LIMIT 20;
```

**Find objects that reference a specific OID:**

```sql
SELECT zoid,
       class_name,
       state->>'title' AS title
FROM object_state
WHERE state @> '{"@ref": "0000000000000001"}'::jsonb;
```

**Count blobs by storage location:**

```sql
SELECT
    CASE
        WHEN s3_key IS NOT NULL THEN 'S3 (MinIO)'
        ELSE 'PostgreSQL bytea'
    END AS storage,
    count(*) AS count,
    pg_size_pretty(sum(blob_size)) AS total_size
FROM blob_state
GROUP BY 1;
```

## S3 Blob Tiering

Blobs smaller than `blob-threshold` (default 100KB) are stored directly
in PostgreSQL as bytea. Larger blobs are uploaded to MinIO/S3.

To see it in action:

1. Upload an image larger than 100KB in Plone
2. Open the MinIO console at http://localhost:9001
3. Browse the `zodb-blobs` bucket to see the uploaded file

The blob metadata is always in PostgreSQL (in `blob_state`), so you can
query which blobs are in S3:

```sql
SELECT zoid, tid, blob_size, s3_key
FROM blob_state
WHERE s3_key IS NOT NULL;
```

## Configuration Reference

The `<pgjsonb>` section in `zope.conf` supports these keys:

| Key                  | Default | Description                                          |
|----------------------|---------|------------------------------------------------------|
| `dsn`                | *required* | PostgreSQL connection string (libpq format)       |
| `name`               | pgjsonb | Storage name                                         |
| `history-preserving` | false   | Enable history-preserving mode (undo support)        |
| `blob-temp-dir`      | auto    | Directory for temporary blob files                   |
| `cache-local-mb`     | 64      | Per-instance object cache size in MB                 |
| `pool-size`          | 1       | Minimum connections in pool                          |
| `pool-max-size`      | 10      | Maximum connections in pool                          |
| `blob-threshold`     | 100KB   | Blobs larger than this go to S3 (if configured)      |
| `s3-bucket-name`     | *none*  | S3 bucket name (enables S3 tiering)                  |
| `s3-endpoint-url`    | *none*  | S3 endpoint (for MinIO, Ceph, etc.)                  |
| `s3-region`          | *none*  | AWS region name                                      |
| `s3-access-key`      | *none*  | AWS access key (uses boto3 chain if omitted)         |
| `s3-secret-key`      | *none*  | AWS secret key (uses boto3 chain if omitted)         |
| `s3-use-ssl`         | true    | Enable SSL for S3 connections                        |
| `s3-prefix`          |         | S3 key prefix for namespace isolation                |
| `blob-cache-dir`     | *none*  | Local cache directory for S3 blobs                   |
| `blob-cache-size`    | 1GB     | Maximum size of local blob cache                     |

### Deployment Modes

| Mode     | Config                      | Where blobs are stored           |
|----------|-----------------------------|----------------------------------|
| PG-only  | No S3 keys set              | All blobs in PostgreSQL bytea    |
| Tiered   | S3 keys + blob-threshold    | Small in PG, large in S3        |
| S3-only  | S3 keys + blob-threshold=0  | All blobs in S3                  |

## Cleanup

Keep data for next time:

```bash
docker compose down
```

Remove all data (fresh start):

```bash
docker compose down -v
```
