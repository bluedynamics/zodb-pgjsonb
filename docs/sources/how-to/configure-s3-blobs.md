<!-- diataxis: how-to -->

# How to configure S3 blob tiering

This guide shows you how to configure tiered blob storage so that small blobs stay in PostgreSQL and large blobs go to S3.

## Install the S3 extra

```bash
pip install zodb-pgjsonb[s3]
# or
uv pip install zodb-pgjsonb[s3]
```

This installs the `zodb-s3blobs` dependency with the boto3 S3 client.

## Add S3 configuration to ZConfig

Add the S3 keys to your `<pgjsonb>` section in `zope.conf`:

```xml
%import zodb_pgjsonb

<zodb_db main>
    <pgjsonb>
        dsn dbname=zodb user=zodb host=localhost port=5432

        s3-bucket-name my-zodb-blobs
        s3-region us-east-1
        s3-prefix production/

        blob-threshold 100KB
        blob-cache-dir /var/cache/zodb-blobs
        blob-cache-size 1GB
    </pgjsonb>
</zodb_db>
```

### S3 configuration keys

| Key | Default | Description |
|---|---|---|
| `s3-bucket-name` | *none* | S3 bucket name (setting this enables S3 tiering) |
| `s3-region` | *none* | AWS region name |
| `s3-endpoint-url` | *none* | S3 endpoint URL (for MinIO, Ceph, or other S3-compatible stores) |
| `s3-access-key` | *none* | AWS access key ID (uses boto3 credential chain if omitted) |
| `s3-secret-key` | *none* | AWS secret access key (uses boto3 credential chain if omitted) |
| `s3-use-ssl` | `true` | Enable SSL for S3 connections |
| `s3-prefix` | *empty* | S3 key prefix for namespace isolation |

### Blob storage keys

| Key | Default | Description |
|---|---|---|
| `blob-threshold` | `100KB` | Blobs larger than this go to S3 |
| `blob-cache-dir` | *auto* | Local cache directory for S3 blobs |
| `blob-cache-size` | `1GB` | Maximum size of the local blob cache |

## Choose a blob-threshold strategy

The `blob-threshold` setting controls how blobs are distributed between PostgreSQL and S3.

**PG-only mode (no S3):**
Omit `s3-bucket-name` entirely.
All blobs stay in PostgreSQL `bytea` columns regardless of size.

**Tiered mode (default with S3):**
Set `blob-threshold` to a size like `100KB` (the default).
Blobs smaller than this stay in PostgreSQL for fast access.
Blobs at or above this threshold go to S3.

**S3-only mode:**
Set `blob-threshold` to `0`.
All blobs go to S3, keeping the PostgreSQL database compact.

## Set up a local blob cache

Set `blob-cache-dir` to a local directory for caching S3 blobs on disk.
This avoids repeated S3 downloads for frequently accessed blobs.

Set `blob-cache-size` to the maximum disk space for the cache.
The cache uses LRU eviction when the limit is reached.

For production, always configure a blob cache directory to reduce S3 latency and cost.

## Use MinIO for development

Start a MinIO container:

```bash
docker run -d --name minio \
  -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

Create a bucket:

```bash
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/zodb-blobs
```

Configure the storage to use MinIO:

```xml
<pgjsonb>
    dsn dbname=zodb user=zodb host=localhost port=5432

    s3-bucket-name zodb-blobs
    s3-endpoint-url http://localhost:9000
    s3-access-key minioadmin
    s3-secret-key minioadmin
    s3-use-ssl false

    blob-threshold 100KB
</pgjsonb>
```
