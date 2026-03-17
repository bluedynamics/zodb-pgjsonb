<!-- diataxis: reference -->

# Configuration options

This page documents all configuration options for zodb-pgjsonb, including
ZConfig directives, Python constructor parameters, S3 deployment modes,
and dependencies.

## ZConfig `<pgjsonb>` section

The storage is configured in `zope.conf` via a `<pgjsonb>` section after
importing the package:

```ini
%import zodb_pgjsonb

<zodb_db main>
  <pgjsonb>
    dsn dbname=zodb host=localhost port=5432 user=zodb password=zodb
  </pgjsonb>
</zodb_db>
```

The factory class is `zodb_pgjsonb.config.PGJsonbStorageFactory`.
It implements `ZODB.storage`.

### Configuration keys

| Key | Type | Default | Required | Description |
|---|---|---|---|---|
| `dsn` | string | -- | yes | PostgreSQL connection string in libpq `key=value` format or `postgresql://` URI format. |
| `name` | string | `pgjsonb` | no | Storage name, used in sort keys. |
| `history-preserving` | boolean | `false` | no | Enable history-preserving mode. |
| `blob-temp-dir` | string | (auto-created tempdir) | no | Directory for temporary blob files. Auto-created if omitted. |
| `cache-local-mb` | integer | `16` | no | Size of the per-instance object cache in megabytes. Caches `load()` results (pickle bytes) to avoid repeated PostgreSQL round-trips and JSONB transcoding. Set to `0` to disable. |
| `pool-size` | integer | `1` | no | Minimum number of connections in the instance connection pool. Set to `0` for on-demand creation. |
| `pool-max-size` | integer | `10` | no | Maximum number of connections in the instance connection pool. |
| `pool-timeout` | float | `30.0` | no | Connection pool acquisition timeout in seconds. Raises `StorageError` if a connection cannot be acquired within this time. |
| `blob-threshold` | byte-size | `100KB` | no | Blobs larger than this value are stored in S3 when S3 is configured. Blobs smaller than this remain in PostgreSQL bytea. Set to `0` to send all blobs to S3. Requires `s3-bucket-name`. |
| `s3-bucket-name` | string | -- | no | S3 bucket name for large blob storage. If omitted, all blobs are stored in PostgreSQL bytea. |
| `s3-prefix` | string | `""` | no | S3 key prefix for namespace isolation. |
| `s3-endpoint-url` | string | -- | no | S3 endpoint URL for MinIO, Ceph, or other S3-compatible stores. |
| `s3-region` | string | -- | no | AWS region name. |
| `s3-access-key` | string | -- | no | AWS access key ID. Uses the boto3 credential chain if omitted. |
| `s3-secret-key` | string | -- | no | AWS secret access key. Uses the boto3 credential chain if omitted. |
| `s3-use-ssl` | boolean | `true` | no | Enable SSL for S3 connections. |
| `blob-cache-dir` | string | (falls back to `blob-temp-dir`) | no | Local cache directory for S3 blobs. Recommended for production to avoid repeated S3 downloads. |
| `blob-cache-size` | byte-size | `1GB` | no | Maximum size of the local blob cache directory. |

## Python constructor

`PGJsonbStorage` accepts the following parameters:

```python
PGJsonbStorage(
    dsn: str,
    name: str = "pgjsonb",
    history_preserving: bool = False,
    blob_temp_dir: str | None = None,
    cache_local_mb: int = 16,
    pool_size: int = 1,
    pool_max_size: int = 10,
    pool_timeout: float = 30.0,
    s3_client: S3Client | None = None,
    blob_cache: S3BlobCache | None = None,
    blob_threshold: int = 102_400,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `dsn` | `str` | -- | PostgreSQL connection string. |
| `name` | `str` | `"pgjsonb"` | Storage name. |
| `history_preserving` | `bool` | `False` | Enable history-preserving mode. |
| `blob_temp_dir` | `str \| None` | `None` | Blob temp directory. A temporary directory is auto-created when `None`. |
| `cache_local_mb` | `int` | `16` | Per-instance LRU cache size in megabytes. |
| `pool_size` | `int` | `1` | Minimum pool connections. |
| `pool_max_size` | `int` | `10` | Maximum pool connections. |
| `pool_timeout` | `float` | `30.0` | Pool acquisition timeout in seconds. |
| `s3_client` | `S3Client \| None` | `None` | S3 client instance from `zodb-s3blobs`. |
| `blob_cache` | `S3BlobCache \| None` | `None` | Local S3 blob cache instance from `zodb-s3blobs`. |
| `blob_threshold` | `int` | `102400` | Byte threshold for S3 tiering. |

The ZConfig factory (`PGJsonbStorageFactory`) constructs `S3Client` and
`S3BlobCache` from the ZConfig keys before passing them to the constructor.

## S3 deployment modes

| Mode | Configuration | Blob behavior |
|---|---|---|
| PG-only | `s3-bucket-name` omitted | All blobs stored as PostgreSQL bytea in `blob_state.data`. |
| Tiered | `s3-bucket-name` set, `blob-threshold` > 0 | Blobs smaller than threshold stored in PG bytea; blobs at or above threshold stored in S3 with key in `blob_state.s3_key`. |
| S3-only | `s3-bucket-name` set, `blob-threshold` = 0 | All blobs stored in S3. |

## Dependencies

### Required

| Package | Purpose |
|---|---|
| `ZODB>=5.8` | ZODB framework. |
| `zodb-json-codec>=1.6.1` | Rust/PyO3 pickle-to-JSON transcoder. |
| `psycopg[binary,pool]>=3.1` | PostgreSQL adapter with connection pooling. |
| `zope.interface` | Interface declarations. |
| `transaction` | Transaction management. |
| `ZConfig` | Configuration file parsing. |

### Optional

| Package | Extra | Purpose |
|---|---|---|
| `zodb-s3blobs` | `s3` | S3 client and blob cache for tiered blob storage. |
| `boto3>=1.26` | `s3` | AWS SDK for S3 access. |

Install S3 support with `pip install zodb-pgjsonb[s3]`.
