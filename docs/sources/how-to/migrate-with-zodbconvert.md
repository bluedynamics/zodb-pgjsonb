<!-- diataxis: how-to -->

# How to migrate with zodbconvert

This guide shows you how to migrate an existing ZODB database (FileStorage or RelStorage) to zodb-pgjsonb using [zodb-convert](https://github.com/bluedynamics/zodb-convert) or the Python API.

## Write a zodbconvert configuration file

Create a file named `migrate.cfg`:

### From FileStorage

```ini
<source>
    <filestorage>
        path /path/to/Data.fs
        blob-dir /path/to/blobstorage
    </filestorage>
</source>
<destination>
    %import zodb_pgjsonb
    <pgjsonb>
        dsn dbname=zodb_new user=zodb host=localhost port=5432
    </pgjsonb>
</destination>
```

### From RelStorage

```ini
<source>
    <relstorage>
        <postgresql>
            dsn dbname=zodb_old user=zodb host=localhost
        </postgresql>
    </relstorage>
</source>
<destination>
    %import zodb_pgjsonb
    <pgjsonb>
        dsn dbname=zodb_new user=zodb host=localhost port=5432
    </pgjsonb>
</destination>
```

## Run the migration

```bash
zodb-convert migrate.cfg
```

This copies all transactions sequentially, including blobs.
The destination storage creates its schema automatically.

`zodb-convert` is a standalone tool that works with any ZODB-compatible storage.
Install it with `pip install zodb-convert` (or `uv pip install zodb-convert`).

## Run a parallel migration

For large databases, use multiple worker threads for faster migration:

```bash
zodb-convert -w 4 migrate.cfg
```

This delegates to the destination storage's `copyTransactionsFrom(source, workers=4)`.
The main thread iterates the source and decodes pickles; worker threads write to PostgreSQL concurrently.
Set `pool-max-size` in your destination config to at least the number of workers plus one.

### Python API

If you need more control, use the Python API directly:

```python
from zodb_pgjsonb.storage import PGJsonbStorage
from ZODB.FileStorage import FileStorage
from ZODB.blob import BlobStorage

source = BlobStorage("/path/to/blobstorage", FileStorage("/path/to/Data.fs"))
dest = PGJsonbStorage(
    dsn="dbname=zodb_new user=zodb host=localhost",
    pool_max_size=10,
)
dest.copyTransactionsFrom(source, workers=4)
dest.close()
source.close()
```

## Resume an interrupted migration

If a migration is interrupted (Ctrl-C, crash, network failure), resume with `--incremental`:

```bash
zodb-convert --incremental -w 4 migrate.cfg
```

During parallel copy, the storage tracks a *watermark* — the highest TID where all
prior TIDs are also committed.
On resume, iteration starts from the watermark (not `lastTransaction()`) to fill any
gaps left by out-of-order worker commits.
Already-committed transactions are skipped automatically.
The watermark table is dropped on successful completion, adding zero overhead to
non-incremental imports.

### Python API

```python
from zodb_pgjsonb.storage import PGJsonbStorage
from ZODB.FileStorage import FileStorage
from ZODB.blob import BlobStorage
from ZODB.utils import p64, u64

source = BlobStorage("/path/to/blobstorage", FileStorage("/path/to/Data.fs"))
dest = PGJsonbStorage(
    dsn="dbname=zodb_new user=zodb host=localhost",
    pool_max_size=10,
)

# Determine where to resume from
start_tid = p64(u64(dest.lastTransaction()) + 1)
dest.copyTransactionsFrom(source, workers=4, start_tid=start_tid)
dest.close()
source.close()
```

## Migrate blobs

Blobs are migrated automatically by both `zodbconvert` and `copyTransactionsFrom`.
The destination storage applies its blob tiering rules (PG bytea vs S3) based on the configured `blob-threshold`.

If you have S3 tiering configured on the destination, large blobs are uploaded to S3 during the migration.

## Faster blob migration with S3

When migrating large databases with many blobs to S3-backed storage, S3 upload
latency can be the bottleneck. Two modes decouple S3 uploads from PostgreSQL writes:

### Background mode (recommended)

Uploads blobs to S3 in a background thread pool while PostgreSQL writes continue
independently:

```bash
zodb-convert -w 8 --background-blobs migrate.cfg
```

PostgreSQL throughput is no longer gated by S3 latency. All uploads complete before
the migration finishes. If any upload fails (after retries), the migration aborts.

### Deferred mode

Skips S3 uploads entirely during migration, writing a manifest file instead.
Upload blobs later in a separate step:

```bash
# Step 1: Fast PG-only migration
zodb-convert -w 8 --deferred-blobs /tmp/blob-manifest.tsv migrate.cfg

# Step 2: Upload blobs from manifest
zodb-convert --upload-blobs /tmp/blob-manifest.tsv --dest-zope-conf zope.conf -w 16
```

This is useful when S3 is temporarily unavailable or when you want to run the
PG import at maximum speed and handle S3 as a separate operational step.

**Note:** Temp blob files are preserved during deferred mode (they are needed for
the upload step). Ensure the staging directory has sufficient disk space.

### Python API

```python
# Background mode
dest.copyTransactionsFrom(source, workers=4, blob_mode="background")

# Deferred mode
dest.copyTransactionsFrom(source, workers=4, blob_mode="deferred:/tmp/manifest.tsv")
```

## Verify the migration

Connect to the destination database and check the object count:

```sql
SELECT count(*) FROM object_state;
SELECT count(*) FROM transaction_log;
SELECT count(*) FROM blob_state;
```

Open the database with ZODB and verify application data:

```python
from zodb_pgjsonb.storage import PGJsonbStorage
import ZODB

storage = PGJsonbStorage(dsn="dbname=zodb_new user=zodb host=localhost")
db = ZODB.DB(storage)
conn = db.open()
root = conn.root()
print(list(root.keys()))
conn.close()
db.close()
```
