<!-- diataxis: tutorial -->

# Migrate a Plone site from FileStorage

## What we will build

In this tutorial we will migrate an existing Plone site from FileStorage (the default `Data.fs` + blob directory) to zodb-pgjsonb, storing all object state as queryable JSONB in PostgreSQL.

By the end we will have a running Plone site backed entirely by PostgreSQL, with the original FileStorage preserved as a backup.

## Prerequisites

- A working Plone 6 site using FileStorage (the default storage)
- PostgreSQL 14+ running and accessible
- Python 3.12+ with the same virtual environment as the Plone site
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Step 1: install zodb-pgjsonb

Activate the virtual environment that our Plone site uses and install zodb-pgjsonb:

```bash
source .venv/bin/activate
uv pip install zodb-pgjsonb
```

This also installs `zodbconvert`, which is part of the ZODB package.

## Step 2: prepare the PostgreSQL database

Create a dedicated database for the ZODB data.
Connect to PostgreSQL as a superuser and run:

```bash
psql -h localhost -p 5432 -U postgres
```

```sql
CREATE USER zodb WITH PASSWORD 'zodb';
CREATE DATABASE zodb OWNER zodb;
\q
```

Verify the connection:

```bash
psql -h localhost -p 5432 -U zodb -d zodb -c "SELECT version();"
```

We should see the PostgreSQL version string.
zodb-pgjsonb creates all required tables (`object_state`, `transaction_log`, `blob_state`, etc.) automatically on first connection.

## Step 3: stop the Plone site

We must stop Plone before migrating to avoid writes to the source FileStorage during the copy.
Use `Ctrl+C` in the terminal where Zope is running, or stop the process through your process manager (supervisord, systemd, etc.).

## Step 4: write the zodbconvert configuration

Create a file called `migrate.cfg` in the project directory:

```xml
<source>
    <filestorage>
        path /path/to/var/filestorage/Data.fs
        blob-dir /path/to/var/blobstorage
    </filestorage>
</source>

<destination>
    %import zodb_pgjsonb
    <pgjsonb>
        dsn dbname=zodb user=zodb password=zodb host=localhost port=5432
    </pgjsonb>
</destination>
```

Replace `/path/to/var/filestorage/Data.fs` and `/path/to/var/blobstorage` with the actual paths from the existing Zope configuration.

:::{tip}
Look in the current `zope.conf` for the `<filestorage>` section to find the exact paths.
A typical Plone installation uses `instance/var/filestorage/Data.fs` and `instance/var/blobstorage`.
:::

## Step 5: run the migration

Run `zodbconvert` to copy all transactions from FileStorage to PostgreSQL:

```bash
.venv/bin/zodbconvert --clear migrate.cfg
```

The `--clear` flag ensures the destination database starts empty.

We should see progress output as transactions are copied:

```text
Copying transactions from FileStorage to PGJsonbStorage
[00:15] 8,200 / 28,394 records (28.9%) | 547 rec/s | 12.3 MB/s
[00:30] 17,500 / 28,394 records (61.6%) | 620 rec/s | 11.8 MB/s
Copied 1,542 transactions with 28,394 records in 45.2s (628 rec/s, 12.3 MB/s)
```

:::{note}
The migration speed depends on database size, network latency, and disk I/O.
A typical Plone site with 50,000 objects migrates in under 5 minutes.
:::

### Parallel migration for large databases

For large databases, zodb-pgjsonb supports parallel migration using multiple PostgreSQL connections.
The `zodbconvert` CLI does not expose a `--workers` flag, so we use a short Python script instead:

Create a file called `migrate_parallel.py`:

```python
from ZODB.FileStorage import FileStorage
from ZODB.blob import BlobStorage
from zodb_pgjsonb import PGJsonbStorage

# Open the source FileStorage (read-only)
fs = FileStorage("/path/to/var/filestorage/Data.fs", read_only=True)
source = BlobStorage("/path/to/var/blobstorage", fs)

# Open the destination with a larger connection pool
dest = PGJsonbStorage(
    dsn="dbname=zodb user=zodb password=zodb host=localhost port=5432",
    pool_max_size=8,
)

# Copy with 4 parallel workers
dest.copyTransactionsFrom(source, workers=4)

source.close()
dest.close()
```

Run the script:

```bash
python migrate_parallel.py
```

The main thread reads from FileStorage and decodes pickles.
Worker threads write to PostgreSQL concurrently.
OID ordering is preserved automatically.

We should see progress output with throughput and ETA:

```text
Copying transactions with 4 parallel workers (total OIDs: 128,503)
[00:12] 15,200 / 128,503 OIDs (11.8%) | 1,267 obj/s | ETA 01:30
[00:24] 31,400 / 128,503 OIDs (24.4%) | 1,350 obj/s | ETA 01:12
[00:48] 72,100 / 128,503 OIDs (56.1%) | 1,502 obj/s | ETA 00:38
[01:38] All 8,421 transactions copied with 4 workers.
```

:::{tip}
Set `pool_max_size` to at least the number of workers.
Using 4 to 6 workers provides the best throughput for most deployments.
:::

## Step 6: update zope.conf

Replace the `<filestorage>` section in `zope.conf` with a `<pgjsonb>` section:

```xml
%import zodb_pgjsonb

<zodb_db main>
    mount-point /
    cache-size 30000
    <pgjsonb>
        dsn dbname=zodb user=zodb password=zodb host=localhost port=5432
        blob-temp-dir ./instance/var/blobtemp
    </pgjsonb>
</zodb_db>
```

Create the blob temp directory:

```bash
mkdir -p instance/var/blobtemp
```

:::{note}
Remove or comment out the old `<filestorage>` and `<blobstorage>` sections.
Do not delete the original `Data.fs` and blob directory yet -- keep them as a backup until we verify the migration.
:::

## Step 7: start and verify

Start the Plone site:

```bash
.venv/bin/runwsgi instance/etc/zope.ini
```

We should see log output ending with:

```text
INFO  [waitress:486] Serving on http://0.0.0.0:8081
```

Open the site in a browser and verify:

1. Log in and browse existing content.
2. Check that images and file downloads work (blob data).
3. Create a new page to confirm writes work.

### Verify with SQL

Open a second terminal and connect to PostgreSQL:

```bash
psql -h localhost -p 5432 -U zodb -d zodb
```

Count the objects:

```sql
SELECT count(*) FROM object_state;
```

List the content types:

```sql
SELECT class_mod || '.' || class_name AS class,
       count(*) AS count
FROM object_state
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
```

We should see the same objects that existed in the original FileStorage.

## What we learned

- zodb-pgjsonb implements `IStorageRestoreable`, making it compatible with `zodbconvert`
- The standard `zodbconvert` tool copies all transactions, including blobs
- For large databases, the Python API `copyTransactionsFrom(source, workers=N)` uses parallel PostgreSQL connections for higher throughput
- After migration, we swap the `<filestorage>` section for `<pgjsonb>` in `zope.conf`
- All ZODB data is now queryable JSON in PostgreSQL

## Next steps

- {doc}`quickstart-docker` to explore SQL queryability and S3 blob tiering
- {doc}`/reference/configuration` for the complete list of configuration options
