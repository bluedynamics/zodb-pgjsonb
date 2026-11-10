<!-- diataxis: tutorial -->

# Migrate a Plone site from FileStorage

(migrate-from-filestorage-what-we-will-build)=

## What we will build

In this tutorial we will migrate an existing Plone site from FileStorage (the default `Data.fs` + blob directory) to zodb-pgjsonb.
We will store all object state as queryable JSONB in PostgreSQL.

By the end we will have a running Plone site backed entirely by PostgreSQL, with the original FileStorage preserved as a backup.

(migrate-from-filestorage-prerequisites)=

## Prerequisites

- A working Plone 6 site using FileStorage (the default storage)
- PostgreSQL 15+ running and accessible
- Python 3.12+ with the same virtual environment as the Plone site
- [uv](https://docs.astral.sh/uv/)

(migrate-from-filestorage-step-1-install-zodb-pgjsonb)=

## Step 1: install zodb-pgjsonb

Activate the virtual environment that our Plone site uses and install zodb-pgjsonb:

```shell
source .venv/bin/activate
uv pip install zodb-pgjsonb zodb-convert
```

This also installs the `zodb-convert` command.

(migrate-from-filestorage-step-2-prepare-the-postgresql-database)=

## Step 2: prepare the PostgreSQL database

Create a dedicated database for the ZODB data.
Connect to PostgreSQL as a superuser and run:

```shell
psql -h localhost -p 5432 -U postgres
```

```psql
CREATE USER zodb WITH PASSWORD 'zodb';
CREATE DATABASE zodb OWNER zodb;
\q
```

Verify the connection:

```shell
psql -h localhost -p 5432 -U zodb -d zodb -c "SELECT version();"
```

We should see the PostgreSQL version string.
zodb-pgjsonb creates all required tables (`object_state`, `transaction_log`, `blob_state`, etc.) automatically on first connection.

(migrate-from-filestorage-step-3-stop-the-plone-site)=

## Step 3: stop the Plone site

We must stop Plone before migrating to avoid writes to the source FileStorage during the copy.
Use `Ctrl+C` in the terminal where Zope is running.

(migrate-from-filestorage-step-4-write-the-zodb-convert-configuration)=

## Step 4: write the zodb-convert configuration

Create a file called `migrate.cfg` in the project directory:

```ini
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
Look in the current `zope.conf` for the `<filestorage>` section to find the exact paths.
A typical Plone installation uses `instance/var/filestorage/Data.fs` and `instance/var/blobstorage`.

(migrate-from-filestorage-step-5-run-the-migration)=

## Step 5: run the migration

Run `zodb-convert` to copy all transactions from FileStorage to PostgreSQL:

```shell
.venv/bin/zodb-convert migrate.cfg
```

zodb-convert logs progress while it copies and logs a summary when it finishes.

For large databases, {doc}`/how-to/migrate-with-zodbconvert` shows how to run the copy with several workers.

(migrate-from-filestorage-step-6-update-zope-conf)=

## Step 6: update zope.conf

Replace the `<filestorage>` section in `zope.conf` with a `<pgjsonb>` section:

```ini
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

```shell
mkdir -p instance/var/blobtemp
```

:::{note}
Remove or comment out the old `<filestorage>` and `<blobstorage>` sections.
Do not delete the original `Data.fs` and blob directory yet.
Keep them as a backup until we verify the migration.
:::

(migrate-from-filestorage-step-7-start-and-verify)=

## Step 7: start and verify

Start the Plone site:

```shell
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

```shell
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

```{seealso}
{doc}`quickstart-docker` explores SQL queryability and S3 blob tiering.
{doc}`/reference/configuration` lists every configuration option.
```
