<!-- diataxis: tutorial -->

# Quickstart with Docker

## What we will build

In this tutorial we will run a Plone 6 site whose entire ZODB lives in PostgreSQL JSONB, with large blobs tiered to MinIO (S3-compatible storage).
We will create content in Plone, then query it directly with SQL using `psql`.

By the end we will have a working Plone instance backed by zodb-pgjsonb and the ability to inspect every ZODB object as queryable JSON.

## Prerequisites

- Docker and Docker Compose v2+
- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

## Step 1: clone the repository

```bash
git clone https://github.com/bluedynamics/zodb-pgjsonb.git
cd zodb-pgjsonb/example
```

All remaining commands assume we are inside the `example/` directory.

## Step 2: start the infrastructure

```bash
docker compose up -d
```

This starts three services:

| Service    | Port | Purpose                     | Credentials             |
|------------|------|-----------------------------|-------------------------|
| PostgreSQL | 5433 | ZODB object storage (JSONB) | user=zodb password=zodb |
| MinIO API  | 9000 | S3-compatible blob storage  | minioadmin / minioadmin |
| MinIO UI   | 9001 | Web console for blobs       | minioadmin / minioadmin |

A one-shot container creates the `zodb-blobs` bucket automatically.

Wait a few seconds, then verify that PostgreSQL is healthy:

```bash
docker compose ps
```

We should see all services in a `healthy` or `exited (0)` state:

```text
NAME                  STATUS
example-postgres-1    Up (healthy)
example-minio-1       Up (healthy)
example-createbucket  Exited (0)
```

## Step 3: install Python dependencies

```bash
cd ..
uv venv -p 3.13
source .venv/bin/activate
uv pip install -r example/requirements.txt
```

This installs Plone 6, zodb-pgjsonb with S3 support, and all dependencies.
The `example/constraints.txt` file pins known-good versions from the Plone 6.1 release line.

## Step 4: generate a Zope instance

```bash
uvx cookiecutter -f --no-input --config-file /dev/null \
    gh:plone/cookiecutter-zope-instance \
    target=instance \
    wsgi_listen=0.0.0.0:8081 \
    initial_user_name=admin \
    initial_user_password=admin
```

Now copy the example configuration files into the instance:

```bash
cp example/zope.conf instance/etc/zope.conf
cp example/zope.ini instance/etc/zope.ini
cp example/site.zcml instance/etc/site.zcml
mkdir -p instance/var/blobtemp instance/var/blobcache
```

The `zope.conf` file configures the `<pgjsonb>` storage section:

```xml
%import zodb_pgjsonb

<zodb_db main>
    mount-point /
    cache-size 30000
    <pgjsonb>
        dsn dbname=zodb user=zodb password=zodb host=localhost port=5433
        blob-temp-dir ./instance/var/blobtemp
        blob-cache-dir ./instance/var/blobcache

        # S3 tiered blob storage (MinIO)
        s3-bucket-name zodb-blobs
        s3-endpoint-url http://localhost:9000
        s3-access-key minioadmin
        s3-secret-key minioadmin
        s3-use-ssl false
        blob-threshold 100KB
    </pgjsonb>
</zodb_db>
```

Blobs smaller than 100 KB stay in PostgreSQL as `bytea`.
Blobs larger than 100 KB are uploaded to MinIO.

## Step 5: start Zope

```bash
.venv/bin/runwsgi instance/etc/zope.ini
```

We should see log output ending with:

```text
INFO  [waitress:486] Serving on http://0.0.0.0:8081
```

## Step 6: create a Plone site

Open <http://localhost:8081> in a browser.
Log in with **admin / admin**.
Click **Create a new Plone site** and accept the defaults.
Click **Create Plone Site**.

After a few seconds, we land on the new Plone site at <http://localhost:8081/Plone>.

## Step 7: add some content

Let's create a test page so we have something to query.

1. Click **Add new** and select **Page**.
2. Set the title to `Hello PostgreSQL` and add some body text.
3. Click **Save**.
4. In the toolbar, click **State: Private** and select **Publish**.

Now upload an image to exercise blob storage:

1. Click **Add new** and select **Image**.
2. Set the title to `Test Image` and upload any image file larger than 100 KB.
3. Click **Save**.

## Step 8: query ZODB data with SQL

Open a new terminal and connect to PostgreSQL:

```bash
psql -h localhost -p 5433 -U zodb -d zodb
```

Enter the password `zodb` when prompted.

### List all object types

```sql
SELECT class_mod || '.' || class_name AS class,
       count(*) AS count
FROM object_state
GROUP BY 1
ORDER BY 2 DESC;
```

We should see dozens of rows, including Plone content types, OFS objects, and PersistentMappings.

### Find our page

```sql
SELECT zoid,
       state->>'title' AS title,
       state->>'portal_type' AS type
FROM object_state
WHERE class_mod LIKE 'plone.app.contenttypes.content%'
ORDER BY zoid;
```

We should see our "Hello PostgreSQL" page in the results:

```text
  zoid  |      title       |   type
--------+------------------+----------
    142 | Hello PostgreSQL | Document
    143 | Test Image       | Image
```

### Check blob storage tiers

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

If we uploaded an image larger than 100 KB, we should see at least one row for "S3 (MinIO)".

### Browse blobs in MinIO

Open <http://localhost:9001> and log in with **minioadmin / minioadmin**.
Navigate to the **zodb-blobs** bucket.
We should see the uploaded image stored as an S3 object.

## Step 9: start pgAdmin (optional)

For a graphical SQL interface, start the pgAdmin container:

```bash
docker compose --profile tools up -d
```

Open <http://localhost:5050> and log in with **admin@example.com / admin**.
Add a server connection with these settings:

- **Host**: `postgres`
- **Port**: `5432`
- **Username**: `zodb`
- **Password**: `zodb`
- **Database**: `zodb`

:::{note}
Use `postgres` (the Docker service name) as the host and port `5432` (the internal port), because pgAdmin runs inside the same Docker network.
:::

## Step 10: clean up

Stop Zope with `Ctrl+C` in the terminal where it is running.

Remove all Docker containers and volumes:

```bash
docker compose --profile tools down -v
```

Omit `-v` to keep the data volumes for next time.

## What we learned

- zodb-pgjsonb stores ZODB object state as queryable JSONB in PostgreSQL
- Blobs are tiered between PostgreSQL `bytea` (small) and S3/MinIO (large)
- Every ZODB object is directly queryable with standard SQL and JSONB operators
- The `<pgjsonb>` ZConfig section plugs into any standard Zope/Plone deployment

## Next steps

- {doc}`migrate-from-filestorage` to migrate an existing Plone site from FileStorage
- {doc}`/reference/configuration` for the complete list of configuration options
