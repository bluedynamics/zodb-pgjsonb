<!-- diataxis: how-to -->

# How to install zodb-pgjsonb

This guide shows you how to install zodb-pgjsonb and connect it to a PostgreSQL database.

## Install the package

```bash
pip install zodb-pgjsonb
# or
uv pip install zodb-pgjsonb
```

Install the optional S3 blob tiering extra if you plan to store large blobs in S3:

```bash
pip install zodb-pgjsonb[s3]
```

Requirements: Python 3.12+, PostgreSQL 15+ (tested with 17).

## Set up PostgreSQL

### Development (Docker)

Start a PostgreSQL 17 container:

```bash
docker run -d --name zodb-pg \
  -e POSTGRES_USER=zodb \
  -e POSTGRES_PASSWORD=zodb \
  -e POSTGRES_DB=zodb \
  -p 5432:5432 \
  postgres:17
```

### Production

Use any PostgreSQL 15+ instance.
The storage creates all required tables and indexes automatically on first startup.

:::{note}
The SQL features used (JSONB, `ON CONFLICT DO UPDATE`, recursive CTEs, advisory locks)
only require PostgreSQL 9.5+. However, we only test with PostgreSQL 17 and recommend
15+ to stay within PostgreSQL's supported version window.
:::

## Configure for Zope or Plone (ZConfig)

Add a `<pgjsonb>` section to your `zope.conf`:

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

The `dsn` accepts both key-value format (`dbname=zodb host=localhost`) and URI format (`postgresql://user:pass@host/db`).

## Configure for standalone ZODB (Python API)

Create the storage directly in Python:

```python
from zodb_pgjsonb import PGJsonbStorage
import ZODB
import transaction

storage = PGJsonbStorage(dsn="dbname=zodb user=zodb host=localhost")
db = ZODB.DB(storage)
conn = db.open()
root = conn.root()

root["hello"] = "world"
transaction.commit()

conn.close()
db.close()
```

## Verify the connection

Connect to PostgreSQL and confirm the tables exist:

```bash
psql "dbname=zodb user=zodb host=localhost"
```

```sql
SELECT count(*) FROM object_state;
SELECT count(*) FROM transaction_log;
```

Both queries should return `0` for a fresh database (or the number of objects if you already committed data).
