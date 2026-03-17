<!-- diataxis: how-to -->

# How to query ZODB data with SQL

This guide shows you how to query your ZODB objects directly in PostgreSQL using SQL and JSONB operators.

## Connect to the database

Use psql, pgAdmin, DBeaver, or any PostgreSQL client.
The connection details match the `dsn` in your storage configuration:

```bash
psql "dbname=zodb user=zodb host=localhost port=5432"
```

## The object_state table

All ZODB object data lives in `object_state`:

| Column | Type | Contains |
|---|---|---|
| `zoid` | `BIGINT` | Object ID (primary key) |
| `tid` | `BIGINT` | Transaction ID of last modification |
| `class_mod` | `TEXT` | Python module of the object class |
| `class_name` | `TEXT` | Python class name |
| `state` | `JSONB` | Object state as queryable JSON |
| `state_size` | `INTEGER` | Serialized state size in bytes |
| `refs` | `BIGINT[]` | Referenced object IDs |

## List all object types

```sql
SELECT class_mod || '.' || class_name AS class, count(*)
FROM object_state
GROUP BY 1
ORDER BY 2 DESC;
```

## Query Plone content by type

```sql
SELECT zoid, state->>'title' AS title, state->>'portal_type' AS type
FROM object_state
WHERE class_mod LIKE 'plone.app.contenttypes.content%';
```

## JSONB operators

PostgreSQL provides several operators for querying JSONB data:

| Operator | Description | Example |
|---|---|---|
| `->>` | Extract value as text | `state->>'title'` |
| `->` | Extract value as JSON | `state->'items'` |
| `@>` | Contains (uses GIN index) | `state @> '{"key": "value"}'` |
| `?` | Key exists | `state ? 'title'` |
| `?&` | All keys exist | `state ?& array['title', 'description']` |
| `?\|` | Any key exists | `state ?\| array['title', 'description']` |

### Containment query (GIN-indexed)

```sql
SELECT zoid, state->>'title'
FROM object_state
WHERE state @> '{"portal_type": "Document"}'::jsonb;
```

### Key existence check

```sql
SELECT zoid, class_mod || '.' || class_name AS class
FROM object_state
WHERE state ? 'workflow_history';
```

### Nested JSON access

```sql
SELECT zoid, state->'creators'->>0 AS first_creator
FROM object_state
WHERE class_mod LIKE 'plone.app.contenttypes%';
```

## Search text across objects

```sql
SELECT zoid, class_mod || '.' || class_name AS class, state->>'title' AS title
FROM object_state
WHERE state::text ILIKE '%search term%';
```

For better performance on large databases, create a GIN trigram index or use full-text search via plone.pgcatalog.

## Join with blob_state for blob metadata

The `blob_state` table stores blob data alongside object references:

```sql
SELECT os.zoid,
       os.state->>'title' AS title,
       bs.blob_size,
       CASE WHEN bs.s3_key IS NOT NULL THEN 's3' ELSE 'pg' END AS storage_tier
FROM object_state os
JOIN blob_state bs ON bs.zoid = os.zoid
ORDER BY bs.blob_size DESC
LIMIT 20;
```

### Blob size distribution

```sql
SELECT
    CASE
        WHEN blob_size < 102400 THEN '< 100 KB'
        WHEN blob_size < 1048576 THEN '100 KB - 1 MB'
        WHEN blob_size < 10485760 THEN '1 MB - 10 MB'
        ELSE '> 10 MB'
    END AS size_range,
    count(*) AS blob_count,
    pg_size_pretty(sum(blob_size)) AS total_size
FROM blob_state
GROUP BY 1
ORDER BY min(blob_size);
```

## Connect Grafana or Metabase

Point Grafana, Metabase, or any BI tool at your PostgreSQL database using the same connection string.
All ZODB data is available as standard SQL tables with JSONB columns.

Example Grafana query for object creation over time:

```sql
SELECT
    date_trunc('day', to_timestamp(tid::double precision / 2^32 / 2^32)) AS time,
    count(*) AS objects
FROM object_state
GROUP BY 1
ORDER BY 1;
```

## Important: read-only

Never modify `object_state`, `transaction_log`, or `blob_state` directly via SQL.
All writes must go through the ZODB transaction lifecycle.
Direct SQL updates will cause data corruption or be overwritten on the next ZODB transaction.
