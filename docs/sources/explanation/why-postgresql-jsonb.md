<!-- diataxis: explanation -->

# Why PostgreSQL JSONB

ZODB has stored object state as Python pickles since its creation in the late 1990s.
This works, but it makes the data opaque to everything except Python code that knows how to unpickle it.
zodb-pgjsonb changes the storage format to PostgreSQL JSONB, making ZODB data queryable with standard SQL while preserving full backward compatibility with ZODB's Python API.

This page explains the problem with opaque storage, how JSONB solves it, what trade-offs are involved, and when you should choose zodb-pgjsonb over the alternatives.

## The problem: opaque pickle storage

ZODB's traditional storage backends -- FileStorage, RelStorage, and ZEO -- all store object state as pickle bytes.
From the database's perspective, each object is an opaque blob of binary data.
You cannot query it, index it, or inspect it without loading it into a Python process and unpickling it.

This has several consequences:

- **No SQL queryability.** You cannot run `SELECT` queries against ZODB data.
Reporting, analytics, and debugging require loading objects through the full ZODB/Zope stack.
- **No cross-tool access.** Standard database tools -- pgAdmin, Grafana, Metabase, custom scripts -- cannot read the data directly.
Every integration must go through Python.
- **Pack requires object loading.** Garbage collection must load and unpickle every object in the database to discover references via `referencesf()`.
On large databases, this is extremely slow.
- **Security risk.** Pickle deserialization can execute arbitrary Python code.
A crafted pickle payload stored in the database could compromise any process that loads it.

## The RelStorage approach

RelStorage stores ZODB data in a relational database (PostgreSQL, MySQL, or Oracle), but it stores object state as bytea -- raw pickle bytes in a binary column.
The data is still opaque.
You get the operational benefits of PostgreSQL (backup, replication, monitoring) but none of the queryability.

```sql
-- RelStorage: data is opaque bytea
SELECT zoid, state FROM object_state WHERE zoid = 42;
-- state: \x80049513000000000000008c0b...  (unreadable binary)
```

RelStorage is a mature, well-tested project and a good choice when you need PostgreSQL's operational infrastructure without changing ZODB's fundamental storage model.
But if you want to query your data with SQL, RelStorage cannot help.

## The JSONB approach

zodb-pgjsonb transcodes pickle bytes to JSONB before writing to PostgreSQL and transcodes back to pickle bytes on read.
ZODB sees pickle at its boundaries; PostgreSQL sees queryable JSON.

```sql
-- zodb-pgjsonb: data is queryable JSONB
SELECT zoid, state->>'title' AS title, state->>'portal_type' AS type
FROM object_state
WHERE class_mod LIKE 'plone.app.contenttypes.content%';
```

The transcoding is handled by zodb-json-codec, a Rust library that converts between pickle and JSON with zero Python overhead on the store path.
The entire encode/decode pipeline runs in Rust with the GIL released, processing 100 objects in under 0.2 ms.

### What JSONB enables

**Direct SQL queries.** You can query any ZODB object's state using PostgreSQL's JSONB operators (`->`, `->>`, `@>`, `?`, `?|`).
Class information is extracted into dedicated columns (`class_mod`, `class_name`) and indexed for fast type-based queries.

**GIN and expression indexes.** PostgreSQL can create GIN indexes on JSONB paths for fast containment queries, and expression indexes (such as `idx->>'portal_type'`) for fast equality lookups.
This is what makes plone-pgcatalog possible -- catalog data is stored as additional JSONB columns on the same `object_state` row and queried with standard SQL.

**Standard tooling.** Any tool that speaks SQL can access ZODB data directly: Grafana dashboards, Metabase analytics, custom reporting scripts, database migration tools, or a simple `psql` session.
You do not need a running Zope instance.

**Pure SQL garbage collection.** Object references are extracted at write time into a `refs` BIGINT[] column.
The pack algorithm uses a recursive CTE to traverse the reference graph entirely within PostgreSQL, never loading a single object.
This is 13-28x faster than RelStorage's approach of loading and unpickling every object.
See {doc}`performance` for detailed numbers.

## Security: no code execution attack surface

This deserves its own emphasis because it is often overlooked.

Pickle deserialization is inherently unsafe.
A pickle can contain instructions to import any module and call any function.
A crafted pickle stored in the database can execute arbitrary code when any ZODB client loads it.
Defenses exist (restricted unpicklers, class whitelists), but they add complexity and are difficult to get right.

JSON has no code execution capability.
A JSONB column in PostgreSQL can contain strings, numbers, booleans, arrays, and objects -- nothing else.
There is no mechanism to embed executable code in JSON.
By storing state as JSONB, zodb-pgjsonb eliminates the entire class of pickle deserialization attacks at the storage level.
See {doc}`security` for the full security analysis.

## Trade-offs

### Transcode overhead on reads

Every uncached load requires a JSONB-to-pickle transcode after the SQL SELECT.
This adds roughly 50-60 microseconds per object compared to RelStorage, which returns raw bytea bytes with no post-processing.
In benchmarks, uncached loads are 1.1-1.6x slower than RelStorage.

In practice, this matters less than it appears.
The ZODB object cache handles the vast majority of reads in production -- typically >95%.
Objects evicted from the ZODB object cache are often still in the storage LRU cache, which serves cached pickle bytes without any database round-trip or transcoding.
The uncached load path is exercised primarily during cold starts and cache misses after large invalidations.

### JSONB storage and indexing overhead on writes

JSONB is larger than pickle bytea for the same data (JSON keys are repeated per-object, types are less compact).
PostgreSQL must also maintain JSONB-aware indexes on write, which adds overhead compared to inserting raw bytes.

In practice, single-object writes are actually 1.3-1.6x *faster* than RelStorage because zodb-pgjsonb's simpler 2PC path (direct SQL, no OID/TID tracking tables) more than compensates for the indexing overhead.
Batch writes of 100 objects are roughly on par.

### PostgreSQL JSONB null-byte limitation

PostgreSQL JSONB cannot store the null byte (`\u0000`) in strings.
zodb-pgjsonb handles this with automatic sanitization: strings containing null bytes are replaced with an `@ns` marker containing the base64-encoded original.
The reverse transformation runs on read.
This is transparent to application code but means that raw SQL queries against strings with embedded null bytes must account for the marker format.
In practice, null bytes in ZODB object state are rare.

## When to choose zodb-pgjsonb

**Choose zodb-pgjsonb when:**
- You want to query ZODB data with SQL (reporting, analytics, debugging).
- You are building on the z3blobs ecosystem (plone-pgcatalog, plone-pgthumbor).
- You want faster pack/GC on large databases.
- You want the security benefit of eliminating pickle deserialization at the storage level.
- You need tiered blob storage (PG bytea + S3).

**Choose RelStorage when:**
- You need MySQL or Oracle support (zodb-pgjsonb is PostgreSQL-only).
- You have an existing RelStorage deployment and do not need SQL queryability.
- You prefer the maturity and larger community of a long-established project.

**Choose FileStorage when:**
- You run a single Zope instance and do not need concurrent access.
- You want the simplest possible deployment with no external database.
- Your database is small enough that pack performance is not a concern.

## The ecosystem advantage

zodb-pgjsonb is not just a storage backend -- it is the foundation for an ecosystem of PostgreSQL-native ZODB tools:

- **plone-pgcatalog** replaces ZCatalog's BTree indexes with SQL queries against extra columns on `object_state`, written atomically via the state processor plugin system.
See {doc}`architecture` for how state processors work.
- **plone-pgthumbor** reads blob data directly from the `blob_state` table for Thumbor image processing, without going through ZODB.
- **Direct SQL access** from Grafana, Metabase, or any SQL-capable tool provides visibility into ZODB data that was previously impossible without Python code.

All of these integrations depend on the data being stored as queryable JSONB rather than opaque pickle bytes.
The storage format is the enabling decision that makes the rest possible.
