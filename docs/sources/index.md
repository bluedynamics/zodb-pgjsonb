---
description: "ZODB storage for PostgreSQL that stores object state as queryable JSONB, with tiered blob storage and SQL access to every object."
---

# zodb-pgjsonb

<!-- diataxis: landing -->

```{image} _static/logo-400.png
:alt: zodb-pgjsonb logo
:width: 200px
:align: center
```

ZODB storage adapter for PostgreSQL using JSONB, powered by [zodb-json-codec](https://github.com/bluedynamics/zodb-json-codec).

Stores ZODB object state as queryable PostgreSQL JSONB instead of opaque pickle bytea blobs.
ZODB sees pickle bytes at its boundaries.
PostgreSQL sees queryable JSON internally.

**Key capabilities:**

- SQL queryability: query ZODB objects directly with PostgreSQL JSONB operators
- Transparent transcoding via Rust-based codec (2x faster than CPython pickle)
- Tiered blob storage: small blobs in PG bytea, large blobs in S3 (configurable threshold)
- Full ZODB compatibility: IStorage, IMVCCStorage, IBlobStorage, IStorageUndoable, IStorageIteration
- Configurable history modes (history-free or history-preserving with undo)
- State processor plugins for writing extra columns atomically alongside object state
- Pure SQL pack and garbage collection through the pre-extracted refs column
- Instant cache invalidation via PostgreSQL LISTEN/NOTIFY

**Requirements:** Python 3.12+, PostgreSQL 15+ (tested with 17), [zodb-json-codec](https://pypi.org/project/zodb-json-codec/)

(index-documentation)=

## Documentation

::::{grid} 2
:gutter: 3

:::{grid-item-card} Tutorials
:link: tutorials/index
:link-type: doc

Step-by-step lessons for readers new to zodb-pgjsonb.
:::

:::{grid-item-card} How-to guides
:link: how-to/index
:link-type: doc

Solutions to specific problems for readers who know the basics.
:::

:::{grid-item-card} Reference
:link: reference/index
:link-type: doc

Configuration tables, schema and API details.
:::

:::{grid-item-card} Explanation
:link: explanation/index
:link-type: doc

Architecture, caching, performance and design decisions.
:::

::::

(index-quick-start)=

## Quick start

1. {doc}`Install zodb-pgjsonb <how-to/install>`
2. {doc}`Run the Docker quickstart <tutorials/quickstart-docker>` (Plone + PostgreSQL + Garage in 10 minutes)
3. {doc}`Migrate an existing site <tutorials/migrate-from-filestorage>`

```{toctree}
---
maxdepth: 3
caption: Documentation
titlesonly: true
hidden: true
---
ecosystem
tutorials/index
how-to/index
reference/index
explanation/index
```
