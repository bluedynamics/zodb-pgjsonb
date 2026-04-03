# zodb-pgjsonb

<!-- diataxis: landing -->

```{image} _static/logo-400.png
:alt: zodb-pgjsonb logo
:width: 200px
:align: center
```

ZODB storage adapter for PostgreSQL using JSONB, powered by [zodb-json-codec](https://github.com/bluedynamics/zodb-json-codec).

Stores ZODB object state as queryable PostgreSQL JSONB instead of opaque pickle bytea blobs.
ZODB sees pickle bytes at its boundaries; PostgreSQL sees queryable JSON internally.

**Key capabilities:**

- SQL queryability -- query ZODB objects directly with PostgreSQL JSONB operators and GIN indexes
- Transparent transcoding via Rust-based codec (2x faster than CPython pickle)
- Tiered blob storage -- small blobs in PG bytea, large blobs in S3 (configurable threshold)
- Full ZODB compatibility -- IStorage, IMVCCStorage, IBlobStorage, IStorageUndoable, IStorageIteration
- Configurable history modes (history-free or history-preserving with undo)
- State processor plugins for writing extra columns atomically alongside object state
- Pure SQL pack/GC via pre-extracted refs column (15-28x faster than RelStorage)
- Instant cache invalidation via PostgreSQL LISTEN/NOTIFY

**Requirements:** Python 3.12+, PostgreSQL 15+ (tested with 17), [zodb-json-codec](https://pypi.org/project/zodb-json-codec/)

## Documentation

::::{grid} 2
:gutter: 3

:::{grid-item-card} Tutorials
:link: tutorials/index
:link-type: doc

**Learning-oriented** -- Step-by-step lessons to build skills.

*Start here if you are new to zodb-pgjsonb.*
:::

:::{grid-item-card} How-To Guides
:link: how-to/index
:link-type: doc

**Goal-oriented** -- Solutions to specific problems.

*Use these when you need to accomplish something.*
:::

:::{grid-item-card} Reference
:link: reference/index
:link-type: doc

**Information-oriented** -- Configuration tables and API details.

*Consult when you need detailed information.*
:::

:::{grid-item-card} Explanation
:link: explanation/index
:link-type: doc

**Understanding-oriented** -- Architecture and design decisions.

*Read to deepen your understanding of how it works.*
:::

::::

## Quick start

1. {doc}`Install zodb-pgjsonb <how-to/install>`
2. {doc}`Run the Docker quickstart <tutorials/quickstart-docker>` (Plone + PostgreSQL + MinIO in 5 minutes)
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
