---
myst:
  html_meta:
    "description": "Guides for installing, configuring, migrating to and operating zodb-pgjsonb."
    "property=og:description": "Guides for installing, configuring, migrating to and operating zodb-pgjsonb."
    "property=og:title": "How-to guides"
    "keywords": "ZODB, PostgreSQL, JSONB, Plone, zodb-pgjsonb, how-to"
---

# How-to guides

<!-- diataxis: how-to -->

Goal-oriented guides that show you how to solve specific problems.
Each guide assumes you already have a working PostgreSQL instance and a Python environment.

(index-setup-and-operations)=

## Setup and operations

```{toctree}
---
maxdepth: 2
titlesonly: true
---
install
configure-s3-blobs
switch-history-modes
deploy-production
```

(index-migration)=

## Migration

```{toctree}
---
maxdepth: 2
titlesonly: true
---
migrate-with-zodbconvert
```

(index-development)=

## Development

```{toctree}
---
maxdepth: 2
titlesonly: true
---
query-with-sql
write-state-processor
```
