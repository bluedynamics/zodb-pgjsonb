# CLAUDE.md — Agent Notes for zodb-pgjsonb

## What This Is

ZODB storage adapter that stores object state as PostgreSQL JSONB instead of
pickle bytea. Uses [zodb-json-codec](https://github.com/bluedynamics/zodb-json-codec)
(Rust/PyO3) for pickle-to-JSON transcoding.

## Further Reading

- Full documentation: https://bluedynamics.github.io/zodb-pgjsonb/
- LLM-optimized context: `docs/sources/llms.txt` (architecture, schema, API, config — all in one file)

## Build & Test

```bash
# Install dependencies (always use uv, never pip)
uv sync --all-extras

# Run tests (requires PostgreSQL on localhost:5433)
pytest

# Code quality
ruff check .
ruff format .
```

### Test Database

Tests use `dbname=zodb_test` on `localhost:5433`. Start with:

```bash
docker run -d --name zodb-pgjsonb-dev \
  -e POSTGRES_USER=zodb -e POSTGRES_PASSWORD=zodb -e POSTGRES_DB=zodb \
  -p 5433:5432 postgres:17
```

## Architecture

- `storage.py` — `PGJsonbStorage` (main, IMVCCStorage) + `PGJsonbStorageInstance` (per-connection)
- `schema.py` — DDL for `object_state`, `object_history`, `transaction_log`, `blob_data`
- `packer.py` — SQL-based pack (history-free and history-preserving modes)
- `config.py` — ZConfig factory (`%import zodb_pgjsonb` + `<pgjsonb>` section)
- `component.xml` — ZConfig schema definition
- `interfaces.py` — IStateProcessor interface

## Key Design Decisions

- MVCC via REPEATABLE READ snapshots (poll_invalidations starts snapshot before reads)
- TID generation uses PostgreSQL advisory locks for serialization
- Blobs stored as PG bytea with optional S3 tiering for large blobs
- State processor plugin system allows extra columns written atomically with object state
- All data is pickle at ZODB boundaries — JSON is internal storage format only
- PG JSONB cannot store `\u0000` — sanitized with `@ns` marker (base64)
