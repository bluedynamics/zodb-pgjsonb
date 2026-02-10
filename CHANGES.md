# Changelog

## Unreleased

### Added

- **State processor plugin system**: Register processors that extract extra
  column data from object state during writes. This enables downstream
  packages (e.g. plone-pgcatalog) to write supplementary columns alongside
  the object state in a single atomic `INSERT...ON CONFLICT` statement.

  New public API:

  - `ExtraColumn(name, value_expr, update_expr=None)` dataclass — declares
    an extra column for `object_state`.
  - `PGJsonbStorage.register_state_processor(processor)` — registers a
    processor whose `process(zoid, class_mod, class_name, state)` method
    can pop keys from the state dict and return extra column data.

  Processors are called in `store()` after pickle-to-JSON decoding.
  Extra columns are included in the pipelined `executemany()` batch write
  during `tpc_vote()`, keeping everything in the same PostgreSQL
  transaction for full atomicity.

- **State processor DDL via `get_schema_sql()`**: Processors can now
  optionally provide a `get_schema_sql()` method returning DDL statements
  (e.g. `ALTER TABLE`, `CREATE INDEX`). The DDL is applied using the
  storage's own connection during `register_state_processor()`, avoiding
  REPEATABLE READ lock conflicts with pool connections.

### Optimized

- **Batch conflict detection**: Conflict checks are now batched into a
  single `SELECT ... WHERE zoid = ANY(...)` query in `tpc_vote()` instead
  of individual per-object queries in `store()`. Eliminates N-1 SQL
  round trips per transaction while holding the advisory lock.

- **Prepared statements**: Added `prepare=True` to hot-path queries
  (`load`, `loadSerial`, `loadBefore`) for faster repeated execution.
