<!-- diataxis: reference -->

# State processor API

State processors are plugins that extract extra column data from object
state during writes.
They allow downstream packages (for example, `plone-pgcatalog`) to write
supplementary columns alongside the object state in a single atomic
`INSERT ... ON CONFLICT` statement.

## Protocol

State processors use duck typing.
A processor must implement the following methods:

### Required methods

`get_extra_columns() -> list[ExtraColumn]`
: Return the list of extra columns this processor writes.
  Called once during registration and during each `tpc_vote()`.

`process(zoid: int, class_mod: str, class_name: str, state: str) -> dict | None`
: Extract extra column data from an object's state.
  `state` is a JSON string (the decoded object state, pre-sanitization).
  The method may modify `state` in-place (for example, pop annotation keys
  to prevent them from being persisted).
  Returns a dict of `{column_name: value}` for extra columns, or `None`
  when no extra data applies to this object.
  Called during `store()` after pickle-to-JSON decoding for every object
  in the transaction.

### Optional methods

`get_schema_sql() -> str | None`
: Return DDL statements to apply (for example, `ALTER TABLE ... ADD COLUMN`,
  `CREATE INDEX`).
  Called once during `register_state_processor()`.
  The DDL is executed via a separate autocommit connection.
  If blocked by startup read transactions (lock conflict), the DDL
  is deferred to the first `tpc_begin()`.
  Returns `None` if no DDL is needed.

`finalize(cursor) -> None`
: Called at the end of `tpc_vote()`, after all objects have been written
  but before the transaction commits.
  The cursor belongs to the same PostgreSQL transaction as the object
  writes, so any additional SQL executed here is atomic with the
  object state.
  This hook is useful for operations that depend on the full set of
  written objects (for example, JSONB merges or aggregation queries).

## ExtraColumn

```python
from zodb_pgjsonb import ExtraColumn
```

A dataclass that declares an extra column for the `object_state` table.

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | `str` | -- | PostgreSQL column name. Must be a valid SQL identifier (letters, digits, underscores; must start with a letter or underscore). Validated on construction. |
| `value_expr` | `str` | -- | SQL value expression for the `INSERT` statement. Supports `%(name)s` parameter placeholders. For example: `"%(path)s"` or `"to_tsvector('simple'::regconfig, %(searchable_text)s)"`. |
| `update_expr` | `str \| None` | `None` | SQL expression for the `ON CONFLICT ... SET` clause. Defaults to `EXCLUDED.{name}` when `None`. |

### Construction

```python
ExtraColumn(
    name="path",
    value_expr="%(path)s",
)

ExtraColumn(
    name="searchable_text",
    value_expr="to_tsvector('simple'::regconfig, %(searchable_text)s)",
    update_expr="to_tsvector('simple'::regconfig, EXCLUDED.searchable_text)",
)
```

The `name` field is validated against the pattern `^[a-zA-Z_][a-zA-Z0-9_]*$`.
A `ValueError` is raised if the name does not match.

### Security

`value_expr` and `update_expr` are interpolated directly into SQL
`INSERT` statements without escaping.
Only register processors from trusted, audited code.
A compromised processor can read, modify, or delete any data in the
database.
Column names are validated against a strict identifier pattern;
expressions are not validated because they may legitimately contain
SQL function calls.

## Lifecycle

The following sequence describes when each method is called during
a ZODB write transaction:

1. **Registration** (`register_state_processor()`):
   `get_extra_columns()` and `get_schema_sql()` are called.
   DDL is applied (or deferred).

2. **Store** (`store()` / `restore()`):
   `process(zoid, class_mod, class_name, state)` is called for each
   object after pickle-to-JSON decoding.
   The returned dict is attached to the object entry.

3. **Vote** (`tpc_vote()`):
   `get_extra_columns()` is called to collect column definitions.
   All objects (with extra column data) are written in a batched
   `executemany()` call.
   After all writes, `finalize(cursor)` is called on each processor
   that implements it.

4. **Finish** (`tpc_finish()`):
   The PostgreSQL transaction is committed.
   No processor methods are called.

5. **Abort** (`tpc_abort()`):
   The PostgreSQL transaction is rolled back.
   No processor methods are called.

## DDL application

DDL from `get_schema_sql()` is applied using a dedicated autocommit
connection (not the pool connection).
This avoids conflicts with `REPEATABLE READ` snapshots held by pool
connections.

At Zope startup, IDatabaseOpenedWithRoot subscribers fire while a
ZODB Connection still holds an `ACCESS SHARE` lock via its
`REPEATABLE READ` snapshot.
`ALTER TABLE` requires `ACCESS EXCLUSIVE`, which would deadlock.

The storage handles this by setting `lock_timeout = '2s'` on the DDL
connection.
If the lock cannot be acquired, the DDL is stored in a pending queue
and applied at the next `tpc_begin()` (after the read transaction
has been committed and the `ACCESS SHARE` lock released).
