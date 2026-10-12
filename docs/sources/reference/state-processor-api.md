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
  The return value decides what happens to this processor's columns for the object:

  - `None`: the columns are left untouched (a new row gets the column defaults).
  - A dict of `{column_name: value}`: all of this processor's columns are written; columns missing from the dict become `NULL`.
  - `{}`: all of this processor's columns are set to `NULL`.

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

`undone(cursor, tid, restored, deleted, entries) -> None`
: Called from `undo()` in history-preserving mode, once per undone transaction.
  Use it when your columns do not derive from the object state alone
  (undo re-runs `process()` on the restored state, which is enough for processors whose columns do).
  `tid` is the undone transaction id as an `int`.
  `restored` is a list of `int` zoids whose previous revision will be written;
  `deleted` is a list of `int` zoids whose rows will be deleted (created in the undone transaction).
  `entries` maps every zoid of both lists to `{"action": "restore" | "delete", "before": rev, "after": rev | None}`:
  `before` is the revision the undone transaction wrote, `after` the revision the undo writes (`None` for a delete).
  Each `rev` is a dict with `class_mod`, `class_name`, `state` (decoded JSON) and `refs` (list of `int` zoids).
  Compare `before` and `after` to see what the undone transaction changed; do not read `object_state.state` for that,
  because with several transactions undone together the row may still hold another transaction's revision.
  The hook runs before anything of the undo is queued or written, on the cursor of the PostgreSQL transaction that will carry the undo (the advisory write lock is held).
  `object_state` therefore still shows the pre-undo rows, including those of `deleted` zoids and the current `path`-like data of `restored` ones.
  SQL executed here is atomic with the undo and is rolled back with it.
  An exception aborts the undo; raise `UndoError` if the processor cannot undo safely.
  With several processors, each is called with the same cursor and sees the SQL of the ones before it; processors without the method are skipped.

  Only zoids written by the undone transaction are reported.
  Rows that its `finalize()` changed without writing them are not,
  so a processor that changes unwritten rows in `finalize()` has to find them itself.

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
)

ExtraColumn(
    name="first_seen",
    value_expr="%(first_seen)s",
    # Keep the first stored value on later writes.
    update_expr="COALESCE(object_state.first_seen, EXCLUDED.first_seen)",
)
```

The `name` field is validated against the pattern `^[a-zA-Z_][a-zA-Z0-9_]*$`.
A `ValueError` is raised if the name does not match.

In the `ON CONFLICT ... SET` clause, `EXCLUDED.{name}` holds the value already computed by `value_expr`; `object_state.{name}` is the stored value.

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
a ZODB write transaction (the undo step only applies to undo transactions):

1. **Registration** (`register_state_processor()`):
   `get_extra_columns()` and `get_schema_sql()` are called.
   DDL is applied (or deferred).

2. **Store** (`store()` / `restore()`):
   `process(zoid, class_mod, class_name, state)` is called for each
   object after pickle-to-JSON decoding.
   The returned dict is attached to the object entry.

3. **Undo** (`undo()`, history-preserving mode only, instead of Store):
   after the undo data is computed and before it is queued, `undone(cursor, tid, restored, deleted, entries)` is called once per undone tid on each processor that implements it.
   The undo is then queued, with `process(...)` re-run on each restored state, and written by the following Vote.

4. **Vote** (`tpc_vote()`):
   `get_extra_columns()` is called to collect column definitions.
   Objects are grouped by the set of columns to write (the columns of every processor that answered)
   and written with one batched `executemany()` per group.
   A processor that returned `None` has its columns left out of the statement, so their stored values stay.
   After all writes, `finalize(cursor)` is called on each processor
   that implements it.

5. **Finish** (`tpc_finish()`):
   The PostgreSQL transaction is committed.
   No processor methods are called.

6. **Abort** (`tpc_abort()`):
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
