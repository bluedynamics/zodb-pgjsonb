<!-- diataxis: how-to -->

# How to write a state processor plugin

This guide shows you how to write a state processor that adds custom columns to the `object_state` table, written atomically alongside ZODB object state.

## Define the processor class

A state processor must implement two methods:

- `get_extra_columns()` -- return a list of `ExtraColumn` declarations.
- `process(zoid, class_mod, class_name, state)` -- extract data and return a dict of column values, or `None` if no extra data applies.

Optionally implement:

- `get_schema_sql()` -- return DDL to apply at registration time (for example, `ALTER TABLE` or `CREATE INDEX` statements).

```python
from zodb_pgjsonb import ExtraColumn


class MyProcessor:

    def get_extra_columns(self):
        return [
            ExtraColumn(
                name="portal_type",
                value_expr="%(portal_type)s",
            ),
            ExtraColumn(
                name="title_tsvector",
                value_expr="to_tsvector('simple'::regconfig, %(title_tsvector)s)",
            ),
        ]

    def get_schema_sql(self):
        return """
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS portal_type TEXT;
ALTER TABLE object_state ADD COLUMN IF NOT EXISTS title_tsvector TSVECTOR;
CREATE INDEX IF NOT EXISTS idx_portal_type ON object_state (portal_type);
CREATE INDEX IF NOT EXISTS idx_title_tsvector ON object_state USING gin (title_tsvector);
"""

    def process(self, zoid, class_mod, class_name, state):
        if not isinstance(state, dict):
            return None
        portal_type = state.get("portal_type")
        title = state.get("title", "")
        if portal_type is None:
            return None
        return {
            "portal_type": portal_type,
            "title_tsvector": title,
        }
```

## Declare extra columns with ExtraColumn

Each `ExtraColumn` describes a PostgreSQL column that the storage writes during `tpc_vote`:

```python
ExtraColumn(name, value_expr, update_expr=None)
```

- **name**: column name (must be a valid SQL identifier: letters, digits, underscores).
- **value_expr**: SQL expression for INSERT, using `%(name)s` as the parameter placeholder.
  Use raw `%(name)s` for simple values or wrap in SQL functions like `to_tsvector('simple'::regconfig, %(name)s)`.
- **update_expr**: optional expression for ON CONFLICT UPDATE.
  Defaults to `EXCLUDED.{name}` when set to `None`.

## Implement the process method

The `process` method receives four arguments:

- **zoid**: integer object ID.
- **class_mod**: Python module of the object class (for example, `plone.app.contenttypes.content`).
- **class_name**: Python class name (for example, `Document`).
- **state**: the decoded JSON state as a Python dict.

Return a dict mapping column names to values, or return `None` to skip writing extra columns for this object.
The `process` method may modify `state` in place (for example, to pop annotation keys that should not be stored in the JSONB column).

## Provide optional DDL via get_schema_sql

Return a SQL string with `ALTER TABLE`, `CREATE INDEX`, or other DDL statements.
The storage applies this DDL when the processor is registered, using a separate autocommit connection with a short lock timeout.
If the DDL is blocked by concurrent read transactions at startup, it is deferred and applied on the first write transaction.

Use `IF NOT EXISTS` clauses to make the DDL idempotent:

```python
def get_schema_sql(self):
    return "ALTER TABLE object_state ADD COLUMN IF NOT EXISTS my_col TEXT;"
```

## Register the processor with the storage

Call `register_state_processor()` on the main `PGJsonbStorage` instance:

```python
from zodb_pgjsonb.storage import PGJsonbStorage

storage = PGJsonbStorage(dsn="dbname=zodb user=zodb host=localhost")
storage.register_state_processor(MyProcessor())
```

In a Zope/Plone application, register the processor in an `IDatabaseOpenedWithRoot` subscriber so it runs after the storage is initialized.

## Security considerations

```{warning}
State processors run with full SQL injection capability.
`value_expr` and `update_expr` are interpolated directly into INSERT statements without escaping.
Only register processors from trusted, audited code.
A compromised processor can read, modify, or delete any data in the database.
```

Column names are validated against a strict SQL identifier pattern.
SQL expressions in `value_expr` and `update_expr` are not validated because they may legitimately contain function calls and casts.
