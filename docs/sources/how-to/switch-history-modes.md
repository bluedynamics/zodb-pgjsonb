<!-- diataxis: how-to -->

# How to switch between history modes

This guide shows you how to switch between history-free and history-preserving modes, and how to compact history without switching modes.

zodb-pgjsonb supports two history modes:

- **History-free** (default): only the current object state is kept.
  Pack removes unreachable objects.
- **History-preserving**: previous revisions are stored in `object_history`.
  Enables undo and `loadBefore` for historical queries.

## Enable history-preserving mode

Change `history-preserving` to `true` in your `zope.conf`:

```xml
<pgjsonb>
    dsn dbname=zodb user=zodb host=localhost port=5432
    history-preserving true
</pgjsonb>
```

Restart your application.
The storage creates the `object_history` and `pack_state` tables automatically on startup.
Existing objects gain history tracking on their next modification.

## Disable history-preserving mode

```{warning}
Converting to history-free mode is **irreversible**.
All undo history is permanently deleted.
```

1. Stop all application instances.
2. Run the conversion utility:

   ```python
   from zodb_pgjsonb.storage import PGJsonbStorage

   storage = PGJsonbStorage(dsn="dbname=zodb user=zodb host=localhost", history_preserving=True)
   counts = storage.convert_to_history_free()
   print(counts)
   storage.close()
   ```

   The method drops `object_history` and `pack_state`, removes old blob versions (keeps only the latest per object), and cleans orphaned transaction log entries.
   It returns a dict with removal counts:

   ```python
   {
       "history_rows": 1234,
       "pack_rows": 56,
       "blob_history_rows": 0,
       "old_blob_versions": 78,
       "orphan_transactions": 90,
   }
   ```

3. Change `history-preserving` to `false` in your `zope.conf`:

   ```xml
   <pgjsonb>
       dsn dbname=zodb user=zodb host=localhost port=5432
       history-preserving false
   </pgjsonb>
   ```

4. Restart your application.

## Compact history without switching modes

If you upgraded from an older version that used dual-write mode (writing the same data to both `object_state` and `object_history`), run `compact_history()` to remove the duplicate entries and reclaim disk space:

```python
from zodb_pgjsonb.storage import PGJsonbStorage

storage = PGJsonbStorage(dsn="dbname=zodb user=zodb host=localhost", history_preserving=True)
deleted_objects, deleted_blobs = storage.compact_history()
print(f"Removed {deleted_objects} object_history rows, {deleted_blobs} blob_history rows")
storage.close()
```

This is an optional maintenance operation.
The storage handles the overlap correctly at runtime, so compaction only saves disk space.
