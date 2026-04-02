# Batch Object Loading

**Date:** 2026-04-02
**Status:** Approved
**Issue:** https://github.com/bluedynamics/zodb-pgjsonb/issues/34

## Problem

pgjsonb loads each ZODB object individually via a separate SQL query.
A single page render triggers 3000+ individual `load()` calls. While
each is fast (<1ms), the accumulated roundtrip overhead adds up to
seconds (10.8s measured on /veranstaltungen with cold cache).

## Design

### Part 1: zodb-pgjsonb --- `load_multiple(oids)`

New method on `PGJsonbStorageInstance`:

```python
def load_multiple(self, oids):
    """Load multiple objects in a single query.

    Skips oids already in _load_cache. Fetches the rest via a
    single SELECT WHERE zoid = ANY($1). Caches all results in
    _load_cache and _serial_cache (same as load()).

    Returns dict[oid, (data_bytes, tid_bytes)].
    Missing oids are silently omitted (no POSKeyError).
    """
```

Implementation:

1. Convert oids to zoids, filter out `_load_cache` hits
2. If no misses, return cached results immediately
3. Single query: `SELECT zoid, tid, class_mod, class_name, state
   FROM object_state WHERE zoid = ANY(%s)`
4. For each row: encode via `zodb_json_codec.encode_zodb_record()`,
   store in `_load_cache` and `_serial_cache`
5. Return dict of all requested oids (cached + freshly loaded)

### Part 2: plone-pgcatalog --- Brain prefetch

New method on `CatalogSearchResults`:

```python
def _prefetch_objects(self, start_index, batch_size=100):
    """Prefetch ZODB objects for a batch of brains.

    Warms the storage _load_cache so subsequent getObject() calls
    hit the cache instead of making individual SQL queries.

    Only prefetches `batch_size` brains starting from `start_index`,
    not the entire result set. Safe for large result sets where
    only a page is rendered (e.g. b_size=20 out of 10,000).
    """
```

Implementation:

1. Slice brains from `start_index` to `start_index + batch_size`
2. Collect zoids from the slice, convert to oids via `p64(zoid)`
3. Get the storage instance via `catalog._p_jar._storage`
4. Call `storage.load_multiple(oids)` --- fills storage cache
5. Track prefetched range to avoid re-fetching

Trigger: automatically on `brain.getObject()`. When a brain at
index N calls getObject(), prefetch brains N to N+batch_size.
If index N is already in a prefetched range, skip. Same pattern
as lazy `_load_idx_batch()` but window-based instead of all-at-once.

Configurable via `PGCATALOG_PREFETCH_BATCH` env var (default 100).

### What does NOT change

- ZODB's `Connection.get()` / `_setstate()` --- unchanged
- `storage.load()` --- unchanged, benefits from warm cache
- Brain attribute access for idx fields --- uses existing lazy batch
- The ZODB Connection cache --- `load_multiple` fills the storage
  cache, not the Connection cache. The Connection calls `load()`
  which hits the storage cache.

### Scope

- `load_multiple()` in `storage.py` (zodb-pgjsonb)
- `prefetch_objects()` in `brain.py` (plone-pgcatalog)
- Trigger wiring in `brain.py` `getObject()` override
- Tests for both

### Expected impact

| Scenario | Before | After |
|----------|--------|-------|
| Collection (20 of 10k shown) | 20 roundtrips | 1 batch(100) + 20 cache hits |
| Collection (50 items total) | 50 roundtrips | 1 batch(100) + 50 cache hits |
| /veranstaltungen (3000 loads) | 3000 roundtrips | 30 batches(100) + rest from ZODB cache |
| Second page load (warm cache) | ZODB cache hits | Same (no change) |

## Implementation steps

1. Add `load_multiple()` to `PGJsonbStorageInstance` in zodb-pgjsonb
2. Tests for load_multiple (batch, cache hits, missing oids)
3. Add `prefetch_objects()` to `CatalogSearchResults` in plone-pgcatalog
4. Wire trigger into `PGCatalogBrain.getObject()`
5. Tests for prefetch (batch trigger, cache warming)
6. Changelog + docs for both packages
