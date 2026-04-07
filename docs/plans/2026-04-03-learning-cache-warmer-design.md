# Learning Cache Warmer — Design Spec

**Issue:** https://github.com/bluedynamics/zodb-pgjsonb/issues/48

## Goal

Eliminate cold-cache latency after pod restarts by pre-loading the
most frequently needed ZODB objects into a shared L2 cache.  The system
learns which objects to warm by recording access patterns from previous
startups.

## Architecture

New module `cache_warmer.py` on zodb-pgjsonb with a `CacheWarmer` class
that manages three phases: record, persist, warm.  Hooks into existing
`load()` and `poll_invalidations()` paths with minimal overhead.

## Data flow

```
Startup:
  1. CacheWarmer reads top-N ZOIDs from cache_warm_stats (PG)
  2. Background thread: load_multiple() → populate _warm_cache (L2)
  3. Main thread continues startup immediately (non-blocking)

Request handling:
  4. Instance.load(oid):
     L1 (_load_cache) → hit? return
     L2 (_warm_cache) → hit? copy to L1, return
     PG query → store in L1, return

  5. Instance.poll_invalidations():
     for each changed oid:
       L2.pop(oid)      # invalidate warm cache
       L1.invalidate(oid) # invalidate instance cache

Recording (first N loads after startup):
  6. Instance.load(oid): if warmer.recording → warmer.record(zoid)
  7. After N unique ZOIDs recorded → persist to cache_warm_stats
```

## Module: `cache_warmer.py`

### `CacheWarmer`

```python
class CacheWarmer:
    def __init__(self, conn, warm_pct=10, decay=0.8, flush_interval=1000):
        self.recording = True
        self._warming_done = False
        self._recorded = set()
        self._pending = set()       # unflushed OIDs
        self._target_count = 0      # set from cache size * warm_pct / 100
        self._flush_interval = flush_interval
        self._decayed = False       # decay applied once per startup
        self._warm_cache = {}       # zoid → (pickle_bytes, tid_bytes)
        self._decay = decay
        self._conn = conn           # main storage PG connection

    def record(self, zoid):
        """Called from Instance.load() during recording phase.

        Flushes to PG every _flush_interval OIDs to limit data loss
        if the pod is killed.  Decay is applied only on the first flush.
        """
        if zoid in self._recorded:
            return
        self._recorded.add(zoid)
        self._pending.add(zoid)
        if len(self._pending) >= self._flush_interval:
            self._flush(decay=not self._decayed)
            self._decayed = True
        if len(self._recorded) >= self._target_count:
            self.recording = False
            if self._pending:
                self._flush(decay=not self._decayed)

    def _flush(self, decay=False):
        """Write pending OIDs to cache_warm_stats.  Called every N OIDs."""

    def _persist(self):
        """Deprecated — replaced by incremental _flush()."""

    def warm(self, load_multiple_fn):
        """Load top-N ZOIDs into _warm_cache.  Runs in background thread.

        Builds a temporary dict, then swaps atomically to avoid race
        conditions with poll_invalidations() during loading.
        """

    def get(self, zoid):
        """L2 lookup.  Returns (data, tid) or None.
        Returns None while warming is in progress (_warming_done=False).
        """

    def invalidate(self, zoid):
        """Remove zoid from warm cache (called by poll_invalidations)."""
```

### L2 warm cache (`_warm_cache`)

- Plain `dict` on the `CacheWarmer` instance (not OrderedDict, no LRU)
- Populated once by `warm()`, never grows after that
- Shrinks via `invalidate()` from `poll_invalidations()`
- Thread-safe reads (dict lookup is atomic in CPython/GIL)
- **Race condition prevention:** the background thread builds results
  into a temporary dict, then does a single atomic swap
  (`self._warm_cache = tmp`).  `get()` returns `None` until
  `_warming_done` is set.  Invalidations during warming go into the
  void (warm_cache is still `{}`).  After the swap, any OID that was
  invalidated during warming may briefly be stale — the next
  `poll_invalidations()` cycle clears it (milliseconds window)

### PG table

```sql
CREATE TABLE IF NOT EXISTS cache_warm_stats (
    zoid   BIGINT PRIMARY KEY,
    score  FLOAT NOT NULL DEFAULT 1.0
);
```

### Flush algorithm (incremental persist)

Recording flushes every `_flush_interval` OIDs (default 1000).
At 7000 target that's ~7 flushes.  Max data loss on pod kill: 1000 OIDs.

```python
def _flush(self, decay=False):
    """Write pending OIDs to PG.  Decay only on first flush per startup."""
    zoids = list(self._pending)
    self._pending.clear()
    with self._conn.cursor() as cur:
        if decay:
            cur.execute(
                "UPDATE cache_warm_stats SET score = score * %(d)s",
                {"d": self._decay},
            )
        cur.execute(
            "INSERT INTO cache_warm_stats (zoid, score) "
            "VALUES (unnest(%(z)s::bigint[]), 1.0) "
            "ON CONFLICT (zoid) DO UPDATE "
            "SET score = cache_warm_stats.score + 1.0",
            {"z": zoids},
        )
        if decay:
            cur.execute("DELETE FROM cache_warm_stats WHERE score < 0.01")
    self._conn.commit()
```

### Warm algorithm

```sql
SELECT zoid FROM cache_warm_stats
ORDER BY score DESC
LIMIT %(limit)s;
```

Then call `load_multiple(oids)` which does one `SELECT ... WHERE zoid = ANY(...)`.
Build results into a temporary dict, then atomic swap:

```python
def warm(self, load_fn):
    top_oids = self._read_top_oids()
    if not top_oids:
        self._warming_done = True
        return
    tmp = {}
    for zoid, (data, tid) in load_fn(top_oids).items():
        tmp[zoid] = (data, tid)
    self._warm_cache = tmp  # atomic swap
    self._warming_done = True
    log.info("Warm cache loaded: %d objects", len(tmp))
```

## Integration points

### `PGJsonbStorage.__init__()` (main storage)

```python
self._warmer = CacheWarmer(self._conn, warm_pct, decay)
# Start background warming thread
threading.Thread(target=self._warmer.warm, args=(self._warm_fn,), daemon=True).start()
```

### `PGJsonbStorageInstance.load()` (per-connection)

```python
# After L1 miss, before PG query:
if self._main._warmer:
    cached = self._main._warmer.get(zoid)
    if cached is not None:
        self._load_cache.set(zoid, *cached)
        return cached

# After PG query, during recording:
if self._main._warmer and self._main._warmer.recording:
    self._main._warmer.record(zoid)
```

### `PGJsonbStorageInstance.poll_invalidations()`

```python
# For each invalidated oid:
if self._main._warmer:
    self._main._warmer.invalidate(zoid)
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `cache_warm_pct` | `10` | % of `_load_cache` max size to warm (0 = disabled) |
| `cache_warm_decay` | `0.8` | Score decay factor per restart cycle |

ZConfig:

```xml
<key name="cache-warm-pct" datatype="integer" default="10" />
<key name="cache-warm-decay" datatype="float" default="0.8" />
```

## Memory budget

At `cache_local_mb=16` and `cache_warm_pct=10`:
- Target: ~7000 OIDs (10% of typical 70000 object cache)
- Average pickle size: ~200-500 bytes
- L2 memory: ~1.4-3.5 MB (one-time, shared across all instances)

## Non-goals

- L2 is not a general-purpose shared cache (no LRU, no growth after warmup)
- No cross-pod coordination (each pod learns independently)
- No eviction policy on L2 (only invalidation from poll_invalidations)

## Testing

- Unit tests for `CacheWarmer.record()`, `_persist()`, `get()`, `invalidate()`
- Integration test: startup → record → restart → verify warm cache populated
- Verify `poll_invalidations` clears stale L2 entries
