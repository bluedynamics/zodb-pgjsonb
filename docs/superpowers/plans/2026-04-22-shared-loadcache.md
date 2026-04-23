# Shared Process-Wide LoadCache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate per-connection duplication of the zodb-pgjsonb LoadCache by introducing a single process-wide shared cache gated by a consensus TID for MVCC correctness (#63).

**Architecture:** Add a `SharedLoadCache` class beside the existing `LoadCache` in `storage.py`. A single instance lives on the main `PGJsonbStorage` as `self._shared_cache`. Each per-instance read path becomes L1 (small, per-connection) → shared (process-wide) → PG. A process-wide `_consensus_tid` field gates reads and writes so an instance with a stale snapshot cannot pollute the cache after another instance's invalidation. The warmer populates the shared cache instead of its own dict.

**Tech Stack:** Python 3.12+ (threading, OrderedDict), psycopg 3, ZODB, existing zodb-pgjsonb.

---

## File structure

| File | Responsibility |
|---|---|
| `src/zodb_pgjsonb/storage.py` | Add `SharedLoadCache` class; instantiate on `PGJsonbStorage.__init__`; new `cache_shared_mb` / `cache_per_connection_mb` params; `cache_local_mb` deprecation alias. |
| `src/zodb_pgjsonb/instance.py` | Read path (`load`, `load_multiple`) cascades L1 → shared → PG; `poll_invalidations` calls `shared.poll_advance`. |
| `src/zodb_pgjsonb/cache_warmer.py` | `warm()` populates shared cache via `shared.set()`; remove `_warm_cache` dict, `get()`, `invalidate()`. |
| `src/zodb_pgjsonb/config.py` | Pass new config knobs to `PGJsonbStorage`. |
| `src/zodb_pgjsonb/component.xml` | ZConfig schema for `cache-shared-mb` / `cache-per-connection-mb`; deprecation note on `cache-local-mb`. |
| `tests/test_shared_load_cache.py` | New unit tests for `SharedLoadCache`. |
| `tests/test_shared_load_cache_concurrency.py` | Race/invariant stress tests. |
| `tests/test_mvcc.py` | Extend with shared-cache integration invariants. |
| `CHANGES.md` | Entry for 1.12.0. |

---

## Task 1: SharedLoadCache — construction + basic get/set

**Files:**
- Create: `tests/test_shared_load_cache.py`
- Modify: `src/zodb_pgjsonb/storage.py` (add `SharedLoadCache` class)

- [ ] **Step 1: Write the failing test**

Create `tests/test_shared_load_cache.py`:

```python
"""Unit tests for SharedLoadCache (#63).

Pure-Python tests — no PostgreSQL required. Focus on correctness
invariants: consensus-TID gating, LRU eviction, byte accounting.
"""

from zodb_pgjsonb.storage import SharedLoadCache

from ZODB.utils import p64


class TestConstruction:
    def test_empty_cache(self):
        cache = SharedLoadCache(max_mb=4)
        assert cache._consensus_tid is None
        assert len(cache._cache) == 0
        assert cache._current_bytes == 0

    def test_max_bytes_from_mb(self):
        cache = SharedLoadCache(max_mb=8)
        assert cache._max_bytes == 8 * 1_000_000


class TestGetSetBasic:
    def test_set_rejected_when_consensus_uninitialized(self):
        cache = SharedLoadCache(max_mb=4)
        cache.set(zoid=1, data=b"x", tid_bytes=p64(100), polled_tid=100)
        assert len(cache._cache) == 0

    def test_get_returns_none_when_consensus_uninitialized(self):
        cache = SharedLoadCache(max_mb=4)
        assert cache.get(zoid=1, polled_tid=100) is None

    def test_set_and_get_after_consensus_initialized(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"xyz", tid_bytes=p64(100), polled_tid=100)
        assert cache.get(zoid=1, polled_tid=100) == (b"xyz", p64(100))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py -v`
Expected: FAIL with `ImportError: cannot import name 'SharedLoadCache'`

- [ ] **Step 3: Write minimal implementation**

In `src/zodb_pgjsonb/storage.py`, add after the `LoadCache` class (around line 217):

```python
class SharedLoadCache:
    """Process-wide LoadCache shared across all PGJsonbStorageInstance.

    Stores ``(pickle_bytes, tid_bytes)`` keyed by ``zoid`` (int).  A
    process-wide ``_consensus_tid`` gates reads and writes so that an
    instance holding a stale snapshot cannot pollute the cache after
    another instance's ``poll_advance`` has already invalidated a zoid
    at a newer TID.  See #63 for the race analysis.

    Thread-safe: a single ``RLock`` protects the OrderedDict, byte
    accounting, and ``_consensus_tid`` atomically.
    """

    def __init__(self, max_mb):
        self._cache = OrderedDict()  # zoid → (data_bytes, tid_bytes)
        self._consensus_tid = None  # int; advanced by poll_advance()
        self._max_bytes = int(max_mb * 1_000_000)
        self._current_bytes = 0
        self._lock = threading.RLock()
        self.hits = 0
        self.misses = 0

    def get(self, zoid, polled_tid):
        """Return (data, tid) for zoid or None.

        Returns None when the cache has never been initialized, when
        the caller's snapshot is older than the current consensus, or
        when the zoid is not cached.
        """
        with self._lock:
            if self._consensus_tid is None or polled_tid is None:
                self.misses += 1
                return None
            if polled_tid < self._consensus_tid:
                self.misses += 1
                return None
            entry = self._cache.get(zoid)
            if entry is None:
                self.misses += 1
                return None
            self._cache.move_to_end(zoid)
            self.hits += 1
            return entry

    def set(self, zoid, data, tid_bytes, polled_tid):
        """Store (data, tid_bytes) for zoid if the caller is up to date.

        Rejects writes from callers whose snapshot is older than the
        current consensus (which could be carrying stale pre-
        invalidation bytes), and never replaces a newer entry with an
        older one.
        """
        with self._lock:
            if self._consensus_tid is None or polled_tid is None:
                return
            if polled_tid < self._consensus_tid:
                return
            tid_int = u64(tid_bytes)
            existing = self._cache.get(zoid)
            if existing is not None and u64(existing[1]) >= tid_int:
                return
            if existing is not None:
                self._current_bytes -= len(existing[0])
            self._cache[zoid] = (data, tid_bytes)
            self._cache.move_to_end(zoid)
            self._current_bytes += len(data)
            while self._current_bytes > self._max_bytes and self._cache:
                _, (evicted_data, _) = self._cache.popitem(last=False)
                self._current_bytes -= len(evicted_data)

    def poll_advance(self, new_tid, changed_zoids):
        """Advance consensus_tid and invalidate changed zoids atomically."""
        with self._lock:
            if self._consensus_tid is None or new_tid >= self._consensus_tid:
                for z in changed_zoids:
                    entry = self._cache.pop(z, None)
                    if entry is not None:
                        self._current_bytes -= len(entry[0])
                self._consensus_tid = new_tid
```

Add `import threading` at the top of `storage.py` if not already present (check line ~30 for existing imports).

- [ ] **Step 4: Run test to verify it passes**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py -v`
Expected: PASS on all 4 tests.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/storage.py tests/test_shared_load_cache.py
git commit -m "feat(shared-cache): add SharedLoadCache skeleton with consensus-TID gate (#63)

Pure data structure with get/set/poll_advance and the stale-snapshot
gating rule. No integration with instance.load yet.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: SharedLoadCache — consensus-TID gating invariants

**Files:**
- Modify: `tests/test_shared_load_cache.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_shared_load_cache.py`:

```python
class TestConsensusTIDGating:
    def test_stale_reader_bypasses_cache(self):
        """A reader with polled_tid < consensus_tid gets None."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"at100", tid_bytes=p64(100), polled_tid=100)

        # Consensus moves forward via another instance's poll
        cache.poll_advance(new_tid=200, changed_zoids=[2])

        # Reader with old snapshot is gated out
        assert cache.get(zoid=1, polled_tid=150) is None

    def test_current_reader_hits(self):
        """A reader with polled_tid >= consensus_tid hits normally."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"at100", tid_bytes=p64(100), polled_tid=100)

        cache.poll_advance(new_tid=200, changed_zoids=[])  # no invalidations

        assert cache.get(zoid=1, polled_tid=200) == (b"at100", p64(100))

    def test_stale_writer_rejected(self):
        """A writer with polled_tid < consensus_tid cannot set."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.poll_advance(new_tid=200, changed_zoids=[])

        # Stale writer tries to set
        cache.set(zoid=1, data=b"stale", tid_bytes=p64(100), polled_tid=100)
        assert cache.get(zoid=1, polled_tid=200) is None

    def test_older_tid_never_replaces_newer(self):
        """set() with a tid_bytes older than the existing entry is ignored."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        cache.set(zoid=1, data=b"newer", tid_bytes=p64(200), polled_tid=200)

        # Another writer with an older tid_bytes (but same polled_tid)
        cache.set(zoid=1, data=b"older", tid_bytes=p64(150), polled_tid=200)
        assert cache.get(zoid=1, polled_tid=200) == (b"newer", p64(200))

    def test_poll_advance_invalidates_changed_zoids(self):
        """poll_advance removes entries for zoids in changed list."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"a", tid_bytes=p64(100), polled_tid=100)
        cache.set(zoid=2, data=b"b", tid_bytes=p64(100), polled_tid=100)

        cache.poll_advance(new_tid=150, changed_zoids=[1])

        assert cache.get(zoid=1, polled_tid=150) is None
        assert cache.get(zoid=2, polled_tid=150) == (b"b", p64(100))

    def test_poll_advance_with_older_tid_is_noop(self):
        """poll_advance with new_tid < current consensus does nothing."""
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        cache.set(zoid=1, data=b"x", tid_bytes=p64(200), polled_tid=200)

        # An instance pollling in from behind shouldn't do anything
        cache.poll_advance(new_tid=100, changed_zoids=[1])

        assert cache._consensus_tid == 200
        assert cache.get(zoid=1, polled_tid=200) == (b"x", p64(200))
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py::TestConsensusTIDGating -v`
Expected: PASS on all 6 tests (the gating logic from Task 1 already handles these cases).

- [ ] **Step 3: Commit**

```bash
git add tests/test_shared_load_cache.py
git commit -m "test(shared-cache): pin consensus-TID gating invariants (#63)

Cover: stale-reader bypass, current-reader hit, stale-writer rejection,
older-tid never replaces, poll_advance invalidation, poll_advance
backward no-op.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: SharedLoadCache — LRU eviction and byte accounting

**Files:**
- Modify: `tests/test_shared_load_cache.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_shared_load_cache.py`:

```python
class TestLRUEviction:
    def test_accounts_bytes_on_set(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"x" * 100, tid_bytes=p64(100), polled_tid=100)
        assert cache._current_bytes == 100

    def test_accounts_bytes_on_replace(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        cache.set(zoid=1, data=b"x" * 100, tid_bytes=p64(100), polled_tid=200)
        cache.set(zoid=1, data=b"y" * 200, tid_bytes=p64(200), polled_tid=200)
        assert cache._current_bytes == 200

    def test_accounts_bytes_on_invalidate(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        cache.set(zoid=1, data=b"x" * 100, tid_bytes=p64(100), polled_tid=100)
        cache.poll_advance(new_tid=200, changed_zoids=[1])
        assert cache._current_bytes == 0

    def test_evicts_lru_when_over_budget(self):
        cache = SharedLoadCache(max_mb=1)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        # Fill with 10 entries of 200_000 bytes each (2 MB total, budget 1 MB)
        for z in range(10):
            cache.set(zoid=z, data=b"x" * 200_000,
                      tid_bytes=p64(100), polled_tid=100)
        # At most ~5 entries fit in 1 MB
        assert len(cache._cache) <= 6
        assert cache._current_bytes <= 1_000_000

    def test_lru_order_promotes_on_get(self):
        cache = SharedLoadCache(max_mb=1)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        # Fill just under budget
        for z in range(4):
            cache.set(zoid=z, data=b"x" * 200_000,
                      tid_bytes=p64(100), polled_tid=100)
        # Touch zoid 0 — bumps it to most-recent
        cache.get(zoid=0, polled_tid=100)
        # Add one more — zoid 1 (oldest now) should be evicted, not zoid 0
        cache.set(zoid=99, data=b"x" * 200_000,
                  tid_bytes=p64(100), polled_tid=100)
        assert cache.get(zoid=0, polled_tid=100) is not None
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py::TestLRUEviction -v`
Expected: PASS on all 5 tests (the LRU logic from Task 1 already handles these cases).

- [ ] **Step 3: Commit**

```bash
git add tests/test_shared_load_cache.py
git commit -m "test(shared-cache): LRU eviction and byte accounting (#63)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: SharedLoadCache — concurrency stress test

**Files:**
- Create: `tests/test_shared_load_cache_concurrency.py`

- [ ] **Step 1: Write the failing test**

Create `tests/test_shared_load_cache_concurrency.py`:

```python
"""Concurrency stress tests for SharedLoadCache (#63).

Runs many threads performing interleaved get/set/poll_advance and
verifies that no bytes are returned for a zoid at a newer TID than
the caller's snapshot, and that byte accounting matches the actual
cache contents.
"""

from zodb_pgjsonb.storage import SharedLoadCache

from ZODB.utils import p64, u64

import random
import threading


def test_no_stale_reads_under_concurrent_writes_and_polls():
    """Under mixed thread load, any get hit honours its polled_tid.

    Invariant: the tid_bytes of any returned entry must be <= the
    reader's polled_tid. A returned entry newer than the reader's
    snapshot indicates the consensus gate failed.
    """
    cache = SharedLoadCache(max_mb=4)
    cache.poll_advance(new_tid=1000, changed_zoids=[])
    stop = threading.Event()
    violations = []

    def reader(tid):
        while not stop.is_set():
            for z in range(100):
                res = cache.get(zoid=z, polled_tid=tid)
                if res is not None:
                    _, tb = res
                    if u64(tb) > tid:
                        violations.append((z, u64(tb), tid))

    def writer(start_tid):
        tid = start_tid
        while not stop.is_set():
            cache.poll_advance(new_tid=tid, changed_zoids=[
                random.randint(0, 99)
            ])
            for _ in range(20):
                z = random.randint(0, 99)
                cache.set(zoid=z, data=b"x" * random.randint(100, 5000),
                          tid_bytes=p64(tid), polled_tid=tid)
            tid += random.randint(1, 10)

    threads = []
    for start in (1000, 1500, 2000):
        threads.append(threading.Thread(target=reader, args=(start,)))
    for start in (1000, 3000):
        threads.append(threading.Thread(target=writer, args=(start,)))
    for t in threads:
        t.start()

    # Run briefly, then stop
    threading.Event().wait(2.0)
    stop.set()
    for t in threads:
        t.join(timeout=5.0)

    assert violations == [], \
        f"Cache returned entries at tid > polled_tid: {violations[:5]}"


def test_byte_accounting_consistent_under_concurrent_load():
    """After the dust settles, _current_bytes equals sum of cached byte lens."""
    cache = SharedLoadCache(max_mb=2)
    cache.poll_advance(new_tid=1000, changed_zoids=[])
    stop = threading.Event()

    def worker():
        tid = 1000
        while not stop.is_set():
            for _ in range(50):
                z = random.randint(0, 199)
                cache.set(zoid=z, data=b"x" * random.randint(100, 50_000),
                          tid_bytes=p64(tid), polled_tid=tid)
            cache.poll_advance(new_tid=tid, changed_zoids=[
                random.randint(0, 199) for _ in range(5)
            ])
            tid += 1

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()

    threading.Event().wait(2.0)
    stop.set()
    for t in threads:
        t.join(timeout=5.0)

    with cache._lock:
        actual_bytes = sum(len(e[0]) for e in cache._cache.values())
        assert cache._current_bytes == actual_bytes, \
            f"Accounting drift: tracked={cache._current_bytes} actual={actual_bytes}"
        assert cache._current_bytes <= cache._max_bytes
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_shared_load_cache_concurrency.py -v`
Expected: PASS on both tests. If either fails, the lock discipline in `SharedLoadCache` is broken — fix before proceeding.

- [ ] **Step 3: Commit**

```bash
git add tests/test_shared_load_cache_concurrency.py
git commit -m "test(shared-cache): concurrency stress — no stale reads, no accounting drift (#63)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Wire SharedLoadCache into PGJsonbStorage

**Files:**
- Modify: `src/zodb_pgjsonb/storage.py:255-393` (PGJsonbStorage.__init__)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_shared_load_cache.py`:

```python
class TestPGJsonbStorageIntegration:
    """Storage exposes a _shared_cache and threads its config properly."""

    def test_storage_has_shared_cache(self, storage):
        assert isinstance(storage._shared_cache, SharedLoadCache)

    def test_shared_cache_size_from_cache_shared_mb(self):
        """cache_shared_mb controls shared cache size independently."""
        from tests.conftest import DSN, clean_db
        from zodb_pgjsonb.storage import PGJsonbStorage

        clean_db()
        s = PGJsonbStorage(DSN, cache_shared_mb=32)
        try:
            assert s._shared_cache._max_bytes == 32 * 1_000_000
        finally:
            s.close()

    def test_cache_per_connection_mb_controls_l1(self):
        """cache_per_connection_mb controls per-instance L1 cache size."""
        from tests.conftest import DSN, clean_db
        from zodb_pgjsonb.storage import PGJsonbStorage

        clean_db()
        s = PGJsonbStorage(DSN, cache_per_connection_mb=8)
        try:
            inst = s.new_instance()
            try:
                assert inst._load_cache._max_size == 8 * 1_000_000
            finally:
                inst.release()
        finally:
            s.close()
```

Add `import pytest` to the top of `tests/test_shared_load_cache.py` (the storage fixture requires DB).

The integration tests need the `pytest.mark.db` marker. Add above `TestPGJsonbStorageIntegration`:

```python
class TestPGJsonbStorageIntegration:
    pytestmark = pytest.mark.db
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py::TestPGJsonbStorageIntegration -v`
Expected: FAIL on all three (no `_shared_cache`, no `cache_shared_mb` / `cache_per_connection_mb` params).

- [ ] **Step 3: Modify PGJsonbStorage.__init__**

In `src/zodb_pgjsonb/storage.py` at line 255, change the signature:

```python
    def __init__(
        self,
        dsn,
        name="pgjsonb",
        history_preserving=False,
        blob_temp_dir=None,
        cache_local_mb=None,          # deprecated alias for cache_shared_mb
        cache_shared_mb=256,
        cache_per_connection_mb=16,
        pool_size=1,
        pool_max_size=10,
        pool_timeout=30.0,
        s3_client=None,
        blob_cache=None,
        blob_threshold=102_400,
        cache_warm_pct=10,
        cache_warm_decay=0.8,
    ):
        BaseStorage.__init__(self, name)
        self._dsn = dsn
        self._history_preserving = history_preserving

        # Deprecation: cache_local_mb used to be per-instance; now it is
        # an alias for cache_shared_mb (see #63).  Warn once per storage.
        if cache_local_mb is not None:
            import warnings
            warnings.warn(
                "cache_local_mb is deprecated since 1.12.0; use "
                "cache_shared_mb for the process-wide cache and "
                "cache_per_connection_mb for the per-instance L1. "
                "cache_local_mb is being mapped to cache_shared_mb.",
                DeprecationWarning,
                stacklevel=2,
            )
            cache_shared_mb = cache_local_mb

        self._cache_shared_mb = cache_shared_mb
        self._cache_per_connection_mb = cache_per_connection_mb
        # Keep the old attribute name for backward compat with any
        # code that reads it (tests etc.)
        self._cache_local_mb = cache_per_connection_mb
        self._ltid = z64
        self._pack_tid = None
```

Replace the existing `self._load_cache = LoadCache(max_mb=cache_local_mb)` at line 306 with:

```python
        # Per-instance L1 load cache (small, fast path, no lock).
        # Size from cache_per_connection_mb.  See instance.py.
        self._load_cache = LoadCache(max_mb=cache_per_connection_mb)

        # Process-wide shared load cache (L2).  One per PGJsonbStorage,
        # visible to all PGJsonbStorageInstance objects on the same
        # process.  Gated by consensus_tid for MVCC correctness (#63).
        self._shared_cache = SharedLoadCache(max_mb=cache_shared_mb)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py::TestPGJsonbStorageIntegration -v`
Expected: PASS.

Also run the existing storage tests to ensure no regressions:

Run: `.venv/bin/pytest tests/test_storage.py tests/test_mvcc.py tests/test_cache.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/storage.py tests/test_shared_load_cache.py
git commit -m "feat(shared-cache): instantiate SharedLoadCache on PGJsonbStorage (#63)

Add cache_shared_mb (default 256 MB, process-wide) and
cache_per_connection_mb (default 16 MB, per-instance L1) constructor
params. cache_local_mb becomes a deprecation alias for
cache_shared_mb with a DeprecationWarning — existing deployments that
set cache_local_mb=256 automatically get a 256 MB shared cache
instead of 256 MB per connection.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Integrate shared cache into instance.load()

**Files:**
- Modify: `src/zodb_pgjsonb/instance.py:217-269` (`load` method)

- [ ] **Step 1: Write the failing test**

Create `tests/test_shared_load_cache_integration.py`:

```python
"""Integration tests: shared cache and instance.load() (#63).

Requires PostgreSQL. Verifies the L1 → shared → PG cascade and the
TID-gated population of the shared cache.
"""

from persistent.mapping import PersistentMapping

import pytest
import transaction as txn


pytestmark = pytest.mark.db


def _create_tree(db, n):
    """Create n children under root, commit."""
    conn = db.open()
    try:
        root = conn.root()
        for i in range(n):
            root[f"c{i}"] = PersistentMapping({"i": i})
        txn.commit()
    finally:
        conn.close()


class TestSharedCachePopulatedByLoad:
    def test_load_populates_shared_cache(self, db):
        """After a load(), the shared cache holds the same entry."""
        _create_tree(db, 5)

        # Clear L1 so reads go to PG and populate shared
        conn = db.open()
        try:
            instance = conn._storage
            instance._load_cache.clear()
            root = conn.root()
            _ = root["c0"]["i"]
            zoid = root["c0"]._p_oid
            from ZODB.utils import u64
            shared = instance._main._shared_cache
            entry = shared.get(u64(zoid), instance._polled_tid)
            assert entry is not None
        finally:
            conn.close()

    def test_shared_cache_hit_promoted_to_l1(self, db):
        """A hit on the shared cache populates the L1 for next time."""
        _create_tree(db, 5)

        conn1 = db.open()
        try:
            root1 = conn1.root()
            _ = root1["c0"]["i"]  # populates both L1 and shared
        finally:
            conn1.close()

        conn2 = db.open()
        try:
            instance = conn2._storage
            # Force L1 miss
            instance._load_cache.clear()
            root2 = conn2.root()
            _ = root2["c0"]["i"]  # should hit shared, not PG
            zoid = root2["c0"]._p_oid
            from ZODB.utils import u64
            # Now L1 should hold the entry (promoted from shared)
            assert instance._load_cache.get(u64(zoid)) is not None
        finally:
            conn2.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_shared_load_cache_integration.py -v`
Expected: FAIL — `instance.load()` does not yet consult or populate the shared cache.

- [ ] **Step 3: Modify instance.load()**

In `src/zodb_pgjsonb/instance.py`, replace the entire `load()` method (starting at line 217) with:

```python
    def load(self, oid, version=""):
        """Load current object state.

        Three-tier cascade: per-instance L1 (fast, no lock) → process-
        wide shared cache (L2, consensus-TID gated) → PostgreSQL.
        """
        zoid = u64(oid)

        # L1: instance load cache (fast path, no lock)
        cached = self._load_cache.get(zoid)
        if cached is not None:
            return cached

        # L2: process-wide shared cache
        shared = self._main._shared_cache
        shared_hit = shared.get(zoid, self._polled_tid)
        if shared_hit is not None:
            self._load_cache.set(zoid, *shared_hit)
            return shared_hit

        # Miss — go to PG
        with self._conn.cursor() as cur:
            cur.execute(
                self._load_sql,
                (zoid,),
                prepare=True,
            )
            row = cur.fetchone()

        if row is None:
            raise POSKeyError(oid)

        record = {
            "@cls": [row["class_mod"], row["class_name"]],
            "@s": _unsanitize_from_pg(row["state"]),
        }
        data = zodb_json_codec.encode_zodb_record(record)
        tid = p64(row["tid"])
        self._serial_cache[(oid, tid)] = data
        self._load_cache.set(zoid, data, tid)
        shared.set(zoid, data, tid, self._polled_tid)

        # Prefetch refs if the expression yielded a non-NULL array
        refs = row.get("refs") if isinstance(row, dict) else None
        if refs:
            ref_oids = [
                p64(ref_zoid)
                for ref_zoid in refs
                if self._load_cache.get(ref_zoid) is None
            ]
            if ref_oids:
                self.load_multiple(ref_oids)

        # Record for cache warmer (#48)
        warmer = self._main._warmer
        if warmer and warmer.recording:
            warmer.record(zoid)

        return data, tid
```

Key change: the L2 check now goes through `self._main._shared_cache.get()` instead of `self._main._warmer.get()`, and after a PG load the shared cache is populated via `shared.set()`. The warmer still receives `record()` for pattern learning.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_shared_load_cache_integration.py -v`
Expected: PASS.

Run: `.venv/bin/pytest tests/ -q`
Expected: PASS, no regressions.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/instance.py tests/test_shared_load_cache_integration.py
git commit -m "feat(shared-cache): wire SharedLoadCache into instance.load() (#63)

Read path now cascades L1 (per-instance) → shared (process-wide) →
PG. A PG load populates both L1 and shared. A shared hit promotes to
L1 for subsequent fast-path access within the same connection.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Integrate shared cache into instance.load_multiple()

**Files:**
- Modify: `src/zodb_pgjsonb/instance.py:271-319` (`load_multiple` method)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_shared_load_cache_integration.py`:

```python
class TestLoadMultipleUsesSharedCache:
    def test_load_multiple_populates_shared(self, db):
        _create_tree(db, 10)

        conn = db.open()
        try:
            instance = conn._storage
            instance._load_cache.clear()
            # Trigger load_multiple by loading several OIDs at once
            oids = []
            root = conn.root()
            for i in range(5):
                oids.append(root[f"c{i}"]._p_oid)
            result = instance.load_multiple(oids)
            assert len(result) == 5

            from ZODB.utils import u64
            shared = instance._main._shared_cache
            for oid in oids:
                assert shared.get(u64(oid), instance._polled_tid) is not None
        finally:
            conn.close()

    def test_load_multiple_hits_shared_before_pg(self, db):
        _create_tree(db, 10)

        # Warm shared cache via another connection
        conn1 = db.open()
        try:
            instance1 = conn1._storage
            oids = []
            root = conn1.root()
            for i in range(5):
                oids.append(root[f"c{i}"]._p_oid)
            instance1.load_multiple(oids)
        finally:
            conn1.close()

        # Second connection: clear L1, load_multiple should hit shared
        conn2 = db.open()
        try:
            instance2 = conn2._storage
            instance2._load_cache.clear()
            # Get oids again from root in this connection
            root = conn2.root()
            oids = [root[f"c{i}"]._p_oid for i in range(5)]
            result = instance2.load_multiple(oids)
            assert len(result) == 5
            # All 5 should now be in L1 (promoted from shared)
            from ZODB.utils import u64
            for oid in oids:
                assert instance2._load_cache.get(u64(oid)) is not None
        finally:
            conn2.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_shared_load_cache_integration.py::TestLoadMultipleUsesSharedCache -v`
Expected: FAIL on the second test (shared cache not consulted in `load_multiple`).

- [ ] **Step 3: Modify instance.load_multiple()**

In `src/zodb_pgjsonb/instance.py`, replace the entire `load_multiple()` method (starting at line 271) with:

```python
    def load_multiple(self, oids):
        """Load multiple objects in a single query.

        Consults L1 and the shared cache before going to PG.

        Args:
            oids: iterable of oid bytes

        Returns:
            dict mapping oid_bytes -> (pickle_bytes, tid_bytes).
            Only includes oids that exist; missing oids are silently omitted.
        """
        result = {}
        miss_oids = []  # list of (oid_bytes, zoid_int) for L1+shared misses
        shared = self._main._shared_cache

        for oid in oids:
            zoid = u64(oid)
            cached = self._load_cache.get(zoid)
            if cached is not None:
                result[oid] = cached
                continue
            shared_hit = shared.get(zoid, self._polled_tid)
            if shared_hit is not None:
                self._load_cache.set(zoid, *shared_hit)
                result[oid] = shared_hit
                continue
            miss_oids.append((oid, zoid))

        if not miss_oids:
            return result

        zoid_list = [zoid for _, zoid in miss_oids]
        zoid_to_oid = {zoid: oid for oid, zoid in miss_oids}

        with self._conn.cursor() as cur:
            cur.execute(
                "SELECT zoid, tid, class_mod, class_name, state "
                "FROM object_state WHERE zoid = ANY(%s)",
                (zoid_list,),
                prepare=True,
            )
            rows = cur.fetchall()

        for row in rows:
            record = {
                "@cls": [row["class_mod"], row["class_name"]],
                "@s": _unsanitize_from_pg(row["state"]),
            }
            data = zodb_json_codec.encode_zodb_record(record)
            tid = p64(row["tid"])
            oid = zoid_to_oid[row["zoid"]]
            self._serial_cache[(oid, tid)] = data
            self._load_cache.set(row["zoid"], data, tid)
            shared.set(row["zoid"], data, tid, self._polled_tid)
            result[oid] = (data, tid)

        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_shared_load_cache_integration.py -v`
Expected: PASS all tests in this file.

Run: `.venv/bin/pytest tests/test_load_multiple.py -v`
Expected: PASS, no regressions.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/instance.py tests/test_shared_load_cache_integration.py
git commit -m "feat(shared-cache): wire SharedLoadCache into load_multiple (#63)

Same cascade as load(): per-OID L1 check, then per-OID shared check,
batched PG query for the remaining misses. Promotes shared hits to
L1 and populates shared from PG results.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Integrate shared cache into poll_invalidations

**Files:**
- Modify: `src/zodb_pgjsonb/instance.py:156-206` (`poll_invalidations` method)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_shared_load_cache_integration.py`:

```python
class TestPollInvalidatesShared:
    def test_poll_invalidations_evicts_from_shared(self, db):
        """A commit from another connection invalidates shared entries."""
        _create_tree(db, 5)

        # Reader populates shared
        conn_reader = db.open()
        try:
            instance_reader = conn_reader._storage
            root = conn_reader.root()
            _ = root["c0"]["i"]
            zoid = root["c0"]._p_oid
        finally:
            conn_reader.close()

        # Writer modifies c0
        conn_writer = db.open()
        try:
            root = conn_writer.root()
            root["c0"]["i"] = 999
            txn.commit()
        finally:
            conn_writer.close()

        # New reader — poll_invalidations must evict c0 from shared
        conn2 = db.open()
        try:
            instance2 = conn2._storage
            from ZODB.utils import u64
            shared = instance2._main._shared_cache
            # poll_invalidations fired on conn2.open() — c0 should be gone
            assert shared.get(u64(zoid), instance2._polled_tid) is None
        finally:
            conn2.close()

    def test_poll_invalidations_advances_consensus(self, db):
        """poll_invalidations updates _consensus_tid on the shared cache."""
        _create_tree(db, 2)

        conn = db.open()
        try:
            instance = conn._storage
            shared = instance._main._shared_cache
            # After opening (which polls), consensus must be set
            assert shared._consensus_tid is not None
            assert shared._consensus_tid == instance._polled_tid
        finally:
            conn.close()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_shared_load_cache_integration.py::TestPollInvalidatesShared -v`
Expected: FAIL — `poll_invalidations` does not yet call `shared.poll_advance`.

- [ ] **Step 3: Modify poll_invalidations**

In `src/zodb_pgjsonb/instance.py`, in the `poll_invalidations` method (starts around line 156), replace the result-collection block (lines 188-206) with:

```python
        result = []
        changed_zoids = []
        if self._polled_tid is not None and new_tid != self._polled_tid:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT zoid FROM object_state "
                    "WHERE tid > %s AND tid <= %s",
                    (self._polled_tid, new_tid),
                )
                rows = cur.fetchall()
            warmer = self._main._warmer
            for r in rows:
                zoid = r["zoid"]
                result.append(p64(zoid))
                changed_zoids.append(zoid)
                self._load_cache.invalidate(zoid)
                if warmer:
                    warmer.invalidate(zoid)

        # Advance the process-wide shared cache atomically with the
        # invalidation set.  Called even when changed_zoids is empty
        # so the consensus_tid reaches the latest observed state
        # (#63 — correctness-critical on first poll).
        self._main._shared_cache.poll_advance(new_tid, changed_zoids)

        self._polled_tid = new_tid
        return result
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_shared_load_cache_integration.py::TestPollInvalidatesShared -v`
Expected: PASS.

Run: `.venv/bin/pytest tests/ -q`
Expected: PASS, no regressions. Special attention to `test_mvcc.py`.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/instance.py tests/test_shared_load_cache_integration.py
git commit -m "feat(shared-cache): advance consensus on poll_invalidations (#63)

Each instance poll hands the new TID and the set of changed zoids to
the shared cache via poll_advance(). The shared cache clears the
invalidated zoids and advances _consensus_tid atomically, which in
turn gates out any stale-snapshot writer from polluting the cache.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Migrate CacheWarmer to populate the shared cache

**Files:**
- Modify: `src/zodb_pgjsonb/cache_warmer.py` (entire `warm`, `get`, `invalidate` logic)
- Modify: `src/zodb_pgjsonb/instance.py:197-232` (remove warmer.get() path from load)

- [ ] **Step 1: Write the failing test**

Append to `tests/test_cache_warmer.py`:

```python
class TestWarmerPopulatesSharedCache:
    """After #63: warmer writes go into the shared cache, not a private dict."""

    def test_warm_populates_shared_cache(self, storage):
        """CacheWarmer.warm loads into PGJsonbStorage._shared_cache."""
        from ZODB.utils import p64, u64
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        # Seed the stats table
        with storage._conn.cursor() as cur:
            cur.execute(
                "INSERT INTO cache_warm_stats (zoid, score) "
                "VALUES (1, 10.0), (2, 9.0), (3, 8.0)"
            )
        storage._conn.commit()

        # Manually trigger a warm cycle
        warmer = CacheWarmer(
            conn=storage._conn,
            target_count=10,
            shared_cache=storage._shared_cache,
            load_current_tid_fn=lambda: 100,
        )

        def load_multiple_fn(oids):
            return {
                oid: (b"data-" + oid, p64(50))
                for oid in oids
            }

        # Prime consensus so set() is accepted
        storage._shared_cache.poll_advance(new_tid=100, changed_zoids=[])
        warmer.warm(load_multiple_fn)

        shared = storage._shared_cache
        assert shared.get(zoid=1, polled_tid=100) == (b"data-" + p64(1), p64(50))
        assert shared.get(zoid=2, polled_tid=100) == (b"data-" + p64(2), p64(50))
```

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_cache_warmer.py::TestWarmerPopulatesSharedCache -v`
Expected: FAIL — `CacheWarmer.__init__` does not accept `shared_cache` / `load_current_tid_fn`.

- [ ] **Step 3: Rewrite CacheWarmer**

Replace `src/zodb_pgjsonb/cache_warmer.py` entirely with:

```python
"""Learning cache warmer for zodb-pgjsonb.

Records which ZOIDs are loaded after each startup, persists scores
to PostgreSQL with exponential decay, and pre-loads the highest-
scored objects into the process-wide SharedLoadCache on the next
startup.

See docs/plans/2026-04-03-learning-cache-warmer-design.md and #63 for
the shared-cache migration.
"""

import contextlib
import logging


log = logging.getLogger(__name__)

# Schema DDL for the warm stats table
WARM_STATS_DDL = """\
CREATE TABLE IF NOT EXISTS cache_warm_stats (
    zoid   BIGINT PRIMARY KEY,
    score  FLOAT NOT NULL DEFAULT 1.0
);
"""


class CacheWarmer:
    """Learning cache warmer.

    Two phases:
    1. **Warm** (startup, background thread): load top-scored ZOIDs
       from ``cache_warm_stats`` directly into the process-wide
       ``SharedLoadCache``.
    2. **Record** (after startup): track the first N unique ZOIDs
       loaded via ``Instance.load()``, flush to PG every
       ``flush_interval``.
    """

    def __init__(
        self,
        conn,
        target_count,
        shared_cache,
        load_current_tid_fn,
        decay=0.8,
        flush_interval=1000,
    ):
        self.recording = target_count > 0
        self._recorded = set()
        self._pending = set()
        self._target_count = target_count
        self._flush_interval = flush_interval
        self._decayed = False
        self._decay = decay

        # Shared cache to populate at warm time (#63)
        self._shared_cache = shared_cache
        self._load_current_tid_fn = load_current_tid_fn

        self._conn = conn

    # ── Recording phase ──────────────────────────────────────────────

    def record(self, zoid):
        if not self.recording:
            return
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
            log.info(
                "Cache warmer: recording complete, %d OIDs captured",
                len(self._recorded),
            )

    def _flush(self, decay=False):
        zoids = sorted(self._pending)
        if not zoids:
            return
        self._pending.clear()
        try:
            self._conn.execute("BEGIN")
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
            self._conn.execute("COMMIT")
        except Exception:
            with contextlib.suppress(Exception):
                self._conn.execute("ROLLBACK")
            log.warning("Cache warmer: flush failed", exc_info=True)

    # ── Warming phase ────────────────────────────────────────────────

    def _read_top_oids(self):
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT zoid FROM cache_warm_stats "
                    "ORDER BY score DESC LIMIT %(n)s",
                    {"n": self._target_count},
                )
                return [row["zoid"] for row in cur.fetchall()]
        except Exception:
            log.warning("Cache warmer: read top OIDs failed", exc_info=True)
            return []

    def warm(self, load_multiple_fn):
        """Load top-N ZOIDs into the shared cache.

        Runs in a background daemon thread.  Primes the consensus TID
        on the shared cache to the current PG max_tid so that
        subsequent ``shared.set`` calls are accepted.
        """
        from ZODB.utils import p64
        from ZODB.utils import u64

        top_zoids = self._read_top_oids()
        if not top_zoids:
            log.info("Cache warmer: no stats yet, skipping warmup")
            return

        oids = [p64(z) for z in top_zoids]
        try:
            results = load_multiple_fn(oids)
        except Exception:
            log.warning("Cache warmer: load_multiple failed", exc_info=True)
            return

        # Prime consensus so set() accepts our writes, then populate.
        current_tid = self._load_current_tid_fn()
        self._shared_cache.poll_advance(new_tid=current_tid, changed_zoids=[])
        written = 0
        for oid, (data, tid_bytes) in results.items():
            self._shared_cache.set(
                zoid=u64(oid),
                data=data,
                tid_bytes=tid_bytes,
                polled_tid=current_tid,
            )
            written += 1
        log.info("Cache warmer: loaded %d objects into shared cache", written)
```

- [ ] **Step 4: Update PGJsonbStorage to pass new params**

In `src/zodb_pgjsonb/storage.py`, in `PGJsonbStorage.__init__` around line 376, replace:

```python
            self._warmer = CacheWarmer(
                self._conn, target_count=target, decay=cache_warm_decay
            )
```

with:

```python
            def _current_max_tid():
                try:
                    with self._conn.cursor() as cur:
                        cur.execute(
                            "SELECT COALESCE(MAX(tid), 0) AS t FROM transaction_log"
                        )
                        row = cur.fetchone()
                        return row["t"] if row else 0
                except Exception:
                    return 0

            self._warmer = CacheWarmer(
                self._conn,
                target_count=target,
                shared_cache=self._shared_cache,
                load_current_tid_fn=_current_max_tid,
                decay=cache_warm_decay,
            )
```

- [ ] **Step 5: Remove warmer.get() and warmer.invalidate() call sites from instance.py**

In `src/zodb_pgjsonb/instance.py`, the `load()` method (already rewritten in Task 6) no longer consults `self._main._warmer` for reads — only `record()`.

In `poll_invalidations()` (already rewritten in Task 8), remove the `warmer.invalidate(zoid)` line. The shared cache now handles all invalidation; the warmer no longer has a separate `_warm_cache` to invalidate.

The final `poll_invalidations` loop becomes:

```python
        result = []
        changed_zoids = []
        if self._polled_tid is not None and new_tid != self._polled_tid:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT DISTINCT zoid FROM object_state "
                    "WHERE tid > %s AND tid <= %s",
                    (self._polled_tid, new_tid),
                )
                rows = cur.fetchall()
            for r in rows:
                zoid = r["zoid"]
                result.append(p64(zoid))
                changed_zoids.append(zoid)
                self._load_cache.invalidate(zoid)

        self._main._shared_cache.poll_advance(new_tid, changed_zoids)
        self._polled_tid = new_tid
        return result
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_cache_warmer.py -v`
Expected: PASS the new test. Existing warmer tests that check `_warm_cache` / `get` / `invalidate` will fail — those tests need updating to reflect the new semantics. Update them inline:

Any `assert len(warmer._warm_cache) == N` becomes `assert shared.get(zoid, polled_tid) is not None` for expected zoids.

Any test that calls `warmer.get(zoid)` should be rewritten to call `shared.get(zoid, polled_tid)`.

Any test that calls `warmer.invalidate(zoid)` should be rewritten to call `shared.poll_advance(new_tid, [zoid])`.

Run the full suite:

Run: `.venv/bin/pytest tests/ -q`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py src/zodb_pgjsonb/storage.py src/zodb_pgjsonb/instance.py tests/test_cache_warmer.py
git commit -m "refactor(warmer): populate SharedLoadCache instead of private dict (#63)

CacheWarmer loses its internal _warm_cache, get(), and invalidate()
methods. warm() now primes consensus_tid on the shared cache and
pushes entries via shared.set(). instance.load() no longer checks
the warmer separately — the shared cache is the only L2.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: ZConfig schema for new knobs

**Files:**
- Modify: `src/zodb_pgjsonb/component.xml`
- Modify: `src/zodb_pgjsonb/config.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/test_config.py`:

```python
class TestCacheConfigMigration:
    def test_cache_shared_mb_zconfig(self, tmp_path):
        """cache-shared-mb threads through to storage._cache_shared_mb."""
        from ZODB.config import storageFromString
        from tests.conftest import DSN

        zconf = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
          dsn {DSN}
          cache-shared-mb 64
          cache-per-connection-mb 4
        </pgjsonb>
        """
        storage = storageFromString(zconf)
        try:
            assert storage._shared_cache._max_bytes == 64 * 1_000_000
            assert storage._load_cache._max_size == 4 * 1_000_000
        finally:
            storage.close()

    def test_cache_local_mb_deprecation_alias(self, tmp_path, recwarn):
        """cache-local-mb still works, warns, maps to cache_shared_mb."""
        import warnings
        from ZODB.config import storageFromString
        from tests.conftest import DSN

        zconf = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
          dsn {DSN}
          cache-local-mb 128
        </pgjsonb>
        """
        with warnings.catch_warnings(record=True) as wlist:
            warnings.simplefilter("always")
            storage = storageFromString(zconf)
            try:
                assert storage._shared_cache._max_bytes == 128 * 1_000_000
                dep = [w for w in wlist
                       if issubclass(w.category, DeprecationWarning)
                       and "cache_local_mb" in str(w.message)]
                assert dep, "expected DeprecationWarning for cache_local_mb"
            finally:
                storage.close()
```

Mark as `@pytest.mark.db` via a class-level `pytestmark`.

- [ ] **Step 2: Run test to verify it fails**

Run: `.venv/bin/pytest tests/test_config.py::TestCacheConfigMigration -v`
Expected: FAIL — `cache-shared-mb` and `cache-per-connection-mb` are not in the ZConfig schema; config.py does not pass them through.

- [ ] **Step 3: Update component.xml**

In `src/zodb_pgjsonb/component.xml`, replace the existing `cache-local-mb` key block with:

```xml
    <key name="cache-local-mb" datatype="integer">
      <description>
        DEPRECATED since 1.12.0.  Alias for cache-shared-mb; emits a
        DeprecationWarning when set.  Before 1.12.0 this was the
        per-instance cache budget; it is now the process-wide shared
        cache budget, so existing deployments automatically see the
        same total memory footprint redistributed into a single cache.
      </description>
    </key>

    <key name="cache-shared-mb" datatype="integer" default="256">
      <description>
        Size of the process-wide shared object cache in megabytes.
        One cache per process, shared across all ZODB Connections.
        Caches load() results (pickle bytes) to avoid repeated
        PostgreSQL round-trips and JSONB transcoding.  Set to 0 to
        disable.  Default: 256.
      </description>
    </key>

    <key name="cache-per-connection-mb" datatype="integer" default="16">
      <description>
        Size of the per-connection L1 object cache in megabytes.
        A small lock-free cache ahead of the shared cache for
        hot-path reads.  Set to 0 to rely entirely on the shared
        cache.  Default: 16.
      </description>
    </key>
```

- [ ] **Step 4: Update config.py**

In `src/zodb_pgjsonb/config.py`, replace the `PGJsonbStorage(...)` call at lines 68-82 with:

```python
        kwargs = dict(
            dsn=config.dsn,
            name=config.name,
            history_preserving=config.history_preserving,
            blob_temp_dir=config.blob_temp_dir,
            cache_shared_mb=getattr(config, "cache_shared_mb", 256),
            cache_per_connection_mb=getattr(config, "cache_per_connection_mb", 16),
            pool_size=config.pool_size,
            pool_max_size=config.pool_max_size,
            pool_timeout=config.pool_timeout,
            s3_client=s3_client,
            blob_cache=blob_cache,
            blob_threshold=getattr(config, "blob_threshold", 100 * 1024),
            cache_warm_pct=getattr(config, "cache_warm_pct", 10),
            cache_warm_decay=getattr(config, "cache_warm_decay", 0.8),
        )
        # Deprecation alias — let PGJsonbStorage.__init__ emit the warning
        if getattr(config, "cache_local_mb", None) is not None:
            kwargs["cache_local_mb"] = config.cache_local_mb

        return PGJsonbStorage(**kwargs)
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_config.py::TestCacheConfigMigration -v`
Expected: PASS.

Run: `.venv/bin/pytest tests/test_config.py -v`
Expected: PASS all existing config tests.

- [ ] **Step 6: Commit**

```bash
git add src/zodb_pgjsonb/component.xml src/zodb_pgjsonb/config.py tests/test_config.py
git commit -m "feat(config): add cache-shared-mb / cache-per-connection-mb, deprecate cache-local-mb (#63)

ZConfig schema now exposes both cache tiers. cache-local-mb stays
as a deprecation alias that maps onto cache-shared-mb, so existing
deployments get the shared cache automatically without re-
configuring.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Dead-code sweep

**Goal:** After Task 9 the warmer's L2 dict, its get/invalidate methods, and the `_warming_done` event are no longer referenced by production code.  Their tests are also obsolete.  This task removes every dead reference so the caching layer tells a single coherent story.

**Files:**
- Modify: `tests/test_cache_warmer.py` (delete obsolete test blocks)
- Verify: `src/zodb_pgjsonb/cache_warmer.py`, `src/zodb_pgjsonb/instance.py`, `src/zodb_pgjsonb/storage.py`

- [ ] **Step 1: Inventory remaining dead references**

Run exactly this grep — it must come up empty against `src/` after Task 9, and must point only at test code that this task deletes:

```bash
grep -rn "_warm_cache\|_warming_done\|warmer\.get\|warmer\.invalidate" \
    src/ tests/ 2>&1
```

Expected:
- Zero hits in `src/` (Task 9 already removed all of them).  If any remain, finish Task 9 first.
- Hits in `tests/test_cache_warmer.py` only — the lines listed in Step 2.

- [ ] **Step 2: Delete the obsolete `TestCacheWarmerL2` test class**

Open `tests/test_cache_warmer.py` and delete the entire `TestCacheWarmerL2` class (five tests: `test_get_returns_none_before_warming_done`, `test_get_returns_data_after_warming_done`, `test_get_returns_none_for_missing`, `test_invalidate_removes_entry`, `test_invalidate_nonexistent_is_noop`).

Rationale: these tests exercised `warmer.get()` and `warmer.invalidate()` — both removed in Task 9.  They are not rewritable into shared-cache equivalents because the shared cache already has its own tests (`tests/test_shared_load_cache.py`).  Keeping them would either be redundant or would reach into `shared._cache` internals and break encapsulation.

- [ ] **Step 3: Rewrite `TestCacheWarmerWarm` tests to assert on the shared cache**

Still in `tests/test_cache_warmer.py`, replace the body of `TestCacheWarmerWarm::test_warm_populates_cache` (around line 133) with an assertion on the shared cache, and add the new constructor kwargs:

```python
class TestCacheWarmerWarm:
    def test_warm_populates_cache(self):
        from zodb_pgjsonb.storage import SharedLoadCache
        from ZODB.utils import p64
        from ZODB.utils import u64

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(50)) for oid in oids}

        w.warm(loader)
        assert shared.get(zoid=1, polled_tid=100) == (b"data-" + p64(1), p64(50))
        assert shared.get(zoid=2, polled_tid=100) == (b"data-" + p64(2), p64(50))
        assert shared.get(zoid=3, polled_tid=100) == (b"data-" + p64(3), p64(50))
```

For `test_warm_empty_stats` (around line 148), replace the body with:

```python
    def test_warm_empty_stats(self):
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )
        w.warm(lambda oids: {})
        # No stats → nothing written
        assert shared._consensus_tid is None
        assert len(shared._cache) == 0
```

For `test_warm_handles_load_multiple_exception` (around line 157), replace the body with:

```python
    def test_warm_handles_load_multiple_exception(self):
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def raising_loader(oids):
            raise RuntimeError("simulated failure")

        # warm must swallow the exception and leave the cache empty
        w.warm(raising_loader)
        assert len(shared._cache) == 0
```

If `_FakeConn` is a test helper defined earlier in `test_cache_warmer.py`, leave it as-is.  If it was defined inline in deleted tests, lift it to module-level so the rewritten tests can reuse it.

- [ ] **Step 4: Rewrite `TestCacheWarmerDB::test_full_record_flush_read_cycle`**

Locate `test_full_record_flush_read_cycle` around line 257.  Any assertion on `w._warm_cache` or `w._warming_done` is dead — replace the cache assertions with `shared_cache.get(...)` assertions, and pass `shared_cache=...` + `load_current_tid_fn=...` through the `CacheWarmer(...)` call.

If the test currently looks like:

```python
    w = CacheWarmer(conn=storage._conn, target_count=5, decay=0.5)
    ...
    assert len(w._warm_cache) == expected
```

it becomes:

```python
    from zodb_pgjsonb.storage import SharedLoadCache
    shared = SharedLoadCache(max_mb=4)
    w = CacheWarmer(
        conn=storage._conn,
        target_count=5,
        shared_cache=shared,
        load_current_tid_fn=lambda: 100,
        decay=0.5,
    )
    ...
    # Count entries via the shared cache
    assert len(shared._cache) == expected
```

- [ ] **Step 5: Verify the sweep is complete**

Run the dead-reference grep from Step 1 again:

```bash
grep -rn "_warm_cache\|_warming_done\|warmer\.get\|warmer\.invalidate" \
    src/ tests/ 2>&1
```

Expected: zero hits.  If any remain, delete or rewrite them.

Also grep for accidentally-orphaned imports:

```bash
grep -rn "from zodb_pgjsonb.cache_warmer import" src/ tests/ 2>&1
```

Every `import` should still point to a symbol that exists (`CacheWarmer`, `WARM_STATS_DDL`).  `_warm_cache` / `_warming_done` are not public, so there should be no imports of them, but check anyway.

- [ ] **Step 6: Run the warmer test suite**

Run: `.venv/bin/pytest tests/test_cache_warmer.py -v`
Expected: every test PASSES.  If any test that was supposed to be rewritten is now FAILING because it still references removed APIs, return to Step 3 or 4 to finish the rewrite.  Do not skip, xfail, or delete a test just to make green — verify the test's intent is preserved against the new shared-cache surface.

- [ ] **Step 7: Run the full test suite**

Run: `.venv/bin/pytest tests/ -q --tb=short`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add tests/test_cache_warmer.py
git commit -m "refactor(warmer): delete dead L2 test block, rewrite warm tests against shared cache (#63)

TestCacheWarmerL2 (get/invalidate/_warm_cache/_warming_done) is gone —
its APIs no longer exist on CacheWarmer.  TestCacheWarmerWarm now
asserts on SharedLoadCache entries.  Zero references to the old L2
dict remain in src/ or tests/.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 12: Full suite + concurrency smoke run

**Files:**
- Run all tests, no new files.

- [ ] **Step 1: Run the full test suite**

Run: `.venv/bin/pytest tests/ -q --tb=short`
Expected: All tests PASS. If any fail, diagnose and fix before proceeding.

- [ ] **Step 2: Run the concurrency stress test in a loop**

Run: `.venv/bin/pytest tests/test_shared_load_cache_concurrency.py -v --count=5`
If `pytest-repeat` is not installed, fall back to a shell loop:

```bash
for i in 1 2 3 4 5; do
  .venv/bin/pytest tests/test_shared_load_cache_concurrency.py -v || exit 1
done
```

Expected: PASS on every iteration.

- [ ] **Step 3: Run conformance tests**

Run: `.venv/bin/pytest tests/test_conformance.py -q`
Expected: PASS. ZODB conformance is the strongest signal that nothing is broken in the storage semantics.

---

## Task 13: CHANGES.md and docstring refresh

**Files:**
- Modify: `CHANGES.md`
- Modify: `src/zodb_pgjsonb/storage.py` (class docstring for `PGJsonbStorage`)

- [ ] **Step 1: Prepend a CHANGES.md entry**

Insert this block right after the top `# Changelog` heading:

```markdown
## 1.12.0

- Introduce a process-wide `SharedLoadCache` that replaces per-
  connection duplication of the pickle-bytes cache (#63).  Each
  ZODB Connection still keeps a small L1 cache for lock-free hot
  reads, but shares a single L2 cache across all connections in
  the process.  On typical Plone deployments this frees ~1 GB per
  pod (5 threads × 256 MB per-instance → 1 × 256 MB shared).

- Correctness is protected by a process-wide `_consensus_tid` that
  gates cache reads and writes: an instance holding a snapshot
  older than another instance's last-polled TID is not allowed to
  read or write the cache.  Any `poll_invalidations` atomically
  advances the consensus and invalidates the changed zoids.

- Config migration:

  - New `cache-shared-mb` (default 256): size of the process-wide
    shared cache.
  - New `cache-per-connection-mb` (default 16): size of the per-
    connection L1 cache.
  - `cache-local-mb` becomes a deprecation alias for
    `cache-shared-mb`.  Existing deployments that set e.g.
    `cache-local-mb=256` automatically get a single 256 MB shared
    cache instead of 256 MB per connection — no action required
    beyond the deprecation warning.

- The learning cache warmer (`CacheWarmer`) now populates the
  shared cache directly.  Its private `_warm_cache` dict, `get()`
  and `invalidate()` are removed; the only surviving public API is
  `record()` and `warm()`.
```

- [ ] **Step 2: Update PGJsonbStorage class docstring**

In `src/zodb_pgjsonb/storage.py` around line 239, update the class docstring for `PGJsonbStorage` to mention the cache topology. Replace the existing docstring with:

```python
    """ZODB storage that stores object state as JSONB in PostgreSQL.

    Implements IMVCCStorage: ZODB.DB uses new_instance() to create
    per-connection storage instances with independent snapshots.

    Cache topology (after #63):

    - Each ``PGJsonbStorageInstance`` owns a small L1 ``LoadCache``
      sized by ``cache_per_connection_mb`` (default 16 MB).  Lock-
      free, per-connection, fast-path reads.
    - The main ``PGJsonbStorage`` owns a single process-wide
      ``SharedLoadCache`` sized by ``cache_shared_mb`` (default
      256 MB), visible to every instance.  Consensus-TID gated for
      MVCC correctness.
    - Read path: L1 → shared → PG.  A PG hit populates both.  A
      shared hit promotes to L1.

    Extends BaseStorage which handles:
    - Lock management (_lock, _commit_lock)
    - TID generation (monotonic timestamps)
    - OID allocation (new_oid)
    - 2PC protocol orchestration (tpc_begin/vote/finish/abort)

    The main storage keeps its own PG connection for schema init,
    admin queries (__len__, getSize, pack), and backward-compatible
    direct use (without ZODB.DB).
    """
```

- [ ] **Step 3: Commit**

```bash
git add CHANGES.md src/zodb_pgjsonb/storage.py
git commit -m "docs: CHANGES entry and docstring refresh for shared cache (#63)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Self-review

**1. Spec coverage:**
- Core `SharedLoadCache` data structure with get/set/poll_advance → Tasks 1-4
- Consensus-TID gating correctness invariant → Tasks 2, 4, 8
- LRU eviction + byte accounting → Task 3
- Wire into `instance.load` → Task 6
- Wire into `instance.load_multiple` → Task 7
- Wire into `poll_invalidations` → Task 8
- Warmer populates shared instead of private dict → Task 9
- Dead-code sweep (warmer L2 dict, get/invalidate, obsolete tests) → Task 11
- Config migration with deprecation → Tasks 5, 10
- Docs + CHANGES → Task 13

**2. Placeholder scan:** every step has exact code or exact command. No TBD, no "add error handling", no "similar to Task N". Code blocks are complete.

**3. Type consistency:** `SharedLoadCache.get(zoid: int, polled_tid: int | None) → (bytes, bytes) | None` stays consistent across Tasks 1-10. `poll_advance(new_tid: int, changed_zoids: list[int])` signature identical across Tasks 1, 8, 9. `set(zoid, data, tid_bytes, polled_tid)` identical everywhere.

One consistency note worth flagging for the executor: `tid_bytes` at the Python boundary and the stored entry's second element are both ZODB oid-style 8-byte big-endian TIDs; `poll_advance` takes `new_tid` as `int` (as returned by PG `SELECT MAX(tid)`). The two conventions live side by side because they match their natural sources. Don't "normalize" them.

---

Plan complete and saved to `docs/superpowers/plans/2026-04-22-shared-loadcache.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
