# Cache Warmer Herd Mitigation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate the cluster-wide cache-warmer thundering herd on rolling Kubernetes deploys (issue #59) by adding per-pod delay + jitter (A2+A1), paced batched warmup (A3), and a PG-advisory-lock semaphore that caps cluster-wide concurrent warmers (B2b).

**Architecture:** All new behavior lives inside `CacheWarmer.warm()` in `src/zodb_pgjsonb/cache_warmer.py`. Six new ZConfig keys (`cache-warm-delay`, `-jitter`, `-concurrency`, `-wait-max`, `-batch-size`, `-batch-pause`) flow through `config.py` → `PGJsonbStorage.__init__` → `CacheWarmer.__init__`. The semaphore reuses the session-level `pg_advisory_lock` pattern from `src/zodb_pgjsonb/startup_locks.py`, opening a dedicated `psycopg.connect` for the warmer's lock so a pod crash auto-releases the slot.

**Tech Stack:** Python 3.11+, psycopg 3, ZODB, ZConfig, pytest, `uv` for env management. Tests use `localhost:5433` PostgreSQL (see `tests/conftest.py:DSN`).

**Spec:** [docs/superpowers/specs/2026-05-29-cache-warmer-herd-mitigation-design.md](../specs/2026-05-29-cache-warmer-herd-mitigation-design.md)

**Default-value convention:**

- `CacheWarmer.__init__` defaults are **off** (`delay=0`, `jitter=0`, `concurrency=0`, `batch_pause=0`) — preserves backward-compat for existing direct-instantiation tests.
- `PGJsonbStorage.__init__` defaults are the **production** values (`delay=15`, `jitter=30`, `concurrency=2`, `wait_max=300`, `batch_size=500`, `batch_pause=0.5`) — actual deployments get the new behavior automatically.
- ZConfig keys default to the same production values, exposed for operator override.

**Lock keyspace constants** (defined in `cache_warmer.py`):

```python
WARMER_LOCK_NS = 0x5A4442    # shared "ZDB" namespace (matches startup_locks.py)
WARMER_SLOT_BASE = 100       # slot keys are WARMER_SLOT_BASE + i for i in 1..concurrency
```

---

## File Structure

**Modified files:**

- `src/zodb_pgjsonb/cache_warmer.py` — extend `CacheWarmer.__init__` with six new parameters; add `_acquire_slot`, `_release_slot`, `_try_acquire_slot_once` helpers; rewrite `warm()` to use delay-jitter-acquire-paced-release flow.
- `src/zodb_pgjsonb/storage.py` — extend `PGJsonbStorage.__init__` signature with the six new parameters (production defaults); forward to `CacheWarmer(...)`.
- `src/zodb_pgjsonb/config.py` — extend the kwargs dict assembled in `PGJsonbStorageFactory.open()` to include the six new ZConfig keys.
- `src/zodb_pgjsonb/component.xml` — add six new `<key>` elements with descriptions and defaults.
- `tests/test_cache_warmer.py` — add new test classes for delay/jitter, batching, slot acquisition, retry loop, release, error handling; add integration tests under `TestCacheWarmerDB`.
- `CHANGES.md` — add 1.13.0 (or 1.12.2) entry under "Features"/"Fixes" describing the six new knobs, defaults, observable behavior changes, and a pointer back to issue #59.

**Created files:** none. All new code lives inside existing files. No new module.

---

## Task 1: Extend `CacheWarmer.__init__` with six new parameters (off defaults, no behavior change yet)

**Files:**

- Modify: `src/zodb_pgjsonb/cache_warmer.py:39-60` (`CacheWarmer.__init__`)
- Test: `tests/test_cache_warmer.py` (new test method in `TestCacheWarmerRecord` or a new mini-class)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cache_warmer.py`, immediately after the `TestCacheWarmerRecord` class:

```python
class TestCacheWarmerNewKwargs:
    """Confirms the six herd-mitigation kwargs are accepted and stored
    on the instance with their defaults (off)."""

    def test_defaults_are_off(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
        )
        assert w._delay == 0
        assert w._jitter == 0
        assert w._concurrency == 0
        assert w._wait_max == 300
        assert w._batch_size == 500
        assert w._batch_pause == 0.0
        assert w._dsn is None

    def test_kwargs_override(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=foo",
            delay=15,
            jitter=30,
            concurrency=2,
            wait_max=120,
            batch_size=250,
            batch_pause=0.25,
        )
        assert w._delay == 15
        assert w._jitter == 30
        assert w._concurrency == 2
        assert w._wait_max == 120
        assert w._batch_size == 250
        assert w._batch_pause == 0.25
        assert w._dsn == "dbname=foo"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerNewKwargs -v`
Expected: FAIL — `AttributeError: 'CacheWarmer' object has no attribute '_delay'` (or `TypeError: __init__() got an unexpected keyword argument 'dsn'`).

- [ ] **Step 3: Extend `CacheWarmer.__init__`**

Modify `src/zodb_pgjsonb/cache_warmer.py` — replace the existing `__init__` (lines 39-60) with:

```python
    def __init__(
        self,
        conn,
        target_count,
        shared_cache,
        load_current_tid_fn,
        dsn=None,
        decay=0.8,
        flush_interval=1000,
        delay=0,
        jitter=0,
        concurrency=0,
        wait_max=300,
        batch_size=500,
        batch_pause=0.0,
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

        # Herd-mitigation knobs (#59).  CacheWarmer defaults are off
        # so direct instantiation in tests preserves prior behavior;
        # PGJsonbStorage / ZConfig set the production defaults.
        self._dsn = dsn
        self._delay = delay
        self._jitter = jitter
        self._concurrency = concurrency
        self._wait_max = wait_max
        self._batch_size = batch_size
        self._batch_pause = batch_pause
```

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerNewKwargs -v`
Expected: PASS (both methods).

- [ ] **Step 5: Run the full warmer test suite to confirm no regression**

Run: `uv run pytest tests/test_cache_warmer.py -v`
Expected: All pre-existing tests still pass. New `TestCacheWarmerNewKwargs` tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "feat(warmer): add six herd-mitigation kwargs (off defaults) (#59)"
```

---

## Task 2: A2 + A1 — baseline delay + jitter sleep at start of `warm()`

**Files:**

- Modify: `src/zodb_pgjsonb/cache_warmer.py:125-145` (top of `warm()`)
- Modify: `src/zodb_pgjsonb/cache_warmer.py` imports
- Test: `tests/test_cache_warmer.py` (extend `TestCacheWarmerWarm`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerWarm`:

```python
    def test_warm_sleeps_delay_plus_jitter(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            delay=15,
            jitter=30,
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(50)) for oid in oids}

        with mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep, \
             mock.patch(
                 "zodb_pgjsonb.cache_warmer.random.uniform",
                 return_value=7.0,
             ):
            w.warm(loader)

        # First sleep call should be delay + uniform(0, jitter) = 15 + 7 = 22
        assert mock_sleep.call_args_list[0] == mock.call(22.0)

    def test_warm_no_sleep_when_disabled(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            delay=0,
            jitter=0,
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(50)) for oid in oids}

        with mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep:
            w.warm(loader)

        # When both delay and jitter are zero, no initial sleep call.
        # (Subsequent calls from batching may exist; check no call with > 0.)
        for c in mock_sleep.call_args_list:
            assert c.args[0] == 0 or c.args == ()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerWarm::test_warm_sleeps_delay_plus_jitter -v`
Expected: FAIL — `IndexError: list index out of range` (no sleep was called).

- [ ] **Step 3: Add `time` and `random` imports + delay/jitter sleep at top of `warm()`**

Modify `src/zodb_pgjsonb/cache_warmer.py`. Replace the import block at lines 12-13:

```python
import contextlib
import logging
import random
import time
```

Then in `warm()`, insert the delay+jitter sleep as the very first statement of the method body (before the `from ZODB.utils import p64` line at 141):

```python
    def warm(self, load_multiple_fn):
        """Load top-N ZOIDs into the shared cache.

        Runs in a background daemon thread.  Primes the consensus TID
        on the shared cache to the current PG max_tid so that
        subsequent ``shared.set`` calls are accepted.  Re-reads
        consensus after ``poll_advance`` and uses that as the
        ``polled_tid`` for set calls — this is the mitigation for the
        startup race where an instance's poll advances consensus past
        the warmer's sampled TID before the set loop begins.

        Skips warmup when the TID is unavailable.  Logs a WARNING when
        every set() was rejected despite a non-empty result set (a
        likely sign of the race being wider than this mitigation
        covers).
        """
        # A2 + A1 (#59): get out of the pod's own cold-start window and
        # smear lock-arrival times across pods so the cluster doesn't
        # stampede the primary on rolling deploys.
        if self._delay > 0 or self._jitter > 0:
            time.sleep(self._delay + random.uniform(0, self._jitter))

        from ZODB.utils import p64
        from ZODB.utils import u64
        # ... rest unchanged
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerWarm -v`
Expected: PASS for both new tests and all pre-existing `TestCacheWarmerWarm` tests.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "feat(warmer): A2+A1 — startup delay + jitter sleep (#59)"
```

---

## Task 3: A3 — paced batched warmup

**Files:**

- Modify: `src/zodb_pgjsonb/cache_warmer.py:154-203` (load + set loop in `warm()`)
- Test: `tests/test_cache_warmer.py` (extend `TestCacheWarmerWarm`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerWarm`:

```python
    def test_warm_batches_with_pause(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        # 1100 OIDs at batch_size=500 → 3 batches (500+500+100),
        # 2 inter-batch sleeps (after batches 1 and 2, not after 3).
        top = list(range(1, 1101))
        shared = SharedLoadCache(max_mb=64)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=top),
            target_count=1100,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            batch_size=500,
            batch_pause=0.25,
        )

        call_chunks = []

        def loader(oids):
            call_chunks.append(len(oids))
            return {oid: (b"x", p64(50)) for oid in oids}

        with mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep:
            w.warm(loader)

        # Three loader calls, sizes 500, 500, 100.
        assert call_chunks == [500, 500, 100]
        # Exactly two batch_pause sleeps of 0.25 (no trailing).
        pause_calls = [c for c in mock_sleep.call_args_list if c.args == (0.25,)]
        assert len(pause_calls) == 2

    def test_warm_single_batch_no_pause(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            batch_size=500,
            batch_pause=0.5,
        )

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        with mock.patch("zodb_pgjsonb.cache_warmer.time.sleep") as mock_sleep:
            w.warm(loader)

        # Single batch → zero batch_pause sleeps.
        pause_calls = [c for c in mock_sleep.call_args_list if c.args == (0.5,)]
        assert pause_calls == []
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerWarm::test_warm_batches_with_pause tests/test_cache_warmer.py::TestCacheWarmerWarm::test_warm_single_batch_no_pause -v`
Expected: FAIL — `assert call_chunks == [500, 500, 100]` fails because loader is called once with all 1100 OIDs.

- [ ] **Step 3: Rewrite the load + set portion of `warm()` to batch**

Modify `src/zodb_pgjsonb/cache_warmer.py`. Replace lines 154-203 (everything from `oids = [p64(z) for z in top_zoids]` to the end of the function) with:

```python
        # Prime consensus so set() accepts our writes.  Another instance
        # may have advanced consensus beyond our sampled current_tid
        # already — in that case poll_advance is a no-op, and the actual
        # consensus is higher than current_tid.  Re-read it so our
        # subsequent set() calls use the effective consensus as their
        # polled_tid and pass the gate.
        self._shared_cache.poll_advance(new_tid=current_tid, changed_zoids=[])
        effective_tid = self._shared_cache.consensus_tid
        if effective_tid is None:  # guarded for paranoia; should not happen
            log.warning("Cache warmer: consensus still None after poll_advance")
            return

        # A3 (#59): batch the fetches and pause between them so a single
        # pod's warmup doesn't burst the connection pool / DB CPU.
        batch_size = max(1, self._batch_size)
        batches = [
            top_zoids[i:i + batch_size]
            for i in range(0, len(top_zoids), batch_size)
        ]
        written = 0
        attempted = 0
        for batch_idx, batch in enumerate(batches):
            oids = [p64(z) for z in batch]
            try:
                results = load_multiple_fn(oids)
            except Exception:
                log.warning(
                    "Cache warmer: load_multiple failed at batch %d/%d",
                    batch_idx + 1, len(batches),
                    exc_info=True,
                )
                return
            for oid, (data, tid_bytes) in results.items():
                attempted += 1
                if self._shared_cache.set(
                    zoid=u64(oid),
                    data=data,
                    tid_bytes=tid_bytes,
                    polled_tid=effective_tid,
                ):
                    written += 1
            # Pause only between batches, never after the last.
            if batch_idx < len(batches) - 1 and self._batch_pause > 0:
                time.sleep(self._batch_pause)

        if attempted == 0:
            log.info("Cache warmer: load_multiple returned no objects")
            return

        if written == 0:
            log.warning(
                "Cache warmer: all %d set() calls rejected by shared cache "
                "(consensus=%d, sampled_tid=%d) — likely raced with a "
                "concurrent instance poll",
                attempted,
                effective_tid,
                current_tid,
            )
        else:
            log.info(
                "Cache warmer: loaded %d of %d objects into shared cache "
                "(%d batches)",
                written,
                attempted,
                len(batches),
            )
```

Note: this replaces the prior `try/except` around the single `load_multiple_fn` call (lines 155-159) and the prior `if not results` early-return (lines 161-163). The empty-result case is now detected via `attempted == 0` after the loop.

- [ ] **Step 4: Run all warmer tests**

Run: `uv run pytest tests/test_cache_warmer.py -v`
Expected: All pre-existing tests still pass (including the prior `test_warm_handles_load_multiple_exception` since the per-batch `try/except` still catches the exception). Both new tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "feat(warmer): A3 — paced batched warmup (#59)"
```

---

## Task 4: B2b — `_try_acquire_slot_once` helper (single attempt across all slots)

**Files:**

- Modify: `src/zodb_pgjsonb/cache_warmer.py` (add module-level constants + new method)
- Test: `tests/test_cache_warmer.py` (new `TestCacheWarmerSlot` class)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_cache_warmer.py`, after `TestCacheWarmerWarm`:

```python
class TestCacheWarmerSlot:
    """Unit tests for the B2b advisory-lock slot helpers."""

    def _make_lock_conn(self, slot_results):
        """Return a mock connection whose execute('SELECT pg_try_advisory_lock(...)')
        returns ``slot_results[i]`` on the i-th call (one row, one column).
        """
        call_count = [0]

        class _LockCursor:
            def __init__(self):
                self._row = None

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def execute(self, sql, params=None):
                if "pg_try_advisory_lock" in sql:
                    i = call_count[0]
                    call_count[0] += 1
                    self._row = (slot_results[i],) if i < len(slot_results) else (False,)
                else:
                    self._row = None
                return self

            def fetchone(self):
                return self._row

        class _LockConn:
            def cursor(self):
                return _LockCursor()

            def execute(self, *a, **kw):
                return None

            def close(self):
                pass

        return _LockConn()

    def test_try_acquire_slot_returns_slot_when_first_free(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=3,
        )
        lock_conn = self._make_lock_conn(slot_results=[True])
        slot = w._try_acquire_slot_once(lock_conn)
        assert slot == 1

    def test_try_acquire_slot_returns_higher_slot_when_first_taken(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=3,
        )
        lock_conn = self._make_lock_conn(slot_results=[False, False, True])
        slot = w._try_acquire_slot_once(lock_conn)
        assert slot == 3

    def test_try_acquire_slot_returns_none_when_all_taken(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=3,
        )
        lock_conn = self._make_lock_conn(slot_results=[False, False, False])
        slot = w._try_acquire_slot_once(lock_conn)
        assert slot is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerSlot -v`
Expected: FAIL — `AttributeError: 'CacheWarmer' object has no attribute '_try_acquire_slot_once'`.

- [ ] **Step 3: Add constants + `_try_acquire_slot_once` method**

Modify `src/zodb_pgjsonb/cache_warmer.py`. After the existing `WARM_STATS_DDL = ...` block (around line 24), add:

```python
# Advisory-lock keyspace for B2b inter-pod warmer semaphore (#59).
# Same namespace as startup_locks.py (separate keys).
WARMER_LOCK_NS = 0x5A4442    # "ZDB"
WARMER_SLOT_BASE = 100       # slot keys are WARMER_SLOT_BASE + i for i in 1..N
```

Then add a new method on `CacheWarmer` (place it just before the `# ── Warming phase ──` comment, so it sits between the recording phase and the warming phase):

```python
    # ── B2b slot acquisition (#59) ───────────────────────────────────

    def _try_acquire_slot_once(self, lock_conn):
        """Try each slot 1..concurrency once.  Returns the acquired
        slot number, or None if all slots are taken.

        ``lock_conn`` must be an autocommit psycopg connection
        dedicated to holding the session-level advisory lock.
        """
        for slot in range(1, self._concurrency + 1):
            key = WARMER_SLOT_BASE + slot
            try:
                with lock_conn.cursor() as cur:
                    cur.execute(
                        "SELECT pg_try_advisory_lock(%s, %s)",
                        (WARMER_LOCK_NS, key),
                    )
                    row = cur.fetchone()
            except Exception:
                log.warning(
                    "Cache warmer: pg_try_advisory_lock raised for slot %d",
                    slot,
                    exc_info=True,
                )
                return None
            if row and row[0]:
                return slot
        return None
```

Note: `row[0]` indexes the tuple from a default psycopg cursor (the `_LockCursor` in the test returns a tuple). The real production lock connection is opened without `row_factory=dict_row`, so it also yields tuples. This is intentional — the dedicated lock connection is plain.

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerSlot -v`
Expected: PASS (all three tests).

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "feat(warmer): B2b — _try_acquire_slot_once helper (#59)"
```

---

## Task 5: B2b — `_acquire_slot` with retry loop + wait cap

**Files:**

- Modify: `src/zodb_pgjsonb/cache_warmer.py` (add new method + module-level helper)
- Test: `tests/test_cache_warmer.py` (extend `TestCacheWarmerSlot`)

- [ ] **Step 1: Write the failing tests**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerSlot`:

```python
    def test_acquire_slot_succeeds_after_retries(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        # First two attempts: all slots taken.  Third attempt: slot 1 free.
        # With concurrency=2 that's 2 + 2 + 1 = 5 try-lock calls before success.
        slot_results = [False, False] + [False, False] + [True]
        lock_conn = self._make_lock_conn(slot_results=slot_results)

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
            wait_max=30,
        )

        # Patch psycopg.connect to return our pre-built lock_conn, and
        # patch sleep so the test doesn't actually wait.
        with mock.patch(
            "zodb_pgjsonb.cache_warmer.psycopg.connect",
            return_value=lock_conn,
        ), mock.patch(
            "zodb_pgjsonb.cache_warmer.time.sleep"
        ), mock.patch(
            "zodb_pgjsonb.cache_warmer.random.uniform",
            return_value=3.0,
        ), mock.patch(
            "zodb_pgjsonb.cache_warmer.time.monotonic",
            side_effect=[0, 3, 6, 9],
        ):
            conn, slot = w._acquire_slot()

        assert conn is lock_conn
        assert slot == 1

    def test_acquire_slot_times_out(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        # All slots permanently taken.
        lock_conn = self._make_lock_conn(slot_results=[False] * 100)

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
            wait_max=10,
        )

        # monotonic side_effect: simulate 0s, 3s, 6s, 9s, 12s — wait_max hit.
        with mock.patch(
            "zodb_pgjsonb.cache_warmer.psycopg.connect",
            return_value=lock_conn,
        ), mock.patch(
            "zodb_pgjsonb.cache_warmer.time.sleep"
        ), mock.patch(
            "zodb_pgjsonb.cache_warmer.random.uniform",
            return_value=3.0,
        ), mock.patch(
            "zodb_pgjsonb.cache_warmer.time.monotonic",
            side_effect=[0, 3, 6, 9, 12],
        ):
            result = w._acquire_slot()

        assert result is None

    def test_acquire_slot_handles_connect_failure(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
            wait_max=30,
        )

        with mock.patch(
            "zodb_pgjsonb.cache_warmer.psycopg.connect",
            side_effect=RuntimeError("connect refused"),
        ):
            result = w._acquire_slot()

        assert result is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerSlot -v -k acquire_slot`
Expected: FAIL — `AttributeError: ... '_acquire_slot'`.

- [ ] **Step 3: Add `psycopg` import + `_acquire_slot` method**

Modify `src/zodb_pgjsonb/cache_warmer.py`. Add to the import block at the top:

```python
import contextlib
import logging
import random
import time

import psycopg
```

Then add the `_acquire_slot` method on `CacheWarmer`, immediately after `_try_acquire_slot_once`:

```python
    def _acquire_slot(self):
        """Acquire one of ``concurrency`` cluster-wide slots.

        Opens a dedicated autocommit psycopg connection and tries each
        slot via ``_try_acquire_slot_once`` in a retry loop.  Returns
        ``(lock_conn, slot)`` on success or ``None`` on timeout / open
        failure.

        Caller is responsible for releasing the slot and closing the
        connection (see ``_release_slot``).  Session-level advisory
        locks auto-release on connection close, so a pod crash mid-warm
        safely frees the slot.
        """
        if self._dsn is None or self._concurrency < 1:
            return None
        try:
            lock_conn = psycopg.connect(self._dsn, autocommit=True)
        except Exception:
            log.warning(
                "Cache warmer: failed to open lock connection, skipping warmup",
                exc_info=True,
            )
            return None

        started = time.monotonic()
        attempt = 0
        try:
            while True:
                attempt += 1
                slot = self._try_acquire_slot_once(lock_conn)
                if slot is not None:
                    waited = time.monotonic() - started
                    log.info(
                        "Cache warmer: acquired slot %d after %.1fs wait "
                        "(attempt %d)",
                        slot, waited, attempt,
                    )
                    return lock_conn, slot

                waited = time.monotonic() - started
                if waited >= self._wait_max:
                    log.warning(
                        "Cache warmer: gave up after %.1fs waiting for slot "
                        "(%d attempts), skipping warmup",
                        waited, attempt,
                    )
                    return None

                log.info(
                    "Cache warmer: all %d slots taken, retrying "
                    "(waited %.1fs so far)",
                    self._concurrency, waited,
                )
                time.sleep(random.uniform(2.0, 5.0))
        except Exception:
            log.warning(
                "Cache warmer: unexpected error in slot acquisition",
                exc_info=True,
            )
            with contextlib.suppress(Exception):
                lock_conn.close()
            return None
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerSlot -v`
Expected: PASS (all six tests including the new three).

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "feat(warmer): B2b — _acquire_slot with retry loop and wait cap (#59)"
```

---

## Task 6: B2b — `_release_slot` (release + close lock connection)

**Files:**

- Modify: `src/zodb_pgjsonb/cache_warmer.py` (add `_release_slot` method)
- Test: `tests/test_cache_warmer.py` (extend `TestCacheWarmerSlot`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerSlot`:

```python
    def test_release_slot_unlocks_and_closes(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.cache_warmer import WARMER_LOCK_NS
        from zodb_pgjsonb.cache_warmer import WARMER_SLOT_BASE

        executed = []
        closed = []

        class _ReleaseConn:
            def execute(self, sql, params=None):
                executed.append((sql, params))

            def close(self):
                closed.append(True)

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=2,
        )
        lock_conn = _ReleaseConn()
        w._release_slot(lock_conn, slot=2)

        assert len(executed) == 1
        sql, params = executed[0]
        assert "pg_advisory_unlock" in sql
        assert params == (WARMER_LOCK_NS, WARMER_SLOT_BASE + 2)
        assert closed == [True]

    def test_release_slot_swallows_errors(self):
        from zodb_pgjsonb.cache_warmer import CacheWarmer

        class _BadConn:
            def execute(self, sql, params=None):
                raise RuntimeError("connection gone")

            def close(self):
                raise RuntimeError("close gone too")

        w = CacheWarmer(
            conn=mock.Mock(),
            target_count=10,
            shared_cache=_mk_shared_cache(),
            load_current_tid_fn=lambda: 100,
            concurrency=2,
        )
        # Must not raise.
        w._release_slot(_BadConn(), slot=1)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerSlot -v -k release_slot`
Expected: FAIL — `AttributeError: ... '_release_slot'`.

- [ ] **Step 3: Add `_release_slot` method**

In `src/zodb_pgjsonb/cache_warmer.py`, after `_acquire_slot`, add:

```python
    def _release_slot(self, lock_conn, slot):
        """Release the advisory lock and close the dedicated connection.

        Errors are logged and suppressed — PG auto-releases session
        locks on connection close, so a failed unlock is recoverable.
        """
        key = WARMER_SLOT_BASE + slot
        try:
            lock_conn.execute(
                "SELECT pg_advisory_unlock(%s, %s)",
                (WARMER_LOCK_NS, key),
            )
        except Exception:
            log.warning(
                "Cache warmer: pg_advisory_unlock failed for slot %d",
                slot,
                exc_info=True,
            )
        with contextlib.suppress(Exception):
            lock_conn.close()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerSlot -v`
Expected: PASS (all eight tests).

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "feat(warmer): B2b — _release_slot helper (#59)"
```

---

## Task 7: Integrate slot lifecycle into `warm()`

**Files:**

- Modify: `src/zodb_pgjsonb/cache_warmer.py:warm()` (wrap warming with acquire/release)
- Test: `tests/test_cache_warmer.py` (extend `TestCacheWarmerWarm`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerWarm`:

```python
    def test_warm_acquires_and_releases_slot(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
        )
        # Pretend slot acquisition succeeds without touching PG.
        acquire_calls = []
        release_calls = []

        def fake_acquire():
            sentinel = object()
            acquire_calls.append(sentinel)
            return (sentinel, 1)

        def fake_release(lock_conn, slot):
            release_calls.append((lock_conn, slot))

        w._acquire_slot = fake_acquire
        w._release_slot = fake_release

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        w.warm(loader)
        assert len(acquire_calls) == 1
        assert len(release_calls) == 1
        assert release_calls[0][1] == 1  # slot number
        assert release_calls[0][0] is acquire_calls[0]  # same conn

    def test_warm_skips_on_acquire_failure(self):
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            dsn="dbname=ignored",
            concurrency=2,
        )
        w._acquire_slot = lambda: None

        load_called = []

        def loader(oids):
            load_called.append(oids)
            return {}

        w.warm(loader)
        # No load when acquire fails.
        assert load_called == []
        assert len(shared._cache) == 0

    def test_warm_no_slot_when_concurrency_zero(self):
        """When concurrency=0, the slot path is bypassed entirely
        (preserves backward-compat for direct test instantiation)."""
        from ZODB.utils import p64
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
            concurrency=0,
        )
        acquired = []
        w._acquire_slot = lambda: acquired.append(True) or None

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        w.warm(loader)
        # _acquire_slot must not be called when concurrency=0.
        assert acquired == []
        # Warming still happens.
        assert len(shared._cache) == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerWarm -v -k 'acquire or skips_on_acquire or concurrency_zero'`
Expected: FAIL — `assert len(acquire_calls) == 1` fails because `warm()` doesn't call `_acquire_slot`.

- [ ] **Step 3: Wrap warming with acquire/release**

Modify `src/zodb_pgjsonb/cache_warmer.py:warm()`. The current structure is:

```python
def warm(self, load_multiple_fn):
    """..."""
    # A2 + A1 sleep
    ...
    from ZODB.utils import p64
    from ZODB.utils import u64

    top_zoids = self._read_top_oids()
    if not top_zoids:
        ...
        return

    current_tid = self._load_current_tid_fn()
    ...
    # batched load + set
```

Insert slot acquisition between the sleep and `_read_top_oids`, and wrap the rest in a try/finally for release. Replace the body of `warm()` from the A2+A1 sleep onwards with:

```python
        # A2 + A1 (#59): get out of the pod's own cold-start window and
        # smear lock-arrival times across pods so the cluster doesn't
        # stampede the primary on rolling deploys.
        if self._delay > 0 or self._jitter > 0:
            time.sleep(self._delay + random.uniform(0, self._jitter))

        # B2b (#59): try to claim one of N cluster-wide slots.  When
        # concurrency is 0 or no DSN is configured, the semaphore is
        # disabled and warming proceeds unconditionally.
        lock_conn = None
        slot = None
        if self._concurrency >= 1:
            acquired = self._acquire_slot()
            if acquired is None:
                # Acquire failed (timeout or connect error); already
                # logged inside _acquire_slot.
                return
            lock_conn, slot = acquired

        try:
            self._warm_body(load_multiple_fn)
        finally:
            if lock_conn is not None:
                self._release_slot(lock_conn, slot)
```

Replace the entire `warm()` method in `src/zodb_pgjsonb/cache_warmer.py` with the following two methods (the Task 3 batched-load logic moves into `_warm_body`):

```python
    def warm(self, load_multiple_fn):
        """Load top-N ZOIDs into the shared cache.

        Runs in a background daemon thread.  Orchestrates the four
        herd-mitigation phases (#59): startup delay + jitter (A2+A1),
        cluster-wide slot acquisition (B2b), paced batched warmup (A3),
        slot release.
        """
        # A2 + A1: get out of the pod's own cold-start window and
        # smear lock-arrival times across pods so the cluster doesn't
        # stampede the primary on rolling deploys.
        if self._delay > 0 or self._jitter > 0:
            time.sleep(self._delay + random.uniform(0, self._jitter))

        # B2b: try to claim one of N cluster-wide slots.  When
        # concurrency is 0 the semaphore is disabled.
        lock_conn = None
        slot = None
        if self._concurrency >= 1:
            acquired = self._acquire_slot()
            if acquired is None:
                # Already logged inside _acquire_slot (timeout or open failure).
                return
            lock_conn, slot = acquired

        try:
            self._warm_body(load_multiple_fn)
        finally:
            if lock_conn is not None:
                self._release_slot(lock_conn, slot)

    def _warm_body(self, load_multiple_fn):
        """Top-OID read + prime-consensus + paced batched load+set.

        Primes the consensus TID on the shared cache to the current PG
        max_tid so that subsequent ``shared.set`` calls are accepted.
        Re-reads consensus after ``poll_advance`` and uses that as the
        ``polled_tid`` for set calls — this is the mitigation for the
        startup race where an instance's poll advances consensus past
        the warmer's sampled TID before the set loop begins.

        Skips warmup when the TID is unavailable.  Logs a WARNING when
        every set() was rejected despite a non-empty result set.
        """
        from ZODB.utils import p64
        from ZODB.utils import u64

        top_zoids = self._read_top_oids()
        if not top_zoids:
            log.info("Cache warmer: no stats yet, skipping warmup")
            return

        current_tid = self._load_current_tid_fn()
        if current_tid is None:
            log.warning("Cache warmer: could not read current TID, skipping warmup")
            return

        # Prime consensus so set() accepts our writes.  Another instance
        # may have advanced consensus beyond our sampled current_tid
        # already — in that case poll_advance is a no-op, and the actual
        # consensus is higher than current_tid.  Re-read it so our
        # subsequent set() calls use the effective consensus as their
        # polled_tid and pass the gate.
        self._shared_cache.poll_advance(new_tid=current_tid, changed_zoids=[])
        effective_tid = self._shared_cache.consensus_tid
        if effective_tid is None:  # guarded for paranoia; should not happen
            log.warning("Cache warmer: consensus still None after poll_advance")
            return

        # A3: batch the fetches and pause between them so a single
        # pod's warmup doesn't burst the connection pool / DB CPU.
        batch_size = max(1, self._batch_size)
        batches = [
            top_zoids[i:i + batch_size]
            for i in range(0, len(top_zoids), batch_size)
        ]
        written = 0
        attempted = 0
        for batch_idx, batch in enumerate(batches):
            oids = [p64(z) for z in batch]
            try:
                results = load_multiple_fn(oids)
            except Exception:
                log.warning(
                    "Cache warmer: load_multiple failed at batch %d/%d",
                    batch_idx + 1, len(batches),
                    exc_info=True,
                )
                return
            for oid, (data, tid_bytes) in results.items():
                attempted += 1
                if self._shared_cache.set(
                    zoid=u64(oid),
                    data=data,
                    tid_bytes=tid_bytes,
                    polled_tid=effective_tid,
                ):
                    written += 1
            # Pause only between batches, never after the last.
            if batch_idx < len(batches) - 1 and self._batch_pause > 0:
                time.sleep(self._batch_pause)

        if attempted == 0:
            log.info("Cache warmer: load_multiple returned no objects")
            return

        if written == 0:
            log.warning(
                "Cache warmer: all %d set() calls rejected by shared cache "
                "(consensus=%d, sampled_tid=%d) — likely raced with a "
                "concurrent instance poll",
                attempted,
                effective_tid,
                current_tid,
            )
        else:
            log.info(
                "Cache warmer: loaded %d of %d objects into shared cache "
                "(%d batches)",
                written,
                attempted,
                len(batches),
            )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_cache_warmer.py -v`
Expected: All tests pass — the three new tests plus all pre-existing ones.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "feat(warmer): B2b — integrate slot lifecycle into warm() (#59)"
```

---

## Task 8: Storage.py wiring — production defaults + DSN passthrough

**Files:**

- Modify: `src/zodb_pgjsonb/storage.py:380-397` (`PGJsonbStorage.__init__` signature) and lines 527-541 (CacheWarmer instantiation)
- Test: existing `tests/test_storage*.py` (regression check only)

- [ ] **Step 1: Extend `PGJsonbStorage.__init__` signature**

Modify `src/zodb_pgjsonb/storage.py`. Find the existing `__init__` signature (around line 380):

```python
    def __init__(
        self,
        dsn,
        name="pgjsonb",
        history_preserving=False,
        blob_temp_dir=None,
        cache_local_mb=None,  # deprecated alias for cache_shared_mb
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
```

Add the six new keyword arguments at the end (preserving the existing kwargs and their defaults):

```python
    def __init__(
        self,
        dsn,
        name="pgjsonb",
        history_preserving=False,
        blob_temp_dir=None,
        cache_local_mb=None,  # deprecated alias for cache_shared_mb
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
        cache_warm_delay=15,
        cache_warm_jitter=30,
        cache_warm_concurrency=2,
        cache_warm_wait_max=300,
        cache_warm_batch_size=500,
        cache_warm_batch_pause=0.5,
    ):
```

- [ ] **Step 2: Forward new kwargs to CacheWarmer**

In `src/zodb_pgjsonb/storage.py`, find the existing `self._warmer = CacheWarmer(...)` call (around line 527) and extend it to:

```python
            self._warmer = CacheWarmer(
                self._conn,
                target_count=target,
                shared_cache=self._shared_cache,
                load_current_tid_fn=self.current_max_tid,
                dsn=self._dsn,
                decay=cache_warm_decay,
                delay=cache_warm_delay,
                jitter=cache_warm_jitter,
                concurrency=cache_warm_concurrency,
                wait_max=cache_warm_wait_max,
                batch_size=cache_warm_batch_size,
                batch_pause=cache_warm_batch_pause,
            )
```

- [ ] **Step 3: Update the existing INFO log**

Find the existing `logger.info("Cache warmer started (target=%d, decay=%.1f)", ...)` (around line 542) and replace with:

```python
            logger.info(
                "Cache warmer started (target=%d, decay=%.1f, delay=%ds, "
                "jitter=%ds, concurrency=%d, batch_size=%d, batch_pause=%.2fs)",
                target,
                cache_warm_decay,
                cache_warm_delay,
                cache_warm_jitter,
                cache_warm_concurrency,
                cache_warm_batch_size,
                cache_warm_batch_pause,
            )
```

- [ ] **Step 4: Run full storage and warmer test suite**

Run: `uv run pytest tests/test_storage.py tests/test_cache_warmer.py -v`
Expected: All pre-existing tests pass. The new behavior is now active when `PGJsonbStorage` is constructed without overrides, but storage tests typically pass `cache_warm_pct=0` to disable the warmer entirely, so there is no observable change in those tests.

If any storage test was relying on `CacheWarmer.__init__` having only its prior kwargs, update it to either set `cache_warm_pct=0` or pass the new kwargs explicitly.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/storage.py
git commit -m "feat(storage): wire herd-mitigation knobs into CacheWarmer (#59)"
```

---

## Task 9: ZConfig keys + factory passthrough

**Files:**

- Modify: `src/zodb_pgjsonb/component.xml` (add six `<key>` elements)
- Modify: `src/zodb_pgjsonb/config.py:70-90` (add to kwargs dict)
- Test: `tests/test_config.py` if it exists, otherwise a smoke test

- [ ] **Step 1: Add six ZConfig `<key>` elements**

Modify `src/zodb_pgjsonb/component.xml`. Find the existing `<key name="cache-warm-decay" ...>` block (around line 90). Immediately after its closing `</key>`, insert:

```xml
    <key name="cache-warm-delay" datatype="integer" default="15">
      <description>
        Baseline sleep (seconds) before the cache warmer starts work.
        Moves the warmer out of the pod's own cold-start window so it
        does not compete with Plone import + plone.pgcatalog schema
        checks + ANALYZE chatter.  Set to 0 to disable.  Default: 15.
        See issue #59.
      </description>
    </key>

    <key name="cache-warm-jitter" datatype="integer" default="30">
      <description>
        Additional uniform-random sleep (0..jitter seconds) on top of
        cache-warm-delay.  Spreads warmer arrival times across pods so
        a rolling deploy does not produce a synchronous DB burst.  Set
        to 0 to disable.  Default: 30.  See issue #59.
      </description>
    </key>

    <key name="cache-warm-concurrency" datatype="integer" default="2">
      <description>
        Maximum number of pods warming concurrently in the cluster,
        enforced via PostgreSQL advisory locks.  Set to a very large
        value (e.g. 9999) to effectively disable the cap.  Default: 2.
        See issue #59.
      </description>
    </key>

    <key name="cache-warm-wait-max" datatype="integer" default="300">
      <description>
        Maximum time (seconds) to wait for a free slot before giving
        up and skipping warmup (logs a WARNING).  Default: 300.  See
        issue #59.
      </description>
    </key>

    <key name="cache-warm-batch-size" datatype="integer" default="500">
      <description>
        Number of ZOIDs fetched per SELECT batch during warmup.  Lower
        values reduce peak qps at the cost of longer warmup duration.
        Default: 500.  See issue #59.
      </description>
    </key>

    <key name="cache-warm-batch-pause" datatype="float" default="0.5">
      <description>
        Sleep (seconds) between warmup batches.  Lowers per-pod peak
        qps.  Set to 0 to disable.  Default: 0.5.  See issue #59.
      </description>
    </key>
```

- [ ] **Step 2: Add the six keys to the factory kwargs dict**

Modify `src/zodb_pgjsonb/config.py`. Find the existing kwargs dict (around lines 70-83) that ends with `"cache_warm_decay": getattr(config, "cache_warm_decay", 0.8),`. Extend it:

```python
            "cache_warm_pct": getattr(config, "cache_warm_pct", 10),
            "cache_warm_decay": getattr(config, "cache_warm_decay", 0.8),
            "cache_warm_delay": getattr(config, "cache_warm_delay", 15),
            "cache_warm_jitter": getattr(config, "cache_warm_jitter", 30),
            "cache_warm_concurrency": getattr(
                config, "cache_warm_concurrency", 2
            ),
            "cache_warm_wait_max": getattr(config, "cache_warm_wait_max", 300),
            "cache_warm_batch_size": getattr(
                config, "cache_warm_batch_size", 500
            ),
            "cache_warm_batch_pause": getattr(
                config, "cache_warm_batch_pause", 0.5
            ),
        }
```

- [ ] **Step 3: Smoke-test the ZConfig parse path**

Find any existing test that parses a `<pgjsonb>` block (e.g. `tests/test_config.py` if present, or grep for `PGJsonbStorageFactory` in tests). If none exists, add to `tests/test_config.py` (create the file if needed):

```python
"""Smoke tests for the ZConfig factory wiring."""

from io import StringIO
import pytest
import ZConfig


SCHEMA = """\
<schema>
    <import package="zodb_pgjsonb" />
    <section type="pgjsonb" name="*" attribute="storage" />
</schema>
"""


def test_warm_knobs_default_through_factory(tmp_path):
    cfg_text = """\
%import zodb_pgjsonb
<pgjsonb test>
    dsn dbname=test
</pgjsonb>
"""
    schema = ZConfig.loadSchemaFile(StringIO(SCHEMA))
    cfg, _ = ZConfig.loadConfigFile(schema, StringIO(cfg_text))
    section = cfg.storage.config
    assert getattr(section, "cache_warm_delay", None) == 15
    assert getattr(section, "cache_warm_jitter", None) == 30
    assert getattr(section, "cache_warm_concurrency", None) == 2
    assert getattr(section, "cache_warm_wait_max", None) == 300
    assert getattr(section, "cache_warm_batch_size", None) == 500
    assert getattr(section, "cache_warm_batch_pause", None) == 0.5


def test_warm_knobs_overridden_through_factory():
    cfg_text = """\
%import zodb_pgjsonb
<pgjsonb test>
    dsn dbname=test
    cache-warm-delay 0
    cache-warm-jitter 0
    cache-warm-concurrency 9999
    cache-warm-batch-pause 0
</pgjsonb>
"""
    schema = ZConfig.loadSchemaFile(StringIO(SCHEMA))
    cfg, _ = ZConfig.loadConfigFile(schema, StringIO(cfg_text))
    section = cfg.storage.config
    assert section.cache_warm_delay == 0
    assert section.cache_warm_jitter == 0
    assert section.cache_warm_concurrency == 9999
    assert section.cache_warm_batch_pause == 0
```

- [ ] **Step 4: Run the smoke test**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS for both new tests.

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/component.xml src/zodb_pgjsonb/config.py tests/test_config.py
git commit -m "feat(config): six ZConfig keys for cache-warm herd mitigation (#59)"
```

---

## Task 10: DB integration test — slot serialization with `concurrency=1`

**Files:**

- Modify: `tests/test_cache_warmer.py` (extend `TestCacheWarmerDB`)

- [ ] **Step 1: Write the failing test**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerDB`:

```python
    def test_concurrency_1_serializes_two_warmers(self):
        """Two warmers with concurrency=1 must not warm in parallel."""
        from ZODB.utils import p64
        from tests.conftest import DSN
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        import threading
        import time

        # Seed warm stats so the warmers have something to do.
        self.conn.execute(
            "INSERT INTO cache_warm_stats (zoid, score) "
            "VALUES (1, 5.0), (2, 4.0), (3, 3.0)"
        )

        shared = SharedLoadCache(max_mb=4)
        load_times = []

        def slow_loader(oids):
            load_times.append(("enter", time.monotonic()))
            time.sleep(0.5)  # simulate slow load
            load_times.append(("exit", time.monotonic()))
            return {oid: (b"x", p64(50)) for oid in oids}

        def run_warmer():
            w = CacheWarmer(
                conn=self.conn,
                target_count=10,
                shared_cache=shared,
                load_current_tid_fn=lambda: 100,
                dsn=DSN,
                concurrency=1,
                wait_max=30,
                batch_size=500,
                batch_pause=0,
            )
            w.warm(slow_loader)

        t1 = threading.Thread(target=run_warmer)
        t2 = threading.Thread(target=run_warmer)
        t1.start(); t2.start()
        t1.join(); t2.join()

        # Two enter/exit pairs, non-overlapping.
        enters = sorted(t for k, t in load_times if k == "enter")
        exits = sorted(t for k, t in load_times if k == "exit")
        assert len(enters) == 2 and len(exits) == 2
        # Either exits[0] <= enters[1] (serialized) — strict serialization
        # by the advisory lock.
        assert exits[0] <= enters[1] + 0.05  # 50ms slack for thread sched
```

- [ ] **Step 2: Run the test to verify it fails before implementation is fully wired**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerDB::test_concurrency_1_serializes_two_warmers -v`
Expected: This test should already PASS if Tasks 1-7 are wired correctly, because all the production code is already in place. (This is an integration test, not a TDD step — its purpose is to confirm the end-to-end wiring is correct against a real PostgreSQL.)

If it FAILS, debug the integration before continuing — likely culprits: lock-connection not opened in autocommit, wrong key namespace, slot accounting off by one.

- [ ] **Step 3: Commit**

```bash
git add tests/test_cache_warmer.py
git commit -m "test(warmer): DB integration — slot serialization with concurrency=1 (#59)"
```

---

## Task 11: DB integration test — N-slot semaphore with `concurrency=2`

**Files:**

- Modify: `tests/test_cache_warmer.py` (extend `TestCacheWarmerDB`)

- [ ] **Step 1: Write the test**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerDB`:

```python
    def test_concurrency_2_allows_two_parallel_one_waits(self):
        """Three warmers, concurrency=2 — two warm in parallel, third waits."""
        from ZODB.utils import p64
        from tests.conftest import DSN
        from zodb_pgjsonb.cache_warmer import CacheWarmer
        from zodb_pgjsonb.storage import SharedLoadCache

        import threading
        import time

        self.conn.execute(
            "INSERT INTO cache_warm_stats (zoid, score) "
            "VALUES (1, 5.0), (2, 4.0), (3, 3.0)"
        )

        shared = SharedLoadCache(max_mb=4)
        load_events = []
        load_events_lock = threading.Lock()

        def slow_loader(oids):
            with load_events_lock:
                load_events.append(("enter", time.monotonic()))
            time.sleep(0.5)
            with load_events_lock:
                load_events.append(("exit", time.monotonic()))
            return {oid: (b"x", p64(50)) for oid in oids}

        def run_warmer():
            w = CacheWarmer(
                conn=self.conn,
                target_count=10,
                shared_cache=shared,
                load_current_tid_fn=lambda: 100,
                dsn=DSN,
                concurrency=2,
                wait_max=30,
                batch_size=500,
                batch_pause=0,
            )
            w.warm(slow_loader)

        threads = [threading.Thread(target=run_warmer) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        enters = sorted(t for k, t in load_events if k == "enter")
        exits = sorted(t for k, t in load_events if k == "exit")
        assert len(enters) == 3 and len(exits) == 3
        # First two enters within a small window (parallel), third
        # enter must be after at least one exit.
        assert enters[1] - enters[0] < 0.3
        assert enters[2] >= exits[0] - 0.05  # 50ms slack
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerDB::test_concurrency_2_allows_two_parallel_one_waits -v`
Expected: PASS.

If FAIL, check that `_acquire_slot`'s retry loop uses the configured `wait_max` correctly and that the retry sleep is short enough to let a freed slot be picked up within the test window.

- [ ] **Step 3: Commit**

```bash
git add tests/test_cache_warmer.py
git commit -m "test(warmer): DB integration — N-slot semaphore (concurrency=2) (#59)"
```

---

## Task 12: DB integration test — crash-safe slot release on connection close

**Files:**

- Modify: `tests/test_cache_warmer.py` (extend `TestCacheWarmerDB`)

- [ ] **Step 1: Write the test**

Add to `tests/test_cache_warmer.py` inside `TestCacheWarmerDB`:

```python
    def test_lock_released_on_connection_close(self):
        """If the warmer's lock connection closes (e.g. pod crash),
        PG auto-releases the session-level lock so another warmer can
        proceed."""
        from tests.conftest import DSN
        from zodb_pgjsonb.cache_warmer import (
            CacheWarmer,
            WARMER_LOCK_NS,
            WARMER_SLOT_BASE,
        )

        import psycopg
        from psycopg.rows import dict_row

        # Pod 1: open a connection, acquire slot 1 manually.
        conn1 = psycopg.connect(DSN, autocommit=True)
        with conn1.cursor() as cur:
            cur.execute(
                "SELECT pg_try_advisory_lock(%s, %s)",
                (WARMER_LOCK_NS, WARMER_SLOT_BASE + 1),
            )
            assert cur.fetchone()[0] is True

        # Pod 2: a fresh warmer with concurrency=1 must time out quickly
        # because slot 1 is held.
        w = CacheWarmer(
            conn=self.conn,
            target_count=10,
            shared_cache=type("S", (), {
                "poll_advance": lambda *a, **k: None,
                "set": lambda *a, **k: True,
                "consensus_tid": 1,
            })(),
            load_current_tid_fn=lambda: 100,
            dsn=DSN,
            concurrency=1,
            wait_max=3,  # very short, just to confirm contention
        )
        assert w._acquire_slot() is None

        # Now close pod 1's connection — auto-releases the lock.
        conn1.close()

        # Pod 3: a new warmer should now acquire slot 1.
        w2 = CacheWarmer(
            conn=self.conn,
            target_count=10,
            shared_cache=type("S", (), {
                "poll_advance": lambda *a, **k: None,
                "set": lambda *a, **k: True,
                "consensus_tid": 1,
            })(),
            load_current_tid_fn=lambda: 100,
            dsn=DSN,
            concurrency=1,
            wait_max=10,
        )
        result = w2._acquire_slot()
        assert result is not None
        lock_conn, slot = result
        assert slot == 1
        # Clean up.
        w2._release_slot(lock_conn, slot)
```

- [ ] **Step 2: Run the test**

Run: `uv run pytest tests/test_cache_warmer.py::TestCacheWarmerDB::test_lock_released_on_connection_close -v`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add tests/test_cache_warmer.py
git commit -m "test(warmer): DB integration — crash-safe slot release (#59)"
```

---

## Task 13: CHANGES.md entry

**Files:**

- Modify: `CHANGES.md` (add a new top entry above 1.12.0)

- [ ] **Step 1: Add changelog entry**

Read the current `CHANGES.md` and identify the topmost release heading (e.g., `## 1.12.0`). Insert a new section above it:

```markdown
## 1.13.0 (unreleased)

### Features

- **Cache warmer herd mitigation** (#59).  Rolling Kubernetes deploys
  used to multiply DB primary CPU by the replica count for the warmer
  startup window, because every pod independently fired its full set
  of warmup `SELECT`s simultaneously.  Four composable behaviors fix
  this, all wired through six new ZConfig keys:

  - `cache-warm-delay` (default 15s): baseline sleep before warmer
    starts. Moves the warmer out of the pod's own cold-start window
    (Plone import, plone.pgcatalog schema check, ANALYZE chatter).
  - `cache-warm-jitter` (default 30s): additional `random(0, jitter)`
    sleep on top.  Spreads arrival times across pods.
  - `cache-warm-concurrency` (default 2): maximum number of pods
    warming in parallel, cluster-wide.  Enforced via a session-level
    PostgreSQL advisory lock semaphore (same pattern as the existing
    startup-DDL lock).  Pods that miss a slot retry with jittered
    backoff until `cache-warm-wait-max` expires.
  - `cache-warm-wait-max` (default 300s): retry cap before giving up
    and skipping warmup (logged as WARNING).
  - `cache-warm-batch-size` (default 500): zoids per `SELECT` batch.
  - `cache-warm-batch-pause` (default 0.5s): sleep between batches.
    Lowers per-pod peak qps.

  **Observable behavior change:** warmer queries are now delayed by
  ~15–45s post-startup (delay + jitter), not immediate.  To restore
  pre-1.13 behavior, set `cache-warm-delay=0`, `cache-warm-jitter=0`,
  `cache-warm-batch-pause=0`, `cache-warm-concurrency=9999`.

  **Last-pod latency:** for a 6-pod fleet with default settings, the
  last pod has a warm L2 cache approximately 45s after the deploy
  window.  All pods serve traffic from T+0; this is the time-to-warm,
  not the time-to-ready.

  Design: [docs/superpowers/specs/2026-05-29-cache-warmer-herd-mitigation-design.md](docs/superpowers/specs/2026-05-29-cache-warmer-herd-mitigation-design.md).
```

- [ ] **Step 2: Commit**

```bash
git add CHANGES.md
git commit -m "docs: changelog entry for cache warmer herd mitigation (#59)"
```

---

## Task 14: Full test suite + lint pass

**Files:** none (verification only)

- [ ] **Step 1: Run full unit test suite**

Run: `uv run pytest tests/test_cache_warmer.py tests/test_config.py -v`
Expected: All tests pass — pre-existing + the 18+ new tests added across Tasks 1-12.

- [ ] **Step 2: Run DB integration tests**

Run: `uv run pytest tests/test_cache_warmer.py -v -m db`
Expected: All `@pytest.mark.db` tests pass against `localhost:5433`. If PostgreSQL is unavailable, the existing skip logic in `_setup_db` handles it.

- [ ] **Step 3: Run linter and formatter**

Run: `uv run ruff check .` then `uv run ruff format --check .`
Expected: clean (or apply `ruff format .` if formatting changes are needed, then commit them as a separate `style:` commit).

- [ ] **Step 4: Run full project test suite as a final regression check**

Run: `uv run pytest`
Expected: all tests pass; no regressions in storage / instance / serialization / shared-cache test modules.

- [ ] **Step 5: If lint fixes are needed**

```bash
uv run ruff format .
git add -u
git commit -m "style: ruff format after herd-mitigation changes"
```

---

## Post-merge actions (not part of this plan, but called out in the spec)

After the PR merges:

1. Post a comment on issue #59 summarizing what shipped (six new knobs and their defaults) and naming the deferred follow-ons (C1 materialized warm-set, C2 dump export, C4 replica routing). **Keep the issue open** as the tracking thread for those structural plays.
2. Validate the change on aaf-prod by observing primary CPU during the next rolling deploy. Expected: the prior 90+% CPU spike replaced by a sustained ~30% baseline lasting ~45s.

---

## Self-review notes

This plan was self-reviewed against the spec:

- **Spec coverage:** A1 (Task 2), A2 (Task 2), A3 (Task 3), B2b (Tasks 4-7), config plumbing (Tasks 8-9), error handling (covered in Tasks 5, 7, with per-batch error handling in Task 3), observability (Tasks 5, 8), integration tests (Tasks 10-12), changelog (Task 13). All sections of the spec have a corresponding task.
- **Placeholder scan:** none. Every step has exact code, exact paths, exact commands.
- **Type consistency:** `_acquire_slot()` returns `tuple[Connection, int] | None`; `_release_slot(lock_conn, slot)` signature consistent; module-level constants `WARMER_LOCK_NS` and `WARMER_SLOT_BASE` used identically in tests and source.
- **One open question for the implementer:** the `_LockCursor` in Task 4's helper uses tuple results (positional indexing `row[0]`). The dedicated lock connection in `_acquire_slot` is opened via `psycopg.connect(self._dsn, autocommit=True)` without `row_factory=dict_row` — confirmed in `startup_locks.py`. Production code matches test expectations.
