# CacheWarmer Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tighten three loose ends in the `CacheWarmer` that were flagged during #63 review: silent error swallowing, a DI pattern with no seam, and a startup race that can silently reject all warmup writes. Plus close a regression-test coverage gap on `PGJsonbStorage._finish → shared.poll_advance`.

**Architecture:** Consolidate the `SELECT COALESCE(MAX(tid), 0) FROM transaction_log` query behind a single `_read_max_tid(conn)` helper and a public `PGJsonbStorage.current_max_tid()` method; route all three existing call sites through it. Teach `SharedLoadCache.set()` to return a boolean (accepted / rejected) and expose `consensus_tid` as a read-only property so the warmer can detect and recover from a consensus-race. Add one regression test that pins the direct-use write path's call into `shared.poll_advance`.

**Tech Stack:** Python 3.12+, psycopg 3, existing zodb-pgjsonb.

---

## File structure

| File | Responsibility |
|---|---|
| `src/zodb_pgjsonb/storage.py` | Add module-level `_read_max_tid(conn)`; add `PGJsonbStorage.current_max_tid()` method (returns `int` or `None`, logs on failure); DRY `_restore_state` and the warmer wiring through it. Update `SharedLoadCache.set()` to return `bool`; add `consensus_tid` read-only property. |
| `src/zodb_pgjsonb/instance.py` | `PGJsonbStorageInstance.poll_invalidations` uses the new helper instead of its own inline SELECT. |
| `src/zodb_pgjsonb/cache_warmer.py` | `warm()` re-reads `shared_cache.consensus_tid` after `poll_advance` and uses that as `polled_tid` for subsequent `set()` calls; counts actual accepted writes via the new `set()` return value; logs a warning when `written == 0` despite non-empty results; handles `load_current_tid_fn() → None` by skipping warmup with a warning. |
| `tests/test_shared_load_cache.py` | New tests for `consensus_tid` property and `set()` return-value contract. |
| `tests/test_cache_warmer.py` | New tests for the race-recovery path and the zero-writes warning. |
| `tests/test_storage.py` | New regression test: main-storage `_finish` (via direct `store()` / `tpc_finish`) advances `_shared_cache._consensus_tid` and evicts the affected zoids. |
| `CHANGES.md` | Entry for the next release. |

---

## Task 1: Consolidate TID-query behind a single helper + method

**Goal:** I1 (log+skip on failure) and I2 (DRY the three copies of the SELECT).

**Files:**
- Modify: `src/zodb_pgjsonb/storage.py` — add `_read_max_tid` helper (module-level); add `PGJsonbStorage.current_max_tid()` method; route `_restore_state` and the warmer wiring through the new code.
- Modify: `src/zodb_pgjsonb/instance.py` — route `poll_invalidations` through `_read_max_tid`.
- Modify: `src/zodb_pgjsonb/cache_warmer.py` — handle `None` return from the TID callable.
- Test: `tests/test_storage.py` (new test class `TestCurrentMaxTid`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_storage.py`:

```python
class TestCurrentMaxTid:
    """PGJsonbStorage.current_max_tid — canonical MAX(tid) accessor (#65)."""

    pytestmark = pytest.mark.db

    def test_returns_zero_on_empty_db(self, storage):
        assert storage.current_max_tid() == 0

    def test_returns_max_tid_after_commit(self, db):
        import transaction as txn
        from persistent.mapping import PersistentMapping

        conn = db.open()
        try:
            conn.root()["x"] = PersistentMapping()
            txn.commit()
        finally:
            conn.close()

        # MAX(tid) should match the last committed TID.
        from ZODB.utils import u64
        assert storage_from_db(db).current_max_tid() == u64(db._storage.lastTransaction())

    def test_returns_none_on_query_failure(self, storage, caplog):
        """Closed or broken connection → log.warning + return None."""
        import logging

        # Close the underlying connection out from under the storage
        # to simulate a broken/terminated session.
        storage._conn.close()

        with caplog.at_level(logging.WARNING, logger="zodb_pgjsonb.storage"):
            assert storage.current_max_tid() is None
        assert any(
            "current_max_tid" in rec.getMessage().lower()
            or "max tid" in rec.getMessage().lower()
            for rec in caplog.records
        ), f"expected warning log, got: {[r.getMessage() for r in caplog.records]}"


def storage_from_db(db):
    """Helper: get the main storage from a ZODB.DB."""
    return db.storage
```

Note: the `storage_from_db` helper is needed because `db.storage` is an `MVCCAdapter` wrapper around the real storage in newer ZODB versions; this helper is a stub we will refine if needed. For now `db.storage` is fine — it exposes the main storage directly.

Check the top of `tests/test_storage.py` for `import pytest` and `import transaction` — add if missing.

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_storage.py::TestCurrentMaxTid -v`
Expected: FAIL with `AttributeError: 'PGJsonbStorage' object has no attribute 'current_max_tid'`.

- [ ] **Step 3: Add the helper + method**

In `src/zodb_pgjsonb/storage.py`, near the top of the module (after the imports block, near the other module-level helpers like `_mask_dsn` if present, or just before `class LoadCache`):

```python
def _read_max_tid(conn):
    """Return the current MAX(tid) from transaction_log, or 0 when empty.

    Raises whatever psycopg raises on a broken connection or closed
    database.  Callers decide whether to catch.
    """
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(tid), 0) AS max_tid FROM transaction_log")
        row = cur.fetchone()
        return row["max_tid"] if row else 0
```

Inside the `PGJsonbStorage` class (place the method near other admin-style read methods such as `__len__`/`getSize`, which are around line 1058 in the current source):

```python
    def current_max_tid(self):
        """Return the current MAX(tid) in transaction_log, or ``None``.

        On any psycopg / DB error the failure is logged at WARNING and
        ``None`` is returned.  ``None`` signals callers to skip any
        work that depends on a fresh TID (e.g. the cache warmer
        gracefully skips warmup rather than installing a fabricated
        consensus of 0).
        """
        try:
            return _read_max_tid(self._conn)
        except Exception:
            logger.warning(
                "PGJsonbStorage.current_max_tid: query failed", exc_info=True
            )
            return None
```

- [ ] **Step 4: Run the new tests**

Run: `.venv/bin/pytest tests/test_storage.py::TestCurrentMaxTid -v`
Expected: PASS on all 3.

- [ ] **Step 5: Route `_restore_state` through the helper**

In `src/zodb_pgjsonb/storage.py`, find `_restore_state` (around line 573). Replace the body after the OID-load block (the `cur.execute("SELECT COALESCE(MAX(tid)...")` block around line 582-589) with:

```python
    def _restore_state(self):
        """Load max OID and last TID from existing data."""
        with self._conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(zoid), 0) AS max_oid FROM object_state")
            row = cur.fetchone()
            max_oid = row["max_oid"]
            if max_oid > 0:
                self._oid = p64(max_oid)

        max_tid = _read_max_tid(self._conn)
        if max_tid > 0:
            self._ltid = p64(max_tid)
            # Ensure _ts is at least as recent as the last committed TID
            # so _new_tid() generates monotonically increasing TIDs.
            self._ts = TimeStamp(self._ltid)
```

- [ ] **Step 6: Route the warmer wiring through `current_max_tid`**

In `src/zodb_pgjsonb/storage.py`, find the `_current_max_tid` inline closure in `__init__` (around line 506-515) and the `CacheWarmer(...)` call below it (around line 517-523). Delete the closure entirely and change the `load_current_tid_fn=` kwarg to point at the bound method:

```python
            self._warmer = CacheWarmer(
                self._conn,
                target_count=target,
                shared_cache=self._shared_cache,
                load_current_tid_fn=self.current_max_tid,
                decay=cache_warm_decay,
            )
```

- [ ] **Step 7: Route `poll_invalidations` through the helper**

In `src/zodb_pgjsonb/instance.py`, find `poll_invalidations` (around line 156). The inline SELECT is at line 183-185. Replace that block with:

```python
        from .storage import _read_max_tid

        new_tid = _read_max_tid(self._conn)
```

The surrounding body stays the same.

- [ ] **Step 8: Teach the warmer to handle `None`**

In `src/zodb_pgjsonb/cache_warmer.py`, find `warm()` (around line 125). Replace the body up to the `shared.poll_advance` call with:

```python
    def warm(self, load_multiple_fn):
        """Load top-N ZOIDs into the shared cache.

        Runs in a background daemon thread.  Primes the consensus TID
        on the shared cache to the current PG max_tid so that
        subsequent ``shared.set`` calls are accepted.  Skips warmup
        entirely when the TID is unavailable.
        """
        from ZODB.utils import p64
        from ZODB.utils import u64

        top_zoids = self._read_top_oids()
        if not top_zoids:
            log.info("Cache warmer: no stats yet, skipping warmup")
            return

        current_tid = self._load_current_tid_fn()
        if current_tid is None:
            log.warning(
                "Cache warmer: could not read current TID, skipping warmup"
            )
            return

        oids = [p64(z) for z in top_zoids]
        try:
            results = load_multiple_fn(oids)
        except Exception:
            log.warning("Cache warmer: load_multiple failed", exc_info=True)
            return

        # Prime consensus so set() accepts our writes, then populate.
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

Note: the `current_tid is None` guard is the new behaviour. The race fix + written-count correctness is in Task 3 — Task 1 leaves the write loop as-is.

- [ ] **Step 9: Run full cache_warmer + storage + mvcc suites**

Run:

```bash
.venv/bin/pytest tests/test_cache_warmer.py tests/test_storage.py tests/test_mvcc.py tests/test_shared_load_cache.py tests/test_shared_load_cache_integration.py -q
```

Expected: all green. The existing `TestWarmerPopulatesSharedCache::test_warm_populates_shared_cache` still passes because the underlying happy-path behaviour is unchanged.

- [ ] **Step 10: Verify grep says zero remaining inline SELECTs**

Run: `grep -n "SELECT COALESCE(MAX(tid)" src/zodb_pgjsonb/*.py`
Expected: exactly one hit at `src/zodb_pgjsonb/storage.py` (the `_read_max_tid` body). No hits in `instance.py` or elsewhere in `storage.py`.

- [ ] **Step 11: Commit**

```bash
git add src/zodb_pgjsonb/storage.py src/zodb_pgjsonb/instance.py \
        src/zodb_pgjsonb/cache_warmer.py tests/test_storage.py
git commit -m "refactor(warmer): consolidate MAX(tid) query + log/skip on failure (#65)

Add module-level _read_max_tid(conn) helper and a public
PGJsonbStorage.current_max_tid() method that logs on failure and
returns None.  Route _restore_state, poll_invalidations, and the
warmer's load_current_tid_fn through them.  Warmer skips warmup
when current_max_tid returns None instead of installing consensus=0.

Closes I1 and I2 of #65.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Expose `consensus_tid` property and make `set()` return bool

**Goal:** Add the two `SharedLoadCache` API extensions needed by Task 3's race fix.

**Files:**
- Modify: `src/zodb_pgjsonb/storage.py` (`SharedLoadCache` class).
- Modify: `tests/test_shared_load_cache.py` (append `TestSharedLoadCacheAPIExtensions`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_shared_load_cache.py` (module already imports `SharedLoadCache` and `p64`):

```python
class TestSharedLoadCacheAPIExtensions:
    """#65: consensus_tid property + set() returns bool."""

    def test_consensus_tid_initially_none(self):
        cache = SharedLoadCache(max_mb=4)
        assert cache.consensus_tid is None

    def test_consensus_tid_reflects_poll_advance(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=123, changed_zoids=[])
        assert cache.consensus_tid == 123

    def test_consensus_tid_does_not_rewind(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        cache.poll_advance(new_tid=100, changed_zoids=[])  # older, ignored
        assert cache.consensus_tid == 200

    def test_set_returns_true_on_accept(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=100, changed_zoids=[])
        accepted = cache.set(
            zoid=1, data=b"xyz", tid_bytes=p64(100), polled_tid=100
        )
        assert accepted is True

    def test_set_returns_false_when_consensus_uninitialized(self):
        cache = SharedLoadCache(max_mb=4)
        accepted = cache.set(
            zoid=1, data=b"xyz", tid_bytes=p64(100), polled_tid=100
        )
        assert accepted is False

    def test_set_returns_false_for_stale_writer(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        accepted = cache.set(
            zoid=1, data=b"xyz", tid_bytes=p64(100), polled_tid=100
        )
        assert accepted is False

    def test_set_returns_false_when_older_than_existing(self):
        cache = SharedLoadCache(max_mb=4)
        cache.poll_advance(new_tid=200, changed_zoids=[])
        cache.set(zoid=1, data=b"new", tid_bytes=p64(200), polled_tid=200)
        accepted = cache.set(
            zoid=1, data=b"older", tid_bytes=p64(100), polled_tid=200
        )
        assert accepted is False
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py::TestSharedLoadCacheAPIExtensions -v`
Expected: the `test_consensus_tid_*` tests fail with `AttributeError: 'SharedLoadCache' object has no attribute 'consensus_tid'`; the `test_set_returns_*` tests fail because `set()` returns `None`, not `True`/`False`.

- [ ] **Step 3: Add `consensus_tid` property**

In `src/zodb_pgjsonb/storage.py`, inside the `SharedLoadCache` class, immediately below `__init__` (look for the existing `__slots__` — consensus_tid is already a slot: `_consensus_tid`):

```python
    @property
    def consensus_tid(self):
        """The highest TID any instance has polled to (read-only).

        Returns ``None`` before the first ``poll_advance`` call.
        """
        with self._lock:
            return self._consensus_tid
```

- [ ] **Step 4: Change `set()` to return bool**

Still in `SharedLoadCache`, find the `set` method. It currently returns `None` from three implicit-return paths. Change each to return `False`, and the final successful path to return `True`:

```python
    def set(self, zoid, data, tid_bytes, polled_tid):
        """Store (data, tid_bytes) for zoid if the caller is up to date.

        Returns True when the entry was accepted (possibly replacing an
        older entry), False when rejected by the consensus gate, the
        monotonicity gate, or the stale-polled_tid gate.
        """
        with self._lock:
            if self._consensus_tid is None or polled_tid is None:
                return False
            if polled_tid < self._consensus_tid:
                return False
            tid_int = u64(tid_bytes)
            existing = self._cache.get(zoid)
            if existing is not None and u64(existing[1]) >= tid_int:
                return False
            if existing is not None:
                self._current_bytes -= len(existing[0])
            self._cache[zoid] = (data, tid_bytes)
            self._cache.move_to_end(zoid)
            self._current_bytes += len(data)
            while self._current_bytes > self._max_bytes and self._cache:
                _, (evicted_data, _) = self._cache.popitem(last=False)
                self._current_bytes -= len(evicted_data)
            return True
```

- [ ] **Step 5: Run tests**

Run: `.venv/bin/pytest tests/test_shared_load_cache.py::TestSharedLoadCacheAPIExtensions -v`
Expected: PASS on all 7.

Regression: `.venv/bin/pytest tests/test_shared_load_cache.py tests/test_shared_load_cache_concurrency.py tests/test_shared_load_cache_integration.py -q`
Expected: all green. Existing callers ignore the return value, so nothing breaks.

- [ ] **Step 6: Commit**

```bash
git add src/zodb_pgjsonb/storage.py tests/test_shared_load_cache.py
git commit -m "feat(shared-cache): consensus_tid property + set() returns bool (#65)

Both are wanted by the cache warmer's race-detection fix (next
commit).  The existing instance.load / load_multiple call sites
ignore the new return value; no behaviour change there.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Warmer race-recovery and zero-writes warning

**Goal:** I3 — warmer samples the effective post-`poll_advance` consensus and uses it as the `polled_tid` for subsequent `set` calls; counts actually-accepted writes; warns loudly when every write was rejected despite a non-empty result set.

**Files:**
- Modify: `src/zodb_pgjsonb/cache_warmer.py` (`warm` method).
- Modify: `tests/test_cache_warmer.py` (append `TestWarmerRaceRecovery`).

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_cache_warmer.py`:

```python
class TestWarmerRaceRecovery:
    """#65 I3: warmer tolerates a consensus race and logs silent failures."""

    def test_warm_uses_effective_consensus_when_race_advances_it(self):
        """If consensus is already ahead, warmer's writes still land."""
        from zodb_pgjsonb.storage import SharedLoadCache
        from ZODB.utils import p64

        shared = SharedLoadCache(max_mb=4)
        # Simulate another instance's poll_advance racing ahead
        shared.poll_advance(new_tid=1000, changed_zoids=[])

        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 500,  # stale — before the race
        )

        def loader(oids):
            return {oid: (b"data-" + oid, p64(500)) for oid in oids}

        w.warm(loader)

        # Entries must be present: the warmer re-read consensus and
        # used 1000 (the actual value) as polled_tid instead of 500.
        assert shared.get(zoid=1, polled_tid=1000) == (b"data-" + p64(1), p64(500))
        assert shared.get(zoid=2, polled_tid=1000) == (b"data-" + p64(2), p64(500))

    def test_warm_logs_warning_when_all_writes_rejected(self, caplog):
        """If consensus advances past the warmer's effective polled_tid
        mid-loop, every set() is rejected and the warmer flags it."""
        import logging
        from zodb_pgjsonb.storage import SharedLoadCache
        from ZODB.utils import p64

        # A cache subclass whose set() always returns False (simulating
        # a mid-loop consensus advance that we can't actually time).
        class _RejectingCache(SharedLoadCache):
            def set(self, *a, **kw):
                super().set(*a, **kw)
                return False

        shared = _RejectingCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2, 3]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        with caplog.at_level(logging.WARNING, logger="zodb_pgjsonb.cache_warmer"):
            w.warm(loader)

        warnings = [
            r for r in caplog.records
            if r.levelno == logging.WARNING
            and "rejected" in r.getMessage().lower()
        ]
        assert warnings, (
            f"expected a WARNING about rejected writes, got: "
            f"{[r.getMessage() for r in caplog.records]}"
        )

    def test_warm_info_log_on_normal_writes(self, caplog):
        """Happy path logs at INFO, not WARNING."""
        import logging
        from zodb_pgjsonb.storage import SharedLoadCache
        from ZODB.utils import p64

        shared = SharedLoadCache(max_mb=4)
        w = CacheWarmer(
            conn=_FakeConn(top_oids=[1, 2]),
            target_count=10,
            shared_cache=shared,
            load_current_tid_fn=lambda: 100,
        )

        def loader(oids):
            return {oid: (b"x", p64(50)) for oid in oids}

        with caplog.at_level(logging.INFO, logger="zodb_pgjsonb.cache_warmer"):
            w.warm(loader)

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert not warnings, (
            f"expected no warnings, got: "
            f"{[r.getMessage() for r in warnings]}"
        )
        infos = [
            r for r in caplog.records
            if r.levelno == logging.INFO
            and "loaded 2 objects" in r.getMessage()
        ]
        assert infos, (
            f"expected INFO with loaded count, got: "
            f"{[r.getMessage() for r in caplog.records]}"
        )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_cache_warmer.py::TestWarmerRaceRecovery -v`
Expected: all 3 FAIL. `test_warm_uses_effective_consensus_when_race_advances_it` fails because today's warmer uses `current_tid=500` as `polled_tid`, which is rejected by the gate (consensus is 1000). `test_warm_logs_warning_when_all_writes_rejected` fails because the current code doesn't distinguish accepted vs. rejected writes. `test_warm_info_log_on_normal_writes` passes today (its intent is to prevent the race-warning from firing on the happy path — verify by running it).

- [ ] **Step 3: Rewrite `warm()` with race-recovery and accepted-write counting**

In `src/zodb_pgjsonb/cache_warmer.py`, replace the entire `warm` method (currently around line 125-160) with:

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
        from ZODB.utils import p64
        from ZODB.utils import u64

        top_zoids = self._read_top_oids()
        if not top_zoids:
            log.info("Cache warmer: no stats yet, skipping warmup")
            return

        current_tid = self._load_current_tid_fn()
        if current_tid is None:
            log.warning(
                "Cache warmer: could not read current TID, skipping warmup"
            )
            return

        oids = [p64(z) for z in top_zoids]
        try:
            results = load_multiple_fn(oids)
        except Exception:
            log.warning("Cache warmer: load_multiple failed", exc_info=True)
            return

        if not results:
            log.info("Cache warmer: load_multiple returned no objects")
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

        written = 0
        attempted = 0
        for oid, (data, tid_bytes) in results.items():
            attempted += 1
            if self._shared_cache.set(
                zoid=u64(oid),
                data=data,
                tid_bytes=tid_bytes,
                polled_tid=effective_tid,
            ):
                written += 1

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
                "Cache warmer: loaded %d of %d objects into shared cache",
                written,
                attempted,
            )
```

- [ ] **Step 4: Run the new tests**

Run: `.venv/bin/pytest tests/test_cache_warmer.py::TestWarmerRaceRecovery -v`
Expected: PASS on all 3.

Regression: `.venv/bin/pytest tests/test_cache_warmer.py -q`
Expected: all green. The pre-existing `TestWarmerPopulatesSharedCache::test_warm_populates_shared_cache` still passes because the happy path is unchanged (it uses `current_tid=100` with fresh consensus — effective_tid == current_tid).

- [ ] **Step 5: Full-suite regression**

Run: `.venv/bin/pytest tests/ -q --tb=line 2>&1 | tail -10`
Expected: all pass, matching the 493 count from the previous #63 baseline plus the Task 1 and Task 2 additions (expected ~500 passed).

- [ ] **Step 6: Commit**

```bash
git add src/zodb_pgjsonb/cache_warmer.py tests/test_cache_warmer.py
git commit -m "fix(warmer): recover from consensus race; warn on zero accepted writes (#65)

Closes I3 of #65.  After poll_advance, re-read consensus_tid and use
that as the polled_tid for subsequent set() calls — if another
instance advanced consensus ahead of our sampled TID, this ensures
our sets still pass the gate.  Track accepted vs. attempted writes
via the new set() return value; log a WARNING when the entire
warmup was rejected despite non-empty results.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Regression test for main-storage `_finish → shared.poll_advance`

**Goal:** Close the coverage gap flagged in the #63 final review: no test pins that `PGJsonbStorage._finish` advances `_shared_cache._consensus_tid` and invalidates the written zoids.

**Files:**
- Modify: `tests/test_shared_load_cache_integration.py` (append `TestMainStorageFinishAdvancesConsensus`).

- [ ] **Step 1: Write the failing / verifying test**

Append to `tests/test_shared_load_cache_integration.py` (imports should already include `pytest`, `transaction as txn`, `PersistentMapping`, and `pytestmark = pytest.mark.db`):

```python
class TestMainStorageFinishAdvancesConsensus:
    """#65 coverage: main-storage _finish → shared.poll_advance.

    Direct-use write paths (restore, zodbconvert, direct store())
    bypass instance.tpc_finish. Main PGJsonbStorage._finish must
    therefore advance shared consensus and invalidate changed zoids
    so that subsequent instance reads don't serve stale state.
    """

    def test_direct_use_store_advances_consensus_and_invalidates(self, db):
        """A commit through db.open() uses instance.tpc_finish but
        the main storage's _finish is what our instance actually
        calls into for the shared cache invalidation — verify both
        consensus advances AND the changed zoid is evicted."""
        from ZODB.utils import u64

        # Warm an entry for zoid 0 (root mapping)
        conn = db.open()
        try:
            root = conn.root()
            root["x"] = PersistentMapping({"i": 1})
            txn.commit()
            root_zoid = u64(root._p_oid)
            shared = conn._storage._main._shared_cache
            # After commit, consensus must equal the committed TID
            assert shared.consensus_tid == u64(conn._storage._polled_tid)
            # Root was modified — must have been invalidated in shared
            assert shared.get(root_zoid, conn._storage._polled_tid) is None
        finally:
            conn.close()

    def test_main_storage_finish_direct_path(self, storage):
        """Exercise PGJsonbStorage._finish directly (not via an
        instance) to cover the main-storage code path."""
        from persistent.mapping import PersistentMapping
        import ZODB

        # Run a committed transaction via ZODB.DB so internal
        # plumbing is correct; assert on the main storage's state.
        db = ZODB.DB(storage)
        try:
            conn = db.open()
            try:
                conn.root()["y"] = PersistentMapping({"j": 2})
                txn.commit()
            finally:
                conn.close()

            # Main storage's shared cache must have advanced.
            # (Both main._finish and instance.tpc_finish advance it;
            # either satisfies the invariant.)
            assert storage._shared_cache.consensus_tid is not None
            assert storage._shared_cache.consensus_tid > 0
        finally:
            db.close()
```

- [ ] **Step 2: Run the tests**

Run: `.venv/bin/pytest tests/test_shared_load_cache_integration.py::TestMainStorageFinishAdvancesConsensus -v`
Expected: PASS on both tests. These pin existing behaviour — they are not expected to fail on main; they exist as regression guards.

If either FAILS on current main (!), stop and diagnose: that would mean the #63 wiring is less complete than the review claimed, and we want to know.

- [ ] **Step 3: Commit**

```bash
git add tests/test_shared_load_cache_integration.py
git commit -m "test(shared-cache): pin main-storage _finish → shared.poll_advance (#65)

Coverage gap flagged in #63 final review.  Pins the invariant that
the direct-use write path advances consensus and invalidates
changed zoids in the shared cache, independent of any instance's
tpc_finish.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: CHANGES entry

**Goal:** Document the hardening for the next release.

**Files:**
- Modify: `CHANGES.md`.

- [ ] **Step 1: Prepend the entry**

Open `CHANGES.md` and insert this block at the top, under `# Changelog` but above the most recent entry (currently 1.12.0 from #63):

```markdown
## 1.12.1

- Harden the `CacheWarmer` lifecycle (#65).
  - The `MAX(tid)` lookup was duplicated in three places with three
    different error-handling policies.  Consolidated behind a single
    `_read_max_tid(conn)` helper and a public
    `PGJsonbStorage.current_max_tid()` method that logs a warning and
    returns `None` on failure.  The warmer skips warmup when the
    method returns `None` instead of installing a fabricated
    consensus of 0.
  - `SharedLoadCache.set()` now returns `True` on accept and `False`
    on rejection.  `SharedLoadCache.consensus_tid` is exposed as a
    read-only property.  Both are used by the warmer's race-recovery
    path; existing `instance.load` / `load_multiple` callers ignore
    the new return value and are unaffected.
  - The warmer re-reads `shared_cache.consensus_tid` after its own
    `poll_advance` and uses that value as `polled_tid` for the
    subsequent `set()` loop — this fixes a startup race where a
    concurrent instance poll could advance consensus past the
    warmer's sampled TID and cause every warmup write to be silently
    rejected.  A WARNING is now logged when the entire warmup was
    rejected despite a non-empty result set.
  - Added a regression test that pins `PGJsonbStorage._finish`
    (direct-use write path) advancing shared consensus and
    invalidating changed zoids.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGES.md
git commit -m "docs: CHANGES entry for 1.12.1 warmer hardening (#65)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Self-review

**Spec coverage:**
- I1 (error-swallow → log+skip) — Task 1, Step 3 (`current_max_tid` with try/except+log), Step 8 (warmer `None` handling).
- I2 (DRY the 3 SELECT copies) — Task 1, Steps 3, 5, 6, 7 (helper + 3 call sites routed).
- I3 (startup race + zero-writes warning) — Task 3 (`warm()` rewrite + new tests).
- Coverage gap (main `_finish` → `shared.poll_advance`) — Task 4.

**Placeholders:** none. Every step has exact code, exact commands, exact file paths.

**Type consistency:**
- `current_max_tid()` returns `int | None` in Task 1 Step 3; warmer checks `is None` in Task 1 Step 8 and Task 3 Step 3. Consistent.
- `set()` returns `bool` in Task 2 Step 4; Task 3 Step 3 uses the return value in `if self._shared_cache.set(...)`. Consistent.
- `consensus_tid` property returns `int | None` in Task 2 Step 3; Task 3 Step 3 checks `effective_tid is None`. Consistent.

**Dependency order:** Task 1 → Task 2 → Task 3 (Task 3 needs Task 2's APIs). Task 4 is independent (coverage test only). Task 5 depends on all prior for accurate CHANGES.

---

Plan complete and saved to `docs/superpowers/plans/2026-04-23-warmer-hardening.md`. Two execution options:

**1. Subagent-Driven (recommended)** — I dispatch a fresh subagent per task, review between tasks, fast iteration.

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints.

Which approach?
