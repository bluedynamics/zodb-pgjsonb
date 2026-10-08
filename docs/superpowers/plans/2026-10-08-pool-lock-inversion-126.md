# Pool / DB-lock inversion (#126) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pool-exhaustion event (for example after a database failover) must never wedge a Zope process: `PGJsonbStorageInstance` must not block on the instance pool while ZODB holds `DB._lock`, and no failed poll may keep a pool slot checked out.

**Architecture:** Check out the pooled PostgreSQL connection lazily on first use (`_ensure_conn()`), never in `__init__`. ZODB calls `new_instance()` under `DB._lock`, while `Connection.open()` → `newTransaction()` → `poll_invalidations()` runs outside it, so the blocking `getconn()` moves out of the lock. A failing `poll_invalidations()` hands its slot back before re-raising.

**Tech Stack:** Python 3.12+, ZODB 6.x, psycopg 3.3, psycopg-pool 3.3, pytest against PostgreSQL 17 on `localhost:5433`.

**Spec:** GitHub issue bluedynamics/zodb-pgjsonb#126 plus the verified root cause below. The root cause differs from the issue's analysis. Read it before starting.

## Root cause (verified by reproduction)

`ZODB.DB.open()` creates a new `Connection` **while holding `DB._lock`** (ZODB 6.3 `DB.py:777`, `c = self.klass(...)` inside `with self._lock:`). `Connection.__init__` calls `storage.new_instance()`, and `PGJsonbStorageInstance.__init__` calls `self._instance_pool.getconn()`, which blocks up to `pool_timeout` (30 s) when the pool is full.

Every thread that holds a pool slot gives its ZODB connection back through `Connection.close()` → `DB._returnToPool()`, which needs `DB._lock`. So:

1. The opener holds `DB._lock` and waits for a pool slot.
2. All slot holders wait for `DB._lock` inside `close()`. They have already run `afterCompletion()` (`COMMIT`), so on the server every slot is `idle`, last statement `COMMIT`. That is exactly the `pg_stat_activity` picture from production.
3. After `pool_timeout` the opener gives up and `DB._lock` is released. The lock is not FIFO, and a pile of waiting openers competes for it: readiness probes run on an unbounded `ThreadingHTTPServer` and every probe opens a ZODB connection. Usually another opener wins, finds no parked connection, creates one and blocks again for 30 s. The convoy practically never drains.

The second stack trace in the issue (`DB.py:777 open -> Connection.__init__ -> new_instance -> getconn() -> PoolTimeout`) is this lock-held wait.

`benchmarks/failover_repro.py` reproduces the wedge reliably (3/3 runs). Thread histogram at the end of a wedged run:

```
  36 probe     open:751                          # waiting for DB._lock
   8 probe     _returnToPool:488 <- close:323    # hold slots, waiting for DB._lock
   2 waitress  _returnToPool:488 <- close:323
   1 waitress  open:751
   1 probe     wait:890 <- _getconn_unchecked    # holds DB._lock, waits for a slot
```

With lazy checkout patched into the same script, the process recovers about 2 s after the database comes back, and probes and requests succeed again.

### Where the issue's analysis holds and where it does not

- **(1) "Exceptions escaping `Connection.open()` strand slots": mostly not the cause.** The quoted path (`_replace_broken_conn` → `getconn()` → `PoolTimeout`) sets `self._conn = None` *before* `getconn()`, so it holds no slot. Stranded connections exist (ZODB `all` grows), but they hold no slots and GC collects them. A slot is only stranded when something fails *after* a successful checkout (for example `_read_max_tid` after `BEGIN`). That case is real but rare, and Task 2 closes it.
- **(2) "Each ZODB connection pins a slot for its whole life": true, and it amplifies the problem,** but alone it does not wedge: parked connections get reused by the next `open()`. It matters for `max_connections` (9 pods × 11 ≈ 100) and for headroom. It is out of scope here; see Follow-ups.
- **(3) "Nothing self-heals": true.** After this fix it heals by itself, because closers no longer wait for the lock.

## Global Constraints

- Python floor and dependency pins stay unchanged (`pyproject.toml`).
- Ruff pinned to 0.16.7; `max-complexity = 13` (C901) with zero `noqa`.
- Every change gets a `CHANGES.md` entry in the same PR.
- Commits and PR end with `Assisted-by: Claude <name of the model doing the work>` (the examples below use Opus 5.5); never `Co-Authored-By`, never a noreply address.
- PR title and body in English.
- Tests run strictly serially against `dbname=zodb_test` on `localhost:5433`: `env -u ZODB_TEST_DSN uv run pytest ...`.

## Review Focus

1. **Explicit-transaction mode / loads before any poll:** a `load()` on a fresh instance with no prior `poll_invalidations()` must check out a connection and work (autocommit read), not crash on `None`. Test in Task 1.
2. **`tpc_abort()` without a connection:** if `tpc_begin()` itself failed while checking out, ZODB still calls `tpc_abort()`; it must not log an `AttributeError` traceback. Test in Task 1.
3. **Instance reused after a failed checkout:** a parked instance whose `_conn` is `None` (failed replace / failed poll) must recover on the next `poll_invalidations()`. Today it dies with `AttributeError: 'NoneType' object has no attribute 'execute'`. Test in Task 2.
4. **`pg_connection` consumers (plone.pgcatalog `get_storage_connection`)** must still get a live connection, never `None`. Test in Task 1.
5. **Burst larger than `pool_max_size`:** closers must not wait for `pool_timeout`, and the process must serve requests again once the burst is over. Test in Task 3.

---

### Task 1: Lazy pool checkout (`_ensure_conn`)

**Files:**
- Modify: `src/zodb_pgjsonb/instance.py` (`__init__` ~L54-58, `pg_connection` ~L100-107, `_begin_read_txn` ~L207-229, `load` ~L320, `load_multiple` ~L414, `loadBefore` ~L445, `loadSerial` ~L452, `tpc_begin` ~L503-520, `tpc_abort` ~L603-608, `loadBlob` ~L653, `undo` ~L720)
- Test: `tests/test_pool_lock_inversion.py` (create)

**Interfaces:**
- Produces: `PGJsonbStorageInstance._ensure_conn() -> psycopg.Connection` (checks out on first use, returns the held connection otherwise). Invariant: `self._conn is None` means "holds no pool slot".

- [ ] **Step 1: Write the failing tests**

Create `tests/test_pool_lock_inversion.py`:

```python
"""#126: the instance must never block on the pool while ZODB holds DB._lock.

ZODB's DB.open() creates new Connections under DB._lock, and
Connection.__init__ calls storage.new_instance().  If new_instance() blocks
in getconn(), every Connection.close() (which needs DB._lock to return its
connection) waits for pool_timeout.  Under load the convoy never drains.
"""

from tests.conftest import clean_db
from tests.conftest import DSN

import pytest
import threading
import time


pytestmark = pytest.mark.db


def _small_storage(**kw):
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    params = dict(cache_warm_pct=0, pool_size=1, pool_max_size=1, pool_timeout=5.0)
    params.update(kw)
    return PGJsonbStorage(DSN, **params)


def _wait_for_pool_waiter(pool, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pool.get_stats().get("requests_waiting", 0) >= 1:
            return
        time.sleep(0.02)
    raise AssertionError("no client started waiting on the pool")


def test_new_instance_does_not_check_out():
    storage = _small_storage()
    try:
        pool = storage._instance_pool
        calls = []
        orig = pool.getconn

        def counting_getconn(*a, **k):
            calls.append(1)
            return orig(*a, **k)

        pool.getconn = counting_getconn
        inst = storage.new_instance()
        try:
            assert calls == []
            assert inst._conn is None
            inst.poll_invalidations()
            assert len(calls) == 1
            assert inst._conn is not None
        finally:
            inst.release()
    finally:
        storage.close()


def test_close_not_blocked_by_open_waiting_for_slot():
    import ZODB

    storage = _small_storage()
    db = ZODB.DB(storage)
    try:
        c1 = db.open()  # pops the connection DB.__init__ parked: holds the slot
        c1.root()
        errors = []

        def opener():
            try:
                c2 = db.open()  # nothing parked: new Connection, needs a slot
                c2.close()
            except Exception as e:  # PoolTimeout is acceptable here
                errors.append(e)

        t = threading.Thread(target=opener, daemon=True)
        t.start()
        _wait_for_pool_waiter(storage._instance_pool)

        t0 = time.monotonic()
        c1.close()
        elapsed = time.monotonic() - t0
        assert elapsed < 1.0, f"close() waited {elapsed:.1f}s for DB._lock"

        t.join(10)
        assert not t.is_alive()
        # Whatever happened to the opener, the process still serves.
        c3 = db.open()
        c3.root()
        c3.close()
    finally:
        db.close()
        storage.close()


def test_pg_connection_checks_out_lazily():
    storage = _small_storage()
    try:
        inst = storage.new_instance()
        try:
            conn = inst.pg_connection
            assert conn is not None
            assert conn.execute("SELECT 1 AS x").fetchone()["x"] == 1
        finally:
            inst.release()
    finally:
        storage.close()


def test_load_before_any_poll_works():
    """Explicit-transaction mode can load before poll_invalidations()."""
    from ZODB.utils import z64

    import ZODB

    # pool_max_size=2: the connection DB() parks after creating the root
    # still pins one slot.
    storage = _small_storage(pool_max_size=2)
    db = ZODB.DB(storage)  # creates the root object
    try:
        inst = storage.new_instance()
        try:
            data, tid = inst.load(z64)
            assert data and tid
        finally:
            inst.release()
    finally:
        db.close()
        storage.close()


def test_tpc_abort_without_connection_is_quiet(caplog):
    storage = _small_storage()
    try:
        inst = storage.new_instance()
        try:
            inst.tpc_abort(None)
            assert "Error during rollback" not in caplog.text
        finally:
            inst.release()
    finally:
        storage.close()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pool_lock_inversion.py -v`
Expected: `test_new_instance_does_not_check_out` FAILS (`calls == [1]`), `test_close_not_blocked_by_open_waiting_for_slot` FAILS (`close() waited ~4.9s`), `test_tpc_abort_without_connection_is_quiet` passes (today the instance has a connection; it guards the new code). The other two pass today and guard against regressions.

- [ ] **Step 3: Implement lazy checkout**

In `__init__`, replace `self._conn = self._instance_pool.getconn()` with:

```python
        # No pool checkout here.  ZODB calls new_instance() while holding
        # DB._lock (DB.open creates Connections under it).  Blocking in
        # getconn() there makes every Connection.close(), which needs
        # DB._lock to return its connection, wait for pool_timeout; under
        # load that convoy never drains (#126).  _ensure_conn() checks out
        # on first use, which is poll_invalidations() during
        # Connection.open(), outside DB._lock.
        self._conn = None  # None = holds no pool slot
```

Add after `release()`:

```python
    def _ensure_conn(self):
        """Return the pooled connection, checking one out on first use (#126)."""
        if self._conn is None:
            self._conn = self._instance_pool.getconn()
        return self._conn
```

`pg_connection` returns `self._ensure_conn()`; update its docstring: "Checked out on first access."

`_begin_read_txn`: first line `conn = self._ensure_conn()`, then `conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")` in the `try`; leave the retry branch unchanged.

`load`, `load_multiple`, `loadBefore`, `undo`: `with self._ensure_conn().cursor() as cur:`.
`loadSerial`: `_do_loadSerial(self._ensure_conn(), ...)`. `loadBlob`: `_materialize_blob(self._ensure_conn(), ...)`.

`tpc_begin`: after `self._main._apply_pending_ddl()` add `conn = self._ensure_conn()` and use `conn.execute("BEGIN")` / `conn.execute("SELECT pg_advisory_xact_lock(0)")`. `tpc_vote`/`tpc_finish` keep using `self._conn` (always set after `tpc_begin`).

`tpc_abort`: wrap the rollback in `if self._conn is not None:`.

Update the class docstring ("Has its own PG connection") and the module docstring: "a PostgreSQL connection checked out from the pool on first use".

- [ ] **Step 4: Run the new and the related existing tests**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pool_lock_inversion.py tests/test_idle_in_xact.py tests/test_mvcc.py tests/test_storage.py -v`
Expected: all pass. Tests that read `instance._conn` right after `new_instance()` (grep `tests/` for `._conn`) must call `poll_invalidations()` or `_ensure_conn()` first. Adjust those tests, not the code.

- [ ] **Step 5: Full suite + lint, then commit**

Run: `env -u ZODB_TEST_DSN uv run pytest -q` (strictly serial, nothing else against `zodb_test` meanwhile), then `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`

```bash
git add src/zodb_pgjsonb/instance.py tests/
git commit -m "fix: check out the pool connection lazily, never under DB._lock (#126)

ZODB creates Connections while holding DB._lock; a blocking getconn() in
new_instance() made every Connection.close() wait for pool_timeout, and
under a burst (failover, piled-up readiness probes) the convoy never
drained.

Assisted-by: Claude Opus 5.5"
```

---

### Task 2: A failed poll gives its slot back

**Files:**
- Modify: `src/zodb_pgjsonb/instance.py` (`_replace_broken_conn` ~L187-205, `poll_invalidations` ~L231-281)
- Test: `tests/test_pool_lock_inversion.py`

**Interfaces:**
- Consumes: `_ensure_conn()` from Task 1.
- Produces: `PGJsonbStorageInstance._drop_conn() -> None`: returns the held connection to the pool (if any), sets `_conn = None` and `_in_read_txn = False`, and never raises.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_pool_lock_inversion.py`:

```python
def test_failed_poll_returns_slot(monkeypatch):
    """An exception after checkout must not escape Connection.open() with
    the slot still held; ZODB strands that Connection (#126)."""
    import psycopg
    import zodb_pgjsonb.instance as instance_mod

    storage = _small_storage(pool_timeout=2.0)
    try:
        inst = storage.new_instance()

        def boom(conn):
            raise psycopg.OperationalError("simulated failure after BEGIN")

        monkeypatch.setattr(instance_mod, "_read_max_tid", boom)
        with pytest.raises(psycopg.OperationalError):
            inst.poll_invalidations()
        assert inst._conn is None
        # pool_max_size=1: the only slot must be free again
        pool = storage._instance_pool
        conn = pool.getconn(timeout=0.5)
        pool.putconn(conn)
        inst.release()
    finally:
        storage.close()


def test_instance_recovers_after_failed_poll(monkeypatch):
    """A parked instance without a connection must work on its next use;
    today a None _conn dies with AttributeError."""
    import psycopg
    import zodb_pgjsonb.instance as instance_mod

    storage = _small_storage(pool_timeout=2.0)
    try:
        inst = storage.new_instance()
        real = instance_mod._read_max_tid

        def boom(conn):
            raise psycopg.OperationalError("simulated")

        monkeypatch.setattr(instance_mod, "_read_max_tid", boom)
        with pytest.raises(psycopg.OperationalError):
            inst.poll_invalidations()
        monkeypatch.setattr(instance_mod, "_read_max_tid", real)
        assert inst.poll_invalidations() == []
        assert inst._in_read_txn is True
        inst.release()
    finally:
        storage.close()
```

- [ ] **Step 2: Run to verify they fail**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pool_lock_inversion.py -k "failed_poll" -v`
Expected: `test_failed_poll_returns_slot` FAILS (`inst._conn is not None`, or `PoolTimeout` on the follow-up `getconn`).

- [ ] **Step 3: Implement**

Add next to `_ensure_conn()`:

```python
    def _drop_conn(self):
        """Return the held connection to the pool and forget it.

        Never raises.  The pool rolls back an open transaction and discards
        a broken connection, so this is safe in any connection state.
        """
        conn, self._conn = self._conn, None
        self._in_read_txn = False
        if conn is not None:
            try:
                self._instance_pool.putconn(conn)
            except Exception:
                logger.warning("returning connection to pool failed", exc_info=True)
```

`_replace_broken_conn` becomes:

```python
        self._drop_conn()
        self._ensure_conn()
```

(keep its docstring).

In `poll_invalidations`, move everything from `self._begin_read_txn()` to `self._polled_tid = new_tid` / `return result` into a new private method `_poll(self)`, and call it:

```python
        try:
            return self._poll()
        except Exception:
            # Never let an exception escape Connection.open() /
            # newTransaction() with a slot checked out: ZODB strands that
            # Connection, and the slot stays lost until GC (#126).  The next
            # use checks out a fresh connection via _ensure_conn().
            self._drop_conn()
            raise
```

`_end_read_txn()` and `_apply_pending_ddl()` stay before the `try` (unchanged order; `_end_read_txn` never raises).

- [ ] **Step 4: Run tests**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pool_lock_inversion.py tests/test_idle_in_xact.py -v`
Expected: all pass, including `test_poll_invalidations_reconnects_when_conn_killed` and `test_open_write_cycle_does_not_leak_pool_on_server_close` (#85 regressions).

- [ ] **Step 5: Commit**

```bash
git add src/zodb_pgjsonb/instance.py tests/test_pool_lock_inversion.py
git commit -m "fix: a failed poll_invalidations() returns its pool slot (#126)

Assisted-by: Claude Opus 5.5"
```

---

### Task 3: Burst scenario test + failover reproduction

**Files:**
- Test: `tests/test_pool_lock_inversion.py`
- Create: `benchmarks/failover_repro.py` (already written during the analysis; review and keep)
- Modify: `benchmarks/README.md` (one short section)

**Interfaces:**
- Consumes: behaviour from Tasks 1 and 2 (no new names).

- [ ] **Step 1: Write the scenario test**

```python
def test_burst_beyond_pool_max_recovers():
    """More concurrent openers than pool slots (piled-up readiness probes
    after a failover) must not wedge the process (#126)."""
    import ZODB

    storage = _small_storage(pool_max_size=3, pool_timeout=2.0)
    db = ZODB.DB(storage, pool_size=7)
    try:
        release = threading.Event()
        close_times = []
        lock = threading.Lock()

        def holder():
            c = db.open()
            c.root()
            release.wait(10)
            t0 = time.monotonic()
            c.close()
            with lock:
                close_times.append(time.monotonic() - t0)

        def opener():
            try:
                c = db.open()
                c.root()
                c.close()
            except Exception:
                pass  # PoolTimeout while the burst lasts is fine

        holders = [threading.Thread(target=holder, daemon=True) for _ in range(3)]
        for t in holders:
            t.start()
        _wait_for_slots_taken(storage._instance_pool, 3)
        openers = [threading.Thread(target=opener, daemon=True) for _ in range(8)]
        for t in openers:
            t.start()
        _wait_for_pool_waiter(storage._instance_pool)
        release.set()
        for t in holders + openers:
            t.join(15)
        assert not any(t.is_alive() for t in holders + openers)
        assert max(close_times) < 1.0, f"close() latencies: {close_times}"

        t0 = time.monotonic()
        c = db.open()
        c.root()
        c.close()
        assert time.monotonic() - t0 < 1.0
    finally:
        db.close()
        storage.close()
```

plus the helper next to `_wait_for_pool_waiter`:

```python
def _wait_for_slots_taken(pool, n, timeout=3.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        s = pool.get_stats()
        if s.get("pool_size", 0) - s.get("pool_available", 0) >= n:
            return
        time.sleep(0.02)
    raise AssertionError(f"{n} pool slots never got checked out")
```

- [ ] **Step 2: Verify it fails without the fix, passes with it**

Without the fix (temporarily `git checkout origin/main -- src/zodb_pgjsonb/instance.py`, never `git stash`, which is shared between worktrees): the test FAILS with `close() latencies` of about 2 s. Restore with `git checkout HEAD -- src/zodb_pgjsonb/instance.py`. On the branch: PASS.

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_pool_lock_inversion.py -v`

- [ ] **Step 3: Manual failover verification**

Needs a PostgreSQL superuser; the script creates and drops its own database `zodb_failover_repro`.

```bash
REPRO_PG="user=zodb password=zodb host=localhost port=5433" \
    uv run python benchmarks/failover_repro.py
```

Expected on the branch: `req_ok`/`probe_ok` reappear within a few seconds of `=== database back`, `threads=` returns to its baseline (about 9), and the final histogram shows no thread in `_returnToPool` or `open:751`. On 1.17.0 the run ends wedged (see the root-cause section). Paste both final histograms into the PR body.

- [ ] **Step 4: README note, then commit**

Add to `benchmarks/README.md` a short section "Failover reproduction (`failover_repro.py`)" with the command above and the healthy/wedged signatures.

```bash
git add tests/test_pool_lock_inversion.py benchmarks/failover_repro.py benchmarks/README.md
git commit -m "test: burst-beyond-pool scenario and failover reproduction (#126)

Assisted-by: Claude Opus 5.5"
```

---

### Task 4: Docs and changelog

**Files:**
- Modify: `CHANGES.md` (new `## 1.17.1 (unreleased)` section on top)
- Modify: `docs/sources/how-to/deploy-production.md` (pool sizing, L9-25)
- Modify: `docs/sources/reference/configuration.md` (`pool-max-size` entry), `docs/sources/llms.txt` (same statement, if it repeats the sizing rule)

- [ ] **Step 1: CHANGES entry**

```markdown
## 1.17.1 (unreleased)

### Bug fixes

- **Fix: a database failover could wedge every Zope process
  permanently (#126).** ZODB creates new connections while holding its
  `DB._lock`, and the storage instance checked a connection out of the
  pool right there. When the pool was full, the opening thread waited
  up to `pool-timeout` while holding the lock, and every thread trying
  to *return* a connection waited for that lock. After a failover,
  piled-up readiness probes kept the convoy alive indefinitely: all
  pool connections sat `idle` (last statement `COMMIT`) and every request
  failed with `PoolTimeout`. The instance now checks out its connection
  on first use, outside the lock, and a failed `poll_invalidations()`
  returns its connection instead of keeping it.
```

- [ ] **Step 2: Correct the sizing rule in the deploy guide**

Replace the `pool-max-size` bullet with:

```markdown
- **pool-max-size**: maximum connections (default: 10).
  Every *open or idle* ZODB connection holds one pool connection, and
  ZODB keeps up to its own `pool-size` (Zope default: 7) idle connections
  around. Size it to at least ZODB `pool-size` + Zope worker threads +
  cache-warmer concurrency + 2. Multiply by the number of processes when
  you size PostgreSQL's `max_connections`.
```

Apply the same rule to the `pool-max-size` entry in `configuration.md` and `llms.txt` if they repeat the old "number of threads plus a margin" wording.

- [ ] **Step 3: Docs build + commit**

Run: `uv run --group docs make -C docs html` if the repo defines that group; otherwise skip the build and note it in the PR.

```bash
git add CHANGES.md docs/
git commit -m "docs: changelog and corrected pool sizing rule (#126)

Assisted-by: Claude Opus 5.5"
```

---

## Out of scope: follow-ups (separate issues/PRs)

1. **zodb-pgjsonb: release the slot while a ZODB connection is parked (the issue's proposal B).** Return the connection in `afterCompletion()` and let `poll_invalidations()` check out again. Parked connections then pin nothing, and the per-process footprint drops from "ZODB pool-size + active" to "active". The costs need measuring first: one pool round trip per transaction plus the `check_connection` round trip (`SELECT ''`) per checkout (non-explicit ZODB re-polls right after every commit). Needs a benchmark gate with `benchmarks/bench.py --runs` before deciding, and possibly dropping `check=` in favour of the BEGIN retry. That is a design fork, so it gets its own spec.
2. **plone.observability: single-flight readiness check.** `ThreadingHTTPServer` starts an unbounded thread per probe, and each one opens a ZODB connection that may wait 30 s. That pile-up turned a short stall into a permanent convoy. Run at most one ZODB check at a time and answer concurrent probes from the last result.
3. **plone.pgcatalog: request connection leaks its slot when the connection is closed.** `release_request_connection()` only calls `putconn()` when `not conn.closed`, and `get_request_connection()` replaces a closed connection without returning it. Each such case loses a pool slot for good. The docstring's claim that `max_idle` reclaims it is wrong: `max_idle` only applies to connections idle *in* the pool. Always `putconn()`; the pool discards broken connections (as zodb-pgjsonb does since #81).
4. **Ops (aaf):** 9 pods × 11 connections reached `max_connections` 100. Revisit after follow-up 1.
5. **Liveness signal (issue proposal C):** expose `pool.get_stats()` (waiting clients, wait time) as a metric. Make liveness fail only on a sustained wedge (for example `requests_waiting > 0` and zero successful checkouts for N minutes), never on a plain DB outage, or a failover turns into a restart storm.
