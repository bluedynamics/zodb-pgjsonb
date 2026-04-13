# Idle-in-Transaction Fix: zodb-pgjsonb (PR1) — Implementation Plan

**Goal:** Stop `PGJsonbStorageInstance` leaking REPEATABLE READ transactions across requests so they no longer block `CREATE INDEX CONCURRENTLY`.

**Architecture:**
1. Implement `IStorage.afterCompletion()` on `PGJsonbStorageInstance` — calls `_end_read_txn()`, idempotent, never raises.  ZODB calls it after every transaction commit/abort and on `Connection.close()`.
2. Apply `idle_in_transaction_session_timeout` (default 60_000ms, env-overridable) to every PG connection the instance pool hands out — defense in depth.

**Tech Stack:** psycopg 3, psycopg_pool, ZODB, pytest

**Spec:** `docs/superpowers/specs/2026-04-13-idle-in-xact-design.md`

---

## File Map

- Modify: `src/zodb_pgjsonb/instance.py` — add `afterCompletion`
- Modify: `src/zodb_pgjsonb/storage.py` — pool `configure` hook applies the idle timeout (replacing the inline `lambda`)
- Create: `tests/test_idle_in_xact.py` — integration tests (require PG)
- Modify: `CHANGES.md` — Unreleased entry

---

### Task 1: Failing test — `afterCompletion` should close read txn

**Files:**
- Create: `tests/test_idle_in_xact.py`

- [ ] **Step 1: Write the test file with one failing test**

```python
"""Tests for #118: read tx must be closed at request end so virtualxids
don't accumulate and block CREATE INDEX CONCURRENTLY.
"""

from tests.conftest import clean_db
from tests.conftest import DSN

import psycopg
import pytest


pytestmark = pytest.mark.db


def _backend_state(monitor_conn, pid):
    """Return (state, xact_age_seconds) for a backend pid via a separate conn."""
    with monitor_conn.cursor() as cur:
        cur.execute(
            "SELECT state, "
            "EXTRACT(EPOCH FROM (now() - xact_start)) AS xact_age "
            "FROM pg_stat_activity WHERE pid = %s",
            (pid,),
        )
        row = cur.fetchone()
        if row is None:
            return (None, None)
        return (row["state"], row["xact_age"])


def test_after_completion_closes_read_txn():
    """afterCompletion() must end the REPEATABLE READ snapshot."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        instance = storage.new_instance()
        try:
            # poll_invalidations opens the read txn
            instance.poll_invalidations()
            assert instance._in_read_txn is True

            # afterCompletion must close it
            instance.afterCompletion()
            assert instance._in_read_txn is False
        finally:
            instance.release()
    finally:
        storage.close()
```

- [ ] **Step 2: Verify the test fails before the fix**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_idle_in_xact.py::test_after_completion_closes_read_txn -v`
Expected: FAIL with `AttributeError: ... 'PGJsonbStorageInstance' object has no attribute 'afterCompletion'`.

- [ ] **Step 3: Commit the failing test**

```bash
git add tests/test_idle_in_xact.py
git commit -m "test: failing regression for #118 — read txn must close on afterCompletion"
```

---

### Task 2: Implement `afterCompletion` on `PGJsonbStorageInstance`

**Files:**
- Modify: `src/zodb_pgjsonb/instance.py` (add method right after `release` at ~line 111)

- [ ] **Step 1: Add the method**

Locate `def _end_read_txn(self):` at line 113.  Insert this *before* it (right after `release`):

```python
    def afterCompletion(self):
        """ZODB hook: end any open REPEATABLE READ snapshot.

        Called by ZODB after every transaction commit/abort and on
        ``Connection.close()`` (see ``ZODB.Connection.close``).
        Closing the read transaction at request end avoids holding a
        ``virtualxid`` that blocks ``CREATE INDEX CONCURRENTLY`` and
        similar maintenance operations (#118).

        Idempotent: ``_end_read_txn`` short-circuits when no read tx
        is open (e.g. right after a write transaction's ``tpc_begin``
        already ended it).  Never raises — the connection may have
        been killed by an external operator or by
        ``idle_in_transaction_session_timeout``; in that case we just
        log and let the next operation rebuild via the pool.
        """
        try:
            self._end_read_txn()
        except Exception:
            log.warning("afterCompletion: _end_read_txn failed", exc_info=True)
```

Verify the file already imports `log` at module top: open the file, search for `^log = ` or `import logging`.  If absent, add at the top of `instance.py`:

```python
import logging

log = logging.getLogger(__name__)
```

(If a logger exists under a different name like `logger`, use it instead of introducing `log`.)

- [ ] **Step 2: Run the previously-failing test**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_idle_in_xact.py::test_after_completion_closes_read_txn -v`
Expected: PASS.

- [ ] **Step 3: Run the full test suite, no regressions**

Run: `PYTHONPATH=src .venv/bin/pytest -q`
Expected: same baseline (allowing 5 pre-existing test_oid_sequence failures unrelated to this change).

- [ ] **Step 4: Commit**

```bash
git add src/zodb_pgjsonb/instance.py
git commit -m "fix(instance): implement afterCompletion to close read txn at request end (#118)"
```

---

### Task 3: Edge-case tests for `afterCompletion`

**Files:**
- Modify: `tests/test_idle_in_xact.py`

- [ ] **Step 1: Append more tests**

Append after the first test:

```python
def test_after_completion_idempotent():
    """Calling afterCompletion twice in a row is safe."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        instance = storage.new_instance()
        try:
            instance.poll_invalidations()
            instance.afterCompletion()
            # Second call: no-op, no error
            instance.afterCompletion()
            assert instance._in_read_txn is False
        finally:
            instance.release()
    finally:
        storage.close()


def test_after_completion_no_op_after_tpc_begin():
    """tpc_begin already ends the read tx — afterCompletion is a no-op then."""
    import transaction as txn
    from persistent.mapping import PersistentMapping
    from zodb_pgjsonb.storage import PGJsonbStorage

    import ZODB

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        db = ZODB.DB(storage)
        try:
            conn = db.open()
            root = conn.root()
            root["x"] = PersistentMapping({"k": "v"})
            txn.commit()
            instance = conn._storage
            # After commit, read tx should be closed (commit path ends it)
            instance.afterCompletion()
            assert instance._in_read_txn is False
            conn.close()
        finally:
            db.close()
    finally:
        storage.close()


def test_after_completion_safe_when_conn_killed():
    """If the conn is externally terminated, afterCompletion must not raise."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        instance = storage.new_instance()
        try:
            instance.poll_invalidations()
            assert instance._in_read_txn is True

            # Kill the backend from a separate session
            pid = instance._conn.info.backend_pid
            killer = psycopg.connect(DSN)
            try:
                killer.execute(
                    "SELECT pg_terminate_backend(%s)", (pid,)
                )
                killer.commit()
            finally:
                killer.close()

            # afterCompletion must swallow the resulting connection error
            instance.afterCompletion()  # must not raise
        finally:
            # Force release without raising — conn is dead
            try:
                instance.release()
            except Exception:
                pass
    finally:
        try:
            storage.close()
        except Exception:
            pass


def test_zodb_connection_close_releases_virtualxid():
    """End-to-end: a read-only ZODB transaction releases its virtualxid.

    Without afterCompletion, this would leave 'idle in transaction' on
    the storage instance's PG connection.
    """
    import transaction as txn
    from zodb_pgjsonb.storage import PGJsonbStorage

    import ZODB

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        db = ZODB.DB(storage)
        try:
            conn = db.open()
            instance = conn._storage
            _ = conn.root()  # triggers poll_invalidations + a load
            assert instance._in_read_txn is True

            pid = instance._conn.info.backend_pid

            # Abort the (read-only) transaction — synchronizer fires
            # afterCompletion via Connection.afterCompletion.
            txn.abort()

            # Read tx must now be closed.
            assert instance._in_read_txn is False

            # Confirm via pg_stat_activity that the backend is idle
            # (not "idle in transaction").
            monitor = psycopg.connect(DSN, row_factory=__import__(
                "psycopg.rows", fromlist=["dict_row"]
            ).dict_row)
            try:
                state, _age = _backend_state(monitor, pid)
                assert state == "idle", f"Expected idle, got {state!r}"
            finally:
                monitor.close()

            conn.close()
        finally:
            db.close()
    finally:
        storage.close()
```

- [ ] **Step 2: Run the new tests**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_idle_in_xact.py -v`
Expected: 4 passed.

- [ ] **Step 3: Commit**

```bash
git add tests/test_idle_in_xact.py
git commit -m "test: edge cases for afterCompletion (idempotent, post-tpc, killed conn, end-to-end)"
```

---

### Task 4: Failing test — `idle_in_transaction_session_timeout` configured

**Files:**
- Modify: `tests/test_idle_in_xact.py`

- [ ] **Step 1: Add the test**

Append:

```python
def test_idle_timeout_set_on_pool_conn():
    """Every conn from the instance pool has idle_in_transaction_session_timeout set."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    try:
        with storage._instance_pool.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT current_setting('idle_in_transaction_session_timeout') AS v"
                )
                row = cur.fetchone()
        # Default 60_000 ms, may include unit suffix (e.g. '60000' or '1min')
        # PostgreSQL typically returns the value as the number of ms with no suffix
        # when set via numeric: '60000'.
        assert row["v"] in ("60000", "1min")
    finally:
        storage.close()


def test_idle_timeout_env_override():
    """ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS env var overrides default."""
    import os

    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    old = os.environ.get("ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS")
    os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = "5000"
    try:
        storage = PGJsonbStorage(DSN, cache_warm_pct=0)
        try:
            with storage._instance_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT current_setting('idle_in_transaction_session_timeout') AS v"
                    )
                    row = cur.fetchone()
            assert row["v"] == "5000"
        finally:
            storage.close()
    finally:
        if old is None:
            del os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"]
        else:
            os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = old


def test_idle_timeout_disabled_when_zero():
    """Setting the env var to 0 disables the timeout (PG default)."""
    import os

    from zodb_pgjsonb.storage import PGJsonbStorage

    clean_db()
    old = os.environ.get("ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS")
    os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = "0"
    try:
        storage = PGJsonbStorage(DSN, cache_warm_pct=0)
        try:
            with storage._instance_pool.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT current_setting('idle_in_transaction_session_timeout') AS v"
                    )
                    row = cur.fetchone()
            assert row["v"] == "0"
        finally:
            storage.close()
    finally:
        if old is None:
            del os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"]
        else:
            os.environ["ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"] = old
```

- [ ] **Step 2: Verify the first one fails (default not configured yet)**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_idle_in_xact.py::test_idle_timeout_set_on_pool_conn -v`
Expected: FAIL — current setting is `'0'` (PG default).

- [ ] **Step 3: Commit failing tests**

```bash
git add tests/test_idle_in_xact.py
git commit -m "test: failing regression for default idle_in_transaction_session_timeout (#118)"
```

---

### Task 5: Implement the pool `configure` hook

**Files:**
- Modify: `src/zodb_pgjsonb/storage.py:268-281`

- [ ] **Step 1: Add the configure helper at module level**

Locate the imports section near the top (search for `import os`).  Add (or extend) an env-driven constant block.  At the bottom of the storage.py imports block (just above `class PGJsonbStorage`), add:

```python
# ── Idle-in-transaction safety net (#118) ─────────────────────────────
# Bound the worst-case duration of any leaked virtualxid so it doesn't
# block CREATE INDEX CONCURRENTLY indefinitely.  Tunable via env var.
DEFAULT_IDLE_IN_XACT_TIMEOUT_MS = 60_000
ENV_IDLE_IN_XACT_TIMEOUT = "ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"


def _configure_pool_conn(conn):
    """psycopg_pool ``configure`` hook applied on every new pool conn.

    Sets autocommit (required by the instance state machine) and the
    idle_in_transaction_session_timeout safety net.
    """
    conn.autocommit = True
    timeout_ms = int(
        os.environ.get(ENV_IDLE_IN_XACT_TIMEOUT, DEFAULT_IDLE_IN_XACT_TIMEOUT_MS)
    )
    if timeout_ms > 0:
        conn.execute(f"SET idle_in_transaction_session_timeout = {timeout_ms}")
    elif timeout_ms == 0:
        # Explicitly disabled — leave PG default (also 0).
        conn.execute("SET idle_in_transaction_session_timeout = 0")
```

Verify `os` is imported at module top.  If not, add `import os`.

- [ ] **Step 2: Replace the inline `lambda` configure with the named helper**

Find the existing pool creation:

```python
        self._instance_pool = ConnectionPool(
            dsn,
            min_size=pool_size,
            max_size=pool_max_size,
            timeout=pool_timeout,
            kwargs={"row_factory": dict_row},
            configure=lambda conn: setattr(conn, "autocommit", True),
            open=True,
        )
```

Change the `configure=` line to:

```python
            configure=_configure_pool_conn,
```

(Keep the rest unchanged.)

- [ ] **Step 3: Run the timeout tests**

Run: `PYTHONPATH=src .venv/bin/pytest tests/test_idle_in_xact.py -v`
Expected: all tests pass (4 from Task 3 + 3 timeout tests).

- [ ] **Step 4: Commit**

```bash
git add src/zodb_pgjsonb/storage.py
git commit -m "feat(pool): set idle_in_transaction_session_timeout on every conn (#118)"
```

---

### Task 6: Full suite + changelog

**Files:**
- Modify: `CHANGES.md`

- [ ] **Step 1: Run the full suite**

Run: `PYTHONPATH=src .venv/bin/pytest -q`
Expected: pre-existing baseline (5 unrelated failures permitted, but no new ones).

- [ ] **Step 2: Add changelog entry**

In `CHANGES.md`, find the `## Unreleased` block (under `# Changelog`).  If absent, create it directly under `# Changelog`.  Add:

```markdown
- Implement ``IStorage.afterCompletion`` on ``PGJsonbStorageInstance``
  so the REPEATABLE READ read-snapshot transaction is committed at
  request end (after every ``transaction.commit/abort`` and on
  ``Connection.close``).  Previously the read tx persisted across
  request boundaries until the connection was reused, leaving
  ``idle in transaction`` sessions with live virtualxids that
  blocked ``CREATE INDEX CONCURRENTLY`` for minutes-to-hours under
  load.  Closes bluedynamics/plone-pgcatalog#118.

- Set ``idle_in_transaction_session_timeout`` (default 60_000 ms,
  env-overridable via ``ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS``) on
  every connection from the instance pool.  Defense in depth for
  any future leak path that bypasses ``afterCompletion`` (e.g.
  ``SIGKILL``-ed worker, buggy plugin).
```

- [ ] **Step 3: Commit**

```bash
git add CHANGES.md docs/superpowers/specs/2026-04-13-idle-in-xact-design.md docs/superpowers/plans/2026-04-13-idle-in-xact-zodb-pgjsonb.md
git commit -m "docs: changelog + spec + plan for #118 read-tx fix"
```

---

### Task 7: Push + open PR

- [ ] **Step 1: Push branch**

```bash
git push -u origin fix/idle-in-xact
```

- [ ] **Step 2: Create PR**

```bash
gh pr create --repo bluedynamics/zodb-pgjsonb --base main --head fix/idle-in-xact \
  --title "Close read txn at request end (afterCompletion) + idle-in-xact safety net (#118)" \
  --body "$(cat <<'EOF'
## Summary

Stops \`PGJsonbStorageInstance\` leaking REPEATABLE READ transactions across requests, so they no longer block \`CREATE INDEX CONCURRENTLY\` and similar maintenance operations.

- **\`afterCompletion\`** on \`PGJsonbStorageInstance\` (root-cause fix).  ZODB calls this hook after every transaction commit/abort and on \`Connection.close()\`.  Previously the class did not define it, so the hook was a silent no-op and the read tx lingered until the connection was eventually evicted.  Method is idempotent (no-op when no read tx is open) and never propagates exceptions (a killed conn is logged, not raised).
- **\`idle_in_transaction_session_timeout = 60_000\`** on every connection the instance pool hands out — defense in depth for any future leak path (SIGKILL-ed worker, third-party plugin).  Tunable via \`ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS\` (set to \`0\` to disable).

Closes bluedynamics/plone-pgcatalog#118.

## Tests

- 4 \`afterCompletion\` integration tests: closes-read-txn, idempotent, no-op-after-tpc-begin, safe-when-killed.
- 1 end-to-end test: ZODB read-only transaction → \`pg_stat_activity\` shows \`state = 'idle'\` (not \`idle in transaction\`).
- 3 idle-timeout tests: default, env override, disabled-when-zero.

## Spec

\`docs/superpowers/specs/2026-04-13-idle-in-xact-design.md\` — full root-cause analysis, trade-off table (Fix 1 / 2 / 3 / 4 / E), test plan.

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

Report PR URL.
