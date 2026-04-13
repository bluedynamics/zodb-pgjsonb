# Idle-in-Transaction Read Sessions Block CIC — Design Spec

**Issue:** https://github.com/bluedynamics/plone-pgcatalog/issues/118
**Date:** 2026-04-13
**Scope:** primarily `zodb-pgjsonb` (architectural fix), secondarily `plone-pgcatalog` (defensive cleanup)

## Problem

`CREATE INDEX CONCURRENTLY` (and `DROP INDEX CONCURRENTLY`,
`REINDEX CONCURRENTLY`) on `object_state` hangs for minutes — often
indefinitely under load — because pgcatalog read paths leave PG
connections in `idle in transaction` state, each holding a
`virtualxid` lock.  PostgreSQL CIC must wait for *all* existing
virtualxids to drain before finalizing.

## Verified root cause

`PGJsonbStorageInstance` opens a `REPEATABLE READ` transaction in
[`poll_invalidations()`](src/zodb_pgjsonb/instance.py) at the start of
every Plone request and keeps it open until *one* of:

1. The next `poll_invalidations()` call (commits, then re-opens).
2. The next `tpc_begin()` call (writes — commits, then opens an
   explicit write txn).
3. `release()` is called.  But ZODB only calls `storage.close()`
   (which calls `release()`) when the connection is *truly discarded*
   from the pool — not on every `Connection.close()`.

ZODB's standard hook for "the request is done, close any open
transaction" is `IStorage.afterCompletion()`:

- [Connection.py:313](.venv/lib/python3.13/site-packages/ZODB/Connection.py):
  `if hasattr(self._storage, 'afterCompletion'): self._storage.afterCompletion()`
- Also fires after every `transaction.commit()` / `abort()` via the
  synchronizer ([Connection.py:737-747](.venv/lib/python3.13/site-packages/ZODB/Connection.py)).

`PGJsonbStorageInstance` **does not implement** `afterCompletion()` —
so the hook is silently a no-op.  The read txn persists across
requests until the connection is actually evicted from the pool, which
under steady load may be hours.

A secondary contribution:
[`plone-pgcatalog/src/plone/pgcatalog/pool.py:74-89`](sources/plone-pgcatalog/src/plone/pgcatalog/pool.py)
calls `pool.putconn(conn)` without an explicit `commit/rollback`.  For
the pool fallback path (tests, scripts, no-ZODB use), pool conns are
not autocommit and any prior `SELECT` leaves an implicit txn open
until the next user reuses the conn.  This is a much smaller leak
(pool connections rotate, the bug only matters for the brief idle
window) but should be fixed for symmetry.

## Why CIC blocks on virtualxids

PostgreSQL `CREATE INDEX CONCURRENTLY` performs three sequential
phases.  Between phases, it `SELECT`s and *waits* for every other
session's `virtualxid` to disappear, so it can prove no concurrent
writer is in flight.  An `idle in transaction` session keeps its
virtualxid alive.  Read transactions are not exempt — virtualxids do
not distinguish read-only from read-write.

A Plone instance under steady load can have many virtualxids alive at
any moment.  CIC's wait set never empties → it waits forever.

## Goal

Eliminate idle-in-transaction PG sessions whose only reason to exist
is "the previous Plone request opened a read snapshot and never
explicitly closed it."  Reduce the worst-case duration of any
`virtualxid` to "the active duration of one Plone request" — a
bounded, manageable quantity.

## Design

### Fix 1 — implement `afterCompletion()` on `PGJsonbStorageInstance` (zodb-pgjsonb)

```python
def afterCompletion(self):
    """ZODB hook: end any open REPEATABLE READ snapshot.

    Called by ZODB after every transaction (commit or abort) and on
    Connection.close().  Closing the read transaction at request end
    avoids holding a virtualxid that blocks CREATE INDEX CONCURRENTLY
    and similar maintenance operations (#118).
    """
    try:
        self._end_read_txn()
    except Exception:
        # Never propagate from a sync hook — the connection may have
        # been killed externally (e.g. by idle_in_transaction_session_timeout).
        # Mark the conn dirty so the next operation rebuilds it via the pool.
        log.warning("afterCompletion: _end_read_txn failed", exc_info=True)
```

**Properties:**
- Idempotent: `_end_read_txn` checks `_in_read_txn` and is a no-op if
  not in a read txn.
- Safe at every call site:
  - After write `tpc_finish`: `tpc_begin` already called
    `_end_read_txn`, so `_in_read_txn=False` → no-op.
  - After read-only request: read txn is open → COMMIT → virtualxid
    released.
  - After abort: same as above.
  - On `Connection.close()`: same as above.
- Snapshot consistency unchanged: ZODB's contract is per-transaction
  consistency; `poll_invalidations()` always opens a fresh snapshot
  for the next transaction anyway.

**Performance impact:**
- One extra `COMMIT` round-trip per Plone request (only when the
  request actually opened a read txn — i.e. did at least one
  `poll_invalidations` + load).  Localhost PG: ~0.1ms.  Network PG:
  one RTT.  Acceptable.
- No effect on write requests (already `_end_read_txn`'d in tpc_begin).

**Risks:**
- A storage that overrides `afterCompletion` (RelStorage-style
  pattern) might do unexpected things on `_end_read_txn` failure.
  Mitigation: try/except + log + don't propagate.

### Fix 2 — `idle_in_transaction_session_timeout` defense in depth (zodb-pgjsonb)

Even with Fix 1, leaks remain possible:
- Crashed Zope worker (SIGKILL): no afterCompletion fires.
- A buggy storage subclass / future plugin that bypasses the hook.
- The pool fallback path in plone-pgcatalog (and any other consumer
  using the same DSN).

Bound the impact with a server-side timer on every PG connection
zodb-pgjsonb opens:

```python
DEFAULT_IDLE_IN_XACT_TIMEOUT_MS = 60_000
ENV_IDLE_IN_XACT_TIMEOUT = "ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS"

def _configure_conn(conn):
    """Per-connection setup applied by the pool's `configure` hook."""
    timeout = int(
        os.environ.get(ENV_IDLE_IN_XACT_TIMEOUT, DEFAULT_IDLE_IN_XACT_TIMEOUT_MS)
    )
    if timeout > 0:
        conn.execute(f"SET idle_in_transaction_session_timeout = {timeout}")
```

Wired via `psycopg_pool.ConnectionPool(..., configure=_configure_conn)`.

**Properties:**
- 60s default: long enough that a slow Plone request doesn't get its
  read tx killed mid-request, short enough that a leaked tx clears
  before CIC (which itself may have a 5-min timeout in
  `apply_index`).
- Override: env var.  Operator can set 0 to disable, or e.g. 30s for
  stricter envs.
- Applies to all conns from zodb-pgjsonb's pool — including the conns
  plone-pgcatalog discovers via `_pool_from_storage()`.  Two birds.

**Risks:**
- A request that legitimately holds a read tx open longer than the
  timeout — e.g. a slow third-party tool — will get killed and have
  to retry.  60s is generous.  Document.

### Fix 3 — explicit rollback before `pool.putconn` (plone-pgcatalog)

In `pool.py:release_request_connection()`, replace:

```python
if conn is not None and pool is not None:
    try:
        if not conn.closed:
            pool.putconn(conn)
    except Exception:
        log.warning("Failed to return connection to pool", exc_info=True)
```

with:

```python
if conn is not None and pool is not None:
    try:
        if not conn.closed:
            with contextlib.suppress(Exception):
                conn.rollback()  # release any implicit txn before pooling
            pool.putconn(conn)
    except Exception:
        log.warning("Failed to return connection to pool", exc_info=True)
```

**Properties:**
- Affects the fallback path only (no ZODB storage available).
- `rollback()` on an autocommit conn or a conn with no open txn is
  cheap and safe.
- Defense in depth — Fix 2's timeout will eventually clean up
  anyway, but explicit rollback is immediate.

### Fix 4 (optional, separate PR) — drop-invalid-indexes UI (plone-pgcatalog)

Independent of the cause-fix.  In the ZMI Catalog → Slow Queries tab:

- Query `pg_index` for `NOT indisvalid` entries on `object_state`.
- For each, render a row with name + size + age + a "Drop" button
  that issues `DROP INDEX CONCURRENTLY IF EXISTS <name>`.
- Refuse to drop indexes outside the `idx_os_*` namespace (mirroring
  `drop_index`'s safety in `suggestions.py`).

Self-service recovery for the failure mode that prompted this issue.
*Out of scope for this fix bundle*; can be a follow-up PR.

### Out of scope

- **Background CIC job with progress reporting** (issue #118 option E).
  Real CIC duration is bounded once Fix 1 + 2 land — no more
  multi-hour hangs.  Adding a job worker is a substantial new
  subsystem; defer until there's an actual signal it's needed.
- **Maintenance-mode toggle** (issue #118 option C).  After Fix 1 +
  2, CIC under normal load should complete in seconds (PG only waits
  for the *currently active* virtualxids, which all clear within
  60s).  Toggle adds operational complexity for a problem the
  primary fixes already solve.

## Trade-off analysis

| Option | Fixes root cause? | Complexity | Risk | Verdict |
|--------|-------------------|------------|------|---------|
| **Fix 1 (afterCompletion)** | yes — virtualxid drops at request end | very low (8 LOC) | low (idempotent, try/except) | **must do** |
| **Fix 2 (PG timeout)** | bounds worst case | low (15 LOC + env var) | low (60s is generous) | **must do** (defense in depth) |
| **Fix 3 (explicit rollback)** | partial (fallback path only) | trivial (3 LOC) | none | **should do** |
| Fix 4 (drop-invalid UI) | no — recovery only | medium (UI + endpoint) | low | **defer to follow-up** |
| Option E (background job) | no — masks symptom | high (worker, queue, status) | high (new subsystem) | **out of scope** |

## Test plan

### Fix 1 — `afterCompletion`

Integration tests in zodb-pgjsonb (require PG):

1. **`test_after_completion_closes_read_txn`**: open instance, call
   `poll_invalidations()` to start a snapshot, assert
   `_in_read_txn` is True, call `afterCompletion()`, assert it's
   False and the conn has no open transaction
   (`SELECT pg_backend_pid()` from a separate conn querying
   `pg_stat_activity` for our pid → state should be `idle`, not
   `idle in transaction`).
2. **`test_after_completion_idempotent`**: call twice in a row, no
   error, second call is a no-op.
3. **`test_after_completion_safe_when_conn_killed`**: open snapshot,
   externally `pg_terminate_backend` the conn, call
   `afterCompletion()`, assert no exception propagates.
4. **`test_after_completion_no_op_after_tpc_begin`**: write txn (tpc_begin
   already ended the read), call `afterCompletion()` — no-op.
5. **End-to-end via ZODB**: open `ZODB.DB(storage)`, do a read-only
   transaction, `connection.close()`, query `pg_stat_activity` for
   our session — assert `state = 'idle'`.

### Fix 2 — `idle_in_transaction_session_timeout`

Integration tests:

1. **`test_idle_timeout_set_on_new_conn`**: open storage, take a conn
   from the pool, query `current_setting('idle_in_transaction_session_timeout')`
   → `'60000'`.
2. **`test_idle_timeout_env_override`**: set env var to `5000`,
   re-open storage, assert setting is `'5000'`.
3. **`test_idle_timeout_disabled_when_zero`**: env var `0` → setting
   is `'0'` (PG's "disabled" value).

### Fix 3 — explicit rollback

Unit test in plone-pgcatalog (mock conn):

1. **`test_release_request_connection_rolls_back`**: mock conn, call
   `release_request_connection()`, assert `conn.rollback()` was
   called before `pool.putconn(conn)`.

## Operator guidance (CHANGES.md + docs)

After deploy:
- No action required — fixes are transparent.
- If a deployment routinely sees CIC stalls before the fix, document
  the pre-fix workaround:
  ```sql
  SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
  WHERE datname = current_database()
    AND state = 'idle in transaction'
    AND pid <> pg_backend_pid();
  ```
- Tunable: `ZODB_PGJSONB_IDLE_IN_XACT_TIMEOUT_MS` (default 60000).
  Set to `0` to disable the safety net entirely.

## PR split

- **PR1 (zodb-pgjsonb):** Fix 1 + Fix 2.  Co-located because Fix 2
  is defense-in-depth for Fix 1 — landing them separately leaves a
  weird intermediate state.
- **PR2 (plone-pgcatalog):** Fix 3.  Independent, small, can land
  in either order.
- **(future) PR3 (plone-pgcatalog):** Fix 4 drop-invalid-indexes UI.
  Operator quality-of-life; not a regression.

## Risk summary

| Risk | Mitigation |
|------|------------|
| `afterCompletion` raises and breaks ZODB pool return | try/except + log |
| 60s timeout kills a slow legitimate request | env-var override; document tunable |
| Snapshot continuity expected by some caller | ZODB only guarantees per-txn; `poll_invalidations` re-opens snapshot |
| Tests can't observe `pg_stat_activity` reliably (CI noise) | use deterministic checks: query backend's own setting, `_in_read_txn` flag, conn `pgconn.transaction_status` |
