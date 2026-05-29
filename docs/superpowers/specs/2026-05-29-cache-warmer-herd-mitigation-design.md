# Cache warmer herd mitigation — design

**Date:** 2026-05-29
**Status:** brainstormed, awaiting implementation plan
**Issue:** [#59](https://github.com/bluedynamics/zodb-pgjsonb/issues/59) — Cache warmer thundering herd on rolling deploys
**Scope:** smear and cap the inter-pod DB load that `CacheWarmer.warm()` produces during rolling Kubernetes deployments. Bundle: per-pod startup delay + jitter (A1+A2), paced batched warmup (A3), and a PG-advisory-lock semaphore that caps cluster-wide concurrent warmers (B2b).

## Problem recap

On every pod startup, `PGJsonbStorage.__init__` spawns a daemon thread that runs `CacheWarmer.warm()`. The warmer issues a single `SELECT ... WHERE zoid = ANY(...)` against `object_state` for ~`target` rows (default `cache_warm_pct=10`% of the per-connection cache budget, ~8800 rows on the production site).

The query itself is fine. The pain is that **N pods do this independently within a tight rolling-deploy window** (~30s), so the primary DB sees N× the qps for the same hot rows with no per-pod additional benefit. Observed: 6 pods × ~1700 qps each = ~10 kqps on a DB whose baseline is ~100 qps, pushing primary CPU from baseline ~10% to 90+%.

Already mitigated by prior work:

- [#63 / 1.12.0](https://github.com/bluedynamics/zodb-pgjsonb/pull/66) — `SharedLoadCache` eliminated intra-pod cache duplication. Each pod now warms one process-wide cache instead of K per-connection caches.
- [#65 / 1.12.1](https://github.com/bluedynamics/zodb-pgjsonb/pull/68) — warmer hardening: consensus-TID race recovery, `current_max_tid` consolidation.
- [#57 / 1.10.4](https://github.com/bluedynamics/zodb-pgjsonb/pull/55) — `startup_locks.py` introduced the **PG advisory lock** primitive we reuse here.

Issue [#59 idea #3](https://github.com/bluedynamics/zodb-pgjsonb/issues/59) (opt-out knob) is already implemented as `cache-warm-pct=0`.

What is **not** yet mitigated: inter-pod simultaneity. That is the scope of this design.

## Goals

1. **Smear the warmer load across pods** so a rolling deploy doesn't produce a synchronous burst.
2. **Move the warmer's DB work out of each pod's own cold-start window**, so it doesn't compete with Plone import + `plone.pgcatalog` schema-check + `ANALYZE object_state` chatter on the same connection pool.
3. **Lower the per-pod peak qps** so even worst-case alignment after smear-out doesn't saturate.
4. **Cap cluster-wide concurrent warmers** so very large fleets (12+ pods) still don't stack on the primary.

Non-goals (deferred to a later release):

- Eliminating the per-pod DB cost structurally (e.g., materialized warm-set table, peer-to-peer warming, replica routing).
- A real readiness-signal handshake with the embedder (Plone/Zope). The "delay" knob here is a fixed-time approximation; a real handshake is a 1.13+ story.
- Cross-pod warm-data transfer.

## Design

Four composable behaviors added to `CacheWarmer.warm()`. Each is independently configurable; all default to "on but conservative". The pacing and semaphore can be effectively disabled by setting their knobs to extreme values (concurrency = very large, batch pause = 0).

### A2 — Baseline startup delay

The warmer thread sleeps `cache_warm_delay` seconds before doing **anything else** (no `SELECT`, no lock acquisition).

- **Purpose:** get the warmer's DB queries out of the pod's own cold-start window, where Plone is still importing, `plone.pgcatalog` is doing its schema-check, and `ANALYZE object_state` is firing on the same connection pool.
- **Default:** 15 seconds.
- **Rationale:** observed Plone cold-start storm typically winds down by T+10–20s on a healthy pod. 15s is a conservative midpoint.

### A1 — Jitter on top of the baseline

After the baseline `cache_warm_delay`, the warmer additionally sleeps `random.uniform(0, cache_warm_jitter)` seconds before proceeding.

- **Purpose:** spread the lock-acquisition arrivals across pods so they don't collide on the lock function.
- **Default:** 30 seconds.
- **Rationale:** with 6 pods booting in ~30s, a 0-30s uniform jitter spreads lock arrivals over the full deploy window. The semaphore (B2b) handles concurrency *after* arrival; jitter just keeps arrivals clean.

Conceptually `A2 + A1` is `random.uniform(delay, delay + jitter)` — single `time.sleep` call, two knobs.

### B2b — N-slot PG-advisory-lock semaphore with wait-on-miss

After the A2+A1 sleep, the warmer tries to acquire a slot:

```python
for slot in range(1, concurrency + 1):
    if pg_try_advisory_lock(WARMER_LOCK_NS, slot):
        # got slot; warm
        break
else:
    # all slots taken
    sleep random.uniform(2, 5); retry
    # bounded by cache_warm_wait_max; on expiry skip + WARNING
```

- **Slot keyspace:** `WARMER_LOCK_NS = 0x5A4442` (`"ZDB"`, same namespace as startup DDL), `slot ∈ {1..N}`. Distinct from the existing `STARTUP_DDL_LOCK_KEY = 1`; reserve `slot ∈ {100..199}` for the warmer to leave room for future lock kinds.
- **Lock connection lifecycle:** mirror `startup_locks.py` — dedicated `psycopg.connect(dsn, autocommit=True)` for the warmer's lock. Session-level lock auto-releases on connection close, so a pod crash mid-warm safely frees the slot.
- **Default concurrency:** 2.
  - Rationale: small enough to keep DB headroom; large enough to cover the common 4–8 pod fleet without unreasonable last-pod latency.
- **Default wait cap:** 300s (5 min).
  - On expiry: skip warming, log WARNING, return (the daemon thread terminates).
- **Retry sleep:** `random.uniform(2, 5)` seconds between attempts. Jittered to prevent retry stampedes.

### A3 — Paced batched warmup

`CacheWarmer.warm()` currently issues a single `SELECT ... WHERE zoid = ANY(...)` for the full `target` set, materializing the entire result in one round trip. Change to batched fetches:

```python
batch_size = cache_warm_batch_size           # default 500
batch_pause = cache_warm_batch_pause         # default 0.5 seconds
for chunk in chunked(top_zoids, batch_size):
    results = load_multiple_fn(chunk_p64)
    for oid, (data, tid_bytes) in results.items():
        shared_cache.set(...)
    time.sleep(batch_pause)
```

- **Purpose:** lower per-pod peak qps. Instead of one big burst, the load is spread over `(target / batch_size) × batch_pause` seconds.
  - With defaults: 8800 / 500 × 0.5s ≈ 9s of work for one pod.
- **Defaults:** batch_size=500, batch_pause=0.5s.
- **Trade-off:** longer per-pod warmup duration (9s vs ~5s today) but lower peak qps and less interference with concurrent activity on the connection pool.
- **Edge:** small `target` (< batch_size) → one batch, no inter-batch sleep needed (skip the trailing `time.sleep`).

### Composed flow

```
storage init
   │
   ├─ spawn daemon thread "zodb-cache-warmer"
   │
   └─ daemon thread:
        1. sleep cache_warm_delay + random(0, cache_warm_jitter)        ← A2 + A1
        2. open dedicated PG conn for lock holding
        3. loop:                                                         ← B2b
             for slot in 1..N:
                 if pg_try_advisory_lock(WARMER_NS, 100+slot):
                     break
             else:
                 if waited > cache_warm_wait_max: log WARN + return
                 sleep random(2, 5); retry
        4. read top-N zoids from cache_warm_stats
        5. prime consensus_tid (existing code path)
        6. for chunk in chunked(top_zoids, batch_size):                  ← A3
               results = load_multiple_fn(chunk)
               for each result: shared_cache.set(...)
               if not last chunk: sleep batch_pause
        7. release lock (or rely on connection close)
        8. close dedicated PG conn
```

## Components

### `src/zodb_pgjsonb/cache_warmer.py`

- `CacheWarmer.__init__` gains: `delay`, `jitter`, `concurrency`, `wait_max`, `batch_size`, `batch_pause` parameters with the defaults above. Also a `dsn` parameter (needed to open the dedicated lock connection — the warmer's existing `conn` is the main storage connection and is the wrong scope for a session-level lock).
- `CacheWarmer.warm()` orchestrates the new flow as shown above. The existing primer-consensus logic is preserved in step 5.
- A new private helper `CacheWarmer._acquire_slot()` returns `(conn, slot)` or `None` on timeout. Wraps the dedicated-connection open, the slot-retry loop, and the WARNING log on timeout.
- A new private helper `CacheWarmer._release_slot(conn, slot)` releases the lock and closes the connection.

### `src/zodb_pgjsonb/storage.py`

- `PGJsonbStorage.__init__` reads the six new config values from kwargs and forwards them to `CacheWarmer(...)`. Also passes `dsn=self._dsn` so the warmer can open its own lock connection.
- Existing daemon-thread spawn stays as-is (`threading.Thread(target=self._warmer.warm, ...)`). All new behavior lives inside `warm()`.

### `src/zodb_pgjsonb/component.xml` + `src/zodb_pgjsonb/config.py`

Six new ZConfig keys (all `default` values per A1/A2/A3/B2b above):

| Key | Type | Default | Description |
|---|---|---|---|
| `cache-warm-delay` | integer (seconds) | 15 | Baseline sleep before warmer starts. Moves warmer out of the pod's own cold-start window. Set to 0 to disable. |
| `cache-warm-jitter` | integer (seconds) | 30 | Additional `random(0, jitter)` sleep on top of `cache-warm-delay`. Spreads warmer arrivals across pods. Set to 0 to disable. |
| `cache-warm-concurrency` | integer | 2 | Maximum number of pods warming concurrently in the cluster (enforced via PG advisory locks). Set to a very high value to effectively disable the cap. |
| `cache-warm-wait-max` | integer (seconds) | 300 | If all slots are taken, wait at most this long before skipping. On skip, log a WARNING. |
| `cache-warm-batch-size` | integer | 500 | Number of zoids fetched per `SELECT` batch. |
| `cache-warm-batch-pause` | float (seconds) | 0.5 | Sleep between batches. Lowers per-pod peak qps. Set to 0 to disable. |

`config.py` maps these via `getattr(config, "cache_warm_X", default)`, following the existing pattern for `cache_warm_pct` and `cache_warm_decay`.

### Lock namespace

Constants in `cache_warmer.py` (not `startup_locks.py` — different concern, different lifetime):

```python
WARMER_LOCK_NS = 0x5A4442   # shared "ZDB" namespace
WARMER_SLOT_BASE = 100      # slot keys are WARMER_SLOT_BASE + i for i in 1..concurrency
```

The slot keys start at 100 to leave a clear gap from `STARTUP_DDL_LOCK_KEY = 1`. There is no hard upper bound — operators who set `cache-warm-concurrency=9999` simply use keys `101..10099`. Future lock kinds should pick a base well above the largest expected `concurrency` (e.g., `100_000+`).

## Error handling

- **Lock connection fails to open** (DSN issue, PG down, pool exhausted) — log WARNING, skip warming, daemon thread exits. No retries — if the DB is unhealthy, the warmer should not pile on.
- **All slots taken until `cache_warm_wait_max` expires** — log WARNING with the wait duration and pod identity hint (logger name + thread name suffice), skip warming, release the dedicated connection.
- **`pg_try_advisory_lock` raises** during the retry loop — treat as a slot-miss for that iteration; the next retry opens a fresh connection if needed.
- **Warming itself raises after slot acquired** — existing `try/except` around `load_multiple_fn(chunk)` is extended to the batch loop. On any chunk failure: log WARNING, release the slot, close the lock connection, return from `warm()` (the daemon thread terminates). Do not retry; recording-phase stats will pick up the missing zoids on next boot anyway.
- **`pg_advisory_unlock` raises on release** — log WARNING; close the connection regardless. PG auto-releases session locks on connection close.

## Observability

- Existing INFO log `Cache warmer started (target=%d, decay=%.1f)` extended to include the new parameters' effective values.
- New INFO log on slot acquisition: `"Cache warmer: acquired slot %d after %.1fs wait"`.
- New INFO log on retry: `"Cache warmer: all %d slots taken, retrying (waited %.1fs so far)"` — logged on every retry. Cheap and operators want to see this.
- New WARNING on timeout: `"Cache warmer: gave up after %.1fs waiting for slot, skipping warmup"`.
- Existing INFO log on completion extended with elapsed time: `"Cache warmer: loaded %d of %d objects into shared cache in %.1fs (%d batches)"`.

## Testing

Following the existing test style (`tests/test_cache_warmer.py`, `tests/test_shared_load_cache.py`):

### Unit tests (no DB needed)

- A2+A1 delay sleep is called with `delay + random(0, jitter)` — assert with a mocked `time.sleep`.
- A3 batching: `target=1100, batch_size=500` produces 3 batches with 2 inter-batch sleeps of `batch_pause`. Last batch does not sleep.
- B2b retry logic: mock `pg_try_advisory_lock` to return False for slots 1..N, True only after K retries; assert K-1 retries occurred, K slot-attempts logged, and warming proceeds.
- B2b timeout: `pg_try_advisory_lock` always False; assert warmer exits after `wait_max`, WARNING logged, no warming performed.
- Lock-connection-open failure: `psycopg.connect` raises; assert WARNING logged, no warming, no crash.

### Integration tests (DB-marked, `@pytest.mark.db`)

- **Slot acquisition end-to-end:** two `CacheWarmer` instances in parallel threads against a real PG with `concurrency=1`; assert serialized warming (one finishes before the other starts).
- **Concurrency=N:** N+1 warmers, `concurrency=N`; assert N warm in parallel and the (N+1)th waits then warms.
- **Crash-safe lock release:** start a warmer, kill its lock connection mid-warm; assert another warmer can acquire the slot.
- **Existing warmup regression tests** (consensus-TID race, set acceptance, recording → flush → top-N read) continue to pass.

### Performance smoke test (manual, documented in CHANGES)

- 6-pod local simulation (6 processes via shell loop, each instantiating `PGJsonbStorage`): measure peak qps against a local PG with `pg_stat_activity` / `pg_stat_statements`. Compare to baseline (all knobs disabled). Expected: peak qps drops by roughly `concurrency / N_pods` and is spread over `delay + jitter + warmup_duration` seconds instead of a 5s burst.

## Migration / compatibility

- All new config keys have defaults; existing deployments get the new behavior automatically.
- The new defaults are conservative but **do change observable behavior**: existing deployments will see warmer queries delayed by ~15-45s post-startup. Document this prominently in the CHANGES entry.
- For deployments that want the pre-1.12.x behavior (immediate, unbatched, uncoordinated) — set `cache-warm-delay=0`, `cache-warm-jitter=0`, `cache-warm-batch-pause=0`, `cache-warm-concurrency=9999`. Document this escape hatch.
- No DB schema changes. No new tables. The advisory lock keys are transient.

## Last-pod latency analysis (for the CHANGES note)

Worst-case time-to-warm for the last pod in a rolling deploy:

```
T_last = cache_warm_delay
       + ceil(N_pods / cache_warm_concurrency) × warmup_duration
       + retry_jitter_average
```

Where `warmup_duration ≈ (target / batch_size) × batch_pause`.

For the production scenario (N=6, concurrency=2, target=8800, batch_size=500, batch_pause=0.5):

```
warmup_duration ≈ 18 × 0.5s = 9s
T_last ≈ 15s + ceil(6/2) × 9s + ~3s = ~45s
```

That's ~45s before the last pod has a warm cache. Pod is already serving traffic from T+0; warmer is daemon. So this is "time until all pods have a warm L2", not "time until pods are serving".

## Open questions

None outstanding for this scope. The bigger architectural plays (materialized warm-set table, peer-to-peer warming, replica routing) are explicitly deferred and tracked separately.

## Issue lifecycle

[#59](https://github.com/bluedynamics/zodb-pgjsonb/issues/59) stays **open** after this work merges. On merge, post a comment on the issue summarizing what was shipped (A1+A2+A3+B2b knobs and their defaults) and naming the deferred follow-ons (C1 materialized warm-set, C2 dump export, C4 replica routing). The issue remains the tracking thread for the bigger structural plays.

## References

- Issue: [#59](https://github.com/bluedynamics/zodb-pgjsonb/issues/59)
- Prior art: [`src/zodb_pgjsonb/startup_locks.py`](../../../src/zodb_pgjsonb/startup_locks.py) — PG advisory lock pattern reused here.
- Related changes: #63 (SharedLoadCache), #65 (warmer hardening), #57 (startup DDL lock)
