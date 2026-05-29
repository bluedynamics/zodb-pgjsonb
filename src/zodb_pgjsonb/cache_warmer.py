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
import random
import time

import psycopg


log = logging.getLogger(__name__)

# Schema DDL for the warm stats table
WARM_STATS_DDL = """\
CREATE TABLE IF NOT EXISTS cache_warm_stats (
    zoid   BIGINT PRIMARY KEY,
    score  FLOAT NOT NULL DEFAULT 1.0
);
"""

# Advisory-lock keyspace for B2b inter-pod warmer semaphore (#59).
# Same namespace as startup_locks.py (separate keys).
WARMER_LOCK_NS = 0x5A4442    # "ZDB"
WARMER_SLOT_BASE = 100       # slot keys are WARMER_SLOT_BASE + i for i in 1..N


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

    # ── Warming phase ────────────────────────────────────────────────

    def _read_top_oids(self):
        try:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT zoid FROM cache_warm_stats ORDER BY score DESC LIMIT %(n)s",
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
