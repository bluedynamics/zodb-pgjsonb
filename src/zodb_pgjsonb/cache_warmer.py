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
