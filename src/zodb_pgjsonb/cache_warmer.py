"""Learning cache warmer for zodb-pgjsonb.

Records which ZOIDs are loaded after each startup, persists scores
to PostgreSQL with exponential decay, and pre-loads the highest-scored
objects into a shared L2 warm cache on the next startup.

See docs/plans/2026-04-03-learning-cache-warmer-design.md for details.
"""

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
    """Learning cache warmer with L2 warm cache.

    Three phases:
    1. **Warm** (startup, background thread): load top-scored ZOIDs
       from ``cache_warm_stats`` into ``_warm_cache``.
    2. **Record** (after startup): track the first N unique ZOIDs
       loaded via ``load()``, flush to PG every ``flush_interval``.
    3. **Serve** (ongoing): Instance ``load()`` checks L2 after L1
       miss; ``poll_invalidations()`` invalidates L2 entries.
    """

    def __init__(self, conn, target_count, decay=0.8, flush_interval=1000):
        # Recording state
        self.recording = target_count > 0
        self._recorded = set()
        self._pending = set()
        self._target_count = target_count
        self._flush_interval = flush_interval
        self._decayed = False
        self._decay = decay

        # L2 warm cache
        self._warm_cache = {}
        self._warming_done = False

        # DB connection (main storage's conn, autocommit=True)
        self._conn = conn

    # ── Recording phase ──────────────────────────────────────────────

    def record(self, zoid):
        """Record a loaded ZOID.  Called from Instance.load().

        Flushes to PG every ``_flush_interval`` OIDs to limit data
        loss if the pod is killed.  Decay is applied only on the
        first flush.
        """
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
        """Write pending OIDs to cache_warm_stats."""
        zoids = list(self._pending)
        if not zoids:
            return
        self._pending.clear()
        try:
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
        except Exception:
            log.warning("Cache warmer: flush failed", exc_info=True)

    # ── Warming phase ────────────────────────────────────────────────

    def _read_top_oids(self):
        """Read top-scored ZOIDs from cache_warm_stats."""
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
        """Load top-N ZOIDs into L2 warm cache.

        Runs in a background daemon thread.  Builds a temporary dict,
        then swaps atomically to prevent race conditions with
        ``poll_invalidations()`` during loading.

        Args:
            load_multiple_fn: callable that takes a list of OID bytes
                and returns dict {oid_bytes: (pickle_bytes, tid_bytes)}.
        """
        from ZODB.utils import p64

        top_zoids = self._read_top_oids()
        if not top_zoids:
            self._warming_done = True
            log.info("Cache warmer: no stats yet, skipping warmup")
            return

        oids = [p64(z) for z in top_zoids]
        try:
            results = load_multiple_fn(oids)
        except Exception:
            log.warning("Cache warmer: load_multiple failed", exc_info=True)
            self._warming_done = True
            return

        from ZODB.utils import u64

        tmp = {}
        for oid, (data, tid) in results.items():
            tmp[u64(oid)] = (data, tid)

        self._warm_cache = tmp  # atomic swap
        self._warming_done = True
        log.info("Cache warmer: loaded %d objects into L2", len(tmp))

    # ── L2 cache access ──────────────────────────────────────────────

    def get(self, zoid):
        """L2 lookup.  Returns (data, tid) or None.

        Returns None while warming is still in progress.
        """
        if not self._warming_done:
            return None
        return self._warm_cache.get(zoid)

    def invalidate(self, zoid):
        """Remove a ZOID from the warm cache.

        Called by ``poll_invalidations()`` for changed objects.
        """
        self._warm_cache.pop(zoid, None)
