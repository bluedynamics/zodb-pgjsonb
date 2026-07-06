"""Micro-benchmark: sequential per-object loads (N+1) vs a single prefetch.

Renders the two ways a result set of N objects gets loaded:

- **sequential** — ``load(oid)`` per object, the pattern ZODB's ``setstate``
  produces for a collection listing: N single-row round-trips.
- **prefetch**  — one ``prefetch(oids)`` (a single ``WHERE zoid = ANY(...)``
  query) that warms the cache, then N cache-hit loads.

Reports the number of ``object_state`` round-trips (RTT-independent proof) and
wall time.  Set ``BENCH_LATENCY_MS`` to simulate network/pooler round-trip
latency and see how the two scale.

Usage:
    env -u ZODB_TEST_DSN python benchmarks/bench_prefetch.py
    env -u ZODB_TEST_DSN BENCH_LATENCY_MS=10 python benchmarks/bench_prefetch.py
"""

from persistent.mapping import PersistentMapping
from tests.conftest import clean_db
from tests.conftest import DSN
from zodb_pgjsonb.storage import PGJsonbStorage

import os
import psycopg
import time
import transaction
import ZODB


N = int(os.environ.get("BENCH_N", "151"))
LATENCY_MS = float(os.environ.get("BENCH_LATENCY_MS", "0"))

# Count object_state round-trips, optionally injecting per-query latency to
# stand in for network/pooler RTT.
_orig_execute = psycopg.Cursor.execute
_counter = {"n": 0}


def _counting_execute(self, query, *args, **kwargs):
    text = query if isinstance(query, str) else bytes(query).decode("utf-8", "ignore")
    if "object_state" in text and text.lstrip().upper().startswith("SELECT"):
        _counter["n"] += 1
        if LATENCY_MS:
            time.sleep(LATENCY_MS / 1000.0)
    return _orig_execute(self, query, *args, **kwargs)


psycopg.Cursor.execute = _counting_execute


def _measure(label, storage, oids, use_prefetch):
    storage.clear_caches()  # cold L1 + L2
    inst = storage.new_instance()
    try:
        inst.poll_invalidations()
        _counter["n"] = 0
        t0 = time.perf_counter()
        if use_prefetch:
            inst.prefetch(oids)
        for oid in oids:
            inst.load(oid)
        elapsed = time.perf_counter() - t0
    finally:
        inst.release()
    print(f"  {label:<12} queries={_counter['n']:>4}  wall={elapsed * 1000:8.2f} ms")
    return _counter["n"], elapsed


def main():
    clean_db()
    storage = PGJsonbStorage(DSN, cache_warm_pct=0)
    db = ZODB.DB(storage)
    try:
        conn = db.open()
        root = conn.root()
        for i in range(N):
            root[f"o{i}"] = PersistentMapping(
                {"i": i, "payload": "x" * 500, "title": f"item {i}"}
            )
        transaction.commit()
        oids = [root[f"o{i}"]._p_oid for i in range(N)]
        conn.close()

        print(f"N={N} objects, simulated per-query latency={LATENCY_MS} ms\n")
        seq_q, seq_t = _measure("sequential", storage, oids, use_prefetch=False)
        pre_q, pre_t = _measure("prefetch", storage, oids, use_prefetch=True)

        print(
            f"\n  round-trips: {seq_q} -> {pre_q}  ({seq_q / max(pre_q, 1):.0f}x fewer)"
        )
        if pre_t > 0:
            print(f"  wall time:   {seq_t / pre_t:.1f}x faster")
    finally:
        db.close()
        storage.close()


if __name__ == "__main__":
    main()
