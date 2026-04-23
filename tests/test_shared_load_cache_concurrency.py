"""Concurrency stress tests for SharedLoadCache (#63).

Runs many threads performing interleaved get/set/poll_advance and
verifies that no bytes are returned for a zoid at a newer TID than
the caller's snapshot, and that byte accounting matches the actual
cache contents.
"""

from ZODB.utils import p64
from ZODB.utils import u64
from zodb_pgjsonb.storage import SharedLoadCache

import random
import threading


def test_no_stale_reads_under_concurrent_writes_and_polls():
    """Under mixed thread load, any get hit honours its polled_tid.

    Invariant: the tid_bytes of any returned entry must be <= the
    reader's polled_tid. A returned entry newer than the reader's
    snapshot indicates the consensus gate failed.
    """
    cache = SharedLoadCache(max_mb=4)
    cache.poll_advance(new_tid=1000, changed_zoids=[])
    stop = threading.Event()
    violations = []

    def reader(tid):
        while not stop.is_set():
            for z in range(100):
                res = cache.get(zoid=z, polled_tid=tid)
                if res is not None:
                    _, tb = res
                    if u64(tb) > tid:
                        violations.append((z, u64(tb), tid))

    def writer(start_tid):
        tid = start_tid
        while not stop.is_set():
            cache.poll_advance(new_tid=tid, changed_zoids=[random.randint(0, 99)])
            for _ in range(20):
                z = random.randint(0, 99)
                cache.set(
                    zoid=z,
                    data=b"x" * random.randint(100, 5000),
                    tid_bytes=p64(tid),
                    polled_tid=tid,
                )
            tid += random.randint(1, 10)

    threads = []
    for start in (1000, 1500, 2000):
        threads.append(threading.Thread(target=reader, args=(start,)))
    for start in (1000, 3000):
        threads.append(threading.Thread(target=writer, args=(start,)))
    for t in threads:
        t.start()

    # Run briefly, then stop
    threading.Event().wait(2.0)
    stop.set()
    for t in threads:
        t.join(timeout=5.0)

    assert violations == [], (
        f"Cache returned entries at tid > polled_tid: {violations[:5]}"
    )


def test_byte_accounting_consistent_under_concurrent_load():
    """After the dust settles, _current_bytes equals sum of cached byte lens."""
    cache = SharedLoadCache(max_mb=2)
    cache.poll_advance(new_tid=1000, changed_zoids=[])
    stop = threading.Event()

    def worker():
        tid = 1000
        while not stop.is_set():
            for _ in range(50):
                z = random.randint(0, 199)
                cache.set(
                    zoid=z,
                    data=b"x" * random.randint(100, 50_000),
                    tid_bytes=p64(tid),
                    polled_tid=tid,
                )
            cache.poll_advance(
                new_tid=tid, changed_zoids=[random.randint(0, 199) for _ in range(5)]
            )
            tid += 1

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()

    threading.Event().wait(2.0)
    stop.set()
    for t in threads:
        t.join(timeout=5.0)

    with cache._lock:
        actual_bytes = sum(len(e[0]) for e in cache._cache.values())
        assert cache._current_bytes == actual_bytes, (
            f"Accounting drift: tracked={cache._current_bytes} actual={actual_bytes}"
        )
        assert cache._current_bytes <= cache._max_bytes
