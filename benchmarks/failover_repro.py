"""Failover reproduction for #126: a DB outage wedges the instance pool.

Time-scaled (~1/10) model of the production setup that failed:
pool-max-size 10, ZODB pool-size 7, three "waitress" threads that follow
Zope's ``load_app`` / ``transaction_pubevents`` semantics, and a readiness
probe that, like plone.observability's ``ThreadingHTTPServer``, starts a
new thread per probe (``db.open()`` / ``root()`` / ``close()``).

The failover is simulated by terminating every backend of the database and
refusing new connections (``ALLOW_CONNECTIONS false``) for ``--outage``
seconds.  A snapshot line is printed every two seconds.

Healthy result: within a few seconds of "database back", ``req_ok`` and
``probe_ok`` reappear and the thread count returns to its baseline.

Wedged result (1.17.0): ``pg_pool size=10 avail=0``, the server shows
``idle|COMMIT: 10``, no ``req_ok``/``probe_ok`` any more, and the thread
count keeps growing.  The thread histogram printed at the end shows one
thread inside ``getconn`` holding ``DB._lock``, the slot holders blocked in
``Connection.close -> DB._returnToPool``, and everyone else in ``DB.open``.

Needs a PostgreSQL superuser (for ``ALTER DATABASE`` and
``pg_terminate_backend``).  Usage::

    REPRO_PG="user=zodb password=zodb host=localhost port=5433" \\
        uv run python benchmarks/failover_repro.py
"""

from collections import Counter
from ZODB.DB import DB
from zodb_pgjsonb.storage import PGJsonbStorage

import argparse
import logging
import os
import psycopg
import sys
import threading
import time
import traceback
import transaction


DB_NAME = "zodb_failover_repro"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--pool-timeout", type=float, default=3.0)
    p.add_argument("--reconnect-timeout", type=float, default=30.0)
    p.add_argument("--before", type=float, default=8.0)
    p.add_argument("--outage", type=float, default=30.0)
    p.add_argument("--after", type=float, default=40.0)
    p.add_argument("--probe-every", type=float, default=1.0)
    p.add_argument("--workers", type=int, default=3)
    return p.parse_args()


class Stats:
    def __init__(self):
        self._lock = threading.Lock()
        self.window = Counter()
        self.errors = Counter()

    def record(self, kind, exc=None):
        with self._lock:
            self.window[kind] += 1
            if exc is not None:
                self.errors[f"{kind}: {type(exc).__name__}: {str(exc)[:60]}"] += 1

    def take(self):
        with self._lock:
            out = dict(self.window)
            self.window.clear()
            return out


def main():
    args = parse_args()
    base = os.environ.get(
        "REPRO_PG", "user=zodb password=zodb host=localhost port=5433"
    )
    admin_dsn = f"dbname=postgres {base}"
    dsn = f"dbname={DB_NAME} {base}"

    with psycopg.connect(admin_dsn, autocommit=True) as a:
        a.execute(f"DROP DATABASE IF EXISTS {DB_NAME} WITH (FORCE)")
        a.execute(f"CREATE DATABASE {DB_NAME}")

    logging.basicConfig(level=logging.ERROR, stream=sys.stdout)
    stats = Stats()
    t0 = time.monotonic()

    storage = PGJsonbStorage(
        dsn,
        pool_size=1,
        pool_max_size=10,
        pool_timeout=args.pool_timeout,
        cache_warm_pct=0,
    )
    storage._instance_pool.reconnect_timeout = args.reconnect_timeout
    db = DB(storage, pool_size=7)
    with db.transaction() as c:
        for i in range(50):
            c.root()[f"k{i}"] = i

    stop = threading.Event()

    def zope_request(i, write):
        """Mirror ZPublisher.WSGIPublisher.load_app + transaction_pubevents."""
        tm = transaction.manager
        conn = db.open()
        try:
            try:
                tm.begin()
                root = conn.root()
                _ = [root[f"k{j}"] for j in range(0, 50, 7)]
                if write:
                    root[f"w{i % 5}"] = time.time()
                tm.commit()
            except Exception:
                tm.abort()
                raise
        finally:
            if tm.manager._txn is not None:
                tm.abort()
            conn.close()

    def worker():
        i = 0
        while not stop.is_set():
            i += 1
            try:
                zope_request(i, write=(i % 10 == 0))
                stats.record("req_ok")
            except Exception as e:
                stats.record("req_fail", e)
            time.sleep(0.05)

    def probe():
        """plone.observability ZODBReadinessCheck."""
        try:
            conn = db.open()
            try:
                conn.root()
            finally:
                conn.close()
            stats.record("probe_ok")
        except Exception as e:
            stats.record("probe_fail", e)

    def prober():
        while not stop.is_set():
            threading.Thread(target=probe, daemon=True, name="probe").start()
            time.sleep(args.probe_every)

    adm = psycopg.connect(admin_dsn, autocommit=True)

    def snapshot(label):
        ps = storage._instance_pool.get_stats()
        parked = [c for _, c in db.pool.available]
        all_c = list(db.pool.all)
        try:
            rows = adm.execute(
                "SELECT state, left(query, 30) FROM pg_stat_activity "
                "WHERE datname = %s",
                (DB_NAME,),
            ).fetchall()
            server = dict(Counter(f"{s}|{q}" for s, q in rows))
        except Exception as e:
            server = f"(query failed: {type(e).__name__})"
        print(
            f"[{time.monotonic() - t0:6.1f}s {label:6}] "
            f"pg_pool size={ps.get('pool_size')} avail={ps.get('pool_available')} "
            f"waiting={ps.get('requests_waiting')} | zodb all={len(all_c)} "
            f"parked={len(parked)} threads={threading.active_count()} | "
            f"{stats.take()} | server={server}",
            flush=True,
        )

    for n in range(args.workers):
        threading.Thread(target=worker, daemon=True, name=f"waitress-{n}").start()
    threading.Thread(target=prober, daemon=True, name="prober").start()

    phase = "before"
    t_back = args.before + args.outage
    while time.monotonic() - t0 < t_back + args.after:
        time.sleep(2)
        elapsed = time.monotonic() - t0
        if phase == "before" and elapsed >= args.before:
            adm.execute(f"ALTER DATABASE {DB_NAME} ALLOW_CONNECTIONS false")
            adm.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                "WHERE datname = %s",
                (DB_NAME,),
            )
            phase = "OUTAGE"
            print("=== failover: backends killed, new connections refused")
        elif phase == "OUTAGE" and elapsed >= t_back:
            adm.execute(f"ALTER DATABASE {DB_NAME} ALLOW_CONNECTIONS true")
            phase = "after"
            print("=== database back")
        snapshot(phase)

    stop.set()
    print("\n=== errors over the whole run")
    for k, v in stats.errors.most_common(10):
        print(f"{v:6d}  {k}")

    print("\n=== where the threads are (innermost library frames)")
    hist = Counter()
    frames = sys._current_frames()
    for t in threading.enumerate():
        f = frames.get(t.ident)
        if f is None:
            continue
        stack = [
            fs
            for fs in traceback.extract_stack(f)
            if "threading.py" not in fs.filename and __file__ not in fs.filename
        ]
        key = " <- ".join(f"{fs.name}:{fs.lineno}" for fs in reversed(stack[-3:]))
        hist[(t.name.split("-")[0], key or "(own code)")] += 1
    for (name, key), v in hist.most_common():
        print(f"{v:4d} {name:9} {key}")


if __name__ == "__main__":
    main()
