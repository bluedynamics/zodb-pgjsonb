"""Benchmark: PGJsonbStorage vs RelStorage performance.

Usage:
    python benchmarks/bench.py storage [--iterations N] [--warmup N]
    python benchmarks/bench.py zodb [--iterations N] [--warmup N]
    python benchmarks/bench.py pack
    python benchmarks/bench.py plone [--docs N]
    python benchmarks/bench.py all [--iterations N]

All commands accept --output FILE (JSON export) and --format {table,json,both}.
Requires PostgreSQL on localhost:5433.
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import UTC
from pathlib import Path
from ZODB.tests.MinPO import MinPO
from ZODB.tests.StorageTestBase import zodb_pickle

import argparse
import contextlib
import json
import psycopg
import statistics
import subprocess
import sys
import tempfile
import time


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

PGJSONB_DSN = (
    "dbname=zodb_bench_pgjsonb user=zodb password=zodb host=localhost port=5433"
)
RELSTORAGE_DSN = (
    "dbname=zodb_bench_relstorage user=zodb password=zodb host=localhost port=5433"
)

HEADER = "\033[1m"
RESET = "\033[0m"
DIM = "\033[2m"
GREEN = "\033[32m"
RED = "\033[31m"


# ---------------------------------------------------------------------------
# TimingStats (adapted from codec bench)
# ---------------------------------------------------------------------------


@dataclass
class TimingStats:
    """Aggregated timing statistics (milliseconds)."""

    samples: list[float] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.samples)

    @property
    def mean(self) -> float:
        return statistics.mean(self.samples) if self.samples else 0.0

    @property
    def median(self) -> float:
        return statistics.median(self.samples) if self.samples else 0.0

    @property
    def p95(self) -> float:
        return _percentile(self.samples, 0.95)

    @property
    def p99(self) -> float:
        return _percentile(self.samples, 0.99)

    @property
    def min_val(self) -> float:
        return min(self.samples) if self.samples else 0.0

    @property
    def max_val(self) -> float:
        return max(self.samples) if self.samples else 0.0

    @property
    def stddev(self) -> float:
        return statistics.stdev(self.samples) if len(self.samples) > 1 else 0.0

    def to_dict(self) -> dict:
        return {
            "count": self.count,
            "mean_ms": round(self.mean, 3),
            "median_ms": round(self.median, 3),
            "p95_ms": round(self.p95, 3),
            "p99_ms": round(self.p99, 3),
            "min_ms": round(self.min_val, 3),
            "max_ms": round(self.max_val, 3),
            "stddev_ms": round(self.stddev, 3),
        }


def _percentile(data: list[float], pct: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[f]
    return s[f] + (k - f) * (s[c] - s[f])


# ---------------------------------------------------------------------------
# Timing helper
# ---------------------------------------------------------------------------


def bench_one(fn, *args, iterations: int = 100, warmup: int = 10) -> TimingStats:
    """Time a function, return stats in milliseconds."""
    for _ in range(warmup):
        fn(*args)

    stats = TimingStats()
    for _ in range(iterations):
        t0 = time.perf_counter()
        fn(*args)
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)
    return stats


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------


def _fmt_ms(val: float) -> str:
    if val >= 1000:
        return f"{val / 1000:.2f} s"
    if val >= 1:
        return f"{val:.1f} ms"
    return f"{val * 1000:.0f} us"


def _comparison(pgjsonb: float, relstorage: float) -> str:
    if pgjsonb <= 0 or relstorage <= 0:
        return "N/A"
    if pgjsonb < relstorage:
        ratio = relstorage / pgjsonb
        return f"{GREEN}{ratio:.1f}x faster{RESET}"
    elif pgjsonb > relstorage:
        ratio = pgjsonb / relstorage
        return f"{RED}{ratio:.1f}x slower{RESET}"
    return "same"


# ---------------------------------------------------------------------------
# Backend factories
# ---------------------------------------------------------------------------


def _clean_pgjsonb_db():
    """Drop all PGJsonbStorage tables."""
    conn = psycopg.connect(PGJSONB_DSN)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state, blob_history, object_history, "
            "blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()


def make_pgjsonb():
    """Create a fresh PGJsonbStorage."""
    from zodb_pgjsonb.storage import PGJsonbStorage

    _clean_pgjsonb_db()
    return PGJsonbStorage(PGJSONB_DSN)


def _clean_relstorage_db():
    """Zap RelStorage tables."""
    conn = psycopg.connect(RELSTORAGE_DSN)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        # Drop known RelStorage tables
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state_tid, pack_state, pack_object, object_refs_added, "
            "object_ref, current_object, object_state, temp_store, "
            "temp_blob_chunk, temp_undo, blob_chunk, commit_row_lock, "
            "transaction, new_oid, temp_pack_visit CASCADE"
        )
    conn.commit()
    conn.close()


def make_relstorage():
    """Create a fresh RelStorage. Returns None if not available."""
    try:
        from relstorage.adapters.postgresql import PostgreSQLAdapter
        from relstorage.options import Options
        from relstorage.storage import RelStorage

        _clean_relstorage_db()
        options = Options(keep_history=False)
        adapter = PostgreSQLAdapter(dsn=RELSTORAGE_DSN, options=options)
        return RelStorage(adapter, options=options)
    except Exception as exc:
        print(
            f"  {DIM}RelStorage not available: {exc}{RESET}",
            file=sys.stderr,
        )
        return None


def make_backends() -> list[tuple[str, object]]:
    """Return available backends as (name, storage) pairs."""
    backends = [("PGJsonbStorage", make_pgjsonb())]
    rs = make_relstorage()
    if rs is not None:
        backends.append(("RelStorage", rs))
    return backends


def close_backends(backends: list[tuple[str, object]]):
    for _, storage in backends:
        with contextlib.suppress(Exception):
            storage.close()


# ---------------------------------------------------------------------------
# Storage-level benchmarks
# ---------------------------------------------------------------------------


def _bench_store_single(storage, iterations, warmup):
    """Benchmark: single store + 2PC cycle."""
    from ZODB.Connection import TransactionMetaData
    from ZODB.utils import z64

    data = zodb_pickle(MinPO(1))

    # Pre-allocate OIDs for all iterations (warmup + actual)
    total = warmup + iterations
    oids = [storage.new_oid() for _ in range(total)]
    idx = [0]

    def do_store():
        oid = oids[idx[0]]
        idx[0] += 1
        t = TransactionMetaData()
        storage.tpc_begin(t)
        storage.store(oid, z64, data, "", t)
        storage.tpc_vote(t)
        storage.tpc_finish(t)

    return bench_one(do_store, iterations=iterations, warmup=warmup)


def _bench_store_batch(storage, batch_size, iterations, warmup):
    """Benchmark: N objects in one transaction."""
    from ZODB.Connection import TransactionMetaData
    from ZODB.utils import z64

    data = zodb_pickle(MinPO(1))
    total = (warmup + iterations) * batch_size
    oids = [storage.new_oid() for _ in range(total)]
    idx = [0]

    def do_store_batch():
        t = TransactionMetaData()
        storage.tpc_begin(t)
        for _ in range(batch_size):
            oid = oids[idx[0]]
            idx[0] += 1
            storage.store(oid, z64, data, "", t)
        storage.tpc_vote(t)
        storage.tpc_finish(t)

    return bench_one(do_store_batch, iterations=iterations, warmup=warmup)


def _bench_load_cached(storage, iterations, warmup):
    """Benchmark: load from in-memory LRU cache (same OID, cache hit after warmup).

    After warmup iterations, the single OID is in the cache.
    All measured iterations are cache hits — this measures cache implementation
    speed (pure-Python OrderedDict for PGJsonb vs Cython for RelStorage).
    """
    from ZODB.Connection import TransactionMetaData
    from ZODB.utils import z64

    # Store one object to load
    oid = storage.new_oid()
    data = zodb_pickle(MinPO(42))
    t = TransactionMetaData()
    storage.tpc_begin(t)
    storage.store(oid, z64, data, "", t)
    storage.tpc_vote(t)
    storage.tpc_finish(t)

    def do_load():
        storage.load(oid)

    return bench_one(do_load, iterations=iterations, warmup=warmup)


def _bench_load_batch_cached(storage, batch_size, iterations, warmup):
    """Benchmark: N sequential load() calls from cache (cache hits after warmup).

    All N objects fit in the LRU cache after warmup. Measured iterations
    are all cache hits — shows cache throughput for N sequential lookups.
    """
    from ZODB.Connection import TransactionMetaData
    from ZODB.utils import z64

    # Store N objects
    data = zodb_pickle(MinPO(42))
    oids = []
    t = TransactionMetaData()
    storage.tpc_begin(t)
    for _ in range(batch_size):
        oid = storage.new_oid()
        oids.append(oid)
        storage.store(oid, z64, data, "", t)
    storage.tpc_vote(t)
    storage.tpc_finish(t)

    def do_load_batch():
        for oid in oids:
            storage.load(oid)

    return bench_one(do_load_batch, iterations=iterations, warmup=warmup)


def _bench_load_uncached(storage, iterations, warmup):
    """Benchmark: cold load from DB (guaranteed cache miss each iteration).

    Stores warmup+iterations unique objects, then loads each exactly once.
    Every measured load is a first-access cache miss, forcing a DB round-trip.
    For PGJsonbStorage this includes JSONB-to-pickle transcoding via the
    Rust codec. For RelStorage this is a raw bytea fetch.
    """
    from ZODB.Connection import TransactionMetaData
    from ZODB.utils import z64

    total = warmup + iterations
    data = zodb_pickle(MinPO(42))

    # Store all objects in batches of 100
    oids = []
    batch = 100
    for start in range(0, total, batch):
        end = min(start + batch, total)
        t = TransactionMetaData()
        storage.tpc_begin(t)
        for _ in range(end - start):
            oid = storage.new_oid()
            oids.append(oid)
            storage.store(oid, z64, data, "", t)
        storage.tpc_vote(t)
        storage.tpc_finish(t)

    idx = [0]

    def do_load():
        storage.load(oids[idx[0]])
        idx[0] += 1

    return bench_one(do_load, iterations=iterations, warmup=warmup)


def run_storage_benchmarks(iterations, warmup):
    """Run storage-level benchmarks for all backends."""
    results = {}

    benchmarks = [
        ("store single", lambda s: _bench_store_single(s, iterations, warmup)),
        ("store batch 10", lambda s: _bench_store_batch(s, 10, iterations, warmup)),
        (
            "store batch 100",
            lambda s: _bench_store_batch(s, 100, max(iterations // 5, 10), warmup),
        ),
        ("load cached", lambda s: _bench_load_cached(s, iterations, warmup)),
        (
            "load batch cached",
            lambda s: _bench_load_batch_cached(
                s, 100, max(iterations // 5, 10), warmup
            ),
        ),
        ("load uncached", lambda s: _bench_load_uncached(s, iterations, warmup)),
    ]

    for bench_name, bench_fn in benchmarks:
        results[bench_name] = {}
        for backend_name, make_fn in [
            ("PGJsonbStorage", make_pgjsonb),
            ("RelStorage", make_relstorage),
        ]:
            storage = make_fn()
            if storage is None:
                continue
            try:
                print(
                    f"  {DIM}{bench_name} ({backend_name})...{RESET}",
                    end="",
                    flush=True,
                )
                stats = bench_fn(storage)
                results[bench_name][backend_name] = stats
                print(f" {_fmt_ms(stats.mean)}")
            except Exception as exc:
                print(f" ERROR: {exc}")
            finally:
                storage.close()

    return results


# ---------------------------------------------------------------------------
# ZODB-level benchmarks
# ---------------------------------------------------------------------------


def _bench_zodb_write_simple(db, iterations, warmup):
    """Benchmark: write single object through ZODB.DB."""
    import transaction

    stats = TimingStats()
    conn = db.open()

    # Warmup
    for i in range(warmup):
        conn.root()[f"warmup_{i}"] = MinPO(i)
        transaction.commit()

    # Measure
    for i in range(iterations):
        t0 = time.perf_counter()
        conn.root()[f"bench_{i}"] = MinPO(i)
        transaction.commit()
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)

    conn.close()
    return stats


def _bench_zodb_write_btree(db, iterations, warmup):
    """Benchmark: insert into OOBTree through ZODB.DB."""
    from BTrees.OOBTree import OOBTree

    import transaction

    conn = db.open()
    conn.root()["tree"] = OOBTree()
    transaction.commit()

    stats = TimingStats()

    # Warmup
    for i in range(warmup):
        conn.root()["tree"][f"warmup_{i}"] = i
        transaction.commit()

    # Measure
    for i in range(iterations):
        t0 = time.perf_counter()
        conn.root()["tree"][f"bench_{i}"] = i
        transaction.commit()
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)

    conn.close()
    return stats


def _bench_zodb_read(db, iterations, warmup):
    """Benchmark: read from ZODB connection object cache.

    After first access, ZODB's connection pool retains the object cache,
    so subsequent reads are Python dict lookups (~2-3us for both backends).
    This measures ZODB object cache overhead, not storage performance.
    """
    import transaction

    conn = db.open()
    conn.root()["target"] = MinPO(42)
    transaction.commit()
    conn.close()

    stats = TimingStats()

    # Each read opens a fresh connection to avoid cache hits
    for _ in range(warmup):
        c = db.open()
        _ = c.root()["target"]
        c.close()

    for _ in range(iterations):
        c = db.open()
        t0 = time.perf_counter()
        _ = c.root()["target"]
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)
        c.close()

    return stats


def _bench_zodb_connection_cycle(db, iterations, warmup):
    """Benchmark: open connection + access root + close."""
    import transaction

    # Ensure root exists
    conn = db.open()
    conn.root()["init"] = 1
    transaction.commit()
    conn.close()

    def do_cycle():
        c = db.open()
        _ = c.root()
        c.close()

    return bench_one(do_cycle, iterations=iterations, warmup=warmup)


def _bench_zodb_write_batch(db, batch_size, iterations, warmup):
    """Benchmark: N objects modified in one transaction."""
    import transaction

    stats = TimingStats()
    conn = db.open()

    # Warmup
    for i in range(warmup):
        for j in range(batch_size):
            conn.root()[f"w_{i}_{j}"] = MinPO(j)
        transaction.commit()

    # Measure
    for i in range(iterations):
        t0 = time.perf_counter()
        for j in range(batch_size):
            conn.root()[f"b_{i}_{j}"] = MinPO(j)
        transaction.commit()
        t1 = time.perf_counter()
        stats.samples.append((t1 - t0) * 1000.0)

    conn.close()
    return stats


def run_zodb_benchmarks(iterations, warmup):
    """Run ZODB-level benchmarks for all backends."""
    import ZODB

    results = {}

    benchmarks = [
        (
            "zodb write simple",
            lambda db: _bench_zodb_write_simple(db, iterations, warmup),
        ),
        (
            "zodb write btree",
            lambda db: _bench_zodb_write_btree(db, iterations, warmup),
        ),
        ("zodb cached read", lambda db: _bench_zodb_read(db, iterations, warmup)),
        (
            "zodb connection cycle",
            lambda db: _bench_zodb_connection_cycle(db, iterations, warmup),
        ),
        (
            "zodb write batch 10",
            lambda db: _bench_zodb_write_batch(
                db, 10, max(iterations // 5, 10), warmup
            ),
        ),
    ]

    for bench_name, bench_fn in benchmarks:
        results[bench_name] = {}
        for backend_name, make_fn in [
            ("PGJsonbStorage", make_pgjsonb),
            ("RelStorage", make_relstorage),
        ]:
            storage = make_fn()
            if storage is None:
                continue
            db = None
            try:
                db = ZODB.DB(storage)
                print(
                    f"  {DIM}{bench_name} ({backend_name})...{RESET}",
                    end="",
                    flush=True,
                )
                stats = bench_fn(db)
                results[bench_name][backend_name] = stats
                print(f" {_fmt_ms(stats.mean)}")
            except Exception as exc:
                print(f" ERROR: {exc}")
            finally:
                if db is not None:
                    db.close()
                else:
                    storage.close()

    return results


# ---------------------------------------------------------------------------
# Pack benchmarks
# ---------------------------------------------------------------------------


def _bench_pack(make_fn, n_objects, pack_iterations=3):
    """Benchmark pack for a single backend at a given size.

    Runs pack_iterations times (fresh DB each), returns TimingStats.
    Each iteration: clean DB → populate → time pack → close.
    """
    from persistent.mapping import PersistentMapping

    import transaction
    import ZODB

    stats = TimingStats()
    reachable = n_objects // 2
    garbage = n_objects - reachable

    for _ in range(pack_iterations):
        storage = make_fn()
        if storage is None:
            return None, 0, 0

        db = None
        try:
            db = ZODB.DB(storage)
            conn = db.open()
            root = conn.root()

            # Create reachable objects
            for i in range(reachable):
                root[f"obj_{i}"] = PersistentMapping({"value": i})
            transaction.commit()

            # Create garbage objects (store directly, not referenced from root)
            from ZODB.Connection import TransactionMetaData
            from ZODB.utils import z64

            data = zodb_pickle(MinPO(999))
            t = TransactionMetaData()
            storage.tpc_begin(t)
            for _ in range(garbage):
                oid = storage.new_oid()
                storage.store(oid, z64, data, "", t)
            storage.tpc_vote(t)
            storage.tpc_finish(t)

            conn.close()

            # Measure pack
            t0 = time.perf_counter()
            db.pack(time.time())
            t1 = time.perf_counter()
            stats.samples.append((t1 - t0) * 1000.0)
        except Exception as exc:
            print(f" ERROR: {exc}")
            return None, 0, 0
        finally:
            if db is not None:
                db.close()
            else:
                storage.close()

    return stats, reachable, garbage


def run_pack_benchmarks():
    """Run pack benchmarks at various sizes (3 iterations each)."""
    results = {}

    for n_objects in [100, 1000, 10000]:
        results[n_objects] = {}
        for backend_name, make_fn in [
            ("PGJsonbStorage", make_pgjsonb),
            ("RelStorage", make_relstorage),
        ]:
            print(
                f"  {DIM}pack {n_objects} ({backend_name})...{RESET}",
                end="",
                flush=True,
            )
            stats, reachable, garbage = _bench_pack(make_fn, n_objects)
            if stats is not None and stats.count > 0:
                results[n_objects][backend_name] = stats
                print(
                    f" {_fmt_ms(stats.mean)} +/- {_fmt_ms(stats.stddev)}"
                    f" ({reachable} reachable, {garbage} garbage, n={stats.count})"
                )
            else:
                print(" skipped")

    return results


# ---------------------------------------------------------------------------
# Plone benchmarks (subprocess-isolated per backend)
# ---------------------------------------------------------------------------


# Paths
BENCH_DIR = Path(__file__).resolve().parent
PGJSONB_DIR = BENCH_DIR.parent
PROJECT_ROOT = PGJSONB_DIR.parent.parent
INSTANCE_HOME = PROJECT_ROOT / "instance"
WORKER_SCRIPT = BENCH_DIR / "bench_plone.py"

ZOPE_CONF_TEMPLATE = """\
%define INSTANCEHOME {instancehome}
instancehome $INSTANCEHOME
%define CLIENTHOME {clienthome}
clienthome $CLIENTHOME

debug-mode off
debug-exceptions off
security-policy-implementation C
verbose-security off
default-zpublisher-encoding utf-8
enable-xmlrpc off

<environment>
    CHAMELEON_CACHE {clienthome}/cache
    zope_i18n_compile_mo_files true
</environment>
<dos_protection>
    form-memory-limit 1MB
    form-disk-limit 1GB
    form-memfile-limit 4KB
</dos_protection>

{db_section}
"""

PGJSONB_DB_SECTION = """\
%import zodb_pgjsonb

<zodb_db main>
    mount-point /
    cache-size 30000
    <pgjsonb>
        dsn {dsn}
    </pgjsonb>
</zodb_db>"""

RELSTORAGE_DB_SECTION = """\
%import relstorage

<zodb_db main>
    mount-point /
    cache-size 30000
    <relstorage>
        keep-history false
        <postgresql>
            dsn {dsn}
        </postgresql>
    </relstorage>
</zodb_db>"""


def _generate_zope_conf(dsn, backend_type, tmp_dir):
    """Generate a temporary zope.conf for a given backend.

    Returns path to the generated config file.
    """
    clienthome = Path(tmp_dir) / f"clienthome-{backend_type}"
    clienthome.mkdir(exist_ok=True)
    (clienthome / "cache").mkdir(exist_ok=True)
    (clienthome / "log").mkdir(exist_ok=True)

    if backend_type == "pgjsonb":
        db_section = PGJSONB_DB_SECTION.format(dsn=dsn)
    elif backend_type == "relstorage":
        db_section = RELSTORAGE_DB_SECTION.format(dsn=dsn)
    else:
        raise ValueError(f"Unknown backend type: {backend_type}")

    conf_content = ZOPE_CONF_TEMPLATE.format(
        instancehome=INSTANCE_HOME,
        clienthome=clienthome,
        db_section=db_section,
    )

    conf_path = Path(tmp_dir) / f"zope-{backend_type}.conf"
    conf_path.write_text(conf_content)
    return str(conf_path)


def _run_plone_worker(conf_path, backend_name, n_docs):
    """Run bench_plone.py in a subprocess. Returns parsed JSON or None."""
    result = subprocess.run(
        [
            sys.executable,
            str(WORKER_SCRIPT),
            "--conf",
            conf_path,
            "--backend",
            backend_name,
            "--docs",
            str(n_docs),
        ],
        capture_output=True,
        text=True,
        timeout=600,  # 10 min max
    )

    if result.returncode != 0:
        print(f" FAILED (exit code {result.returncode})")
        # Show last few lines of stderr for debugging
        stderr_lines = result.stderr.strip().split("\n")
        for line in stderr_lines[-10:]:
            print(f"    {DIM}{line}{RESET}")
        return None

    # Parse JSON from stdout (last non-empty line)
    stdout_lines = [line for line in result.stdout.strip().split("\n") if line.strip()]
    if not stdout_lines:
        print(" FAILED (no output)")
        return None

    try:
        return json.loads(stdout_lines[-1])
    except json.JSONDecodeError as exc:
        print(f" FAILED (bad JSON: {exc})")
        return None


def run_plone_benchmarks(n_docs):
    """Run Plone benchmarks for all available backends."""
    results = {}

    with tempfile.TemporaryDirectory(prefix="zodb-bench-plone-") as tmp_dir:
        backends = [
            ("PGJsonbStorage", PGJSONB_DSN, "pgjsonb"),
        ]

        # Check if RelStorage is available
        try:
            import psycopg2  # noqa: F401
            import relstorage  # noqa: F401

            backends.append(("RelStorage", RELSTORAGE_DSN, "relstorage"))
        except ImportError:
            pass

        for backend_name, dsn, backend_type in backends:
            print(f"  {DIM}{backend_name} ({n_docs} docs)...{RESET}", flush=True)

            # Clean database
            if backend_type == "pgjsonb":
                _clean_pgjsonb_db()
            else:
                _clean_relstorage_db()

            # Generate temp zope.conf
            conf_path = _generate_zope_conf(dsn, backend_type, tmp_dir)

            # Run worker subprocess
            worker_result = _run_plone_worker(conf_path, backend_name, n_docs)
            if worker_result:
                results[backend_name] = worker_result
                site_ms = worker_result.get("site_creation_ms", 0)
                create_ms = worker_result.get("content_creation", {}).get("mean_ms", 0)
                query_ms = worker_result.get("catalog_query", {}).get("mean_ms", 0)
                modify_ms = worker_result.get("content_modification", {}).get(
                    "mean_ms", 0
                )
                print(
                    f"    site: {_fmt_ms(site_ms)}, "
                    f"create: {_fmt_ms(create_ms)}/doc, "
                    f"query: {_fmt_ms(query_ms)}, "
                    f"modify: {_fmt_ms(modify_ms)}/doc"
                )

    return results


# ---------------------------------------------------------------------------
# Output: terminal tables
# ---------------------------------------------------------------------------


def print_storage_results(results: dict, iterations: int, warmup: int):
    print(f"\n{HEADER}{'=' * 78}")
    print(f" Storage API ({iterations} iterations, {warmup} warmup)")
    print(f"{'=' * 78}{RESET}\n")

    has_relstorage = any("RelStorage" in v for v in results.values())

    if has_relstorage:
        print(
            f"  {'Operation':<24} {'PGJsonb':>12} {'RelStorage':>12} {'Comparison':>20}"
        )
        print(f"  {'-' * 70}")
    else:
        print(f"  {'Operation':<24} {'PGJsonb':>12}")
        print(f"  {'-' * 38}")

    for bench_name, backend_results in results.items():
        pgjsonb_stats = backend_results.get("PGJsonbStorage")
        rs_stats = backend_results.get("RelStorage")

        pj_str = _fmt_ms(pgjsonb_stats.mean) if pgjsonb_stats else "N/A"

        if has_relstorage:
            rs_str = _fmt_ms(rs_stats.mean) if rs_stats else "N/A"
            cmp_str = (
                _comparison(pgjsonb_stats.mean, rs_stats.mean)
                if pgjsonb_stats and rs_stats
                else ""
            )
            print(f"  {bench_name:<24} {pj_str:>12} {rs_str:>12} {cmp_str:>20}")
        else:
            print(f"  {bench_name:<24} {pj_str:>12}")

    print()


def print_zodb_results(results: dict, iterations: int, warmup: int):
    print(f"\n{HEADER}{'=' * 78}")
    print(f" ZODB.DB ({iterations} iterations, {warmup} warmup)")
    print(f"{'=' * 78}{RESET}\n")

    has_relstorage = any("RelStorage" in v for v in results.values())

    if has_relstorage:
        print(
            f"  {'Operation':<24} {'PGJsonb':>12} {'RelStorage':>12} {'Comparison':>20}"
        )
        print(f"  {'-' * 70}")
    else:
        print(f"  {'Operation':<24} {'PGJsonb':>12}")
        print(f"  {'-' * 38}")

    for bench_name, backend_results in results.items():
        pgjsonb_stats = backend_results.get("PGJsonbStorage")
        rs_stats = backend_results.get("RelStorage")

        pj_str = _fmt_ms(pgjsonb_stats.mean) if pgjsonb_stats else "N/A"

        if has_relstorage:
            rs_str = _fmt_ms(rs_stats.mean) if rs_stats else "N/A"
            cmp_str = (
                _comparison(pgjsonb_stats.mean, rs_stats.mean)
                if pgjsonb_stats and rs_stats
                else ""
            )
            print(f"  {bench_name:<24} {pj_str:>12} {rs_str:>12} {cmp_str:>20}")
        else:
            print(f"  {bench_name:<24} {pj_str:>12}")

    print()


def print_pack_results(results: dict):
    print(f"\n{HEADER}{'=' * 78}")
    print(" Pack / GC (3 iterations each)")
    print(f"{'=' * 78}{RESET}\n")

    has_relstorage = any("RelStorage" in v for v in results.values())

    if has_relstorage:
        print(
            f"  {'Objects':<12} {'PGJsonb':>20} {'RelStorage':>20} {'Comparison':>20}"
        )
        print(f"  {'-' * 74}")
    else:
        print(f"  {'Objects':<12} {'PGJsonb':>20}")
        print(f"  {'-' * 34}")

    for n_objects, backend_results in results.items():
        pj_stats = backend_results.get("PGJsonbStorage")
        rs_stats = backend_results.get("RelStorage")

        pj_str = (
            f"{_fmt_ms(pj_stats.mean)} +/- {_fmt_ms(pj_stats.stddev)}"
            if pj_stats is not None
            else "N/A"
        )

        if has_relstorage:
            rs_str = (
                f"{_fmt_ms(rs_stats.mean)} +/- {_fmt_ms(rs_stats.stddev)}"
                if rs_stats is not None
                else "N/A"
            )
            cmp_str = (
                _comparison(pj_stats.mean, rs_stats.mean)
                if pj_stats is not None and rs_stats is not None
                else ""
            )
            print(f"  {n_objects:<12} {pj_str:>20} {rs_str:>20} {cmp_str:>20}")
        else:
            print(f"  {n_objects:<12} {pj_str:>20}")

    print()


def print_plone_results(results: dict, n_docs: int):
    print(f"\n{HEADER}{'=' * 78}")
    print(f" Plone Operations ({n_docs} documents)")
    print(f"{'=' * 78}{RESET}\n")

    has_relstorage = "RelStorage" in results

    if has_relstorage:
        print(
            f"  {'Operation':<24} {'PGJsonb':>12} {'RelStorage':>12} {'Comparison':>20}"
        )
        print(f"  {'-' * 70}")
    else:
        print(f"  {'Operation':<24} {'PGJsonb':>12}")
        print(f"  {'-' * 38}")

    # Operations to display: (label, json_key, stat_key_or_None)
    ops = [
        ("site creation", "site_creation_ms", None),
        ("content create/doc", "content_creation", "mean_ms"),
        ("catalog query", "catalog_query", "mean_ms"),
        ("content modify/doc", "content_modification", "mean_ms"),
    ]

    for label, key, stat_key in ops:
        pj_data = results.get("PGJsonbStorage", {})
        rs_data = results.get("RelStorage", {})

        if stat_key:
            pj_val = pj_data.get(key, {}).get(stat_key)
            rs_val = rs_data.get(key, {}).get(stat_key) if rs_data else None
        else:
            pj_val = pj_data.get(key)
            rs_val = rs_data.get(key) if rs_data else None

        pj_str = _fmt_ms(pj_val) if pj_val is not None else "N/A"

        if has_relstorage:
            rs_str = _fmt_ms(rs_val) if rs_val is not None else "N/A"
            cmp_str = (
                _comparison(pj_val, rs_val)
                if pj_val is not None and rs_val is not None
                else ""
            )
            print(f"  {label:<24} {pj_str:>12} {rs_str:>12} {cmp_str:>20}")
        else:
            print(f"  {label:<24} {pj_str:>12}")

    print()


# ---------------------------------------------------------------------------
# JSON export
# ---------------------------------------------------------------------------


def results_to_json(
    storage_results: dict | None,
    zodb_results: dict | None,
    pack_results: dict | None,
    plone_results: dict | None,
    iterations: int,
    warmup: int,
) -> dict:
    out: dict = {
        "timestamp": datetime.now(UTC).isoformat(),
        "python_version": sys.version,
        "iterations": iterations,
        "warmup": warmup,
        "config": {
            "pgjsonb": {
                "cache_type": "OrderedDict LRU (pure Python)",
                "cache_default_mb": 16,
                "cache_scope": "per-instance (not shared)",
                "transcode": "zodb-json-codec (Rust/PyO3)",
                "storage_format": "JSONB",
            },
            "relstorage": {
                "cache_type": "generational LRU (Cython)",
                "cache_default_mb": 10,
                "cache_scope": "shared across instances",
                "transcode": "none (raw pickle bytes)",
                "storage_format": "bytea",
            },
        },
    }

    try:
        import zodb_pgjsonb

        out["pgjsonb_version"] = getattr(zodb_pgjsonb, "__version__", "dev")
    except Exception:
        pass

    try:
        import relstorage

        out["relstorage_version"] = getattr(relstorage, "__version__", "unknown")
    except ImportError:
        pass

    if storage_results:
        out["storage"] = {}
        for bench_name, backend_results in storage_results.items():
            out["storage"][bench_name] = {
                name: stats.to_dict() for name, stats in backend_results.items()
            }

    if zodb_results:
        out["zodb"] = {}
        for bench_name, backend_results in zodb_results.items():
            out["zodb"][bench_name] = {
                name: stats.to_dict() for name, stats in backend_results.items()
            }

    if pack_results:
        out["pack"] = {}
        for n_objects, backend_results in pack_results.items():
            out["pack"][str(n_objects)] = {
                name: stats.to_dict() for name, stats in backend_results.items()
            }

    if plone_results:
        out["plone"] = plone_results

    return out


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark PGJsonbStorage vs RelStorage"
    )
    sub = parser.add_subparsers(dest="command")

    # storage
    st = sub.add_parser("storage", help="Storage API benchmarks")
    st.add_argument("--iterations", type=int, default=100)
    st.add_argument("--warmup", type=int, default=10)

    # zodb
    zd = sub.add_parser("zodb", help="ZODB.DB benchmarks")
    zd.add_argument("--iterations", type=int, default=100)
    zd.add_argument("--warmup", type=int, default=10)

    # pack
    sub.add_parser("pack", help="Pack/GC benchmarks")

    # plone
    pl = sub.add_parser("plone", help="Plone application benchmarks")
    pl.add_argument("--docs", type=int, default=50, help="Number of documents")

    # all
    al = sub.add_parser("all", help="Run all benchmarks")
    al.add_argument("--iterations", type=int, default=100)
    al.add_argument("--warmup", type=int, default=10)
    al.add_argument("--docs", type=int, default=50, help="Number of documents (plone)")

    for p in [st, zd, al, pl]:
        p.add_argument("--output", help="Write JSON results to file")
        p.add_argument(
            "--format",
            choices=["table", "json", "both"],
            default="table",
            dest="fmt",
        )

    # pack also gets output/format
    pack_parser = sub.choices["pack"]
    pack_parser.add_argument("--output", help="Write JSON results to file")
    pack_parser.add_argument(
        "--format",
        choices=["table", "json", "both"],
        default="table",
        dest="fmt",
    )

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(1)

    iterations = getattr(args, "iterations", 100)
    warmup = getattr(args, "warmup", 10)
    n_docs = getattr(args, "docs", 50)

    print(f"\n{HEADER}PGJsonbStorage vs RelStorage — Performance Benchmarks{RESET}")

    # Check RelStorage availability
    try:
        import psycopg2  # noqa: F401
        import relstorage  # noqa: F401

        print(f"  {DIM}RelStorage available (comparison enabled){RESET}")
    except ImportError:
        print(f"  {DIM}RelStorage not available (PGJsonbStorage-only){RESET}")

    storage_results = None
    zodb_results = None
    pack_results = None
    plone_results = None

    if args.command in ("storage", "all"):
        print(f"\n{HEADER}Running storage benchmarks...{RESET}")
        storage_results = run_storage_benchmarks(iterations, warmup)

    if args.command in ("zodb", "all"):
        print(f"\n{HEADER}Running ZODB benchmarks...{RESET}")
        zodb_results = run_zodb_benchmarks(iterations, warmup)

    if args.command in ("pack", "all"):
        print(f"\n{HEADER}Running pack benchmarks...{RESET}")
        pack_results = run_pack_benchmarks()

    if args.command in ("plone", "all"):
        print(f"\n{HEADER}Running Plone benchmarks...{RESET}")
        plone_results = run_plone_benchmarks(n_docs)

    # Output
    fmt = getattr(args, "fmt", "table")
    if fmt in ("table", "both"):
        if storage_results:
            print_storage_results(storage_results, iterations, warmup)
        if zodb_results:
            print_zodb_results(zodb_results, iterations, warmup)
        if pack_results:
            print_pack_results(pack_results)
        if plone_results:
            print_plone_results(plone_results, n_docs)

    json_data = results_to_json(
        storage_results,
        zodb_results,
        pack_results,
        plone_results,
        iterations,
        warmup,
    )

    if fmt in ("json", "both"):
        print(json.dumps(json_data, indent=2))

    output = getattr(args, "output", None)
    if output:
        Path(output).write_text(json.dumps(json_data, indent=2))
        print(f"\nResults written to {output}")


if __name__ == "__main__":
    main()
