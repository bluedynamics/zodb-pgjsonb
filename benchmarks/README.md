# Benchmarks

Reproducible performance benchmarks for zodb-pgjsonb, and the source of the
numbers in the [Performance characteristics](https://bluedynamics.github.io/zodb-pgjsonb/explanation/performance.html)
documentation.

Two harnesses live here:

- **`bench.py`** — the full suite: `PGJsonbStorage` versus **RelStorage** across
  storage-API, ZODB.DB, pack/GC, history-preserving, and Plone workloads.
- **`bench_prefetch.py`** — a focused micro-benchmark that shows the N+1 collapse
  from the ZODB `prefetch` hook (sequential per-object loads versus one batched
  prefetch).

> [!IMPORTANT]
> **Absolute numbers are machine-specific.** A run on your laptop and a run on a
> production-class server will differ by a large factor. The portable signal is
> the **PGJsonb-versus-RelStorage ratio** on the *same* machine. When you update
> the documentation, always record the environment (see below) alongside the
> numbers.

## Prerequisites

1. **PostgreSQL on `localhost:5433`.** The project's dev container works:

   ```shell
   docker run -d --name zodb-pgjsonb-dev \
     -e POSTGRES_USER=zodb -e POSTGRES_PASSWORD=zodb -e POSTGRES_DB=zodb \
     -p 5433:5432 postgres:17
   ```

2. **Two benchmark databases** (created once; the harness reuses and cleans them):

   ```shell
   uv run python -c "import psycopg; c=psycopg.connect('host=localhost port=5433 user=zodb password=zodb dbname=postgres', autocommit=True); [c.execute(f'CREATE DATABASE {d}') for d in ('zodb_bench_pgjsonb','zodb_bench_relstorage') if not c.execute('SELECT 1 FROM pg_database WHERE datname=%s',(d,)).fetchone()]"
   ```

3. **RelStorage** (the comparison baseline). If it is not importable, the suite
   still runs and reports PGJsonb-only numbers.

   ```shell
   uv pip install relstorage
   ```

4. **Plone** (only for the `plone` subset). The storage/zodb/pack/history subsets
   are self-contained (`ZODB.tests.MinPO`) and need no Plone. The `plone` subset
   needs `Products.CMFPlone` importable in the environment.

## Running

```shell
# individual subsets
uv run python benchmarks/bench.py storage  --iterations 100 --warmup 10
uv run python benchmarks/bench.py zodb     --iterations 100 --warmup 10
uv run python benchmarks/bench.py history  --iterations 100 --warmup 10
uv run python benchmarks/bench.py pack
uv run python benchmarks/bench.py plone    --docs 100        # needs Plone

# everything
uv run python benchmarks/bench.py all --iterations 100
```

Flags:

- `--iterations N` / `--warmup N` — measured and warmup iterations (default 100 / 10).
- `--format {table,json,both}` — console table, machine-readable JSON, or both.
- `--output FILE` — write the JSON results to `FILE`.

Each figure is the median of `N` iterations after `warmup` discarded runs. The
harness runs both storages back-to-back against the same PostgreSQL server so the
comparison ratio cancels out most machine noise.

### Prefetch micro-benchmark

```shell
uv run python benchmarks/bench_prefetch.py                       # real local latency
BENCH_LATENCY_MS=20 uv run python benchmarks/bench_prefetch.py   # simulate prod RTT
BENCH_N=500 uv run python benchmarks/bench_prefetch.py           # larger result set
```

It counts `object_state` round-trips (RTT-independent) and wall time for a
sequential per-object load versus one `prefetch`, with an optional injected
per-query latency to show how the two scale with network round-trip time.

## Updating the documentation

The numbers in `docs/sources/explanation/performance.md` are produced by this
suite. To refresh them:

1. Run on a defined machine, ideally the same one each time so the series is
   comparable:

   ```shell
   uv run python benchmarks/bench.py all --iterations 100 --output results.json
   ```

2. Transcribe the numbers into `docs/sources/explanation/performance.md`.

3. **Update the "Benchmark environment" block** in that page to match the run —
   CPU, OS, PostgreSQL version, and the versions of `relstorage`,
   `zodb-json-codec`, and Python. Numbers without their environment are not
   meaningful.

Capture the environment with:

```shell
uv run python -c "import sys, platform, importlib.metadata as m; print('Python', sys.version.split()[0]); print('OS', platform.platform()); print('CPU', platform.processor() or platform.machine()); [print(p, m.version(p)) for p in ('relstorage','zodb-json-codec','ZODB','psycopg')]"
```
