# Real PG query counter + honest observability names — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a per-round-trip `_pg_query_count` to zodb-pgjsonb (v1.16.0) and, in plone.observability (b18), rename the misnamed `plone.zodb.load_pg_queries` (objects) to `load_pg_objects` and add a real `load_pg_queries` (queries).

**Architecture:** Two coordinated repos. Part A adds a plain-int counter next to `_pg_load_count`, incremented at the same two sites (`load()` and `load_multiple()` PG branches). Part B reads it best-effort and exposes objects-vs-queries per span. Ship A first, then B.

**Tech Stack:** zodb-pgjsonb (`instance.py`, hatch-vcs tag releases, `pytest.mark.db`), plone.observability (`otel/dbcounts.py`, towncrier releases), pytest.

## Global Constraints

- `_pg_query_count`: plain int per instance, incremented **only** where `_pg_load_count` is (verified: `load()` PG-miss and `load_multiple()` PG-batch — nowhere else; `loadBefore` counts neither). In `load_multiple()` it is `+= 1` per batch query (one `WHERE zoid = ANY(%s)`), **not** `len(rows)`.
- observability attribute rename: `plone.zodb.load_pg_queries` → `plone.zodb.load_pg_objects` (reads `_pg_load_count`); NEW `plone.zodb.load_pg_queries` reads `_pg_query_count` (best-effort `getattr(..., 0)`). `load_l2_hits` unchanged.
- `read_counts` → 6-tuple `(loads, stores, load_time_ns, l2_hits, pg_objects, pg_queries)`; `annotate` sets all six. No call-site changes (they pass `read_counts` straight to `annotate`).
- Ordering: zodb-pgjsonb **v1.16.0** first, then plone.observability **b18**.
- Commit footer exactly: `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>`
- Tests: zodb-pgjsonb `.venv/bin/pytest tests/test_load_counters.py -v` (needs the test DB, `pytest.mark.db`); plone.observability `.venv/bin/pytest tests/test_otel_dbcounts.py -v`. Before committing in either repo run `uvx pre-commit run --all-files` where a pre-commit config exists.

## Verified facts

- `zodb_pgjsonb/instance.py`: counters init ≈ line 64-65 (`_l2_load_hits`, `_pg_load_count`); `load()` PG branch has `self._pg_load_count += 1` (≈ line 330); `load_multiple()` PG branch has `self._pg_load_count += len(rows)` (≈ line 421). `load_multiple` uses a single `WHERE zoid = ANY(%s)` (not chunked).
- `tests/test_load_counters.py` pattern: fixtures `db`, `storage`; `storage.new_instance()`; `inst.poll_invalidations()`; `inst.load(oid)` / `inst.load_multiple([...])`; assert `inst._pg_load_count` / `._l2_load_hits`; `inst.release()`; `pytestmark = pytest.mark.db`; helper `_store_object(db)` commits one `PersistentMapping` and returns its oid.
- zodb-pgjsonb CHANGES.md has an open `## unreleased` section; releases are tag-based (RELEASE.md): land under `## unreleased`, release PR promotes it to the version, tag `vX.Y.Z` → GH release → PyPI.
- plone.observability `otel/dbcounts.py` (b17) reads counters from `conn._storage`: `l2_hits = getattr(instance, "_l2_load_hits", 0)`, `pg_queries = getattr(instance, "_pg_load_count", 0)`; `read_counts` returns a 5-tuple; `annotate` sets `_L2_HITS_ATTR`/`_PG_QUERIES_ATTR` at indices [3]/[4]. Test fakes: `_Storage(l2, pg)` with `_l2_load_hits`/`_pg_load_count`; `_Conn(..., storage=None)` sets `self._storage`.

---

### Task 1 (zodb-pgjsonb): add `_pg_query_count`

**Files:**
- Modify: `src/zodb_pgjsonb/instance.py`
- Modify: `CHANGES.md`, `docs/sources/explanation/performance.md`
- Test: `tests/test_load_counters.py`

**Interfaces:**
- Produces: `instance._pg_query_count` (int), incremented per PG round-trip.

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_load_counters.py`:

```python
def _store_objects(db, n):
    """Commit n PersistentMappings and return their oids."""
    conn = db.open()
    try:
        root = conn.root()
        objs = []
        for i in range(n):
            obj = PersistentMapping({"i": i})
            root[f"obj{i}"] = obj
            objs.append(obj)
        transaction.commit()
        return [o._p_oid for o in objs]
    finally:
        conn.close()


def test_query_counter_single_load(db, storage):
    oid = _store_object(db)
    inst = storage.new_instance()
    try:
        inst.poll_invalidations()
        inst.load(oid)
        assert inst._pg_load_count == 1
        assert inst._pg_query_count == 1  # one object, one round-trip
        inst.load(oid)  # now L1-cached
        assert inst._pg_query_count == 1
    finally:
        inst.release()


def test_query_counter_batch_is_one_per_query(db, storage):
    oids = _store_objects(db, 5)
    inst = storage.new_instance()
    try:
        inst.poll_invalidations()
        inst.load_multiple(oids)
        assert inst._pg_load_count == 5  # five objects
        assert inst._pg_query_count == 1  # one ANY() query
    finally:
        inst.release()
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `.venv/bin/pytest tests/test_load_counters.py::test_query_counter_single_load tests/test_load_counters.py::test_query_counter_batch_is_one_per_query -v`
Expected: FAIL — `AttributeError: ... has no attribute '_pg_query_count'`.

- [ ] **Step 3: Add the counter**

In `src/zodb_pgjsonb/instance.py`, init it next to the others:

```python
        self._l2_load_hits = 0  # objects served from the shared (L2) cache
        self._pg_load_count = 0  # objects fetched from PostgreSQL
        self._pg_query_count = 0  # PostgreSQL round-trips (queries)
```

In `load()`, right after `self._pg_load_count += 1`:

```python
        self._pg_load_count += 1
        self._pg_query_count += 1
```

In `load_multiple()`, right after `self._pg_load_count += len(rows)`:

```python
        self._pg_load_count += len(rows)
        self._pg_query_count += 1
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `.venv/bin/pytest tests/test_load_counters.py -v`
Expected: PASS (existing counter tests + the two new ones).

- [ ] **Step 5: CHANGES + performance.md**

In `CHANGES.md` under `## unreleased`, add (create the `### Features` group if absent, order it above `### Documentation`):

```markdown
### Features

- Add a per-connection `_pg_query_count` counter (PostgreSQL round-trips) next to the
  existing `_pg_load_count` (objects). `load()` bumps both by 1; `load_multiple()` bumps
  `_pg_load_count` by the batch size but `_pg_query_count` by **1** (one `WHERE zoid = ANY(...)`
  query), so `_pg_load_count / _pg_query_count` quantifies batching/prefetch. Plain int, no
  dependency; read best-effort by plone.observability. #96
```

In `docs/sources/explanation/performance.md`, after the ZODB `prefetch` hook paragraph (the
"1.15.0" discussion of turning a result set's N+1 into one `ANY(...)` query), add:

```markdown
### Load counters for observability

Each connection instance exposes plain-int counters (read best-effort by tracing tools such as
plone.observability, with no dependency added here):

- `_l2_load_hits` — objects served from the shared (L2) cache.
- `_pg_load_count` — objects fetched from PostgreSQL.
- `_pg_query_count` — PostgreSQL round-trips (queries).

`_pg_load_count` counts *objects*; `_pg_query_count` counts *queries*. A single
`load_multiple()` / `prefetch` that fetches N objects in one `WHERE zoid = ANY(...)` bumps
`_pg_load_count` by N but `_pg_query_count` by 1 — so their ratio makes the batching/prefetch
win directly visible, where a per-object N+1 pattern keeps the ratio near 1.
```

- [ ] **Step 6: Lint + commit**

```bash
uvx pre-commit run --all-files
git add src/zodb_pgjsonb/instance.py tests/test_load_counters.py CHANGES.md docs/sources/explanation/performance.md
git commit -m "feat: per-connection _pg_query_count (PG round-trips) (#96)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```
(Check the commit's own exit code.)

---

### Task 2 (zodb-pgjsonb): release v1.16.0

**Files:** `CHANGES.md`

Per `RELEASE.md` (tag-based, hatch-vcs):

- [ ] **Step 1: Release branch + promote CHANGES**

On a fresh branch off `main` (Task 1 already merged to `main` via PR first — open Task 1 as its own PR, green CI, merge), promote the `## unreleased` heading to `## 1.16.0`:

```bash
git checkout main && git pull --ff-only
git checkout -b release/1.16.0
# edit CHANGES.md: `## unreleased` -> `## 1.16.0`
git commit -am "docs: finalize CHANGES for 1.16.0 release

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
git push -u origin release/1.16.0
gh pr create -R bluedynamics/zodb-pgjsonb --base main --title "Release 1.16.0" --fill
```

- [ ] **Step 2: Merge on green CI, tag, release**

```bash
gh pr merge <PR#> -R bluedynamics/zodb-pgjsonb --squash --delete-branch
git checkout main && git pull --ff-only
git tag v1.16.0 && git push origin v1.16.0
gh release create v1.16.0 -R bluedynamics/zodb-pgjsonb --title "v1.16.0" --notes "See CHANGES.md 1.16.0."
```

- [ ] **Step 3: Verify PyPI**

```bash
gh run watch <release-run-id> -R bluedynamics/zodb-pgjsonb --exit-status
curl -s -o /dev/null -w '%{http_code}\n' https://pypi.org/pypi/zodb-pgjsonb/1.16.0/json   # expect 200
```

---

### Task 3 (plone.observability): honest attribute names

**Files:**
- Modify: `src/plone/observability/otel/dbcounts.py`
- Test: `tests/test_otel_dbcounts.py`
- Modify: the tracing reference doc listing the attributes (`docs/sources/reference/tracing.md` if the attribute list lives there).

**Interfaces:**
- Consumes: `_pg_query_count` on `conn._storage` (Task 1; best-effort).
- Produces: `read_counts -> (loads, stores, load_time_ns, l2_hits, pg_objects, pg_queries)`; `annotate` sets `plone.zodb.load_pg_objects` and `plone.zodb.load_pg_queries`.

- [ ] **Step 1: Update the tests**

In `tests/test_otel_dbcounts.py`, extend the `_Storage` fake with the query counter:

```python
class _Storage:
    """Fake per-connection storage exposing zodb-pgjsonb load counters."""

    def __init__(self, l2=0, pg=0, pg_queries=0):
        self._l2_load_hits = l2
        self._pg_load_count = pg
        self._pg_query_count = pg_queries
```

Update `test_read_counts_peeks_without_reset` to the 6-tuple (find its current assertion and extend it) — e.g. a connection with `storage=_Storage(l2=2, pg=10, pg_queries=3)` and `load_time_ns` set asserts `read_counts(...) == (loads, stores, load_time_ns, 2, 10, 3)`.

Update `test_annotate_sets_delta_including_zero` to 6-tuples and assert both new attributes:

```python
    dbcounts.annotate(span, (1, 1, 1_000_000, 2, 10, 5), (4, 1, 6_000_000, 9, 11, 6))
    assert span.attrs["plone.zodb.objects_loaded"] == 3
    assert span.attrs["plone.zodb.objects_stored"] == 0
    assert span.attrs["plone.zodb.load_time_ms"] == 5.0
    assert span.attrs["plone.zodb.load_l2_hits"] == 7
    assert span.attrs["plone.zodb.load_pg_objects"] == 1
    assert span.attrs["plone.zodb.load_pg_queries"] == 1
```

Update the subrequest integration test that currently asserts `load_pg_queries == 11`: the fake advances `storage._pg_load_count += 11`, so that becomes `load_pg_objects == 11`; also advance `storage._pg_query_count += 2` and assert `load_pg_queries == 2`:

```python
def fake(url, **kw):
    conn.loads += 151
    storage._l2_load_hits += 140
    storage._pg_load_count += 11
    storage._pg_query_count += 2
    return _Resp(200)


...
assert span.attributes["plone.zodb.load_l2_hits"] == 140
assert span.attributes["plone.zodb.load_pg_objects"] == 11
assert span.attributes["plone.zodb.load_pg_queries"] == 2
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `.venv/bin/pytest tests/test_otel_dbcounts.py -q`
Expected: FAIL — `read_counts` returns a 5-tuple / `load_pg_objects` attribute missing / `load_pg_queries` still reads `_pg_load_count`.

- [ ] **Step 3: Update `dbcounts.py`**

Replace the attribute constants:

```python
_L2_HITS_ATTR = "plone.zodb.load_l2_hits"
_PG_OBJECTS_ATTR = "plone.zodb.load_pg_objects"
_PG_QUERIES_ATTR = "plone.zodb.load_pg_queries"
```

Update `read_counts` to return the 6-tuple:

```python
    instance = getattr(conn, "_storage", None)
    l2_hits = getattr(instance, "_l2_load_hits", 0)
    pg_objects = getattr(instance, "_pg_load_count", 0)
    pg_queries = getattr(instance, "_pg_query_count", 0)
    return (
        loads,
        stores,
        getattr(conn, "_otel_load_time_ns", 0),
        l2_hits,
        pg_objects,
        pg_queries,
    )
```

Update the `read_counts` docstring tuple to `(loads, stores, load_time_ns, l2_hits, pg_objects, pg_queries)`.

Update `annotate` to set both:

```python
    span.set_attribute(_L2_HITS_ATTR, after[3] - before[3])
    span.set_attribute(_PG_OBJECTS_ATTR, after[4] - before[4])
    span.set_attribute(_PG_QUERIES_ATTR, after[5] - before[5])
```

Also update the comment block above the attr constants to describe the trio (l2 hits / pg objects / pg queries) and the objects-per-round-trip ratio.

- [ ] **Step 4: Run tests to verify they pass**

Run: `.venv/bin/pytest tests/test_otel_dbcounts.py -v`
Expected: PASS.

- [ ] **Step 5: Reference doc + full suite + lint**

If `docs/sources/reference/tracing.md` lists the per-span ZODB attributes, update it: rename `load_pg_queries`'s description to `load_pg_objects` (objects fetched from PG) and add `load_pg_queries` (PG round-trips), noting `load_pg_objects / load_pg_queries` ≈ objects per round-trip. (Grep the docs for `load_pg_queries` / `load_l2_hits` to find the exact spot; if none, skip.)

Run: `.venv/bin/pytest -q` → all pass.
Run: `uvx pre-commit run --all-files` → all hooks pass.

- [ ] **Step 6: Commit**

```bash
git add src/plone/observability/otel/dbcounts.py tests/test_otel_dbcounts.py docs/
git commit -m "feat(otel): rename load_pg_queries->load_pg_objects, add real load_pg_queries (#96)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4 (plone.observability): release b18

Per the plone.observability `RELEASE.md`:

- [ ] **Step 1:** Task 3 merged to `main` via its own PR (green CI). Then on `main`: `git checkout -b release-1.0.0b18`; bump `pyproject.toml` `version = "1.0.0b18"`; `uvx towncrier build --yes --version 1.0.0b18`; sanity-check `CHANGES.md` (no duplicate issue links — the news fragment for Task 3 should NOT embed a bare `#96`, towncrier appends the link from `<issue>.feature`). Commit with `git add -A`.
- [ ] **Step 2:** Push, open "Release 1.0.0b18" PR, green CI, squash-merge, sync `main`, confirm `version = "1.0.0b18"`.
- [ ] **Step 3:** `gh release create 1.0.0b18 --title "1.0.0b18" --notes-file <b18 CHANGES section>` → watch release workflow → verify `https://pypi.org/pypi/plone.observability/1.0.0b18/json` is 200.

> Task 3's news fragment: create `news/96.feature` in the observability repo during Task 3 (so towncrier consumes it here). Content: "Rename the misnamed ``plone.zodb.load_pg_queries`` span attribute (it counted objects) to ``plone.zodb.load_pg_objects``, and add a real ``plone.zodb.load_pg_queries`` reading zodb-pgjsonb's new ``_pg_query_count`` (PG round-trips). Their ratio makes batching/prefetch visible." (No trailing bare ``#96``.)

---

## Self-Review

**Spec coverage:**
- `_pg_query_count` at the two `_pg_load_count` sites, `+= 1` per batch → Task 1 Step 3 + `test_query_counter_batch_is_one_per_query`. ✓
- Counter docs (comment + performance.md) → Task 1 Step 5. ✓
- v1.16.0 release → Task 2. ✓
- Rename `load_pg_queries`→`load_pg_objects` + new real `load_pg_queries` (best-effort) → Task 3 Step 3 + tests. ✓
- 6-tuple `read_counts`, `annotate` sets all six, no call-site changes → Task 3. ✓
- Reference doc → Task 3 Step 5. ✓
- b18 release + fragment without duplicate link → Task 4. ✓
- Ordering A→B → Tasks 1-2 before 3-4. ✓

**Placeholder scan:** none (the "if the attribute list lives there" doc step is a conditional lookup with a concrete grep, not a TODO).

**Type consistency:** `_pg_query_count` (int), `read_counts -> 6-tuple`, `annotate` indices [3]/[4]/[5] = l2_hits/pg_objects/pg_queries, attribute constants `_L2_HITS_ATTR`/`_PG_OBJECTS_ATTR`/`_PG_QUERIES_ATTR` — consistent across tasks and the fakes.
