# Real PG query counter + honest observability attribute names

**Date:** 2026-07-02
**Issue:** [zodb-pgjsonb#96](https://github.com/bluedynamics/zodb-pgjsonb/issues/96)
**Status:** Approved (design)
**Spans two repos:** zodb-pgjsonb (Part A) and plone.observability (Part B).

## Problem

`_pg_load_count` counts **objects** fetched from PostgreSQL, not queries: `load()` `+= 1`
(one object) and `load_multiple()` `+= len(rows)` (whole batch). So batching/prefetch is
invisible to consumers. plone.observability b17 exposes this counter per span as
`plone.zodb.load_pg_queries` — a name that implies *query* semantics over an *object* counter.
A request that loads 44 objects via 44 sequential `load()` calls and one that loads them via a
single `WHERE zoid = ANY(...)` (`Connection.prefetch`, #94) both report `load_pg_queries = 44`.

## Goal

1. Add a real per-query counter in zodb-pgjsonb (`_pg_query_count`) — round-trips, not objects.
2. In plone.observability, rename the misnamed attribute to honest object semantics and add a
   new attribute with true query semantics. Together `load_pg_objects` vs `load_pg_queries`
   makes N+1 (ratio ≈ 1) vs batched loads (ratio ≫ 1) directly visible, and makes `prefetch`
   adoption verifiable without wall-time A/B.

---

## Part A — zodb-pgjsonb: `_pg_query_count`

A plain-int per-instance counter alongside `_l2_load_hits` / `_pg_load_count`, incremented at
**exactly the two sites that already increment `_pg_load_count`** (verified — nowhere else;
`loadBefore` increments neither):

- `instance.py` init (next to the others): `self._pg_query_count = 0  # PG round-trips (queries)`.
- `load()` PG-miss branch (right after `self._pg_load_count += 1`): `self._pg_query_count += 1`
  — one single-object `SELECT` is one round-trip.
- `load_multiple()` PG-batch branch (right after `self._pg_load_count += len(rows)`):
  `self._pg_query_count += 1` — **one** `WHERE zoid = ANY(%s)` query for the whole batch,
  independent of `len(rows)` (not chunked). So `_pg_load_count / _pg_query_count` = objects per
  round-trip.

`load()`'s refs-prefetch calls `load_multiple(...)` recursively, whose own `+= 1` then counts
that additional query — correct (two round-trips = 2).

**Semantics/lifecycle:** same as the existing counters — per storage instance, dependency-neutral
plain int, reset never (consumers read deltas), documented for best-effort external `getattr`.

**Docs:** extend `docs/sources/explanation/performance.md` — note that the storage exposes both
an object counter and a round-trip counter, and that their ratio quantifies batching/prefetch.

**Tests:** `load()` PG miss bumps both counters by 1; a repeat `load()` (now L1-cached) bumps
neither; `load_multiple()` of N PG-missing oids bumps `_pg_load_count` by N and `_pg_query_count`
by 1; an all-cached `load_multiple()` bumps neither. (Use the repo's existing storage/instance
test fixtures.)

**Release:** **v1.16.0** — land the change + a `CHANGES.md` `## unreleased` entry, then the
tag-based release flow (RELEASE.md): release PR → merge → promote CHANGES to `1.16.0` → tag
`v1.16.0` → GitHub release triggers the PyPI publish.

---

## Part B — plone.observability: honest attribute names (b18)

`otel/dbcounts.py` currently (b17) returns a 5-tuple and sets, from the connection instance:
`_L2_HITS_ATTR = "plone.zodb.load_l2_hits"` (← `_l2_load_hits`) and
`_PG_QUERIES_ATTR = "plone.zodb.load_pg_queries"` (← `_pg_load_count`, misnamed).

Change to:

- Rename the object attribute: `plone.zodb.load_pg_queries` → **`plone.zodb.load_pg_objects`**,
  still reading `_pg_load_count`.
- Add a new query attribute: **`plone.zodb.load_pg_queries`**, reading `_pg_query_count`
  best-effort (`getattr(instance, "_pg_query_count", 0)` → 0 on storages/older versions without
  it).
- `read_counts(request)` returns a **6-tuple**
  `(loads, stores, load_time_ns, l2_hits, pg_objects, pg_queries)` (or `None`).
- `annotate(span, before, after)` sets all six deltas: `objects_loaded`, `objects_stored`,
  `load_time_ms`, `load_l2_hits`, `load_pg_objects`, `load_pg_queries`.
- `load_l2_hits` keeps its name and meaning (objects served from the shared cache).

Every call site (pubevents/subrequest/rendering) passes `read_counts` straight to `annotate`, so
**no call-site changes**.

**Docs:** update the tracing reference (attribute list) to describe the trio: `load_l2_hits`
(shared-cache), `load_pg_objects` (objects fetched from PG), `load_pg_queries` (PG round-trips),
and note `load_pg_objects / load_pg_queries` ≈ objects-per-round-trip.

**Tests:** `read_counts` 6-tuple incl. `pg_objects`/`pg_queries` (fake connection carrying
`_pg_load_count` and `_pg_query_count`); `annotate` sets both `load_pg_objects` and
`load_pg_queries` from the deltas; existing span integration tests updated to the new names.

**Release:** **b18** — bump + towncrier feature fragment + PR + GitHub release → PyPI, per the
plone.observability RELEASE.md.

## Ordering & compatibility

Ship **A (v1.16.0) first** so `_pg_query_count` exists; then **B (b18)** surfaces it. B is
best-effort, so it is safe to ship independently — on a storage without the counter,
`load_pg_queries` is simply 0. The `load_pg_queries` → `load_pg_objects` rename is a breaking
attribute change, acceptable in the beta series (b17 is < 1 day old, near-zero adoption). After
both releases, aaf bumps zodb-pgjsonb ≥ 1.16.0 and plone.observability ≥ b18.

## Out of scope

- Chunking `load_multiple` (it is a single ANY query today; if that ever changes, `+= 1` per
  chunk is the natural extension).
- Counting `loadBefore` / historical loads (they do not increment `_pg_load_count` either).
