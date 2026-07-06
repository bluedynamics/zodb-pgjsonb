<!-- diataxis: explanation -->

(cache-tiers)=

# The cache tiers

zodb-pgjsonb serves object reads through several layers of cache before it ever touches PostgreSQL.
Understanding those layers explains why the same request can be sub-second when the caches are warm and several seconds when they are cold, and it tells you which knob to reach for.

This page is about the read caches.
For the exact configuration keys and defaults, see {doc}`/reference/configuration`.
For where the caches sit in the overall read path, see {doc}`/explanation/architecture`.

## The layers, top to bottom

A read that misses every cache travels the full stack.

1. **ZODB object cache** — ZODB's own in-memory cache of *live* (un-ghosted) persistent objects, one per ZODB connection, sized by the `cache-size` object-count setting.
   When an object is active here, no `setstate` happens at all and none of the layers below are consulted.
   This handles the large majority of reads in a warm process.

2. **L1: the per-connection load cache** — once an object is ghosted out of the ZODB object cache, the next access calls `setstate`, which calls the storage's `load()`.
   Each storage instance (one per ZODB connection) keeps its own `LoadCache`: an LRU of already-transcoded pickle bytes keyed by object id, sized by `cache-per-connection-mb` (default 16 MB).
   It is a lock-free hot path with no cross-thread coordination, so it is cheap — but it is duplicated per connection and cold on a fresh one.

3. **L2: the process-wide shared cache** — behind L1 sits a single `SharedLoadCache` shared by every connection in the process, sized by `cache-shared-mb` (default 256 MB).
   It exists so that N connections do not each keep their own copy of the same hot objects; before it was introduced a busy pod could waste roughly a gigabyte on duplicated per-connection caches.
   An L2 hit still costs a lock acquisition and an L1 fill, but it avoids a PostgreSQL round-trip and the pickle-to-JSON transcode.

4. **PostgreSQL** — a full miss runs a single-row `SELECT` on `object_state`, transcodes the JSONB back to pickle bytes, and populates L2, L1, and the serial cache on the way out.

The transcode cost is paid only on this last path; every cache hit returns pickle bytes directly.

(shared-cache-consensus-gate)=

## Why the shared cache is gated by a consensus TID

An MVCC storage hands each connection a point-in-time snapshot.
Two connections can therefore legitimately disagree about the current state of an object.
A process-wide cache shared between them is dangerous: a connection holding an old snapshot could write a stale object into the cache *after* a newer commit has already superseded it, and a third connection would then read the stale bytes.

The `SharedLoadCache` prevents that with a single process-wide consensus TID — the highest transaction id any connection in the process has polled to.
Every `poll_invalidations` advances it and drops the object ids that changed.
L2 reads and writes are gated on it: a connection whose snapshot is older than the consensus may neither read from nor write to L2, and its loads fall through to PostgreSQL instead.
This keeps the shared cache correct without per-object locking or versioning.

The gate has an observable cost.
During a burst of writes — a bulk `reindexObject` run, a publishing wave — the consensus advances on nearly every request, so a reader that opened its snapshot a moment earlier is "behind" and sees L2 as empty for the rest of that transaction.
Its object loads go to PostgreSQL one at a time.
This is the main reason an otherwise warm pod can serve a slow, cold-looking request in the middle of heavy write activity, even though nothing is wrong.

## The cache warmer

A freshly started pod has an empty L2 and empty L1s.
The first requests to touch a given working set pay the full PostgreSQL path for every object, so on a rolling deploy across several replicas you see a burst of slow requests until the caches fill.

The cache warmer mitigates this by priming L2 on startup.
It reads the most-accessed object ids and loads them in paced batches before the pod takes much traffic.
Because every replica would otherwise warm at once and multiply the database's startup load by the replica count, the warmer is deliberately un-synchronized: a baseline delay (`cache-warm-delay`), random jitter (`cache-warm-jitter`), a cluster-wide concurrency cap enforced by an advisory-lock semaphore (`cache-warm-concurrency`), and batch pacing (`cache-warm-batch-size`, `cache-warm-batch-pause`) spread the work out.
The trade-off is that a pod's L2 is warm roughly 15–45 seconds after startup rather than instantly.
It serves traffic from the first second, but the first requests for a cold working set still pay the PostgreSQL path.

## Observing cache effectiveness

When zodb-pgjsonb runs under [plone.observability](https://github.com/plone/plone.observability), each span carries three ZODB attributes that together show how a request's object loads were served.

`plone.zodb.objects_loaded`
:   Objects that reached the storage — that is, missed the ZODB object cache.

`plone.zodb.load_l2_hits`
:   Of those, how many the shared cache (L2) served.

`plone.zodb.load_pg_queries`
:   How many fell through to PostgreSQL.

The shared-cache hit ratio for a request is `load_l2_hits / (load_l2_hits + load_pg_queries)`.
Reading a slow span this way tells you which layer failed.

- `load_pg_queries` ≈ `objects_loaded` and `load_l2_hits` ≈ 0 — the caches were cold or the consensus gate was closed, and the request did a round-trip per object.
- `load_pg_queries` small but `load_time_ms` still high — few loads, but slow ones: high per-round-trip latency, or a handful of very large objects.

`plone.zodb.load_time_ms` is the wall time spent inside those `setstate` loads, so comparing it against `load_pg_queries` gives the effective per-load latency.

## What makes a request cold, and which knob helps

When cold requests dominate, three levers matter, in rough order of impact.

The sequential, one-object-at-a-time load pattern is the amplifier.
A page that wakes 150 ghosts issues 150 round-trips, so anything that raises per-round-trip latency — a connection pooler, a cross-zone network hop — is multiplied by the object count.
Loading a working set in a single batched query, which `load_multiple` and plone-pgcatalog's reference prefetch do, collapses those round-trips, and it is the most robust fix because it helps even when the cache is cold.

Cache sizing keeps the warm state warm.
If the working set is larger than `cache-shared-mb`, L2 evicts hot objects and cold requests recur; raising it trades memory for fewer round-trips.

The warmer keeps fresh pods from starting cold, and tuning its pacing trades startup database load against time-to-warm.

None of these change the consensus gate.
Under sustained writes, readers still fall through to PostgreSQL, which is correct; the durable answer there is to make falling through cheap by reducing the per-object round-trip cost.

```{seealso}
{doc}`/explanation/architecture` for the read path and MVCC snapshots,
{doc}`/explanation/performance` for cached-versus-uncached benchmarks, and
{doc}`/reference/configuration` for the cache and warmer configuration keys.
```
