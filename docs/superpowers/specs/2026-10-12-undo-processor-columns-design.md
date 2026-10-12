# Undo and state-processor columns

**Date:** 2026-10-12
**Issues:** [zodb-pgjsonb#121](https://github.com/bluedynamics/zodb-pgjsonb/issues/121) (storage),
[plone-pgcatalog#265](https://github.com/bluedynamics/plone-pgcatalog/issues/265) (catalog)
**Status:** Accepted 2026-10-12 (option 3a, all decisions below settled)
**Spans two repos:** zodb-pgjsonb (Part A) and plone.pgcatalog (Part B)
**Depends on:** zodb-pgjsonb#120 / PR #123 (per-processor write semantics, released in 1.17.0)

Line references are to the `design/undo-121` worktree of zodb-pgjsonb and the
`main` checkout of plone-pgcatalog as of today.

## Problem

In history-preserving mode, undoing a transaction restores object state but
not the catalog columns plone.pgcatalog keeps on `object_state`. The restored
objects keep the catalog data of the undone transaction, and objects whose
columns the undone transaction changed in `finalize()` without writing them
(security partial reindex, bulk path move, gopip rank sync) are not touched at
all. The worst case is security: after undoing a sharing change, descendants
stay findable for the user whose access was revoked. ZCatalog cannot have this
problem because its BTrees are ordinary ZODB objects and undo reverts them.

## Verified facts the design rests on

Storage side:

- Undo only exists in HP mode (`storage.py:1501-1503`). `PGJsonbStorage`
  implements `IMVCCStorage` (`storage.py:373`), so `ZODB.DB` uses the storage
  itself as `_mvcc_storage` (`ZODB/DB.py:439-440`) and `TransactionalUndo`
  opens a fresh `new_instance()` for every undo transaction (`DB.py:1076-1082`;
  there is no `undo_instance`). **Production undo therefore runs through
  `PGJsonbStorageInstance.undo()` (`instance.py:759-804`)**, with its own PG
  connection and its own `tpc_begin/tpc_vote/tpc_finish`. The main storage's
  `undo()` (`storage.py:1561-1616`) is a second, duplicated implementation that
  is only hit when a caller drives the main storage directly (tests do). Both
  compute entries with `_compute_undo()` (`undo.py:145-193`), queue them into
  `_tmp`, and re-run `_process_state()` on the restored state (added by #43).
- `TransactionalUndo.commit()` calls `storage.undo(tid, txn)` once per tid, in
  the order given, and discards the returned oid list (`DB.py:1084-1087`).
  Invalidation of other connections does not depend on that list: the instance
  advances the shared cache with the zoids in `_tmp` (`instance.py:639-640`),
  and every connection polls `object_state` by tid.
- `undo()` is called after `tpc_begin`, which has already run `BEGIN` and
  `SELECT pg_advisory_xact_lock(0)` (`instance.py:561-562`,
  `storage.py:1230-1238`). So inside `undo()` the storage sits in the PG
  transaction that will carry the undo, serialized against every other writer,
  and nothing of the undo has been written yet: writes happen in `tpc_vote`
  (`instance.py:606-609`) or `_vote` (`storage.py:1277-1280`), and
  `finalize(cur)` runs right after them (`instance.py:626-628`,
  `storage.py:1297-1299`). `finalize` receives no tid.
- Entries queued by undo carry no `_serial`, so `_batch_resolve_conflicts`
  ignores them (`conflict.py:18`); conflict handling for undo happens entirely
  in `_undo_one_object()` (`undo.py:98-142`): untouched object → restore the
  previous revision or delete (created in the undone transaction); modified
  later with identical state → cascading undo; otherwise conflict resolution
  via `tryToResolveConflict`, `UndoError` on failure. Content classes have no
  `_p_resolveConflict`, BTree buckets and `Length` do.
- `"action": "delete"` entries exist only for undo (`undo.py:11-13`). In HP
  mode the row is removed from `object_state` and a tombstone goes to
  `object_history` (`batch.py:150-179`); its processor columns disappear with
  the row. A restored row is upserted with base columns only when no processor
  answers (`batch.py:123-147`), so existing processor columns stay and a
  re-inserted row (undo of an undo-of-add) gets the column defaults.
- `object_state.refs` has a GIN index (`schema.py:42-43`), so referrers of a
  zoid can be found cheaply. Pack deletes `transaction_log` rows at or before
  the pack time (`packer.py:199-204`).
- The transaction package calls after-commit hooks after the synchronizers'
  `afterCompletion` and before `_free()`, swallows and logs their exceptions,
  and aborts all resources afterwards (`transaction/_transaction.py:286-288,
  345-385`). `TransactionManager.free()` rejects a foreign transaction
  (`_manager.py:92-95`). Running a second ZODB transaction on the thread's
  default manager from inside an after-commit hook is therefore not a supported
  pattern, and plone.pgcatalog's pending store joins `transaction.get()`
  (`pending.py:316-327`), whose status would be `COMMITTED` at that point
  (`Transaction.join` raises).

plone.pgcatalog side:

- `CatalogStateProcessor.process()` pops thread-local pending data or the
  `_pgcatalog_pending` key from the state and returns `None` otherwise
  (`processor.py:155-172`). Stored states never carry the key (it is popped
  before the write), so during undo the processor always returns `None`, and
  since 1.17.0 the restored rows keep their current columns.
- `finalize(cursor)` changes columns of objects that were not written:
  partial `idx` merges per zoid guarded by `idx IS NOT NULL`
  (`processor.py:253-272`), bulk path moves by prefix (`processor.py:277-293`),
  gopip rank syncs per container (`processor.py:305-309`), Tika queue rows
  (`processor.py:312-313`), and the `pgcatalog_change_seq` bump that
  invalidates the query cache (`processor.py:318-324`, `cache.py:89-98`).
- Every full catalog write sets `_p_changed = True` on the object
  (`catalog.py:539-540`), so an object that was fully reindexed in a
  transaction is always among the objects written by it. Partial reindexes do
  not write the object (`catalog.py:552-594`). A transaction that only
  registers partial updates never reaches the storage; the pending data
  manager applies them through the pool (`pending.py:271-288`, #249), so such
  a transaction has no storage transaction and cannot be undone.
- `reindexObjectSecurity()` runs a partial reindex of `allowedRolesAndUsers`
  on the object itself and every descendant found by an unrestricted path
  query (`Products/CMFCore/CMFCatalogAware.py:100-131`). `allowed_roles` is a
  dedicated `TEXT[]` column (`columns.py:513-519`), and every restricted query
  injects an `allowedRolesAndUsers` filter (`catalog.py:794-802`,
  `query.py:346-351`). Every query also requires `idx IS NOT NULL`
  (`query.py:429`). `path` has a `text_pattern_ops` index for prefix queries
  (`schema.py:113`).
- Moves and renames go through the OFS dispatch wrappers, which register one
  `(old_prefix, new_prefix, depth_delta)` per moved root (`move.py:136-174`)
  and, for cross-container moves, partially reindex security on the
  descendants using the old paths (`move.py:182-232`). Reorders write the
  container's ordering annotations (`PersistentList`, `OIBTree`:
  `plone/folder/default.py:162-178`), not the container itself.
- The #244 repair (`maintenance.repair_uncataloged`, `maintenance.py:354-427`)
  walks the site and recatalogs objects whose row has `path IS NULL OR idx IS
  NULL`. It cannot see stale-but-present data. `refreshCatalog(clear=0)` and
  `reindexIndex()` skip rows with `idx IS NULL` (`catalog.py:910-913,
  955-958`). `_commit_batch` retries a conflicted batch once
  (`maintenance.py:227-258`).
- All three undo entry points end in `App.Undo.manage_undo_transactions`,
  which notes the transaction, calls `db.undoMultiple(tids)` and **commits
  inside the request** (`App/Undo.py:103-119`): the ZMI form calls it
  directly, `UndoTool.undo()` calls it after its permission check
  (`Products/CMFCore/UndoTool.py:83-105`). plone.restapi's `@transactions`
  PATCH is not installed in this checkout; from the upstream source it calls
  `portal_undo.undo()` (not verified locally). After that commit the request
  continues and the publisher commits once more at the end
  (`ZPublisher/WSGIPublisher.py:183-188`: `PubBeforeCommit`, commit,
  `PubSuccess`).
- The PG test layer builds a history-free storage (`testing.py:129`,
  default) and snapshots only `transaction_log`, `object_state`, `blob_state`
  (`zodb_pgjsonb/testing.py:55`). No plone.pgcatalog test exercises undo.

## The cases in #265, checked against the code

| Case | What the undo restores (ZODB) | What stays stale (PG columns) | Draft in #265 |
|---|---|---|---|
| Sharing (`manage_setLocalRoles` + `reindexObjectSecurity`) | `__ac_local_roles__` on the folder | `allowed_roles` of the folder **and** of every descendant (the folder's own row was partially reindexed too, `CMFCatalogAware.py:110-129`) | Right; add the folder's own row. Workflow transitions behave the same (object written, `reindexObjectSecurity` on descendants, `review_state` in `idx`). |
| Move or rename | the moved object (`_setId` writes it), the containers' BTree buckets and ordering annotations | the moved object's row (`path`, `parent_path`, `path_depth`), all descendants' paths (bulk `UPDATE`), and for cross-container moves the descendants' `allowed_roles` | Right; add the moved object's own row and the security partials of cross-container moves. |
| Edit | the object | every column of the object's row (`idx`, `searchable_text`, extra columns) | Right. Before 1.17.0 the row was NULLed, since 1.17.0 it keeps the undone values. |

Cases the issue misses:

- **Undo of a delete.** `manage_delObjects` uncatalogs the object and its
  descendants (NULL columns, each written with `_p_changed`,
  `catalog.py:695-699`) and writes the parent's bucket. Undo restores all of
  them, `process()` returns `None`, the rows stay NULL: the content is back in
  the tree but invisible. Not a regression (same before 1.17.0), not security
  relevant, but the most common real-world use of undo, and the one case where
  the rows carry no usable `path` for a follow-up.
- **Undo of an add.** The created rows are deleted. Children added in later
  transactions into a BTree folder do not rewrite the folder, so the undo
  succeeds and leaves their rows (path, `idx`) behind as dangling catalog
  entries. ZCatalog has the same dangling brains after such an undo; parity,
  but worth documenting.
- **Undo of a reorder.** Only the ordering annotations are written, the folder
  is not, so no cataloged object is among the undone zoids; the gopip ranks
  keep the undone order. Not security relevant; `maintenance.resync_gopip`
  heals it.
- **Undo of a transaction that was only a partial reindex.** Not undoable:
  no storage transaction exists (#249 path). Its column effects cannot be
  reverted; the only partial-only transactions Plone produces are
  `reindexObject(idxs=...)` calls without a write, never security changes
  (those always write the object).
- **Several tids in one `undoMultiple`.** `undo()` runs once per tid against
  `_tmp` of the earlier ones (`undo.py:29-36`, `storage.py:1587`); the hook
  below sees the DB state before any of them is written.
- **Later transactions touched the same objects.** ZODB decides: content
  conflicts fail the undo, BTree buckets usually resolve. Descendants edited
  after the undone sharing change still carry the widened roles (the edit
  recomputed them from the widened folder), so "restore old column values"
  approaches cannot work without recomputation; see option 2.
- **Partial updates applied through the pool** (#249) are not covered by the
  storage's undo at all.

## Options

### 1. Historize the processor columns

Mirror the processor columns into `object_history` and restore them with the
state. Simple and neutral, but it only covers objects written in the undone
transaction. It cannot restore the `finalize()` effects on descendants, which
is exactly the security case. Rejected as the sole mechanism.

### 2. Track all column changes per transaction and revert them

A trigger (or explicit logging) records the old value of every processor
column per tid, including `finalize()` updates; undo writes the old values
back after a conflict check. Exact for untouched rows, no application context
needed. But: a descendant that was fully reindexed after the undone sharing
change has its `allowed_roles` rewritten by that later transaction (still
widened, derived from the folder). Reverting is wrong (the later transaction
is not undone), skipping leaves the hole, and recomputing needs application
context again. It also doubles the history volume (idx JSONB and tsvectors per
row per write) and needs pack integration. Rejected.

### 3. Processor undo hook, hide fail closed, recatalog in a follow-up

The storage tells each processor which zoids an undo touches, inside the
undo's PG transaction. plone.pgcatalog hides the affected rows in that
transaction (no row exposes data of the undone transaction after commit) and
recomputes them from the restored content in a later transaction, which has
application context. The storage stays processor-neutral.

How plone.pgcatalog finds the rows that `finalize()` changed:

- **3a, conservative subtree.** Treat the subtree of every written cataloged
  object as affected: hide `allowed_roles` below it and re-run the security
  partial reindex on every descendant in the follow-up. Always safe, no new
  write-path cost, no new state besides the follow-up queue. The price: undoing
  *any* write of a container (a title edit of a top-level folder) hides its
  subtree from restricted searches and reindexes security on every descendant,
  the same work a sharing change on that folder costs.
- **3b, effects log.** `finalize()` records *which* rows and keys it touched
  per tid; undo hides exactly those. Precise and cheap at undo time, but new
  state, a tid in `finalize`, and every future `finalize` side effect must
  record itself. See "Follow-up: effects log (backlog)".

**Decision (2026-10-12): option 3 with variant 3a.** Simpler, and undo is a
rarely used, poorly behaved tool on busy multi-user databases anyway. 3b is
technically better and goes to the backlog as a later addition on top of 3a;
3a has to exist in any case as the fallback for transactions that predate an
effects log. The invariant of the design: *written objects are recataloged
fully, the subtree below them gets its security recomputed*.

## Part A: zodb-pgjsonb

### The hook

```python
def undone(self, cursor, tid, restored, deleted):
    """A transaction is being undone.

    cursor:   cursor on the connection of the undo's PG transaction (same
              transaction that writes the undo and runs finalize())
    tid:      int, the tid being undone
    restored: list[int], zoids whose previous revision will be written
    deleted:  list[int], zoids whose rows will be deleted (created in tid)
    """
```

Optional, duck-typed like `finalize`. Called from both undo implementations
right after `_compute_undo()` returns and before the entries are queued
(`storage.py:1577`, `instance.py:767`), once per undone tid, in the order
`TransactionalUndo.commit()` undoes them, in registration order over the
processors. The cursor is the one `_compute_undo` used; the `with` block is
extended to cover the hook calls.

Why there and not at vote time: at `undo()` time nothing of the undo is
written yet and the advisory lock is held, so the processor sees exactly the
pre-undo database, including the rows of `deleted` zoids and the current
`path` of `restored` ones. After `_batch_delete_objects` those rows are gone.
Per-tid calls are also the natural granularity.

Error handling: exceptions propagate out of `undo()`, so `TransactionalUndo`
raises, the transaction aborts and `tpc_abort` rolls the PG transaction back,
including the processor's SQL. A processor that cannot undo safely raises
`UndoError`.

The existing `_process_state()` call on the restored state stays. It is
correct for processors whose columns derive from the state alone (they are
restored by it) and harmless for plone.pgcatalog, which answers `None`. The
docs say both: *"undo re-runs `process()` on the restored state; a processor
whose columns come from elsewhere implements `undone()`"*.

Several processors: each is called with the same cursor and sees the SQL of
the ones before it. Processors without the hook are skipped. Nothing in the
batch writer or in `finalize` changes (#120 already leaves unanswered columns
alone; 3a does not need the tid in `finalize`).

### Code structure

The queueing loop is duplicated in `storage.py:1582-1614` and
`instance.py:769-802`. Suggested: move it into `undo.py` as
`queue_undo_entries(tmp, undo_data, process_state)` and call the hook from
there, so the hook exists in one place. Not required.

### Docs

- `register_state_processor` docstring (`storage.py:750-783`): add `undone`.
- `docs/sources/reference/state-processor-api.md`: new optional method, the
  lifecycle gets an "Undo" step between Store and Vote, and the note that
  `process()` runs on restored state.
- `docs/sources/how-to/write-state-processor.md`, `docs/sources/llms.txt`
  (`IStateProcessor` sketch), `docs/sources/reference/storage-api.md`
  (`undo()` entries), `CHANGES.md`.
- `interfaces.py` has only `IPGJsonbStorage` and `IBlobManager` today
  (`interfaces.py:1-17`), although `CLAUDE.md` already lists it as the home
  of an `IStateProcessor` interface. Add that documentary `IStateProcessor`
  with the optional methods described in docstrings (`get_schema_sql`,
  `finalize`, `undone`), still duck-typed at runtime (decision 8).

### Tests (zodb-pgjsonb)

The fake must behave like plone.pgcatalog, not like what the storage would
like to see: `PendingProcessor` (`tests/test_state_processor.py:483-503`)
gets a `finalize(cursor)` that applies out-of-band column changes to zoids it
was told about (like a partial reindex: `UPDATE ... WHERE zoid = %s`) without
those objects being written, and an `undone()` that records its arguments and
NULLs its own columns for `restored` zoids. It keeps returning `None` on undo.

- Hook called once per undone tid with int tid and int zoid lists; order with
  `undoMultiple`; `deleted` for an object created in the undone transaction.
- Both paths: `DB.undo()` (instance) and driving `PGJsonbStorage` directly
  (`tpc_begin`, `undo`, `tpc_vote`, `tpc_finish`, main `_vote` path).
- Pre-write view: inside the hook, `object_state.tid` of a restored zoid is
  still the undone tid and the row of a `deleted` zoid still exists.
- Atomicity: a hook that raises aborts the whole undo; no row changed, no
  processor SQL survives, `transaction.commit()` raises.
- Two processors, one without the hook: both column sets behave as
  documented; the undo write does not touch columns of a processor that
  answered `None`. `test_undo_does_not_null_columns` (`:665-677`) splits into
  the no-hook case (unchanged) and the hook case.
- The limitation itself: `finalize` changes a column of an unwritten zoid;
  undoing that transaction reports only the written zoids. This test
  documents why option 1 is insufficient and what 3a has to compensate for.
- `test_conformance.py` (ZODB undo conformance) stays green.

## Part B: plone.pgcatalog

New module `plone/pgcatalog/undo.py`; the processor delegates to it.

### Schema (via `get_schema_sql()`, applied once by the startup gate)

```sql
CREATE TABLE IF NOT EXISTS pgcatalog_undo_queue (
    id          BIGSERIAL PRIMARY KEY,
    kind        TEXT   NOT NULL,     -- 'recatalog' | 'security'
    zoid        BIGINT NOT NULL,
    path        TEXT,                -- pre-undo path hint (NULL for uncataloged rows)
    undone_tid  BIGINT NOT NULL,
    queued_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

The exact DDL (unique index on `(kind, zoid)` for de-duplication across
several undos) is an implementation detail. The table is created in HF mode
too; it stays empty there.

### Hiding (`undone`, inside the undo's PG transaction)

1. Read the current `path` of every `restored` and `deleted` zoid: the roots
   of the subtree rule and the path hints for the follow-up.
2. Restored zoids whose class is catalogable (`maintenance._catalogable_class`,
   `maintenance.py:261-267`, no app context needed): queue `recatalog` rows
   with the path hint, then hide the rows completely (same column set as
   `indexing.uncatalog_object`, `indexing.py:96-128`: path trio, `idx`,
   `searchable_text`, extra idx columns, backend columns). Everything in such
   a row belongs to the undone transaction or is derived from it, so all of it
   is suspect. Deleted zoids need no hiding; their rows go away at vote.
3. Subtree rule: for every restored zoid that had a path in step 1, hide
   `allowed_roles` of the rows below it (`path LIKE p || '/%' AND idx IS NOT
   NULL`; the root itself is covered by step 2) and queue a `security` row
   per such row with its current path as hint. Both are single statements per
   root over the `text_pattern_ops` path index. The subtree is read by the
   *current* paths, so after a move it is the post-move location, which is
   where the descendants' rows are.
4. Set `self._catalog_changed = True` so `finalize` bumps
   `pgcatalog_change_seq` in this transaction: the query cache is invalid as
   soon as the undo commits, not only after the follow-up.
5. Set a thread-local flag for the request-end subscriber.

Why `allowed_roles = NULL` fails closed: every restricted search injects
`allowedRolesAndUsers`, and `NULL && ARRAY[...]` is not true, so the row is
invisible to every restricted query, including anonymous ones.
`unrestrictedSearchResults` still sees it, which `reindexObjectSecurity` and
the follow-up rely on. Fully hidden rows (`idx IS NULL`) are invisible to
everything, exactly like uncataloged rows, so the #244 tools apply to them.

What the subtree rule covers and what it does not: it reverts the security
partials of sharing changes, workflow transitions and cross-container moves
(all write the root object), and, together with the move inversion in the
follow-up, the descendants' paths. It does not revert non-security partial
reindexes that an undone transaction ran on descendants without writing them
(nothing in Plone core does that; add-ons might), nor the ranks of a reorder
whose folder was not written. Both are non-security and are what the effects
log (backlog) adds.

Hidden rows and concurrent writers before the follow-up runs: a full reindex
by an editor makes the row visible with correct current data; a partial
reindex on a fully hidden row is dropped by `idx IS NOT NULL` (the follow-up
recatalogs fully anyway); a security partial on an `allowed_roles`-hidden row
writes the correct value.

### Follow-up: `undo.process_queue(catalog, limit)`

Runs in ordinary ZODB transactions with application context, after the undo
has committed.

For each queue row, in `id` order (the hook inserts `recatalog` rows before
`security` rows):

- `recatalog`: resolve the object (below), then `catalog.catalog_object(obj,
  path)`. If the resolved path differs from the hint, register
  `add_pending_move(hint, path, depth_delta)` so `finalize` rewrites the
  descendants' paths in one `UPDATE` (`processor.py:277-293`), the exact
  inverse of the undone move; remember the prefix rewrite for the hints of the
  `security` rows that follow in this run. For orderable folders also
  `gopip._register_container(obj)`: the restored ordering is the authority for
  its children's ranks.
- `security`: resolve, then `catalog.reindexObject(obj, idxs=idxs)` with
  `idxs = getattr(obj, "_cmf_security_indexes", ("allowedRolesAndUsers",))`
  as `move.py:202-204` does; this is the partial path (`catalog.py:632-643`)
  and the exact inverse of the undone partial.
- Unresolvable or no longer catalogable: drop the row and log at warning.
  A fully hidden row then stays uncataloged until `repair_uncataloged` finds
  it (it does: `path IS NULL`).

Commit every `_REBUILD_BATCH` rows through `maintenance._commit_batch`
(conflict retry once), delete the processed rows after each successful commit
with a pool connection, `cacheMinimize` between batches. Idempotent: a repeated
run recatalogs again.

**Budget.** `limit` rows per run, from `PGCATALOG_UNDO_FOLLOWUP_LIMIT`,
default 2000. With the subtree rule the queue is as large as the subtree of
every written container, so the budget matters for every undo. A security
partial costs a traversal, an object load, one indexer and one `UPDATE`,
roughly 1 to 3 ms; 2000 rows keep the request-end drain in the low seconds.
When the budget is exhausted: the processed rows are committed and deleted,
the rest stays queued and stays hidden (fail closed), the subscriber logs a
warning with the remaining count, the ZMI summary shows it, and the queue is
continued by the next undo in any process, by the ZMI button, by the
maintenance function, or by `repair_uncataloged`. Whether later requests of
the same process should continue draining in slices on their own is an open
decision.

Path resolution for queue rows: (1) `site.unrestrictedTraverse(hint)` and
compare `_p_oid`; covers edits, sharing, workflow, and every unwritten
descendant whose prefix was not rewritten. (2) Otherwise walk referrers in
SQL: `WITH RECURSIVE` over `object_state.refs @> ARRAY[zoid]` (GIN-indexed)
up to the first row with a non-NULL `path`, depth-limited (bucket → BTree →
folder is 2 to 4 hops), then `traverse(parent_path)[obj.getId()]` and compare
`_p_oid`. Covers undo of a delete (hint is NULL), rename and cross-container
move, where the restored parent bucket references the object again, and
`security` rows left from an earlier run whose prefix was rewritten since.
Candidates that do not contain the object (relation catalogs reference content
too) are rejected by the oid check. Objects loaded with `jar.get()` alone
cannot give their path: Dexterity does not persist `__parent__` (acquisition
only; `plone/dexterity/content.py` has no `__parent__` handling), hence the
walk.

Trigger with application context, in order of preference:

1. **`IPubSuccess` subscriber** (`WSGIPublisher.py:188`, after the
   publisher's commit, security manager still active until `endInteraction`
   at `:245`). If the thread-local flag is set: clear it, find the site in
   `request.PARENTS` (or `getSite()`), `transaction.begin()`, run
   `process_queue(catalog, limit)`, commit; everything wrapped so the
   subscriber never raises (an exception there turns a finished request into
   a failure and, on `ConflictError`, into a retry that would run the undo a
   second time and fail with `UndoError`). Covers the ZMI form, the Undo tool
   and plone.restapi: all commit the undo mid-request and let the publisher
   finish the request. Not `IPubBeforeCommit`: a conflict in the follow-up
   would then abort and retry the whole request. Not an after-commit hook on
   the undo transaction, for the reasons verified above.
2. **Explicit call** for console scripts and tests, documented next to the
   maintenance functions. `repair_uncataloged` (#244) drains the queue as its
   first step, so the one repair operators know also fixes undo leftovers,
   including `allowed_roles`-only hidden rows that its site walk cannot see.
3. **ZMI**: `manage_get_catalog_summary` (`catalog.py:1179-1199`) reports the
   queue length; a button runs the drain. Startup only logs a warning when the
   queue is not empty (several pods would otherwise drain concurrently).

The subscriber drains the whole queue up to the budget, not only this
request's rows, so leftovers of a crashed process are picked up by the next
undo anywhere.

Concurrent drains (two undos at once, or the ZMI button during an undo) do
duplicate work; the ZODB write conflict on the same object is retried once
and the result is the same. No row claiming in v1; a `claimed_at` column with
expiry is the extension if it ever matters.

### Cost

- Undo transaction: one `SELECT` for paths, one `UPDATE ... WHERE zoid =
  ANY(...)` for the full hides, and per root one `UPDATE` plus one `INSERT ...
  SELECT` over the path prefix index. For a root with N descendants: N rows
  updated and N queued. Milliseconds to a second.
- Follow-up: full `catalog_object` for the written objects (usually a
  handful) and N security partials per written container, whatever the undone
  transaction did to that container. For a sharing change, a workflow
  transition or a cross-container move this is the work the undone
  transaction itself did; for an edit of a container it is new cost. Full
  recatalogs of File or Image objects re-queue Tika extraction
  (`processor.py:434-454`), one job per restored file; security partials
  never touch `searchable_text`, so the bulk of a subtree has no Tika cost.
  #247 does not change this.
- No write-path cost in ordinary transactions.

### Known gaps, documented rather than solved

- Undo of a reorder: only ordering annotations are written, no cataloged
  object is among the undone zoids, ranks stay as undone.
  `maintenance.resync_gopip` heals; the effects log (backlog) closes it.
- Non-security partial reindexes on descendants by add-ons: not reverted;
  same backlog item.
- Tika picks the newest `blob_state` row of a Blob zoid (`processor.py:355-
  360`). After undoing an in-place blob rewrite that is the undone version.
  Assigning a new `NamedBlobFile` creates a new Blob object (inferred from
  plone.namedfile, not verified here), so this should not occur in practice;
  file an issue, not security.
- Partial-only transactions are not undoable (no storage transaction).
- `docs/move-optimization.md:226-231` ("after undo of a move, catalog columns
  are stale, a rebuild is needed") becomes wrong and is updated.

### Observation outside this design, to verify separately

A Zope transaction that has both dirty objects in its `Connection` and an undo
joined commits through two storage instances (the Connection's and
`TransactionalUndo`'s), each running `SELECT pg_advisory_xact_lock(0)` in its
own PG session at `tpc_begin`. The second `tpc_begin` would block on the
first forever. `manage_undo_transactions` normally has nothing dirty, and
plone.pgcatalog's `process()` would also pop the app's pending data for the
undo entry (`processor.py:158`). Worth a test and, if confirmed, its own issue
in zodb-pgjsonb. It also means the recatalog can never share the undo's
transaction, which the design respects anyway.

## Failure modes

| Failure | Effect | Recovery |
|---|---|---|
| Hook raises (SQL error, `UndoError`) | undo transaction aborts, PG rolls back hide and queue rows | none needed; the undo did not happen |
| Crash or process kill after the undo commit, before the follow-up | rows stay hidden (fail closed), queue rows persist, thread-local flag lost | next undo in any process drains the queue; `repair_uncataloged` or the ZMI button; startup warning |
| Budget exhausted | processed rows committed and deleted; the rest stays queued and hidden; warning with the count | next undo, ZMI button, maintenance function, `repair_uncataloged` |
| Follow-up transaction conflicts | `_commit_batch` retries once, then reports the batch as failed and leaves its queue rows | next drain |
| Request times out during the follow-up | processed batches are committed and their rows deleted; the rest stays queued | next drain; the budget keeps the request bounded |
| Object unresolvable (really gone, or moved again in a way the walk cannot follow) | row dropped, object stays uncataloged | `repair_uncataloged` (site walk finds `path IS NULL`) |
| Concurrent writer edits a hidden object | full reindex makes it visible with current data; later follow-up recatalogs again | harmless |
| Query cache | invalidated by the change-seq bump in the undo transaction, again by every follow-up commit | none |
| plone.pgcatalog new, zodb-pgjsonb old (no hook) | undo stays as today (stale columns) | version pin; runtime warning at registration when `supportsUndo()` and the installed zodb-pgjsonb is too old (version check like `_check_storage_fixed`, `maintenance.py:208-224`) |

## Test plan (plone.pgcatalog)

History-preserving PG layer: `PGCatalogPGFixture(history_preserving=True)`
building `PGJsonbStorage(dsn, history_preserving=True)` and `PGTestDB(dsn,
tables=TABLES + ("object_history", "pack_state"))`, otherwise `restore()`
leaves history rows of earlier tests behind and `_undo_one_object` may pick a
previous test's revision of the same zoid. New module
`tests/test_undo_integration.py` with its own layer object and a base class
carrying `layer = PGCATALOG_PG_HP_FIXTURE`. `zope.pytestlayer` has bitten this
repo before when two modules call `fixture.create` on the same layer; a
distinct layer per module is the pattern the other integration modules use,
but verify with a full-suite run before merging.

Helper: `_undo_last(portal)` = `portal.manage_undo_transactions([<last id>])`
(this commits), then `undo.process_queue(catalog, limit)` + commit when the
test wants the follow-up. Assertions read `object_state` directly, as
`_row_by_zoid` does (`test_pg_integration.py:1991-1999`).

1. **Sharing (security).** Folder F with child C. `F.manage_setLocalRoles
   ("bob", ["Reader"])`, `F.reindexObjectSecurity()`, commit; C's
   `allowed_roles` contains `user:bob`. Undo, commit. Before the follow-up:
   `allowed_roles IS NULL` for F and C, `searchResults` as bob finds neither,
   `unrestrictedSearchResults(path=F)` still lists C. After the follow-up:
   values equal the pre-sharing ones, bob finds nothing, Manager finds both,
   queue empty.
2. **Workflow transition** on a folder with children: same shape, plus
   `review_state` in `idx` restored.
3. **Rename** (`manage_renameObject`) and **cross-container move** (cut/paste
   as in `test_pg_integration.py:2744-2800`) of a folder with children: after
   the follow-up every row is at its restored path (`_assert_cataloged_at`)
   and no row remains under the undone prefix; descendants' `allowed_roles`
   reflect the restored container.
4. **Edit**: title and body edit; hidden after undo (`idx IS NULL`),
   recataloged with the old title and `searchable_text`.
5. **Edit of a container** with children: the subtree rule hides the
   children's `allowed_roles` although the edit did not touch security; the
   follow-up restores them. Documents the accepted 3a cost.
6. **Undo of a delete**: `manage_delObjects`, undo; before the follow-up the
   row is NULL; after it the object is cataloged at its path (exercises the
   referrer walk, the hint is NULL).
7. **Undo of an add** with a child added in a later transaction: the add's
   rows are gone, no queue leftover, the child's dangling row behaviour is
   whatever the decision below says.
8. **Several tids in one `undoMultiple`** (sharing then edit on the same
   folder, undone together in both orders).
9. **Budget**: `limit=1` on a subtree of three; after one run two rows remain
   queued and hidden, a second run finishes, nothing is visible in between
   for a restricted user.
10. **Crash simulation**: undo without the subscriber, then
    `repair_uncataloged` fixes both fully hidden and `allowed_roles`-only
    hidden rows.
11. **Entry point**: notify `PubSuccess(request)` after `manage_undo_
    transactions` and assert the follow-up ran; a subscriber exception is
    logged, not raised.
12. **Query cache**: a cached `searchResults` before the undo is not served
    after the undo commit.
13. **HF layer** (`test_pg_integration.py`): `undone` is never called, the
    queue stays empty.
14. **Deadlock observation**: a transaction with a dirty object and an undo;
    expect either a clean failure or document the hang (with a lock timeout
    in the test).

## Release order and minimum versions

1. **zodb-pgjsonb 1.18.0**: `undone()` hook, docs, tests. Backward
   compatible; processors without the hook are unaffected.
2. **plone.pgcatalog** (next release after 1.0.0rc7): requires
   `zodb-pgjsonb>=1.18.0` (pyproject, today `>=1.17.0`), runtime version check
   at registration when the storage supports undo, the queue table through
   `get_schema_sql()`. No upgrade step: nothing to migrate. `CHANGES.md`:
   sites that undid transactions on an HP storage before this release have
   stale rows nothing can detect; recommend one Clear and Rebuild (the #265
   workaround) after upgrading.
3. Per site: deploy zodb-pgjsonb first (no behaviour change alone), then
   plone.pgcatalog. HF sites see no change beyond one empty table.

## Follow-up: effects log (backlog)

Variant 3b, deferred on 2026-10-12. On top of 3a it adds precision: instead
of treating the whole subtree of a written container as affected, `undone()`
hides only the rows and keys that the undone transaction's `finalize()`
actually touched, and replays only those. It closes the two gaps 3a documents
(reorder undo, non-security partials on unwritten descendants) and removes
the subtree cost from undos of container edits.

What it needs:

- zodb-pgjsonb: the committing tid in `finalize`. Proposal:
  `finalize(cursor, tid=None)`, with `register_state_processor` inspecting the
  signature once and `_vote`, `tpc_vote` and `_write_prepared_transaction`
  passing `tid_int` when accepted. The alternative, reading the latest
  `transaction_log` row inside `finalize`, is wrong on the #249 pool path,
  where the latest tid belongs to someone else.
- plone.pgcatalog: a table `pgcatalog_undo_effects (tid, kind, zoid, keys,
  parent_path)` written in `finalize` only when the storage supports undo
  (`CatalogStateProcessor(track_undo=storage.supportsUndo())`): one `partial`
  row per zoid of `pop_all_partial_pending()` with its keys, one `order` row
  per gopip container. A `tracking-since` marker row (max tid when the table
  is first seen empty). In `undone()`: for `tid >= tracking-since` hide the
  recorded keys (`allowed_roles = NULL`, the mapped column for other extra idx
  keys, `idx = idx - keys` for plain keys) and queue the recorded zoids with
  their keys plus an `order` queue kind for containers; for older tids fall
  back to 3a. Pruning of effects whose tid left `transaction_log` (pack), run
  by the follow-up and the maintenance function. Tests: one effects row per
  descendant of a sharing change, none for partial-only transactions, pruning
  after pack, precise hide of a container edit (no descendant touched).

Why deferred: 3a is enough for the security requirement; the effects log is
new state on the write path and couples every future `finalize` side effect
to undo; and undo is used rarely and works poorly on busy multi-user
databases regardless of the catalog.

## Decisions

1. **Precision: 3a vs 3b.** *Decided 2026-10-12: 3a; 3b to the backlog as a
   later addition (see above).*
2. **Hook timing and signature.** In `undo()` before any write with
   `(cursor, tid, restored, deleted)` vs at vote time after the writes (loses
   the rows of deleted zoids and per-tid granularity). Name `undone` vs
   `on_undo`. *Decided 2026-10-12: in `undo()`, `undone(cursor, tid, restored,
   deleted)`, ints throughout.*
3. **Hide granularity.** Full uncatalog for restored objects plus
   `allowed_roles`-only for the subtree (follow-up replays security partials,
   cheap) vs full hide of the subtree too (uniform with #244, but N full
   recatalogs instead of N partials, and the subtree vanishes from
   unrestricted queries as well). *Decided 2026-10-12: mixed, as described. What matters is that the
   security data fails closed for the short window until the follow-up has
   run; stale paths or titles in unrestricted queries during that window are
   accepted.*
4. **Follow-up trigger.** `IPubSuccess` subscriber with its own transactions
   vs `IPubBeforeCommit` inside the publisher's transaction (conflicts retry
   the whole request) vs explicit only (leaves every UI undo hidden until an
   operator acts). *Decided 2026-10-12: `IPubSuccess`, plus explicit call and
   ZMI.*
5. **Budget.** Default for `PGCATALOG_UNDO_FOLLOWUP_LIMIT` (2000 proposed),
   and whether later requests of the same process continue draining a
   leftover queue in smaller slices on their own, or only the next undo, the
   ZMI button and maintenance do. *Decided 2026-10-12: 2000; no automatic
   continuation in v1, the warning and the ZMI counter make leftovers visible
   and the fail-closed state is safe.*
6. **Dangling rows after undo of an add.** Leave them (ZCatalog parity) vs
   apply the subtree rule to `deleted` zoids as well and let the follow-up
   drop the unresolvable rows (cheap, strictly better than ZCatalog).
   *Decided 2026-10-12: apply it; it is the same two statements.*
7. **Startup handling of a non-empty queue.** Warning only vs auto-drain
   (needs per-site context and coordination across pods). *Decided 2026-10-12:
   warning; the next undo or the ZMI button drains.*
8. **`interfaces.py`.** Add a documentary `IStateProcessor` (optional
   methods in docstrings, not enforced) vs keep the protocol in the reference
   docs only. *Decided 2026-10-12: add it; `CLAUDE.md` already claims it exists.*
9. **Shared undo loop.** Factor the duplicated queueing loop of
   `storage.py`/`instance.py` into `undo.py` while touching it vs minimal
   diff. *Decided 2026-10-12: factor it; one place for the hook call.*

## Out of scope

- Undo in history-free mode (does not exist).
- Restoring Tika-extracted text (#247) and the newest-blob-row assumption.
- The possible advisory-lock self-deadlock of mixed dirty-object + undo
  transactions (separate issue if confirmed).
- A background worker for the follow-up; the in-request drain plus
  maintenance covers the sizes undo is used for. If budgets are hit
  regularly, the queue table is already the hand-off point for one.
