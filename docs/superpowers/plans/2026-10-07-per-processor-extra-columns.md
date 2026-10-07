# Per-processor extra column writes Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A state processor that returns `None` for an object leaves that processor's `object_state` columns untouched, instead of the batch writer overwriting them with `NULL`.

**Architecture:** `_process_state()` stops merging processor results into one dict and instead reports which processors answered (`{processor_position: result}`). `_batch_write_objects()` resolves that per object into exactly the columns to write (all columns of every answering processor, missing keys as `NULL`), groups objects by that column set, and runs one `executemany` per group. A group without extra columns upserts base columns only, so `ON CONFLICT` keeps the stored values. `ExtraColumn.update_expr` is honoured in the `ON CONFLICT` clause.

**Tech Stack:** Python 3.12+, psycopg 3 (`executemany` pipelining), PostgreSQL, ZODB, pytest.

**Spec:** bluedynamics/zodb-pgjsonb#120 (issue body, section "Required semantics: per processor"). Undo correctness is out of scope (#121).

## Global Constraints

- Work only in the worktree `sources/zodb-pgjsonb-wt/fix-120-processor-columns` (branch `fix/120-processor-columns`). Never in `sources/zodb-pgjsonb/`.
- Processor-neutral: no code may know about plone.pgcatalog. Correct for any number of registered processors.
- Per-processor rule, exactly: `None` → that processor's columns untouched (new rows get column defaults); dict → all of that processor's columns written, missing keys as `NULL`; `{}` → all of that processor's columns written as `NULL`.
- Do NOT implement a per-key rule ("write only keys present"). plone.pgcatalog relies on missing keys becoming `NULL` (`extract_extra_idx_columns()` drops `None` keys).
- Two processors declaring the same column name: the later-registered processor wins (same as today's `extra.update()`).
- History tables (`object_history`) stay base-columns only. No schema change.
- ruff pinned: `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .` must pass. C901 max-complexity is 13; split helpers rather than adding `noqa`.
- Imports at module top (tests may import inline, as existing tests do).
- Every commit message ends with `Assisted-by: Claude Opus 5.5` (or the model doing the work). Never `Co-Authored-By`, never a noreply address.
- `CHANGES.md` gets an entry under `## unreleased` in this PR.
- Tests need the dev DB: `docker --context default start zodb-pgjsonb-dev`, then run pytest as `env -u ZODB_TEST_DSN uv run pytest ...`. Never run two pytest processes at the same time (shared `zodb_test` DB). Set up the worktree venv once with `uv sync --all-extras` (see the repo's CLAUDE.md).

## Review Focus

1. **Conflict resolution keeps columns.** An object whose conflict gets resolved in `tpc_vote`, with no processor answer, must keep its stored columns (`conflict.py` only sets `_extra` when re-processing answers). Pinned in Task 3.
2. **History-preserving mode.** A plain write in HP mode keeps the columns and still copies the old revision to `object_history`. Pinned in Task 3.
3. **Undo no longer NULLs.** After `DB.undo()`, rows keep their (current) column values instead of `NULL`; full correctness is #121. Pinned in Task 3.
4. **Mixed groups in one commit.** One transaction writing answered and unanswered objects (the normal pgcatalog case: content plus BTree buckets) writes each correctly; duplicate zoids in the queue resolve to the last entry. Pinned in Task 3 (direct writer test).
5. **New object, no answer.** First write of an object no processor answers for still inserts the row, extra columns at their defaults. Pinned in Task 1.

---

## File Structure

- `src/zodb_pgjsonb/storage.py`: `_process_state()` returns per-processor answers; new `_get_processor_columns()`; `_vote()` passes processor columns; `register_state_processor()` docstring documents the rule.
- `src/zodb_pgjsonb/instance.py`: `tpc_vote()` passes processor columns.
- `src/zodb_pgjsonb/batch.py`: `_batch_write_objects()` groups by resolved column set; new helpers `_base_params()`, `_extra_columns_for()`, `_upsert_group()`; `_write_prepared_transaction()` takes `processor_columns`.
- `src/zodb_pgjsonb/migration.py`: `_make_write_worker()` passes processor columns.
- `tests/test_state_processor.py`: new fake `PendingProcessor` and test classes.
- `docs/sources/reference/state-processor-api.md`, `docs/sources/how-to/write-state-processor.md`: return-value contract, `update_expr` example.
- `CHANGES.md`.

---

### Task 1: Per-processor write semantics

**Files:**
- Modify: `src/zodb_pgjsonb/storage.py` (`_process_state` at ~1006, `_get_extra_columns` at ~1015, `_vote` at ~1253)
- Modify: `src/zodb_pgjsonb/instance.py` (`tpc_vote` at ~560)
- Modify: `src/zodb_pgjsonb/batch.py` (`_batch_write_objects`, `_write_prepared_transaction`)
- Modify: `src/zodb_pgjsonb/migration.py` (`_make_write_worker` at ~583)
- Test: `tests/test_state_processor.py`

**Interfaces:**
- Produces: `PGJsonbStorage._process_state(zoid, class_mod, class_name, state) -> dict[int, dict] | None` (keys are positions in `_state_processors`, values the processor's returned dict, `{}` included).
- Produces: `PGJsonbStorage._get_processor_columns() -> list[list[ExtraColumn]] | None` (aligned with `_state_processors`).
- Produces: `batch._batch_write_objects(cur, objects, tid_int, history_preserving=False, processor_columns=None)`; `objects[i]["_extra"]` is the `_process_state()` result.
- Produces: `batch._write_prepared_transaction(conn, txn_data, history_preserving, processor_columns, processors, blob_sink=None, blob_threshold=102_400, idempotent=False)`.
- Unchanged: `_get_extra_columns()` (flat list, still used by the DDL gate).

- [ ] **Step 1: Add the fake processor and fixture**

Append to `tests/test_state_processor.py` (after the existing fixtures section is fine; keep helpers near the new tests):

```python
# ── Per-processor write semantics (#120) ───────────────────────────


class PendingProcessor:
    """Fake shaped like plone.pgcatalog's CatalogStateProcessor.

    Data arrives out of band (``pending``, keyed by zoid) and is popped
    on use, so a later plain write of the same object finds nothing and
    returns None.  A dict answer may omit columns (they must become NULL);
    ``{}`` clears all of this processor's columns.
    """

    def __init__(self, *columns):
        self.columns = [
            c if isinstance(c, ExtraColumn) else ExtraColumn(c, f"%({c})s")
            for c in columns
        ]
        self.pending = {}

    def get_extra_columns(self):
        return list(self.columns)

    def process(self, zoid, class_mod, class_name, state):
        return self.pending.pop(zoid, None)


_PP_COLUMNS = ("a_one", "a_two", "b_one")


def _pp_storage(storage_factory=PGJsonbStorage, **kw):
    clean_db()
    s = storage_factory(DSN, **kw)
    for col in _PP_COLUMNS:
        s._conn.execute(f"ALTER TABLE object_state ADD COLUMN IF NOT EXISTS {col} TEXT")
    s._conn.commit()
    return s


def _pp_row(zoid):
    with psycopg.connect(DSN, row_factory=dict_row) as pg, pg.cursor() as cur:
        cur.execute(
            "SELECT a_one, a_two, b_one FROM object_state WHERE zoid = %s",
            (zoid,),
        )
        return cur.fetchone()


@pytest.fixture
def two_processors():
    """ZODB.DB over a storage with processor A (a_one, a_two) and B (b_one)."""
    s = _pp_storage()
    proc_a = PendingProcessor("a_one", "a_two")
    proc_b = PendingProcessor("b_one")
    s.register_state_processor(proc_a)
    s.register_state_processor(proc_b)
    database = ZODB.DB(s)
    yield database, proc_a, proc_b
    database.close()
    s.close()


def _commit_root(database, value):
    """Write the root object (zoid 0) once.

    Set ``proc.pending[0]`` before calling to make a processor answer.
    """
    conn = database.open()
    conn.root()["v"] = value
    txn.commit()
    conn.close()
```

- [ ] **Step 2: Write the failing tests**

```python
class TestPerProcessorWriteSemantics:
    """A processor's None leaves its columns alone; a dict writes all of them."""

    def _seed(self, database, proc_a, proc_b):
        proc_a.pending[0] = {"a_one": "x", "a_two": "y"}
        proc_b.pending[0] = {"b_one": "z"}
        _commit_root(database, 1)
        assert _pp_row(0) == {"a_one": "x", "a_two": "y", "b_one": "z"}

    def test_none_keeps_stored_columns(self, two_processors):
        database, proc_a, proc_b = two_processors
        self._seed(database, proc_a, proc_b)
        _commit_root(database, 2)  # plain write, nobody answers
        assert _pp_row(0) == {"a_one": "x", "a_two": "y", "b_one": "z"}

    def test_mixed_answers_touch_only_the_answering_processor(self, two_processors):
        database, proc_a, proc_b = two_processors
        self._seed(database, proc_a, proc_b)
        proc_a.pending[0] = {"a_one": "x2", "a_two": "y2"}
        _commit_root(database, 2)
        assert _pp_row(0) == {"a_one": "x2", "a_two": "y2", "b_one": "z"}

    def test_missing_key_in_answer_becomes_null(self, two_processors):
        database, proc_a, proc_b = two_processors
        self._seed(database, proc_a, proc_b)
        proc_a.pending[0] = {"a_one": "x2"}
        _commit_root(database, 2)
        assert _pp_row(0) == {"a_one": "x2", "a_two": None, "b_one": "z"}

    def test_empty_dict_clears_only_that_processors_columns(self, two_processors):
        database, proc_a, proc_b = two_processors
        self._seed(database, proc_a, proc_b)
        proc_a.pending[0] = {}
        _commit_root(database, 2)
        assert _pp_row(0) == {"a_one": None, "a_two": None, "b_one": "z"}

    def test_explicit_none_values_clear(self, two_processors):
        """The uncatalog-sentinel shape: every column explicitly None."""
        database, proc_a, proc_b = two_processors
        self._seed(database, proc_a, proc_b)
        proc_a.pending[0] = {"a_one": None, "a_two": None}
        _commit_root(database, 2)
        assert _pp_row(0) == {"a_one": None, "a_two": None, "b_one": "z"}

    def test_new_object_without_answer_gets_defaults(self, two_processors):
        from persistent.mapping import PersistentMapping

        database, proc_a, proc_b = two_processors
        conn = database.open()
        child = PersistentMapping()
        conn.root()["child"] = child
        txn.commit()
        zoid = int.from_bytes(child._p_oid, "big")
        conn.close()
        assert _pp_row(zoid) == {"a_one": None, "a_two": None, "b_one": None}

    def test_process_state_reports_answering_processors(self, two_processors):
        database, proc_a, proc_b = two_processors
        storage = database.storage
        proc_a.pending[7] = {}
        assert storage._process_state(7, "m", "C", "{}") == {0: {}}
        assert storage._process_state(7, "m", "C", "{}") is None
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_state_processor.py::TestPerProcessorWriteSemantics -v`
Expected: `test_new_object_without_answer_gets_defaults` PASSES; the other six FAIL (`b_one` / all columns come back `None`; `_process_state` returns a merged dict or `None` for `{}`).

- [ ] **Step 4: Change `_process_state` and add `_get_processor_columns` in `storage.py`**

Replace `_process_state`:

```python
def _process_state(self, zoid, class_mod, class_name, state):
    """Run all registered state processors.

    Returns ``{processor_position: result}`` for every processor whose
    ``process()`` did not return *None* (an empty dict counts as an
    answer), or *None* when no processor answered.  The batch writer
    writes exactly the columns of the processors that answered, so a
    processor returning *None* leaves its stored columns untouched
    (#120).
    """
    answers = {}
    for pos, proc in enumerate(self._state_processors):
        result = proc.process(zoid, class_mod, class_name, state)
        if result is not None:
            answers[pos] = result
    return answers or None


def _get_processor_columns(self):
    """Extra columns per processor, aligned with ``_state_processors``."""
    if not self._state_processors:
        return None
    return [list(proc.get_extra_columns()) for proc in self._state_processors]
```

Keep `_get_extra_columns()` as is. All call sites of `_process_state` (`store`, `restore`, undo in `storage.py` and `instance.py`, `conflict.py`, `migration.py`) keep `if extra: entry["_extra"] = extra` unchanged: the new return value is truthy exactly when some processor answered.

- [ ] **Step 5: Rewrite `_batch_write_objects` in `batch.py`**

Replace the function with the version below plus helpers (base column lists move to module level):

```python
_BASE_COLS = [
    "zoid",
    "tid",
    "class_mod",
    "class_name",
    "state",
    "state_size",
    "refs",
]
_BASE_VALS = [f"%({c})s" for c in _BASE_COLS]


def _base_params(obj, tid_int):
    return {
        "zoid": obj["zoid"],
        "tid": tid_int,
        "class_mod": obj["class_mod"],
        "class_name": obj["class_name"],
        "state": (
            Json(obj["state"], dumps=lambda s: s)
            if isinstance(obj["state"], str)
            else Json(obj["state"])
        ),
        "state_size": obj["state_size"],
        "refs": obj["refs"],
    }


def _extra_columns_for(answers, processor_columns):
    """Return ``(columns, values)`` to write for one object.

    *answers* maps processor position to the dict its ``process()``
    returned.  Every column of an answering processor is written, keys
    missing from its dict as NULL.  Columns of processors that did not
    answer are left out, so ON CONFLICT keeps their stored values.  If two
    processors declare the same column, the later one wins.
    """
    columns = {}
    values = {}
    for pos in sorted(answers):
        result = answers[pos]
        for ec in processor_columns[pos]:
            columns[ec.name] = ec
            values[ec.name] = result.get(ec.name)
    return tuple(columns.values()), values


def _upsert_group(cur, columns, params_list):
    """Upsert *params_list* with the base columns plus *columns*."""
    cols = _BASE_COLS + [ec.name for ec in columns]
    vals = _BASE_VALS + [ec.value_expr for ec in columns]
    update_parts = [f"{c} = EXCLUDED.{c}" for c in _BASE_COLS[1:]]
    update_parts += [f"{ec.name} = EXCLUDED.{ec.name}" for ec in columns]
    cur.executemany(
        f"INSERT INTO object_state ({', '.join(cols)}) "
        f"VALUES ({', '.join(vals)}) "
        f"ON CONFLICT (zoid) DO UPDATE SET {', '.join(update_parts)}",
        params_list,
    )


def _batch_write_objects(
    cur, objects, tid_int, history_preserving=False, processor_columns=None
):
    """Write multiple objects in batch using executemany (pipelined).

    psycopg3's executemany() automatically uses pipeline mode, sending
    all statements in a single network round-trip instead of waiting for
    each individual result.

    *processor_columns* (from ``_get_processor_columns()``) lists the
    extra columns per state processor.  Each object's ``_extra`` (from
    ``_process_state()``) says which processors answered; only their
    columns are written for that object.  Objects are grouped by the
    resulting column set, one executemany per group.  History tables
    always use the base columns only.
    """
    # SECURITY NOTE: Table names (object_state, object_history) are string
    # constants, not user input.  If table names are ever made configurable,
    # use psycopg.sql.Identifier() to prevent SQL injection.
    if not objects:
        return

    # One row per zoid, the last queued entry wins (as with the former single
    # executemany); grouping below would otherwise reorder duplicates.
    objects = list({obj["zoid"]: obj for obj in objects}.values())

    groups = {}  # column names -> (columns, params_list)
    for obj in objects:
        params = _base_params(obj, tid_int)
        answers = obj.get("_extra")
        columns = ()
        if answers and processor_columns:
            columns, values = _extra_columns_for(answers, processor_columns)
            params.update(values)
        key = tuple(ec.name for ec in columns)
        groups.setdefault(key, (columns, []))[1].append(params)

    # ── History: preserve old versions before overwrite ──────────
    if history_preserving:
        zoid_list = [obj["zoid"] for obj in objects]
        cur.execute(
            "INSERT INTO object_history "
            "(zoid, tid, class_mod, class_name, state, state_size, refs) "
            "SELECT zoid, tid, class_mod, class_name, state, state_size, refs "
            "FROM object_state WHERE zoid = ANY(%s) "
            "ON CONFLICT (zoid, tid) DO NOTHING",
            (zoid_list,),
        )

    for columns, params_list in groups.values():
        _upsert_group(cur, columns, params_list)
```

In `_write_prepared_transaction`, rename the parameter `extra_columns` to `processor_columns` (docstring too) and pass `processor_columns=processor_columns` to `_batch_write_objects`.

- [ ] **Step 6: Update the callers**

`storage.py` `_vote()` and `instance.py` `tpc_vote()`:

```python
processor_columns = self._get_processor_columns()  # storage.py
_batch_write_objects(cur, writes, tid_int, hp, processor_columns=processor_columns)
```

```python
processor_columns = self._main._get_processor_columns()  # instance.py
_batch_write_objects(cur, writes, tid_int, hp, processor_columns=processor_columns)
```

`migration.py` `_make_write_worker()`: replace `extra_columns = self._get_extra_columns()` with `processor_columns = self._get_processor_columns()` and pass `processor_columns` positionally where `extra_columns` was passed to `_write_prepared_transaction`.

Then check nothing else uses the old keyword: `grep -rn "extra_columns=" src/ tests/` must show no calls to `_batch_write_objects` / `_write_prepared_transaction` with it.

- [ ] **Step 7: Run the new and the existing processor tests**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_state_processor.py -v`
Expected: all PASS, including the pre-existing `TestStateProcessorWritePath` and `TestNullSentinelProcessor`.

- [ ] **Step 8: Run the full suite and lint**

Run: `env -u ZODB_TEST_DSN uv run pytest -q` then `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`
Expected: no failures; ruff clean (C901 ≤ 13).

- [ ] **Step 9: Commit**

```bash
git add src/zodb_pgjsonb/storage.py src/zodb_pgjsonb/instance.py src/zodb_pgjsonb/batch.py src/zodb_pgjsonb/migration.py tests/test_state_processor.py
git commit -m "fix: leave a state processor's columns untouched when it returns None (#120)

The batch writer wrote every registered extra column for every object,
so a processor answering None got its columns overwritten with NULL.
With plone.pgcatalog this silently uncataloged content on plain writes.

_process_state() now reports which processors answered, and the writer
writes exactly their columns (missing keys as NULL, {} clears them),
grouped into one executemany per column set.

Assisted-by: Claude Opus 5.5"
```

---

### Task 2: Honour `ExtraColumn.update_expr`

**Files:**
- Modify: `src/zodb_pgjsonb/batch.py` (`_upsert_group`)
- Modify: `docs/sources/reference/state-processor-api.md` (Construction example)
- Test: `tests/test_state_processor.py`

**Interfaces:**
- Consumes: `_upsert_group(cur, columns, params_list)` from Task 1; `PendingProcessor`, `_pp_storage`, `_pp_row`, `_commit_root` from Task 1.

- [ ] **Step 1: Write the failing test**

```python
class TestUpdateExpr:
    def test_update_expr_used_on_conflict(self):
        s = _pp_storage()
        proc = PendingProcessor(
            ExtraColumn(
                "a_one", "%(a_one)s", "COALESCE(EXCLUDED.a_one, object_state.a_one)"
            ),
            "a_two",
        )
        s.register_state_processor(proc)
        database = ZODB.DB(s)
        try:
            proc.pending[0] = {"a_one": "first", "a_two": "first"}
            _commit_root(database, 1)
            proc.pending[0] = {"a_one": None, "a_two": None}
            _commit_root(database, 2)
            row = _pp_row(0)
        finally:
            database.close()
            s.close()
        # update_expr keeps the old value; a_two uses the default EXCLUDED.a_two
        assert row["a_one"] == "first"
        assert row["a_two"] is None
```

- [ ] **Step 2: Run it to verify it fails**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_state_processor.py::TestUpdateExpr -v`
Expected: FAIL, `row["a_one"]` is `None`.

- [ ] **Step 3: Use `update_expr` in `_upsert_group`**

```python
    update_parts += [
        f"{ec.name} = {ec.update_expr or f'EXCLUDED.{ec.name}'}" for ec in columns
    ]
```

- [ ] **Step 4: Fix the docs example**

In `docs/sources/reference/state-processor-api.md`, the second construction example passes `update_expr="to_tsvector('simple'::regconfig, EXCLUDED.searchable_text)"`. That would fail once `update_expr` is honoured: `EXCLUDED.searchable_text` is already the `tsvector` produced by `value_expr`. Replace that example with:

```python
ExtraColumn(
    name="searchable_text",
    value_expr="to_tsvector('simple'::regconfig, %(searchable_text)s)",
)

ExtraColumn(
    name="first_seen",
    value_expr="%(first_seen)s",
    # Keep the first stored value on later writes.
    update_expr="COALESCE(object_state.first_seen, EXCLUDED.first_seen)",
)
```

and add one sentence after the field table: "`EXCLUDED.{name}` holds the value already computed by `value_expr`; `object_state.{name}` is the stored value."

- [ ] **Step 5: Run tests and lint**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_state_processor.py -v && uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .`
Expected: all PASS, ruff clean.

- [ ] **Step 6: Commit**

```bash
git add src/zodb_pgjsonb/batch.py tests/test_state_processor.py docs/sources/reference/state-processor-api.md
git commit -m "fix: honour ExtraColumn.update_expr in the ON CONFLICT clause

It was documented but never used. The docs example built a tsvector
from EXCLUDED, which is already a tsvector; replaced.

Assisted-by: Claude Opus 5.5"
```

---

### Task 3: Pin the other write paths

All tests in this task are expected to PASS right away after Task 1; they pin behaviour on paths the fix reaches indirectly. If one fails, fix the path, do not weaken the test.

**Files:**
- Test: `tests/test_state_processor.py`

**Interfaces:**
- Consumes: `PendingProcessor`, `_pp_storage`, `_pp_row`, `_commit_root` (Task 1); `batch._batch_write_objects(cur, objects, tid_int, history_preserving=False, processor_columns=None)` (Task 1).

- [ ] **Step 1: Write the tests**

```python
class TestWritePathsKeepColumns:
    def _db(self, **kw):
        s = _pp_storage(**kw)
        proc = PendingProcessor("a_one", "a_two")
        s.register_state_processor(proc)
        return s, ZODB.DB(s), proc

    def test_history_preserving_plain_write(self):
        s, database, proc = self._db(history_preserving=True)
        try:
            proc.pending[0] = {"a_one": "x", "a_two": "y"}
            _commit_root(database, 1)
            _commit_root(database, 2)
            with psycopg.connect(DSN) as pg, pg.cursor() as cur:
                cur.execute("SELECT count(*) FROM object_history WHERE zoid = 0")
                history_rows = cur.fetchone()[0]
        finally:
            database.close()
            s.close()
        assert _pp_row(0)["a_one"] == "x"
        assert history_rows >= 2

    def test_undo_does_not_null_columns(self):
        """Interim behaviour: undo keeps the current values (#121 makes it exact)."""
        s, database, proc = self._db(history_preserving=True)
        try:
            proc.pending[0] = {"a_one": "x", "a_two": "y"}
            _commit_root(database, 1)
            _commit_root(database, 2)
            database.undo(s.lastTransaction())
            txn.commit()
        finally:
            database.close()
            s.close()
        assert _pp_row(0) == {"a_one": "x", "a_two": "y", "b_one": None}

    def test_conflict_resolution_keeps_columns(self):
        from BTrees.Length import Length

        s, database, proc = self._db()
        try:
            conn1 = database.open()
            conn1.root()["len"] = Length()
            txn.commit()
            length = conn1.root()["len"]
            zoid = int.from_bytes(length._p_oid, "big")
            proc.pending[zoid] = {"a_one": "x", "a_two": "y"}
            length.change(1)
            txn.commit()

            tm2 = txn.TransactionManager()
            conn2 = database.open(transaction_manager=tm2)
            conn2.root()["len"].change(1)  # conn2 reads the current state
            length.change(1)
            txn.commit()  # conn1 wins
            tm2.commit()  # conn2 conflicts, Length resolves it
            conn1.close()
            conn2.close()
        finally:
            database.close()
            s.close()
        assert _pp_row(zoid) == {"a_one": "x", "a_two": "y", "b_one": None}


class TestBatchWriterDirect:
    """_batch_write_objects with hand-built entries: grouping and duplicates."""

    def _entry(self, zoid, extra=None, state="{}"):
        e = {
            "zoid": zoid,
            "class_mod": "m",
            "class_name": "C",
            "state": state,
            "state_size": 2,
            "refs": [],
        }
        if extra is not None:
            e["_extra"] = extra
        return e

    def test_mixed_groups_and_duplicates(self):
        from zodb_pgjsonb.batch import _batch_write_objects

        s = _pp_storage()
        s.close()
        cols = [
            [ExtraColumn("a_one", "%(a_one)s"), ExtraColumn("a_two", "%(a_two)s")],
            [ExtraColumn("b_one", "%(b_one)s")],
        ]
        with psycopg.connect(DSN) as pg, pg.cursor() as cur:
            cur.execute(
                "INSERT INTO object_state "
                "(zoid, tid, class_mod, class_name, state, state_size, refs, "
                " a_one, a_two, b_one) "
                "VALUES (10, 1, 'm', 'C', '{}', 2, '{}', 'old', 'old', 'old'), "
                "       (11, 1, 'm', 'C', '{}', 2, '{}', 'old', 'old', 'old')"
            )
            _batch_write_objects(
                cur,
                [
                    self._entry(10),  # nobody answers
                    self._entry(11, {1: {"b_one": "first"}}),
                    self._entry(12, {0: {"a_one": "new"}}),  # new row
                    self._entry(11, {0: {}, 1: {"b_one": "last"}}),  # duplicate, wins
                ],
                tid_int=2,
                processor_columns=cols,
            )
        assert _pp_row(10) == {"a_one": "old", "a_two": "old", "b_one": "old"}
        assert _pp_row(11) == {"a_one": None, "a_two": None, "b_one": "last"}
        assert _pp_row(12) == {"a_one": "new", "a_two": None, "b_one": None}
```

Note: the conflict test needs `TransactionManager` from `transaction`; the module imports `transaction as txn`, so `txn.TransactionManager()` works. If ZODB raises `ReadConflictError` instead of resolving, check that both connections loaded `len` before either commit (call `conn2.root()["len"].value` before `length.change(1)`).

- [ ] **Step 2: Run them**

Run: `env -u ZODB_TEST_DSN uv run pytest tests/test_state_processor.py::TestWritePathsKeepColumns tests/test_state_processor.py::TestBatchWriterDirect -v`
Expected: all PASS. Optional teeth check: `git stash` is off limits (shared stash stack), so if you want to see these fail on the old writer, check out `main` in a throwaway worktree, copy only this test class over, and run it there. Skip it if time is short; Task 1's red run already showed the bug.

- [ ] **Step 3: Lint and commit**

```bash
uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .
git add tests/test_state_processor.py
git commit -m "test: pin processor columns on HP writes, undo, conflict resolution and grouping (#120)

Assisted-by: Claude Opus 5.5"
```

---

### Task 4: Document the contract, changelog

**Files:**
- Modify: `src/zodb_pgjsonb/storage.py` (`register_state_processor` docstring, ~750)
- Modify: `docs/sources/reference/state-processor-api.md` (`process()` entry and Lifecycle step 3)
- Modify: `docs/sources/how-to/write-state-processor.md` (section "Implement the process method")
- Modify: `CHANGES.md`

- [ ] **Step 1: Docstring**

In `register_state_processor`, replace the paragraph starting "``process`` may modify *state* in-place" with:

```
        ``process`` may modify *state* in-place (e.g. pop annotation keys).
        Its return value decides what happens to **this processor's**
        columns for the object:

        - *None*: the columns are left untouched (a new row gets the
          column defaults).
        - a dict ``{column_name: value}``: all of this processor's
          columns are written; columns missing from the dict become NULL.
        - ``{}``: all of this processor's columns are set to NULL.

        Other processors' columns are only affected by their own answers.
        If two processors declare the same column, the later one wins.
```

- [ ] **Step 2: Reference docs**

In `state-processor-api.md`, replace the `process()` sentence "Returns a dict of `{column_name: value}` for extra columns, or `None` when no extra data applies to this object." with the same three cases as a list, and replace Lifecycle step 3's "All objects (with extra column data) are written in a batched `executemany()` call." with: "Objects are grouped by the set of columns to write (the columns of every processor that answered) and written with one batched `executemany()` per group. A processor that returned `None` has its columns left out of the statement, so their stored values stay."

- [ ] **Step 3: How-to**

In `write-state-processor.md`, replace "Return a dict mapping column names to values, or return `None` to skip writing extra columns for this object." with:

```markdown
Return `None` when you have nothing to say about this object: your columns keep their stored values.
Return a dict to write your columns: every column you declared is written, and columns missing from the dict become `NULL`.
Return `{}` to clear all your columns.
```

- [ ] **Step 4: Changelog**

Add at the top of `## unreleased` in `CHANGES.md`:

```markdown
- **Fix: a plain object write no longer wipes state-processor columns
  (#120).** The batch writer wrote every registered extra column for every
  object, so a processor that returned `None` ("nothing to say about this
  object") got its columns overwritten with `NULL`. With plone.pgcatalog
  this silently uncataloged content whenever an object was written without
  a full reindex, for example when opening the edit form (edit lock), on
  the sharing tab, or on a write followed by a partial reindex. Now the
  rule is per processor: `None` leaves its columns untouched, a dict
  writes all its columns (missing keys as `NULL`), `{}` clears them. This
  also holds with several registered processors. Rows wiped before this
  fix stay wiped; plone.pgcatalog ships a repair
  (bluedynamics/plone-pgcatalog#244). Undo still restores processor
  columns incorrectly (#121), but no longer sets them to `NULL`.
- `ExtraColumn.update_expr` is now used in the `ON CONFLICT` clause. It
  was documented but ignored.
```

- [ ] **Step 5: Build check and commit**

Run: `uvx ruff@0.16.7 format --check . && uvx ruff@0.16.7 check .` (docs build optional locally; CI builds it).

```bash
git add src/zodb_pgjsonb/storage.py docs/sources/reference/state-processor-api.md docs/sources/how-to/write-state-processor.md CHANGES.md
git commit -m "docs: document the per-processor return-value contract (#120)

Assisted-by: Claude Opus 5.5"
```

---

### After the tasks

- Push the branch and open a PR against `main` titled `fix: leave a state processor's columns untouched when it returns None (#120)`, body in English, ending with `Assisted-by: Claude Opus 5.5`, `Fixes #120`.
- Release (new minor, expected 1.17.0, because `{}` and `update_expr` change behaviour) happens after merge via a GitHub Release. plone.pgcatalog#244 then raises its minimum to that version.
