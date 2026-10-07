"""Tests for the state processor plugin infrastructure.

Verifies that registered state processors can:
- Extract keys from object state and provide them as extra columns
- Write extra columns atomically alongside object_state rows
- Handle the NULL case (no extra data for an object)
- Work correctly with both main and instance storage paths
"""

from psycopg.rows import dict_row
from tests.conftest import clean_db
from tests.conftest import DSN
from zodb_pgjsonb.storage import ExtraColumn
from zodb_pgjsonb.storage import PGJsonbStorage

import json
import psycopg
import pytest
import transaction as txn
import ZODB


pytestmark = pytest.mark.db


def _state_as_dict(state):
    """Convert state (dict or JSON string) to dict for annotation extraction."""
    if isinstance(state, str):
        return json.loads(state)
    return state


# ── Test processor ──────────────────────────────────────────────────


class DummyProcessor:
    """Extracts a ``_test_extra`` key from state → writes it as a PG column."""

    ANNOTATION_KEY = "_test_extra"

    def get_extra_columns(self):
        return [
            ExtraColumn("test_label", "%(test_label)s"),
        ]

    def process(self, zoid, class_mod, class_name, state):
        state_dict = _state_as_dict(state)
        pending = state_dict.get(self.ANNOTATION_KEY)
        if pending is None:
            return None
        return {"test_label": pending}


class NullSentinelProcessor:
    """Processor that handles both dict values and None sentinel."""

    ANNOTATION_KEY = "_pgcat"

    def get_extra_columns(self):
        return [
            ExtraColumn("cat_path", "%(cat_path)s"),
        ]

    def process(self, zoid, class_mod, class_name, state):
        state_dict = _state_as_dict(state)
        if self.ANNOTATION_KEY not in state_dict:
            return None
        pending = state_dict.get(self.ANNOTATION_KEY)
        if pending is None:
            # Sentinel: clear the column
            return {"cat_path": None}
        return {"cat_path": pending.get("path")}


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def storage():
    """PGJsonbStorage with DummyProcessor registered."""
    clean_db()
    s = PGJsonbStorage(DSN)
    # Add the test_label column to object_state
    s._conn.execute("ALTER TABLE object_state ADD COLUMN IF NOT EXISTS test_label TEXT")
    s._conn.commit()
    s.register_state_processor(DummyProcessor())
    yield s
    s.close()


@pytest.fixture
def db(storage):
    database = ZODB.DB(storage)
    yield database
    database.close()


# ── Tests ───────────────────────────────────────────────────────────


class TestExtraColumn:
    """ExtraColumn dataclass basics."""

    def test_defaults(self):
        ec = ExtraColumn("foo", "%(foo)s")
        assert ec.name == "foo"
        assert ec.value_expr == "%(foo)s"
        assert ec.update_expr is None

    def test_custom_update_expr(self):
        ec = ExtraColumn("bar", "%(bar)s", "COALESCE(EXCLUDED.bar, object_state.bar)")
        assert ec.update_expr == "COALESCE(EXCLUDED.bar, object_state.bar)"

    def test_invalid_name_rejected(self):
        with pytest.raises(ValueError, match="valid SQL identifier"):
            ExtraColumn("drop table;--", "%(x)s")


class TestStateProcessorRegistration:
    """Registration and discovery."""

    def test_register_processor(self, storage):
        # DummyProcessor is already registered by fixture
        assert len(storage._state_processors) == 1

    def test_get_extra_columns(self, storage):
        cols = storage._get_extra_columns()
        assert cols is not None
        assert len(cols) == 1
        assert cols[0].name == "test_label"

    def test_no_processors_returns_none(self):
        clean_db()
        s = PGJsonbStorage(DSN)
        try:
            assert s._get_extra_columns() is None
            assert s._process_state(1, "mod", "cls", {}) is None
        finally:
            s.close()


class TestStateProcessorWritePath:
    """Extra columns written alongside object_state via ZODB.DB."""

    def test_extra_data_written_to_pg(self, db):
        """Object with annotation → extra column is written."""
        conn = db.open()
        root = conn.root()
        root["title"] = "Hello"
        root._test_extra = "my-label"
        txn.commit()

        # Read directly from PG to verify
        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label, state FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row is not None
        assert row["test_label"] == "my-label"

    def test_no_annotation_writes_null(self, db):
        """Object without annotation → extra column is NULL."""
        conn = db.open()
        root = conn.root()
        root["other"] = "value"
        txn.commit()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row is not None
        assert row["test_label"] is None

    def test_annotation_update_on_second_commit(self, db):
        """Second commit with new annotation → extra column updated."""
        conn = db.open()
        root = conn.root()
        root["title"] = "v1"
        root._test_extra = "label-v1"
        txn.commit()

        root["title"] = "v2"
        root._test_extra = "label-v2"
        txn.commit()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row["test_label"] == "label-v2"

    def test_abort_does_not_write_extra(self, db):
        """Transaction abort → no extra column data written."""
        conn = db.open()
        root = conn.root()
        root["title"] = "committed"
        txn.commit()

        # Now start a new change but abort
        root["title"] = "aborted"
        root._test_extra = "should-not-appear"
        txn.abort()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute(
                "SELECT test_label FROM object_state WHERE zoid = %s",
                (zoid,),
            )
            row = cur.fetchone()
        pg.close()
        conn.close()

        # Should be NULL — the abort prevented the write
        assert row["test_label"] is None


class TestNullSentinelProcessor:
    """Processor that supports None sentinel for clearing columns."""

    @pytest.fixture
    def storage_with_sentinel(self):
        clean_db()
        s = PGJsonbStorage(DSN)
        s._conn.execute(
            "ALTER TABLE object_state ADD COLUMN IF NOT EXISTS cat_path TEXT"
        )
        s._conn.commit()
        s.register_state_processor(NullSentinelProcessor())
        yield s
        s.close()

    @pytest.fixture
    def db_sentinel(self, storage_with_sentinel):
        database = ZODB.DB(storage_with_sentinel)
        yield database
        database.close()

    def test_set_then_clear(self, db_sentinel):
        """Set extra column, then clear it via None sentinel."""
        conn = db_sentinel.open()
        root = conn.root()

        # Set
        root._pgcat = {"path": "/plone/doc"}
        root["title"] = "Doc"
        txn.commit()

        zoid = int.from_bytes(root._p_oid, "big")
        pg = psycopg.connect(DSN, row_factory=dict_row)
        with pg.cursor() as cur:
            cur.execute("SELECT cat_path FROM object_state WHERE zoid = %s", (zoid,))
            row = cur.fetchone()
        assert row["cat_path"] == "/plone/doc"

        # Clear via None sentinel
        root._pgcat = None
        txn.commit()

        with pg.cursor() as cur:
            cur.execute("SELECT cat_path FROM object_state WHERE zoid = %s", (zoid,))
            row = cur.fetchone()
        pg.close()
        conn.close()

        assert row["cat_path"] is None


# ── Finalize hook ──────────────────────────────────────────────────


class FinalizeProcessor:
    """Processor with finalize() hook for testing.

    Records cursor references to verify finalize is called with
    the same cursor used for batch writes (same PG transaction).
    """

    finalize_calls: list = []  # noqa: RUF012

    def get_extra_columns(self):
        return []

    def process(self, zoid, class_mod, class_name, state):
        return None

    def finalize(self, cursor):
        self.finalize_calls.append(cursor)


class TestFinalizeHook:
    """Verify finalize(cursor) is called during tpc_vote."""

    @pytest.fixture(autouse=True)
    def _reset(self):
        FinalizeProcessor.finalize_calls = []

    @pytest.fixture
    def storage_finalize(self):
        clean_db()
        s = PGJsonbStorage(DSN)
        self.proc = FinalizeProcessor()
        s.register_state_processor(self.proc)
        yield s
        s.close()

    @pytest.fixture
    def db_finalize(self, storage_finalize):
        database = ZODB.DB(storage_finalize)
        # Clear calls from DB initialization (root object creation)
        self.proc.finalize_calls.clear()
        yield database
        database.close()

    def test_finalize_called_during_tpc_vote(self, db_finalize):
        """finalize() is called once per commit with a live cursor."""
        conn = db_finalize.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()

        assert len(self.proc.finalize_calls) == 1
        # The cursor was valid during the call (it's closed after)
        assert self.proc.finalize_calls[0] is not None
        conn.close()

    def test_finalize_called_on_each_commit(self, db_finalize):
        """finalize() is called once per commit, not accumulated."""
        conn = db_finalize.open()
        root = conn.root()
        root["x"] = 1
        txn.commit()
        root["x"] = 2
        txn.commit()

        assert len(self.proc.finalize_calls) == 2
        conn.close()

    def test_finalize_can_execute_sql(self, db_finalize):
        """finalize() can execute SQL on the provided cursor."""

        class SQLFinalizeProcessor:
            def get_extra_columns(self):
                return []

            def process(self, zoid, class_mod, class_name, state):
                return None

            def finalize(self, cursor):
                # Execute a harmless SQL statement
                cursor.execute("SELECT 1")

        storage = db_finalize.storage
        storage.register_state_processor(SQLFinalizeProcessor())

        conn = db_finalize.open()
        root = conn.root()
        root["y"] = 42
        txn.commit()  # Should not raise
        conn.close()

    def test_processor_without_finalize_not_called(self):
        """Processors without finalize() are silently skipped."""
        clean_db()
        s = PGJsonbStorage(DSN)
        s.register_state_processor(DummyProcessor())  # no finalize method
        s._conn.execute(
            "ALTER TABLE object_state ADD COLUMN IF NOT EXISTS test_label TEXT"
        )
        s._conn.commit()
        db = ZODB.DB(s)
        conn = db.open()
        root = conn.root()
        root["z"] = 1
        txn.commit()  # Should not raise
        conn.close()
        db.close()
        s.close()

    def test_finalize_not_called_on_abort(self, db_finalize):
        """finalize() is NOT called when transaction is aborted."""
        conn = db_finalize.open()
        root = conn.root()
        root["x"] = 1
        txn.abort()

        assert len(self.proc.finalize_calls) == 0
        conn.close()


# ── Deferred DDL on read path (#105) ────────────────────────────────


class DDLProcessor:
    """Processor that provides DDL adding a column."""

    def get_extra_columns(self):
        return [ExtraColumn("ddl_test_col", "%(ddl_test_col)s")]

    def get_schema_sql(self):
        return "ALTER TABLE object_state ADD COLUMN IF NOT EXISTS ddl_test_col TEXT"

    def process(self, zoid, class_mod, class_name, state):
        return None


class TestDeferredDDLOnReadPath:
    """DDL deferred from startup must be applied on first read, not just write.

    Regression test for #105: if the first request after startup is a
    read-only GET, poll_invalidations() must apply pending DDL so that
    queries referencing new columns don't crash with UndefinedColumn.
    """

    def test_poll_invalidations_applies_pending_ddl(self):
        clean_db()
        storage = PGJsonbStorage(DSN)
        try:
            # Register processor — DDL is deferred to _pending_ddl
            storage.register_state_processor(DDLProcessor())
            assert len(storage._pending_ddl) > 0, "DDL should be deferred"

            # Column should NOT exist yet
            pg = psycopg.connect(DSN, row_factory=dict_row)
            with pg.cursor() as cur:
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = 'object_state' "
                    "AND column_name = 'ddl_test_col'"
                )
                assert cur.fetchone() is None, "Column must not exist before poll"
            pg.close()

            # Create an instance and call poll_invalidations (read path)
            instance = storage.new_instance()
            try:
                instance.poll_invalidations()

                # DDL should have been applied — column now exists
                pg = psycopg.connect(DSN, row_factory=dict_row)
                with pg.cursor() as cur:
                    cur.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_name = 'object_state' "
                        "AND column_name = 'ddl_test_col'"
                    )
                    row = cur.fetchone()
                pg.close()
                assert row is not None, "Column should exist after poll_invalidations"
                assert len(storage._pending_ddl) == 0, "Pending DDL should be cleared"
            finally:
                instance.release()
        finally:
            storage.close()


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

        database, _proc_a, _proc_b = two_processors
        conn = database.open()
        child = PersistentMapping()
        conn.root()["child"] = child
        txn.commit()
        zoid = int.from_bytes(child._p_oid, "big")
        conn.close()
        assert _pp_row(zoid) == {"a_one": None, "a_two": None, "b_one": None}

    def test_process_state_reports_answering_processors(self, two_processors):
        database, proc_a, _proc_b = two_processors
        storage = database.storage
        proc_a.pending[7] = {}
        assert storage._process_state(7, "m", "C", "{}") == {0: {}}
        assert storage._process_state(7, "m", "C", "{}") is None
