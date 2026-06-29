"""Tests for the double-checked schema-version gate around startup DDL.

See issue bluedynamics/zodb-pgjsonb#78: loser replicas must bail out of
``_apply_pending_ddl`` with one cheap SELECT before taking the advisory lock.
"""

import psycopg
import pytest


pytestmark = pytest.mark.db


def _table_exists(name):
    from tests.conftest import DSN

    conn = psycopg.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass(%s) IS NOT NULL AS ok", (name,))
            row = cur.fetchone()
            return row["ok"] if isinstance(row, dict) else row[0]
    finally:
        conn.close()


def test_install_schema_creates_marker_table(storage):
    """install_schema (run by the storage constructor) creates the
    pgjsonb_schema_state marker table."""
    assert _table_exists("pgjsonb_schema_state")


class _SchemaProcessor:
    """Minimal state processor exposing a static DDL block."""

    SQL = "CREATE INDEX IF NOT EXISTS idx_gate_test ON object_state (zoid);"

    def get_extra_columns(self):
        return []

    def process(self, zoid, class_mod, class_name, state):
        return None

    def get_schema_sql(self):
        return self.SQL


def test_processor_ddl_tagged_with_sha256_version(storage):
    """register_state_processor stores a 3-tuple whose version is the
    sha256 of the processor's get_schema_sql() output."""
    import hashlib

    storage.register_state_processor(_SchemaProcessor())

    entries = [e for e in storage._pending_ddl if e[1] == "_SchemaProcessor"]
    assert len(entries) == 1
    action, _name, version = entries[0]
    assert action == _SchemaProcessor.SQL
    expected = "sha256:" + hashlib.sha256(_SchemaProcessor.SQL.encode()).hexdigest()
    assert version == expected


def test_defer_startup_action_passes_version_through(storage):
    """defer_startup_action records the supplied version; defaults to None."""

    def _noop(dsn):
        return None

    storage.defer_startup_action(_noop, "tagged", version="v1")
    storage.defer_startup_action(_noop, "untagged")

    by_name = {name: version for _, name, version in storage._pending_ddl}
    assert by_name["tagged"] == "v1"
    assert by_name["untagged"] is None


def _read_marker(source):
    from tests.conftest import DSN

    conn = psycopg.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT version FROM pgjsonb_schema_state WHERE source = %s",
                (source,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            return row["version"] if isinstance(row, dict) else row[0]
    finally:
        conn.close()


def test_gate_skips_action_when_version_current(storage):
    """A tagged action runs once and records its version; a second
    _apply_pending_ddl with the same version skips it (no re-run)."""
    calls = []

    def _action(dsn):
        calls.append(1)

    storage.defer_startup_action(_action, "counter", version="v1")
    storage._apply_pending_ddl()
    assert calls == [1]
    assert _read_marker("counter") == "v1"

    # Same version again → gate bails, action not re-invoked.
    storage.defer_startup_action(_action, "counter", version="v1")
    storage._apply_pending_ddl()
    assert calls == [1]


def test_gate_reruns_action_when_version_changes(storage):
    """A changed version makes the action stale again → it re-runs and
    the marker is updated."""
    calls = []

    def _action(dsn):
        calls.append(1)

    storage.defer_startup_action(_action, "counter", version="v1")
    storage._apply_pending_ddl()
    assert calls == [1]

    storage.defer_startup_action(_action, "counter", version="v2")
    storage._apply_pending_ddl()
    assert calls == [1, 1]
    assert _read_marker("counter") == "v2"


def test_untagged_action_always_runs_and_is_not_marked(storage):
    """version=None entries always run (today's behavior) and write no marker."""
    calls = []

    def _action(dsn):
        calls.append(1)

    storage.defer_startup_action(_action, "untagged", version=None)
    storage._apply_pending_ddl()
    storage.defer_startup_action(_action, "untagged", version=None)
    storage._apply_pending_ddl()
    assert calls == [1, 1]
    assert _read_marker("untagged") is None


def test_gate_tolerates_missing_marker_table(storage):
    """If the marker table is absent (first deploy / probe race), the gate
    treats everything as stale, runs the action, and does not crash."""
    from tests.conftest import DSN

    conn = psycopg.connect(DSN, autocommit=True)
    conn.execute("DROP TABLE IF EXISTS pgjsonb_schema_state")
    conn.close()

    calls = []

    def _action(dsn):
        calls.append(1)

    storage.defer_startup_action(_action, "counter", version="v1")
    storage._apply_pending_ddl()
    assert calls == [1]


def test_force_ddl_env_bypasses_gate(storage, monkeypatch):
    """ZODB_PGJSONB_FORCE_DDL re-runs a tagged action even when current."""
    calls = []

    def _action(dsn):
        calls.append(1)

    storage.defer_startup_action(_action, "counter", version="v1")
    storage._apply_pending_ddl()
    assert calls == [1]

    monkeypatch.setenv("ZODB_PGJSONB_FORCE_DDL", "1")
    storage.defer_startup_action(_action, "counter", version="v1")
    storage._apply_pending_ddl()
    assert calls == [1, 1]


def test_in_lock_recheck_bails_when_other_replica_applied(storage, monkeypatch):
    """When the marker turns current between the pre-lock probe and the
    in-lock re-check, the action is skipped (no duplicate work)."""
    calls = []

    def _action(dsn):
        calls.append(1)

    storage.defer_startup_action(_action, "counter", version="v1")

    state = {"n": 0}

    def fake_filter(pending, conn=None):
        state["n"] += 1
        # First call = pre-lock probe (stale); second = in-lock re-check
        # (another replica applied while we waited → nothing left).
        return list(pending) if state["n"] == 1 else []

    monkeypatch.setattr(storage, "_filter_stale", fake_filter)

    storage._apply_pending_ddl()
    assert calls == []
    assert state["n"] == 2
