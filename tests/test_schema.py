"""Unit tests for schema helpers (no database required)."""

from zodb_pgjsonb.schema import _set_lz4_compression

import psycopg


class _FakeResult:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _RecordingConn:
    """Minimal psycopg-like connection that records executed SQL.

    Emulates ``to_regclass`` (NULL for absent relations) and PostgreSQL's
    server-side error when ``ALTER TABLE`` targets a missing relation.
    """

    def __init__(self, present_tables):
        self.present = set(present_tables)
        self.executed = []
        self.altered_absent = []  # relations PG would have logged an ERROR for

    def execute(self, sql, params=None):
        self.executed.append(sql)
        stripped = sql.strip().upper()
        if stripped.startswith("SELECT TO_REGCLASS"):
            table = params[0]
            return _FakeResult((table if table in self.present else None,))
        if stripped.startswith("ALTER TABLE"):
            table = sql.split()[2]
            if table not in self.present:
                self.altered_absent.append(table)
                raise psycopg.errors.UndefinedTable(
                    f'relation "{table}" does not exist'
                )
        return _FakeResult(None)

    def commit(self):
        pass

    def rollback(self):
        pass


def test_lz4_skips_absent_tables_without_issuing_alter():
    """In non-HP mode object_history is absent; _set_lz4_compression must
    probe with to_regclass and skip it, never issuing a failing ALTER that
    PostgreSQL logs as a server-side ERROR (#82)."""
    conn = _RecordingConn(present_tables={"object_state", "blob_state"})

    _set_lz4_compression(conn)

    assert conn.altered_absent == []
    assert not any("ALTER TABLE object_history" in sql for sql in conn.executed)
    # Present tables are still compressed.
    assert any("ALTER TABLE object_state" in sql for sql in conn.executed)
    assert any("ALTER TABLE blob_state" in sql for sql in conn.executed)
