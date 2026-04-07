"""Shared test configuration and fixtures for zodb-pgjsonb tests.

Usage:
    pytest                    # all tests (requires PostgreSQL)
    pytest -m "not db"        # fast unit tests only (~1s, no DB needed)
    pytest -m db              # DB integration tests only
"""

from zodb_pgjsonb.storage import PGJsonbStorage

import os
import psycopg
import pytest
import ZODB


# Allow DSN override via environment variable for CI.
# Default: local Docker on port 5433 (development setup).
DSN = os.environ.get(
    "ZODB_TEST_DSN",
    "dbname=zodb_test user=zodb password=zodb host=localhost port=5433",
)

# All tables that may exist across history-free and history-preserving modes.
ALL_TABLES = (
    "cache_warm_stats",
    "migration_watermark",
    "pack_state",
    "blob_history",
    "object_history",
    "blob_state",
    "object_state",
    "transaction_log",
)

# Fixtures that require a running PostgreSQL.
_DB_FIXTURES = frozenset({"storage", "db", "hp_storage", "hp_db"})


def _pg_available():
    """Check if the test PostgreSQL is reachable."""
    try:
        conn = psycopg.connect(DSN, connect_timeout=2)
        conn.close()
        return True
    except Exception:
        return False


_pg_ok = None  # lazy singleton


def _check_pg():
    global _pg_ok
    if _pg_ok is None:
        _pg_ok = _pg_available()
    return _pg_ok


# ── Markers ──────────────────────────────────────────────────────────────


def pytest_configure(config):
    config.addinivalue_line("markers", "db: test requires PostgreSQL")


def pytest_collection_modifyitems(config, items):
    """Auto-mark tests that use DB fixtures with @pytest.mark.db.

    Tests are marked as DB tests if they:
    - Use a conftest fixture that needs PG (storage, db, hp_storage, hp_db)
    - Already carry @pytest.mark.db (set via pytestmark in the test module)

    DB tests are skipped when PostgreSQL is unreachable (graceful degradation).
    """
    skip_db = pytest.mark.skip(reason="PostgreSQL not available")
    for item in items:
        # Auto-detect from fixtures
        fixture_names = set(getattr(item, "fixturenames", []))
        if fixture_names & _DB_FIXTURES:
            item.add_marker(pytest.mark.db)
        # Skip all DB-marked tests if PG is down
        if item.get_closest_marker("db") and not _check_pg():
            item.add_marker(skip_db)


# ── Database helpers ─────────────────────────────────────────────────────


def clean_db():
    """Drop all tables for a fresh start.

    Terminates other connections to the test database first to avoid
    blocking on open REPEATABLE READ transactions left by prior tests.
    """
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        cur.execute("DROP TABLE IF EXISTS " + ", ".join(ALL_TABLES) + " CASCADE")
        cur.execute("DROP SEQUENCE IF EXISTS zoid_seq")
    conn.commit()
    conn.close()


# ── Shared pytest fixtures ───────────────────────────────────────────────


@pytest.fixture
def storage():
    """Fresh PGJsonbStorage (history-free) with clean database."""
    clean_db()
    s = PGJsonbStorage(DSN)
    yield s
    s.close()


@pytest.fixture
def db(storage):
    """ZODB.DB using our storage."""
    database = ZODB.DB(storage)
    yield database
    database.close()


@pytest.fixture
def hp_storage():
    """Fresh PGJsonbStorage in history-preserving mode with clean database."""
    clean_db()
    s = PGJsonbStorage(DSN, history_preserving=True)
    yield s
    s.close()


@pytest.fixture
def hp_db(hp_storage):
    """ZODB.DB using history-preserving storage."""
    database = ZODB.DB(hp_storage)
    yield database
    database.close()
