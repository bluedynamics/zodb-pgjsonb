"""Shared test configuration and fixtures for zodb-pgjsonb tests.

Usage:
    pytest                    # all tests (requires PostgreSQL)
    pytest -m "not db"        # fast unit tests only (~1s, no DB needed)
    pytest -m db              # DB integration tests only

PostgreSQL resolution order (via ``zodb_pgjsonb.testing.pg_dsn``):
    1. ``ZODB_TEST_DSN`` environment variable
    2. Default local Docker on port 5433
    3. testcontainers auto-start (fallback)
"""

from zodb_pgjsonb.storage import PGJsonbStorage
from zodb_pgjsonb.testing import get_test_dsn

import psycopg
import pytest
import ZODB


# Resolved once at import time (testcontainers auto-start if needed).
DSN = get_test_dsn()

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


# ── Markers ──────────────────────────────────────────────────────────────


def pytest_configure(config):
    config.addinivalue_line("markers", "db: test requires PostgreSQL")


def pytest_collection_modifyitems(config, items):
    """Auto-mark tests that use DB fixtures with @pytest.mark.db."""
    for item in items:
        fixture_names = set(getattr(item, "fixturenames", []))
        if fixture_names & _DB_FIXTURES:
            item.add_marker(pytest.mark.db)


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
