"""Tests for zodb_pgjsonb.startup_locks."""

from unittest import mock

import pytest


def test_lock_namespace_and_key_constants():
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_KEY
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_NS

    assert STARTUP_DDL_LOCK_NS == 0x5A4442  # "ZDB"
    assert STARTUP_DDL_LOCK_KEY == 1


def test_context_manager_sql_sequence():
    """Verify exact SQL sequence issued on acquire + release."""
    from zodb_pgjsonb.startup_locks import startup_ddl_lock
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_KEY
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_NS

    fake_conn = mock.MagicMock()
    with (
        mock.patch(
            "zodb_pgjsonb.startup_locks.psycopg.connect", return_value=fake_conn
        ),
        startup_ddl_lock("dsn=test", timeout="2s"),
    ):
        pass

    calls = fake_conn.execute.call_args_list
    # SET lock_timeout, pg_advisory_lock, pg_advisory_unlock
    assert calls[0].args[0] == "SET lock_timeout = '2s'"
    assert calls[1].args[0] == "SELECT pg_advisory_lock(%s, %s)"
    assert calls[1].args[1] == (STARTUP_DDL_LOCK_NS, STARTUP_DDL_LOCK_KEY)
    assert calls[2].args[0] == "SELECT pg_advisory_unlock(%s, %s)"
    assert calls[2].args[1] == (STARTUP_DDL_LOCK_NS, STARTUP_DDL_LOCK_KEY)
    fake_conn.close.assert_called_once()


def test_env_var_overrides_default_timeout():
    """ZODB_PGJSONB_DDL_LOCK_TIMEOUT overrides the 15min default."""
    from zodb_pgjsonb.startup_locks import startup_ddl_lock

    fake_conn = mock.MagicMock()
    with (
        mock.patch(
            "zodb_pgjsonb.startup_locks.psycopg.connect", return_value=fake_conn
        ),
        mock.patch.dict("os.environ", {"ZODB_PGJSONB_DDL_LOCK_TIMEOUT": "42s"}),
        startup_ddl_lock("dsn=test"),
    ):
        pass

    first_sql = fake_conn.execute.call_args_list[0].args[0]
    assert first_sql == "SET lock_timeout = '42s'"


def test_release_happens_on_exception():
    """Lock is released even when the body raises."""
    from zodb_pgjsonb.startup_locks import startup_ddl_lock

    fake_conn = mock.MagicMock()
    with (
        mock.patch(
            "zodb_pgjsonb.startup_locks.psycopg.connect", return_value=fake_conn
        ),
        pytest.raises(RuntimeError, match="boom"),
        startup_ddl_lock("dsn=test", timeout="1s"),
    ):
        raise RuntimeError("boom")

    # Verify unlock was called and connection closed
    unlock_calls = [
        c for c in fake_conn.execute.call_args_list if "pg_advisory_unlock" in c.args[0]
    ]
    assert len(unlock_calls) == 1
    fake_conn.close.assert_called_once()
