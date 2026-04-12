"""Tests for zodb_pgjsonb.startup_locks."""


def test_lock_namespace_and_key_constants():
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_KEY
    from zodb_pgjsonb.startup_locks import STARTUP_DDL_LOCK_NS

    assert STARTUP_DDL_LOCK_NS == 0x5A4442  # "ZDB"
    assert STARTUP_DDL_LOCK_KEY == 1
