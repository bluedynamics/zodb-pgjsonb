"""Tests for ZConfig integration (config.py + component.xml)."""

from tests.conftest import DSN
from zodb_pgjsonb.storage import PGJsonbStorage

import psycopg
import pytest


def _clean_db():
    conn = psycopg.connect(DSN)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT pg_terminate_backend(pid) "
            "FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid != pg_backend_pid()"
        )
        cur.execute(
            "DROP TABLE IF EXISTS "
            "pack_state, blob_history, object_history, "
            "blob_state, object_state, transaction_log CASCADE"
        )
    conn.commit()
    conn.close()


class TestZConfig:
    """Test that ZConfig <pgjsonb> section creates a working storage."""

    def test_zconfig_minimal(self):
        """Minimal ZConfig creates a PGJsonbStorage."""
        import ZODB.config

        _clean_db()
        config_text = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
            dsn {DSN}
        </pgjsonb>
        """
        storage = ZODB.config.storageFromString(config_text)
        try:
            assert isinstance(storage, PGJsonbStorage)
            assert storage.getName() == "pgjsonb"
        finally:
            storage.close()

    def test_zconfig_with_name(self):
        """ZConfig with custom name."""
        import ZODB.config

        _clean_db()
        config_text = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
            dsn {DSN}
            name my-storage
        </pgjsonb>
        """
        storage = ZODB.config.storageFromString(config_text)
        try:
            assert storage.getName() == "my-storage"
        finally:
            storage.close()

    def test_zconfig_history_preserving(self):
        """ZConfig with history-preserving mode."""
        import ZODB.config

        _clean_db()
        config_text = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
            dsn {DSN}
            history-preserving true
        </pgjsonb>
        """
        storage = ZODB.config.storageFromString(config_text)
        try:
            assert isinstance(storage, PGJsonbStorage)
            assert storage._history_preserving is True
        finally:
            storage.close()

    def test_zconfig_pool_settings(self):
        """ZConfig with pool size settings."""
        import ZODB.config

        _clean_db()
        config_text = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
            dsn {DSN}
            pool-size 2
            pool-max-size 5
        </pgjsonb>
        """
        storage = ZODB.config.storageFromString(config_text)
        try:
            assert isinstance(storage, PGJsonbStorage)
        finally:
            storage.close()

    def test_zconfig_s3_missing_dependency(self):
        """ZConfig with S3 config when zodb_s3blobs is not installed raises."""
        import sys
        import unittest.mock
        import ZODB.config

        _clean_db()
        config_text = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
            dsn {DSN}
            s3-bucket-name test-bucket
        </pgjsonb>
        """
        # Mock zodb_s3blobs as not importable
        with (
            unittest.mock.patch.dict(
                sys.modules,
                {
                    "zodb_s3blobs": None,
                    "zodb_s3blobs.cache": None,
                    "zodb_s3blobs.s3client": None,
                },
            ),
            pytest.raises(ImportError, match="S3 blob storage requires"),
        ):
            ZODB.config.storageFromString(config_text)
