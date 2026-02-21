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


class TestDsnValidation:
    def test_empty_dsn_rejected(self):
        """Empty DSN should raise ValueError."""
        from unittest.mock import MagicMock
        from zodb_pgjsonb.config import PGJsonbStorageFactory

        factory = PGJsonbStorageFactory.__new__(PGJsonbStorageFactory)
        factory.config = MagicMock()
        factory.config.dsn = ""
        with pytest.raises(ValueError, match="must not be empty"):
            factory.open()

    def test_invalid_dsn_format_rejected(self):
        """DSN without = or postgresql:// prefix should raise."""
        from unittest.mock import MagicMock
        from zodb_pgjsonb.config import PGJsonbStorageFactory

        factory = PGJsonbStorageFactory.__new__(PGJsonbStorageFactory)
        factory.config = MagicMock()
        factory.config.dsn = "not a valid dsn"
        with pytest.raises(ValueError, match="Invalid DSN format"):
            factory.open()

    def test_keyvalue_dsn_accepted(self):
        """Key=value DSN should pass validation (may fail later on connect)."""
        from unittest.mock import MagicMock
        from zodb_pgjsonb.config import PGJsonbStorageFactory

        factory = PGJsonbStorageFactory.__new__(PGJsonbStorageFactory)
        factory.config = MagicMock()
        factory.config.dsn = "dbname=test host=localhost"
        # Will fail at PGJsonbStorage() creation but should pass DSN validation
        with pytest.raises(Exception) as exc_info:
            factory.open()
        # Should NOT be a ValueError about DSN format
        assert "Invalid DSN format" not in str(exc_info.value)

    def test_uri_dsn_accepted(self):
        """PostgreSQL URI should pass validation."""
        from unittest.mock import MagicMock
        from zodb_pgjsonb.config import PGJsonbStorageFactory

        factory = PGJsonbStorageFactory.__new__(PGJsonbStorageFactory)
        factory.config = MagicMock()
        factory.config.dsn = "postgresql://user:pass@localhost/db"
        with pytest.raises(Exception) as exc_info:
            factory.open()
        assert "Invalid DSN format" not in str(exc_info.value)


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

    def test_zconfig_s3_happy_path(self):
        """ZConfig with S3 config creates storage with S3 client + cache."""
        import types
        import unittest.mock
        import ZODB.config

        _clean_db()
        config_text = f"""\
        %import zodb_pgjsonb
        <pgjsonb>
            dsn {DSN}
            s3-bucket-name my-bucket
            s3-prefix blobs/
            s3-endpoint-url http://localhost:9000
            s3-region us-east-1
            s3-access-key TESTKEY
            s3-secret-key TESTSECRET
            s3-use-ssl false
            blob-cache-dir /tmp/test-blob-cache
            blob-cache-size 512MB
        </pgjsonb>
        """
        mock_s3_client_cls = unittest.mock.MagicMock()
        mock_blob_cache_cls = unittest.mock.MagicMock()

        # Create fake modules with the expected classes
        fake_s3client = types.ModuleType("zodb_s3blobs.s3client")
        fake_s3client.S3Client = mock_s3_client_cls
        fake_cache = types.ModuleType("zodb_s3blobs.cache")
        fake_cache.S3BlobCache = mock_blob_cache_cls
        fake_pkg = types.ModuleType("zodb_s3blobs")

        import sys

        with unittest.mock.patch.dict(
            sys.modules,
            {
                "zodb_s3blobs": fake_pkg,
                "zodb_s3blobs.s3client": fake_s3client,
                "zodb_s3blobs.cache": fake_cache,
            },
        ):
            storage = ZODB.config.storageFromString(config_text)
            try:
                assert isinstance(storage, PGJsonbStorage)
                mock_s3_client_cls.assert_called_once_with(
                    bucket_name="my-bucket",
                    prefix="blobs/",
                    endpoint_url="http://localhost:9000",
                    region_name="us-east-1",
                    aws_access_key_id="TESTKEY",
                    aws_secret_access_key="TESTSECRET",
                    use_ssl=False,
                )
                mock_blob_cache_cls.assert_called_once_with(
                    cache_dir="/tmp/test-blob-cache",
                    max_size=512 * 1024 * 1024,
                )
            finally:
                storage.close()
