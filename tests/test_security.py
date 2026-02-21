"""Tests for security hardening fixes."""

import pickle
import pytest


class TestDeserializeExtension:
    def test_json_extension(self):
        from zodb_pgjsonb.storage import _deserialize_extension

        import json

        data = json.dumps({"user": "admin"}).encode("utf-8")
        assert _deserialize_extension(data) == {"user": "admin"}

    def test_empty_extension(self):
        from zodb_pgjsonb.storage import _deserialize_extension

        assert _deserialize_extension(b"") == {}
        assert _deserialize_extension(None) == {}

    def test_legacy_pickle_safe_types(self):
        from zodb_pgjsonb.storage import _deserialize_extension

        data = pickle.dumps({"user": "admin", "count": 42})
        result = _deserialize_extension(data)
        assert result == {"user": "admin", "count": 42}

    def test_pickle_with_dangerous_class_blocked(self):
        from zodb_pgjsonb.storage import _deserialize_extension

        # Craft a pickle that would execute os.system("echo pwned")
        malicious = (
            b"\x80\x04\x95\x1e\x00\x00\x00\x00\x00\x00\x00"
            b"\x8c\x02os\x8c\x06system\x93\x8c\x0becho pwned\x85R."
        )
        # Should return empty dict, NOT execute the code
        assert _deserialize_extension(malicious) == {}

    def test_memoryview_input(self):
        from zodb_pgjsonb.storage import _deserialize_extension

        import json

        data = json.dumps({"key": "val"}).encode("utf-8")
        assert _deserialize_extension(memoryview(data)) == {"key": "val"}


class TestMaskDsn:
    def test_mask_unquoted_password(self):
        from zodb_pgjsonb.storage import _mask_dsn

        dsn = "host=localhost password=secret dbname=test"
        assert "secret" not in _mask_dsn(dsn)
        assert "***" in _mask_dsn(dsn)

    def test_mask_quoted_password(self):
        from zodb_pgjsonb.storage import _mask_dsn

        dsn = "host=localhost password='my secret pass' dbname=test"
        assert "my secret pass" not in _mask_dsn(dsn)
        assert "***" in _mask_dsn(dsn)

    def test_no_password(self):
        from zodb_pgjsonb.storage import _mask_dsn

        dsn = "host=localhost dbname=test"
        assert _mask_dsn(dsn) == dsn


class TestUnsanitizeFromPg:
    def test_normal_string_unchanged(self):
        from zodb_pgjsonb.storage import _unsanitize_from_pg

        assert _unsanitize_from_pg("hello") == "hello"

    def test_ns_marker_restored(self):
        from zodb_pgjsonb.storage import _unsanitize_from_pg

        import base64

        val = {"@ns": base64.b64encode(b"hello\x00world").decode()}
        assert _unsanitize_from_pg(val) == "hello\x00world"

    def test_invalid_utf8_raises(self):
        from zodb_pgjsonb.storage import _unsanitize_from_pg

        import base64

        # Invalid UTF-8 sequence
        val = {"@ns": base64.b64encode(b"\xff\xfe").decode()}
        with pytest.raises(UnicodeDecodeError):
            _unsanitize_from_pg(val)
