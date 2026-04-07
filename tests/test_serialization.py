"""Tests for serialization helpers and security hardening.

Consolidates tests from test_storage.py (TestExtensionSerialization,
TestUnsanitizeFromPg) and test_security.py (TestDeserializeExtension,
TestMaskDsn, TestUnsanitizeFromPg).
"""

from zodb_pgjsonb.serialization import _deserialize_extension
from zodb_pgjsonb.serialization import _serialize_extension
from zodb_pgjsonb.serialization import _unsanitize_from_pg
from zodb_pgjsonb.storage import _mask_dsn

import base64
import json
import pickle
import pytest


class TestSerializeExtension:
    """Test _serialize_extension."""

    def test_serialize_empty_bytes(self):
        assert _serialize_extension(b"") == b""

    def test_serialize_empty_dict(self):
        assert _serialize_extension({}) == b""

    def test_serialize_dict(self):
        result = _serialize_extension({"key": "value"})
        assert json.loads(result) == {"key": "value"}

    def test_serialize_pickle_bytes(self):
        """Pickle bytes from ZODB are converted to JSON."""
        ext_dict = {"user": "admin"}
        pkl = pickle.dumps(ext_dict, protocol=3)
        result = _serialize_extension(pkl)
        assert json.loads(result) == ext_dict

    def test_serialize_pickle_empty_dict(self):
        """Pickled empty dict returns empty bytes (not JSON)."""
        pkl = pickle.dumps({}, protocol=3)
        assert _serialize_extension(pkl) == b""

    def test_serialize_none_type(self):
        """Non-bytes, non-dict returns empty bytes."""
        assert _serialize_extension(None) == b""


class TestDeserializeExtension:
    """Test _deserialize_extension including security hardening."""

    def test_deserialize_empty(self):
        assert _deserialize_extension(b"") == {}
        assert _deserialize_extension(None) == {}

    def test_deserialize_json(self):
        data = json.dumps({"key": "value"}).encode()
        assert _deserialize_extension(data) == {"key": "value"}

    def test_deserialize_memoryview(self):
        """memoryview is converted to bytes before parsing."""
        data = json.dumps({"x": 1}).encode()
        mv = memoryview(data)
        assert _deserialize_extension(mv) == {"x": 1}

    def test_deserialize_pickle_fallback(self):
        """Pickle bytes (legacy data) are deserialized via fallback."""
        ext_dict = {"legacy": True}
        pkl = pickle.dumps(ext_dict, protocol=3)
        assert _deserialize_extension(pkl) == ext_dict

    def test_legacy_pickle_safe_types(self):
        data = pickle.dumps({"user": "admin", "count": 42})
        result = _deserialize_extension(data)
        assert result == {"user": "admin", "count": 42}

    def test_deserialize_garbage_returns_empty(self):
        """Unparseable data returns empty dict."""
        assert _deserialize_extension(b"\xff\xfe\xfd") == {}

    def test_pickle_with_dangerous_class_blocked(self):
        # Craft a pickle that would execute os.system("echo pwned")
        malicious = (
            b"\x80\x04\x95\x1e\x00\x00\x00\x00\x00\x00\x00"
            b"\x8c\x02os\x8c\x06system\x93\x8c\x0becho pwned\x85R."
        )
        # Should return empty dict, NOT execute the code
        assert _deserialize_extension(malicious) == {}


class TestMaskDsn:
    """Test _mask_dsn password masking."""

    def test_mask_unquoted_password(self):
        dsn = "host=localhost password=secret dbname=test"
        assert "secret" not in _mask_dsn(dsn)
        assert "***" in _mask_dsn(dsn)

    def test_mask_quoted_password(self):
        dsn = "host=localhost password='my secret pass' dbname=test"
        assert "my secret pass" not in _mask_dsn(dsn)
        assert "***" in _mask_dsn(dsn)

    def test_no_password(self):
        dsn = "host=localhost dbname=test"
        assert _mask_dsn(dsn) == dsn


class TestUnsanitizeFromPg:
    """Test _unsanitize_from_pg for strings, dicts, and lists."""

    def test_normal_string_unchanged(self):
        assert _unsanitize_from_pg("hello") == "hello"

    def test_ns_marker_restored(self):
        val = {"@ns": base64.b64encode(b"hello\x00world").decode()}
        assert _unsanitize_from_pg(val) == "hello\x00world"

    def test_invalid_utf8_raises(self):
        # Invalid UTF-8 sequence
        val = {"@ns": base64.b64encode(b"\xff\xfe").decode()}
        with pytest.raises(UnicodeDecodeError):
            _unsanitize_from_pg(val)

    def test_list_with_ns_marker(self):
        """Lists containing @ns markers are unsanitized."""
        val = "hello\x00world"
        encoded = base64.b64encode(val.encode("utf-8", errors="surrogatepass")).decode()
        result = _unsanitize_from_pg([{"@ns": encoded}, "normal"])
        assert result[0] == val
        assert result[1] == "normal"

    def test_list_unchanged(self):
        """Lists without markers are returned unchanged (same object)."""
        original = ["a", "b", "c"]
        result = _unsanitize_from_pg(original)
        assert result is original
