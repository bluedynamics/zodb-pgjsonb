"""Serialization helpers for transaction extension data and PG sanitization."""

import base64
import io
import json
import pickle


def _serialize_extension(ext):
    """Serialize transaction extension for BYTEA storage.

    ZODB extensions are dicts; we store them as JSON bytes in PG.
    BaseStorage passes extension_bytes (pickle) — we always convert to JSON.
    """
    if isinstance(ext, bytes):
        if not ext:
            return b""
        # BaseStorage passes zodbpickle-serialized bytes — decode to dict
        try:
            from ZODB._compat import loads as zodb_loads

            ext_dict = zodb_loads(ext)
            if isinstance(ext_dict, dict) and ext_dict:
                return json.dumps(ext_dict).encode("utf-8")
            return b""
        except (
            ImportError,
            TypeError,
            KeyError,
            AttributeError,
            EOFError,
            ValueError,
            OverflowError,
        ):
            return ext  # fallback: store as-is
    if isinstance(ext, dict):
        if not ext:
            return b""
        return json.dumps(ext).encode("utf-8")
    return b""


class _RestrictedUnpickler(pickle.Unpickler):
    """Unpickler that only allows safe built-in types.

    Used for legacy extension data that may be stored as pickle.
    Blocks arbitrary code execution from crafted pickle payloads.
    """

    _SAFE_BUILTINS = frozenset(
        {
            "dict",
            "list",
            "tuple",
            "set",
            "frozenset",
            "str",
            "bytes",
            "int",
            "float",
            "bool",
            "complex",
        }
    )

    def find_class(self, module, name):
        import builtins

        if module == "builtins" and name in self._SAFE_BUILTINS:
            return getattr(builtins, name)
        raise pickle.UnpicklingError(
            f"Restricted unpickler: {module}.{name} is not allowed"
        )


def _restricted_loads(data):
    """Load pickle data using the restricted unpickler (safe types only)."""
    return _RestrictedUnpickler(io.BytesIO(data)).load()


def _deserialize_extension(ext_data):
    """Deserialize extension bytes from PG BYTEA back to a dict.

    Stored as JSON by _serialize_extension.  Falls back to a restricted
    unpickler for legacy data written before JSON serialization was in place.
    The restricted unpickler only allows safe types (dict, str, bytes, int,
    float, bool, list, tuple).
    """
    if not ext_data:
        return {}
    if isinstance(ext_data, memoryview):
        ext_data = bytes(ext_data)
    try:
        result = json.loads(ext_data)
        if isinstance(result, dict):
            return result
    except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
        pass
    # Fallback: restricted unpickle (legacy data only — safe types)
    try:
        result = _restricted_loads(ext_data)
        if isinstance(result, dict):
            return result
    except Exception:
        pass
    return {}


def _unsanitize_from_pg(obj):
    """Reverse ``@ns`` markers back to strings with null bytes.

    Returns original objects unchanged when no @ns markers are found
    (zero allocations in the common case).
    """
    if isinstance(obj, dict):
        if "@ns" in obj and len(obj) == 1:
            return base64.b64decode(obj["@ns"]).decode("utf-8")
        new = {}
        changed = False
        for k, v in obj.items():
            new_v = _unsanitize_from_pg(v)
            if new_v is not v:
                changed = True
            new[k] = new_v
        return new if changed else obj
    if isinstance(obj, list):
        new = []
        changed = False
        for item in obj:
            new_item = _unsanitize_from_pg(item)
            if new_item is not item:
                changed = True
            new.append(new_item)
        return new if changed else obj
    return obj
