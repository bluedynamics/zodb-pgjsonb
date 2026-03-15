"""ZMI integration for PGJsonbStorage.

Monkey-patches App.ApplicationManager.AltDatabaseManager to add a
"Blob Storage" tab when the database uses PGJsonbStorage.

Only loaded when Zope is available (guarded import in config.py).
"""

from .interfaces import IPGJsonbStorage


_patched = False


def _is_pgjsonb_storage(self):
    """Filter function: show tab only for PGJsonbStorage databases."""
    try:
        storage = self._getDB().storage
        return IPGJsonbStorage.providedBy(storage)
    except Exception:
        return False


def _manage_get_blob_stats(self):
    """Delegate to storage's get_blob_stats()."""
    storage = self._getDB().storage
    if hasattr(storage, "get_blob_stats"):
        return storage.get_blob_stats()
    return {"available": False}


def _manage_get_blob_histogram(self):
    """Delegate to storage's get_blob_histogram()."""
    storage = self._getDB().storage
    if hasattr(storage, "get_blob_histogram"):
        return storage.get_blob_histogram()
    return []


def patch_database_manager():
    """Add 'Blob Storage' tab to AltDatabaseManager."""
    global _patched
    if _patched:
        return
    _patched = True

    from App.ApplicationManager import AltDatabaseManager
    from App.special_dtml import DTMLFile

    # DTML template for the Blob Storage tab
    AltDatabaseManager.manage_blobStats = DTMLFile("www/blobStats", globals())

    # Delegation methods
    AltDatabaseManager.manage_get_blob_stats = _manage_get_blob_stats
    AltDatabaseManager.manage_get_blob_histogram = _manage_get_blob_histogram

    # Security: Manager-only access
    AltDatabaseManager.manage_blobStats__roles__ = ("Manager",)
    AltDatabaseManager.manage_get_blob_stats__roles__ = ("Manager",)
    AltDatabaseManager.manage_get_blob_histogram__roles__ = ("Manager",)

    # Append tab with filter: only visible for PGJsonbStorage databases
    AltDatabaseManager.manage_options = (
        *AltDatabaseManager.manage_options,
        {
            "label": "Blob Storage",
            "action": "manage_blobStats",
            "filter": _is_pgjsonb_storage,
        },
    )
