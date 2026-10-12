"""Zope interface declarations for zodb-pgjsonb."""

from zope.interface import Interface


class IPGJsonbStorage(Interface):
    """Marker interface for PostgreSQL JSONB storage."""


class IBlobManager(Interface):
    """Manages tiered blob storage (PG bytea + optional S3)."""

    def store_blob(cursor, oid, tid, blob_path):
        """Store a blob file. Small blobs go to PG, large to S3."""

    def load_blob(cursor, oid, tid):
        """Load a blob, returning a local file path."""


class IStateProcessor(Interface):
    """Plugin that maintains extra ``object_state`` columns.

    Documentary only: processors are duck-typed at runtime and registered
    with ``PGJsonbStorage.register_state_processor()``.  Only
    ``get_extra_columns`` and ``process`` are required; the other methods are
    optional and looked up with ``hasattr``.  See the state processor API
    reference for the full semantics.
    """

    def get_extra_columns():
        """Return the list of ``ExtraColumn`` this processor writes."""

    def process(zoid, class_mod, class_name, state):
        """Return the column values for an object, or None to leave them.

        None keeps this processor's stored columns, a dict writes all of them
        (missing keys become NULL), ``{}`` sets them all to NULL.  Undo
        re-runs this on the restored state.
        """

    def get_schema_sql():
        """Optional.  Return DDL to apply at registration, or None."""

    def finalize(cursor):
        """Optional.  Called at the end of the vote, in the writing PG transaction."""

    def undone(cursor, tid, restored, deleted, entries):
        """Optional.  A transaction is being undone (history-preserving only).

        Called from ``undo()`` once per undone tid, before anything of the
        undo is written, on the cursor of the PG transaction that will carry
        the undo.  ``object_state`` still shows the pre-undo rows.

        cursor:   cursor of the undo's PG transaction (dict rows)
        tid:      int, the tid being undone
        restored: list[int], zoids whose previous revision will be written
        deleted:  list[int], zoids whose rows will be deleted (created in tid)
        entries:  dict[int, dict], per zoid of both lists:
                  ``{"action": "restore" | "delete", "before": rev,
                  "after": rev or None}``, where *before* is the revision
                  the undone tid wrote and *after* the one the undo writes
                  (None for "delete"); each rev is a dict with
                  ``class_mod``, ``class_name``, ``state`` (decoded JSON)
                  and ``refs`` (list[int]).  Read these instead of
                  ``object_state.state``: with several tids in one undo the
                  row may hold another tid's revision.

        Exceptions abort the undo.  Raise ``UndoError`` if the processor
        cannot undo safely.  Zoids changed only by ``finalize()`` without
        being written are not reported.
        """
