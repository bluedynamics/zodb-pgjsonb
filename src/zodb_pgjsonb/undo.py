"""Undo computation for history-preserving storage."""

from .serialization import _unsanitize_from_pg
from ZODB.POSException import ConflictError
from ZODB.POSException import UndoError
from ZODB.utils import p64

import zodb_json_codec


def _delete_entry(zoid):
    """Undo entry deleting *zoid* (object was created in the undone txn)."""
    return {"zoid": zoid, "action": "delete"}


def _restore_entry(zoid, row):
    """Undo entry restoring *zoid* to the revision in *row*."""
    return {
        "zoid": zoid,
        "action": "restore",
        "class_mod": row["class_mod"],
        "class_name": row["class_name"],
        "state": row["state"],
        "state_size": row["state_size"],
        "refs": row["refs"],
    }


def _current_state_for(cur, zoid, pending_entry):
    """Return (state, row) of *zoid* for comparison against the undone state.

    A prior undo in the same transaction may already have queued a write
    for this zoid; then that pending state is authoritative, not the DB.
    """
    if pending_entry is not None:
        return pending_entry.get("state"), pending_entry
    cur.execute(
        "SELECT class_mod, class_name, state FROM object_state WHERE zoid = %s",
        (zoid,),
    )
    row = cur.fetchone()
    return (row["state"] if row else None), row


def _resolve_conflicting_undo(zoid, current_tid, prev, cur_row, storage):
    """States genuinely differ — attempt conflict resolution.

    Returns a restore entry with the resolved state, or raises UndoError.
    """
    if prev is None:
        # Object was created in undone txn, but modified later
        raise UndoError(
            f"Can't undo creation of object {zoid:#x}: modified in later transaction"
        )

    # Encode pre-undo state as pickle for conflict resolution
    pre_undo_record = {
        "@cls": [prev["class_mod"], prev["class_name"]],
        "@s": _unsanitize_from_pg(prev["state"]),
    }
    pre_undo_data = zodb_json_codec.encode_zodb_record(pre_undo_record)

    # Get current state as pickle
    current_record = {
        "@cls": [cur_row["class_mod"], cur_row["class_name"]],
        "@s": _unsanitize_from_pg(cur_row["state"]),
    }
    current_data = zodb_json_codec.encode_zodb_record(current_record)

    try:
        resolved = storage.tryToResolveConflict(
            p64(zoid),
            p64(current_tid),
            current_data,
            pre_undo_data,
        )
    except ConflictError as err:
        raise UndoError(f"Can't undo: conflict on object {zoid:#x}") from err

    if resolved is None:
        raise UndoError(f"Can't undo: conflict on object {zoid:#x}")

    # Decode resolved pickle back to JSONB
    r_mod, r_name, r_state, r_refs = zodb_json_codec.decode_zodb_record_for_pg_json(
        resolved
    )
    return {
        "zoid": zoid,
        "action": "restore",
        "class_mod": r_mod,
        "class_name": r_name,
        "state": r_state,
        "state_size": len(resolved),
        "refs": r_refs,
    }


def _undo_one_object(cur, obj, tid_int, storage, pending_by_zoid):
    """Compute the undo entry for one object of the undone transaction."""
    zoid = obj["zoid"]

    # Check current version of the object
    cur.execute(
        "SELECT tid FROM object_state WHERE zoid = %s",
        (zoid,),
    )
    current = cur.fetchone()
    current_tid = current["tid"] if current else None

    # Find previous revision (state before the undone transaction)
    cur.execute(
        "SELECT tid, class_mod, class_name, state, "
        "state_size, refs FROM ("
        "  SELECT tid, class_mod, class_name, state, state_size, refs"
        "  FROM object_history WHERE zoid = %s AND tid < %s"
        "  UNION"
        "  SELECT tid, class_mod, class_name, state, state_size, refs"
        "  FROM object_state WHERE zoid = %s AND tid < %s"
        ") sub ORDER BY tid DESC LIMIT 1",
        (zoid, tid_int, zoid, tid_int),
    )
    prev = cur.fetchone()

    if current_tid is None or current_tid == tid_int:
        # Object untouched since the undone transaction — simple undo.
        if prev is None:
            return _delete_entry(zoid)  # created in this txn
        return _restore_entry(zoid, prev)

    # Object was modified after the undone transaction — check if states
    # actually differ before triggering conflict resolution (cascading
    # undos may change TID but preserve the same state).
    pending_entry = pending_by_zoid.get(zoid)
    current_state, cur_row = _current_state_for(cur, zoid, pending_entry)
    if current_state == obj["state"]:
        # States match — cascading undo: the TID changed (from a prior
        # undo) but the data is the same. Treat as simple undo.
        if prev is None:
            return _delete_entry(zoid)
        return _restore_entry(zoid, prev)

    return _resolve_conflicting_undo(zoid, current_tid, prev, cur_row, storage)


def _compute_undo(cur, tid_int, storage, pending=None):
    """Compute undo data for a transaction.

    For each object modified in the undone transaction:
    - If the object's current version matches the undone tid, restore
      the previous revision (or delete if the object was created).
    - If the object was modified after the undone transaction, attempt
      conflict resolution. Raises UndoError if resolution fails.

    Args:
        cur: database cursor
        tid_int: integer tid of the transaction to undo
        storage: storage instance (for conflict resolution)
        pending: list of pending _tmp entries (for multi-undo in same txn)

    Returns:
        List of dicts with 'action' ('restore' or 'delete') and data.
    """
    # Build index of pending writes by zoid for multi-undo
    pending_by_zoid = {}
    if pending:
        for entry in pending:
            pending_by_zoid[entry["zoid"]] = entry
    # Verify transaction exists
    cur.execute(
        "SELECT tid FROM transaction_log WHERE tid = %s",
        (tid_int,),
    )
    if cur.fetchone() is None:
        raise UndoError("Transaction not found")

    # Find all objects modified in that transaction
    cur.execute(
        "SELECT zoid, class_mod, class_name, state, state_size, refs "
        "FROM object_history WHERE tid = %s "
        "UNION "
        "SELECT zoid, class_mod, class_name, state, state_size, refs "
        "FROM object_state WHERE tid = %s",
        (tid_int, tid_int),
    )
    undone_objects = cur.fetchall()

    if not undone_objects:
        raise UndoError("Transaction has no object changes")

    return [
        _undo_one_object(cur, obj, tid_int, storage, pending_by_zoid)
        for obj in undone_objects
    ]
