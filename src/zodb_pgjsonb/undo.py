"""Undo computation for history-preserving storage."""

from .serialization import _unsanitize_from_pg
from ZODB.POSException import ConflictError
from ZODB.POSException import UndoError
from ZODB.utils import p64

import zodb_json_codec


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

    undo_data = []
    for obj in undone_objects:
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

        # Check if there's a pending write for this zoid (multi-undo)
        pending_entry = pending_by_zoid.get(zoid)

        if current_tid is not None and current_tid != tid_int:
            # Object was modified after the undone transaction —
            # check if states actually differ before triggering
            # conflict resolution (cascading undos may change TID
            # but preserve the same state).

            undone_state = obj["state"]
            cur_row = None

            if pending_entry is not None:
                # A prior undo in this same transaction already
                # queued a write for this zoid. Use that pending
                # state for comparison instead of the DB state.
                current_state = pending_entry.get("state")
                cur_row = pending_entry
            else:
                # Get current state from object_state
                cur.execute(
                    "SELECT class_mod, class_name, state "
                    "FROM object_state WHERE zoid = %s",
                    (zoid,),
                )
                cur_row = cur.fetchone()
                current_state = cur_row["state"] if cur_row else None

            if current_state == undone_state and current is not None:
                # States match — this is a cascading undo scenario.
                # The TID changed (from a prior undo) but data is the same.
                # Treat as simple undo: restore previous revision.
                if prev is None:
                    undo_data.append(
                        {
                            "zoid": zoid,
                            "action": "delete",
                        }
                    )
                else:
                    undo_data.append(
                        {
                            "zoid": zoid,
                            "action": "restore",
                            "class_mod": prev["class_mod"],
                            "class_name": prev["class_name"],
                            "state": prev["state"],
                            "state_size": prev["state_size"],
                            "refs": prev["refs"],
                        }
                    )
            else:
                # States genuinely differ — requires conflict resolution
                oid_bytes = p64(zoid)

                # Get the state we want to restore (pre-undo)
                if prev is None:
                    # Object was created in undone txn, but modified later
                    raise UndoError(
                        f"Can't undo creation of object {zoid:#x}: "
                        f"modified in later transaction"
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

                # Try conflict resolution
                try:
                    resolved = storage.tryToResolveConflict(
                        oid_bytes,
                        p64(current_tid),
                        current_data,
                        pre_undo_data,
                    )
                except ConflictError as err:
                    raise UndoError(
                        f"Can't undo: conflict on object {zoid:#x}"
                    ) from err

                if resolved is None:
                    raise UndoError(f"Can't undo: conflict on object {zoid:#x}")

                # Decode resolved pickle back to JSONB
                r_mod, r_name, r_state, r_refs = (
                    zodb_json_codec.decode_zodb_record_for_pg_json(resolved)
                )
                undo_data.append(
                    {
                        "zoid": zoid,
                        "action": "restore",
                        "class_mod": r_mod,
                        "class_name": r_name,
                        "state": r_state,
                        "state_size": len(resolved),
                        "refs": r_refs,
                    }
                )
        elif prev is None:
            # Object was created in this txn — delete it
            undo_data.append(
                {
                    "zoid": zoid,
                    "action": "delete",
                }
            )
        else:
            # Simple undo: restore previous revision
            undo_data.append(
                {
                    "zoid": zoid,
                    "action": "restore",
                    "class_mod": prev["class_mod"],
                    "class_name": prev["class_name"],
                    "state": prev["state"],
                    "state_size": prev["state_size"],
                    "refs": prev["refs"],
                }
            )

    return undo_data
