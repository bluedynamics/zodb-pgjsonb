"""Conflict detection and resolution for batch commits."""

from ZODB.POSException import ConflictError
from ZODB.POSException import ReadConflictError
from ZODB.utils import p64
from ZODB.utils import u64

import zodb_json_codec


def _batch_resolve_conflicts(cur, tmp, resolved, storage):
    """Check all queued stores for write conflicts in a single round trip.

    Entries with ``_serial`` are checked: if the committed TID differs from
    the expected serial, conflict resolution is attempted.  Resolved entries
    are updated in-place with re-decoded state.
    """
    conflict_entries = [e for e in tmp if "_serial" in e]
    if not conflict_entries:
        return

    check_zoids = [e["zoid"] for e in conflict_entries]
    cur.execute(
        "SELECT zoid, tid FROM object_state WHERE zoid = ANY(%s)",
        (check_zoids,),
        prepare=True,
    )
    committed = {row["zoid"]: row["tid"] for row in cur.fetchall()}

    for entry in conflict_entries:
        zoid = entry["zoid"]
        if zoid not in committed:
            continue  # New object, no conflict possible
        committed_tid = p64(committed[zoid])
        if committed_tid == entry["_serial"]:
            continue  # Serial matches, no conflict

        # Conflict detected — attempt resolution
        oid = entry["_oid"]
        committed_data = storage.loadSerial(oid, committed_tid)
        result = storage.tryToResolveConflict(
            oid,
            committed_tid,
            entry["_serial"],
            entry["_data"],
            committedData=committed_data,
        )
        if result:
            # Re-decode resolved pickle and update entry in-place
            class_mod, class_name, state, refs = (
                zodb_json_codec.decode_zodb_record_for_pg_json(result)
            )
            entry["class_mod"] = class_mod
            entry["class_name"] = class_name
            entry["state"] = state
            entry["state_size"] = len(result)
            entry["refs"] = refs
            # Re-process state for catalog/extra columns
            if hasattr(storage, "_main"):
                extra = storage._main._process_state(zoid, class_mod, class_name, state)
            else:
                extra = storage._process_state(zoid, class_mod, class_name, state)
            if extra:
                entry["_extra"] = extra
            resolved.append(oid)
        else:
            raise ConflictError(oid=oid, serials=(committed_tid, entry["_serial"]))

    # Clean up conflict detection data from entries
    for entry in conflict_entries:
        entry.pop("_oid", None)
        entry.pop("_serial", None)
        entry.pop("_data", None)


def _batch_check_read_conflicts(cur, read_conflicts):
    """Check all queued read-conflict checks in a single round trip."""
    if not read_conflicts:
        return

    zoids = [u64(oid) for oid, _ in read_conflicts]
    serial_map = {u64(oid): serial for oid, serial in read_conflicts}
    cur.execute(
        "SELECT zoid, tid FROM object_state WHERE zoid = ANY(%s)",
        (zoids,),
        prepare=True,
    )
    for row in cur.fetchall():
        zoid = row["zoid"]
        current_serial = p64(row["tid"])
        expected = serial_map[zoid]
        if current_serial != expected:
            raise ReadConflictError(
                oid=p64(zoid),
                serials=(current_serial, expected),
            )
