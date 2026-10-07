"""Batch write functions for PG transaction commits."""

from .serialization import _serialize_extension
from psycopg.types.json import Json

import contextlib
import os


def _write_txn_log(cur, tid_int, user, desc, ext, idempotent=False):
    """Write a transaction log entry.

    When *idempotent* is True, silently skips if the TID already exists
    (used for resuming interrupted parallel imports).
    """
    conflict_clause = " ON CONFLICT (tid) DO NOTHING" if idempotent else ""
    cur.execute(
        "INSERT INTO transaction_log "
        "(tid, username, description, extension) "
        f"VALUES (%s, %s, %s, %s){conflict_clause}",
        (tid_int, user, desc, _serialize_extension(ext)),
    )


_BASE_COLS = [
    "zoid",
    "tid",
    "class_mod",
    "class_name",
    "state",
    "state_size",
    "refs",
]
_BASE_VALS = [f"%({c})s" for c in _BASE_COLS]


def _base_params(obj, tid_int):
    return {
        "zoid": obj["zoid"],
        "tid": tid_int,
        "class_mod": obj["class_mod"],
        "class_name": obj["class_name"],
        "state": (
            Json(obj["state"], dumps=lambda s: s)
            if isinstance(obj["state"], str)
            else Json(obj["state"])
        ),
        "state_size": obj["state_size"],
        "refs": obj["refs"],
    }


def _extra_columns_for(answers, processor_columns):
    """Return ``(columns, values)`` to write for one object.

    *answers* maps processor position to the dict its ``process()``
    returned.  Every column of an answering processor is written, keys
    missing from its dict as NULL.  Columns of processors that did not
    answer are left out, so ON CONFLICT keeps their stored values.  If two
    processors declare the same column, the later one wins.
    """
    columns = {}
    values = {}
    for pos in sorted(answers):
        result = answers[pos]
        for ec in processor_columns[pos]:
            columns[ec.name] = ec
            values[ec.name] = result.get(ec.name)
    return tuple(columns.values()), values


def _upsert_group(cur, columns, params_list):
    """Upsert *params_list* with the base columns plus *columns*."""
    cols = _BASE_COLS + [ec.name for ec in columns]
    vals = _BASE_VALS + [ec.value_expr for ec in columns]
    update_parts = [f"{c} = EXCLUDED.{c}" for c in _BASE_COLS[1:]]
    update_parts += [
        f"{ec.name} = {ec.update_expr or f'EXCLUDED.{ec.name}'}" for ec in columns
    ]
    cur.executemany(
        f"INSERT INTO object_state ({', '.join(cols)}) "
        f"VALUES ({', '.join(vals)}) "
        f"ON CONFLICT (zoid) DO UPDATE SET {', '.join(update_parts)}",
        params_list,
    )


def _batch_write_objects(
    cur, objects, tid_int, history_preserving=False, processor_columns=None
):
    """Write multiple objects in batch using executemany (pipelined).

    psycopg3's executemany() automatically uses pipeline mode, sending
    all statements in a single network round-trip instead of waiting for
    each individual result.

    *processor_columns* (from ``_get_processor_columns()``) lists the
    extra columns per state processor.  Each object's ``_extra`` (from
    ``_process_state()``) says which processors answered; only their
    columns are written for that object.  Objects are grouped by the
    resulting column set, one executemany per group.  History tables
    always use the base columns only.
    """
    # SECURITY NOTE: Table names (object_state, object_history) are string
    # constants, not user input.  If table names are ever made configurable,
    # use psycopg.sql.Identifier() to prevent SQL injection.
    if not objects:
        return

    # One row per zoid, the last queued entry wins (as with the former single
    # executemany); grouping below would otherwise reorder duplicates.
    objects = list({obj["zoid"]: obj for obj in objects}.values())

    groups = {}  # column names -> (columns, params_list)
    for obj in objects:
        params = _base_params(obj, tid_int)
        answers = obj.get("_extra")
        columns = ()
        if answers and processor_columns:
            columns, values = _extra_columns_for(answers, processor_columns)
            params.update(values)
        key = tuple(ec.name for ec in columns)
        groups.setdefault(key, (columns, []))[1].append(params)

    # ── History: preserve old versions before overwrite ──────────
    if history_preserving:
        zoid_list = [obj["zoid"] for obj in objects]
        cur.execute(
            "INSERT INTO object_history "
            "(zoid, tid, class_mod, class_name, state, state_size, refs) "
            "SELECT zoid, tid, class_mod, class_name, state, state_size, refs "
            "FROM object_state WHERE zoid = ANY(%s) "
            "ON CONFLICT (zoid, tid) DO NOTHING",
            (zoid_list,),
        )

    for columns, params_list in groups.values():
        _upsert_group(cur, columns, params_list)


def _batch_delete_objects(cur, zoids, tid_int, history_preserving=False):
    """Delete multiple objects in batch.

    In history-preserving mode, records tombstones with state=NULL.
    Removes objects from object_state (current state table).
    """
    if not zoids:
        return
    if history_preserving:
        # Preserve pre-delete state in history
        zoid_list = list(zoids)
        cur.execute(
            "INSERT INTO object_history "
            "(zoid, tid, class_mod, class_name, state, state_size, refs) "
            "SELECT zoid, tid, class_mod, class_name, state, state_size, refs "
            "FROM object_state WHERE zoid = ANY(%s) "
            "ON CONFLICT (zoid, tid) DO NOTHING",
            (zoid_list,),
        )
        # Record tombstone (state=NULL) marking deletion at this tid
        cur.executemany(
            "INSERT INTO object_history "
            "(zoid, tid, class_mod, class_name, state, state_size, refs) "
            "VALUES (%s, %s, '', '', NULL, 0, '{}')",
            [(zoid, tid_int) for zoid in zoids],
        )
    cur.executemany(
        "DELETE FROM object_state WHERE zoid = %s",
        [(zoid,) for zoid in zoids],
    )


def _batch_write_blobs(
    cur,
    blobs,
    tid_int,
    history_preserving=False,
    blob_sink=None,
    blob_threshold=102_400,
):
    """Write multiple blobs in batch with optional S3 tiering.

    When blob_sink is set, blobs >= blob_threshold are submitted to
    the sink and only metadata (s3_key, no data) is stored in PG.
    Smaller blobs go directly to PG bytea.
    """
    if not blobs:
        return

    pg_params = []  # (zoid, tid, size, data) — PG bytea blobs
    s3_params = []  # (zoid, tid, size, s3_key) — S3 metadata-only rows
    s3_uploads = []  # (blob_path, s3_key, zoid, size, cleanup) — pending sink submissions
    pg_temp_paths = []  # PG bytea temp paths to unlink immediately

    for blob_entry in blobs:
        # Support both (zoid, path) and (zoid, path, is_temp) tuples.
        if len(blob_entry) == 3:
            zoid, blob_path, is_temp = blob_entry
        else:
            zoid, blob_path = blob_entry
            is_temp = True
        size = os.path.getsize(blob_path)
        if blob_sink is not None and size >= blob_threshold:
            # Large blob → S3 via sink (cleanup ownership transfers to sink)
            s3_key = f"blobs/{zoid:016x}/{tid_int:016x}.blob"
            cleanup = blob_path if is_temp else None
            s3_uploads.append((blob_path, s3_key, zoid, size, cleanup))
            s3_params.append((zoid, tid_int, size, s3_key))
        else:
            # Small blob or no sink → PG bytea
            with open(blob_path, "rb") as f:
                blob_data = f.read()
            pg_params.append((zoid, tid_int, size, blob_data))
            if is_temp:
                pg_temp_paths.append(blob_path)

    # Submit S3 blobs to the sink
    for blob_path, s3_key, zoid, size, cleanup in s3_uploads:
        blob_sink.submit(blob_path, s3_key, zoid, size, cleanup_path=cleanup)

    # Clean up PG bytea temp files (S3 temps are owned by the sink now)
    for path in pg_temp_paths:
        os.unlink(path)

    # Batch insert PG bytea blobs (blob_state keeps all versions via PK (zoid, tid))
    if pg_params:
        cur.executemany(
            "INSERT INTO blob_state (zoid, tid, blob_size, data) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (zoid, tid) DO UPDATE SET "
            "blob_size = EXCLUDED.blob_size, data = EXCLUDED.data",
            pg_params,
        )

    # Batch insert S3 metadata (data=NULL, s3_key=key)
    if s3_params:
        cur.executemany(
            "INSERT INTO blob_state "
            "(zoid, tid, blob_size, s3_key) "
            "VALUES (%s, %s, %s, %s) "
            "ON CONFLICT (zoid, tid) DO UPDATE SET "
            "blob_size = EXCLUDED.blob_size, "
            "s3_key = EXCLUDED.s3_key, data = NULL",
            s3_params,
        )


def _write_prepared_transaction(
    conn,
    txn_data,
    history_preserving,
    processor_columns,
    processors,
    blob_sink=None,
    blob_threshold=102_400,
    idempotent=False,
):
    """Write a pre-decoded transaction directly to PG.

    Bypasses the TPC protocol and advisory lock — intended only for
    parallel migration where the caller guarantees OID ordering.

    When *idempotent* is True, duplicate TIDs and OIDs are silently
    skipped (used for resuming interrupted parallel imports).

    *conn* must have ``autocommit=True`` (the default for pool conns).
    """
    tid_int = txn_data["tid_int"]

    conn.execute("BEGIN")
    try:
        with conn.cursor() as cur:
            _write_txn_log(
                cur,
                tid_int,
                txn_data["user"],
                txn_data["description"],
                txn_data["extension"],
                idempotent=idempotent,
            )
            _batch_write_objects(
                cur,
                txn_data["objects"],
                tid_int,
                history_preserving,
                processor_columns=processor_columns,
            )
            _batch_write_blobs(
                cur,
                txn_data["blobs"],
                tid_int,
                history_preserving,
                blob_sink=blob_sink,
                blob_threshold=blob_threshold,
            )
            for proc in processors:
                if hasattr(proc, "finalize"):
                    proc.finalize(cur)
        conn.execute("COMMIT")
    except BaseException:
        with contextlib.suppress(Exception):
            conn.execute("ROLLBACK")
        raise
