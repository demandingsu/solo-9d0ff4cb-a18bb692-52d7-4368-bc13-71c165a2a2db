"""Service layer: upload, query and atomic merge.

Concurrency design (single container, in-process guarantees backed by SQLite):

* **Uploads never block reads.** A new Parquet file is written and atomically
  renamed into place first; only then is its metadata row committed. Queries
  select published rows at snapshot time.
* **Merge is all-or-nothing.** The merged file is built in the temp area and
  only renamed into place after all inputs are read and row counts verified.
  The metadata swap (insert merged + supersede sources) happens in one
  ``BEGIN IMMEDIATE`` transaction. On any failure nothing published changes and
  the half-finished file is deleted.
* **Pinned snapshot per merge.** The source file set is selected once and
  recorded in ``merge_locks``; uploads landing while the merge runs get their
  own new files and stay published untouched.
* **One merge per station/day.** A SQLite row lock makes a second concurrent
  merge fail fast with HTTP 409 instead of double-counting files.
* **Old files outlive queries.** Files marked superseded are unlinked only by
  the reaper, once every query snapshot holding them has closed.
"""
from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from . import db
from .errors import CsvValidationError, JointUploadError, MergeConflictError, MergeFailedError, NotFoundError
from .ingestion import (
    COLUMN_ALIASES,
    JOINT_MAX_STATIONS,
    JOINT_MIN_STATIONS,
    hash_tables,
    parse_csv,
    parse_joint_csv,
    table_to_records,
    validate_day,
    validate_station_id,
)
from .storage import FileManager, FileRef, file_manager


def _new_batch_id(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}-{uuid.uuid4().hex[:10]}"


# --------------------------------------------------------------------- upload
def upload_csv(
    station_id: str,
    day: str,
    content: bytes,
    fm: FileManager = file_manager,
) -> dict:
    """Persist one uploaded CSV as an independent, individually retained batch."""
    batch_id = _new_batch_id("B")
    # Parse fully before touching disk: invalid input creates no state.
    table, digest = parse_csv(content, station_id, day, batch_id)

    path, row_count, size_bytes = fm.write_parquet_atomic(
        table, station_id, day, batch_id
    )
    try:
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO batches "
                "(batch_id, station_id, day, file_path, row_count, size_bytes, "
                " content_sha256, status, is_merged) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'published', 0)",
                (
                    batch_id,
                    station_id,
                    day,
                    str(path),
                    row_count,
                    size_bytes,
                    digest,
                ),
            )
    except Exception:
        # Metadata never landed: remove the unreferenced file.
        path.unlink(missing_ok=True)
        raise
    return {
        "batch_id": batch_id,
        "station_id": station_id,
        "day": day,
        "row_count": row_count,
        "size_bytes": size_bytes,
        "content_sha256": digest,
        "status": "published",
        "is_merged": False,
    }


# ------------------------------------------------------------- joint upload
def _publish_joint_group(conn: sqlite3.Connection, group_id: str, expected: int) -> None:
    """Flip every pending row of a joint group to published in one statement.

    Kept as a separate function so tests can simulate a publication failure.
    """
    cur = conn.execute(
        "UPDATE batches SET status = 'published' "
        "WHERE group_id = ? AND status = 'pending'",
        (group_id,),
    )
    if cur.rowcount != expected:
        raise JointUploadError(
            f"expected {expected} pending rows, published {cur.rowcount}"
        )
    conn.execute(
        "UPDATE upload_groups SET status = 'published', "
        "published_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') "
        "WHERE group_id = ?",
        (group_id,),
    )


def _rollback_joint(group_id: str, fm: FileManager) -> None:
    """Remove every trace of a joint group that did not reach publication."""
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT file_path FROM batches WHERE group_id = ?", (group_id,)
        ).fetchall()
    finally:
        conn.close()
    for row in rows:
        if row["file_path"]:
            Path(row["file_path"]).unlink(missing_ok=True)
    with db.transaction() as tx:
        tx.execute("DELETE FROM batches WHERE group_id = ?", (group_id,))
        tx.execute(
            "UPDATE upload_groups SET status = 'failed' WHERE group_id = ?",
            (group_id,),
        )


def upload_joint_csv(
    day: str,
    content: bytes,
    fm: FileManager = file_manager,
) -> dict:
    """Atomically ingest one joint CSV covering 2-10 stations of one UTC day.

    Two-phase publication:

    1. **write phase** – every station gets its own Parquet file plus a
       ``pending`` metadata row. Pending rows are invisible to queries,
       listings and merges, and the per-file writes never take global locks,
       so ordinary uploads/queries/merges keep running.
    2. **publish phase** – a single ``BEGIN IMMEDIATE`` transaction flips all
       rows of the group to ``published`` at once. Before the commit the whole
       group is invisible; afterwards every station is queryable together.

    Any failure, or a process restart during the write phase, leaves no
    station published: the transaction is rolled back, the files are unlinked
    and the client can simply upload again.
    """
    validate_day(day)
    group_id = _new_batch_id("J")

    # First pass: read the CSV to learn the distinct station set. The parser
    # below repeats the validation but also enforces 2-10 stations and that
    # every station has >= 1 row before anything touches disk.
    probe = _distinct_stations(content)
    batch_ids = {station: _new_batch_id("B") for station in probe}
    tables = parse_joint_csv(content, day, batch_ids)  # fully validated here

    try:
        # Persist the group header and all pending rows up front: if the
        # process dies mid-write, startup recovery finds the whole group via
        # upload_groups/batches.group_id and discards it as one unit.
        with db.transaction() as conn:
            conn.execute(
                "INSERT INTO upload_groups (group_id, day, station_cnt, status) "
                "VALUES (?, ?, ?, 'pending')",
                (group_id, day, len(tables)),
            )
            for station, batch_id in batch_ids.items():
                conn.execute(
                    "INSERT INTO batches "
                    "(batch_id, station_id, day, file_path, row_count, size_bytes, "
                    " content_sha256, status, is_merged, group_id) "
                    "VALUES (?, ?, ?, '', 0, 0, '', 'pending', 0, ?)",
                    (batch_id, station, day, group_id),
                )

        # Per-station files, written independently (no global metadata lock).
        # Each file is atomically renamed into its final location but stays
        # unqueryable until the publish transaction flips its status.
        for station, table in tables.items():
            batch_id = batch_ids[station]
            path, row_count, size_bytes = fm.write_parquet_atomic(
                table, station, day, batch_id
            )
            digest = hash_tables([table])
            with db.transaction() as conn:
                conn.execute(
                    "UPDATE batches SET file_path = ?, row_count = ?, size_bytes = ?, "
                    "content_sha256 = ? WHERE batch_id = ? AND status = 'pending'",
                    (str(path), row_count, size_bytes, digest, batch_id),
                )

        # Single atomic publication point for the whole group.
        with db.transaction() as conn:
            _publish_joint_group(conn, group_id, len(tables))
    except Exception as exc:
        _rollback_joint(group_id, fm)
        if isinstance(exc, JointUploadError):
            raise
        raise JointUploadError(
            f"joint upload {group_id} rolled back, no station was published: {exc}"
        ) from exc

    stations_out = []
    for station, table in tables.items():
        batch_id = batch_ids[station]
        path = fm.target_path(station, day, batch_id)
        stations_out.append(
            {
                "station_id": station,
                "batch_id": batch_id,
                "day": day,
                "row_count": table.num_rows,
                "size_bytes": path.stat().st_size,
                "content_sha256": hash_tables([table]),
                "status": "published",
                "is_merged": False,
            }
        )
    return {
        "group_id": group_id,
        "day": day,
        "station_count": len(stations_out),
        "stations": stations_out,
    }


def _distinct_stations(content: bytes) -> list[str]:
    """Read only the station column to size the joint group before parsing.

    Enforces the 2-10 distinct-station rule and station-id syntax without
    touching the time/rain columns (those are fully validated afterwards by
    :func:`parse_joint_csv`).
    """
    try:
        text = content.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError("CSV must be UTF-8 encoded") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise CsvValidationError("empty CSV: header row required") from exc

    normalized = [re.sub(r"[\s_\-]+", "", h.strip().lower()) for h in header]
    aliased = [COLUMN_ALIASES.get(h) for h in normalized]
    if "station_id" not in aliased:
        raise CsvValidationError(
            f"CSV must contain a station column; got header {header!r}"
        )
    station_idx = aliased.index("station_id")

    seen: list[str] = []
    seen_set: set[str] = set()
    for line_no, row in enumerate(reader, start=2):
        if not row or all(not cell.strip() for cell in row):
            continue
        if len(row) < len(header):
            raise CsvValidationError(
                f"line {line_no}: expected {len(header)} columns, got {len(row)}"
            )
        station = row[station_idx].strip()
        validate_station_id(station)
        if station not in seen_set:
            seen_set.add(station)
            seen.append(station)
            if len(seen) > JOINT_MAX_STATIONS:
                raise CsvValidationError(
                    f"joint CSV accepts at most {JOINT_MAX_STATIONS} distinct stations, "
                    f"found extra station {station!r}"
                )
    if len(seen) < JOINT_MIN_STATIONS:
        raise CsvValidationError(
            f"joint CSV must contain at least {JOINT_MIN_STATIONS} distinct stations, "
            f"found {len(seen)}: {seen!r}"
        )
    return seen


# ---------------------------------------------------------------------- query
def query_station_day(
    station_id: str,
    day: str,
    limit: int | None = None,
    fm: FileManager = file_manager,
) -> dict:
    """Read the actual published Parquet files for one station/day.

    One immutable file set is pinned for the whole call, so a merge finishing
    mid-query cannot change what the caller sees.
    """
    validate_station_id(station_id)
    validate_day(day)
    refs, token = fm.open_snapshot(station_id, day)
    try:
        table = fm.read_table(refs)
        records = table_to_records(table)
    finally:
        fm.close_snapshot(refs)

    truncated = False
    if limit is not None and limit >= 0 and len(records) > limit:
        records = records[:limit]
        truncated = True

    return {
        "station_id": station_id,
        "day": day,
        "snapshot_token": token,
        "file_count": len(refs),
        "batch_ids": [r.batch_id for r in refs],
        "row_count": len(records),
        "truncated": truncated,
        "records": records,
    }


# ---------------------------------------------------------------------- merge
def _acquire_merge_lock(
    conn: sqlite3.Connection, station_id: str, day: str, merged_id: str
) -> None:
    try:
        conn.execute(
            "INSERT INTO merge_locks (station_id, day, batch_id) VALUES (?, ?, ?)",
            (station_id, day, merged_id),
        )
    except sqlite3.IntegrityError as exc:
        raise MergeConflictError(
            f"a merge for station {station_id} day {day} is already running"
        ) from exc


def _release_merge_lock(conn: sqlite3.Connection, station_id: str, day: str) -> None:
    conn.execute(
        "DELETE FROM merge_locks WHERE station_id = ? AND day = ?",
        (station_id, day),
    )


def _pin_refs(fm: FileManager, refs: List[FileRef]) -> None:
    with fm._ref_lock:
        for r in refs:
            key = r.path.name
            fm._refs[key] = fm._refs.get(key, 0) + 1


def _unpin_refs(fm: FileManager, refs: List[FileRef]) -> None:
    with fm._ref_lock:
        for r in refs:
            key = r.path.name
            count = fm._refs.get(key, 0)
            if count <= 1:
                fm._refs.pop(key, None)
            else:
                fm._refs[key] = count - 1


def merge_station_day(
    station_id: str,
    day: str,
    fm: FileManager = file_manager,
) -> dict:
    """Compact all currently published small batches into one Parquet file."""
    validate_station_id(station_id)
    validate_day(day)
    merged_id = _new_batch_id("M")

    # Phase 1: take the per station/day lock and pin the exact source set.
    with db.transaction() as conn:
        _acquire_merge_lock(conn, station_id, day, merged_id)
        rows = conn.execute(
            "SELECT batch_id, file_path, row_count, is_merged, created_at "
            "FROM batches "
            "WHERE station_id = ? AND day = ? AND status = 'published' "
            "ORDER BY created_at ASC, rowid ASC",
            (station_id, day),
        ).fetchall()

    if not rows:
        with db.transaction() as conn:
            _release_merge_lock(conn, station_id, day)
        raise NotFoundError(f"no published batches for {station_id} / {day}")

    refs: List[FileRef] = [
        FileRef(
            batch_id=r["batch_id"],
            path=Path(r["file_path"]),
            row_count=r["row_count"],
            is_merged=bool(r["is_merged"]),
            created_at=r["created_at"],
        )
        for r in rows
    ]
    source_ids = [r.batch_id for r in refs]
    expected_rows = sum(r.row_count for r in refs)

    # Phase 2: build the merged file OUTSIDE the published set. Queries and
    # uploads keep working against the existing files while this runs. The
    # source files are pinned so the reaper cannot touch them mid-merge.
    merged_path: Path | None = None
    try:
        _pin_refs(fm, refs)
        try:
            tables = [pq.read_table(r.path) for r in refs]
            merged = pa.concat_tables(tables, promote_options="default")
            if merged.num_rows != expected_rows:
                raise MergeFailedError(
                    f"row count mismatch: sources={expected_rows} "
                    f"merged={merged.num_rows}"
                )
            merged_digest = hash_tables([merged])
            merged_path, merged_rows, merged_size = fm.write_parquet_atomic(
                merged, station_id, day, merged_id
            )
        finally:
            _unpin_refs(fm, refs)
    except (MergeConflictError, NotFoundError):
        raise
    except Exception as exc:
        # Failure before publication: release lock, published set untouched.
        with db.transaction() as conn:
            _release_merge_lock(conn, station_id, day)
        raise MergeFailedError(
            f"merge failed, published set unchanged: {exc}"
        ) from exc

    # Phase 3: atomic publication in one IMMEDIATE transaction. The new file
    # already exists on disk but is unqueryable until this commit lands.
    placeholders = ",".join("?" for _ in source_ids)
    try:
        with db.transaction() as conn:
            # The lock row from phase 1 is still held by this merge.
            conn.execute(
                "INSERT INTO batches "
                "(batch_id, station_id, day, file_path, row_count, size_bytes, "
                " content_sha256, status, is_merged, source_batches) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'published', 1, ?)",
                (
                    merged_id,
                    station_id,
                    day,
                    str(merged_path),
                    merged_rows,
                    merged_size,
                    merged_digest,
                    json.dumps(source_ids),
                ),
            )
            conn.execute(
                f"UPDATE batches SET status = 'superseded' "
                f"WHERE batch_id IN ({placeholders}) AND status = 'published'",
                source_ids,
            )
            # Guard: exactly the pinned sources were superseded. New batches
            # uploaded concurrently are not in source_ids and stay published.
            cur = conn.execute(
                "SELECT COUNT(*) AS c FROM batches "
                f"WHERE batch_id IN ({placeholders}) AND status = 'superseded'",
                source_ids,
            )
            if cur.fetchone()["c"] != len(source_ids):
                raise MergeFailedError("source set changed during merge; aborting")
            _release_merge_lock(conn, station_id, day)
    except Exception as exc:
        # Rollback restores the old published set; drop the unpublished file.
        if merged_path is not None:
            merged_path.unlink(missing_ok=True)
        with db.transaction() as conn:
            _release_merge_lock(conn, station_id, day)
        if isinstance(exc, MergeFailedError):
            raise
        raise MergeFailedError(
            f"merge publication failed, no state changed: {exc}"
        ) from exc

    # Old files are NOT unlinked here: the reaper waits for in-flight queries.
    return {
        "merged_batch_id": merged_id,
        "station_id": station_id,
        "day": day,
        "source_batch_ids": source_ids,
        "source_count": len(source_ids),
        "row_count": merged_rows,
        "size_bytes": merged_size,
        "content_sha256": merged_digest,
        "status": "published",
    }


# --------------------------------------------------------------- introspection
def list_batches(station_id: str, day: str) -> dict:
    validate_station_id(station_id)
    validate_day(day)
    conn = db.connect()
    try:
        rows = conn.execute(
            "SELECT batch_id, file_path, row_count, size_bytes, status, is_merged, "
            "source_batches, created_at FROM batches "
            "WHERE station_id = ? AND day = ? AND status != 'pending' "
            "ORDER BY created_at ASC, rowid ASC",
            (station_id, day),
        ).fetchall()
    finally:
        conn.close()
    return {
        "station_id": station_id,
        "day": day,
        "batches": [
            {
                "batch_id": r["batch_id"],
                "row_count": r["row_count"],
                "size_bytes": r["size_bytes"],
                "status": r["status"],
                "is_merged": bool(r["is_merged"]),
                "source_batches": json.loads(r["source_batches"])
                if r["source_batches"]
                else None,
                "file_exists": Path(r["file_path"]).exists(),
                "created_at": r["created_at"],
            }
            for r in rows
        ],
    }


def force_reap(fm: FileManager = file_manager) -> dict:
    """Admin hook used by tests/demos; the background reaper normally does this."""
    return {"reaped_batch_ids": fm.reap_once()}
