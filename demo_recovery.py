#!/usr/bin/env python3
"""Interruption-recovery demo for the joint multi-station upload.

The HTTP server cannot be killed mid-request from a shell script in a
deterministic way, so this script drives the *same service layer* the API
uses, against an isolated data directory, and simulates a process restart by
running the startup sweep again on the same on-disk state.

Scenario (all on UTC day 2026-11-12):

  1. A joint upload for stations DEMO_A/B/C is interrupted *after* the group
     metadata and ``pending`` rows were written and one Parquet file landed,
     but before the publication transaction.
  2. The process "restarts": ``startup_sweep`` must remove the whole
     half-finished group (metadata + partial files). Nothing is queryable and
     no station was ever published.
  3. The exact same CSV is re-uploaded; now all three stations publish
     atomically and become queryable.
  4. A normal single-station upload and a merge still work afterwards.

Run:  python3 demo_recovery.py
Optional env: RAIN_DEMO_DIR (defaults to a temp directory).
"""
from __future__ import annotations

import os
import sys
import tempfile
from pathlib import Path

# Make the app importable whether run from the repo root or another cwd.
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
_pydeps = ROOT / ".pydeps"
if _pydeps.exists():
    sys.path.insert(0, str(_pydeps))

# Isolate ALL storage before importing the app so settings pick it up.
demo_dir = Path(os.environ.get("RAIN_DEMO_DIR", tempfile.mkdtemp(prefix="rain-recovery-")))
os.environ["RAIN_DATA_DIR"] = str(demo_dir)
os.environ["RAIN_REAPER_INTERVAL"] = "3600"

from app import db  # noqa: E402
from app.ingestion import hash_tables  # noqa: E402
from app.services import (  # noqa: E402
    _new_batch_id,
    parse_csv,
    query_station_day,
    upload_csv,
    upload_joint_csv,
    merge_station_day,
)
from app.storage import file_manager  # noqa: E402

DAY = "2026-11-12"
STATIONS = ["DEMO_A", "DEMO_B", "DEMO_C"]

JOINT_CSV = (
    "station_id,obs_time,rain_mm\n"
    "DEMO_A,2026-11-12T00:00Z,0.1\n"
    "DEMO_A,2026-11-12T00:01Z,0.2\n"
    "DEMO_B,2026-11-12T00:00Z,0.3\n"
    "DEMO_C,2026-11-12T00:00Z,0.4\n"
    "DEMO_C,2026-11-12T00:01Z,0.5\n"
).encode("utf-8")


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)


def show_visibility(label: str) -> None:
    print(f"  [{label}] query visibility:")
    for st in STATIONS:
        body = query_station_day(st, DAY)
        print(f"    {st}: files={body['file_count']} rows={body['row_count']}")


def main() -> int:
    banner(f"0. initialise fresh storage at {demo_dir}")
    db.init_db()
    file_manager.startup_sweep()
    print("  schema initialised (upload_groups + pending-capable batches)")

    banner("1. simulate a joint upload interrupted during the write phase")
    group_id = _new_batch_id("J")
    written_batch = _new_batch_id("B")
    pending_only_batch = _new_batch_id("B")
    third_batch = _new_batch_id("B")

    # One station's Parquet file really lands on disk, exactly as it would
    # just before the process is killed.
    table, _ = parse_csv(
        ("station_id,obs_time,rain_mm\nDEMO_A,2026-11-12T00:00Z,0.1\n"
         "DEMO_A,2026-11-12T00:01Z,0.2\n").encode(),
        STATIONS[0], DAY, written_batch,
    )
    path, _, _ = file_manager.write_parquet_atomic(
        table, STATIONS[0], DAY, written_batch
    )
    print(f"  group {group_id}: wrote {path.name} for DEMO_A; B and C not yet written")

    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO upload_groups (group_id, day, station_cnt, status) "
            "VALUES (?, ?, 3, 'pending')",
            (group_id, DAY),
        )
        for batch_id, station, file_path, has_file in (
            (written_batch, STATIONS[0], str(path), True),
            (pending_only_batch, STATIONS[1], "", False),
            (third_batch, STATIONS[2], "", False),
        ):
            conn.execute(
                "INSERT INTO batches (batch_id, station_id, day, file_path, row_count, "
                "size_bytes, content_sha256, status, is_merged, group_id) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?)",
                (
                    batch_id, station, DAY, file_path,
                    2 if has_file else 0,
                    path.stat().st_size if has_file else 0,
                    hash_tables([table]) if has_file else "",
                    group_id,
                ),
            )
    print("  ... process is killed BEFORE the publish transaction (simulated)")

    conn = db.connect()
    try:
        pending = conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE status='pending'"
        ).fetchone()["c"]
    finally:
        conn.close()
    print(f"  on-disk state now contains {pending} pending rows and 1 orphan-looking parquet")
    show_visibility("before restart")
    assert path.exists(), "the half-written file should exist pre-sweep"

    banner("2. process restart: startup_sweep discards the interrupted group atomically")
    sweep = file_manager.startup_sweep()
    print(f"  sweep result: {sweep}")
    assert group_id in sweep["interrupted_joint_groups"]
    assert not path.exists(), "partial parquet must be removed"

    conn = db.connect()
    try:
        leftover_batches = conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE group_id = ?", (group_id,)
        ).fetchone()["c"]
        leftover_groups = conn.execute(
            "SELECT COUNT(*) c FROM upload_groups WHERE group_id = ?", (group_id,)
        ).fetchone()["c"]
        pending = conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE status='pending'"
        ).fetchone()["c"]
    finally:
        conn.close()
    assert leftover_batches == leftover_groups == pending == 0
    print("  no pending rows, no group header, no partial files remain")
    show_visibility("after restart")

    banner("3. re-upload the SAME joint CSV: all three stations publish at once")
    result = upload_joint_csv(DAY, JOINT_CSV)
    print(f"  new group_id = {result['group_id']}")
    for info in result["stations"]:
        print(
            f"    {info['station_id']}: batch={info['batch_id']} "
            f"rows={info['row_count']} status={info['status']}"
        )
    assert result["station_count"] == 3
    show_visibility("after re-upload")
    for info in result["stations"]:
        body = query_station_day(info["station_id"], DAY)
        assert body["row_count"] == info["row_count"]
        assert body["batch_ids"] == [info["batch_id"]]

    banner("4. ordinary single-station upload and merge still work")
    single = upload_csv(
        "DEMO_B", DAY,
        ("station_id,obs_time,rain_mm\nDEMO_B,2026-11-12T08:00Z,1.7\n").encode(),
    )
    print(f"  legacy single-station batch: {single['batch_id']}")
    merged = merge_station_day("DEMO_B", DAY)
    print(f"  DEMO_B merge: {merged['merged_batch_id']} from {merged['source_count']} source(s)")
    body = query_station_day("DEMO_B", DAY)
    assert body["row_count"] == 2 and body["file_count"] == 1
    print(f"  DEMO_B final state: files={body['file_count']} rows={body['row_count']}")

    banner("ALL RECOVERY CHECKS PASSED")
    print(f"data directory left intact for inspection: {demo_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
