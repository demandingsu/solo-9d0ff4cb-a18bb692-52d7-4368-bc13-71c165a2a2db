"""Joint multi-station upload: atomic publication, invisibility, recovery."""
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from app import db, services
from app.storage import file_manager
from tests.conftest import make_csv


DAY = "2026-11-12"


def joint_csv(rows):
    """rows: list of (station, ts, rain)."""
    lines = ["station_id,obs_time,rain_mm"]
    lines.extend(f"{s},{ts},{r}" for s, ts, r in rows)
    return ("\n".join(lines) + "\n").encode("utf-8")


def three_station_rows(prefix="JT"):
    return [
        (f"{prefix}A", "2026-11-12T00:00Z", 0.1),
        (f"{prefix}A", "2026-11-12T00:01Z", 0.2),
        (f"{prefix}B", "2026-11-12T00:00Z", 0.3),
        (f"{prefix}C", "2026-11-12T00:00Z", 0.4),
        (f"{prefix}C", "2026-11-12T00:01Z", 0.5),
        (f"{prefix}C", "2026-11-12T00:02Z", 0.6),
    ]


def _joint_upload(client, rows, day=DAY):
    r = client.post(
        f"/days/{day}/joint-batches",
        files={"file": ("joint.csv", joint_csv(rows), "text/csv")},
    )
    return r


# --------------------------------------------------------------------- happy
def test_joint_upload_publishes_all_stations_at_once(client):
    rows = three_station_rows("H1")
    r = _joint_upload(client, rows)
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["group_id"].startswith("J-")
    assert body["station_count"] == 3
    stations = {s["station_id"]: s for s in body["stations"]}
    assert set(stations) == {"H1A", "H1B", "H1C"}
    assert stations["H1A"]["row_count"] == 2
    assert stations["H1B"]["row_count"] == 1
    assert stations["H1C"]["row_count"] == 3
    assert all(s["status"] == "published" for s in body["stations"])
    # Per-station batch ids are unique and independent Parquet files.
    batch_ids = [s["batch_id"] for s in body["stations"]]
    assert len(set(batch_ids)) == 3
    for station, info in stations.items():
        path = file_manager.target_path(station, DAY, info["batch_id"])
        assert path.exists()

    # After publication every station is queryable through the normal API.
    for station, info in stations.items():
        q = client.get(f"/stations/{station}/days/{DAY}/records").json()
        assert q["row_count"] == info["row_count"]
        assert q["batch_ids"] == [info["batch_id"]]
        listing = client.get(f"/stations/{station}/days/{DAY}/batches").json()
        assert [b["batch_id"] for b in listing["batches"]] == [info["batch_id"]]
        assert listing["batches"][0]["status"] == "published"


def test_joint_upload_sample_file_works(client):
    sample = (
        Path(__file__).resolve().parents[1]
        / "samples" / "joint_st001_st002_st003_2026-11-12.csv"
    )
    r = client.post(
        f"/days/{DAY}/joint-batches",
        files={"file": ("joint.csv", sample.read_bytes(), "text/csv")},
    )
    assert r.status_code == 201, r.text
    assert r.json()["station_count"] == 3


# --------------------------------------------------------------- validation
def test_joint_upload_rejects_single_station(client):
    r = _joint_upload(
        client,
        [("V1A", "2026-11-12T00:00Z", 1.0), ("V1A", "2026-11-12T00:01Z", 2.0)],
    )
    assert r.status_code == 400
    assert "CsvValidationError" in r.text


def test_joint_upload_rejects_blank_station_id(client):
    # Station ids come from the rows; an invalid id fails the same syntax
    # check as the single-station endpoint.
    bad = [
        ("bad id", "2026-11-12T00:00Z", 0.3),
        ("V2B", "2026-11-12T00:00Z", 0.4),
    ]
    r = _joint_upload(client, bad)
    assert r.status_code == 400
    assert "CsvValidationError" in r.text


def test_joint_upload_rejects_eleven_stations(client):
    rows = [(f"S{i:02d}", "2026-11-12T00:00Z", 1.0) for i in range(11)]
    r = _joint_upload(client, rows)
    assert r.status_code == 400
    assert "at most 10" in r.text


def test_joint_upload_reuses_time_and_rain_validation(client):
    bad = three_station_rows("V3")
    bad[0] = ("V3A", "2026-11-11T23:59Z", 0.1)  # wrong UTC day
    r = _joint_upload(client, bad)
    assert r.status_code == 400 and "CsvValidationError" in r.text

    bad = three_station_rows("V4")
    bad[1] = ("V4A", "2026-11-12T00:01Z", -0.5)  # negative rain
    r = _joint_upload(client, bad)
    assert r.status_code == 400 and "CsvValidationError" in r.text

    # Validation failure creates no visible state: no batch rows for the
    # rejected stations and no pending group left behind.
    conn = db.connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM upload_groups WHERE status = 'pending'"
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE station_id IN ('V3A','V3B','V3C','V4A','V4B','V4C')"
        ).fetchone()["c"] == 0
    finally:
        conn.close()


# ------------------------------------------------------------- invisibility
def test_pending_group_is_invisible_to_query_and_merge(client):
    rows = three_station_rows("PV")
    original_write = file_manager.write_parquet_atomic
    seen_while_writing = {}
    first_released = threading.Event()

    def observing_write(table, station, day, batch_id):
        result = original_write(table, station, day, batch_id)
        # While the group is still in the write phase, nothing is visible.
        conn = db.connect()
        try:
            pending = conn.execute(
                "SELECT COUNT(*) c FROM batches WHERE status='pending'"
            ).fetchone()["c"]
            published = conn.execute(
                "SELECT COUNT(*) c FROM batches WHERE station_id = ? AND day = ? "
                "AND status = 'published'",
                (station, day),
            ).fetchone()["c"]
        finally:
            conn.close()
        seen_while_writing[batch_id] = (pending, published)
        q = services.query_station_day(station, day)
        assert q["file_count"] == 0 and q["records"] == []
        listing = services.list_batches(station, day)
        assert listing["batches"] == []
        # A merge starting during the write phase finds nothing to merge.
        with pytest.raises(services.NotFoundError):
            services.merge_station_day(station, day)
        first_released.set()
        return result

    file_manager.write_parquet_atomic = observing_write
    try:
        r = _joint_upload(client, rows)
    finally:
        file_manager.write_parquet_atomic = original_write
    assert r.status_code == 201, r.text
    assert len(seen_while_writing) == 3
    for pending, published in seen_while_writing.values():
        assert pending >= 1 and published == 0


def test_merge_started_before_publication_does_not_swallow_new_group(client):
    station_a, station_b, station_c = "MGA", "MGB", "MGC"
    # Pre-existing published data for station A; merge begins and blocks while
    # the joint group is being written.
    services.upload_csv(
        station_a, DAY, make_csv([("2026-11-12T03:00Z", 7.7)], station_a)
    )
    original_write = file_manager.write_parquet_atomic
    merge_selected = threading.Event()
    allow_merge_finish = threading.Event()

    def blocking_merge_write(table, station, day, batch_id):
        if batch_id.startswith("M"):
            merge_selected.set()
            allow_merge_finish.wait(timeout=15)
        return original_write(table, station, day, batch_id)

    file_manager.write_parquet_atomic = blocking_merge_write
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            merge_future = pool.submit(services.merge_station_day, station_a, DAY)
            assert merge_selected.wait(timeout=10)

            joint = _joint_upload(
                client,
                [
                    (station_a, "2026-11-12T00:00Z", 0.1),
                    (station_b, "2026-11-12T00:00Z", 0.2),
                    (station_c, "2026-11-12T00:00Z", 0.3),
                ],
            )
            assert joint.status_code == 201, joint.text
            allow_merge_finish.set()
            merged = merge_future.result(timeout=15)
    finally:
        file_manager.write_parquet_atomic = original_write

    group_station_a = next(
        s for s in joint.json()["stations"] if s["station_id"] == station_a
    )
    # The in-flight merge only compacted the pre-existing batch.
    assert merged["source_count"] == 1
    assert group_station_a["batch_id"] not in merged["source_batch_ids"]

    body = services.query_station_day(station_a, DAY)
    assert group_station_a["batch_id"] in body["batch_ids"]
    assert merged["merged_batch_id"] in body["batch_ids"]
    assert body["row_count"] == 2  # old merged row + joint row, neither lost
    for station in (station_b, station_c):
        q = services.query_station_day(station, DAY)
        assert q["row_count"] == 1


# ----------------------------------------------------------------- failures
def test_write_failure_rolls_back_entire_group(client):
    rows = three_station_rows("RF")
    calls = {"n": 0}
    original_write = file_manager.write_parquet_atomic

    def fail_second_write(table, station, day, batch_id):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("injected disk failure")
        return original_write(table, station, day, batch_id)

    file_manager.write_parquet_atomic = fail_second_write
    try:
        r = _joint_upload(client, rows)
    finally:
        file_manager.write_parquet_atomic = original_write
    assert r.status_code == 500
    assert "JointUploadError" in r.text

    for station, _, _ in rows:
        q = client.get(f"/stations/{station}/days/{DAY}/records").json()
        assert q["records"] == [] and q["file_count"] == 0
        assert not file_manager.station_day_dir(station, DAY).exists() or not list(
            file_manager.station_day_dir(station, DAY).glob("*.parquet")
        )
    conn = db.connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE status='pending'"
        ).fetchone()["c"] == 0
        # The group created by this call is marked failed (its batch rows gone).
        failed_here = conn.execute(
            "SELECT COUNT(*) c FROM upload_groups WHERE status = 'failed' "
            "AND group_id NOT IN (SELECT group_id FROM batches)"
        ).fetchall()
        assert len(failed_here) >= 1
    finally:
        conn.close()

    # The same CSV can be re-uploaded successfully after the failure.
    retry = _joint_upload(client, rows)
    assert retry.status_code == 201, retry.text
    assert retry.json()["station_count"] == 3


def test_publish_failure_rolls_back_entire_group(client):
    rows = three_station_rows("PF")
    real_publish = services._publish_joint_group

    class FakeError(RuntimeError):
        pass

    def boom(conn, group_id, expected):
        raise FakeError("injected publication failure")

    services._publish_joint_group = boom
    try:
        r = _joint_upload(client, rows)
    finally:
        services._publish_joint_group = real_publish
    assert r.status_code == 500
    assert "JointUploadError" in r.text

    for station, _, _ in rows:
        q = client.get(f"/stations/{station}/days/{DAY}/records").json()
        assert q["records"] == []
    conn = db.connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE status='pending'"
        ).fetchone()["c"] == 0
    finally:
        conn.close()
    # Retry succeeds.
    retry = _joint_upload(client, rows)
    assert retry.status_code == 201, retry.text


# ------------------------------------------------------------- crash recovery
def test_startup_sweep_discards_interrupted_group_and_allows_reupload():
    stations = ["CRA", "CRB", "CRC"]
    # Simulate a process restart mid-write: group header + one pending row with
    # a file on disk, another pending row without a file.
    group_id = services._new_batch_id("J")
    written_batch = services._new_batch_id("B")
    missing_batch = services._new_batch_id("B")
    third_batch = services._new_batch_id("B")

    table, _ = services.parse_csv(
        make_csv([("2026-11-12T00:00Z", 1.0)], stations[0]),
        stations[0], DAY, written_batch,
    )
    path, _, _ = file_manager.write_parquet_atomic(
        table, stations[0], DAY, written_batch
    )
    assert path.exists()

    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO upload_groups (group_id, day, station_cnt, status) "
            "VALUES (?, ?, 3, 'pending')",
            (group_id, DAY),
        )
        conn.execute(
            "INSERT INTO batches (batch_id, station_id, day, file_path, row_count, "
            "size_bytes, content_sha256, status, is_merged, group_id) "
            "VALUES (?, ?, ?, ?, 1, 1, 'x', 'pending', 0, ?)",
            (written_batch, stations[0], DAY, str(path), group_id),
        )
        conn.execute(
            "INSERT INTO batches (batch_id, station_id, day, file_path, row_count, "
            "size_bytes, content_sha256, status, is_merged, group_id) "
            "VALUES (?, ?, ?, '', 0, 0, '', 'pending', 0, ?)",
            (missing_batch, stations[1], DAY, group_id),
        )
        conn.execute(
            "INSERT INTO batches (batch_id, station_id, day, file_path, row_count, "
            "size_bytes, content_sha256, status, is_merged, group_id) "
            "VALUES (?, ?, ?, '', 0, 0, '', 'pending', 0, ?)",
            (third_batch, stations[2], DAY, group_id),
        )

    sweep = file_manager.startup_sweep()
    assert group_id in sweep["interrupted_joint_groups"]
    assert not path.exists()

    conn = db.connect()
    try:
        assert conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE group_id = ?", (group_id,)
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM upload_groups WHERE group_id = ?", (group_id,)
        ).fetchone()["c"] == 0
        assert conn.execute(
            "SELECT COUNT(*) c FROM batches WHERE status='pending'"
        ).fetchone()["c"] == 0
    finally:
        conn.close()
    for station in stations:
        assert services.query_station_day(station, DAY)["records"] == []

    # Re-upload the same logical CSV after restart: works, all stations visible.
    result = services.upload_joint_csv(
        DAY,
        joint_csv(
            [
                (stations[0], "2026-11-12T00:00Z", 1.0),
                (stations[1], "2026-11-12T00:00Z", 2.0),
                (stations[2], "2026-11-12T00:00Z", 3.0),
            ]
        ),
    )
    assert result["station_count"] == 3
    for info in result["stations"]:
        q = services.query_station_day(info["station_id"], DAY)
        assert q["row_count"] == info["row_count"] == 1


def test_ordinary_traffic_keeps_running_during_joint_write_phase(client):
    rows = three_station_rows("CW")
    original_write = file_manager.write_parquet_atomic
    entered = threading.Event()

    def slow_write(table, station, day, batch_id):
        entered.set()
        threading.Event().wait(0.05)  # widen the write-phase window
        return original_write(table, station, day, batch_id)

    file_manager.write_parquet_atomic = slow_write
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            joint_future = pool.submit(_joint_upload, client, rows)
            assert entered.wait(timeout=10)
            # A regular single-station upload for another station completes
            # while the joint group is still being written.
            regular = client.post(
                "/stations/CWZ/days/2026-11-12/batches",
                files={"file": ("r.csv", make_csv([("2026-11-12T00:00Z", 0.8)], "CWZ"), "text/csv")},
            )
            # And a query against an unrelated station never blocks/fails.
            q = client.get("/stations/CWZ/days/2026-11-12/records")
            joint = joint_future.result(timeout=15)
    finally:
        file_manager.write_parquet_atomic = original_write

    assert regular.status_code == 201
    assert q.json()["row_count"] == 1
    assert joint.status_code == 201, joint.text
    # After publication the regular batch and the whole joint group coexist.
    assert client.get("/stations/CWZ/days/2026-11-12/records").json()["row_count"] == 1


def test_single_station_endpoint_still_works_alongside_joint(client):
    # Existing entry point untouched.
    r = client.post(
        "/stations/SS1/days/2026-11-12/batches",
        files={"file": ("s.csv", make_csv([("2026-11-12T00:00Z", 0.9)], "SS1"), "text/csv")},
    )
    assert r.status_code == 201
    joint = _joint_upload(
        client,
        [
            ("SS2", "2026-11-12T00:00Z", 0.1),
            ("SS3", "2026-11-12T00:00Z", 0.2),
        ],
    )
    assert joint.status_code == 201
    assert client.get("/stations/SS1/days/2026-11-12/records").json()["row_count"] == 1
    assert client.get("/stations/SS2/days/2026-11-12/records").json()["row_count"] == 1


def test_joint_upload_empty_and_oversized_rejected(client):
    r = client.post(
        f"/days/{DAY}/joint-batches",
        files={"file": ("e.csv", b"", "text/csv")},
    )
    assert r.status_code == 400

    big = b"station_id,obs_time,rain_mm\n" + b"X,2026-11-12T00:00Z,1\n" * 10
    r = client.post(
        f"/days/{DAY}/joint-batches",
        files={"file": ("b.csv", big, "text/csv")},
    )
    assert r.status_code in (400, 413)
