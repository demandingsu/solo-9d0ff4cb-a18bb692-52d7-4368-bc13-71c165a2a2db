"""SQLite access primitives.

The metadata database is the *source of truth* for which Parquet files are
"published". Only rows committed here are visible to queries.

SQLite runs in WAL mode with a busy timeout. All writes are additionally
serialized in-process by ``write_lock`` so that transactions such as
"publish merged file + supersede old files" never interleave.
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from .config import settings

# Single in-process writer gate. FastAPI runs sync endpoints on a threadpool,
# so this lock is what makes metadata transactions atomic across requests.
write_lock = threading.RLock()


SCHEMA_TABLES = """
CREATE TABLE IF NOT EXISTS batches (
    batch_id        TEXT PRIMARY KEY,
    station_id      TEXT NOT NULL,
    day             TEXT NOT NULL,
    file_path       TEXT NOT NULL,
    row_count       INTEGER NOT NULL,
    size_bytes      INTEGER NOT NULL,
    content_sha256  TEXT NOT NULL,
    status          TEXT NOT NULL CHECK (status IN ('pending', 'published', 'superseded')),
    is_merged       INTEGER NOT NULL DEFAULT 0,
    source_batches  TEXT,
    group_id        TEXT,
    created_at      TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    reaped          INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS upload_groups (
    group_id    TEXT PRIMARY KEY,
    day         TEXT NOT NULL,
    station_cnt INTEGER NOT NULL,
    status      TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed')),
    created_at  TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    published_at TEXT
);

CREATE TABLE IF NOT EXISTS merge_locks (
    station_id  TEXT NOT NULL,
    day         TEXT NOT NULL,
    batch_id    TEXT NOT NULL,
    locked_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (station_id, day)
);
"""

# Indexes are created separately: on an old database the batches table is first
# rebuilt by ``migrate`` (which also recreates its indexes), and only then can
# statements referencing the new ``group_id`` column run safely.
SCHEMA_INDEXES = """
CREATE INDEX IF NOT EXISTS idx_batches_lookup
    ON batches (station_id, day, status);

CREATE INDEX IF NOT EXISTS idx_batches_group
    ON batches (group_id);
"""

# Kept for backwards compatibility / reference; ``init_db`` uses the split
# versions below so migration can run between table and index creation.
SCHEMA = SCHEMA_TABLES + SCHEMA_INDEXES


def connect() -> sqlite3.Connection:
    """Open a new connection (caller closes it)."""
    Path(settings.db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        settings.db_path,
        timeout=30.0,
        isolation_level=None,  # explicit BEGIN/COMMIT
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}


def migrate(conn: sqlite3.Connection) -> None:
    """Upgrade an older database in place (SQLite has no ALTER ... CHECK).

    * add ``batches.group_id`` and the ``pending`` status;
    * create the ``upload_groups`` bookkeeping table.

    The migration is idempotent and safe to run on every startup.
    """
    batch_cols = _column_names(conn, "batches")
    if "group_id" not in batch_cols:
        conn.execute("BEGIN IMMEDIATE")
        try:
            conn.execute("ALTER TABLE batches ADD COLUMN group_id TEXT")
            conn.execute(
                "CREATE TABLE batches_new ("
                "batch_id TEXT PRIMARY KEY, station_id TEXT NOT NULL, day TEXT NOT NULL, "
                "file_path TEXT NOT NULL, row_count INTEGER NOT NULL, size_bytes INTEGER NOT NULL, "
                "content_sha256 TEXT NOT NULL, "
                "status TEXT NOT NULL CHECK (status IN ('pending','published','superseded')), "
                "is_merged INTEGER NOT NULL DEFAULT 0, source_batches TEXT, group_id TEXT, "
                "created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), "
                "reaped INTEGER NOT NULL DEFAULT 0)"
            )
            conn.execute(
                "INSERT INTO batches_new "
                "(batch_id, station_id, day, file_path, row_count, size_bytes, content_sha256, "
                " status, is_merged, source_batches, group_id, created_at, reaped) "
                "SELECT batch_id, station_id, day, file_path, row_count, size_bytes, content_sha256, "
                " status, is_merged, source_batches, NULL, created_at, reaped FROM batches"
            )
            conn.execute("DROP TABLE batches")
            conn.execute("ALTER TABLE batches_new RENAME TO batches")
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_batches_lookup "
                "ON batches (station_id, day, status)"
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_batches_group ON batches (group_id)")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
    conn.executescript(
        "CREATE TABLE IF NOT EXISTS upload_groups ("
        "group_id TEXT PRIMARY KEY, day TEXT NOT NULL, station_cnt INTEGER NOT NULL, "
        "status TEXT NOT NULL CHECK (status IN ('pending','published','failed')), "
        "created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')), "
        "published_at TEXT)"
    )


def init_db() -> None:
    conn = connect()
    try:
        # Tables first (IF NOT EXISTS makes this a no-op on existing DBs),
        # then the in-place upgrade, then indexes that may use new columns.
        conn.executescript(SCHEMA_TABLES)
        migrate(conn)
        conn.executescript(SCHEMA_INDEXES)
    finally:
        conn.close()


@contextmanager
def transaction() -> Iterator[sqlite3.Connection]:
    """Serialized immediate-write transaction."""
    conn = connect()
    # RLock + BEGIN IMMEDIATE: the first writer wins the SQLite reserved lock,
    # other threads wait on busy_timeout and are additionally gated in-process.
    with write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()
