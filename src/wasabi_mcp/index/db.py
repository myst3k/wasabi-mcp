from __future__ import annotations

import logging
from pathlib import Path

import aiosqlite

logger = logging.getLogger(__name__)

# Bump this when the FTS tokenizer or schema changes to trigger a rebuild.
_FTS_VERSION = 2

SCHEMA = """
CREATE TABLE IF NOT EXISTS objects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    bucket TEXT NOT NULL,
    key TEXT NOT NULL,
    size INTEGER NOT NULL DEFAULT 0,
    last_modified TEXT NOT NULL,
    etag TEXT,
    storage_class TEXT DEFAULT 'STANDARD',
    content_type TEXT,
    UNIQUE(bucket, key)
);

CREATE INDEX IF NOT EXISTS idx_objects_bucket ON objects(bucket);
CREATE INDEX IF NOT EXISTS idx_objects_last_modified ON objects(last_modified);
CREATE INDEX IF NOT EXISTS idx_objects_size ON objects(size);

CREATE VIRTUAL TABLE IF NOT EXISTS objects_fts USING fts5(
    key,
    content='objects',
    content_rowid='id',
    tokenize="unicode61 separators '/._-'"
);

CREATE TRIGGER IF NOT EXISTS objects_ai AFTER INSERT ON objects BEGIN
    INSERT INTO objects_fts(rowid, key) VALUES (new.id, new.key);
END;

CREATE TRIGGER IF NOT EXISTS objects_ad AFTER DELETE ON objects BEGIN
    INSERT INTO objects_fts(objects_fts, rowid, key) VALUES ('delete', old.id, old.key);
END;

CREATE TRIGGER IF NOT EXISTS objects_au AFTER UPDATE ON objects BEGIN
    INSERT INTO objects_fts(objects_fts, rowid, key) VALUES ('delete', old.id, old.key);
    INSERT INTO objects_fts(rowid, key) VALUES (new.id, new.key);
END;

CREATE TABLE IF NOT EXISTS sync_state (
    bucket TEXT NOT NULL,
    prefix TEXT NOT NULL DEFAULT '',
    last_synced TEXT NOT NULL,
    objects_count INTEGER DEFAULT 0,
    last_modified_cursor TEXT,
    PRIMARY KEY(bucket, prefix)
);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


async def _rebuild_fts(db: aiosqlite.Connection) -> None:
    """Drop and recreate the FTS table, repopulating from existing objects."""
    count = await db.execute_fetchall("SELECT COUNT(*) FROM objects")
    total = count[0][0] if count else 0
    logger.info(f"Rebuilding FTS index for {total} objects...")

    await db.execute("DROP TRIGGER IF EXISTS objects_ai")
    await db.execute("DROP TRIGGER IF EXISTS objects_ad")
    await db.execute("DROP TRIGGER IF EXISTS objects_au")
    await db.execute("DROP TABLE IF EXISTS objects_fts")
    await db.commit()

    # Re-run full schema to recreate FTS table and triggers
    await db.executescript(SCHEMA)

    # Repopulate FTS from existing data
    await db.execute(
        "INSERT INTO objects_fts(rowid, key) SELECT id, key FROM objects"
    )
    await db.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES ('fts_version', ?)",
        (str(_FTS_VERSION),),
    )
    await db.commit()
    logger.info("FTS index rebuild complete")


async def init_database(db_path: Path) -> aiosqlite.Connection:
    db = await aiosqlite.connect(str(db_path))
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    await db.executescript(SCHEMA)
    await db.commit()

    # Check if FTS needs rebuilding
    try:
        row = await db.execute_fetchall(
            "SELECT value FROM meta WHERE key = 'fts_version'"
        )
        current = int(row[0][0]) if row else 0
    except Exception:
        current = 0

    if current < _FTS_VERSION:
        await _rebuild_fts(db)

    return db
