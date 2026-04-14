from __future__ import annotations

from pathlib import Path

import aiosqlite

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
    tokenize="unicode61 tokenchars '._-' separators '/'"
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
"""


async def init_database(db_path: Path) -> aiosqlite.Connection:
    db = await aiosqlite.connect(str(db_path))
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA foreign_keys=ON")
    await db.executescript(SCHEMA)
    await db.commit()
    return db
