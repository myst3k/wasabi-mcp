from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import aiosqlite

logger = logging.getLogger(__name__)


async def sync_bucket(
    db: aiosqlite.Connection,
    s3_client: Any,
    bucket: str,
    prefix: str = "",
    force_full: bool = False,
) -> dict[str, Any]:
    start = time.monotonic()

    last_cursor = None
    if not force_full:
        row = await db.execute_fetchall(
            "SELECT last_modified_cursor FROM sync_state WHERE bucket = ? AND prefix = ?",
            (bucket, prefix),
        )
        if row:
            last_cursor = row[0][0]

    paginator = s3_client.get_paginator("list_objects_v2")
    pages_kwargs: dict[str, Any] = {"Bucket": bucket}
    if prefix:
        pages_kwargs["Prefix"] = prefix

    seen_keys: set[str] = set()
    upserted = 0
    newest_modified: str | None = last_cursor

    def _paginate() -> list[list[dict[str, Any]]]:
        all_pages = []
        for page in paginator.paginate(**pages_kwargs):
            contents = page.get("Contents", [])
            if contents:
                all_pages.append(contents)
        return all_pages

    all_pages = await asyncio.to_thread(_paginate)

    for contents in all_pages:
        rows = []
        for obj in contents:
            key = obj["Key"]
            seen_keys.add(key)
            last_mod = obj["LastModified"]
            last_mod_iso = last_mod.isoformat() if hasattr(last_mod, "isoformat") else str(last_mod)

            if last_cursor and not force_full and last_mod_iso <= last_cursor:
                continue

            if newest_modified is None or last_mod_iso > newest_modified:
                newest_modified = last_mod_iso

            rows.append((
                bucket,
                key,
                obj.get("Size", 0),
                last_mod_iso,
                obj.get("ETag", "").strip('"'),
                obj.get("StorageClass", "STANDARD"),
            ))

        if rows:
            await db.executemany(
                """INSERT INTO objects (bucket, key, size, last_modified, etag, storage_class)
                   VALUES (?, ?, ?, ?, ?, ?)
                   ON CONFLICT(bucket, key) DO UPDATE SET
                     size = excluded.size,
                     last_modified = excluded.last_modified,
                     etag = excluded.etag,
                     storage_class = excluded.storage_class""",
                rows,
            )
            upserted += len(rows)

    deleted = 0
    if force_full and seen_keys:
        # Remove objects that no longer exist in S3
        existing = await db.execute_fetchall(
            "SELECT key FROM objects WHERE bucket = ? AND (? = '' OR key LIKE ? || '%')",
            (bucket, prefix, prefix),
        )
        stale_keys = [row[0] for row in existing if row[0] not in seen_keys]
        if stale_keys:
            placeholders = ",".join("?" * len(stale_keys))
            await db.execute(
                f"DELETE FROM objects WHERE bucket = ? AND key IN ({placeholders})",
                [bucket, *stale_keys],
            )
            deleted = len(stale_keys)

    now_iso = datetime.now(timezone.utc).isoformat()
    await db.execute(
        """INSERT INTO sync_state (bucket, prefix, last_synced, objects_count, last_modified_cursor)
           VALUES (?, ?, ?, ?, ?)
           ON CONFLICT(bucket, prefix) DO UPDATE SET
             last_synced = excluded.last_synced,
             objects_count = excluded.objects_count,
             last_modified_cursor = excluded.last_modified_cursor""",
        (bucket, prefix, now_iso, len(seen_keys), newest_modified),
    )
    await db.commit()

    duration = time.monotonic() - start
    return {
        "bucket": bucket,
        "prefix": prefix,
        "objects_scanned": len(seen_keys),
        "objects_upserted": upserted,
        "objects_deleted": deleted,
        "duration_seconds": round(duration, 2),
        "was_incremental": not force_full and last_cursor is not None,
    }
