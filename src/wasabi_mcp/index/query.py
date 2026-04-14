from __future__ import annotations

import os
from typing import Any

import aiosqlite


async def search(
    db: aiosqlite.Connection,
    query: str,
    bucket: str | None = None,
    min_size: int | None = None,
    max_size: int | None = None,
    modified_after: str | None = None,
    modified_before: str | None = None,
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    conditions = []
    params: list[Any] = []

    if bucket:
        conditions.append("o.bucket = ?")
        params.append(bucket)
    if min_size is not None:
        conditions.append("o.size >= ?")
        params.append(min_size)
    if max_size is not None:
        conditions.append("o.size <= ?")
        params.append(max_size)
    if modified_after:
        conditions.append("o.last_modified >= ?")
        params.append(modified_after)
    if modified_before:
        conditions.append("o.last_modified <= ?")
        params.append(modified_before)

    where = " AND ".join(conditions)
    if where:
        where = "WHERE " + where

    if query.strip() in ("*", ""):
        sql = f"""
            SELECT o.bucket, o.key, o.size, o.last_modified, o.etag, o.storage_class
            FROM objects o
            {where}
            ORDER BY o.last_modified DESC
            LIMIT ? OFFSET ?
        """
        params.extend([limit, offset])
    else:
        fts_query = query if "*" in query else f"{query}*"
        if where:
            where = "AND " + " AND ".join(conditions)
        else:
            where = ""
        sql = f"""
            SELECT o.bucket, o.key, o.size, o.last_modified, o.etag, o.storage_class
            FROM objects_fts fts
            JOIN objects o ON o.id = fts.rowid
            WHERE fts.key MATCH ?
            {where}
            ORDER BY rank
            LIMIT ? OFFSET ?
        """
        params = [fts_query, *params, limit, offset]

    rows = await db.execute_fetchall(sql, params)

    total = await db.execute_fetchall("SELECT COUNT(*) FROM objects")
    total_count = total[0][0] if total else 0

    return {
        "query": query,
        "results": [
            {
                "bucket": row[0],
                "key": row[1],
                "size": row[2],
                "last_modified": row[3],
                "etag": row[4],
                "storage_class": row[5],
            }
            for row in rows
        ],
        "count": len(rows),
        "offset": offset,
        "limit": limit,
        "has_more": len(rows) == limit,
        "total_indexed": total_count,
    }


async def get_stats(
    db: aiosqlite.Connection,
    bucket: str | None = None,
) -> dict[str, Any]:
    if bucket:
        rows = await db.execute_fetchall(
            """SELECT o.bucket, COUNT(*) as cnt, SUM(o.size) as total_size,
                      s.last_synced, s.last_modified_cursor
               FROM objects o
               LEFT JOIN sync_state s ON s.bucket = o.bucket AND s.prefix = ''
               WHERE o.bucket = ?
               GROUP BY o.bucket""",
            (bucket,),
        )
    else:
        rows = await db.execute_fetchall(
            """SELECT o.bucket, COUNT(*) as cnt, SUM(o.size) as total_size,
                      s.last_synced, s.last_modified_cursor
               FROM objects o
               LEFT JOIN sync_state s ON s.bucket = o.bucket AND s.prefix = ''
               GROUP BY o.bucket""",
        )

    total = await db.execute_fetchall("SELECT COUNT(*) FROM objects")
    total_count = total[0][0] if total else 0

    # Get DB file size from the connection's path
    db_size = 0
    try:
        db_path = await db.execute_fetchall("PRAGMA database_list")
        if db_path and db_path[0][2]:
            db_size = os.path.getsize(db_path[0][2])
    except Exception:
        pass

    return {
        "total_objects": total_count,
        "db_size_bytes": db_size,
        "buckets": [
            {
                "name": row[0],
                "count": row[1],
                "total_size": row[2],
                "last_synced": row[3],
                "last_modified_cursor": row[4],
            }
            for row in rows
        ],
    }


async def get_prefix_tree(
    db: aiosqlite.Connection,
    bucket: str,
    depth: int = 2,
) -> dict[str, Any]:
    rows = await db.execute_fetchall(
        "SELECT key, size FROM objects WHERE bucket = ?",
        (bucket,),
    )

    tree: dict[str, dict[str, int]] = {}
    for key, size in rows:
        parts = key.split("/")
        # Build prefix at each depth level up to the requested depth
        for d in range(1, min(depth + 1, len(parts) + 1)):
            if d <= len(parts) - 1:
                prefix = "/".join(parts[:d]) + "/"
            else:
                prefix = key
            if prefix not in tree:
                tree[prefix] = {"object_count": 0, "total_size": 0}
            tree[prefix]["object_count"] += 1
            tree[prefix]["total_size"] += size

    # Only return prefixes at the requested depth (directories), sorted
    result = sorted(
        [
            {"prefix": prefix, **stats}
            for prefix, stats in tree.items()
            if prefix.endswith("/")
        ],
        key=lambda x: x["prefix"],
    )

    return {
        "bucket": bucket,
        "depth": depth,
        "tree": result,
    }


async def drop_indexed(
    db: aiosqlite.Connection,
    bucket: str | None = None,
) -> dict[str, Any]:
    if bucket:
        count_rows = await db.execute_fetchall(
            "SELECT COUNT(*) FROM objects WHERE bucket = ?", (bucket,)
        )
        count = count_rows[0][0] if count_rows else 0
        await db.execute("DELETE FROM objects WHERE bucket = ?", (bucket,))
        await db.execute("DELETE FROM sync_state WHERE bucket = ?", (bucket,))
    else:
        count_rows = await db.execute_fetchall("SELECT COUNT(*) FROM objects")
        count = count_rows[0][0] if count_rows else 0
        await db.execute("DELETE FROM objects")
        await db.execute("DELETE FROM sync_state")

    await db.commit()

    return {
        "dropped_bucket": bucket or "all",
        "objects_removed": count,
    }
