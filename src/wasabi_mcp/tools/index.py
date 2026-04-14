from __future__ import annotations

import logging
from typing import Any

from mcp.server.fastmcp import Context

from wasabi_mcp.index.query import drop_indexed, get_prefix_tree, get_stats, search
from wasabi_mcp.index.sync import sync_bucket
from wasabi_mcp.server import AppContext, mcp

logger = logging.getLogger(__name__)


def _ctx(ctx: Context) -> AppContext:
    return ctx.request_context.lifespan_context


@mcp.tool()
async def index_bucket(
    bucket: str,
    ctx: Context,
    prefix: str = "",
    force_full: bool = False,
) -> dict[str, Any]:
    """Scan a Wasabi bucket and index its contents into the local search database.

    The index enables fast local full-text search across object keys without
    making S3 API calls. By default, performs an incremental sync (only processes
    objects modified since the last scan). Use force_full to detect deletions.

    Args:
        bucket: The bucket name to index
        prefix: Only index objects under this prefix
        force_full: Force a full re-scan including delete detection
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    return await sync_bucket(app.db, s3, bucket, prefix, force_full)


@mcp.tool()
async def search_index(
    query: str,
    ctx: Context,
    bucket: str = "",
    min_size: int | None = None,
    max_size: int | None = None,
    modified_after: str = "",
    modified_before: str = "",
    limit: int = 50,
    offset: int = 0,
) -> dict[str, Any]:
    """Search the local index for objects matching a query.

    Searches across indexed object keys using full-text search. Object keys
    are tokenized by path separators, so searching for "vacation" will find
    "photos/2024/vacation/img001.jpg". Must run index_bucket first.

    Supports pagination via limit and offset. Use has_more in the response
    to determine if more results are available.

    Args:
        query: Search terms to match against object keys (use "*" to list all)
        bucket: Filter to a specific bucket
        min_size: Minimum object size in bytes
        max_size: Maximum object size in bytes
        modified_after: Only objects modified after this ISO 8601 date
        modified_before: Only objects modified before this ISO 8601 date
        limit: Maximum results to return per page
        offset: Number of results to skip (for pagination)
    """
    app = _ctx(ctx)
    return await search(
        app.db,
        query,
        bucket=bucket or None,
        min_size=min_size,
        max_size=max_size,
        modified_after=modified_after or None,
        modified_before=modified_before or None,
        limit=limit,
        offset=offset,
    )


@mcp.tool()
async def index_stats(
    ctx: Context,
    bucket: str = "",
) -> dict[str, Any]:
    """Get statistics about the local search index.

    Shows total indexed objects, per-bucket counts, last sync times,
    and database size.

    Args:
        bucket: Filter stats to a specific bucket (empty for all)
    """
    app = _ctx(ctx)
    return await get_stats(app.db, bucket=bucket or None)


@mcp.tool()
async def index_prefix_tree(
    bucket: str,
    ctx: Context,
    depth: int = 2,
) -> dict[str, Any]:
    """Build a directory-like prefix tree from the local index.

    Shows the folder structure of a bucket with object counts and
    total sizes at each level. Requires the bucket to be indexed first.

    Args:
        bucket: The bucket name
        depth: How many levels deep to show (default: 2)
    """
    app = _ctx(ctx)
    return await get_prefix_tree(app.db, bucket, depth)


@mcp.tool()
async def drop_index(
    ctx: Context,
    bucket: str = "",
) -> dict[str, Any]:
    """Delete indexed data for a bucket or all buckets.

    Use this to clean up the local index before re-indexing.

    Args:
        bucket: Bucket to drop (empty to drop all indexed data)
    """
    app = _ctx(ctx)
    return await drop_indexed(app.db, bucket=bucket or None)
