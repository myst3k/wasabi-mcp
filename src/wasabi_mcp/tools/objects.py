from __future__ import annotations

import asyncio
import fnmatch
import logging
from typing import Any

from mcp.server.fastmcp import Context

from wasabi_mcp.server import AppContext, mcp

logger = logging.getLogger(__name__)


def _ctx(ctx: Context) -> AppContext:
    return ctx.request_context.lifespan_context


def _format_object(obj: dict[str, Any]) -> dict[str, Any]:
    last_mod = obj.get("LastModified")
    return {
        "key": obj["Key"],
        "size": obj.get("Size", 0),
        "last_modified": last_mod.isoformat() if hasattr(last_mod, "isoformat") else str(last_mod),
        "etag": obj.get("ETag", "").strip('"'),
        "storage_class": obj.get("StorageClass", "STANDARD"),
    }


@mcp.tool()
async def list_objects(
    bucket: str,
    ctx: Context,
    prefix: str = "",
    max_keys: int = 100,
    continuation_token: str = "",
) -> dict[str, Any]:
    """List objects in a Wasabi bucket with pagination.

    Args:
        bucket: The bucket name
        prefix: Filter to objects starting with this prefix
        max_keys: Maximum number of objects to return (1-1000)
        continuation_token: Token from a previous response to get the next page
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": min(max_keys, 1000)}
    if prefix:
        kwargs["Prefix"] = prefix
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    resp = await asyncio.to_thread(s3.list_objects_v2, **kwargs)

    objects = [_format_object(obj) for obj in resp.get("Contents", [])]

    return {
        "bucket": bucket,
        "prefix": prefix,
        "objects": objects,
        "count": len(objects),
        "is_truncated": resp.get("IsTruncated", False),
        "next_token": resp.get("NextContinuationToken", ""),
    }


@mcp.tool()
async def list_prefixes(
    bucket: str,
    ctx: Context,
    prefix: str = "",
    delimiter: str = "/",
    max_keys: int = 100,
    continuation_token: str = "",
) -> dict[str, Any]:
    """Browse a bucket like a directory, listing common prefixes (folders) and objects at this level.

    Args:
        bucket: The bucket name
        prefix: Starting prefix to list from
        delimiter: Delimiter for grouping (default: /)
        max_keys: Maximum number of keys to process (1-1000)
        continuation_token: Token from a previous response to get the next page
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    kwargs: dict[str, Any] = {
        "Bucket": bucket,
        "Prefix": prefix,
        "Delimiter": delimiter,
        "MaxKeys": min(max_keys, 1000),
    }
    if continuation_token:
        kwargs["ContinuationToken"] = continuation_token

    resp = await asyncio.to_thread(s3.list_objects_v2, **kwargs)

    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    objects = [_format_object(obj) for obj in resp.get("Contents", [])]

    return {
        "bucket": bucket,
        "prefix": prefix,
        "prefixes": prefixes,
        "objects_at_level": objects,
        "prefix_count": len(prefixes),
        "object_count": len(objects),
        "is_truncated": resp.get("IsTruncated", False),
        "next_token": resp.get("NextContinuationToken", ""),
    }


@mcp.tool()
async def head_object(
    bucket: str,
    key: str,
    ctx: Context,
) -> dict[str, Any]:
    """Get metadata for an object without downloading its content.

    Args:
        bucket: The bucket name
        key: The full object key
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    resp = await asyncio.to_thread(s3.head_object, Bucket=bucket, Key=key)

    last_mod = resp.get("LastModified")
    return {
        "bucket": bucket,
        "key": key,
        "size": resp.get("ContentLength", 0),
        "last_modified": last_mod.isoformat() if hasattr(last_mod, "isoformat") else str(last_mod),
        "etag": resp.get("ETag", "").strip('"'),
        "content_type": resp.get("ContentType", ""),
        "storage_class": resp.get("StorageClass", "STANDARD"),
        "metadata": resp.get("Metadata", {}),
        "version_id": resp.get("VersionId", ""),
    }


@mcp.tool()
async def search_objects(
    bucket: str,
    ctx: Context,
    prefix: str = "",
    pattern: str = "*",
    max_results: int = 100,
    continuation_token: str = "",
) -> dict[str, Any]:
    """Search for objects in a bucket by prefix and glob pattern.

    Lists objects with the given prefix, then filters by a glob pattern
    applied to the object key. Use standard glob syntax: *, ?, [abc].
    Supports pagination for large result sets.

    Args:
        bucket: The bucket name
        prefix: S3 prefix to narrow the listing before pattern matching
        pattern: Glob pattern to match against the full object key (e.g. "*.jpg", "backup-202?-*")
        max_results: Maximum matching results to return per page
        continuation_token: Token from a previous response to continue scanning
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    def _search() -> tuple[list[dict[str, Any]], str, bool]:
        matches: list[dict[str, Any]] = []
        next_token = ""
        kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": 1000}
        if prefix:
            kwargs["Prefix"] = prefix
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        while True:
            resp = s3.list_objects_v2(**kwargs)
            for obj in resp.get("Contents", []):
                if fnmatch.fnmatch(obj["Key"], pattern):
                    matches.append(obj)
                    if len(matches) >= max_results:
                        # Return the S3 continuation token so caller can resume
                        next_token = resp.get("NextContinuationToken", "")
                        return matches, next_token, True

            if not resp.get("IsTruncated"):
                break
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]

        return matches, "", False

    matches, next_token, has_more = await asyncio.to_thread(_search)
    objects = [_format_object(obj) for obj in matches]

    return {
        "bucket": bucket,
        "prefix": prefix,
        "pattern": pattern,
        "matches": objects,
        "count": len(objects),
        "is_truncated": has_more,
        "next_token": next_token,
    }


@mcp.tool()
async def get_object_acl(
    bucket: str,
    key: str,
    ctx: Context,
) -> dict[str, Any]:
    """Get the ACL for a specific object.

    Args:
        bucket: The bucket name
        key: The full object key
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    resp = await asyncio.to_thread(s3.get_object_acl, Bucket=bucket, Key=key)

    owner = resp.get("Owner", {})
    grants = [
        {
            "grantee": g.get("Grantee", {}).get("DisplayName") or g.get("Grantee", {}).get("URI", ""),
            "grantee_type": g.get("Grantee", {}).get("Type", ""),
            "permission": g.get("Permission", ""),
        }
        for g in resp.get("Grants", [])
    ]

    return {
        "bucket": bucket,
        "key": key,
        "owner": {
            "id": owner.get("ID", ""),
            "display_name": owner.get("DisplayName", ""),
        },
        "grants": grants,
    }


@mcp.tool()
async def get_object_tags(
    bucket: str,
    key: str,
    ctx: Context,
) -> dict[str, Any]:
    """Get the tags for a specific object.

    Args:
        bucket: The bucket name
        key: The full object key
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    resp = await asyncio.to_thread(s3.get_object_tagging, Bucket=bucket, Key=key)

    tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}
    return {
        "bucket": bucket,
        "key": key,
        "tags": tags,
        "has_tags": bool(tags),
    }


@mcp.tool()
async def list_object_versions(
    bucket: str,
    ctx: Context,
    prefix: str = "",
    max_keys: int = 100,
    key_marker: str = "",
    version_id_marker: str = "",
) -> dict[str, Any]:
    """List all versions of objects in a versioned bucket.

    Returns both current and non-current versions, plus delete markers.
    Only works on buckets with versioning enabled.

    Args:
        bucket: The bucket name
        prefix: Filter to versions of objects starting with this prefix
        max_keys: Maximum number of versions to return (1-1000)
        key_marker: Object key to start listing after (for pagination)
        version_id_marker: Version ID to start after (use with key_marker)
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    kwargs: dict[str, Any] = {"Bucket": bucket, "MaxKeys": min(max_keys, 1000)}
    if prefix:
        kwargs["Prefix"] = prefix
    if key_marker:
        kwargs["KeyMarker"] = key_marker
    if version_id_marker:
        kwargs["VersionIdMarker"] = version_id_marker

    resp = await asyncio.to_thread(s3.list_object_versions, **kwargs)

    def _fmt_version(v: dict[str, Any]) -> dict[str, Any]:
        last_mod = v.get("LastModified")
        return {
            "key": v["Key"],
            "version_id": v.get("VersionId", ""),
            "is_latest": v.get("IsLatest", False),
            "size": v.get("Size", 0),
            "last_modified": last_mod.isoformat() if hasattr(last_mod, "isoformat") else str(last_mod),
            "etag": v.get("ETag", "").strip('"'),
            "storage_class": v.get("StorageClass", "STANDARD"),
        }

    versions = [_fmt_version(v) for v in resp.get("Versions", [])]
    delete_markers = [
        {
            "key": d["Key"],
            "version_id": d.get("VersionId", ""),
            "is_latest": d.get("IsLatest", False),
            "last_modified": d["LastModified"].isoformat() if hasattr(d["LastModified"], "isoformat") else str(d["LastModified"]),
        }
        for d in resp.get("DeleteMarkers", [])
    ]

    return {
        "bucket": bucket,
        "prefix": prefix,
        "versions": versions,
        "delete_markers": delete_markers,
        "version_count": len(versions),
        "delete_marker_count": len(delete_markers),
        "is_truncated": resp.get("IsTruncated", False),
        "next_key_marker": resp.get("NextKeyMarker", ""),
        "next_version_id_marker": resp.get("NextVersionIdMarker", ""),
    }


@mcp.tool()
async def list_multipart_uploads(
    bucket: str,
    ctx: Context,
    prefix: str = "",
    max_uploads: int = 100,
    key_marker: str = "",
    upload_id_marker: str = "",
) -> dict[str, Any]:
    """List in-progress multipart uploads in a bucket.

    Useful for finding stuck or abandoned uploads that may be consuming storage.

    Args:
        bucket: The bucket name
        prefix: Filter to uploads for objects starting with this prefix
        max_uploads: Maximum number of uploads to return (1-1000)
        key_marker: Object key to start listing after (for pagination)
        upload_id_marker: Upload ID to start after (use with key_marker)
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    kwargs: dict[str, Any] = {"Bucket": bucket, "MaxUploads": min(max_uploads, 1000)}
    if prefix:
        kwargs["Prefix"] = prefix
    if key_marker:
        kwargs["KeyMarker"] = key_marker
    if upload_id_marker:
        kwargs["UploadIdMarker"] = upload_id_marker

    resp = await asyncio.to_thread(s3.list_multipart_uploads, **kwargs)

    uploads = [
        {
            "key": u["Key"],
            "upload_id": u["UploadId"],
            "initiated": u["Initiated"].isoformat() if hasattr(u["Initiated"], "isoformat") else str(u["Initiated"]),
            "storage_class": u.get("StorageClass", "STANDARD"),
            "initiator": u.get("Initiator", {}).get("DisplayName", ""),
        }
        for u in resp.get("Uploads", [])
    ]

    return {
        "bucket": bucket,
        "prefix": prefix,
        "uploads": uploads,
        "count": len(uploads),
        "is_truncated": resp.get("IsTruncated", False),
        "next_key_marker": resp.get("NextKeyMarker", ""),
        "next_upload_id_marker": resp.get("NextUploadIdMarker", ""),
    }


@mcp.tool()
async def generate_presigned_url(
    bucket: str,
    key: str,
    ctx: Context,
    expires_in: int = 3600,
) -> dict[str, Any]:
    """Generate a temporary presigned URL for downloading an object.

    The URL allows anyone with it to download the object without credentials,
    until it expires.

    Args:
        bucket: The bucket name
        key: The full object key
        expires_in: URL expiration time in seconds (default: 3600 = 1 hour, max: 604800 = 7 days)
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    expires_in = min(expires_in, 604800)

    url = await asyncio.to_thread(
        s3.generate_presigned_url,
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )

    return {
        "bucket": bucket,
        "key": key,
        "url": url,
        "expires_in_seconds": expires_in,
    }


@mcp.tool()
async def get_object_retention(
    bucket: str,
    key: str,
    ctx: Context,
) -> dict[str, Any]:
    """Get the retention configuration for an object (compliance/governance lock).

    Only applicable to buckets with Object Lock enabled.

    Args:
        bucket: The bucket name
        key: The full object key
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    try:
        resp = await asyncio.to_thread(s3.get_object_retention, Bucket=bucket, Key=key)
        retention = resp.get("Retention", {})
        retain_until = retention.get("RetainUntilDate")
        return {
            "bucket": bucket,
            "key": key,
            "mode": retention.get("Mode", ""),
            "retain_until_date": retain_until.isoformat() if hasattr(retain_until, "isoformat") else str(retain_until) if retain_until else "",
            "has_retention": True,
        }
    except Exception as e:
        err = str(e)
        if "ObjectLock" in err or "InvalidRequest" in err or "NotImplemented" in err:
            return {"bucket": bucket, "key": key, "has_retention": False, "mode": "", "retain_until_date": ""}
        raise


@mcp.tool()
async def get_object_legal_hold(
    bucket: str,
    key: str,
    ctx: Context,
) -> dict[str, Any]:
    """Get the legal hold status for an object.

    Only applicable to buckets with Object Lock enabled.

    Args:
        bucket: The bucket name
        key: The full object key
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    try:
        resp = await asyncio.to_thread(s3.get_object_legal_hold, Bucket=bucket, Key=key)
        hold = resp.get("LegalHold", {})
        return {
            "bucket": bucket,
            "key": key,
            "status": hold.get("Status", "OFF"),
            "has_legal_hold": hold.get("Status") == "ON",
        }
    except Exception as e:
        err = str(e)
        if "ObjectLock" in err or "InvalidRequest" in err or "NotImplemented" in err:
            return {"bucket": bucket, "key": key, "status": "OFF", "has_legal_hold": False}
        raise
