from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from mcp.server.fastmcp import Context

from wasabi_mcp.server import AppContext, mcp

logger = logging.getLogger(__name__)


def _ctx(ctx: Context) -> AppContext:
    return ctx.request_context.lifespan_context


@mcp.tool()
async def list_buckets(ctx: Context) -> dict[str, Any]:
    """List all Wasabi buckets with creation date, region, public access state, and versioning.

    Uses Wasabi's extended ListBuckets API to return rich metadata in a single call.
    """
    app = _ctx(ctx)
    buckets = await app.clients.list_buckets_extended()
    return {
        "buckets": [
            {
                "name": b.name,
                "creation_date": b.creation_date,
                "region": b.region,
                "public_access": b.public_access,
                "versioning": b.versioning,
            }
            for b in buckets
        ],
        "count": len(buckets),
    }


@mcp.tool()
async def get_bucket_info(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get detailed info for a bucket: location, versioning status.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)

    location, versioning = await asyncio.gather(
        asyncio.to_thread(s3.get_bucket_location, Bucket=bucket),
        asyncio.to_thread(s3.get_bucket_versioning, Bucket=bucket),
    )

    return {
        "name": bucket,
        "region": location.get("LocationConstraint") or app.config.region,
        "versioning": versioning.get("Status", "Disabled"),
        "mfa_delete": versioning.get("MFADelete", "Disabled"),
    }


@mcp.tool()
async def get_bucket_policy(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get the bucket policy as parsed JSON.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    try:
        resp = await asyncio.to_thread(s3.get_bucket_policy, Bucket=bucket)
        policy = json.loads(resp["Policy"])
        return {"bucket": bucket, "policy": policy, "has_policy": True}
    except s3.exceptions.from_code("NoSuchBucketPolicy"):
        return {"bucket": bucket, "policy": None, "has_policy": False}
    except Exception as e:
        if "NoSuchBucketPolicy" in str(e):
            return {"bucket": bucket, "policy": None, "has_policy": False}
        raise


@mcp.tool()
async def get_bucket_lifecycle(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get the bucket lifecycle configuration rules.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    try:
        resp = await asyncio.to_thread(s3.get_bucket_lifecycle_configuration, Bucket=bucket)
        return {
            "bucket": bucket,
            "rules": resp.get("Rules", []),
            "has_lifecycle": True,
        }
    except Exception as e:
        if "NoSuchLifecycleConfiguration" in str(e):
            return {"bucket": bucket, "rules": [], "has_lifecycle": False}
        raise


@mcp.tool()
async def get_bucket_tags(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get the bucket tags as key-value pairs.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    try:
        resp = await asyncio.to_thread(s3.get_bucket_tagging, Bucket=bucket)
        tags = {t["Key"]: t["Value"] for t in resp.get("TagSet", [])}
        return {"bucket": bucket, "tags": tags, "has_tags": bool(tags)}
    except Exception as e:
        if "NoSuchTagSet" in str(e):
            return {"bucket": bucket, "tags": {}, "has_tags": False}
        raise


@mcp.tool()
async def get_bucket_acl(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get the bucket ACL showing owner and grants.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    resp = await asyncio.to_thread(s3.get_bucket_acl, Bucket=bucket)

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
        "owner": {
            "id": owner.get("ID", ""),
            "display_name": owner.get("DisplayName", ""),
        },
        "grants": grants,
    }


@mcp.tool()
async def get_bucket_cors(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get the CORS configuration for a bucket.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    try:
        resp = await asyncio.to_thread(s3.get_bucket_cors, Bucket=bucket)
        rules = [
            {
                "allowed_origins": r.get("AllowedOrigins", []),
                "allowed_methods": r.get("AllowedMethods", []),
                "allowed_headers": r.get("AllowedHeaders", []),
                "expose_headers": r.get("ExposeHeaders", []),
                "max_age_seconds": r.get("MaxAgeSeconds", 0),
            }
            for r in resp.get("CORSRules", [])
        ]
        return {"bucket": bucket, "cors_rules": rules, "has_cors": bool(rules)}
    except Exception as e:
        if "NoSuchCORSConfiguration" in str(e):
            return {"bucket": bucket, "cors_rules": [], "has_cors": False}
        raise


@mcp.tool()
async def get_bucket_logging(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get the server access logging configuration for a bucket.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    resp = await asyncio.to_thread(s3.get_bucket_logging, Bucket=bucket)

    logging_config = resp.get("LoggingEnabled")
    if logging_config:
        return {
            "bucket": bucket,
            "has_logging": True,
            "target_bucket": logging_config.get("TargetBucket", ""),
            "target_prefix": logging_config.get("TargetPrefix", ""),
        }
    return {"bucket": bucket, "has_logging": False, "target_bucket": "", "target_prefix": ""}


@mcp.tool()
async def get_bucket_replication(bucket: str, ctx: Context) -> dict[str, Any]:
    """Get the replication configuration for a bucket.

    Args:
        bucket: The bucket name
    """
    app = _ctx(ctx)
    s3 = await app.clients.s3_for_bucket(bucket)
    try:
        resp = await asyncio.to_thread(s3.get_bucket_replication, Bucket=bucket)
        config = resp.get("ReplicationConfiguration", {})
        rules = [
            {
                "id": r.get("ID", ""),
                "status": r.get("Status", ""),
                "prefix": r.get("Prefix", r.get("Filter", {}).get("Prefix", "")),
                "destination_bucket": r.get("Destination", {}).get("Bucket", ""),
                "destination_storage_class": r.get("Destination", {}).get("StorageClass", ""),
            }
            for r in config.get("Rules", [])
        ]
        return {
            "bucket": bucket,
            "has_replication": bool(rules),
            "role": config.get("Role", ""),
            "rules": rules,
        }
    except Exception as e:
        if "ReplicationConfigurationNotFoundError" in str(e):
            return {"bucket": bucket, "has_replication": False, "role": "", "rules": []}
        raise
