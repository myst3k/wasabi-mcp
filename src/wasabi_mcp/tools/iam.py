from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
from typing import Any

from mcp.server.fastmcp import Context

from wasabi_mcp.server import AppContext, mcp

logger = logging.getLogger(__name__)


def _ctx(ctx: Context) -> AppContext:
    return ctx.request_context.lifespan_context


def _format_date(dt: Any) -> str:
    if hasattr(dt, "isoformat"):
        return dt.isoformat()
    return str(dt)


@mcp.tool()
async def list_users(
    ctx: Context,
    path_prefix: str = "/",
    max_items: int = 50,
    marker: str = "",
) -> dict[str, Any]:
    """List IAM users in the Wasabi account with pagination.

    Args:
        path_prefix: Filter users by path prefix (default: /)
        max_items: Maximum number of users to return (1-1000)
        marker: Pagination marker from a previous response's next_marker
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    kwargs: dict[str, Any] = {
        "PathPrefix": path_prefix,
        "MaxItems": min(max_items, 1000),
    }
    if marker:
        kwargs["Marker"] = marker

    resp = await asyncio.to_thread(iam.list_users, **kwargs)

    return {
        "users": [
            {
                "username": u["UserName"],
                "user_id": u["UserId"],
                "arn": u["Arn"],
                "path": u["Path"],
                "create_date": _format_date(u["CreateDate"]),
            }
            for u in resp.get("Users", [])
        ],
        "count": len(resp.get("Users", [])),
        "is_truncated": resp.get("IsTruncated", False),
        "next_marker": resp.get("Marker", "") if resp.get("IsTruncated") else "",
    }


@mcp.tool()
async def get_user(
    username: str,
    ctx: Context,
) -> dict[str, Any]:
    """Get detailed info for an IAM user.

    Args:
        username: The IAM username
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    resp = await asyncio.to_thread(iam.get_user, UserName=username)
    u = resp["User"]

    try:
        tags_resp = await asyncio.to_thread(iam.list_user_tags, UserName=username)
        tags = {t["Key"]: t["Value"] for t in tags_resp.get("Tags", [])}
    except Exception:
        tags = {}

    return {
        "username": u["UserName"],
        "user_id": u["UserId"],
        "arn": u["Arn"],
        "path": u["Path"],
        "create_date": _format_date(u["CreateDate"]),
        "tags": tags,
    }


@mcp.tool()
async def list_access_keys(
    username: str,
    ctx: Context,
    marker: str = "",
    max_items: int = 50,
) -> dict[str, Any]:
    """List access keys for an IAM user.

    Args:
        username: The IAM username
        marker: Pagination marker from a previous response
        max_items: Maximum number of keys to return
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    kwargs: dict[str, Any] = {
        "UserName": username,
        "MaxItems": min(max_items, 1000),
    }
    if marker:
        kwargs["Marker"] = marker

    resp = await asyncio.to_thread(iam.list_access_keys, **kwargs)

    return {
        "username": username,
        "keys": [
            {
                "access_key_id": k["AccessKeyId"],
                "status": k["Status"],
                "create_date": _format_date(k["CreateDate"]),
            }
            for k in resp.get("AccessKeyMetadata", [])
        ],
        "count": len(resp.get("AccessKeyMetadata", [])),
        "is_truncated": resp.get("IsTruncated", False),
        "next_marker": resp.get("Marker", "") if resp.get("IsTruncated") else "",
    }


@mcp.tool()
async def list_groups(
    ctx: Context,
    path_prefix: str = "/",
    max_items: int = 50,
    marker: str = "",
) -> dict[str, Any]:
    """List IAM groups with pagination.

    Args:
        path_prefix: Filter groups by path prefix (default: /)
        max_items: Maximum number of groups to return (1-1000)
        marker: Pagination marker from a previous response's next_marker
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    kwargs: dict[str, Any] = {
        "PathPrefix": path_prefix,
        "MaxItems": min(max_items, 1000),
    }
    if marker:
        kwargs["Marker"] = marker

    resp = await asyncio.to_thread(iam.list_groups, **kwargs)

    return {
        "groups": [
            {
                "group_name": g["GroupName"],
                "group_id": g["GroupId"],
                "arn": g["Arn"],
                "path": g["Path"],
                "create_date": _format_date(g["CreateDate"]),
            }
            for g in resp.get("Groups", [])
        ],
        "count": len(resp.get("Groups", [])),
        "is_truncated": resp.get("IsTruncated", False),
        "next_marker": resp.get("Marker", "") if resp.get("IsTruncated") else "",
    }


@mcp.tool()
async def list_group_members(
    group_name: str,
    ctx: Context,
    max_items: int = 50,
    marker: str = "",
) -> dict[str, Any]:
    """List the members (users) of an IAM group with pagination.

    Args:
        group_name: The IAM group name
        max_items: Maximum number of members to return (1-1000)
        marker: Pagination marker from a previous response's next_marker
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    kwargs: dict[str, Any] = {
        "GroupName": group_name,
        "MaxItems": min(max_items, 1000),
    }
    if marker:
        kwargs["Marker"] = marker

    resp = await asyncio.to_thread(iam.get_group, **kwargs)

    return {
        "group_name": group_name,
        "users": [
            {
                "username": u["UserName"],
                "user_id": u["UserId"],
                "arn": u["Arn"],
            }
            for u in resp.get("Users", [])
        ],
        "count": len(resp.get("Users", [])),
        "is_truncated": resp.get("IsTruncated", False),
        "next_marker": resp.get("Marker", "") if resp.get("IsTruncated") else "",
    }


@mcp.tool()
async def list_policies(
    ctx: Context,
    scope: str = "Local",
    path_prefix: str = "/",
    max_items: int = 50,
    marker: str = "",
) -> dict[str, Any]:
    """List IAM policies with pagination. Use scope "Local" for customer-created policies.

    Args:
        scope: Policy scope - "Local" for customer-created, "AWS" for Wasabi-managed, "All" for both
        path_prefix: Filter policies by path prefix
        max_items: Maximum number of policies to return (1-1000)
        marker: Pagination marker from a previous response's next_marker
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    kwargs: dict[str, Any] = {
        "Scope": scope,
        "PathPrefix": path_prefix,
        "MaxItems": min(max_items, 1000),
    }
    if marker:
        kwargs["Marker"] = marker

    resp = await asyncio.to_thread(iam.list_policies, **kwargs)

    return {
        "policies": [
            {
                "policy_name": p["PolicyName"],
                "policy_id": p["PolicyId"],
                "arn": p["Arn"],
                "path": p["Path"],
                "attachment_count": p.get("AttachmentCount", 0),
                "default_version_id": p.get("DefaultVersionId", ""),
                "create_date": _format_date(p["CreateDate"]),
            }
            for p in resp.get("Policies", [])
        ],
        "count": len(resp.get("Policies", [])),
        "is_truncated": resp.get("IsTruncated", False),
        "next_marker": resp.get("Marker", "") if resp.get("IsTruncated") else "",
    }


@mcp.tool()
async def get_policy_document(
    policy_arn: str,
    ctx: Context,
    version_id: str = "",
) -> dict[str, Any]:
    """Get the actual JSON policy document for a managed policy.

    Args:
        policy_arn: The ARN of the policy
        version_id: Specific version ID (defaults to the policy's default version)
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    if not version_id:
        policy_resp = await asyncio.to_thread(iam.get_policy, PolicyArn=policy_arn)
        version_id = policy_resp["Policy"]["DefaultVersionId"]

    resp = await asyncio.to_thread(
        iam.get_policy_version, PolicyArn=policy_arn, VersionId=version_id
    )

    document_raw = resp["PolicyVersion"]["Document"]
    if isinstance(document_raw, str):
        document = json.loads(urllib.parse.unquote(document_raw))
    else:
        document = document_raw

    return {
        "policy_arn": policy_arn,
        "version_id": version_id,
        "document": document,
    }


@mcp.tool()
async def list_user_policies(
    username: str,
    ctx: Context,
) -> dict[str, Any]:
    """List all policies (inline and attached) for an IAM user.

    Args:
        username: The IAM username
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    inline_resp, attached_resp = await asyncio.gather(
        asyncio.to_thread(iam.list_user_policies, UserName=username),
        asyncio.to_thread(iam.list_attached_user_policies, UserName=username),
    )

    return {
        "username": username,
        "inline_policies": inline_resp.get("PolicyNames", []),
        "attached_policies": [
            {
                "policy_name": p["PolicyName"],
                "policy_arn": p["PolicyArn"],
            }
            for p in attached_resp.get("AttachedPolicies", [])
        ],
    }


@mcp.tool()
async def list_group_policies(
    group_name: str,
    ctx: Context,
) -> dict[str, Any]:
    """List all policies (inline and attached) for an IAM group.

    Args:
        group_name: The IAM group name
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    inline_resp, attached_resp = await asyncio.gather(
        asyncio.to_thread(iam.list_group_policies, GroupName=group_name),
        asyncio.to_thread(iam.list_attached_group_policies, GroupName=group_name),
    )

    return {
        "group_name": group_name,
        "inline_policies": inline_resp.get("PolicyNames", []),
        "attached_policies": [
            {
                "policy_name": p["PolicyName"],
                "policy_arn": p["PolicyArn"],
            }
            for p in attached_resp.get("AttachedPolicies", [])
        ],
    }


@mcp.tool()
async def list_entities_for_policy(
    policy_arn: str,
    ctx: Context,
    max_items: int = 50,
    marker: str = "",
) -> dict[str, Any]:
    """List all IAM users, groups, and roles that a policy is attached to.

    Useful for auditing who has access via a specific policy.

    Args:
        policy_arn: The ARN of the policy
        max_items: Maximum number of entities to return
        marker: Pagination marker from a previous response
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    kwargs: dict[str, Any] = {
        "PolicyArn": policy_arn,
        "MaxItems": min(max_items, 1000),
    }
    if marker:
        kwargs["Marker"] = marker

    resp = await asyncio.to_thread(iam.list_entities_for_policy, **kwargs)

    return {
        "policy_arn": policy_arn,
        "users": [
            {"username": u["UserName"], "user_id": u.get("UserId", "")}
            for u in resp.get("PolicyUsers", [])
        ],
        "groups": [
            {"group_name": g["GroupName"], "group_id": g.get("GroupId", "")}
            for g in resp.get("PolicyGroups", [])
        ],
        "roles": [
            {"role_name": r["RoleName"], "role_id": r.get("RoleId", "")}
            for r in resp.get("PolicyRoles", [])
        ],
        "is_truncated": resp.get("IsTruncated", False),
        "next_marker": resp.get("Marker", "") if resp.get("IsTruncated") else "",
    }


@mcp.tool()
async def get_access_key_last_used(
    access_key_id: str,
    ctx: Context,
) -> dict[str, Any]:
    """Get when an access key was last used and for what service/region.

    Useful for security audits to find unused or stale access keys.

    Args:
        access_key_id: The access key ID to check
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    resp = await asyncio.to_thread(iam.get_access_key_last_used, AccessKeyId=access_key_id)

    info = resp.get("AccessKeyLastUsed", {})
    last_used = info.get("LastUsedDate")

    return {
        "access_key_id": access_key_id,
        "username": resp.get("UserName", ""),
        "last_used_date": last_used.isoformat() if hasattr(last_used, "isoformat") else str(last_used) if last_used else "",
        "service_name": info.get("ServiceName", ""),
        "region": info.get("Region", ""),
    }


@mcp.tool()
async def list_policy_versions(
    policy_arn: str,
    ctx: Context,
    max_items: int = 50,
    marker: str = "",
) -> dict[str, Any]:
    """List all versions of a managed policy.

    Args:
        policy_arn: The ARN of the policy
        max_items: Maximum number of versions to return
        marker: Pagination marker from a previous response
    """
    app = _ctx(ctx)
    iam = app.clients.iam

    kwargs: dict[str, Any] = {
        "PolicyArn": policy_arn,
        "MaxItems": min(max_items, 1000),
    }
    if marker:
        kwargs["Marker"] = marker

    resp = await asyncio.to_thread(iam.list_policy_versions, **kwargs)

    return {
        "policy_arn": policy_arn,
        "versions": [
            {
                "version_id": v["VersionId"],
                "is_default_version": v.get("IsDefaultVersion", False),
                "create_date": _format_date(v["CreateDate"]),
            }
            for v in resp.get("Versions", [])
        ],
        "count": len(resp.get("Versions", [])),
        "is_truncated": resp.get("IsTruncated", False),
        "next_marker": resp.get("Marker", "") if resp.get("IsTruncated") else "",
    }
