#!/usr/bin/env python3
"""Live integration test for wasabi-mcp.

Runs through all MCP tools against a real Wasabi account.
Requires valid Wasabi credentials in ~/.aws/credentials or env vars.

Usage:
    uv run python test_live.py
"""

import json
import subprocess
import sys
import time


class MCPClient:
    def __init__(self):
        self.proc = subprocess.Popen(
            ["uv", "run", "wasabi-mcp"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self._id = 0
        self._init()

    def _init(self):
        resp = self.call("initialize", {
            "protocolVersion": "2024-11-05",
            "capabilities": {},
            "clientInfo": {"name": "test", "version": "1.0"},
        })
        assert resp.get("serverInfo", {}).get("name") == "Wasabi MCP"
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"})

    def _send(self, msg):
        self.proc.stdin.write((json.dumps(msg) + "\n").encode())
        self.proc.stdin.flush()

    def _recv(self):
        line = self.proc.stdout.readline().decode().strip()
        return json.loads(line) if line else None

    def call(self, method, params=None):
        self._id += 1
        msg = {"jsonrpc": "2.0", "id": self._id, "method": method}
        if params is not None:
            msg["params"] = params
        self._send(msg)
        resp = self._recv()
        if resp and "error" in resp:
            raise RuntimeError(f"MCP error: {resp['error']}")
        return resp.get("result", {}) if resp else {}

    def tool(self, name, **kwargs):
        result = self.call("tools/call", {"name": name, "arguments": kwargs})
        text = result["content"][0]["text"]
        return json.loads(text)

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=10)


def test_all():
    passed = 0
    failed = 0
    skipped = 0
    results = []

    def run_test(name, fn):
        nonlocal passed, failed, skipped
        try:
            result = fn()
            passed += 1
            results.append(("PASS", name, result))
            print(f"  PASS  {name}: {result}")
        except Exception as e:
            err = str(e)
            if "skip" in err.lower():
                skipped += 1
                results.append(("SKIP", name, err))
                print(f"  SKIP  {name}: {err}")
            else:
                failed += 1
                results.append(("FAIL", name, err))
                print(f"  FAIL  {name}: {err}")

    print("Starting wasabi-mcp integration tests...")
    print()

    client = MCPClient()

    # --- Bucket tools ---
    print("Bucket Tools:")
    test_bucket = None
    test_bucket_region = None

    def test_list_buckets():
        nonlocal test_bucket, test_bucket_region
        r = client.tool("list_buckets")
        assert r["count"] > 0, "No buckets found"
        test_bucket = r["buckets"][0]["name"]
        test_bucket_region = r["buckets"][0]["region"]
        return f"{r['count']} buckets, first: {test_bucket} ({test_bucket_region})"

    run_test("list_buckets", test_list_buckets)

    def test_get_bucket_info():
        r = client.tool("get_bucket_info", bucket=test_bucket)
        assert r["name"] == test_bucket
        return f"region={r['region']}, versioning={r['versioning']}"

    run_test("get_bucket_info", test_get_bucket_info)

    def test_get_bucket_policy():
        r = client.tool("get_bucket_policy", bucket=test_bucket)
        return f"has_policy={r['has_policy']}"

    run_test("get_bucket_policy", test_get_bucket_policy)

    def test_get_bucket_lifecycle():
        r = client.tool("get_bucket_lifecycle", bucket=test_bucket)
        return f"has_lifecycle={r['has_lifecycle']}, rules={len(r['rules'])}"

    run_test("get_bucket_lifecycle", test_get_bucket_lifecycle)

    def test_get_bucket_tags():
        r = client.tool("get_bucket_tags", bucket=test_bucket)
        return f"has_tags={r['has_tags']}, tags={r['tags']}"

    run_test("get_bucket_tags", test_get_bucket_tags)

    def test_get_bucket_acl():
        r = client.tool("get_bucket_acl", bucket=test_bucket)
        assert "owner" in r
        return f"owner={r['owner']['display_name']}, grants={len(r['grants'])}"

    run_test("get_bucket_acl", test_get_bucket_acl)

    def test_get_bucket_cors():
        r = client.tool("get_bucket_cors", bucket=test_bucket)
        return f"has_cors={r['has_cors']}, rules={len(r['cors_rules'])}"

    run_test("get_bucket_cors", test_get_bucket_cors)

    def test_get_bucket_logging():
        r = client.tool("get_bucket_logging", bucket=test_bucket)
        return f"has_logging={r['has_logging']}"

    run_test("get_bucket_logging", test_get_bucket_logging)

    def test_get_bucket_replication():
        r = client.tool("get_bucket_replication", bucket=test_bucket)
        return f"has_replication={r['has_replication']}"

    run_test("get_bucket_replication", test_get_bucket_replication)

    # --- Object tools ---
    print("\nObject Tools:")
    test_key = None

    def test_list_objects():
        nonlocal test_key
        r = client.tool("list_objects", bucket=test_bucket, max_keys=5)
        if r["count"] > 0:
            test_key = r["objects"][0]["key"]
        return f"{r['count']} objects, truncated={r['is_truncated']}"

    run_test("list_objects", test_list_objects)

    def test_list_prefixes():
        r = client.tool("list_prefixes", bucket=test_bucket)
        return f"{r['prefix_count']} prefixes, {r['object_count']} objects at root"

    run_test("list_prefixes", test_list_prefixes)

    def test_head_object():
        if not test_key:
            raise RuntimeError("Skip: no objects in test bucket")
        r = client.tool("head_object", bucket=test_bucket, key=test_key)
        return f"size={r['size']}, type={r['content_type']}"

    run_test("head_object", test_head_object)

    def test_search_objects():
        r = client.tool("search_objects", bucket=test_bucket, pattern="*", max_keys=3)
        return f"{r['count']} matches"

    run_test("search_objects", test_search_objects)

    def test_get_object_acl():
        if not test_key:
            raise RuntimeError("Skip: no objects in test bucket")
        r = client.tool("get_object_acl", bucket=test_bucket, key=test_key)
        return f"owner={r['owner']['display_name']}, grants={len(r['grants'])}"

    run_test("get_object_acl", test_get_object_acl)

    def test_get_object_tags():
        if not test_key:
            raise RuntimeError("Skip: no objects in test bucket")
        r = client.tool("get_object_tags", bucket=test_bucket, key=test_key)
        return f"has_tags={r['has_tags']}"

    run_test("get_object_tags", test_get_object_tags)

    def test_list_object_versions():
        r = client.tool("list_object_versions", bucket=test_bucket, max_keys=5)
        return f"{r['version_count']} versions, {r['delete_marker_count']} delete markers"

    run_test("list_object_versions", test_list_object_versions)

    def test_list_multipart_uploads():
        r = client.tool("list_multipart_uploads", bucket=test_bucket)
        return f"{r['count']} in-progress uploads"

    run_test("list_multipart_uploads", test_list_multipart_uploads)

    def test_generate_presigned_url():
        if not test_key:
            raise RuntimeError("Skip: no objects in test bucket")
        r = client.tool("generate_presigned_url", bucket=test_bucket, key=test_key, expires_in=300)
        assert r["url"].startswith("https://"), "URL should be HTTPS"
        return f"expires_in={r['expires_in_seconds']}s, url_length={len(r['url'])}"

    run_test("generate_presigned_url", test_generate_presigned_url)

    def test_get_object_retention():
        if not test_key:
            raise RuntimeError("Skip: no objects in test bucket")
        r = client.tool("get_object_retention", bucket=test_bucket, key=test_key)
        return f"has_retention={r['has_retention']}"

    run_test("get_object_retention", test_get_object_retention)

    def test_get_object_legal_hold():
        if not test_key:
            raise RuntimeError("Skip: no objects in test bucket")
        r = client.tool("get_object_legal_hold", bucket=test_bucket, key=test_key)
        return f"has_legal_hold={r['has_legal_hold']}"

    run_test("get_object_legal_hold", test_get_object_legal_hold)

    # --- IAM tools ---
    print("\nIAM Tools:")
    test_user = None
    test_policy_arn = None

    def test_list_users():
        nonlocal test_user
        r = client.tool("list_users", max_items=10)
        assert r["count"] > 0, "No IAM users found"
        test_user = r["users"][0]["username"]
        return f"{r['count']} users, truncated={r['is_truncated']}, first: {test_user}"

    run_test("list_users", test_list_users)

    def test_list_users_pagination():
        page1 = client.tool("list_users", max_items=5)
        assert page1["is_truncated"], "Need >5 users to test pagination"
        page2 = client.tool("list_users", max_items=5, marker=page1["next_marker"])
        assert page2["count"] > 0, "Second page should have results"
        assert page1["users"][0]["username"] != page2["users"][0]["username"], "Pages should differ"
        return f"page1={page1['count']}, page2={page2['count']}, different users confirmed"

    run_test("list_users_pagination", test_list_users_pagination)

    def test_get_user():
        r = client.tool("get_user", username=test_user)
        assert r["username"] == test_user
        return f"arn={r['arn']}"

    run_test("get_user", test_get_user)

    def test_list_access_keys():
        r = client.tool("list_access_keys", username=test_user)
        return f"{r['count']} keys"

    run_test("list_access_keys", test_list_access_keys)

    def test_list_groups():
        r = client.tool("list_groups")
        return f"{r['count']} groups"

    run_test("list_groups", test_list_groups)

    def test_list_group_members():
        groups = client.tool("list_groups")
        if groups["count"] == 0:
            raise RuntimeError("Skip: no groups to test")
        group = groups["groups"][0]["group_name"]
        r = client.tool("list_group_members", group_name=group)
        return f"{r['count']} members in {group}"

    run_test("list_group_members", test_list_group_members)

    def test_list_policies():
        nonlocal test_policy_arn
        r = client.tool("list_policies", scope="Local")
        if r["count"] > 0:
            test_policy_arn = r["policies"][0]["arn"]
        return f"{r['count']} local policies"

    run_test("list_policies", test_list_policies)

    def test_get_policy_document():
        if not test_policy_arn:
            raise RuntimeError("Skip: no policies to test")
        r = client.tool("get_policy_document", policy_arn=test_policy_arn)
        stmts = len(r["document"].get("Statement", []))
        return f"version_id={r['version_id']}, {stmts} statements"

    run_test("get_policy_document", test_get_policy_document)

    def test_list_user_policies():
        r = client.tool("list_user_policies", username=test_user)
        inline = len(r["inline_policies"])
        attached = len(r["attached_policies"])
        return f"{inline} inline, {attached} attached"

    run_test("list_user_policies", test_list_user_policies)

    def test_list_group_policies():
        groups = client.tool("list_groups")
        if groups["count"] == 0:
            raise RuntimeError("Skip: no groups to test")
        group = groups["groups"][0]["group_name"]
        r = client.tool("list_group_policies", group_name=group)
        inline = len(r["inline_policies"])
        attached = len(r["attached_policies"])
        return f"{inline} inline, {attached} attached for {group}"

    run_test("list_group_policies", test_list_group_policies)

    def test_list_entities_for_policy():
        if not test_policy_arn:
            raise RuntimeError("Skip: no policies to test")
        r = client.tool("list_entities_for_policy", policy_arn=test_policy_arn)
        return f"{len(r['users'])} users, {len(r['groups'])} groups, {len(r['roles'])} roles"

    run_test("list_entities_for_policy", test_list_entities_for_policy)

    def test_get_access_key_last_used():
        keys = client.tool("list_access_keys", username=test_user)
        if keys["count"] == 0:
            raise RuntimeError("Skip: no access keys to test")
        key_id = keys["keys"][0]["access_key_id"]
        r = client.tool("get_access_key_last_used", access_key_id=key_id)
        return f"last_used={r['last_used_date']}, service={r['service_name']}"

    run_test("get_access_key_last_used", test_get_access_key_last_used)

    def test_list_policy_versions():
        if not test_policy_arn:
            raise RuntimeError("Skip: no policies to test")
        r = client.tool("list_policy_versions", policy_arn=test_policy_arn)
        return f"{r['count']} versions"

    run_test("list_policy_versions", test_list_policy_versions)

    # --- Index tools ---
    print("\nIndex Tools:")

    def test_index_bucket():
        r = client.tool("index_bucket", bucket=test_bucket, prefix="", force_full=False)
        return f"scanned={r['objects_scanned']}, upserted={r['objects_upserted']}, {r['duration_seconds']}s"

    run_test("index_bucket", test_index_bucket)

    def test_search_index():
        # Search for anything
        r = client.tool("search_index", query="*", limit=3)
        return f"{r['count']} results, {r['total_indexed']} total indexed"

    run_test("search_index", test_search_index)

    def test_index_stats():
        r = client.tool("index_stats")
        return f"{r['total_objects']} total, {len(r['buckets'])} buckets, db={r['db_size_bytes']} bytes"

    run_test("index_stats", test_index_stats)

    def test_index_prefix_tree():
        r = client.tool("index_prefix_tree", bucket=test_bucket, depth=2)
        return f"{len(r['tree'])} prefixes at depth {r['depth']}"

    run_test("index_prefix_tree", test_index_prefix_tree)

    def test_drop_index():
        r = client.tool("drop_index", bucket=test_bucket)
        return f"dropped {r['objects_removed']} objects from {r['dropped_bucket']}"

    run_test("drop_index", test_drop_index)

    client.close()

    # Summary
    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {skipped} skipped")
    print(f"Total:   {passed + failed + skipped} / 25 tools tested")

    if failed:
        print("\nFailed tests:")
        for status, name, detail in results:
            if status == "FAIL":
                print(f"  {name}: {detail}")

    return failed == 0


if __name__ == "__main__":
    success = test_all()
    sys.exit(0 if success else 1)
