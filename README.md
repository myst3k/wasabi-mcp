# wasabi-mcp

MCP server for [Wasabi](https://wasabi.com) cloud storage. Browse buckets, inspect objects, manage IAM users and policies, and search across your storage with a local full-text index.

37 tools across S3, IAM, and local search — designed to give AI assistants full visibility into your Wasabi account. All tools support pagination for large result sets.

## Features

- **Multi-region routing** — automatically discovers each bucket's region and connects to the correct Wasabi endpoint
- **Extended bucket metadata** — uses Wasabi's API extensions to get region, public access state, and versioning in a single call
- **Local search index** — index buckets into SQLite with FTS5 full-text search for instant queries across millions of object keys
- **IAM visibility** — list users, groups, policies, access keys, and inspect policy documents
- **Zero configuration** — works with existing `~/.aws/credentials`, or pass credentials directly

## Installation

### Claude Code

```bash
claude mcp add wasabi-mcp -- uvx --from git+https://github.com/myst3k/wasabi-mcp wasabi-mcp
```

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "wasabi": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/myst3k/wasabi-mcp", "wasabi-mcp"],
      "env": {
        "WASABI_ACCESS_KEY_ID": "your-access-key",
        "WASABI_SECRET_ACCESS_KEY": "your-secret-key"
      }
    }
  }
}
```

If you already have Wasabi credentials in `~/.aws/credentials` (default profile), you can omit the `env` block entirely:

```json
{
  "mcpServers": {
    "wasabi": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/myst3k/wasabi-mcp", "wasabi-mcp"]
    }
  }
}
```

<details>
<summary>Config file locations</summary>

| OS | Path |
|----|------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/Claude/claude_desktop_config.json` |

</details>

### ChatGPT (via MCP bridge)

ChatGPT doesn't natively support MCP servers, but you can use an MCP-to-OpenAI bridge like [mcp-openai](https://github.com/nichochar/mcp-openai) or [mcpx](https://github.com/sidhyaashu/mcpx):

```bash
# Install and run the bridge with wasabi-mcp
uvx --from git+https://github.com/myst3k/wasabi-mcp wasabi-mcp
```

Refer to your bridge's documentation for connecting it to ChatGPT custom actions.

### Local Development

```bash
git clone https://github.com/myst3k/wasabi-mcp.git
cd wasabi-mcp
uv sync

# Run the server
uv run wasabi-mcp

# Run integration tests (requires Wasabi credentials)
uv run python test_live.py
```

For local dev with Claude Desktop:

```json
{
  "mcpServers": {
    "wasabi": {
      "command": "uv",
      "args": ["run", "--directory", "/path/to/wasabi-mcp", "wasabi-mcp"]
    }
  }
}
```

## Configuration

All settings are optional. The server works out of the box if you have Wasabi credentials in `~/.aws/credentials`.

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `WASABI_ACCESS_KEY_ID` | — | Wasabi access key (checked first) |
| `WASABI_SECRET_ACCESS_KEY` | — | Wasabi secret key (checked first) |
| `AWS_PROFILE` | `default` | AWS credentials profile (fallback if no direct keys) |
| `WASABI_REGION` | `us-east-1` | Default region for initial API connection |
| `WASABI_IAM_ENDPOINT` | `https://iam.wasabisys.com` | IAM endpoint override |
| `WASABI_INDEX_DB_PATH` | platform-specific | SQLite index database path |

**Default index database location:**

| OS | Path |
|----|------|
| macOS | `~/Library/Application Support/wasabi-mcp/index.db` |
| Windows | `%LOCALAPPDATA%/wasabi-mcp/index.db` |
| Linux | `~/.local/share/wasabi-mcp/index.db` |

## Tools

### S3 Bucket Tools (9)

| Tool | Description |
|------|-------------|
| `list_buckets` | List all buckets with region, public access, versioning |
| `get_bucket_info` | Bucket location and versioning status |
| `get_bucket_policy` | Parsed JSON bucket policy |
| `get_bucket_lifecycle` | Lifecycle configuration rules |
| `get_bucket_tags` | Bucket tags as key-value pairs |
| `get_bucket_acl` | Bucket owner and access grants |
| `get_bucket_cors` | CORS configuration rules |
| `get_bucket_logging` | Server access logging configuration |
| `get_bucket_replication` | Cross-region replication rules |

### S3 Object Tools (11)

| Tool | Description |
|------|-------------|
| `list_objects` | List objects with pagination |
| `list_prefixes` | Browse bucket like a directory (folder listing) |
| `head_object` | Object metadata without downloading content |
| `search_objects` | Search by prefix + glob pattern (e.g. `*.jpg`, `backup-202?-*`) |
| `get_object_acl` | Object-level access control list |
| `get_object_tags` | Object tags |
| `list_object_versions` | List all versions in versioned buckets |
| `list_multipart_uploads` | Find in-progress or stuck multipart uploads |
| `generate_presigned_url` | Create temporary shareable download URLs |
| `get_object_retention` | Object Lock retention status (compliance/governance) |
| `get_object_legal_hold` | Object Lock legal hold status |

### IAM Tools (12)

| Tool | Description |
|------|-------------|
| `list_users` | All IAM users with pagination |
| `get_user` | Detailed user info with tags |
| `list_access_keys` | Access keys for a user |
| `get_access_key_last_used` | When a key was last used (security audit) |
| `list_groups` | All IAM groups with pagination |
| `list_group_members` | Users in a group |
| `list_group_policies` | Inline + attached policies for a group |
| `list_policies` | Managed policies with pagination |
| `get_policy_document` | Actual JSON policy document |
| `list_policy_versions` | Version history of a policy |
| `list_user_policies` | Inline + attached policies for a user |
| `list_entities_for_policy` | Which users/groups/roles use a policy |

### Local Index Tools (5)

| Tool | Description |
|------|-------------|
| `index_bucket` | Scan a bucket and populate the local search index |
| `search_index` | Full-text search across indexed object keys (with offset/limit) |
| `index_stats` | Index statistics: counts, sizes, last sync times |
| `index_prefix_tree` | Directory tree with object counts from index |
| `drop_index` | Clear indexed data for a bucket or all |

## How the Index Works

The local index stores object metadata (key, size, last modified, etag, storage class) in a SQLite database with FTS5 full-text search.

**Indexing:** `index_bucket` scans a bucket via S3 ListObjectsV2 and stores the results locally. Subsequent calls are incremental — only objects modified since the last scan are updated. Use `force_full=true` to detect deleted objects.

**Searching:** Object keys are tokenized by path separator (`/`), so searching for "vacation" instantly finds `photos/2024/vacation/img001.jpg`. You can also filter by bucket, size range, and date range.

**Example flow:**

```
You: Index my backup bucket
AI:  → calls index_bucket(bucket="company-backups")
     Indexed 142,387 objects in 12.3s

You: Find any SQL dumps from March
AI:  → calls search_index(query="sql dump", modified_after="2024-03-01", modified_before="2024-04-01")
     Found 3 matches: ...
```

For very large buckets, use the `prefix` parameter to index subtrees:

```
You: Index just the 2024 logs
AI:  → calls index_bucket(bucket="company-backups", prefix="logs/2024/")
```

## Multi-Region Support

Wasabi requires connecting to the region-specific S3 endpoint for each bucket. This server handles it automatically:

1. On startup, fetches the full region map from Wasabi's `?describeRegions` endpoint
2. `list_buckets` uses Wasabi's `?publicAccess&location` extension to discover every bucket's region in a single call
3. All per-bucket operations automatically route to the correct regional endpoint (e.g., `s3.eu-central-1.wasabisys.com`)
4. Regional S3 clients are cached and reused

You never need to specify a region — the server figures it out.

## Requirements

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (for `uvx` installation)
- Wasabi account with access keys

## License

Apache-2.0
