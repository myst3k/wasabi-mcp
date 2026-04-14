from __future__ import annotations

import platform
import sys
from typing import Any

from wasabi_mcp import __version__
from wasabi_mcp.server import mcp


@mcp.tool()
async def server_info() -> dict[str, Any]:
    """Return version and runtime info for the Wasabi MCP server.

    Useful for confirming which version of the server is running.
    """
    return {
        "version": __version__,
        "python": sys.version,
        "platform": platform.platform(),
    }
