from __future__ import annotations

import argparse
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any

import aiosqlite
from mcp.server.fastmcp import FastMCP

from wasabi_mcp.clients import ClientManager
from wasabi_mcp.config import WasabiConfig
from wasabi_mcp.index.db import init_database

logging.basicConfig(level=logging.INFO, format="%(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


@dataclass
class AppContext:
    clients: ClientManager
    db: aiosqlite.Connection
    config: WasabiConfig


_cli_profile: str | None = None


@asynccontextmanager
async def app_lifespan(server: FastMCP) -> AsyncIterator[AppContext]:
    config = WasabiConfig.from_env(profile=_cli_profile)
    clients = ClientManager(config)

    config.index_db_path.parent.mkdir(parents=True, exist_ok=True)
    db = await init_database(config.index_db_path)

    await clients.fetch_region_map()

    logger.info("Wasabi MCP server started")
    logger.info(f"Default region: {config.region}")
    logger.info(f"Index DB: {config.index_db_path}")

    try:
        yield AppContext(clients=clients, db=db, config=config)
    finally:
        await db.close()
        clients.close()
        logger.info("Wasabi MCP server stopped")


mcp = FastMCP(
    name="Wasabi MCP",
    lifespan=app_lifespan,
)

# Import tools so they register with the mcp instance
import wasabi_mcp.tools  # noqa: E402, F401


def main() -> None:
    global _cli_profile
    parser = argparse.ArgumentParser(description="Wasabi MCP Server")
    parser.add_argument("--profile", help="AWS/Wasabi config profile name to use for credentials")
    args = parser.parse_args()
    _cli_profile = args.profile
    mcp.run(transport="stdio")
