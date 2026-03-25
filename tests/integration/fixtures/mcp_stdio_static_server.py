"""Minimal stdio MCP server exposing static text resources (for ingest integration tests)."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

mcp = FastMCP("akc-ingest-fixture")


@mcp.resource("test://hello")
def hello() -> str:
    return "hello from fixture"


@mcp.resource("test://other")
def other() -> str:
    return "other resource"


if __name__ == "__main__":
    mcp.run(transport="stdio")
