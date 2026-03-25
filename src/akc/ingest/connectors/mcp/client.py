"""MCP client session helpers (stdio + streamable HTTP)."""

from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from mcp import ClientSession

from akc.ingest.exceptions import ConnectorError

from .config import ResolvedMcpServer, validate_mcp_http_url

T = TypeVar("T")


def _merge_stdio_env(server_env: Mapping[str, str] | None) -> dict[str, str] | None:
    if server_env is None:
        return None
    merged = dict(os.environ)
    merged.update(dict(server_env))
    return merged


async def _with_stdio_session(
    server: ResolvedMcpServer,
    op: Callable[[ClientSession], Awaitable[T]],
) -> T:
    try:
        from mcp import ClientSession
        from mcp.client.stdio import StdioServerParameters, stdio_client
    except ImportError as e:  # pragma: no cover
        raise ConnectorError(
            "The 'mcp' package is not installed. Install akc with the ingest-mcp extra, "
            "for example: uv sync --extra ingest-mcp"
        ) from e

    if not server.command:
        raise ConnectorError("mcp stdio transport requires 'command'")

    params = StdioServerParameters(
        command=server.command,
        args=list(server.args),
        env=_merge_stdio_env(server.env),
        cwd=server.cwd,
    )
    async with stdio_client(params) as streams, ClientSession(streams[0], streams[1]) as session:
        await session.initialize()
        return await op(session)


async def _with_http_session(
    server: ResolvedMcpServer,
    op: Callable[[ClientSession], Awaitable[T]],
) -> T:
    try:
        import httpx
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared.version import SUPPORTED_PROTOCOL_VERSIONS
    except ImportError as e:  # pragma: no cover
        raise ConnectorError(
            "The 'mcp' package (and httpx) are required for MCP HTTP transport. Install akc with the ingest-mcp extra."
        ) from e

    if not server.url:
        raise ConnectorError("mcp http transport requires 'url'")
    validate_mcp_http_url(server.url)

    proto = server.protocol_version or SUPPORTED_PROTOCOL_VERSIONS[-1]
    headers: dict[str, str] = {"MCP-Protocol-Version": proto}
    if server.auth_header_env:
        token = os.environ.get(server.auth_header_env, "")
        if token.strip():
            headers["Authorization"] = token.strip()

    timeout = httpx.Timeout(connect=30.0, read=120.0, write=60.0, pool=10.0)
    async with (
        httpx.AsyncClient(headers=headers, timeout=timeout, follow_redirects=False) as http_client,
        streamable_http_client(server.url, http_client=http_client) as streams,
    ):
        read_stream, write_stream, _get_id = streams
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            return await op(session)


async def run_mcp_session(
    server: ResolvedMcpServer,
    op: Callable[[ClientSession], Awaitable[T]],
) -> T:
    if server.transport == "stdio":
        return await _with_stdio_session(server, op)
    if server.transport == "http":
        return await _with_http_session(server, op)
    raise ConnectorError(f"unknown mcp transport: {server.transport!r}")


def run_mcp_session_sync(
    server: ResolvedMcpServer,
    op: Callable[[ClientSession], Awaitable[T]],
    *,
    timeout_s: float | None,
) -> T:
    """Run *op* against a fresh MCP session (stdio or HTTP)."""

    async def _runner() -> T:
        return await run_mcp_session(server, op)

    if timeout_s is None or timeout_s <= 0:
        return asyncio.run(_runner())
    try:
        return asyncio.run(asyncio.wait_for(_runner(), timeout=timeout_s))
    except TimeoutError as e:
        raise ConnectorError(f"mcp session timed out after {timeout_s}s") from e


async def list_all_resources(session: ClientSession) -> list[object]:
    """Paginated ``resources/list``."""

    from mcp.types import ListResourcesResult

    out: list[object] = []
    cursor: str | None = None
    while True:
        result: ListResourcesResult = await session.list_resources(cursor=cursor)
        out.extend(result.resources)
        cursor = result.nextCursor
        if not cursor:
            break
    return out


def read_resource_contents_to_parts(
    read_result: object,
) -> Sequence[object]:
    """Return the ``contents`` sequence from a ``ReadResourceResult``."""

    contents = getattr(read_result, "contents", None)
    if not isinstance(contents, list):
        return []
    return contents
