"""CLI entrypoint for ``akc mcp serve``."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import cast

from akc.ingest.pipeline import IndexBackend

from .embedder import build_embedder
from .server import build_fastmcp_server
from .state import build_mcp_serve_state


def run_mcp_serve(args: argparse.Namespace) -> int:
    outputs_root = Path(str(args.outputs_root)).expanduser().resolve()
    allow = list(getattr(args, "allow_tenant", None) or [])
    env_allow = os.environ.get("AKC_MCP_ALLOWED_TENANTS", "").strip()
    if env_allow:
        allow.extend(x.strip() for x in env_allow.split(",") if x.strip())

    tool_token = getattr(args, "tool_token", None)
    if tool_token is None or not str(tool_token).strip():
        tool_token = os.environ.get("AKC_MCP_TOOL_TOKEN")
    http_bearer = getattr(args, "http_bearer_token", None)
    env_http = os.environ.get("AKC_MCP_HTTP_BEARER_TOKEN")
    if env_http and env_http.strip() and (http_bearer is None or not str(http_bearer).strip()):
        http_bearer = env_http.strip()

    http_bearer_s = str(http_bearer).strip() if http_bearer and str(http_bearer).strip() else None
    skip_tool = http_bearer_s is not None

    embedder = build_embedder(
        name=str(args.embedder),
        openai_api_key=getattr(args, "openai_api_key", None),
        openai_base_url=getattr(args, "openai_base_url", None),
        openai_model=getattr(args, "openai_model", None),
        gemini_api_key=getattr(args, "gemini_api_key", None),
        gemini_base_url=getattr(args, "gemini_base_url", None),
        gemini_model=getattr(args, "gemini_model", None),
        hash_dimension=getattr(args, "hash_embedder_dimension", None),
    )

    backend = cast(IndexBackend, str(args.index_backend))
    state = build_mcp_serve_state(
        outputs_root=outputs_root,
        allowed_tenant_ids=allow or None,
        expected_tool_token=str(tool_token).strip() if tool_token and str(tool_token).strip() else None,
        skip_tool_token_check=skip_tool,
        index_backend=backend,
        sqlite_path=str(args.sqlite_path).strip() if getattr(args, "sqlite_path", None) else None,
        pg_dsn=str(args.pg_dsn).strip() if getattr(args, "pg_dsn", None) else None,
        pg_dimension=int(args.pg_dimension) if getattr(args, "pg_dimension", None) is not None else None,
        pg_table=str(getattr(args, "pg_table", "akc_documents")),
        embedder=embedder,
        max_hit_content_chars=int(getattr(args, "max_hit_content_chars", 8000)),
    )

    transport = str(getattr(args, "transport", "stdio"))
    host = str(getattr(args, "host", "127.0.0.1"))
    port = int(getattr(args, "port", 8765))
    stream_path = str(getattr(args, "streamable_http_path", "/mcp"))

    app = build_fastmcp_server(
        state=state,
        host=host,
        port=port,
        streamable_http_path=stream_path,
        http_bearer_token=http_bearer_s,
    )

    if transport == "stdio":
        app.run(transport="stdio")
    elif transport == "streamable-http":
        app.run(transport="streamable-http")
    elif transport == "sse":
        app.run(transport="sse", mount_path=getattr(args, "sse_mount_path", None))
    else:
        raise SystemExit(f"Unknown transport: {transport}")
    return 0
