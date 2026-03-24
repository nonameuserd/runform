"""``akc mcp serve`` — expose read-only AKC tools over MCP."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

from akc.cli.common import configure_logging


def cmd_mcp_serve(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))

    try:
        from akc.mcp_serve.runner import run_mcp_serve
    except ImportError as e:
        print(
            "The `mcp` package is required for `akc mcp serve`. "
            "Install with: uv sync --extra mcp-serve",
            file=sys.stderr,
        )
        print(str(e), file=sys.stderr)
        return 2

    backend = str(getattr(args, "index_backend", "sqlite"))
    if backend == "sqlite":
        sp = getattr(args, "sqlite_path", None)
        if sp is None or not str(sp).strip():
            args.sqlite_path = str(Path.cwd() / ".akc" / "ingest" / "index.sqlite3")
    if backend == "pgvector":
        if not getattr(args, "pg_dsn", None) or not str(args.pg_dsn).strip():
            print("error: --pg-dsn is required when --index-backend pgvector", file=sys.stderr)
            return 2
        if getattr(args, "pg_dimension", None) is None:
            print("error: --pg-dimension is required when --index-backend pgvector", file=sys.stderr)
            return 2

    embedder = str(getattr(args, "embedder", "hash"))
    if backend != "memory" and embedder in ("none", ""):
        print(
            "error: vector index backend requires an embedder (use --embedder hash or configure openai/gemini)",
            file=sys.stderr,
        )
        return 2

    try:
        return int(run_mcp_serve(args))
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2


def register_mcp_parser(sub: Any) -> None:
    mcp = sub.add_parser(
        "mcp",
        help="Run AKC as an MCP server (read-only tools; requires mcp-serve extra)",
    )
    mcp_sub = mcp.add_subparsers(dest="mcp_command", required=True)

    serve = mcp_sub.add_parser(
        "serve",
        help="Start MCP server (stdio default, or streamable HTTP / SSE)",
    )
    serve.add_argument("--verbose", action="store_true", help="Enable debug logging")
    serve.add_argument(
        "--transport",
        choices=["stdio", "streamable-http", "sse"],
        default=os.environ.get("AKC_MCP_TRANSPORT", "stdio"),
        help="Transport (default: stdio, or AKC_MCP_TRANSPORT)",
    )
    serve.add_argument(
        "--host",
        default=os.environ.get("AKC_MCP_HOST", "127.0.0.1"),
        help="Bind address for HTTP transports (default: 127.0.0.1)",
    )
    serve.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("AKC_MCP_PORT", "8765")),
        help="Port for HTTP transports (default: 8765)",
    )
    serve.add_argument(
        "--streamable-http-path",
        default=os.environ.get("AKC_MCP_STREAMABLE_PATH", "/mcp"),
        help="Streamable HTTP path (default: /mcp)",
    )
    serve.add_argument(
        "--sse-mount-path",
        default=None,
        help="Optional mount path for SSE transport",
    )
    serve.add_argument(
        "--outputs-root",
        default=os.environ.get("AKC_OUTPUTS_ROOT", "."),
        help="Outputs root containing <tenant>/.akc/control/operations.sqlite (default: CWD)",
    )
    serve.add_argument(
        "--allow-tenant",
        action="append",
        default=None,
        dest="allow_tenant",
        metavar="TENANT_ID",
        help="Repeatable tenant allowlist (also AKC_MCP_ALLOWED_TENANTS=comma-separated)",
    )
    serve.add_argument(
        "--tool-token",
        default=None,
        help="Optional shared secret; callers must pass tool_token (or set AKC_MCP_TOOL_TOKEN)",
    )
    serve.add_argument(
        "--http-bearer-token",
        default=None,
        help="Optional bearer for streamable HTTP/SSE (AKC_MCP_HTTP_BEARER_TOKEN); disables tool_token requirement",
    )
    serve.add_argument(
        "--index-backend",
        choices=["memory", "sqlite", "pgvector"],
        default=os.environ.get("AKC_MCP_INDEX_BACKEND", "sqlite"),
        help="Vector index backend for akc_query_index (default: sqlite)",
    )
    serve.add_argument(
        "--sqlite-path",
        default=os.environ.get("AKC_MCP_INDEX_SQLITE"),
        help="SQLite vector index path (default: .akc/ingest/index.sqlite3 under CWD when unset)",
    )
    serve.add_argument("--pg-dsn", default=os.environ.get("AKC_PG_DSN"), help="Postgres DSN for pgvector backend")
    serve.add_argument("--pg-dimension", type=int, default=None, help="Embedding dimension for pgvector")
    serve.add_argument("--pg-table", default="akc_documents", help="Postgres table for pgvector")
    serve.add_argument(
        "--embedder",
        choices=["none", "hash", "openai", "gemini"],
        default=os.environ.get("AKC_MCP_EMBEDDER", "hash"),
        help="Embedder for index queries (default: hash)",
    )
    serve.add_argument("--hash-embedder-dimension", type=int, default=None, help="Hash embedder dimension override")
    serve.add_argument("--openai-api-key", default=None, help="OpenAI API key (or AKC_OPENAI_API_KEY)")
    serve.add_argument("--openai-base-url", default=None, help="OpenAI-compatible base URL")
    serve.add_argument("--openai-model", default=None, help="OpenAI embedding model id")
    serve.add_argument("--gemini-api-key", default=None, help="Gemini API key (or AKC_GEMINI_API_KEY)")
    serve.add_argument("--gemini-base-url", default=None, help="Gemini base URL")
    serve.add_argument("--gemini-model", default=None, help="Gemini embedding model id")
    serve.add_argument(
        "--max-hit-content-chars",
        type=int,
        default=8000,
        help="Truncate indexed document text in tool responses (default: 8000)",
    )
    serve.set_defaults(func=cmd_mcp_serve)
