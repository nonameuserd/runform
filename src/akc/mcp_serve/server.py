"""FastMCP server exposing read-only AKC tools."""

from __future__ import annotations

import hmac
import json
from typing import Any, cast

from mcp.server.auth.provider import AccessToken, TokenVerifier
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations
from pydantic import AnyHttpUrl

from akc.compile.interfaces import IndexDocument

from .state import McpServeState

_READ_ONLY = True


class StaticBearerTokenVerifier:
    """Minimal bearer verifier for streamable HTTP (shared secret)."""

    def __init__(self, secret: str) -> None:
        self._secret = secret.encode("utf-8")

    async def verify_token(self, token: str) -> AccessToken | None:
        raw = token.encode("utf-8")
        if len(raw) != len(self._secret):
            return None
        if not hmac.compare_digest(raw, self._secret):
            return None
        return AccessToken(
            token=token,
            client_id="akc-mcp-bearer",
            scopes=["akc.read"],
        )


def _truncate(text: str, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n[truncated]"


def _doc_payload(doc: IndexDocument, max_content: int) -> dict[str, Any]:
    md = doc.metadata or {}
    return {
        "doc_id": doc.doc_id,
        "title": doc.title,
        "score": doc.score,
        "content": _truncate(doc.content, max_content),
        "metadata": dict(md),
    }


def build_fastmcp_server(
    *,
    state: McpServeState,
    host: str,
    port: int,
    streamable_http_path: str,
    http_bearer_token: str | None,
) -> FastMCP:
    """Construct a :class:`FastMCP` app wired to ``state``."""

    annotations = ToolAnnotations(readOnlyHint=_READ_ONLY)

    auth_settings: AuthSettings | None = None
    token_verifier: TokenVerifier | None = None
    if http_bearer_token is not None and str(http_bearer_token).strip():
        secret = str(http_bearer_token).strip()
        token_verifier = StaticBearerTokenVerifier(secret)
        base = f"http://{host}:{port}".rstrip("/")
        path = streamable_http_path if streamable_http_path.startswith("/") else f"/{streamable_http_path}"
        auth_settings = AuthSettings(
            issuer_url=cast(AnyHttpUrl, f"{base}/"),
            resource_server_url=cast(AnyHttpUrl, f"{base}{path}"),
            required_scopes=None,
        )

    mcp = FastMCP(
        name="akc",
        instructions=(
            "Read-only Agentic Knowledge Compiler (AKC) tools: query a tenant-scoped vector index, "
            "list recent compile runs from the operations index, and fetch compile status for one run. "
            "Always pass the correct tenant_id; when the server sets AKC_MCP_TOOL_TOKEN, include tool_token."
        ),
        host=host,
        port=port,
        streamable_http_path=streamable_http_path,
        auth=auth_settings,
        token_verifier=token_verifier,
    )

    @mcp.tool(
        name="akc_query_index",
        description="Semantic search over ingested documents for a tenant.",
        annotations=annotations,
    )
    def akc_query_index(
        tenant_id: str,
        query: str,
        k: int = 10,
        repo_id: str | None = None,
        tool_token: str | None = None,
    ) -> str:
        state.authorize(tenant_id=tenant_id, tool_token=tool_token)
        hits = state.query_index(tenant_id=tenant_id, query_text=query, k=k, repo_id=repo_id)
        payload = {
            "tenant_id": tenant_id.strip(),
            "k_requested": int(k),
            "hits": [_doc_payload(d, state.max_hit_content_chars) for d in hits],
        }
        return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)

    @mcp.tool(
        name="akc_list_recent_runs",
        description="List recent compile runs from the tenant operations index (newest first).",
        annotations=annotations,
    )
    def akc_list_recent_runs(
        tenant_id: str,
        limit: int = 20,
        repo_id: str | None = None,
        tool_token: str | None = None,
    ) -> str:
        state.authorize(tenant_id=tenant_id, tool_token=tool_token)
        idx = state.operations_index(tenant_id=tenant_id)
        rows = idx.list_runs(
            tenant_id=tenant_id.strip(),
            repo_id=str(repo_id).strip() if repo_id and str(repo_id).strip() else None,
            limit=min(max(1, int(limit)), 500),
        )
        return json.dumps(
            {"tenant_id": tenant_id.strip(), "runs": rows},
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        )

    @mcp.tool(
        name="akc_get_compile_status",
        description="Return indexed status fields and pointers for one compile run.",
        annotations=annotations,
    )
    def akc_get_compile_status(
        tenant_id: str,
        repo_id: str,
        run_id: str,
        tool_token: str | None = None,
    ) -> str:
        state.authorize(tenant_id=tenant_id, tool_token=tool_token)
        idx = state.operations_index(tenant_id=tenant_id)
        row = idx.get_run(tenant_id=tenant_id.strip(), repo_id=repo_id, run_id=run_id)
        if row is None:
            return json.dumps(
                {"found": False, "tenant_id": tenant_id.strip(), "repo_id": repo_id, "run_id": run_id},
                ensure_ascii=False,
                sort_keys=True,
            )
        return json.dumps({"found": True, "run": row}, ensure_ascii=False, sort_keys=True, default=str)

    return mcp
