"""Runtime state for MCP tools (tenant isolation + backing stores)."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from akc.compile.interfaces import Index, IndexDocument, IndexQuery, TenantRepoScope
from akc.compile.vectorstore_index_adapter import VectorStoreIndexAdapter
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.ingest.embedding import Embedder
from akc.ingest.index import InMemoryVectorStore, VectorStore, build_index
from akc.ingest.pipeline import IndexBackend, build_vector_store
from akc.memory.models import normalize_repo_id

from .guards import ensure_tenant_allowed, ensure_tool_token, normalize_allowed_tenants


@dataclass(frozen=True, slots=True)
class McpServeState:
    """Shared read-only services for MCP tool handlers."""

    outputs_root: Path
    allowed_tenants: frozenset[str] | None
    expected_tool_token: str | None
    skip_tool_token_check: bool
    index_adapter: Index | None
    max_hit_content_chars: int

    def authorize(self, *, tenant_id: str, tool_token: str | None) -> None:
        ensure_tenant_allowed(allowed=self.allowed_tenants, tenant_id=tenant_id)
        if not self.skip_tool_token_check:
            ensure_tool_token(expected=self.expected_tool_token, provided=tool_token)

    def operations_index(self, *, tenant_id: str) -> OperationsIndex:
        db = operations_sqlite_path(outputs_root=self.outputs_root, tenant_id=tenant_id)
        return OperationsIndex(sqlite_path=db)

    def query_index(
        self,
        *,
        tenant_id: str,
        query_text: str,
        k: int,
        repo_id: str | None,
    ) -> Sequence[IndexDocument]:
        if self.index_adapter is None:
            raise RuntimeError("vector index is not configured for this server (set --index-backend / sqlite path)")
        # Vector search is keyed by tenant_id; repo scoping is optional metadata filter only.
        scope = TenantRepoScope(tenant_id=tenant_id.strip(), repo_id="_")
        filters = None
        if repo_id is not None and str(repo_id).strip():
            filters = {"repo_id": normalize_repo_id(str(repo_id).strip())}
        return self.index_adapter.query(
            scope=scope,
            query=IndexQuery(text=str(query_text).strip(), k=int(k), filters=filters),
        )


def build_mcp_serve_state(
    *,
    outputs_root: Path,
    allowed_tenant_ids: Sequence[str] | None,
    expected_tool_token: str | None,
    skip_tool_token_check: bool,
    index_backend: IndexBackend,
    sqlite_path: str | None,
    pg_dsn: str | None,
    pg_dimension: int | None,
    pg_table: str,
    embedder: Embedder | None,
    max_hit_content_chars: int,
) -> McpServeState:
    allowed = normalize_allowed_tenants(tuple(allowed_tenant_ids) if allowed_tenant_ids else None)

    vs: VectorStore
    if index_backend == "memory":
        vs = InMemoryVectorStore()
    else:
        vs = build_vector_store(
            backend=index_backend,
            sqlite_path=sqlite_path,
            pg_dsn=pg_dsn,
            pg_dimension=pg_dimension,
            pg_table=pg_table,
        )
    ingest_index = build_index(vector_store=vs)
    index_adapter: Index | None = (
        None if embedder is None else VectorStoreIndexAdapter(index=ingest_index, embedder=embedder)
    )

    exp_tok: str | None = None
    if expected_tool_token and str(expected_tool_token).strip():
        exp_tok = str(expected_tool_token).strip()

    return McpServeState(
        outputs_root=outputs_root.resolve(),
        allowed_tenants=allowed,
        expected_tool_token=exp_tok,
        skip_tool_token_check=skip_tool_token_check,
        index_adapter=index_adapter,
        max_hit_content_chars=max(256, int(max_hit_content_chars)),
    )
