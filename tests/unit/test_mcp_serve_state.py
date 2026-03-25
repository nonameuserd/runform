"""Unit tests for MCP server index wiring (tenant isolation)."""

from __future__ import annotations

import json

import pytest

from akc.compile.vectorstore_index_adapter import VectorStoreIndexAdapter
from akc.ingest.embedding import HashEmbedder
from akc.ingest.index import InMemoryVectorStore, build_index
from akc.ingest.models import Document
from akc.mcp_serve.state import McpServeState


def test_query_index_filters_other_tenant(tmp_path) -> None:
    vs = InMemoryVectorStore()
    ingest_index = build_index(vector_store=vs)
    embedder = HashEmbedder(dimension=64)
    d1 = Document(
        id="1",
        content="hello planet alpha",
        metadata={
            "tenant_id": "t1",
            "source": "test",
            "source_type": "test",
            "repo_id": "r1",
        },
        embedding=tuple(embedder.embed(["hello planet alpha"])[0]),
    )
    d2 = Document(
        id="2",
        content="hello planet beta",
        metadata={
            "tenant_id": "t2",
            "source": "test",
            "source_type": "test",
            "repo_id": "r1",
        },
        embedding=tuple(embedder.embed(["hello planet beta"])[0]),
    )
    ingest_index.add(tenant_id="t1", documents=[d1])
    ingest_index.add(tenant_id="t2", documents=[d2])
    adapter = VectorStoreIndexAdapter(index=ingest_index, embedder=embedder)
    state = McpServeState(
        outputs_root=tmp_path,
        allowed_tenants=None,
        expected_tool_token=None,
        skip_tool_token_check=False,
        index_adapter=adapter,
        max_hit_content_chars=1000,
    )
    hits = state.query_index(tenant_id="t1", query_text="planet", k=5, repo_id=None)
    assert len(hits) == 1
    assert hits[0].doc_id == "1"


def test_authorize_allowlist_and_tool_token(tmp_path) -> None:
    state = McpServeState(
        outputs_root=tmp_path,
        allowed_tenants=frozenset({"t1"}),
        expected_tool_token="tok",
        skip_tool_token_check=False,
        index_adapter=None,
        max_hit_content_chars=1000,
    )
    with pytest.raises(PermissionError):
        state.authorize(tenant_id="t2", tool_token="tok")
    with pytest.raises(PermissionError):
        state.authorize(tenant_id="t1", tool_token=None)
    state.authorize(tenant_id="t1", tool_token="tok")


def test_build_fastmcp_tool_handlers_json(tmp_path) -> None:
    pytest.importorskip("mcp")
    from akc.mcp_serve.server import build_fastmcp_server

    vs = InMemoryVectorStore()
    ingest_index = build_index(vector_store=vs)
    embedder = HashEmbedder(dimension=64)
    doc = Document(
        id="d1",
        content="indexed content about widgets",
        metadata={"tenant_id": "tenant-a", "source": "test", "source_type": "test"},
        embedding=tuple(embedder.embed(["indexed content about widgets"])[0]),
    )
    ingest_index.add(tenant_id="tenant-a", documents=[doc])
    adapter = VectorStoreIndexAdapter(index=ingest_index, embedder=embedder)
    state = McpServeState(
        outputs_root=tmp_path,
        allowed_tenants=None,
        expected_tool_token=None,
        skip_tool_token_check=False,
        index_adapter=adapter,
        max_hit_content_chars=200,
    )
    app = build_fastmcp_server(
        state=state,
        host="127.0.0.1",
        port=8765,
        streamable_http_path="/mcp",
        http_bearer_token=None,
    )
    import asyncio

    async def _call() -> str:
        blocks = await app.call_tool(
            "akc_query_index",
            {"tenant_id": "tenant-a", "query": "widgets", "k": 3},
        )
        if isinstance(blocks, dict):
            return json.dumps(blocks)
        if isinstance(blocks, tuple) and len(blocks) == 2:
            payload, meta = blocks
            if isinstance(meta, dict):
                res = meta.get("result")
                if isinstance(res, str) and res.strip():
                    return res
            blocks = payload
        for item in blocks:
            text = getattr(item, "text", None)
            if isinstance(text, str) and text.strip():
                return text
        raise AssertionError(f"unexpected call_tool payload: {blocks!r}")

    out = asyncio.run(_call())
    data = json.loads(out)
    assert data["tenant_id"] == "tenant-a"
    assert len(data["hits"]) >= 1
    assert "widgets" in data["hits"][0]["content"]
