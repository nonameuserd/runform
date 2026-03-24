"""In-memory why graph store (Phase 2)."""

from __future__ import annotations

from collections.abc import Iterable, Iterator

from akc.memory.models import WhyEdge, WhyNode, WhyNodeType, json_dumps, require_non_empty
from akc.memory.why_graph_store_base import WhyGraphStore, require_scope


class InMemoryWhyGraphStore(WhyGraphStore):
    def __init__(self) -> None:
        # tenant -> repo -> node_id -> WhyNode
        self._nodes: dict[str, dict[str, dict[str, WhyNode]]] = {}
        # tenant -> repo -> src -> list[WhyEdge]
        self._out: dict[str, dict[str, dict[str, list[WhyEdge]]]] = {}

    def upsert_nodes(self, *, tenant_id: str, repo_id: str, nodes: Iterable[WhyNode]) -> int:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        bucket = self._nodes.setdefault(tenant_id, {}).setdefault(repo, {})
        wrote = 0
        for n in nodes:
            json_dumps(dict(n.payload))
            bucket[n.id] = n
            wrote += 1
        return wrote

    def add_edges(self, *, tenant_id: str, repo_id: str, edges: Iterable[WhyEdge]) -> int:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        out_bucket = self._out.setdefault(tenant_id, {}).setdefault(repo, {})
        wrote = 0
        for e in edges:
            if e.payload is not None:
                json_dumps(dict(e.payload))
            out_bucket.setdefault(e.src, []).append(e)
            wrote += 1
        return wrote

    def get_node(self, *, tenant_id: str, repo_id: str, node_id: str) -> WhyNode | None:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        require_non_empty(node_id, name="node_id")
        return self._nodes.get(tenant_id, {}).get(repo, {}).get(node_id)

    def iter_out_edges(self, *, tenant_id: str, repo_id: str, src: str) -> Iterator[WhyEdge]:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        require_non_empty(src, name="src")
        yield from self._out.get(tenant_id, {}).get(repo, {}).get(src, [])

    def list_nodes_by_type(self, *, tenant_id: str, repo_id: str, node_type: WhyNodeType) -> list[WhyNode]:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        require_non_empty(node_type, name="node_type")
        return [n for n in self._nodes.get(tenant_id, {}).get(repo, {}).values() if n.type == node_type]
