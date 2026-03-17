"""Unified structured index facade."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass

from akc.ingest.index.graph import GraphStore, InMemoryGraphStore
from akc.ingest.index.vector_store import VectorSearchResult, VectorStore
from akc.ingest.models import Document


@dataclass(frozen=True, slots=True)
class IndexConfig:
    """Configuration for building an Index.

    This is intentionally small for Phase 1; more knobs (namespaces, filters,
    metadata indexes) can be added without breaking the interface.
    """

    enable_graph: bool = False


class Index:
    """Facade over a tenant-isolated vector store and optional graph store."""

    def __init__(self, *, vector_store: VectorStore, graph_store: GraphStore | None = None) -> None:
        self._vs = vector_store
        self._gs = graph_store

    @property
    def vector_store(self) -> VectorStore:
        return self._vs

    @property
    def graph_store(self) -> GraphStore | None:
        return self._gs

    def add(self, *, tenant_id: str, documents: Iterable[Document]) -> int:
        return self._vs.add(tenant_id=tenant_id, documents=documents)

    def upsert_nodes(self, *, tenant_id: str, nodes) -> int:  # type: ignore[no-untyped-def]
        """Upsert graph nodes for the tenant (if enabled)."""
        if self._gs is None:
            raise RuntimeError("graph store is not enabled for this index")
        return self._gs.upsert_nodes(tenant_id=tenant_id, nodes=nodes)

    def add_edges(self, *, tenant_id: str, edges) -> int:  # type: ignore[no-untyped-def]
        """Add graph edges for the tenant (if enabled)."""
        if self._gs is None:
            raise RuntimeError("graph store is not enabled for this index")
        return self._gs.add_edges(tenant_id=tenant_id, edges=edges)

    def similarity_search_by_vector(
        self,
        *,
        tenant_id: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> list[VectorSearchResult]:
        return self._vs.similarity_search_by_vector(
            tenant_id=tenant_id, query_vector=query_vector, k=k
        )

    def similarity_search(
        self,
        *,
        tenant_id: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> list[Document]:
        return self._vs.similarity_search(tenant_id=tenant_id, query_vector=query_vector, k=k)


def build_index(*, vector_store: VectorStore, config: IndexConfig | None = None) -> Index:
    """Build an Index with sensible Phase 1 defaults."""
    cfg = config or IndexConfig()
    graph_store: GraphStore | None = InMemoryGraphStore() if cfg.enable_graph else None
    return Index(vector_store=vector_store, graph_store=graph_store)
