"""Structured index: vector store + optional graph store.

Phase 1 focuses on:
- Strict tenant isolation at the index boundary
- A dependency-light in-memory vector store for tests/dev
- Optional persistence via pluggable backends
"""

from akc.ingest.index.facade import Index, IndexConfig, build_index
from akc.ingest.index.graph import (
    Edge,
    GraphStore,
    GraphStoreError,
    InMemoryGraphStore,
    Node,
    SQLiteGraphStore,
)
from akc.ingest.index.vector_store import (
    InMemoryVectorStore,
    PgVectorStore,
    SQLiteVectorStore,
    VectorSearchResult,
    VectorStore,
    VectorStoreError,
)

__all__ = [
    "Edge",
    "GraphStore",
    "GraphStoreError",
    "InMemoryGraphStore",
    "InMemoryVectorStore",
    "PgVectorStore",
    "Index",
    "IndexConfig",
    "build_index",
    "Node",
    "SQLiteGraphStore",
    "SQLiteVectorStore",
    "VectorSearchResult",
    "VectorStore",
    "VectorStoreError",
]
