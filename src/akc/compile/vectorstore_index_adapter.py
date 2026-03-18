"""Compile-layer adapter for the ingestion vector store index.

The ingestion `akc.ingest.index.facade.Index` supports vector similarity search by vector,
not by text. This adapter embeds the query text and maps results into the Phase 3
`akc.compile.interfaces.Index` protocol.
"""

from __future__ import annotations

from collections.abc import Sequence

from akc.compile.interfaces import Index, IndexDocument, IndexQuery, TenantRepoScope
from akc.ingest.embedding import Embedder, embed_query
from akc.ingest.index.facade import Index as IngestIndex


class VectorStoreIndexAdapter(Index):
    """Adapter that enables text queries over an ingestion vector store index."""

    def __init__(self, *, index: IngestIndex, embedder: Embedder) -> None:
        self._index = index
        self._embedder = embedder

    def query(self, *, scope: TenantRepoScope, query: IndexQuery) -> Sequence[IndexDocument]:
        qvec = embed_query(self._embedder, query.text)
        results = self._index.similarity_search_by_vector(
            tenant_id=scope.tenant_id,
            query_vector=qvec,
            k=int(query.k),
        )

        repo_filter = None
        if query.filters is not None:
            rf = query.filters.get("repo_id")
            if isinstance(rf, str) and rf.strip():
                repo_filter = rf.strip()

        docs: list[IndexDocument] = []
        for r in results:
            md = dict(r.document.metadata)
            # Enforce repo scoping if requested. This assumes connectors store repo_id in metadata
            # when indexing repo-specific documents. If missing, we exclude conservatively.
            if repo_filter is not None:
                md_repo = md.get("repo_id")
                if not isinstance(md_repo, str) or md_repo.strip() != repo_filter:
                    continue

            title = None
            # Best-effort title extraction.
            for key in ("path", "url", "source"):
                v = md.get(key)
                if isinstance(v, str) and v.strip():
                    title = v.strip()
                    break

            docs.append(
                IndexDocument(
                    doc_id=r.document.id,
                    title=title,
                    content=r.document.content,
                    score=float(r.score),
                    metadata=md,  # type: ignore[arg-type]
                )
            )
        return docs
