from __future__ import annotations

from akc.compile.interfaces import IndexQuery, TenantRepoScope
from akc.compile.vectorstore_index_adapter import VectorStoreIndexAdapter
from akc.ingest.embedding import HashEmbedder
from akc.ingest.index import InMemoryVectorStore, build_index
from akc.ingest.models import Document


def test_vectorstore_index_adapter_enforces_repo_filter_conservatively() -> None:
    vs = InMemoryVectorStore()
    idx = build_index(vector_store=vs)
    embedder = HashEmbedder(dimension=32)
    adapter = VectorStoreIndexAdapter(index=idx, embedder=embedder)

    docs = [
        Document(
            id="a",
            content="alpha",
            metadata={"tenant_id": "t1", "source": "docs", "source_type": "md", "repo_id": "repo1"},
        ),
        Document(
            id="b",
            content="beta",
            metadata={"tenant_id": "t1", "source": "docs", "source_type": "md", "repo_id": "repo2"},
        ),
        # Missing repo_id -> should be excluded once repo filter is applied.
        Document(
            id="c",
            content="gamma",
            metadata={"tenant_id": "t1", "source": "docs", "source_type": "md"},
        ),
    ]
    # Embed and add to vector store.
    from akc.ingest.embedding import embed_documents

    idx.add(tenant_id="t1", documents=list(embed_documents(embedder, docs)))

    out = adapter.query(
        scope=TenantRepoScope(tenant_id="t1", repo_id="repo1"),
        query=IndexQuery(text="alpha", k=10, filters={"repo_id": "repo1"}),
    )
    assert out
    assert all((d.metadata or {}).get("repo_id") == "repo1" for d in out)


def test_vectorstore_index_adapter_enforces_tenant_isolation() -> None:
    vs = InMemoryVectorStore()
    idx = build_index(vector_store=vs)
    embedder = HashEmbedder(dimension=32)
    adapter = VectorStoreIndexAdapter(index=idx, embedder=embedder)

    docs_t1 = [
        Document(
            id="t1-a",
            content="alpha shared words",
            metadata={"tenant_id": "t1", "source": "docs", "source_type": "md", "repo_id": "repo1"},
        )
    ]
    docs_t2 = [
        Document(
            id="t2-a",
            content="alpha shared words",
            metadata={"tenant_id": "t2", "source": "docs", "source_type": "md", "repo_id": "repo1"},
        )
    ]

    from akc.ingest.embedding import embed_documents

    idx.add(tenant_id="t1", documents=list(embed_documents(embedder, docs_t1)))
    idx.add(tenant_id="t2", documents=list(embed_documents(embedder, docs_t2)))

    out = adapter.query(
        scope=TenantRepoScope(tenant_id="t1", repo_id="repo1"),
        query=IndexQuery(text="alpha", k=10, filters={"repo_id": "repo1"}),
    )

    ids = {d.doc_id for d in out}
    assert "t1-a" in ids
    assert "t2-a" not in ids
