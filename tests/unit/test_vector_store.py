from __future__ import annotations

import pytest

from akc.ingest import Document, InMemoryVectorStore, VectorStoreError


def _doc(*, tenant_id: str, doc_id: str, embedding: tuple[float, ...]) -> Document:
    return Document(
        id=doc_id,
        content=f"content-{doc_id}",
        metadata={"tenant_id": tenant_id, "source": "s", "source_type": "docs"},
        embedding=embedding,
    )


def test_in_memory_vector_store_similarity_search_orders_by_score() -> None:
    store = InMemoryVectorStore()
    tenant_id = "t1"

    a = _doc(tenant_id=tenant_id, doc_id="a", embedding=(1.0, 0.0))
    b = _doc(tenant_id=tenant_id, doc_id="b", embedding=(0.0, 1.0))
    c = _doc(tenant_id=tenant_id, doc_id="c", embedding=(1.0, 1.0))

    wrote = store.add(tenant_id=tenant_id, documents=[a, b, c])
    assert wrote == 3

    results = store.similarity_search_by_vector(tenant_id=tenant_id, query_vector=(1.0, 0.0), k=3)
    assert [r.document.id for r in results] == ["a", "c", "b"]
    assert results[0].score >= results[1].score >= results[2].score


def test_in_memory_vector_store_is_tenant_isolated() -> None:
    store = InMemoryVectorStore()
    a = _doc(tenant_id="tenant-a", doc_id="a", embedding=(1.0, 0.0))
    b = _doc(tenant_id="tenant-b", doc_id="b", embedding=(1.0, 0.0))

    store.add(tenant_id="tenant-a", documents=[a])
    store.add(tenant_id="tenant-b", documents=[b])

    ra = store.similarity_search(tenant_id="tenant-a", query_vector=(1.0, 0.0), k=10)
    rb = store.similarity_search(tenant_id="tenant-b", query_vector=(1.0, 0.0), k=10)

    assert [d.id for d in ra] == ["a"]
    assert [d.id for d in rb] == ["b"]


def test_in_memory_vector_store_rejects_cross_tenant_add() -> None:
    store = InMemoryVectorStore()
    doc = _doc(tenant_id="tenant-a", doc_id="a", embedding=(1.0, 0.0))
    with pytest.raises(VectorStoreError, match=r"tenant_id mismatch"):
        store.add(tenant_id="tenant-b", documents=[doc])


def test_in_memory_vector_store_requires_embeddings() -> None:
    store = InMemoryVectorStore()
    doc = Document(
        id="a",
        content="x",
        metadata={"tenant_id": "t", "source": "s", "source_type": "docs"},
        embedding=None,
    )
    with pytest.raises(VectorStoreError, match=r"missing embedding"):
        store.add(tenant_id="t", documents=[doc])


def test_in_memory_vector_store_validates_k_and_dimensions() -> None:
    store = InMemoryVectorStore()
    store.add(tenant_id="t", documents=[_doc(tenant_id="t", doc_id="a", embedding=(1.0, 0.0))])

    with pytest.raises(ValueError, match=r"k must be > 0"):
        store.similarity_search_by_vector(tenant_id="t", query_vector=(1.0, 0.0), k=0)

    with pytest.raises(VectorStoreError, match=r"dimension mismatch"):
        store.similarity_search_by_vector(tenant_id="t", query_vector=(1.0, 0.0, 0.0), k=1)
