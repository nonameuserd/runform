from __future__ import annotations

import os

import pytest

from akc.ingest import Document, PgVectorStore, VectorStoreError


def _doc(*, tenant_id: str, doc_id: str, embedding: tuple[float, ...]) -> Document:
    return Document(
        id=doc_id,
        content=f"content-{doc_id}",
        metadata={"tenant_id": tenant_id, "source": "s", "source_type": "docs"},
        embedding=embedding,
    )


@pytest.mark.skipif(
    not os.environ.get("AKC_PGVECTOR_DSN"),
    reason="set AKC_PGVECTOR_DSN to run pgvector backend tests",
)
def test_pgvector_store_is_tenant_isolated_and_orders_results() -> None:
    dsn = os.environ["AKC_PGVECTOR_DSN"]
    store = PgVectorStore(dsn=dsn, dimension=2, table="akc_test_documents")

    store.add(
        tenant_id="tenant-a",
        documents=[
            _doc(tenant_id="tenant-a", doc_id="a", embedding=(1.0, 0.0)),
            _doc(tenant_id="tenant-a", doc_id="b", embedding=(0.0, 1.0)),
        ],
    )
    store.add(
        tenant_id="tenant-b",
        documents=[
            _doc(tenant_id="tenant-b", doc_id="c", embedding=(1.0, 0.0)),
        ],
    )

    ra = store.similarity_search(tenant_id="tenant-a", query_vector=(1.0, 0.0), k=10)
    rb = store.similarity_search(tenant_id="tenant-b", query_vector=(1.0, 0.0), k=10)

    assert [d.id for d in ra] == ["a", "b"]
    assert [d.id for d in rb] == ["c"]


def test_pgvector_store_validates_dimensions_without_db() -> None:
    # Dimension mismatch checks should fail before any DB interaction in search().
    store = PgVectorStore.__new__(PgVectorStore)  # bypass __init__
    store._dsn = "x"  # type: ignore[attr-defined]
    store._dimension = 2  # type: ignore[attr-defined]
    store._table = "t"  # type: ignore[attr-defined]

    with pytest.raises(VectorStoreError, match=r"dimension mismatch"):
        store.similarity_search_by_vector(tenant_id="t", query_vector=(1.0, 0.0, 0.0), k=1)
