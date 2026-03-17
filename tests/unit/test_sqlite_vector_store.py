from __future__ import annotations

import pytest

from akc.ingest import Document, SQLiteVectorStore, VectorStoreError


def _doc(*, tenant_id: str, doc_id: str, embedding: tuple[float, ...]) -> Document:
    return Document(
        id=doc_id,
        content=f"content-{doc_id}",
        metadata={"tenant_id": tenant_id, "source": "s", "source_type": "docs"},
        embedding=embedding,
    )


def test_sqlite_vector_store_persists_and_is_tenant_isolated(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = tmp_path / "vectors.sqlite3"

    s1 = SQLiteVectorStore(path=str(db_path))
    s1.add(
        tenant_id="tenant-a",
        documents=[_doc(tenant_id="tenant-a", doc_id="a", embedding=(1.0, 0.0))],
    )
    s1.add(
        tenant_id="tenant-b",
        documents=[_doc(tenant_id="tenant-b", doc_id="b", embedding=(1.0, 0.0))],
    )

    # Re-open to verify persistence.
    s2 = SQLiteVectorStore(path=str(db_path))
    ra = s2.similarity_search(tenant_id="tenant-a", query_vector=(1.0, 0.0), k=10)
    rb = s2.similarity_search(tenant_id="tenant-b", query_vector=(1.0, 0.0), k=10)

    assert [d.id for d in ra] == ["a"]
    assert [d.id for d in rb] == ["b"]


def test_sqlite_vector_store_rejects_cross_tenant_add(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = tmp_path / "vectors.sqlite3"
    store = SQLiteVectorStore(path=str(db_path))
    doc = _doc(tenant_id="tenant-a", doc_id="a", embedding=(1.0, 0.0))
    with pytest.raises(VectorStoreError, match=r"tenant_id mismatch"):
        store.add(tenant_id="tenant-b", documents=[doc])


def test_sqlite_vector_store_requires_embeddings(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = tmp_path / "vectors.sqlite3"
    store = SQLiteVectorStore(path=str(db_path))
    doc = Document(
        id="a",
        content="x",
        metadata={"tenant_id": "t", "source": "s", "source_type": "docs"},
        embedding=None,
    )
    with pytest.raises(VectorStoreError, match=r"missing embedding"):
        store.add(tenant_id="t", documents=[doc])


def test_sqlite_vector_store_validates_k_and_dimensions(tmp_path) -> None:  # type: ignore[no-untyped-def]
    db_path = tmp_path / "vectors.sqlite3"
    store = SQLiteVectorStore(path=str(db_path))
    store.add(tenant_id="t", documents=[_doc(tenant_id="t", doc_id="a", embedding=(1.0, 0.0))])

    with pytest.raises(ValueError, match=r"k must be > 0"):
        store.similarity_search_by_vector(tenant_id="t", query_vector=(1.0, 0.0), k=0)

    with pytest.raises(VectorStoreError, match=r"dimension mismatch"):
        store.similarity_search_by_vector(tenant_id="t", query_vector=(1.0, 0.0, 0.0), k=1)
