from __future__ import annotations

import pytest

from akc.ingest import Document, content_hash, stable_document_id
from akc.ingest.connectors.base import BaseConnector


def test_content_hash_is_stable() -> None:
    assert content_hash("hello") == content_hash("hello")
    assert content_hash("hello") != content_hash("hello!")


def test_stable_document_id_is_deterministic() -> None:
    id1 = stable_document_id(
        tenant_id="t1",
        source="/docs/a.md",
        logical_locator="/docs/a.md#intro",
        chunk_index=0,
    )
    id2 = stable_document_id(
        tenant_id="t1",
        source="/docs/a.md",
        logical_locator="/docs/a.md#intro",
        chunk_index=0,
    )
    assert id1 == id2


def test_stable_document_id_is_tenant_scoped() -> None:
    id1 = stable_document_id(
        tenant_id="t1",
        source="/docs/a.md",
        logical_locator="/docs/a.md#intro",
        chunk_index=0,
    )
    id2 = stable_document_id(
        tenant_id="t2",
        source="/docs/a.md",
        logical_locator="/docs/a.md#intro",
        chunk_index=0,
    )
    assert id1 != id2


def test_document_requires_tenant_and_source_fields() -> None:
    with pytest.raises(ValueError, match=r"metadata\.tenant_id is required"):
        Document(id="x", content="hello", metadata={"source": "s", "source_type": "docs"})  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=r"metadata\.source is required"):
        Document(id="x", content="hello", metadata={"tenant_id": "t", "source_type": "docs"})  # type: ignore[arg-type]

    with pytest.raises(ValueError, match=r"metadata\.source_type is required"):
        Document(id="x", content="hello", metadata={"tenant_id": "t", "source": "s"})  # type: ignore[arg-type]


def test_base_connector_emits_tenant_scoped_documents() -> None:
    class DummyConnector(BaseConnector):
        def __init__(self) -> None:
            super().__init__(tenant_id="tenant-a", source_type="docs")

        def list_sources(self):  # type: ignore[override]
            return ["source-1"]

        def fetch(self, source_id: str):  # type: ignore[override]
            return [
                self._make_document(
                    source=source_id,
                    logical_locator=source_id,
                    content="hello",
                )
            ]

    conn = DummyConnector()
    docs = list(conn.fetch("source-1"))
    assert docs[0].tenant_id == "tenant-a"
    assert docs[0].metadata["source_type"] == "docs"
    assert docs[0].metadata["source"] == "source-1"
