from __future__ import annotations

from akc.ingest.chunking import ChunkingConfig, chunk_documents, normalize_text
from akc.ingest.models import Document


def _doc(*, tenant_id: str = "t1", source: str = "/docs/a.md", content: str) -> Document:
    return Document(
        id="parent",
        content=content,
        metadata={"tenant_id": tenant_id, "source": source, "source_type": "docs"},
    )


def test_normalize_text_is_deterministic_and_strips() -> None:
    raw = "A\r\nB  \n\n\nC\rD\n"
    assert normalize_text(raw) == "A\nB\n\nC\nD"
    assert normalize_text(raw) == normalize_text(raw)


def test_chunk_documents_emits_parent_scoped_chunks_and_metadata() -> None:
    content = "\n\n".join(f"para {i} " + ("x" * 40) for i in range(10))
    parent = _doc(content=content)
    cfg = ChunkingConfig(chunk_size_chars=120, overlap_chars=20)

    chunks = list(chunk_documents([parent], config=cfg))
    assert len(chunks) >= 2

    for i, c in enumerate(chunks):
        assert c.metadata["tenant_id"] == "t1"
        assert c.metadata["source"] == "/docs/a.md"
        assert c.metadata["source_type"] == "docs"
        assert c.metadata["parent_id"] == "parent"
        assert c.metadata["chunk_index"] == i
        assert isinstance(c.metadata.get("chunk_start"), int)
        assert isinstance(c.metadata.get("chunk_end"), int)
        assert 0 <= c.metadata["chunk_start"] <= c.metadata["chunk_end"]  # type: ignore[operator]
        assert len(c.content) <= cfg.chunk_size_chars + cfg.overlap_chars + 10
        assert c.embedding is None

    # Stable IDs: rerunning chunking should produce identical ids.
    chunks2 = list(chunk_documents([parent], config=cfg))
    assert [c.id for c in chunks] == [c.id for c in chunks2]


def test_chunk_documents_are_tenant_scoped_via_ids() -> None:
    content = "x" * 500
    cfg = ChunkingConfig(chunk_size_chars=200, overlap_chars=0)

    a = list(chunk_documents([_doc(tenant_id="tenant-a", content=content)], config=cfg))
    b = list(chunk_documents([_doc(tenant_id="tenant-b", content=content)], config=cfg))

    assert len(a) == len(b)
    assert [c.id for c in a] != [c.id for c in b]
