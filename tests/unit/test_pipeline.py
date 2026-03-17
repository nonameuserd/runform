from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from akc.ingest.embedding import Embedder
from akc.ingest.index import InMemoryVectorStore
from akc.ingest.pipeline import IngestionStateStore, default_state_path, run_ingest


@dataclass(frozen=True, slots=True)
class DummyEmbedder(Embedder):
    dim: int = 4

    def embed(self, texts):  # type: ignore[no-untyped-def]
        out: list[tuple[float, ...]] = []
        for t in texts:
            # deterministic "embedding": length + checksum-ish.
            s = float(sum(ord(c) for c in t) % 97)
            out.append((float(len(t)) % 17, s, 1.0, 0.5)[: self.dim])
        return out


def test_default_state_path_is_deterministic(tmp_path: Path) -> None:
    p1 = default_state_path(tenant_id="t1", connector="docs", base_dir=tmp_path)
    p2 = default_state_path(tenant_id="t1", connector="docs", base_dir=tmp_path)
    assert p1 == p2
    assert "t1" in str(p1)


def test_run_ingest_docs_to_memory_index(tmp_path: Path) -> None:
    root = tmp_path / "docs"
    root.mkdir()
    (root / "a.md").write_text("# Title\n\nhello world\n", encoding="utf-8")

    state = IngestionStateStore(tmp_path / "state.json")
    vs = InMemoryVectorStore()
    res = run_ingest(
        connector_name="docs",
        tenant_id="tenant-1",
        input_value=str(root),
        embedder=DummyEmbedder(),
        vector_store=vs,
        state_store=state,
        incremental=True,
    )
    assert res.stats.sources_seen == 1
    assert res.stats.documents_fetched == 1
    assert res.stats.documents_chunked >= 1
    assert res.stats.documents_indexed == res.stats.documents_embedded


def test_incremental_skips_unchanged_sources(tmp_path: Path) -> None:
    root = tmp_path / "docs"
    root.mkdir()
    p = root / "a.md"
    p.write_text("hello\n", encoding="utf-8")

    state = IngestionStateStore(tmp_path / "state.json")
    vs = InMemoryVectorStore()
    emb = DummyEmbedder()

    first = run_ingest(
        connector_name="docs",
        tenant_id="tenant-1",
        input_value=str(root),
        embedder=emb,
        vector_store=vs,
        state_store=state,
        incremental=True,
    )
    assert first.stats.sources_skipped == 0

    second = run_ingest(
        connector_name="docs",
        tenant_id="tenant-1",
        input_value=str(root),
        embedder=emb,
        vector_store=vs,
        state_store=state,
        incremental=True,
    )
    assert second.stats.sources_seen == 1
    assert second.stats.sources_skipped == 1
    assert second.stats.documents_fetched == 0
    assert second.stats.documents_indexed == 0


def test_run_ingest_requires_embedder_when_index_enabled(tmp_path: Path) -> None:
    root = tmp_path / "docs"
    root.mkdir()
    (root / "a.md").write_text("hello\n", encoding="utf-8")
    with pytest.raises(ValueError, match="embedder is required"):
        run_ingest(
            connector_name="docs",
            tenant_id="tenant-1",
            input_value=str(root),
            embedder=None,
            vector_store=InMemoryVectorStore(),
        )


def test_run_ingest_no_index_allows_no_embedder(tmp_path: Path) -> None:
    root = tmp_path / "docs"
    root.mkdir()
    (root / "a.md").write_text("hello\n", encoding="utf-8")
    res = run_ingest(
        connector_name="docs",
        tenant_id="tenant-1",
        input_value=str(root),
        embedder=None,
        vector_store=None,
    )
    assert res.stats.documents_fetched == 1
