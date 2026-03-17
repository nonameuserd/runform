from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from akc.ingest import (
    Document,
    EmbeddingError,
    HashEmbedder,
    OpenAICompatibleEmbedder,
    embed_documents,
    embed_query,
)


class _FakeHTTPResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._raw = json.dumps(payload).encode("utf-8")

    def read(self) -> bytes:  # noqa: D401 (simple stub)
        return self._raw

    def __enter__(self) -> _FakeHTTPResponse:
        return self

    def __exit__(self, _exc_type, _exc, _tb) -> None:  # type: ignore[override]
        return None


def test_openai_compatible_embedder_parses_embeddings(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.ingest.embedding as emb

    def fake_urlopen(_req, timeout: float):  # type: ignore[no-untyped-def]
        assert timeout == 5.0
        return _FakeHTTPResponse(
            {
                "data": [
                    {"embedding": [0.0, 1.5, -2.0]},
                    {"embedding": [3.0, 4.0, 5.0]},
                ]
            }
        )

    monkeypatch.setattr(emb, "urlopen", fake_urlopen)

    e = OpenAICompatibleEmbedder(
        base_url="https://example.com",
        model="text-embedding-3-small",
        api_key="k",
        timeout_s=5.0,
    )
    out = e.embed(["hello", "world"])
    assert out == [(0.0, 1.5, -2.0), (3.0, 4.0, 5.0)]


def test_openai_compatible_orders_by_index(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.ingest.embedding as emb

    def fake_urlopen(_req, timeout: float):  # type: ignore[no-untyped-def]
        assert timeout == 30.0
        return _FakeHTTPResponse(
            {
                "data": [
                    {"index": 1, "embedding": [9.0, 9.0]},
                    {"index": 0, "embedding": [1.0, 2.0]},
                ]
            }
        )

    monkeypatch.setattr(emb, "urlopen", fake_urlopen)

    e = OpenAICompatibleEmbedder(base_url="https://x", model="m", api_key="k")
    out = e.embed(["a", "b"])
    assert out == [(1.0, 2.0), (9.0, 9.0)]


def test_embed_documents_attaches_embeddings(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.ingest.embedding as emb

    def fake_urlopen(_req, timeout: float):  # type: ignore[no-untyped-def]
        assert timeout == 30.0
        return _FakeHTTPResponse({"data": [{"embedding": [1.0, 2.0]}]})

    monkeypatch.setattr(emb, "urlopen", fake_urlopen)

    d = Document(
        id="doc-1",
        content="hello",
        metadata={"tenant_id": "t", "source": "s", "source_type": "docs"},
    )
    e = OpenAICompatibleEmbedder(base_url="https://x", model="m", api_key="k")
    out = list(embed_documents(e, [d]))
    assert out[0].embedding == (1.0, 2.0)


def test_embed_documents_empty_input_yields_nothing() -> None:
    e = SimpleNamespace(embed=lambda texts: [(0.0,)] * len(texts))  # type: ignore[assignment]
    assert list(embed_documents(e, [])) == []


def test_embed_query_returns_single_vector(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.ingest.embedding as emb

    def fake_urlopen(_req, timeout: float):  # type: ignore[no-untyped-def]
        return _FakeHTTPResponse({"data": [{"embedding": [0.25, 0.5]}]})

    monkeypatch.setattr(emb, "urlopen", fake_urlopen)

    e = OpenAICompatibleEmbedder(base_url="https://x", model="m", api_key="k")
    vec = embed_query(e, "what is akc?")
    assert vec == (0.25, 0.5)


def test_openai_compatible_rejects_size_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.ingest.embedding as emb

    def fake_urlopen(_req, timeout: float):  # type: ignore[no-untyped-def]
        return _FakeHTTPResponse({"data": [{"embedding": [1.0, 2.0]}]})

    monkeypatch.setattr(emb, "urlopen", fake_urlopen)

    e = OpenAICompatibleEmbedder(base_url="https://x", model="m", api_key="k")
    with pytest.raises(EmbeddingError, match=r"size mismatch"):
        e.embed(["a", "b"])


def test_openai_compatible_rejects_non_finite_values(monkeypatch: pytest.MonkeyPatch) -> None:
    import akc.ingest.embedding as emb

    def fake_urlopen(_req, timeout: float):  # type: ignore[no-untyped-def]
        return _FakeHTTPResponse({"data": [{"embedding": [1.0, float("nan")]}]})

    monkeypatch.setattr(emb, "urlopen", fake_urlopen)

    e = OpenAICompatibleEmbedder(base_url="https://x", model="m", api_key="k")
    with pytest.raises(EmbeddingError, match=r"NaN/Inf"):
        e.embed(["hello"])


def test_hash_embedder_is_deterministic_and_sized() -> None:
    e = HashEmbedder(dimension=8, salt="s")
    v1 = e.embed(["hello"])[0]
    v2 = e.embed(["hello"])[0]
    assert v1 == v2
    assert len(v1) == 8
