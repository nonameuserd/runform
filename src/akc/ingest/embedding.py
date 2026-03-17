"""Embedding utilities for ingestion.

Phase 1: dependency-light HTTP clients for OpenAI-compatible and Gemini embeddings.
"""

from __future__ import annotations

import hashlib
import json
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from akc.ingest.exceptions import EmbeddingError
from akc.ingest.models import Document


class Embedder(ABC):
    """Embeds text into vectors usable by a vector store."""

    @abstractmethod
    def embed(self, texts: Sequence[str]) -> Sequence[tuple[float, ...]]: ...


def embed_documents(embedder: Embedder, documents: Iterable[Document]) -> Iterator[Document]:
    """Embed each document's content and attach the embedding.

    The output order matches the input order.
    """

    docs = list(documents)
    if not docs:
        return
    vectors = embedder.embed([d.content for d in docs])
    if len(vectors) != len(docs):
        raise EmbeddingError(
            f"embedder returned {len(vectors)} embeddings for {len(docs)} documents"
        )
    for doc, vec in zip(docs, vectors, strict=True):
        yield doc.with_updates(embedding=vec)


def embed_query(embedder: Embedder, query: str) -> tuple[float, ...]:
    if not isinstance(query, str) or not query.strip():
        raise ValueError("query must be a non-empty string")
    vectors = embedder.embed([query])
    if len(vectors) != 1:
        raise EmbeddingError("expected exactly one embedding for query")
    return vectors[0]


def _validate_vector(vec: Sequence[Any]) -> tuple[float, ...]:
    try:
        out = tuple(float(x) for x in vec)
    except TypeError as e:
        raise EmbeddingError("embedding vector must be a sequence of numbers") from e
    if not out:
        raise EmbeddingError("embedding vector must not be empty")
    if any(not isfinite(x) for x in out):
        raise EmbeddingError("embedding vector must not contain NaN/Inf values")
    return out


@dataclass(frozen=True, slots=True)
class HashEmbedder(Embedder):
    """Offline, deterministic embedder for tests and local demos.

    This is not intended to produce semantically strong embeddings; it exists to enable
    fully-offline ingestion + indexing flows (including CLI integration tests).
    """

    dimension: int = 64
    salt: str = "akc-hash-embedder-v1"

    def __post_init__(self) -> None:
        if not isinstance(self.dimension, int) or self.dimension <= 0:
            raise ValueError("dimension must be a positive integer")
        if not isinstance(self.salt, str) or not self.salt.strip():
            raise ValueError("salt must be a non-empty string")

    def embed(self, texts: Sequence[str]) -> Sequence[tuple[float, ...]]:
        if not texts:
            return []
        for t in texts:
            if not isinstance(t, str) or not t.strip():
                raise ValueError("all texts must be non-empty strings")
        return [self._embed_one(t) for t in texts]

    def _embed_one(self, text: str) -> tuple[float, ...]:
        # Expand SHA256 digest stream into `dimension` bytes, then map to [-1, 1].
        out: list[float] = []
        counter = 0
        while len(out) < self.dimension:
            h = hashlib.sha256()
            h.update(self.salt.encode("utf-8"))
            h.update(b"\x00")
            h.update(counter.to_bytes(4, "big", signed=False))
            h.update(b"\x00")
            h.update(text.encode("utf-8"))
            digest = h.digest()  # 32 bytes
            for b in digest:
                # b in [0,255] -> float in [-1, 1]
                out.append((b / 127.5) - 1.0)
                if len(out) >= self.dimension:
                    break
            counter += 1
        return tuple(out)


def _post_json(
    url: str,
    *,
    payload: Mapping[str, Any],
    headers: Mapping[str, str] | None = None,
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    if timeout_s <= 0:
        raise ValueError("timeout_s must be > 0")
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    data = json.dumps(payload).encode("utf-8")
    req = Request(url, method="POST", data=data, headers=req_headers)
    try:
        with urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 (controlled URL)
            raw = resp.read()
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:  # pragma: no cover
            body = ""
        raise EmbeddingError(f"embedding request failed ({e.code}): {body}".strip()) from e
    except URLError as e:
        raise EmbeddingError("embedding request failed to connect") from e

    try:
        decoded = raw.decode("utf-8")
        parsed = json.loads(decoded)
    except Exception as e:  # pragma: no cover
        raise EmbeddingError("embedding response was not valid JSON") from e
    if not isinstance(parsed, dict):
        raise EmbeddingError("embedding response JSON must be an object")
    return parsed


@dataclass(frozen=True, slots=True)
class OpenAICompatibleEmbedder(Embedder):
    """OpenAI-compatible embeddings via HTTP.

    Compatible with OpenAI-style providers that implement `POST /v1/embeddings`.
    """

    base_url: str
    model: str
    api_key: str
    timeout_s: float = 30.0
    extra_headers: Mapping[str, str] | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.base_url, str) or not self.base_url.strip():
            raise ValueError("base_url must be a non-empty string")
        if not isinstance(self.model, str) or not self.model.strip():
            raise ValueError("model must be a non-empty string")
        if not isinstance(self.api_key, str) or not self.api_key.strip():
            raise ValueError("api_key must be a non-empty string")
        if self.timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")

    def embed(self, texts: Sequence[str]) -> Sequence[tuple[float, ...]]:
        if not texts:
            return []
        for t in texts:
            if not isinstance(t, str) or not t.strip():
                raise ValueError("all texts must be non-empty strings")

        url = self.base_url.rstrip("/") + "/v1/embeddings"
        headers: dict[str, str] = {"Authorization": f"Bearer {self.api_key}"}
        if self.extra_headers:
            headers.update(self.extra_headers)

        parsed = _post_json(
            url,
            payload={"model": self.model, "input": list(texts)},
            headers=headers,
            timeout_s=self.timeout_s,
        )
        data = parsed.get("data")
        if not isinstance(data, list):
            raise EmbeddingError("OpenAI-compatible response missing 'data' list")
        vectors_by_index: dict[int, tuple[float, ...]] = {}
        for item in data:
            if not isinstance(item, dict):
                raise EmbeddingError("OpenAI-compatible response 'data' items must be objects")
            emb = item.get("embedding")
            if not isinstance(emb, list):
                raise EmbeddingError("OpenAI-compatible response item missing 'embedding' list")
            idx = item.get("index")
            if idx is None:
                # Some providers omit index; assume returned order corresponds to input order.
                idx = len(vectors_by_index)
            if not isinstance(idx, int) or idx < 0:
                raise EmbeddingError("OpenAI-compatible response item has invalid 'index'")
            if idx in vectors_by_index:
                raise EmbeddingError("OpenAI-compatible response contained duplicate 'index'")
            vectors_by_index[idx] = _validate_vector(emb)

        if len(vectors_by_index) != len(texts):
            raise EmbeddingError("OpenAI-compatible response size mismatch")
        try:
            return [vectors_by_index[i] for i in range(len(texts))]
        except KeyError as e:
            raise EmbeddingError("OpenAI-compatible response missing one or more indices") from e


@dataclass(frozen=True, slots=True)
class GeminiEmbedder(Embedder):
    """Gemini embeddings via Google Generative Language API (v1beta).

    This uses the `batchEmbedContents` method shape. Providers or API versions may
    vary; callers should treat this as best-effort Phase 1 support.
    """

    api_key: str
    model: str = "text-embedding-004"
    base_url: str = "https://generativelanguage.googleapis.com"
    timeout_s: float = 30.0

    def __post_init__(self) -> None:
        if not isinstance(self.api_key, str) or not self.api_key.strip():
            raise ValueError("api_key must be a non-empty string")
        if not isinstance(self.model, str) or not self.model.strip():
            raise ValueError("model must be a non-empty string")
        if not isinstance(self.base_url, str) or not self.base_url.strip():
            raise ValueError("base_url must be a non-empty string")
        if self.timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")

    def embed(self, texts: Sequence[str]) -> Sequence[tuple[float, ...]]:
        if not texts:
            return []
        for t in texts:
            if not isinstance(t, str) or not t.strip():
                raise ValueError("all texts must be non-empty strings")

        # REST: POST /v1beta/models/{model}:batchEmbedContents?key=API_KEY
        url = (
            self.base_url.rstrip("/")
            + f"/v1beta/models/{self.model}:batchEmbedContents?key={self.api_key}"
        )
        payload: dict[str, Any] = {
            "requests": [{"content": {"parts": [{"text": t}]}} for t in texts],
        }
        parsed = _post_json(url, payload=payload, timeout_s=self.timeout_s)
        embeddings = parsed.get("embeddings")
        if not isinstance(embeddings, list):
            raise EmbeddingError("Gemini response missing 'embeddings' list")
        vectors: list[tuple[float, ...]] = []
        for item in embeddings:
            if not isinstance(item, dict):
                raise EmbeddingError("Gemini embeddings items must be objects")
            values = item.get("values")
            if not isinstance(values, list):
                raise EmbeddingError("Gemini embedding item missing 'values' list")
            vectors.append(_validate_vector(values))
        if len(vectors) != len(texts):
            raise EmbeddingError("Gemini response size mismatch")
        return vectors
