"""Vector store abstraction for ingestion.

Tenant isolation is enforced at the interface boundary: every operation requires
an explicit tenant_id, and implementations must never return cross-tenant data.
"""

from __future__ import annotations

import json
import re
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from math import sqrt
from typing import Any, cast

from akc.ingest.models import Document, DocumentMetadata


class VectorStoreError(Exception):
    """Raised when a vector store operation fails."""


@dataclass(frozen=True, slots=True)
class VectorSearchResult:
    document: Document
    score: float


def _require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _cosine_similarity(a: Sequence[float], b: Sequence[float]) -> float:
    if len(a) != len(b):
        raise VectorStoreError("vector dimension mismatch")
    dot = 0.0
    na = 0.0
    nb = 0.0
    for x, y in zip(a, b, strict=True):
        dot += float(x) * float(y)
        na += float(x) * float(x)
        nb += float(y) * float(y)
    if na <= 0.0 or nb <= 0.0:
        # A zero-norm vector is not meaningful for cosine similarity.
        raise VectorStoreError("zero-norm vector encountered")
    return dot / (sqrt(na) * sqrt(nb))


class VectorStore(ABC):
    """Vector search store for `Document` objects."""

    @abstractmethod
    def add(self, *, tenant_id: str, documents: Iterable[Document]) -> int:
        """Add or update documents for the tenant. Returns number of docs written."""

    @abstractmethod
    def similarity_search_by_vector(
        self,
        *,
        tenant_id: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> list[VectorSearchResult]:
        """Return top-k documents by similarity for the tenant."""

    def similarity_search(
        self,
        *,
        tenant_id: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> list[Document]:
        return [
            r.document
            for r in self.similarity_search_by_vector(
                tenant_id=tenant_id,
                query_vector=query_vector,
                k=k,
            )
        ]


class InMemoryVectorStore(VectorStore):
    """A dependency-light in-memory vector store for Phase 1 and tests."""

    def __init__(self) -> None:
        # tenant_id -> doc_id -> Document
        self._docs: dict[str, dict[str, Document]] = {}

    def add(self, *, tenant_id: str, documents: Iterable[Document]) -> int:
        _require_non_empty(tenant_id, name="tenant_id")
        bucket = self._docs.setdefault(tenant_id, {})
        wrote = 0
        for doc in documents:
            if doc.tenant_id != tenant_id:
                raise VectorStoreError("tenant_id mismatch between argument and document")
            if doc.embedding is None:
                raise VectorStoreError("document missing embedding")
            bucket[doc.id] = doc
            wrote += 1
        return wrote

    def similarity_search_by_vector(
        self,
        *,
        tenant_id: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> list[VectorSearchResult]:
        _require_non_empty(tenant_id, name="tenant_id")
        if k <= 0:
            raise ValueError("k must be > 0")
        docs = self._docs.get(tenant_id, {})
        if not docs:
            return []

        scored: list[VectorSearchResult] = []
        for doc in docs.values():
            emb = doc.embedding
            if emb is None:
                # Should not happen given add() validation, but keep defensive.
                continue
            score = _cosine_similarity(query_vector, emb)
            scored.append(VectorSearchResult(document=doc, score=score))

        scored.sort(key=lambda r: r.score, reverse=True)
        return scored[:k]


def _json_dumps(payload: object) -> str:
    try:
        return json.dumps(payload, sort_keys=True, ensure_ascii=False)
    except TypeError as e:
        raise VectorStoreError("value must be JSON-serializable") from e


def _json_loads_object(raw: str, *, what: str) -> dict[str, Any]:
    try:
        loaded = json.loads(raw)
    except Exception as e:  # pragma: no cover
        raise VectorStoreError(f"stored {what} was not valid JSON") from e
    if not isinstance(loaded, dict):
        raise VectorStoreError(f"stored {what} must be a JSON object")
    return loaded


def _json_loads_vector(raw: str) -> tuple[float, ...]:
    try:
        loaded = json.loads(raw)
    except Exception as e:  # pragma: no cover
        raise VectorStoreError("stored embedding was not valid JSON") from e
    if not isinstance(loaded, list):
        raise VectorStoreError("stored embedding must be a JSON array")
    try:
        return tuple(float(x) for x in loaded)
    except Exception as e:
        raise VectorStoreError("stored embedding must be an array of numbers") from e


_PG_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$")


def _validate_pg_identifier(value: str, *, name: str) -> str:
    """Validate a Postgres identifier like `table` or `schema.table`.

    We keep this strict because the identifier is interpolated into SQL strings.
    """
    _require_non_empty(value, name=name)
    if _PG_IDENT_RE.fullmatch(value) is None:
        raise ValueError(
            f"{name} must be a simple identifier (letters/numbers/underscore), "
            "optionally schema-qualified"
        )
    return value


class SQLiteVectorStore(VectorStore):
    """SQLite-backed vector store.

    This is a small, dependency-free persistent backend suitable for local
    indexing and tests. Similarity search is implemented in Python, so it's not
    intended for large-scale production use.
    """

    def __init__(self, *, path: str) -> None:
        _require_non_empty(path, name="path")
        self._path = path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS documents (
                  tenant_id TEXT NOT NULL,
                  doc_id    TEXT NOT NULL,
                  content   TEXT NOT NULL,
                  metadata  TEXT NOT NULL,
                  embedding TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, doc_id)
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS documents_by_tenant ON documents(tenant_id)")

    def add(self, *, tenant_id: str, documents: Iterable[Document]) -> int:
        _require_non_empty(tenant_id, name="tenant_id")
        rows: list[tuple[str, str, str, str, str]] = []
        for doc in documents:
            if doc.tenant_id != tenant_id:
                raise VectorStoreError("tenant_id mismatch between argument and document")
            if doc.embedding is None:
                raise VectorStoreError("document missing embedding")
            _require_non_empty(doc.id, name="document.id")
            rows.append(
                (
                    tenant_id,
                    doc.id,
                    doc.content,
                    _json_dumps(dict(doc.metadata)),
                    _json_dumps(list(doc.embedding)),
                )
            )
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO documents (tenant_id, doc_id, content, metadata, embedding)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, doc_id) DO UPDATE SET
                  content=excluded.content,
                  metadata=excluded.metadata,
                  embedding=excluded.embedding
                """,
                rows,
            )
        return len(rows)

    def similarity_search_by_vector(
        self,
        *,
        tenant_id: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> list[VectorSearchResult]:
        _require_non_empty(tenant_id, name="tenant_id")
        if k <= 0:
            raise ValueError("k must be > 0")

        with self._connect() as conn:
            cur = conn.execute(
                "SELECT doc_id, content, metadata, embedding FROM documents WHERE tenant_id=?",
                (tenant_id,),
            )
            rows = cur.fetchall()
        if not rows:
            return []

        scored: list[VectorSearchResult] = []
        for doc_id, content, metadata_raw, embedding_raw in rows:
            metadata = _json_loads_object(str(metadata_raw), what="metadata")
            metadata_typed = cast(DocumentMetadata, metadata)
            embedding = _json_loads_vector(str(embedding_raw))
            doc = Document(
                id=str(doc_id),
                content=str(content),
                metadata=metadata_typed,
                embedding=embedding,
            )
            score = _cosine_similarity(query_vector, embedding)
            scored.append(VectorSearchResult(document=doc, score=score))

        scored.sort(key=lambda r: r.score, reverse=True)
        return scored[:k]


class PgVectorStore(VectorStore):
    """Postgres + pgvector-backed vector store.

    This backend is intended for production use: it delegates nearest-neighbor
    search to Postgres/pgvector and supports persistence + concurrent access.
    """

    def __init__(
        self,
        *,
        dsn: str,
        dimension: int,
        table: str = "akc_documents",
    ) -> None:
        _require_non_empty(dsn, name="dsn")
        if dimension <= 0:
            raise ValueError("dimension must be > 0")
        table = _validate_pg_identifier(table, name="table")
        self._dsn = dsn
        self._dimension = dimension
        self._table = table
        self._ensure_schema()

    def _connect(self) -> Any:
        try:
            import importlib
        except Exception as e:  # pragma: no cover
            raise VectorStoreError(
                "PgVectorStore requires optional dependencies: psycopg[binary]"
            ) from e
        try:
            psycopg = importlib.import_module("psycopg")
        except Exception as e:  # pragma: no cover
            raise VectorStoreError(
                "PgVectorStore requires optional dependencies: psycopg[binary]"
            ) from e
        return psycopg.connect(self._dsn)

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self._table} (
                  tenant_id TEXT NOT NULL,
                  doc_id    TEXT NOT NULL,
                  content   TEXT NOT NULL,
                  metadata  JSONB NOT NULL,
                  embedding vector({self._dimension}) NOT NULL,
                  PRIMARY KEY (tenant_id, doc_id)
                )
                """
            )
            # This index can be swapped for HNSW in future versions of pgvector.
            conn.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {self._table}_embedding_ivfflat
                ON {self._table}
                USING ivfflat (embedding vector_cosine_ops)
                WITH (lists = 100)
                """
            )
            conn.execute(
                f"CREATE INDEX IF NOT EXISTS {self._table}_tenant_id ON {self._table}(tenant_id)"
            )

    def add(self, *, tenant_id: str, documents: Iterable[Document]) -> int:
        _require_non_empty(tenant_id, name="tenant_id")
        rows: list[tuple[str, str, str, dict[str, Any], list[float]]] = []
        for doc in documents:
            if doc.tenant_id != tenant_id:
                raise VectorStoreError("tenant_id mismatch between argument and document")
            if doc.embedding is None:
                raise VectorStoreError("document missing embedding")
            if len(doc.embedding) != self._dimension:
                raise VectorStoreError("vector dimension mismatch")
            rows.append(
                (
                    tenant_id,
                    doc.id,
                    doc.content,
                    dict(doc.metadata),
                    [float(x) for x in doc.embedding],
                )
            )
        if not rows:
            return 0

        # Avoid per-row commits.
        with self._connect() as conn, conn.cursor() as cur:
            cur.executemany(
                f"""
                INSERT INTO {self._table} (tenant_id, doc_id, content, metadata, embedding)
                VALUES (%s, %s, %s, %s, %s::vector)
                ON CONFLICT (tenant_id, doc_id) DO UPDATE SET
                  content=EXCLUDED.content,
                  metadata=EXCLUDED.metadata,
                  embedding=EXCLUDED.embedding
                """,
                rows,
            )
        return len(rows)

    def similarity_search_by_vector(
        self,
        *,
        tenant_id: str,
        query_vector: Sequence[float],
        k: int = 10,
    ) -> list[VectorSearchResult]:
        _require_non_empty(tenant_id, name="tenant_id")
        if k <= 0:
            raise ValueError("k must be > 0")
        if len(query_vector) != self._dimension:
            raise VectorStoreError("vector dimension mismatch")

        q = [float(x) for x in query_vector]

        with self._connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                    SELECT doc_id, content, metadata, (1 - (embedding <=> %s::vector)) AS score
                    FROM {self._table}
                    WHERE tenant_id = %s
                    ORDER BY embedding <=> %s::vector
                    LIMIT %s
                    """,
                (q, tenant_id, q, k),
            )
            rows = cur.fetchall()

        results: list[VectorSearchResult] = []
        for doc_id, content, metadata, score in rows:
            # Note: we don't return embeddings here to keep payload light.
            metadata_typed = cast(DocumentMetadata, dict(metadata))
            doc = Document(
                id=str(doc_id),
                content=str(content),
                metadata=metadata_typed,
                embedding=None,
            )
            results.append(VectorSearchResult(document=doc, score=float(score)))
        return results
