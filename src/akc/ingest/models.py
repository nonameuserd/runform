"""Shared ingestion models and helpers."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from math import isfinite
from typing import Any, NotRequired, Required, TypedDict


class DocumentMetadata(TypedDict, total=False):
    tenant_id: Required[str]
    source: Required[str]
    source_type: Required[str]
    path: NotRequired[str]
    url: NotRequired[str]
    channel: NotRequired[str]
    thread_id: NotRequired[str]
    timestamp: NotRequired[str]
    user: NotRequired[str]
    chunk_index: NotRequired[int]
    parent_id: NotRequired[str]
    # Optional structured provenance (A3); connectors may omit any field.
    connector_id: NotRequired[str]
    indexed_at_ms: NotRequired[int]
    operation_id: NotRequired[str]
    openapi_path: NotRequired[str]
    openapi_method: NotRequired[str]


def _require_non_empty(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        msg = f"{field_name} must be a non-empty string"
        raise ValueError(msg)


def content_hash(text: str) -> str:
    _require_non_empty(text, field_name="content")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def stable_document_id(
    *,
    tenant_id: str,
    source: str,
    logical_locator: str,
    chunk_index: int = 0,
) -> str:
    _require_non_empty(tenant_id, field_name="tenant_id")
    _require_non_empty(source, field_name="source")
    _require_non_empty(logical_locator, field_name="logical_locator")
    if chunk_index < 0:
        raise ValueError("chunk_index must be >= 0")
    raw = f"{tenant_id}\n{source}\n{logical_locator}\n{chunk_index}".encode()
    return hashlib.sha256(raw).hexdigest()


@dataclass(frozen=True, slots=True)
class Document:
    id: str
    content: str
    metadata: DocumentMetadata
    embedding: tuple[float, ...] | None = None
    content_hash: str = field(init=False)

    def __post_init__(self) -> None:
        _require_non_empty(self.id, field_name="id")
        _require_non_empty(self.content, field_name="content")
        tenant_id = self.metadata.get("tenant_id")
        source = self.metadata.get("source")
        source_type = self.metadata.get("source_type")
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("metadata.tenant_id is required")
        if not isinstance(source, str) or not source.strip():
            raise ValueError("metadata.source is required")
        if not isinstance(source_type, str) or not source_type.strip():
            raise ValueError("metadata.source_type is required")
        object.__setattr__(self, "content_hash", content_hash(self.content))
        if self.embedding is not None:
            try:
                embedding = tuple(float(x) for x in self.embedding)
            except TypeError as e:  # non-iterable / wrong element types
                raise TypeError("embedding must be a sequence of numbers") from e
            if any(not isfinite(x) for x in embedding):
                raise ValueError("embedding must not contain NaN/Inf values")
            object.__setattr__(self, "embedding", embedding)

    @property
    def tenant_id(self) -> str:
        tenant_id = self.metadata["tenant_id"]
        return tenant_id

    def with_updates(
        self,
        *,
        content: str | None = None,
        metadata_updates: dict[str, Any] | None = None,
        embedding: tuple[float, ...] | None = None,
        new_id: str | None = None,
    ) -> Document:
        new_metadata: dict[str, Any] = dict(self.metadata)
        if metadata_updates is not None:
            new_metadata.update(metadata_updates)
        return Document(
            id=self.id if new_id is None else new_id,
            content=self.content if content is None else content,
            metadata=new_metadata,  # type: ignore[arg-type]
            embedding=self.embedding if embedding is None else embedding,
        )
