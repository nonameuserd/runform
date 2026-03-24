"""Connector contracts for ingestion."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from typing import Literal, final

from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document, stable_document_id


class Connector(ABC):
    """Connector protocol for enumerating and fetching sources."""

    @property
    @abstractmethod
    def tenant_id(self) -> str: ...

    @property
    @abstractmethod
    def source_type(self) -> str: ...

    @abstractmethod
    def list_sources(self) -> Iterable[str]: ...

    @abstractmethod
    def fetch(self, source_id: str) -> Iterable[Document]: ...


class BaseConnector(Connector):
    def __init__(self, *, tenant_id: str, source_type: str) -> None:
        if not isinstance(tenant_id, str) or not tenant_id.strip():
            raise ValueError("tenant_id must be a non-empty string")
        if not isinstance(source_type, str) or not source_type.strip():
            raise ValueError("source_type must be a non-empty string")
        self._tenant_id = tenant_id
        self._source_type = source_type

    @final
    @property
    def tenant_id(self) -> str:
        return self._tenant_id

    @final
    @property
    def source_type(self) -> str:
        return self._source_type

    @final
    def _make_document(
        self,
        *,
        source: str,
        logical_locator: str,
        content: str,
        chunk_index: int = 0,
        metadata: dict[str, object] | None = None,
        embedding: tuple[float, ...] | None = None,
    ) -> Document:
        if not isinstance(source, str) or not source.strip():
            raise ConnectorError("source must be a non-empty string")
        if not isinstance(logical_locator, str) or not logical_locator.strip():
            raise ConnectorError("logical_locator must be a non-empty string")
        if not isinstance(content, str) or not content.strip():
            raise ConnectorError("content must be a non-empty string")
        if chunk_index < 0:
            raise ConnectorError("chunk_index must be >= 0")
        doc_id = stable_document_id(
            tenant_id=self.tenant_id,
            source=source,
            logical_locator=logical_locator,
            chunk_index=chunk_index,
        )
        md: dict[str, object] = {}
        if metadata is not None:
            md.update(metadata)
        md["tenant_id"] = self.tenant_id
        md["source"] = source
        md["source_type"] = self.source_type
        md["chunk_index"] = chunk_index
        if "connector_id" not in md:
            md["connector_id"] = self.source_type
        try:
            return Document(
                id=doc_id,
                content=content,
                metadata=md,  # type: ignore[arg-type]
                embedding=embedding,
            )
        except (ValueError, TypeError) as e:
            raise ConnectorError("failed to construct Document") from e


def iter_documents(
    connector: Connector,
    *,
    on_error: Literal["raise", "skip"] = "raise",
    on_skip: Callable[[str, ConnectorError], object] | None = None,
) -> Iterator[Document]:
    """Enumerate and yield documents from a connector.

    This is a small orchestration helper so callers can choose whether
    per-source connector failures should stop ingestion or be skipped.
    """

    for source_id in connector.list_sources():
        try:
            yield from connector.fetch(source_id)
        except ConnectorError as e:
            if on_error == "skip":
                if on_skip is not None:
                    on_skip(source_id, e)
                continue
            raise
