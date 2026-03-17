"""Ingestion-layer exceptions."""

from __future__ import annotations


class IngestionError(Exception):
    """Base error for ingestion failures."""


class ConnectorError(IngestionError):
    """Raised when a connector cannot enumerate or fetch sources."""


class ChunkingError(IngestionError):
    """Raised when chunking fails for a document or source."""


class EmbeddingError(IngestionError):
    """Raised when embedding fails for a document batch or query."""
