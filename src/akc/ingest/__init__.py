"""Ingestion: connectors, chunking, and structured index."""

from akc.ingest.chunking import ChunkingConfig, chunk_documents, normalize_documents, normalize_text
from akc.ingest.connectors.base import BaseConnector, Connector, iter_documents
from akc.ingest.connectors.docs import DocsConnector, DocsConnectorConfig, build_docs_connector
from akc.ingest.connectors.messaging.discord import (
    DiscordConnector,
    DiscordConnectorConfig,
    build_discord_connector,
)
from akc.ingest.connectors.messaging.slack import (
    SlackConnector,
    SlackConnectorConfig,
    build_slack_connector,
)
from akc.ingest.connectors.messaging.telegram import (
    TelegramConnectorConfig,
    TelegramUpdatesConnector,
    build_telegram_connector,
)
from akc.ingest.connectors.openapi import (
    OpenAPIConnector,
    OpenAPIConnectorConfig,
    build_openapi_connector,
)
from akc.ingest.embedding import (
    Embedder,
    GeminiEmbedder,
    HashEmbedder,
    OpenAICompatibleEmbedder,
    embed_documents,
    embed_query,
)
from akc.ingest.exceptions import ChunkingError, ConnectorError, EmbeddingError, IngestionError
from akc.ingest.index import (
    Edge,
    GraphStore,
    GraphStoreError,
    Index,
    IndexConfig,
    InMemoryGraphStore,
    InMemoryVectorStore,
    Node,
    PgVectorStore,
    SQLiteGraphStore,
    SQLiteVectorStore,
    VectorSearchResult,
    VectorStore,
    VectorStoreError,
)
from akc.ingest.models import Document, DocumentMetadata, content_hash, stable_document_id
from akc.ingest.pipeline import (
    IngestionStateStore,
    IngestResult,
    IngestStats,
    build_vector_store,
    default_state_path,
    run_ingest,
)

__all__ = [
    "BaseConnector",
    "ChunkingConfig",
    "ChunkingError",
    "Connector",
    "ConnectorError",
    "chunk_documents",
    "Embedder",
    "EmbeddingError",
    "GeminiEmbedder",
    "HashEmbedder",
    "iter_documents",
    "DocsConnector",
    "DocsConnectorConfig",
    "Document",
    "DocumentMetadata",
    "IngestionError",
    "OpenAICompatibleEmbedder",
    "build_docs_connector",
    "OpenAPIConnector",
    "OpenAPIConnectorConfig",
    "build_openapi_connector",
    "SlackConnector",
    "SlackConnectorConfig",
    "build_slack_connector",
    "DiscordConnector",
    "DiscordConnectorConfig",
    "build_discord_connector",
    "TelegramUpdatesConnector",
    "TelegramConnectorConfig",
    "build_telegram_connector",
    "content_hash",
    "embed_documents",
    "embed_query",
    "normalize_documents",
    "normalize_text",
    "stable_document_id",
    "Edge",
    "GraphStore",
    "GraphStoreError",
    "InMemoryGraphStore",
    "InMemoryVectorStore",
    "Index",
    "IndexConfig",
    "Node",
    "PgVectorStore",
    "SQLiteGraphStore",
    "SQLiteVectorStore",
    "VectorSearchResult",
    "VectorStore",
    "VectorStoreError",
    "IngestResult",
    "IngestStats",
    "IngestionStateStore",
    "build_vector_store",
    "default_state_path",
    "run_ingest",
]
