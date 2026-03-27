"""Connector interfaces and implementations."""

from akc.ingest.connectors.base import BaseConnector, Connector, iter_documents
from akc.ingest.connectors.codebase import CodebaseConnector, CodebaseConnectorConfig, build_codebase_connector
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

__all__ = [
    "BaseConnector",
    "Connector",
    "iter_documents",
    "CodebaseConnector",
    "CodebaseConnectorConfig",
    "build_codebase_connector",
    "DocsConnector",
    "DocsConnectorConfig",
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
]
