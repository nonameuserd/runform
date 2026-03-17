"""Connector interfaces and implementations."""

from akc.ingest.connectors.base import BaseConnector, Connector, iter_documents
from akc.ingest.connectors.docs import DocsConnector, DocsConnectorConfig, build_docs_connector
from akc.ingest.connectors.messaging.slack import (
    SlackConnector,
    SlackConnectorConfig,
    build_slack_connector,
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
    "DocsConnector",
    "DocsConnectorConfig",
    "build_docs_connector",
    "OpenAPIConnector",
    "OpenAPIConnectorConfig",
    "build_openapi_connector",
    "SlackConnector",
    "SlackConnectorConfig",
    "build_slack_connector",
]
