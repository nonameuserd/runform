"""Messaging connector abstractions and implementations."""

from akc.ingest.connectors.messaging.base import (
    Channel,
    Message,
    MessagingClient,
    MessagingError,
    QAPair,
    Thread,
    extract_qa_pairs,
)
from akc.ingest.connectors.messaging.discord import (
    DiscordConnector,
    DiscordConnectorConfig,
    DiscordMessagingClient,
    build_discord_connector,
)
from akc.ingest.connectors.messaging.slack import (
    SlackConnector,
    SlackConnectorConfig,
    build_slack_connector,
)
from akc.ingest.connectors.messaging.telegram import (
    TelegramBotClient,
    TelegramConnectorConfig,
    TelegramUpdatesConnector,
    build_telegram_connector,
)

__all__ = [
    "Channel",
    "Message",
    "MessagingClient",
    "MessagingError",
    "QAPair",
    "Thread",
    "extract_qa_pairs",
    "SlackConnector",
    "SlackConnectorConfig",
    "build_slack_connector",
    "DiscordConnector",
    "DiscordConnectorConfig",
    "DiscordMessagingClient",
    "build_discord_connector",
    "TelegramBotClient",
    "TelegramConnectorConfig",
    "TelegramUpdatesConnector",
    "build_telegram_connector",
]
