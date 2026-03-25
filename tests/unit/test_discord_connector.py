from __future__ import annotations

import io
from collections.abc import Mapping
from typing import Any

import pytest

from akc.ingest.connectors.messaging.discord import (
    DiscordConnector,
    DiscordConnectorConfig,
    MessagingError,
    _discord_api_get_once,
)
from akc.ingest.exceptions import ConnectorError


def test_discord_connector_emits_thread_documents(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, Mapping[str, object] | None]] = []

    def fake_get_once(*, token: str, path: str, query: dict[str, Any] | None = None, timeout_s: float = 30.0):
        assert token == "bot-test"
        calls.append((path, query))

        if path == "/channels/C1/messages":
            # One message that starts a thread, one normal message.
            return [
                {
                    "id": "M2",
                    "timestamp": "2026-03-25T10:00:01.000Z",
                    "content": "How do I deploy?",
                    "author": {"id": "U1"},
                    "thread": {"id": "T1"},
                },
                {
                    "id": "M1",
                    "timestamp": "2026-03-25T10:00:00.000Z",
                    "content": "FYI: release today",
                    "author": {"id": "U2"},
                },
            ]

        if path == "/channels/T1/messages":
            # Discord returns newest-first; connector reverses for extraction.
            return [
                {
                    "id": "R2",
                    "timestamp": "2026-03-25T10:00:03.000Z",
                    "content": "Use the staging pipeline.",
                    "author": {"id": "U3"},
                },
                {
                    "id": "R1",
                    "timestamp": "2026-03-25T10:00:02.000Z",
                    "content": "First, tag a release.",
                    "author": {"id": "U4"},
                },
            ]

        if path == "/channels/C1/messages/M1":
            return {
                "id": "M1",
                "timestamp": "2026-03-25T10:00:00.000Z",
                "content": "FYI: release today",
                "author": {"id": "U2"},
            }

        if path == "/channels/T1":
            return {"id": "T1", "type": 11}

        if path == "/channels/M1":
            return {"id": "M1", "type": 0}

        raise AssertionError(f"unexpected request: {path} {query}")

    monkeypatch.setattr("akc.ingest.connectors.messaging.discord._discord_api_get_once", fake_get_once)

    conn = DiscordConnector(
        tenant_id="tenant-1",
        config=DiscordConnectorConfig(
            channel_id="C1",
            token="bot-test",
            history_limit=10,
            max_threads=10,
            max_answers=3,
        ),
    )
    docs = list(conn.fetch("C1"))
    assert len(docs) == 2
    assert all(d.metadata["tenant_id"] == "tenant-1" for d in docs)
    assert all(d.metadata["source_type"] == "messaging" for d in docs)
    assert any("How do I deploy?" in d.content for d in docs)
    assert any("Use the staging pipeline." in d.content for d in docs)
    assert any("FYI: release today" in d.content for d in docs)
    assert calls, "expected at least one API call"


def test_discord_connector_rejects_unknown_source_id() -> None:
    conn = DiscordConnector(tenant_id="t", config=DiscordConnectorConfig(channel_id="C1", token="x"))
    with pytest.raises(ConnectorError, match=r"unknown source_id"):
        list(conn.fetch("C2"))


def test_discord_client_paginates_before(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object | None]] = []

    def fake_get_once(*, token: str, path: str, query: dict[str, Any] | None = None, timeout_s: float = 30.0):
        assert token == "bot-test"
        if path != "/channels/C1/messages":
            raise AssertionError(f"unexpected path: {path}")
        before = (query or {}).get("before") if query else None
        calls.append((path, before))
        if before is None:
            return [
                {
                    "id": "M2",
                    "timestamp": "2026-03-25T10:00:01.000Z",
                    "content": "Q2",
                    "author": {"id": "U2"},
                }
            ]
        if before == "M2":
            return [
                {
                    "id": "M1",
                    "timestamp": "2026-03-25T10:00:00.000Z",
                    "content": "Q1",
                    "author": {"id": "U1"},
                }
            ]
        return []

    monkeypatch.setattr("akc.ingest.connectors.messaging.discord._discord_api_get_once", fake_get_once)

    conn = DiscordConnector(
        tenant_id="tenant-1",
        config=DiscordConnectorConfig(channel_id="C1", token="bot-test", history_limit=2, max_threads=2, max_answers=1),
    )
    docs = list(conn.fetch("C1"))
    assert len(docs) == 2
    assert calls == [
        ("/channels/C1/messages", None),
        ("/channels/C1/messages", "M2"),
    ]


def test_discord_client_retries_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    called = {"n": 0}

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    def fake_get_once(*, token: str, path: str, query: dict[str, Any] | None = None, timeout_s: float = 30.0):
        assert token == "bot-test"
        called["n"] += 1
        if called["n"] == 1:
            e = MessagingError("Discord API request failed (429): rate limited")
            e.status_code = 429  # type: ignore[attr-defined]
            e.retry_after_seconds = 0.0  # type: ignore[attr-defined]
            raise e
        if path == "/channels/C1/messages":
            return [
                {
                    "id": "M1",
                    "timestamp": "2026-03-25T10:00:00.000Z",
                    "content": "Q",
                    "author": {"id": "U1"},
                }
            ]
        if path == "/channels/C1/messages/M1":
            return {
                "id": "M1",
                "timestamp": "2026-03-25T10:00:00.000Z",
                "content": "Q",
                "author": {"id": "U1"},
            }
        if path == "/channels/M1":
            return {"id": "M1", "type": 0}
        raise AssertionError(f"unexpected request: {path} {query}")

    monkeypatch.setattr("akc.ingest.connectors.messaging.discord._sleep_s", fake_sleep)
    monkeypatch.setattr("akc.ingest.connectors.messaging.discord._discord_api_get_once", fake_get_once)

    conn = DiscordConnector(
        tenant_id="tenant-1",
        config=DiscordConnectorConfig(channel_id="C1", token="bot-test", max_threads=1, max_retries=1),
    )
    docs = list(conn.fetch("C1"))
    assert len(docs) == 1
    assert slept, "expected a backoff sleep on rate limit"


def test_discord_connector_dedupes_threads_by_thread_id(monkeypatch: pytest.MonkeyPatch) -> None:
    """If multiple parent messages point at same Discord thread channel, only emit once."""

    def fake_get_once(*, token: str, path: str, query: dict[str, Any] | None = None, timeout_s: float = 30.0):
        assert token == "bot-test"
        if path == "/channels/C1/messages":
            return [
                {
                    "id": "M2",
                    "timestamp": "2026-03-25T10:00:01.000Z",
                    "content": "Parent A",
                    "author": {"id": "U1"},
                    "thread": {"id": "T1"},
                },
                {
                    "id": "M1",
                    "timestamp": "2026-03-25T10:00:00.000Z",
                    "content": "Parent B (duplicate thread)",
                    "author": {"id": "U2"},
                    "thread": {"id": "T1"},
                },
            ]
        if path == "/channels/T1/messages":
            return [
                {
                    "id": "R1",
                    "timestamp": "2026-03-25T10:00:02.000Z",
                    "content": "Reply",
                    "author": {"id": "U3"},
                }
            ]
        if path == "/channels/T1":
            return {"id": "T1", "type": 11}
        raise AssertionError(f"unexpected request: {path} {query}")

    monkeypatch.setattr("akc.ingest.connectors.messaging.discord._discord_api_get_once", fake_get_once)

    conn = DiscordConnector(
        tenant_id="tenant-1",
        config=DiscordConnectorConfig(
            channel_id="C1",
            token="bot-test",
            history_limit=10,
            max_threads=10,
            max_answers=3,
        ),
    )
    docs = list(conn.fetch("C1"))
    assert len(docs) == 1


def test_discord_connector_raises_connector_error_on_http_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_once(*, token: str, path: str, query: dict[str, Any] | None = None, timeout_s: float = 30.0):
        e = MessagingError("Discord API request failed (401): unauthorized")
        e.status_code = 401  # type: ignore[attr-defined]
        raise e

    monkeypatch.setattr("akc.ingest.connectors.messaging.discord._discord_api_get_once", fake_get_once)
    conn = DiscordConnector(tenant_id="t", config=DiscordConnectorConfig(channel_id="C1", token="bot-test"))
    with pytest.raises(ConnectorError, match=r"Discord list channel messages failed"):
        list(conn.fetch("C1"))


def test_discord_api_get_once_maps_http_error_and_parses_retry_after(monkeypatch: pytest.MonkeyPatch) -> None:
    # Simulate an HTTP 429 where retry-after is present.
    from urllib.error import HTTPError

    def fake_urlopen(req, timeout: float = 30.0):  # noqa: ANN001
        raise HTTPError(
            url=req.full_url,
            code=429,
            msg="Too Many Requests",
            hdrs={"Retry-After": "1.5"},
            fp=io.BytesIO(b'{"message":"rate limited"}'),
        )

    monkeypatch.setattr("akc.ingest.connectors.messaging.discord.urlopen", fake_urlopen)
    with pytest.raises(MessagingError) as ei:
        _discord_api_get_once(token="bot-test", path="/channels/C1/messages", query={"limit": 1}, timeout_s=1.0)
    err = ei.value
    assert getattr(err, "status_code", None) == 429
    assert getattr(err, "retry_after_seconds", None) == 1.5


def test_discord_api_get_once_raises_on_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyResp:
        status = 200

        def __enter__(self):  # noqa: ANN001
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def read(self) -> bytes:
            return b"not-json"

    def fake_urlopen(req, timeout: float = 30.0):  # noqa: ANN001
        return DummyResp()

    monkeypatch.setattr("akc.ingest.connectors.messaging.discord.urlopen", fake_urlopen)
    with pytest.raises(MessagingError, match=r"not valid JSON"):
        _discord_api_get_once(token="bot-test", path="/channels/C1/messages", query={"limit": 1}, timeout_s=1.0)
