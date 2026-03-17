from __future__ import annotations

from collections.abc import Mapping

import pytest

from akc.ingest.connectors.messaging.slack import (
    MessagingError,
    SlackConnector,
    SlackConnectorConfig,
)
from akc.ingest.exceptions import ConnectorError


def test_slack_connector_emits_thread_documents(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    def fake_post(
        *, token: str, method: str, payload: Mapping[str, object], timeout_s: float = 30.0
    ):
        calls.append(method)
        assert token == "xoxb-test"
        if method == "conversations.history":
            # Two top-level messages; one without explicit thread_ts.
            return {
                "ok": True,
                "messages": [
                    {
                        "ts": "1700.0001",
                        "text": "How do I deploy?",
                        "user": "U1",
                        "thread_ts": "1700.0001",
                    },
                    {"ts": "1700.0002", "text": "FYI: release today", "user": "U2"},
                ],
            }
        if method == "conversations.replies":
            ts = payload.get("ts")
            if ts == "1700.0001":
                return {
                    "ok": True,
                    "messages": [
                        {
                            "ts": "1700.0001",
                            "text": "How do I deploy?",
                            "user": "U1",
                            "thread_ts": "1700.0001",
                        },
                        {"ts": "1700.0003", "text": "Use the staging pipeline.", "user": "U3"},
                    ],
                }
            if ts == "1700.0002":
                return {
                    "ok": True,
                    "messages": [
                        {
                            "ts": "1700.0002",
                            "text": "FYI: release today",
                            "user": "U2",
                            "thread_ts": "1700.0002",
                        },
                        {"ts": "1700.0004", "text": "Thanks!", "user": "U4"},
                    ],
                }
            raise AssertionError(f"unexpected thread ts: {ts!r}")
        raise AssertionError(f"unexpected method: {method}")

    monkeypatch.setattr("akc.ingest.connectors.messaging.slack._slack_api_post_once", fake_post)

    conn = SlackConnector(
        tenant_id="tenant-1",
        config=SlackConnectorConfig(
            channel_id="C1",
            token="xoxb-test",
            max_threads=10,
            max_answers=3,
        ),
    )
    docs = list(conn.fetch("C1"))
    assert len(docs) == 2
    assert all(d.metadata["tenant_id"] == "tenant-1" for d in docs)
    assert all(d.metadata["source_type"] == "messaging" for d in docs)
    assert all(d.metadata["channel"] == "C1" for d in docs)
    assert any("Q (U1" in d.content for d in docs)
    assert any("Use the staging pipeline." in d.content for d in docs)
    assert calls.count("conversations.history") == 1
    assert calls.count("conversations.replies") == 2


def test_slack_connector_rejects_unknown_source_id() -> None:
    conn = SlackConnector(tenant_id="t", config=SlackConnectorConfig(channel_id="C1", token="x"))
    with pytest.raises(ConnectorError, match=r"unknown source_id"):
        list(conn.fetch("C2"))


def test_slack_connector_paginates_history(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[tuple[str, object | None]] = []

    def fake_post(
        *, token: str, method: str, payload: Mapping[str, object], timeout_s: float = 30.0
    ):
        assert token == "xoxb-test"
        cursor = payload.get("cursor")
        calls.append((method, cursor))
        if method == "conversations.history" and cursor is None:
            return {
                "ok": True,
                "messages": [{"ts": "1700.0001", "text": "Q1", "user": "U1"}],
                "response_metadata": {"next_cursor": "next"},
            }
        if method == "conversations.history" and cursor == "next":
            return {
                "ok": True,
                "messages": [{"ts": "1700.0002", "text": "Q2", "user": "U2"}],
                "response_metadata": {"next_cursor": ""},
            }
        if method == "conversations.replies":
            ts = payload.get("ts")
            return {
                "ok": True,
                "messages": [
                    {"ts": str(ts), "text": "Q", "user": "U1", "thread_ts": str(ts)},
                    {"ts": "1700.0003", "text": "A", "user": "U3"},
                ],
            }
        raise AssertionError(f"unexpected method: {method}")

    monkeypatch.setattr("akc.ingest.connectors.messaging.slack._slack_api_post_once", fake_post)

    conn = SlackConnector(
        tenant_id="tenant-1",
        config=SlackConnectorConfig(
            channel_id="C1",
            token="xoxb-test",
            history_limit=2,
            max_threads=2,
            max_answers=1,
        ),
    )
    docs = list(conn.fetch("C1"))
    assert len(docs) == 2
    assert [m for (m, _c) in calls if m == "conversations.history"] == [
        "conversations.history",
        "conversations.history",
    ]


def test_slack_client_retries_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []
    calls: list[str] = []

    def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    state = {"called": False}

    def fake_post(
        *, token: str, method: str, payload: Mapping[str, object], timeout_s: float = 30.0
    ):
        calls.append(method)
        if not state["called"]:
            state["called"] = True
            e = MessagingError("Slack API request failed (429): rate limited")
            e.status_code = 429  # type: ignore[attr-defined]
            e.retry_after_seconds = 0.0  # type: ignore[attr-defined]
            raise e
        if method == "conversations.history":
            return {
                "ok": True,
                "messages": [{"ts": "1700.0001", "text": "Q", "user": "U1"}],
            }
        if method == "conversations.replies":
            ts = payload.get("ts")
            return {
                "ok": True,
                "messages": [
                    {"ts": str(ts), "text": "Q", "user": "U1", "thread_ts": str(ts)},
                    {"ts": "1700.0002", "text": "A", "user": "U2"},
                ],
            }
        raise AssertionError(f"unexpected method: {method}")

    monkeypatch.setattr("akc.ingest.connectors.messaging.slack._sleep_s", fake_sleep)
    monkeypatch.setattr("akc.ingest.connectors.messaging.slack._slack_api_post_once", fake_post)

    conn = SlackConnector(
        tenant_id="tenant-1",
        config=SlackConnectorConfig(
            channel_id="C1",
            token="xoxb-test",
            max_threads=1,
            max_retries=1,
        ),
    )
    docs = list(conn.fetch("C1"))
    assert len(docs) == 1
    assert slept, "expected a backoff sleep on rate limit"
    assert "conversations.history" in calls
