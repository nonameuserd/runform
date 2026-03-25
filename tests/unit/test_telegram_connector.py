from __future__ import annotations

import io
import json
from pathlib import Path
from typing import Any

import pytest

from akc.ingest.connectors.messaging.telegram import (
    MessagingError,
    TelegramConnectorConfig,
    TelegramUpdatesConnector,
    _telegram_api_get_once,
)
from akc.ingest.exceptions import ConnectorError


def test_telegram_connector_emits_docs_and_advances_offset(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    state_path = tmp_path / "telegram.state.json"
    calls: list[dict[str, object]] = []

    def fake_get_once(
        *,
        token: str,
        method: str,
        query: dict[str, Any] | None = None,
        timeout_s: float = 60.0,
    ) -> dict[str, Any]:
        assert token == "bot-test"
        assert method == "/getUpdates"
        assert isinstance(query, dict)
        calls.append(dict(query))

        # First call returns two updates (one allowed chat, one filtered out).
        offset = query.get("offset")
        if offset in (None, 100):
            return {
                "ok": True,
                "result": [
                    {
                        "update_id": 100,
                        "message": {
                            "message_id": 7,
                            "date": 1_742_900_000,
                            "chat": {"id": 111},
                            "from": {"id": 222},
                            "text": "Hello",
                        },
                    },
                    {
                        "update_id": 101,
                        "message": {
                            "message_id": 8,
                            "date": 1_742_900_001,
                            "chat": {"id": 999},
                            "from": {"id": 333},
                            "text": "Should be filtered",
                        },
                    },
                ],
            }

        # Second call is empty (drain complete).
        assert offset == 102
        return {"ok": True, "result": []}

    monkeypatch.setattr("akc.ingest.connectors.messaging.telegram._telegram_api_get_once", fake_get_once)

    conn = TelegramUpdatesConnector(
        tenant_id="tenant-1",
        config=TelegramConnectorConfig(
            bot_token="bot-test",
            allowed_chat_ids=(111,),
            state_path=str(state_path),
            initial_offset=100,
            max_updates_per_run=10,
            long_poll_timeout_s=0,
        ),
    )
    docs = list(conn.fetch("updates"))
    assert len(docs) == 1
    assert docs[0].metadata["tenant_id"] == "tenant-1"
    assert docs[0].metadata["source_type"] == "messaging"
    assert "Hello" in docs[0].content

    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["tenant_id"] == "tenant-1"
    assert saved["next_update_id"] == 102
    assert calls, "expected at least one API call"


def test_telegram_connector_rejects_unknown_source_id() -> None:
    conn = TelegramUpdatesConnector(tenant_id="t", config=TelegramConnectorConfig(bot_token="x"))
    with pytest.raises(ConnectorError, match=r"unknown source_id"):
        list(conn.fetch("nope"))


def test_telegram_connector_blocks_cross_tenant_state(tmp_path: Path) -> None:
    state_path = tmp_path / "telegram.state.json"
    state_path.write_text(json.dumps({"tenant_id": "other-tenant", "next_update_id": 10}), encoding="utf-8")
    conn = TelegramUpdatesConnector(
        tenant_id="tenant-1",
        config=TelegramConnectorConfig(bot_token="x", state_path=str(state_path)),
    )
    with pytest.raises(ConnectorError, match=r"tenant_id mismatch"):
        list(conn.fetch("updates"))


def test_telegram_connector_paginates_until_empty_and_persists_next_offset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "telegram.state.json"
    seen_offsets: list[int | None] = []

    def fake_get_once(
        *,
        token: str,
        method: str,
        query: dict[str, Any] | None = None,
        timeout_s: float = 60.0,
    ) -> dict[str, Any]:
        assert token == "bot-test"
        assert method == "/getUpdates"
        assert isinstance(query, dict)
        seen_offsets.append(query.get("offset"))  # type: ignore[arg-type]

        # One page with < page_limit items => connector stops after this page.
        if query.get("offset") in (None, 10):
            return {
                "ok": True,
                "result": [
                    {
                        "update_id": 10,
                        "message": {
                            "message_id": 1,
                            "date": 1_742_900_000,
                            "chat": {"id": 111},
                            "from": {"id": 222},
                            "text": "A",
                        },
                    },
                    {
                        "update_id": 11,
                        "message": {
                            "message_id": 2,
                            "date": 1_742_900_001,
                            "chat": {"id": 111},
                            "from": {"id": 222},
                            "text": "B",
                        },
                    },
                ],
            }
        raise AssertionError("unexpected second getUpdates call")

    monkeypatch.setattr("akc.ingest.connectors.messaging.telegram._telegram_api_get_once", fake_get_once)
    conn = TelegramUpdatesConnector(
        tenant_id="tenant-1",
        config=TelegramConnectorConfig(
            bot_token="bot-test",
            state_path=str(state_path),
            initial_offset=10,
            max_updates_per_run=10,
            long_poll_timeout_s=0,
        ),
    )
    docs = list(conn.fetch("updates"))
    assert len(docs) == 2
    saved = json.loads(state_path.read_text(encoding="utf-8"))
    assert saved["next_update_id"] == 12
    assert seen_offsets == [10]


def test_telegram_connector_raises_connector_error_on_http_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_get_once(
        *,
        token: str,
        method: str,
        query: dict[str, Any] | None = None,
        timeout_s: float = 60.0,
    ) -> dict[str, Any]:
        e = MessagingError("Telegram API request failed (401): unauthorized")
        e.status_code = 401  # type: ignore[attr-defined]
        raise e

    monkeypatch.setattr("akc.ingest.connectors.messaging.telegram._telegram_api_get_once", fake_get_once)
    conn = TelegramUpdatesConnector(tenant_id="t", config=TelegramConnectorConfig(bot_token="bot-test"))
    with pytest.raises(ConnectorError, match=r"Telegram getUpdates failed"):
        list(conn.fetch("updates"))


def test_telegram_api_get_once_maps_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    from urllib.error import HTTPError

    def fake_urlopen(req, timeout: float = 60.0):  # noqa: ANN001
        raise HTTPError(
            url=req.full_url,
            code=500,
            msg="Internal Server Error",
            hdrs={},
            fp=io.BytesIO(b"boom"),
        )

    monkeypatch.setattr("akc.ingest.connectors.messaging.telegram.urlopen", fake_urlopen)
    with pytest.raises(MessagingError) as ei:
        _telegram_api_get_once(token="bot-test", method="/getUpdates", query={"timeout": 0}, timeout_s=1.0)
    assert getattr(ei.value, "status_code", None) == 500


def test_telegram_api_get_once_raises_on_invalid_json(monkeypatch: pytest.MonkeyPatch) -> None:
    class DummyResp:
        def __enter__(self):  # noqa: ANN001
            return self

        def __exit__(self, exc_type, exc, tb):  # noqa: ANN001
            return False

        def read(self) -> bytes:
            return b"not-json"

    def fake_urlopen(req, timeout: float = 60.0):  # noqa: ANN001
        return DummyResp()

    monkeypatch.setattr("akc.ingest.connectors.messaging.telegram.urlopen", fake_urlopen)
    with pytest.raises(MessagingError, match=r"not valid JSON"):
        _telegram_api_get_once(token="bot-test", method="/getUpdates", query={"timeout": 0}, timeout_s=1.0)
