"""Telegram messaging connector (Phase 1).

Telegram Bot API does not support historical backfill. Instead, bots receive new
events via `getUpdates` long-polling (or webhooks). This connector drains updates
incrementally using an `offset` stored in a small per-tenant state file.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from akc.ingest.connectors.base import BaseConnector
from akc.ingest.connectors.messaging.base import MessagingError
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document


def _require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _sleep_s(seconds: float) -> None:
    # Kept as a function so tests can monkeypatch it.
    time.sleep(seconds)


def _telegram_api_get_once(
    *,
    token: str,
    method: str,
    query: Mapping[str, Any] | None = None,
    timeout_s: float = 60.0,
) -> dict[str, Any]:
    _require_non_empty(token, name="token")
    _require_non_empty(method, name="method")
    if timeout_s <= 0:
        raise ValueError("timeout_s must be > 0")

    base = f"https://api.telegram.org/bot{token}"
    url = base + (method if method.startswith("/") else f"/{method}")
    if query:
        url = url + "?" + urlencode({k: v for k, v in dict(query).items() if v is not None}, doseq=True)
    req = Request(
        url,
        method="GET",
        headers={
            "Accept": "application/json",
            "User-Agent": "akc-ingest/telegram (stdlib)",
        },
    )
    try:
        with urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 (controlled URL)
            raw = resp.read()
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        msg = f"Telegram API request failed ({e.code}): {body}".strip()
        err = MessagingError(msg)
        err.status_code = int(e.code)  # type: ignore[attr-defined]
        raise err from e
    except URLError as e:
        raise MessagingError("Telegram API request failed to connect") from e

    try:
        decoded = raw.decode("utf-8")
        parsed = json.loads(decoded)
    except Exception as e:
        raise MessagingError("Telegram API response was not valid JSON") from e
    if not isinstance(parsed, dict):
        raise MessagingError("Telegram API response JSON must be an object")
    ok = parsed.get("ok")
    if ok is not True:
        desc = parsed.get("description")
        msg = f"Telegram API error: {desc}" if isinstance(desc, str) and desc.strip() else "Telegram API error"
        raise MessagingError(msg)
    return parsed


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(s)
        except Exception:
            return None
    return None


def _extract_message_text(message: Mapping[str, Any]) -> str:
    text = message.get("text")
    if isinstance(text, str):
        return text
    caption = message.get("caption")
    if isinstance(caption, str):
        return caption
    return ""


def _parse_update_message(update: Mapping[str, Any]) -> dict[str, Any] | None:
    # Telegram update can include many different fields. Focus on inbound message-like events.
    msg = update.get("message")
    if isinstance(msg, dict):
        return msg
    msg = update.get("edited_message")
    if isinstance(msg, dict):
        return msg
    msg = update.get("channel_post")
    if isinstance(msg, dict):
        return msg
    msg = update.get("edited_channel_post")
    if isinstance(msg, dict):
        return msg
    return None


class TelegramBotClient:
    """Minimal Telegram Bot API client used by the updates-drain connector."""

    def __init__(
        self,
        *,
        token: str,
        request_timeout_s: float = 70.0,
        max_retries: int = 3,
        min_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
    ) -> None:
        _require_non_empty(token, name="token")
        if request_timeout_s <= 0:
            raise ValueError("request_timeout_s must be > 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if min_backoff_s <= 0:
            raise ValueError("min_backoff_s must be > 0")
        if max_backoff_s <= 0:
            raise ValueError("max_backoff_s must be > 0")
        self._token = token
        self._request_timeout_s = float(request_timeout_s)
        self._max_retries = int(max_retries)
        self._min_backoff_s = float(min_backoff_s)
        self._max_backoff_s = float(max_backoff_s)

    def get_updates(
        self,
        *,
        offset: int | None = None,
        timeout_s: int = 50,
        limit: int = 100,
        allowed_updates: Sequence[str] | None = None,
    ) -> list[dict[str, Any]]:
        if timeout_s < 0:
            raise ValueError("timeout_s must be >= 0")
        if limit <= 0:
            return []

        # Telegram getUpdates accepts up to 100 updates per request.
        page_limit = min(100, int(limit))
        query: dict[str, Any] = {
            "timeout": int(timeout_s),
            "limit": int(page_limit),
        }
        if offset is not None:
            query["offset"] = int(offset)
        if allowed_updates is not None:
            query["allowed_updates"] = list(allowed_updates)

        attempt = 0
        backoff_s = self._min_backoff_s
        while True:
            try:
                parsed = _telegram_api_get_once(
                    token=self._token,
                    method="/getUpdates",
                    query=query,
                    timeout_s=self._request_timeout_s,
                )
                result = parsed.get("result")
                if result is None:
                    return []
                if not isinstance(result, list):
                    raise MessagingError("Telegram getUpdates result must be a list")
                return [u for u in result if isinstance(u, dict)]
            except MessagingError as e:
                status_code = getattr(e, "status_code", None)
                is_transient = status_code in {408, 429, 500, 502, 503, 504}
                if attempt >= self._max_retries or not is_transient:
                    raise
                _sleep_s(min(self._max_backoff_s, max(0.0, backoff_s)))
                attempt += 1
                backoff_s = min(self._max_backoff_s, backoff_s * 2.0)


@dataclass(frozen=True, slots=True)
class TelegramConnectorConfig:
    """Configuration for Telegram updates-drain ingestion.

    Args:
        bot_token: Bot token (do not hardcode; pass from env/CLI).
        allowed_chat_ids: Optional chat id allowlist. If set, only messages from these chats are emitted.
        allowed_updates: Optional list forwarded to getUpdates (e.g. ["message", "edited_message"]).
        max_updates_per_run: Safety cap on how many updates to drain per `fetch()` call.
        long_poll_timeout_s: getUpdates long-poll timeout seconds.
        request_timeout_s: HTTP request timeout seconds (must exceed long_poll_timeout_s with some headroom).
        state_path: Optional path to a per-tenant offset state JSON file. If omitted, offset is not persisted.
        initial_offset: Optional starting update_id offset when no state exists.
    """

    bot_token: str
    allowed_chat_ids: tuple[int, ...] | None = None
    allowed_updates: tuple[str, ...] | None = None
    max_updates_per_run: int = 1000
    long_poll_timeout_s: int = 50
    request_timeout_s: float = 70.0
    max_retries: int = 3
    state_path: str | None = None
    initial_offset: int | None = None

    def __post_init__(self) -> None:
        _require_non_empty(self.bot_token, name="bot_token")
        if self.allowed_chat_ids is not None and any(not isinstance(x, int) for x in self.allowed_chat_ids):
            raise ValueError("allowed_chat_ids must be ints")
        if self.allowed_updates is not None and any(
            (not isinstance(x, str)) or (not x.strip()) for x in self.allowed_updates
        ):
            raise ValueError("allowed_updates must be non-empty strings")
        if self.max_updates_per_run <= 0:
            raise ValueError("max_updates_per_run must be > 0")
        if self.long_poll_timeout_s < 0:
            raise ValueError("long_poll_timeout_s must be >= 0")
        if self.request_timeout_s <= 0:
            raise ValueError("request_timeout_s must be > 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if self.state_path is not None:
            _require_non_empty(self.state_path, name="state_path")


class _TelegramOffsetState:
    """Tiny JSON state file holding the next update_id offset (per tenant)."""

    def __init__(self, *, path: str, tenant_id: str) -> None:
        _require_non_empty(path, name="path")
        _require_non_empty(tenant_id, name="tenant_id")
        self._path = Path(path)
        self._tenant_id = tenant_id

    def load_offset(self, *, fallback: int | None = None) -> int | None:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return fallback
        except OSError as e:
            raise ConnectorError(f"failed to read telegram state: {self._path}") from e
        try:
            data = json.loads(raw)
        except Exception as e:
            raise ConnectorError(f"telegram state is not valid JSON: {self._path}") from e
        if not isinstance(data, dict):
            raise ConnectorError(f"telegram state must be a JSON object: {self._path}")
        tid = data.get("tenant_id")
        if isinstance(tid, str) and tid and tid != self._tenant_id:
            raise ConnectorError("telegram state tenant_id mismatch (possible cross-tenant state)")
        offset = _coerce_int(data.get("next_update_id"))
        return offset if offset is not None else fallback

    def save_offset(self, *, next_update_id: int) -> None:
        if next_update_id < 0:
            raise ConnectorError("next_update_id must be >= 0")
        payload = {"tenant_id": self._tenant_id, "next_update_id": int(next_update_id)}
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self._path)
        except OSError as e:
            raise ConnectorError(f"failed to write telegram state: {self._path}") from e


class TelegramUpdatesConnector(BaseConnector):
    """Telegram connector emitting one document per new message-like update."""

    def __init__(self, *, tenant_id: str, config: TelegramConnectorConfig) -> None:
        super().__init__(tenant_id=tenant_id, source_type="messaging")
        self._config = config
        self._client = TelegramBotClient(
            token=config.bot_token,
            request_timeout_s=config.request_timeout_s,
            max_retries=config.max_retries,
        )
        self._state = (
            _TelegramOffsetState(path=config.state_path, tenant_id=tenant_id) if config.state_path is not None else None
        )

    @property
    def config(self) -> TelegramConnectorConfig:
        return self._config

    def list_sources(self) -> Sequence[str]:
        # Single logical source: this bot's updates stream (optionally chat-filtered).
        return ["updates"]

    def _chat_allowed(self, *, chat_id: int) -> bool:
        allow = self._config.allowed_chat_ids
        if allow is None:
            return True
        return chat_id in allow

    def fetch(self, source_id: str) -> Iterable[Document]:
        if source_id != "updates":
            raise ConnectorError("unknown source_id for TelegramUpdatesConnector")

        offset = self._state.load_offset(fallback=self._config.initial_offset) if self._state is not None else None
        drained = 0
        next_offset = offset

        while drained < self._config.max_updates_per_run:
            remaining = self._config.max_updates_per_run - drained
            page_limit = min(100, int(remaining))
            try:
                updates = self._client.get_updates(
                    offset=next_offset,
                    timeout_s=int(self._config.long_poll_timeout_s),
                    limit=int(page_limit),
                    allowed_updates=(
                        list(self._config.allowed_updates) if self._config.allowed_updates is not None else None
                    ),
                )
            except MessagingError as e:
                raise ConnectorError("Telegram getUpdates failed") from e

            if not updates:
                break

            for upd in updates:
                drained += 1
                update_id = _coerce_int(upd.get("update_id"))
                if update_id is None:
                    continue
                # Next offset is last update_id + 1 (canonical Telegram semantics).
                if next_offset is None or update_id + 1 > next_offset:
                    next_offset = update_id + 1

                msg = _parse_update_message(upd)
                if msg is None:
                    continue

                chat = msg.get("chat")
                if not isinstance(chat, dict):
                    continue
                chat_id_val = _coerce_int(chat.get("id"))
                if chat_id_val is None:
                    continue
                if not self._chat_allowed(chat_id=chat_id_val):
                    continue

                message_id_val = _coerce_int(msg.get("message_id"))
                if message_id_val is None:
                    continue

                text = _extract_message_text(msg).strip()
                if not text:
                    continue

                from_user = msg.get("from")
                user_id: str = ""
                if isinstance(from_user, dict):
                    uid = _coerce_int(from_user.get("id"))
                    if uid is not None:
                        user_id = str(uid)

                date_unix = _coerce_int(msg.get("date"))
                timestamp = str(date_unix) if date_unix is not None else ""

                # Thread mapping: Telegram forum topics supply message_thread_id; otherwise treat each message
                # as its own thread.
                thread_id_val = _coerce_int(msg.get("message_thread_id"))
                thread_id = str(thread_id_val) if thread_id_val is not None else str(message_id_val)

                indexed_at_ms = (int(date_unix) * 1000) if date_unix is not None else int(time.time() * 1000)
                content = (
                    "\n".join(
                        [
                            f"Telegram message {message_id_val} in chat {chat_id_val}",
                            "",
                            (f"From {user_id} @ {timestamp}:" if user_id or timestamp else "Message:"),
                            text,
                            "",
                        ]
                    ).strip()
                    + "\n"
                )
                metadata: dict[str, object] = {
                    "platform": "telegram",
                    "chat_id": str(chat_id_val),
                    "thread_id": thread_id,
                    "message_id": str(message_id_val),
                    "timestamp": timestamp,
                    "user": user_id,
                    "ingest_source_kind": "messaging",
                    "indexed_at_ms": indexed_at_ms,
                }
                yield self._make_document(
                    source=f"telegram:{chat_id_val}",
                    logical_locator=f"thread:{thread_id}/message:{message_id_val}",
                    content=content,
                    metadata=metadata,
                )

            # If Telegram returned a "full page", keep draining; otherwise stop to avoid busy loop.
            if len(updates) < page_limit:
                break

        if self._state is not None and next_offset is not None:
            self._state.save_offset(next_update_id=int(next_offset))


def build_telegram_connector(
    *,
    tenant_id: str,
    bot_token: str,
    allowed_chat_ids: Sequence[int] | None = None,
    allowed_updates: Sequence[str] | None = None,
    max_updates_per_run: int = 1000,
    long_poll_timeout_s: int = 50,
    request_timeout_s: float = 70.0,
    max_retries: int = 3,
    state_path: str | None = None,
    initial_offset: int | None = None,
) -> TelegramUpdatesConnector:
    return TelegramUpdatesConnector(
        tenant_id=tenant_id,
        config=TelegramConnectorConfig(
            bot_token=bot_token,
            allowed_chat_ids=(tuple(int(x) for x in allowed_chat_ids) if allowed_chat_ids is not None else None),
            allowed_updates=(tuple(str(x) for x in allowed_updates) if allowed_updates is not None else None),
            max_updates_per_run=int(max_updates_per_run),
            long_poll_timeout_s=int(long_poll_timeout_s),
            request_timeout_s=float(request_timeout_s),
            max_retries=int(max_retries),
            state_path=state_path,
            initial_offset=initial_offset,
        ),
    )

