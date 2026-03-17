"""Slack messaging connector (Phase 1).

Uses Slack Web API via stdlib HTTP to avoid introducing a hard dependency on slack-sdk.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from akc.ingest.connectors.base import BaseConnector
from akc.ingest.connectors.messaging.base import (
    Channel,
    Message,
    MessagingClient,
    MessagingError,
    Thread,
    extract_qa_pairs,
)
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document


def _require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _parse_retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        s = value.strip()
        if not s:
            return None
        # Slack uses integer seconds. Accept float just in case.
        return max(0.0, float(s))
    except Exception:
        return None


def _slack_api_post_once(
    *,
    token: str,
    method: str,
    payload: Mapping[str, Any],
    timeout_s: float = 30.0,
) -> dict[str, Any]:
    _require_non_empty(token, name="token")
    _require_non_empty(method, name="method")
    if timeout_s <= 0:
        raise ValueError("timeout_s must be > 0")

    url = "https://slack.com/api/" + method.lstrip("/")
    data = json.dumps(dict(payload)).encode("utf-8")
    req = Request(
        url,
        method="POST",
        data=data,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "Authorization": f"Bearer {token}",
        },
    )
    try:
        with urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 (controlled URL)
            raw = resp.read()
    except HTTPError as e:
        # Re-raise with details so the caller can decide whether to retry.
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        retry_after = _parse_retry_after_seconds(e.headers.get("Retry-After"))
        msg = f"Slack API request failed ({e.code}): {body}".strip()
        err = MessagingError(msg)
        err.status_code = int(e.code)  # type: ignore[attr-defined]
        err.retry_after_seconds = retry_after  # type: ignore[attr-defined]
        raise err from e
    except URLError as e:
        raise MessagingError("Slack API request failed to connect") from e

    try:
        decoded = raw.decode("utf-8")
        parsed = json.loads(decoded)
    except Exception as e:
        raise MessagingError("Slack API response was not valid JSON") from e
    if not isinstance(parsed, dict):
        raise MessagingError("Slack API response JSON must be an object")
    ok = parsed.get("ok")
    if ok is not True:
        err_msg = parsed.get("error")
        msg = (
            f"Slack API error: {err_msg}"
            if isinstance(err_msg, str) and err_msg
            else "Slack API error"
        )
        raise MessagingError(msg)
    return parsed


def _to_message(*, channel_id: str, item: Mapping[str, Any]) -> Message:
    ts = item.get("ts")
    if not isinstance(ts, str) or not ts.strip():
        raise MessagingError("Slack message missing ts")
    text = item.get("text")
    if not isinstance(text, str):
        text = ""
    user_id: str | None = None
    user = item.get("user")
    if isinstance(user, str) and user.strip():
        user_id = user
    # Bot signals
    is_bot = False
    if item.get("bot_id") is not None:
        is_bot = True
    subtype = item.get("subtype")
    if isinstance(subtype, str) and "bot" in subtype:
        is_bot = True
    thread_ts = item.get("thread_ts")
    thread_id: str | None = None
    if isinstance(thread_ts, str) and thread_ts.strip():
        thread_id = thread_ts
    return Message(
        id=ts,
        channel_id=channel_id,
        user_id=user_id,
        text=text,
        timestamp=ts,
        thread_id=thread_id,
        is_bot=is_bot,
    )


def _sleep_s(seconds: float) -> None:
    # Kept as a function so tests can monkeypatch it.
    time.sleep(seconds)


class SlackMessagingClient(MessagingClient):
    """Slack implementation of the normalized MessagingClient abstraction.

    This client is responsible for:
    - Cursor-based pagination
    - Basic retry handling for rate limits and transient errors
    """

    def __init__(
        self,
        *,
        token: str,
        timeout_s: float = 30.0,
        max_retries: int = 3,
        min_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
    ) -> None:
        _require_non_empty(token, name="token")
        if timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if min_backoff_s <= 0:
            raise ValueError("min_backoff_s must be > 0")
        if max_backoff_s <= 0:
            raise ValueError("max_backoff_s must be > 0")
        self._token = token
        self._timeout_s = float(timeout_s)
        self._max_retries = int(max_retries)
        self._min_backoff_s = float(min_backoff_s)
        self._max_backoff_s = float(max_backoff_s)

    def _post(self, *, method: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        attempt = 0
        backoff_s = self._min_backoff_s
        while True:
            try:
                parsed = _slack_api_post_once(
                    token=self._token,
                    method=method,
                    payload=payload,
                    timeout_s=self._timeout_s,
                )
                return parsed
            except MessagingError as e:
                status_code = getattr(e, "status_code", None)
                retry_after = getattr(e, "retry_after_seconds", None)

                # Slack can signal rate limits either via HTTP 429 or via error text.
                is_ratelimited = status_code == 429 or "ratelimited" in str(e).lower()
                is_transient = status_code in {500, 502, 503, 504}

                if attempt >= self._max_retries or not (is_ratelimited or is_transient):
                    raise

                sleep_s = (
                    float(retry_after)
                    if isinstance(retry_after, (int, float)) and retry_after is not None
                    else backoff_s
                )
                _sleep_s(min(self._max_backoff_s, max(0.0, sleep_s)))
                attempt += 1
                backoff_s = min(self._max_backoff_s, backoff_s * 2.0)

    def _paginate(
        self,
        *,
        method: str,
        payload: dict[str, Any],
        items_key: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        while len(out) < limit:
            remaining = limit - len(out)
            page_limit = min(200, remaining)
            payload2 = dict(payload)
            payload2["limit"] = int(page_limit)
            if cursor:
                payload2["cursor"] = cursor
            parsed = self._post(method=method, payload=payload2)

            items = parsed.get(items_key)
            if not isinstance(items, list):
                raise MessagingError(f"Slack {method} missing {items_key} list")
            for item in items:
                if isinstance(item, dict):
                    out.append(item)
                    if len(out) >= limit:
                        break

            meta = parsed.get("response_metadata")
            next_cursor: str | None = None
            if isinstance(meta, dict):
                nc = meta.get("next_cursor")
                if isinstance(nc, str) and nc.strip():
                    next_cursor = nc.strip()
            if not next_cursor:
                break
            cursor = next_cursor
        return out

    def list_channels(self) -> Sequence[Channel]:
        raw = self._paginate(
            method="conversations.list",
            payload={"exclude_archived": True, "types": "public_channel,private_channel"},
            items_key="channels",
            limit=1000,
        )
        out: list[Channel] = []
        for c in raw:
            cid = c.get("id")
            if not isinstance(cid, str) or not cid.strip():
                continue
            name = c.get("name")
            out.append(
                Channel(
                    id=cid,
                    name=name if isinstance(name, str) and name.strip() else None,
                )
            )
        return out

    def list_channel_messages(
        self,
        channel_id: str,
        *,
        oldest: str | None = None,
        latest: str | None = None,
        limit: int = 200,
    ) -> Sequence[Message]:
        _require_non_empty(channel_id, name="channel_id")
        payload: dict[str, Any] = {"channel": channel_id}
        if oldest is not None:
            payload["oldest"] = oldest
        if latest is not None:
            payload["latest"] = latest
        raw = self._paginate(
            method="conversations.history",
            payload=payload,
            items_key="messages",
            limit=int(limit),
        )
        out: list[Message] = []
        for item in raw:
            out.append(_to_message(channel_id=channel_id, item=item))
        return out

    def get_thread(
        self,
        channel_id: str,
        *,
        thread_id: str,
        limit: int = 200,
    ) -> Thread:
        _require_non_empty(channel_id, name="channel_id")
        _require_non_empty(thread_id, name="thread_id")
        raw = self._paginate(
            method="conversations.replies",
            payload={"channel": channel_id, "ts": thread_id},
            items_key="messages",
            limit=int(limit),
        )
        if not raw:
            raise MessagingError("Slack conversations.replies returned no messages")
        msgs = [_to_message(channel_id=channel_id, item=item) for item in raw]
        root = next((m for m in msgs if m.id == thread_id), msgs[0])
        replies = tuple(m for m in msgs if m.id != root.id)
        return Thread(channel_id=channel_id, thread_id=thread_id, root=root, replies=replies)


@dataclass(frozen=True, slots=True)
class SlackConnectorConfig:
    """Configuration for Slack ingestion.

    Args:
        channel_id: Channel to ingest (e.g. C123...).
        token: Bot/user token (do not hardcode; pass from env/CLI).
        oldest: Optional Slack timestamp string to bound history.
        latest: Optional Slack timestamp string to bound history.
        history_limit: Page size for history/replies.
        max_threads: Cap top-level messages processed (safety / cost control).
        max_answers: Max answers per thread for Q&A extraction.
        include_bot_answers: Whether bot messages are eligible as answers.
    """

    channel_id: str
    token: str
    oldest: str | None = None
    latest: str | None = None
    history_limit: int = 200
    max_threads: int = 200
    max_answers: int = 3
    include_bot_answers: bool = False
    timeout_s: float = 30.0
    max_retries: int = 3

    def __post_init__(self) -> None:
        _require_non_empty(self.channel_id, name="channel_id")
        _require_non_empty(self.token, name="token")
        if self.history_limit <= 0:
            raise ValueError("history_limit must be > 0")
        if self.max_threads <= 0:
            raise ValueError("max_threads must be > 0")
        if self.max_answers <= 0:
            raise ValueError("max_answers must be > 0")
        if self.timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")
        if self.max_retries < 0:
            raise ValueError("max_retries must be >= 0")


class SlackConnector(BaseConnector):
    """Slack connector emitting per-thread Q&A documents."""

    def __init__(self, *, tenant_id: str, config: SlackConnectorConfig) -> None:
        super().__init__(tenant_id=tenant_id, source_type="messaging")
        self._config = config
        self._client = SlackMessagingClient(
            token=config.token,
            timeout_s=config.timeout_s,
            max_retries=config.max_retries,
        )

    @property
    def config(self) -> SlackConnectorConfig:
        return self._config

    def list_sources(self) -> Sequence[str]:
        # Treat the configured channel as the single source for this connector instance.
        return [self._config.channel_id]

    def _list_channel_history(self) -> Sequence[Message]:
        try:
            return self._client.list_channel_messages(
                self._config.channel_id,
                oldest=self._config.oldest,
                latest=self._config.latest,
                limit=int(self._config.history_limit),
            )
        except MessagingError as e:
            raise ConnectorError("Slack conversations.history failed") from e

    def _get_thread(self, *, thread_id: str) -> Thread:
        try:
            return self._client.get_thread(
                self._config.channel_id,
                thread_id=thread_id,
                limit=int(self._config.history_limit),
            )
        except MessagingError as e:
            raise ConnectorError("Slack conversations.replies failed") from e

    def fetch(self, source_id: str) -> Iterable[Document]:
        if source_id != self._config.channel_id:
            raise ConnectorError("unknown source_id for SlackConnector")

        history = self._list_channel_history()

        # Choose thread roots: message.thread_id if present else message.id.
        seen_threads: set[str] = set()
        thread_roots: list[str] = []
        for m in history:
            tid = m.thread_id or m.id
            if tid in seen_threads:
                continue
            seen_threads.add(tid)
            thread_roots.append(tid)
            if len(thread_roots) >= self._config.max_threads:
                break

        for thread_id in thread_roots:
            thread = self._get_thread(thread_id=thread_id)
            qa = extract_qa_pairs(
                thread,
                max_answers=int(self._config.max_answers),
                include_bots=bool(self._config.include_bot_answers),
            )

            # Build a compact, retrieval-friendly doc body.
            q_user = qa.question.user_id or "unknown"
            body_lines: list[str] = [
                f"Slack thread {qa.thread_id} in channel {qa.channel_id}",
                "",
                f"Q ({q_user} @ {qa.question.timestamp}):",
                qa.question.text.strip(),
                "",
            ]
            for i, ans in enumerate(qa.answers, start=1):
                a_user = ans.user_id or "unknown"
                body_lines.append(f"A{i} ({a_user} @ {ans.timestamp}):")
                body_lines.append(ans.text.strip())
                body_lines.append("")
            content = "\n".join(body_lines).strip() + "\n"

            message_ids = [qa.question.id] + [a.id for a in qa.answers]
            user_ids = [qa.question.user_id] + [a.user_id for a in qa.answers]
            metadata: dict[str, object] = {
                "channel": qa.channel_id,
                "thread_id": qa.thread_id,
                "timestamp": qa.question.timestamp,
                "user": qa.question.user_id or "",
                "message_ids": [m for m in message_ids if isinstance(m, str)],
                "user_ids": [u for u in user_ids if isinstance(u, str) and u],
            }

            yield self._make_document(
                source=f"slack:{qa.channel_id}",
                logical_locator=f"thread:{qa.thread_id}",
                content=content,
                metadata=metadata,
            )


def build_slack_connector(
    *,
    tenant_id: str,
    channel_id: str,
    token: str,
    oldest: str | None = None,
    latest: str | None = None,
    history_limit: int = 200,
    max_threads: int = 200,
    max_answers: int = 3,
    include_bot_answers: bool = False,
    timeout_s: float = 30.0,
    max_retries: int = 3,
) -> SlackConnector:
    return SlackConnector(
        tenant_id=tenant_id,
        config=SlackConnectorConfig(
            channel_id=channel_id,
            token=token,
            oldest=oldest,
            latest=latest,
            history_limit=history_limit,
            max_threads=max_threads,
            max_answers=max_answers,
            include_bot_answers=include_bot_answers,
            timeout_s=timeout_s,
            max_retries=max_retries,
        ),
    )
