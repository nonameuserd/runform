"""Discord messaging connector (Phase 1).

Implements pull/backfill ingestion via Discord REST API using stdlib HTTP, mirroring
the Slack connector approach (no extra dependency).
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
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


def _sleep_s(seconds: float) -> None:
    # Kept as a function so tests can monkeypatch it.
    time.sleep(seconds)


def _parse_retry_after_seconds(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        s = value.strip()
        if not s:
            return None
        return max(0.0, float(s))
    except Exception:
        return None


def _parse_discord_timestamp_to_ms(ts: str | None) -> int | None:
    if not isinstance(ts, str) or not ts.strip():
        return None
    s = ts.strip()
    # Discord uses ISO8601 with 'Z' suffix; Python fromisoformat expects +00:00.
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return int(dt.timestamp() * 1000.0)


def _discord_api_get_once(
    *,
    token: str,
    path: str,
    query: dict[str, Any] | None = None,
    timeout_s: float = 30.0,
) -> Any:
    _require_non_empty(token, name="token")
    _require_non_empty(path, name="path")
    if timeout_s <= 0:
        raise ValueError("timeout_s must be > 0")

    base = "https://discord.com/api/v10"
    url = base + (path if path.startswith("/") else f"/{path}")
    if query:
        url = url + "?" + urlencode({k: v for k, v in query.items() if v is not None})
    req = Request(
        url,
        method="GET",
        headers={
            "Authorization": f"Bot {token}",
            "Accept": "application/json",
            "User-Agent": "akc-ingest/discord (stdlib)",
        },
    )
    try:
        with urlopen(req, timeout=timeout_s) as resp:  # noqa: S310 (controlled URL)
            raw = resp.read()
            status = getattr(resp, "status", 200)
    except HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        retry_after = _parse_retry_after_seconds(e.headers.get("Retry-After"))
        # Discord also returns JSON containing retry_after on 429.
        if retry_after is None and body:
            try:
                parsed_body = json.loads(body)
                ra = parsed_body.get("retry_after")
                if isinstance(ra, (int, float)):
                    retry_after = max(0.0, float(ra))
            except Exception:
                pass
        msg = f"Discord API request failed ({e.code}): {body}".strip()
        err = MessagingError(msg)
        err.status_code = int(e.code)  # type: ignore[attr-defined]
        err.retry_after_seconds = retry_after  # type: ignore[attr-defined]
        raise err from e
    except URLError as e:
        raise MessagingError("Discord API request failed to connect") from e

    if status == 204:
        return None
    try:
        decoded = raw.decode("utf-8")
        parsed = json.loads(decoded) if decoded.strip() else None
    except Exception as e:
        raise MessagingError("Discord API response was not valid JSON") from e

    # Discord uses error objects for non-2xx; urlopen raises those as HTTPError.
    # Still, enforce basic type sanity to reduce downstream surprises.
    if parsed is None:
        return None
    return parsed


def _to_message(*, channel_id: str, item: dict[str, Any], thread_id: str | None = None) -> Message:
    mid = item.get("id")
    if not isinstance(mid, str) or not mid.strip():
        raise MessagingError("Discord message missing id")
    ts = item.get("timestamp")
    if not isinstance(ts, str) or not ts.strip():
        raise MessagingError("Discord message missing timestamp")

    author = item.get("author")
    user_id: str | None = None
    is_bot = False
    if isinstance(author, dict):
        aid = author.get("id")
        if isinstance(aid, str) and aid.strip():
            user_id = aid
        if author.get("bot") is True:
            is_bot = True

    content = item.get("content")
    text = content if isinstance(content, str) else ""

    return Message(
        id=mid,
        channel_id=channel_id,
        user_id=user_id,
        text=text,
        timestamp=ts,
        thread_id=thread_id,
        is_bot=is_bot,
    )


class DiscordMessagingClient(MessagingClient):
    """Discord implementation of the normalized MessagingClient abstraction.

    This client is responsible for:
    - REST pagination using before/after
    - Basic retry handling for rate limits (HTTP 429) and transient 5xx errors
    """

    def __init__(
        self,
        *,
        token: str,
        guild_id: str | None = None,
        timeout_s: float = 30.0,
        max_retries: int = 3,
        min_backoff_s: float = 1.0,
        max_backoff_s: float = 30.0,
    ) -> None:
        _require_non_empty(token, name="token")
        if guild_id is not None:
            _require_non_empty(guild_id, name="guild_id")
        if timeout_s <= 0:
            raise ValueError("timeout_s must be > 0")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if min_backoff_s <= 0:
            raise ValueError("min_backoff_s must be > 0")
        if max_backoff_s <= 0:
            raise ValueError("max_backoff_s must be > 0")
        self._token = token
        self._guild_id = guild_id
        self._timeout_s = float(timeout_s)
        self._max_retries = int(max_retries)
        self._min_backoff_s = float(min_backoff_s)
        self._max_backoff_s = float(max_backoff_s)
        self._channel_type_cache: dict[str, int] = {}

    def _get(self, *, path: str, query: dict[str, Any] | None = None) -> Any:
        attempt = 0
        backoff_s = self._min_backoff_s
        while True:
            try:
                return _discord_api_get_once(
                    token=self._token,
                    path=path,
                    query=query,
                    timeout_s=self._timeout_s,
                )
            except MessagingError as e:
                status_code = getattr(e, "status_code", None)
                retry_after = getattr(e, "retry_after_seconds", None)
                is_ratelimited = status_code == 429
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

    def list_channels(self) -> list[Channel]:
        if self._guild_id is None:
            raise MessagingError("Discord list_channels requires guild_id")
        raw = self._get(path=f"/guilds/{self._guild_id}/channels")
        if not isinstance(raw, list):
            raise MessagingError("Discord guild channels response must be a list")
        out: list[Channel] = []
        for c in raw:
            if not isinstance(c, dict):
                continue
            cid = c.get("id")
            if not isinstance(cid, str) or not cid.strip():
                continue
            name = c.get("name")
            out.append(Channel(id=cid, name=name if isinstance(name, str) and name.strip() else None))
        return out

    def _list_messages_desc(
        self,
        *,
        channel_id: str,
        before: str | None,
        after: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        if limit <= 0:
            return []
        out: list[dict[str, Any]] = []
        cursor_before = before

        # Discord caps per-request limit at 100.
        while len(out) < limit:
            remaining = limit - len(out)
            page_limit = min(100, remaining)
            query: dict[str, Any] = {"limit": int(page_limit)}
            if cursor_before is not None:
                query["before"] = cursor_before
            if after is not None:
                query["after"] = after
            raw = self._get(path=f"/channels/{channel_id}/messages", query=query)
            if not isinstance(raw, list):
                raise MessagingError("Discord channel messages response must be a list")
            page: list[dict[str, Any]] = [m for m in raw if isinstance(m, dict)]
            if not page:
                break
            out.extend(page)

            # Pagination uses "before": pass the last (oldest) message id from this page.
            last_id = page[-1].get("id")
            if not isinstance(last_id, str) or not last_id.strip():
                break
            if cursor_before == last_id:
                break
            cursor_before = last_id
        return out[:limit]

    def list_channel_messages(
        self,
        channel_id: str,
        *,
        oldest: str | None = None,
        latest: str | None = None,
        limit: int = 200,
    ) -> list[Message]:
        _require_non_empty(channel_id, name="channel_id")
        if limit <= 0:
            return []

        before = latest
        after = oldest
        raw = self._list_messages_desc(channel_id=channel_id, before=before, after=after, limit=int(limit))

        out: list[Message] = []
        for item in raw:
            # Thread mapping: if the message starts a thread, Discord includes `thread` with thread channel id.
            tid: str | None = None
            thread_obj = item.get("thread")
            if isinstance(thread_obj, dict):
                t_id = thread_obj.get("id")
                if isinstance(t_id, str) and t_id.strip():
                    tid = t_id
            out.append(_to_message(channel_id=channel_id, item=item, thread_id=tid))
        return out

    def _get_channel_type(self, channel_id: str) -> int | None:
        if channel_id in self._channel_type_cache:
            return self._channel_type_cache[channel_id]
        raw = self._get(path=f"/channels/{channel_id}")
        if not isinstance(raw, dict):
            return None
        t = raw.get("type")
        if isinstance(t, int):
            self._channel_type_cache[channel_id] = t
            return t
        return None

    def _is_thread_channel(self, channel_id: str) -> bool:
        # Discord thread channel types: 10/11/12. (News thread, public thread, private thread)
        t = self._get_channel_type(channel_id)
        return t in {10, 11, 12}

    def get_thread(
        self,
        channel_id: str,
        *,
        thread_id: str,
        limit: int = 200,
    ) -> Thread:
        _require_non_empty(channel_id, name="channel_id")
        _require_non_empty(thread_id, name="thread_id")

        # Best-effort:
        # - If `thread_id` is a thread channel id: fetch messages from that channel and pick the oldest as root.
        # - Else treat it as a single-message "thread" with no replies.
        if self._is_thread_channel(thread_id):
            raw = self._list_messages_desc(channel_id=thread_id, before=None, after=None, limit=int(limit))
            if not raw:
                raise MessagingError("Discord thread returned no messages")
            # Returned in desc order; root should be the oldest message in the fetched set.
            msgs = [_to_message(channel_id=thread_id, item=item, thread_id=thread_id) for item in raw]
            root = msgs[-1]
            replies = tuple(m for m in reversed(msgs[:-1]))
            return Thread(channel_id=channel_id, thread_id=thread_id, root=root, replies=replies)

        raw_msg = self._get(path=f"/channels/{channel_id}/messages/{thread_id}")
        if not isinstance(raw_msg, dict):
            raise MessagingError("Discord get message returned invalid payload")
        root = _to_message(channel_id=channel_id, item=raw_msg, thread_id=thread_id)
        return Thread(channel_id=channel_id, thread_id=thread_id, root=root, replies=())

    def list_messages_in_channel(
        self,
        *,
        channel_id: str,
        limit: int,
        before: str | None = None,
        after: str | None = None,
    ) -> list[Message]:
        """Fetch messages from any Discord channel id (including thread channels)."""
        _require_non_empty(channel_id, name="channel_id")
        raw = self._list_messages_desc(channel_id=channel_id, before=before, after=after, limit=int(limit))
        out: list[Message] = []
        for item in raw:
            out.append(_to_message(channel_id=channel_id, item=item, thread_id=None))
        return out


@dataclass(frozen=True, slots=True)
class DiscordConnectorConfig:
    """Configuration for Discord ingestion.

    Args:
        channel_id: Channel to ingest.
        token: Bot token (do not hardcode; pass from env/CLI).
        guild_id: Optional guild id; used only for list_channels.
        oldest: Optional message id boundary (treated as Discord `after`).
        latest: Optional message id boundary (treated as Discord `before`).
        history_limit: Max messages fetched per history/replies fetch.
        max_threads: Cap top-level items processed (safety / cost control).
        max_answers: Max answers per thread for Q&A extraction.
        include_bot_answers: Whether bot messages are eligible as answers.
    """

    channel_id: str
    token: str
    guild_id: str | None = None
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
        if self.guild_id is not None:
            _require_non_empty(self.guild_id, name="guild_id")
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


class DiscordConnector(BaseConnector):
    """Discord connector emitting per-thread Q&A documents.

    Thread mapping:
    - If a channel message has an attached Discord thread, treat that thread channel as the "thread".
      Root is the parent message; replies are pulled from the thread channel.
    - Otherwise treat each message as its own thread root with no replies.
    """

    def __init__(self, *, tenant_id: str, config: DiscordConnectorConfig) -> None:
        super().__init__(tenant_id=tenant_id, source_type="messaging")
        self._config = config
        self._client = DiscordMessagingClient(
            token=config.token,
            guild_id=config.guild_id,
            timeout_s=config.timeout_s,
            max_retries=config.max_retries,
        )

    @property
    def config(self) -> DiscordConnectorConfig:
        return self._config

    def list_sources(self) -> list[str]:
        return [self._config.channel_id]

    def _list_channel_history(self) -> list[Message]:
        try:
            return self._client.list_channel_messages(
                self._config.channel_id,
                oldest=self._config.oldest,
                latest=self._config.latest,
                limit=int(self._config.history_limit),
            )
        except MessagingError as e:
            raise ConnectorError("Discord list channel messages failed") from e

    def _get_thread_from_parent_message(self, *, parent: Message, thread_channel_id: str) -> Thread:
        try:
            # Fetch replies inside the thread channel. Keep the parent message as the root.
            raw = self._client.list_messages_in_channel(
                channel_id=thread_channel_id,
                limit=int(self._config.history_limit),
                before=None,
                after=None,
            )
        except MessagingError as e:
            raise ConnectorError("Discord list thread messages failed") from e

        # Replies should be chronological for extraction; Discord returns newest first.
        replies = tuple(reversed(raw))
        return Thread(channel_id=self._config.channel_id, thread_id=thread_channel_id, root=parent, replies=replies)

    def fetch(self, source_id: str) -> Iterable[Document]:
        if source_id != self._config.channel_id:
            raise ConnectorError("unknown source_id for DiscordConnector")

        history = self._list_channel_history()

        seen_threads: set[str] = set()
        roots: list[Message] = []
        for m in history:
            tid = m.thread_id or m.id
            if tid in seen_threads:
                continue
            seen_threads.add(tid)
            roots.append(m)
            if len(roots) >= self._config.max_threads:
                break

        for root in roots:
            if root.thread_id:
                thread = self._get_thread_from_parent_message(parent=root, thread_channel_id=root.thread_id)
            else:
                thread = Thread(channel_id=self._config.channel_id, thread_id=root.id, root=root, replies=())

            qa = extract_qa_pairs(
                thread,
                max_answers=int(self._config.max_answers),
                include_bots=bool(self._config.include_bot_answers),
            )

            q_user = qa.question.user_id or "unknown"
            body_lines: list[str] = [
                f"Discord thread {qa.thread_id} in channel {qa.channel_id}",
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
            indexed_at_ms = _parse_discord_timestamp_to_ms(qa.question.timestamp) or int(time.time() * 1000)
            metadata: dict[str, object] = {
                "channel": qa.channel_id,
                "thread_id": qa.thread_id,
                "timestamp": qa.question.timestamp,
                "user": qa.question.user_id or "",
                "message_ids": [m for m in message_ids if isinstance(m, str)],
                "user_ids": [u for u in user_ids if isinstance(u, str) and u],
                "ingest_source_kind": "messaging",
                "indexed_at_ms": indexed_at_ms,
            }
            yield self._make_document(
                source=f"discord:{qa.channel_id}",
                logical_locator=f"thread:{qa.thread_id}",
                content=content,
                metadata=metadata,
            )


def build_discord_connector(
    *,
    tenant_id: str,
    channel_id: str,
    token: str,
    guild_id: str | None = None,
    oldest: str | None = None,
    latest: str | None = None,
    history_limit: int = 200,
    max_threads: int = 200,
    max_answers: int = 3,
    include_bot_answers: bool = False,
    timeout_s: float = 30.0,
    max_retries: int = 3,
) -> DiscordConnector:
    return DiscordConnector(
        tenant_id=tenant_id,
        config=DiscordConnectorConfig(
            channel_id=channel_id,
            token=token,
            guild_id=guild_id,
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
