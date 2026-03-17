"""Messaging connector abstraction.

This provides a small normalized surface area so future platforms (Discord, Teams,
Matrix, etc.) can plug into ingestion without changing the pipeline.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass


class MessagingError(Exception):
    """Raised when a messaging connector cannot list or fetch messages."""


@dataclass(frozen=True, slots=True)
class Channel:
    id: str
    name: str | None = None


@dataclass(frozen=True, slots=True)
class Message:
    id: str
    channel_id: str
    user_id: str | None
    text: str
    timestamp: str
    thread_id: str | None = None
    is_bot: bool = False


@dataclass(frozen=True, slots=True)
class Thread:
    channel_id: str
    thread_id: str
    root: Message
    replies: tuple[Message, ...]


@dataclass(frozen=True, slots=True)
class QAPair:
    channel_id: str
    thread_id: str
    question: Message
    answers: tuple[Message, ...]


class MessagingClient(ABC):
    """A normalized client for messaging sources."""

    @abstractmethod
    def list_channels(self) -> Sequence[Channel]: ...

    @abstractmethod
    def list_channel_messages(
        self,
        channel_id: str,
        *,
        oldest: str | None = None,
        latest: str | None = None,
        limit: int = 200,
    ) -> Sequence[Message]: ...

    @abstractmethod
    def get_thread(
        self,
        channel_id: str,
        *,
        thread_id: str,
        limit: int = 200,
    ) -> Thread: ...


def extract_qa_pairs(
    thread: Thread,
    *,
    max_answers: int = 3,
    include_bots: bool = False,
) -> QAPair:
    """Phase 1 heuristic Q&A extraction.

    - Question: thread.root
    - Answers: first non-bot replies (up to max_answers)
    """

    if max_answers <= 0:
        raise ValueError("max_answers must be > 0")
    answers: list[Message] = []
    for m in thread.replies:
        if m.id == thread.root.id:
            continue
        if (not include_bots) and m.is_bot:
            continue
        if not m.text.strip():
            continue
        answers.append(m)
        if len(answers) >= max_answers:
            break
    return QAPair(
        channel_id=thread.channel_id,
        thread_id=thread.thread_id,
        question=thread.root,
        answers=tuple(answers),
    )
