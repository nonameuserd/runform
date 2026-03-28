from __future__ import annotations

import json
import time
import uuid
from dataclasses import replace
from pathlib import Path

from akc.path_security import safe_resolve_path, safe_resolve_scoped_path

from .models import AssistantMode, AssistantScope, AssistantSession


class AssistantSessionStoreError(Exception):
    """Raised when assistant sessions cannot be persisted or loaded."""


class AssistantSessionStore:
    """Local filesystem store for assistant sessions under ``.akc/assistant/sessions``."""

    def __init__(self, *, root: Path, retention_days: int = 14) -> None:
        self._root = safe_resolve_path(root)
        self._retention_days = max(1, int(retention_days))
        self._sessions_dir = safe_resolve_scoped_path(self._root, ".akc", "assistant", "sessions")
        self._sessions_dir.mkdir(parents=True, exist_ok=True)

    @property
    def sessions_dir(self) -> Path:
        return self._sessions_dir

    def create_session(self, *, mode: AssistantMode, scope: AssistantScope) -> AssistantSession:
        now_ms = int(time.time() * 1000)
        sid = str(uuid.uuid4())
        return AssistantSession(
            schema_version=2,
            session_id=sid,
            created_at_ms=now_ms,
            updated_at_ms=now_ms,
            mode=mode,
            scope=scope.normalized(),
            turns=(),
            pending_actions={},
            last_suggested_command=None,
            memory_index={},
            compacted_turn_refs=(),
            pin_set=(),
            last_memory_trace_ref=None,
        )

    def load(self, *, session_id: str) -> AssistantSession:
        sid = str(session_id or "").strip()
        if not sid:
            raise AssistantSessionStoreError("session_id is required")
        p = self._session_path(sid)
        if not p.is_file():
            raise AssistantSessionStoreError(f"assistant session not found: {sid}")
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as e:
            raise AssistantSessionStoreError(f"failed to read assistant session {sid}: {e}") from e
        try:
            return AssistantSession.from_json_obj(raw)
        except (TypeError, ValueError) as e:
            raise AssistantSessionStoreError(f"invalid assistant session payload for {sid}: {e}") from e

    def save(self, *, session: AssistantSession) -> None:
        session.validate()
        now_ms = int(time.time() * 1000)
        normalized = replace(session, schema_version=2, updated_at_ms=max(int(session.updated_at_ms), now_ms))
        p = self._session_path(normalized.session_id)
        tmp = p.with_suffix(".json.tmp")
        payload = normalized.to_json_obj()
        try:
            tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            tmp.replace(p)
        except OSError as e:
            raise AssistantSessionStoreError(f"failed to persist assistant session {normalized.session_id}: {e}") from e

    def list_session_ids(self) -> tuple[str, ...]:
        out: list[str] = []
        for fp in sorted(self._sessions_dir.glob("*.json")):
            sid = fp.stem.strip()
            if sid:
                out.append(sid)
        return tuple(out)

    def prune_expired(self, *, now_ms: int | None = None) -> int:
        ms = int(now_ms if now_ms is not None else time.time() * 1000)
        cutoff = ms - (self._retention_days * 24 * 60 * 60 * 1000)
        removed = 0
        for fp in self._sessions_dir.glob("*.json"):
            try:
                stat = fp.stat()
            except OSError:
                continue
            mtime_ms = int(stat.st_mtime * 1000)
            if mtime_ms >= cutoff:
                continue
            try:
                fp.unlink()
                removed += 1
            except OSError:
                continue
        return removed

    def _session_path(self, session_id: str) -> Path:
        sid = str(session_id or "").strip()
        if not sid:
            raise AssistantSessionStoreError("session_id is required")
        if "/" in sid or "\\" in sid or ".." in sid:
            raise AssistantSessionStoreError("invalid session_id")
        return safe_resolve_scoped_path(self._sessions_dir, f"{sid}.json")
