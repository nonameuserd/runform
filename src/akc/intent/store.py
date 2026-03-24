from __future__ import annotations

import json
import os
import sqlite3
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from akc.intent.models import IntentModelError, IntentSpecV1
from akc.memory.models import MemoryModelError, normalize_repo_id, now_ms, require_non_empty


class IntentStoreError(Exception):
    """Raised when intent persistence operations fail."""


class IntentStore(ABC):
    @abstractmethod
    def create_intent(self, *, intent: IntentSpecV1) -> IntentSpecV1: ...

    @abstractmethod
    def load_intent(self, *, tenant_id: str, repo_id: str, intent_id: str) -> IntentSpecV1 | None: ...

    @abstractmethod
    def save_intent(self, *, tenant_id: str, repo_id: str, intent: IntentSpecV1) -> None: ...

    @abstractmethod
    def set_active_intent(self, *, tenant_id: str, repo_id: str, intent_id: str) -> None: ...

    @abstractmethod
    def get_active_intent_id(self, *, tenant_id: str, repo_id: str) -> str | None: ...


def _safe_id_for_path(value: str) -> str:
    # Match Phase 1 pattern: human-readable, avoid traversal.
    return value.replace(os.sep, "_").replace("..", "_")


def default_intent_dir(*, base_dir: Path | None = None) -> Path:
    base = base_dir or Path.cwd()
    return base / ".akc" / "intent"


def _intent_path(*, base_dir: Path, tenant_id: str, repo_id: str, intent_id: str) -> Path:
    safe_tenant = _safe_id_for_path(tenant_id)
    safe_repo = _safe_id_for_path(normalize_repo_id(repo_id))
    require_non_empty(intent_id, name="intent_id")
    return base_dir / safe_tenant / safe_repo / f"{intent_id}.json"


class JsonFileIntentStore(IntentStore):
    """Tenant+repo-scoped intent persistence (dependency-light, filesystem-based)."""

    def __init__(self, *, base_dir: Path | None = None) -> None:
        self._base_dir = default_intent_dir(base_dir=base_dir)

    @property
    def base_dir(self) -> Path:
        return self._base_dir

    def create_intent(self, *, intent: IntentSpecV1) -> IntentSpecV1:
        i = intent.normalized()
        self.save_intent(tenant_id=i.tenant_id, repo_id=i.repo_id, intent=i)
        self.set_active_intent(tenant_id=i.tenant_id, repo_id=i.repo_id, intent_id=i.intent_id)
        return i

    def _active_pointer_path(self, *, tenant_id: str, repo_id: str) -> Path:
        safe_tenant = _safe_id_for_path(tenant_id)
        safe_repo = _safe_id_for_path(normalize_repo_id(repo_id))
        return self._base_dir / safe_tenant / safe_repo / "active.json"

    def set_active_intent(self, *, tenant_id: str, repo_id: str, intent_id: str) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(intent_id, name="intent_id")
        p = self._active_pointer_path(tenant_id=tenant_id, repo_id=repo)
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(
                json.dumps({"intent_id": intent_id}, indent=2, sort_keys=True),
                encoding="utf-8",
            )
            tmp.replace(p)
        except OSError as e:
            raise IntentStoreError(f"failed to write active intent pointer: {p}") from e

    def get_active_intent_id(self, *, tenant_id: str, repo_id: str) -> str | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        p = self._active_pointer_path(tenant_id=tenant_id, repo_id=repo)
        try:
            raw = p.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as e:
            raise IntentStoreError(f"failed to read active intent pointer: {p}") from e
        try:
            data = json.loads(raw)
        except Exception as e:  # pragma: no cover
            raise IntentStoreError(f"active intent pointer is not valid JSON: {p}") from e
        if not isinstance(data, dict):
            raise IntentStoreError(f"active intent pointer must be a JSON object: {p}")
        intent_id = data.get("intent_id")
        if not isinstance(intent_id, str) or not intent_id.strip():
            return None
        return intent_id

    def load_intent(self, *, tenant_id: str, repo_id: str, intent_id: str) -> IntentSpecV1 | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(intent_id, name="intent_id")
        p = _intent_path(base_dir=self._base_dir, tenant_id=tenant_id, repo_id=repo, intent_id=intent_id)
        try:
            raw = p.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as e:
            raise IntentStoreError(f"failed to read intent: {p}") from e
        try:
            data: Any = json.loads(raw)
        except Exception as e:  # pragma: no cover
            raise IntentStoreError(f"intent is not valid JSON: {p}") from e
        if not isinstance(data, dict):
            raise IntentStoreError(f"intent must be a JSON object: {p}")
        try:
            intent = IntentSpecV1.from_json_obj(data)
        except (IntentModelError, MemoryModelError) as e:  # pragma: no cover
            raise IntentStoreError(f"stored intent was invalid: {p}") from e
        if intent.tenant_id != tenant_id or normalize_repo_id(intent.repo_id) != repo:
            raise IntentStoreError("tenant_id/repo_id mismatch when loading intent")
        return intent

    def save_intent(self, *, tenant_id: str, repo_id: str, intent: IntentSpecV1) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        i = intent.normalized()
        if i.tenant_id != tenant_id:
            raise IntentStoreError("tenant_id mismatch between argument and intent.tenant_id")
        if normalize_repo_id(i.repo_id) != repo:
            raise IntentStoreError("repo_id mismatch between argument and intent.repo_id")

        p = _intent_path(base_dir=self._base_dir, tenant_id=tenant_id, repo_id=repo, intent_id=i.intent_id)
        obj = i.to_json_obj()
        try:
            p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(p.suffix + ".tmp")
            tmp.write_text(
                json.dumps(obj, indent=2, sort_keys=True, ensure_ascii=False),
                encoding="utf-8",
            )
            tmp.replace(p)
        except OSError as e:
            raise IntentStoreError(f"failed to write intent: {p}") from e


class SQLiteIntentStore(IntentStore):
    """SQLite-backed intent persistence (queryable, durable local storage)."""

    def __init__(self, *, path: str) -> None:
        require_non_empty(path, name="path")
        self._path = path
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_schema(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS intents (
                  tenant_id     TEXT NOT NULL,
                  repo_id       TEXT NOT NULL,
                  intent_id    TEXT NOT NULL,
                  spec_version  INTEGER NOT NULL,
                  status        TEXT NOT NULL,
                  created_at_ms INTEGER NOT NULL,
                  updated_at_ms INTEGER NOT NULL,
                  spec_json     TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, repo_id, intent_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS active_intents (
                  tenant_id     TEXT NOT NULL,
                  repo_id       TEXT NOT NULL,
                  intent_id    TEXT NOT NULL,
                  updated_at_ms INTEGER NOT NULL,
                  PRIMARY KEY (tenant_id, repo_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS intents_by_repo_updated
                ON intents(tenant_id, repo_id, updated_at_ms DESC)
                """
            )

    def create_intent(self, *, intent: IntentSpecV1) -> IntentSpecV1:
        i = intent.normalized()
        self.save_intent(tenant_id=i.tenant_id, repo_id=i.repo_id, intent=i)
        self.set_active_intent(tenant_id=i.tenant_id, repo_id=i.repo_id, intent_id=i.intent_id)
        return i

    def set_active_intent(self, *, tenant_id: str, repo_id: str, intent_id: str) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(intent_id, name="intent_id")
        t = now_ms()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO active_intents (tenant_id, repo_id, intent_id, updated_at_ms)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id) DO UPDATE SET
                  intent_id=excluded.intent_id,
                  updated_at_ms=excluded.updated_at_ms
                """,
                (tenant_id, repo, intent_id, t),
            )

    def get_active_intent_id(self, *, tenant_id: str, repo_id: str) -> str | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT intent_id FROM active_intents WHERE tenant_id=? AND repo_id=?",
                (tenant_id, repo),
            )
            row = cur.fetchone()
        if row is None:
            return None
        intent_id = row[0]
        return str(intent_id) if isinstance(intent_id, str) and intent_id.strip() else None

    def load_intent(self, *, tenant_id: str, repo_id: str, intent_id: str) -> IntentSpecV1 | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(intent_id, name="intent_id")
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT spec_json
                FROM intents
                WHERE tenant_id=? AND repo_id=? AND intent_id=?
                """,
                (tenant_id, repo, intent_id),
            )
            row = cur.fetchone()
        if row is None:
            return None
        spec_json = row[0]
        if not isinstance(spec_json, str) or not spec_json.strip():
            raise IntentStoreError("stored intent spec_json is empty/corrupt")
        try:
            loaded = json.loads(spec_json)
        except Exception as e:  # pragma: no cover
            raise IntentStoreError("stored intent spec_json is not valid JSON") from e
        if not isinstance(loaded, dict):
            raise IntentStoreError("stored intent spec_json must be a JSON object")
        try:
            intent = IntentSpecV1.from_json_obj(loaded)
        except (IntentModelError, MemoryModelError) as e:  # pragma: no cover
            raise IntentStoreError("stored IntentSpecV1 was invalid") from e
        if intent.tenant_id != tenant_id or normalize_repo_id(intent.repo_id) != repo:
            raise IntentStoreError("tenant_id/repo_id mismatch when loading intent")
        return intent

    def save_intent(self, *, tenant_id: str, repo_id: str, intent: IntentSpecV1) -> None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        i = intent.normalized()
        if i.tenant_id != tenant_id:
            raise IntentStoreError("tenant_id mismatch between argument and intent.tenant_id")
        if normalize_repo_id(i.repo_id) != repo:
            raise IntentStoreError("repo_id mismatch between argument and intent.repo_id")

        obj = i.to_json_obj()
        spec_json = json.dumps(obj, sort_keys=True, ensure_ascii=False)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO intents (
                  tenant_id, repo_id, intent_id, spec_version, status,
                  created_at_ms, updated_at_ms, spec_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id, intent_id) DO UPDATE SET
                  spec_version=excluded.spec_version,
                  status=excluded.status,
                  created_at_ms=excluded.created_at_ms,
                  updated_at_ms=excluded.updated_at_ms,
                  spec_json=excluded.spec_json
                """,
                (
                    tenant_id,
                    repo,
                    i.intent_id,
                    int(i.spec_version),
                    str(i.status),
                    int(i.created_at_ms),
                    int(i.updated_at_ms),
                    spec_json,
                ),
            )
