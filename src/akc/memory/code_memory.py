"""Code memory store (Phase 2).

Stores typed `CodeMemoryItem` entries scoped by (tenant_id, repo_id) and optionally
grouped by artifact_id. This is not embedding-based recall; retrieval is basic
filtering + recency for Phase 2.
"""

from __future__ import annotations

import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterable, Mapping, Sequence
from typing import Any, Literal

from akc.memory.models import (
    CodeArtifactRef,
    CodeMemoryItem,
    CodeMemoryKind,
    json_dumps,
    json_loads_object,
    normalize_repo_id,
    now_ms,
    require_non_empty,
)


class CodeMemoryError(Exception):
    """Raised when a code memory operation fails."""


OrderBy = Literal["updated_desc", "created_desc"]


class CodeMemoryStore(ABC):
    @abstractmethod
    def upsert_items(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        artifact_id: str | None,
        items: Iterable[CodeMemoryItem],
    ) -> int: ...

    @abstractmethod
    def list_items(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        artifact_id: str | None = None,
        kind_filter: Sequence[CodeMemoryKind] | None = None,
        limit: int = 50,
        cursor_updated_at_ms: int | None = None,
        order_by: OrderBy = "updated_desc",
    ) -> list[CodeMemoryItem]: ...

    @abstractmethod
    def get_item(self, *, tenant_id: str, repo_id: str, item_id: str) -> CodeMemoryItem | None: ...

    @abstractmethod
    def delete_item(self, *, tenant_id: str, repo_id: str, item_id: str) -> bool: ...


def _require_positive(limit: int, *, name: str) -> None:
    if limit <= 0:
        raise ValueError(f"{name} must be > 0")


class InMemoryCodeMemoryStore(CodeMemoryStore):
    def __init__(self) -> None:
        # tenant -> repo -> item_id -> CodeMemoryItem
        self._items: dict[str, dict[str, dict[str, CodeMemoryItem]]] = {}

    def upsert_items(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        artifact_id: str | None,
        items: Iterable[CodeMemoryItem],
    ) -> int:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        bucket = self._items.setdefault(tenant_id, {}).setdefault(repo, {})
        wrote = 0
        for item in items:
            if item.ref.tenant_id != tenant_id:
                raise CodeMemoryError("tenant_id mismatch between argument and item.ref")
            if normalize_repo_id(item.ref.repo_id) != repo:
                raise CodeMemoryError("repo_id mismatch between argument and item.ref")
            if (item.ref.artifact_id or None) != (artifact_id or None):
                raise CodeMemoryError("artifact_id mismatch between argument and item.ref")
            # Ensure JSON discipline early.
            item.to_json_obj()
            bucket[item.id] = item
            wrote += 1
        return wrote

    def list_items(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        artifact_id: str | None = None,
        kind_filter: Sequence[CodeMemoryKind] | None = None,
        limit: int = 50,
        cursor_updated_at_ms: int | None = None,
        order_by: OrderBy = "updated_desc",
    ) -> list[CodeMemoryItem]:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        _require_positive(limit, name="limit")
        items = list(self._items.get(tenant_id, {}).get(repo, {}).values())
        if artifact_id is not None:
            items = [i for i in items if (i.ref.artifact_id or None) == (artifact_id or None)]
        if kind_filter is not None:
            allowed = set(kind_filter)
            items = [i for i in items if i.kind in allowed]
        if cursor_updated_at_ms is not None:
            items = [i for i in items if i.updated_at_ms < int(cursor_updated_at_ms)]

        if order_by == "updated_desc":
            items.sort(key=lambda i: i.updated_at_ms, reverse=True)
        elif order_by == "created_desc":
            items.sort(key=lambda i: i.created_at_ms, reverse=True)
        else:  # pragma: no cover
            raise ValueError(f"unknown order_by: {order_by}")
        return items[:limit]

    def get_item(self, *, tenant_id: str, repo_id: str, item_id: str) -> CodeMemoryItem | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(item_id, name="item_id")
        return self._items.get(tenant_id, {}).get(repo, {}).get(item_id)

    def delete_item(self, *, tenant_id: str, repo_id: str, item_id: str) -> bool:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(item_id, name="item_id")
        bucket = self._items.get(tenant_id, {}).get(repo)
        if not bucket:
            return False
        return bucket.pop(item_id, None) is not None


class SQLiteCodeMemoryStore(CodeMemoryStore):
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
                CREATE TABLE IF NOT EXISTS code_memory_items (
                  tenant_id     TEXT NOT NULL,
                  repo_id       TEXT NOT NULL,
                  item_id       TEXT NOT NULL,
                  artifact_id   TEXT NULL,
                  kind          TEXT NOT NULL,
                  content       TEXT NOT NULL,
                  metadata      TEXT NOT NULL,
                  created_at_ms INTEGER NOT NULL,
                  updated_at_ms INTEGER NOT NULL,
                  PRIMARY KEY (tenant_id, repo_id, item_id)
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS code_memory_by_repo_updated
                ON code_memory_items(tenant_id, repo_id, updated_at_ms DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS code_memory_by_repo_artifact_kind
                ON code_memory_items(tenant_id, repo_id, artifact_id, kind, updated_at_ms DESC)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS code_memory_by_repo_kind
                ON code_memory_items(tenant_id, repo_id, kind, updated_at_ms DESC)
                """
            )

    def upsert_items(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        artifact_id: str | None,
        items: Iterable[CodeMemoryItem],
    ) -> int:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)

        rows: list[tuple[str, str, str, str | None, str, str, str, int, int]] = []
        for item in items:
            if item.ref.tenant_id != tenant_id:
                raise CodeMemoryError("tenant_id mismatch between argument and item.ref")
            if normalize_repo_id(item.ref.repo_id) != repo:
                raise CodeMemoryError("repo_id mismatch between argument and item.ref")
            if (item.ref.artifact_id or None) != (artifact_id or None):
                raise CodeMemoryError("artifact_id mismatch between argument and item.ref")
            obj = item.to_json_obj()  # validates metadata JSON discipline too
            md_obj = obj.get("metadata")
            if not isinstance(md_obj, dict):
                raise CodeMemoryError("metadata must be a JSON object")
            rows.append(
                (
                    tenant_id,
                    repo,
                    item.id,
                    artifact_id,
                    str(item.kind),
                    item.content,
                    json_dumps(md_obj),
                    int(item.created_at_ms),
                    int(item.updated_at_ms),
                )
            )

        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO code_memory_items (
                  tenant_id, repo_id, item_id, artifact_id, kind, content, metadata,
                  created_at_ms, updated_at_ms
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id, item_id) DO UPDATE SET
                  artifact_id=excluded.artifact_id,
                  kind=excluded.kind,
                  content=excluded.content,
                  metadata=excluded.metadata,
                  created_at_ms=excluded.created_at_ms,
                  updated_at_ms=excluded.updated_at_ms
                """,
                rows,
            )
        return len(rows)

    def list_items(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        artifact_id: str | None = None,
        kind_filter: Sequence[CodeMemoryKind] | None = None,
        limit: int = 50,
        cursor_updated_at_ms: int | None = None,
        order_by: OrderBy = "updated_desc",
    ) -> list[CodeMemoryItem]:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        _require_positive(limit, name="limit")

        where = ["tenant_id = ?", "repo_id = ?"]
        params: list[object] = [tenant_id, repo]

        if artifact_id is not None:
            where.append("artifact_id = ?")
            params.append(artifact_id)

        if kind_filter is not None:
            if not kind_filter:
                return []
            placeholders = ", ".join(["?"] * len(kind_filter))
            where.append(f"kind IN ({placeholders})")
            params.extend([str(k) for k in kind_filter])

        if cursor_updated_at_ms is not None:
            where.append("updated_at_ms < ?")
            params.append(int(cursor_updated_at_ms))

        if order_by == "updated_desc":
            order_sql = "ORDER BY updated_at_ms DESC"
        elif order_by == "created_desc":
            order_sql = "ORDER BY created_at_ms DESC"
        else:
            raise ValueError(f"unknown order_by: {order_by}")

        sql = (
            "SELECT item_id, artifact_id, kind, content, metadata, created_at_ms, updated_at_ms "
            "FROM code_memory_items "
            f"WHERE {' AND '.join(where)} "
            f"{order_sql} "
            "LIMIT ?"
        )
        params.append(int(limit))

        with self._connect() as conn:
            cur = conn.execute(sql, tuple(params))
            rows = cur.fetchall()

        out: list[CodeMemoryItem] = []
        for (
            item_id,
            artifact_id_out,
            kind,
            content,
            metadata_raw,
            created_at_ms,
            updated_at_ms,
        ) in rows:
            md = json_loads_object(str(metadata_raw), what="code memory metadata")
            out.append(
                CodeMemoryItem(
                    id=str(item_id),
                    ref=CodeArtifactRef(
                        tenant_id=tenant_id,
                        repo_id=repo,
                        artifact_id=str(artifact_id_out) if artifact_id_out is not None else None,
                    ),
                    kind=str(kind),  # type: ignore[arg-type]
                    content=str(content),
                    metadata=md,
                    created_at_ms=int(created_at_ms),
                    updated_at_ms=int(updated_at_ms),
                )
            )

        return out

    def get_item(self, *, tenant_id: str, repo_id: str, item_id: str) -> CodeMemoryItem | None:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(item_id, name="item_id")
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT artifact_id, kind, content, metadata, created_at_ms, updated_at_ms
                FROM code_memory_items
                WHERE tenant_id=? AND repo_id=? AND item_id=?
                """,
                (tenant_id, repo, item_id),
            )
            row = cur.fetchone()
        if row is None:
            return None
        artifact_id_out, kind, content, metadata_raw, created_at_ms, updated_at_ms = row
        md = json_loads_object(str(metadata_raw), what="code memory metadata")
        try:
            return CodeMemoryItem(
                id=str(item_id),
                ref=CodeArtifactRef(
                    tenant_id=tenant_id,
                    repo_id=repo,
                    artifact_id=str(artifact_id_out) if artifact_id_out is not None else None,
                ),
                kind=str(kind),  # type: ignore[arg-type]
                content=str(content),
                metadata=md,
                created_at_ms=int(created_at_ms),
                updated_at_ms=int(updated_at_ms),
            )
        except Exception as e:  # pragma: no cover
            raise CodeMemoryError("stored CodeMemoryItem was invalid") from e

    def delete_item(self, *, tenant_id: str, repo_id: str, item_id: str) -> bool:
        require_non_empty(tenant_id, name="tenant_id")
        repo = normalize_repo_id(repo_id)
        require_non_empty(item_id, name="item_id")
        with self._connect() as conn:
            cur = conn.execute(
                "DELETE FROM code_memory_items WHERE tenant_id=? AND repo_id=? AND item_id=?",
                (tenant_id, repo, item_id),
            )
            deleted = int(getattr(cur, "rowcount", 0))
        return deleted > 0


def make_item(
    *,
    tenant_id: str,
    repo_id: str,
    artifact_id: str | None,
    item_id: str,
    kind: CodeMemoryKind,
    content: str,
    metadata: Mapping[str, Any] | None = None,
    created_at_ms: int | None = None,
    updated_at_ms: int | None = None,
) -> CodeMemoryItem:
    """Create a CodeMemoryItem with consistent timestamps."""

    t = int(created_at_ms if created_at_ms is not None else now_ms())
    u = int(updated_at_ms if updated_at_ms is not None else t)
    return CodeMemoryItem(
        id=item_id,
        ref=CodeArtifactRef(tenant_id=tenant_id, repo_id=repo_id, artifact_id=artifact_id),
        kind=kind,
        content=content,
        metadata=dict(metadata or {}),
        created_at_ms=t,
        updated_at_ms=u,
    )
