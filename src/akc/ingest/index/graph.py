"""Graph store abstraction for ingestion.

Phase 1 graph is optional and intentionally minimal:
- Nodes/edges are tenant-scoped
- Payloads are JSON-serializable dicts
- SQLite backend supports persistence without extra dependencies
"""

from __future__ import annotations

import json
import sqlite3
from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from typing import Any


class GraphStoreError(Exception):
    """Raised when a graph store operation fails."""


def _require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _json_dumps(payload: Mapping[str, Any]) -> str:
    try:
        return json.dumps(dict(payload), sort_keys=True, ensure_ascii=False)
    except TypeError as e:
        raise GraphStoreError("payload must be JSON-serializable") from e


@dataclass(frozen=True, slots=True)
class Node:
    id: str
    type: str
    payload: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class Edge:
    src: str
    dst: str
    type: str
    payload: Mapping[str, Any] | None = None


class GraphStore(ABC):
    @abstractmethod
    def upsert_nodes(self, *, tenant_id: str, nodes: Iterable[Node]) -> int: ...

    @abstractmethod
    def add_edges(self, *, tenant_id: str, edges: Iterable[Edge]) -> int: ...

    @abstractmethod
    def get_node(self, *, tenant_id: str, node_id: str) -> Node | None: ...

    @abstractmethod
    def iter_out_edges(self, *, tenant_id: str, src: str) -> Iterator[Edge]: ...


class InMemoryGraphStore(GraphStore):
    def __init__(self) -> None:
        # tenant -> node_id -> Node
        self._nodes: dict[str, dict[str, Node]] = {}
        # tenant -> src -> list[Edge]
        self._out: dict[str, dict[str, list[Edge]]] = {}

    def upsert_nodes(self, *, tenant_id: str, nodes: Iterable[Node]) -> int:
        _require_non_empty(tenant_id, name="tenant_id")
        bucket = self._nodes.setdefault(tenant_id, {})
        wrote = 0
        for n in nodes:
            _require_non_empty(n.id, name="node.id")
            _require_non_empty(n.type, name="node.type")
            # Validate payload is JSON-serializable (schema discipline).
            _json_dumps(n.payload)
            bucket[n.id] = n
            wrote += 1
        return wrote

    def add_edges(self, *, tenant_id: str, edges: Iterable[Edge]) -> int:
        _require_non_empty(tenant_id, name="tenant_id")
        out_bucket = self._out.setdefault(tenant_id, {})
        wrote = 0
        for e in edges:
            _require_non_empty(e.src, name="edge.src")
            _require_non_empty(e.dst, name="edge.dst")
            _require_non_empty(e.type, name="edge.type")
            if e.payload is not None:
                _json_dumps(e.payload)
            out_bucket.setdefault(e.src, []).append(e)
            wrote += 1
        return wrote

    def get_node(self, *, tenant_id: str, node_id: str) -> Node | None:
        _require_non_empty(tenant_id, name="tenant_id")
        _require_non_empty(node_id, name="node_id")
        return self._nodes.get(tenant_id, {}).get(node_id)

    def iter_out_edges(self, *, tenant_id: str, src: str) -> Iterator[Edge]:
        _require_non_empty(tenant_id, name="tenant_id")
        _require_non_empty(src, name="src")
        yield from self._out.get(tenant_id, {}).get(src, [])


class SQLiteGraphStore(GraphStore):
    """SQLite-backed graph store.

    Schema is created automatically. Suitable for local persistence and simple
    graph lookups during retrieval/debugging.
    """

    def __init__(self, *, path: str) -> None:
        _require_non_empty(path, name="path")
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
                CREATE TABLE IF NOT EXISTS nodes (
                  tenant_id TEXT NOT NULL,
                  node_id   TEXT NOT NULL,
                  type      TEXT NOT NULL,
                  payload   TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, node_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS edges (
                  tenant_id TEXT NOT NULL,
                  src       TEXT NOT NULL,
                  dst       TEXT NOT NULL,
                  type      TEXT NOT NULL,
                  payload   TEXT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS edges_by_tenant_src ON edges(tenant_id, src)")

    def upsert_nodes(self, *, tenant_id: str, nodes: Iterable[Node]) -> int:
        _require_non_empty(tenant_id, name="tenant_id")
        rows: list[tuple[str, str, str, str]] = []
        for n in nodes:
            _require_non_empty(n.id, name="node.id")
            _require_non_empty(n.type, name="node.type")
            rows.append((tenant_id, n.id, n.type, _json_dumps(n.payload)))
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO nodes (tenant_id, node_id, type, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(tenant_id, node_id) DO UPDATE SET
                  type=excluded.type,
                  payload=excluded.payload
                """,
                rows,
            )
        return len(rows)

    def add_edges(self, *, tenant_id: str, edges: Iterable[Edge]) -> int:
        _require_non_empty(tenant_id, name="tenant_id")
        rows: list[tuple[str, str, str, str, str | None]] = []
        for e in edges:
            _require_non_empty(e.src, name="edge.src")
            _require_non_empty(e.dst, name="edge.dst")
            _require_non_empty(e.type, name="edge.type")
            payload = _json_dumps(e.payload) if e.payload is not None else None
            rows.append((tenant_id, e.src, e.dst, e.type, payload))
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                "INSERT INTO edges (tenant_id, src, dst, type, payload) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        return len(rows)

    def get_node(self, *, tenant_id: str, node_id: str) -> Node | None:
        _require_non_empty(tenant_id, name="tenant_id")
        _require_non_empty(node_id, name="node_id")
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT node_id, type, payload FROM nodes WHERE tenant_id=? AND node_id=?",
                (tenant_id, node_id),
            )
            row = cur.fetchone()
        if row is None:
            return None
        node_id_out, type_out, payload_raw = row
        try:
            payload = json.loads(payload_raw)
        except Exception as e:  # pragma: no cover
            raise GraphStoreError("stored node payload was not valid JSON") from e
        if not isinstance(payload, dict):
            raise GraphStoreError("stored node payload must be a JSON object")
        return Node(id=str(node_id_out), type=str(type_out), payload=payload)

    def iter_out_edges(self, *, tenant_id: str, src: str) -> Iterator[Edge]:
        _require_non_empty(tenant_id, name="tenant_id")
        _require_non_empty(src, name="src")
        with self._connect() as conn:
            cur = conn.execute(
                "SELECT src, dst, type, payload FROM edges WHERE tenant_id=? AND src=?",
                (tenant_id, src),
            )
            rows = cur.fetchall()
        for src_out, dst_out, type_out, payload_raw in rows:
            payload: dict[str, Any] | None = None
            if payload_raw is not None:
                loaded = json.loads(payload_raw)
                if not isinstance(loaded, dict):
                    raise GraphStoreError("stored edge payload must be a JSON object")
                payload = loaded
            yield Edge(src=str(src_out), dst=str(dst_out), type=str(type_out), payload=payload)
