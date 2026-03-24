"""SQLite why graph store (Phase 2)."""

from __future__ import annotations

import sqlite3
from collections.abc import Iterable, Iterator
from typing import Any, cast

from akc.memory.models import (
    WhyEdge,
    WhyEdgeType,
    WhyNode,
    WhyNodeType,
    json_dumps,
    json_loads_object,
    require_non_empty,
)
from akc.memory.why_graph_store_base import WhyGraphStore, require_scope

_ALLOWED_NODE_TYPES: set[str] = {"constraint", "decision", "rationale", "observation"}
_ALLOWED_EDGE_TYPES: set[str] = {
    "related_to",
    "refines",
    "supports",
    "causes",
    "prevents",
    "depends_on",
}


class SQLiteWhyGraphStore(WhyGraphStore):
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
                CREATE TABLE IF NOT EXISTS why_nodes (
                  tenant_id TEXT NOT NULL,
                  repo_id   TEXT NOT NULL,
                  node_id   TEXT NOT NULL,
                  type      TEXT NOT NULL,
                  payload   TEXT NOT NULL,
                  PRIMARY KEY (tenant_id, repo_id, node_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS why_edges (
                  tenant_id TEXT NOT NULL,
                  repo_id   TEXT NOT NULL,
                  src       TEXT NOT NULL,
                  dst       TEXT NOT NULL,
                  type      TEXT NOT NULL,
                  payload   TEXT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS why_edges_by_repo_src
                ON why_edges(tenant_id, repo_id, src)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS why_nodes_by_repo_type
                ON why_nodes(tenant_id, repo_id, type)
                """
            )

    def upsert_nodes(self, *, tenant_id: str, repo_id: str, nodes: Iterable[WhyNode]) -> int:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        rows: list[tuple[str, str, str, str, str]] = []
        for n in nodes:
            rows.append((tenant_id, repo, n.id, str(n.type), json_dumps(dict(n.payload))))
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO why_nodes (tenant_id, repo_id, node_id, type, payload)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id, node_id) DO UPDATE SET
                  type=excluded.type,
                  payload=excluded.payload
                """,
                rows,
            )
        return len(rows)

    def add_edges(self, *, tenant_id: str, repo_id: str, edges: Iterable[WhyEdge]) -> int:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        rows: list[tuple[str, str, str, str, str, str | None]] = []
        for e in edges:
            payload = json_dumps(dict(e.payload)) if e.payload is not None else None
            rows.append((tenant_id, repo, e.src, e.dst, str(e.type), payload))
        if not rows:
            return 0
        with self._connect() as conn:
            conn.executemany(
                """
                INSERT INTO why_edges (tenant_id, repo_id, src, dst, type, payload)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
        return len(rows)

    def get_node(self, *, tenant_id: str, repo_id: str, node_id: str) -> WhyNode | None:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        require_non_empty(node_id, name="node_id")
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT node_id, type, payload
                FROM why_nodes
                WHERE tenant_id=? AND repo_id=? AND node_id=?
                """,
                (tenant_id, repo, node_id),
            )
            row = cur.fetchone()
        if row is None:
            return None
        node_id_out, type_out, payload_raw = row
        payload = json_loads_object(str(payload_raw), what="why node payload")
        t = str(type_out)
        if t not in _ALLOWED_NODE_TYPES:
            raise ValueError(f"unknown why node type in DB: {t}")
        return WhyNode(id=str(node_id_out), type=cast(WhyNodeType, t), payload=payload)

    def iter_out_edges(self, *, tenant_id: str, repo_id: str, src: str) -> Iterator[WhyEdge]:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        require_non_empty(src, name="src")
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT src, dst, type, payload
                FROM why_edges
                WHERE tenant_id=? AND repo_id=? AND src=?
                """,
                (tenant_id, repo, src),
            )
            rows = cur.fetchall()
        for src_out, dst_out, type_out, payload_raw in rows:
            payload: dict[str, Any] | None = None
            if payload_raw is not None:
                payload = json_loads_object(str(payload_raw), what="why edge payload")
            t = str(type_out)
            if t not in _ALLOWED_EDGE_TYPES:
                raise ValueError(f"unknown why edge type in DB: {t}")
            yield WhyEdge(
                src=str(src_out),
                dst=str(dst_out),
                type=cast(WhyEdgeType, t),
                payload=payload,
            )

    def list_nodes_by_type(self, *, tenant_id: str, repo_id: str, node_type: WhyNodeType) -> list[WhyNode]:
        repo = require_scope(tenant_id=tenant_id, repo_id=repo_id)
        require_non_empty(node_type, name="node_type")
        with self._connect() as conn:
            cur = conn.execute(
                """
                SELECT node_id, type, payload
                FROM why_nodes
                WHERE tenant_id=? AND repo_id=? AND type=?
                """,
                (tenant_id, repo, str(node_type)),
            )
            rows = cur.fetchall()
        out: list[WhyNode] = []
        for node_id_out, type_out, payload_raw in rows:
            payload = json_loads_object(str(payload_raw), what="why node payload")
            t = str(type_out)
            if t not in _ALLOWED_NODE_TYPES:
                raise ValueError(f"unknown why node type in DB: {t}")
            out.append(WhyNode(id=str(node_id_out), type=cast(WhyNodeType, t), payload=payload))
        return out
