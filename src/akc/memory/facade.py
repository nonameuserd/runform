"""Memory facade (Phase 2).

Provides a single builder for the memory layer stores.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from akc.memory.code_memory import CodeMemoryStore, InMemoryCodeMemoryStore, SQLiteCodeMemoryStore
from akc.memory.plan_state import JsonFilePlanStateStore, PlanStateStore, SQLitePlanStateStore
from akc.memory.why_graph import InMemoryWhyGraphStore, SQLiteWhyGraphStore, WhyGraphStore

MemoryBackend = Literal["memory", "sqlite"]


@dataclass(frozen=True, slots=True)
class Memory:
    code_memory: CodeMemoryStore
    plan_state: PlanStateStore
    why_graph: WhyGraphStore


def build_memory(
    *,
    backend: MemoryBackend = "memory",
    sqlite_path: str | None = None,
) -> Memory:
    if backend == "memory":
        return Memory(
            code_memory=InMemoryCodeMemoryStore(),
            plan_state=JsonFilePlanStateStore(),
            why_graph=InMemoryWhyGraphStore(),
        )
    if backend == "sqlite":
        if sqlite_path is None or not str(sqlite_path).strip():
            raise ValueError("sqlite_path is required for sqlite memory backend")
        p = str(sqlite_path)
        return Memory(
            code_memory=SQLiteCodeMemoryStore(path=p),
            plan_state=SQLitePlanStateStore(path=p),
            why_graph=SQLiteWhyGraphStore(path=p),
        )
    raise ValueError(f"unknown backend: {backend}")
