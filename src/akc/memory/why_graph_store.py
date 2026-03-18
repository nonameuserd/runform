"""Why graph store facade (Phase 2).

Kept as a stable import path while implementations live in smaller modules.
"""

from __future__ import annotations

from akc.memory.why_graph_store_base import (
    ConstraintKey,
    WhyGraphError,
    WhyGraphStore,
    require_scope,
)
from akc.memory.why_graph_store_memory import InMemoryWhyGraphStore
from akc.memory.why_graph_store_sqlite import SQLiteWhyGraphStore

__all__ = [
    "ConstraintKey",
    "InMemoryWhyGraphStore",
    "SQLiteWhyGraphStore",
    "WhyGraphError",
    "WhyGraphStore",
    "require_scope",
]
