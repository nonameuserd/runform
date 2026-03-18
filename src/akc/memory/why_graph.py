"""Why graph facade (Phase 2).

This file preserves the stable import path `akc.memory.why_graph` while the
implementation is split across smaller modules:

- `akc.memory.why_graph_store`: store interfaces + backends
- `akc.memory.why_conflicts`: conflict surfacing utilities
"""

from __future__ import annotations

from akc.memory.why_conflicts import ConflictDetector
from akc.memory.why_graph_store import (
    ConstraintKey,
    InMemoryWhyGraphStore,
    SQLiteWhyGraphStore,
    WhyGraphError,
    WhyGraphStore,
)

__all__ = [
    "ConflictDetector",
    "ConstraintKey",
    "InMemoryWhyGraphStore",
    "SQLiteWhyGraphStore",
    "WhyGraphError",
    "WhyGraphStore",
]
