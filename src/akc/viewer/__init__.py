"""Read-only viewers for AKC artifacts (Phase 5).

The viewer is intentionally local-first and read-only:
- It reads plan state (.akc/plan or sqlite memory) and emitted artifacts (manifest + files).
- It never executes code, runs tools, applies patches, or mutates repositories.
"""

from __future__ import annotations

from .models import EvidenceIndex, ViewerInputs, ViewerSnapshot
from .snapshot import load_viewer_snapshot

__all__ = ["EvidenceIndex", "ViewerInputs", "ViewerSnapshot", "load_viewer_snapshot"]
