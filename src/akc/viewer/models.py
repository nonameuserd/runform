from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, TypedDict

from akc.memory.models import PlanState, require_non_empty


class ManifestArtifact(TypedDict, total=False):
    path: str
    media_type: str
    sha256: str
    size_bytes: int
    metadata: dict[str, Any] | None


class Manifest(TypedDict, total=False):
    schema_version: int
    schema_id: str
    tenant_id: str
    repo_id: str
    name: str
    artifacts: list[ManifestArtifact]
    metadata: dict[str, Any] | None


@dataclass(frozen=True, slots=True)
class ViewerInputs:
    """Input roots for a read-only view of one tenant/repo scope."""

    tenant_id: str
    repo_id: str
    outputs_root: Path
    # Base directory that contains `.akc/plan` (default: CWD).
    # The viewer never writes to this directory.
    plan_base_dir: Path | None = None
    schema_version: int = 1

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        if int(self.schema_version) <= 0:
            raise ValueError("schema_version must be > 0")


EvidenceKind = Literal["manifest", "plan_state", "execution_stage", "verifier_result", "text"]


@dataclass(frozen=True, slots=True)
class EvidenceRef:
    """A pointer to an evidence file (relative to scoped outputs dir)."""

    kind: EvidenceKind
    relpath: str
    media_type: str | None = None
    sha256: str | None = None
    size_bytes: int | None = None
    metadata: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class EvidenceIndex:
    """Convenience index for linking plan steps to artifacts."""

    by_step: dict[str, list[EvidenceRef]]
    all: list[EvidenceRef]


@dataclass(frozen=True, slots=True)
class ViewerSnapshot:
    """Resolved, schema-validated snapshot for rendering."""

    inputs: ViewerInputs
    plan: PlanState
    manifest: Manifest | None
    scoped_outputs_dir: Path
    evidence: EvidenceIndex
    # C4: optional persisted knowledge envelope + conflict reports for operator debugging.
    knowledge_envelope: dict[str, Any] | None = None
    # Versioned envelope from ``.akc/knowledge/mediation.json`` when present.
    knowledge_mediation_envelope: dict[str, Any] | None = None
    conflict_reports: tuple[dict[str, Any], ...] = ()
    # Track 6 path A: optional forensics bundle + playbook report summaries (read-only).
    operator_panels: dict[str, Any] | None = None
