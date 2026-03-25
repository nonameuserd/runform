"""Phase 2 memory models: code memory, plan state, and why/conflict graph payloads.

All payloads that are persisted must be JSON-serializable (dict/list/str/int/float/bool/None).
Tenant isolation is enforced at interface boundaries in stores; models keep the IDs explicit.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Literal, TypeAlias, cast

from akc.artifacts.contracts import apply_schema_envelope

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]

# Provenance pointers are produced by the compiler provenance mapper and then
# persisted in why-graph constraint payloads. This alias keeps the memory layer
# decoupled from compile-time imports while still type-documenting intent.
ProvenancePointerJson: TypeAlias = dict[str, Any]

CodeMemoryKind = Literal[
    "snippet",
    "file_snapshot",
    "patch",
    "test_result",
    "test_smoke_result",
    "test_full_result",
    "conflict_report",
    "note",
]

PlanStepStatus = Literal["pending", "in_progress", "done", "failed", "skipped"]
PlanStatus = Literal["active", "completed", "abandoned"]

WhyNodeType = Literal["constraint", "decision", "rationale", "observation"]
WhyEdgeType = Literal[
    "related_to",
    "refines",
    "supports",
    "causes",
    "prevents",
    "depends_on",
]

ConflictSeverity = Literal["low", "med", "high"]
ConflictType = Literal["constraint_contradiction", "plan_drift"]


class MemoryModelError(Exception):
    """Raised when a memory model cannot be validated or serialized."""


def require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def json_value_as_int(value: JSONValue | None, *, default: int) -> int:
    """Best-effort int coercion for numeric fields stored as JSON."""

    if value is None:
        return default
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            s = value.strip()
            return int(s) if s else default
        except ValueError:
            return default
    return default


def json_value_as_float(value: JSONValue | None, *, default: float) -> float:
    """Best-effort float coercion for numeric fields stored as JSON."""

    if value is None:
        return default
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            s = value.strip()
            return float(s) if s else default
        except ValueError:
            return default
    return default


def json_value_as_optional_int(value: JSONValue | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        try:
            s = value.strip()
            return int(s) if s else None
        except ValueError:
            return None
    return None


def json_value_as_optional_float(value: JSONValue | None) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            s = value.strip()
            return float(s) if s else None
        except ValueError:
            return None
    return None


def now_ms() -> int:
    return int(time.time() * 1000)


def new_uuid() -> str:
    return str(uuid.uuid4())


def goal_fingerprint(goal: str) -> str:
    """Stable fingerprint for a plan goal (used to detect drift)."""

    require_non_empty(goal, name="goal")
    h = sha256(goal.encode("utf-8")).hexdigest()
    return h[:16]


def normalize_tenant_id(tenant_id: str) -> str:
    """Normalize tenant_id into a single path segment under ``outputs_root`` (no traversal)."""

    require_non_empty(tenant_id, name="tenant_id")
    s = tenant_id.strip()
    if os.sep in s or (os.altsep is not None and os.altsep in s):
        raise ValueError("tenant_id must not contain path separators")
    if ".." in s:
        raise ValueError("tenant_id must not contain '..'")
    return s


def normalize_repo_id(repo_id: str) -> str:
    """Normalize a repo_id into a safe, stable identifier.

    We keep this strict to prevent path traversal when used in filesystem stores.
    """

    require_non_empty(repo_id, name="repo_id")
    s = repo_id.strip()
    if os.sep in s or (os.altsep is not None and os.altsep in s):
        raise ValueError("repo_id must not contain path separators")
    if ".." in s:
        raise ValueError("repo_id must not contain '..'")
    return s


def json_dumps(value: JSONValue) -> str:
    try:
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    except TypeError as e:
        raise MemoryModelError("value must be JSON-serializable") from e


def json_loads_object(raw: str, *, what: str) -> dict[str, Any]:
    try:
        loaded = json.loads(raw)
    except Exception as e:  # pragma: no cover
        raise MemoryModelError(f"stored {what} was not valid JSON") from e
    if not isinstance(loaded, dict):
        raise MemoryModelError(f"stored {what} must be a JSON object")
    return loaded


def _as_json_object(value: Mapping[str, Any]) -> dict[str, JSONValue]:
    # Best-effort cast for mypy; json_dumps will validate at runtime.
    return cast(dict[str, JSONValue], dict(value))


@dataclass(frozen=True, slots=True)
class CodeArtifactRef:
    tenant_id: str
    repo_id: str
    artifact_id: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")

    def normalized(self) -> CodeArtifactRef:
        return CodeArtifactRef(
            tenant_id=self.tenant_id.strip(),
            repo_id=normalize_repo_id(self.repo_id),
            artifact_id=self.artifact_id.strip() if isinstance(self.artifact_id, str) else None,
        )


@dataclass(frozen=True, slots=True)
class CodeMemoryItem:
    id: str
    ref: CodeArtifactRef
    kind: CodeMemoryKind
    content: str
    metadata: Mapping[str, Any]
    created_at_ms: int
    updated_at_ms: int

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="id")
        require_non_empty(self.kind, name="kind")
        require_non_empty(self.content, name="content")
        # Ensure metadata is JSON-serializable and an object.
        json_dumps(_as_json_object(self.metadata))

    def to_json_obj(self) -> dict[str, JSONValue]:
        ref = self.ref.normalized()
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "ref": {
                "tenant_id": ref.tenant_id,
                "repo_id": ref.repo_id,
                "artifact_id": ref.artifact_id,
            },
            "kind": self.kind,
            "content": self.content,
            "metadata": _as_json_object(self.metadata),
            "created_at_ms": int(self.created_at_ms),
            "updated_at_ms": int(self.updated_at_ms),
        }
        # Validate discipline.
        json_dumps(obj)
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> CodeMemoryItem:
        ref_raw = obj.get("ref")
        if not isinstance(ref_raw, dict):
            raise MemoryModelError("CodeMemoryItem.ref must be an object")
        ref = CodeArtifactRef(
            tenant_id=str(ref_raw.get("tenant_id", "")),
            repo_id=str(ref_raw.get("repo_id", "")),
            artifact_id=cast(str | None, ref_raw.get("artifact_id")),
        )
        md_raw = obj.get("metadata")
        if not isinstance(md_raw, dict):
            raise MemoryModelError("CodeMemoryItem.metadata must be an object")
        return CodeMemoryItem(
            id=str(obj.get("id", "")),
            ref=ref,
            kind=cast(CodeMemoryKind, str(obj.get("kind", ""))),
            content=str(obj.get("content", "")),
            metadata=md_raw,
            created_at_ms=int(obj.get("created_at_ms", 0)),
            updated_at_ms=int(obj.get("updated_at_ms", 0)),
        )


@dataclass(frozen=True, slots=True)
class PlanStep:
    id: str
    title: str
    status: PlanStepStatus
    order_idx: int
    started_at_ms: int | None = None
    finished_at_ms: int | None = None
    notes: str | None = None
    inputs: Mapping[str, Any] | None = None
    outputs: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="step.id")
        require_non_empty(self.title, name="step.title")
        require_non_empty(self.status, name="step.status")
        if self.order_idx < 0:
            raise ValueError("step.order_idx must be >= 0")
        if self.inputs is not None:
            json_dumps(_as_json_object(self.inputs))
        if self.outputs is not None:
            json_dumps(_as_json_object(self.outputs))

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "id": self.id,
            "title": self.title,
            "status": self.status,
            "order_idx": int(self.order_idx),
            "started_at_ms": self.started_at_ms,
            "finished_at_ms": self.finished_at_ms,
            "notes": self.notes,
            "inputs": _as_json_object(self.inputs or {}),
            "outputs": _as_json_object(self.outputs or {}),
        }
        json_dumps(obj)
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> PlanStep:
        inputs = obj.get("inputs")
        outputs = obj.get("outputs")
        if inputs is not None and not isinstance(inputs, dict):
            raise MemoryModelError("PlanStep.inputs must be an object")
        if outputs is not None and not isinstance(outputs, dict):
            raise MemoryModelError("PlanStep.outputs must be an object")
        return PlanStep(
            id=str(obj.get("id", "")),
            title=str(obj.get("title", "")),
            status=cast(PlanStepStatus, str(obj.get("status", ""))),
            order_idx=int(obj.get("order_idx", 0)),
            started_at_ms=cast(int | None, obj.get("started_at_ms")),
            finished_at_ms=cast(int | None, obj.get("finished_at_ms")),
            notes=cast(str | None, obj.get("notes")),
            inputs=cast(dict[str, Any] | None, inputs),
            outputs=cast(dict[str, Any] | None, outputs),
        )


@dataclass(frozen=True, slots=True)
class PlanState:
    id: str
    tenant_id: str
    repo_id: str
    goal: str
    status: PlanStatus
    created_at_ms: int
    updated_at_ms: int
    steps: tuple[PlanStep, ...]
    next_step_id: str | None = None
    budgets: Mapping[str, Any] | None = None
    last_feedback: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="plan.id")
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        require_non_empty(self.goal, name="goal")
        require_non_empty(self.status, name="status")
        if self.budgets is not None:
            json_dumps(_as_json_object(self.budgets))
        if self.last_feedback is not None:
            json_dumps(_as_json_object(self.last_feedback))

    def normalized(self) -> PlanState:
        return PlanState(
            id=self.id,
            tenant_id=self.tenant_id.strip(),
            repo_id=normalize_repo_id(self.repo_id),
            goal=self.goal,
            status=self.status,
            created_at_ms=self.created_at_ms,
            updated_at_ms=self.updated_at_ms,
            steps=self.steps,
            next_step_id=self.next_step_id,
            budgets=self.budgets,
            last_feedback=self.last_feedback,
        )

    def to_json_obj(self) -> dict[str, JSONValue]:
        p = self.normalized()
        obj: dict[str, JSONValue] = {
            "id": p.id,
            "tenant_id": p.tenant_id,
            "repo_id": p.repo_id,
            "goal": p.goal,
            "status": p.status,
            "created_at_ms": int(p.created_at_ms),
            "updated_at_ms": int(p.updated_at_ms),
            "steps": [s.to_json_obj() for s in p.steps],
            "next_step_id": p.next_step_id,
            "budgets": _as_json_object(p.budgets or {}),
            "last_feedback": _as_json_object(p.last_feedback or {}),
        }
        apply_schema_envelope(obj=cast(dict[str, Any], obj), kind="plan_state")
        json_dumps(obj)
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> PlanState:
        steps_raw = obj.get("steps")
        if not isinstance(steps_raw, list):
            raise MemoryModelError("PlanState.steps must be an array")
        steps: list[PlanStep] = []
        for s in steps_raw:
            if not isinstance(s, dict):
                raise MemoryModelError("PlanState.steps entries must be objects")
            steps.append(PlanStep.from_json_obj(s))

        budgets = obj.get("budgets")
        last_feedback = obj.get("last_feedback")
        if budgets is not None and not isinstance(budgets, dict):
            raise MemoryModelError("PlanState.budgets must be an object")
        if last_feedback is not None and not isinstance(last_feedback, dict):
            raise MemoryModelError("PlanState.last_feedback must be an object")

        return PlanState(
            id=str(obj.get("id", "")),
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            goal=str(obj.get("goal", "")),
            status=cast(PlanStatus, str(obj.get("status", "active"))),
            created_at_ms=int(obj.get("created_at_ms", 0)),
            updated_at_ms=int(obj.get("updated_at_ms", 0)),
            steps=tuple(steps),
            next_step_id=cast(str | None, obj.get("next_step_id")),
            budgets=cast(dict[str, Any] | None, budgets),
            last_feedback=cast(dict[str, Any] | None, last_feedback),
        )


@dataclass(frozen=True, slots=True)
class WhyNode:
    id: str
    type: WhyNodeType
    payload: Mapping[str, Any]

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="node.id")
        require_non_empty(self.type, name="node.type")
        json_dumps(_as_json_object(self.payload))


@dataclass(frozen=True, slots=True)
class WhyEdge:
    src: str
    dst: str
    type: WhyEdgeType
    payload: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.src, name="edge.src")
        require_non_empty(self.dst, name="edge.dst")
        require_non_empty(self.type, name="edge.type")
        if self.payload is not None:
            json_dumps(_as_json_object(self.payload))


@dataclass(frozen=True, slots=True)
class ConflictReport:
    conflict_id: str
    detected_at_ms: int
    severity: ConflictSeverity
    repo_id: str
    plan_id: str | None
    conflict_type: ConflictType
    entities: tuple[str, ...]
    summary: str
    suggested_actions: tuple[str, ...]
    # Best-effort evidence enrichment. Populated when the underlying why-graph
    # constraint nodes include provenance/evidence pointers.
    conflicting_provenance: dict[str, list[ProvenancePointerJson]] | None = None
    evidence_doc_ids: tuple[str, ...] | None = None
    # Phase 2: align with knowledge-layer mediation (assertion node ids / rules).
    participant_assertion_ids: tuple[str, ...] | None = None
    mediation_rule: str | None = None
    intent_constraint_ids: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.conflict_id, name="conflict_id")
        require_non_empty(self.repo_id, name="repo_id")
        require_non_empty(self.conflict_type, name="conflict_type")
        require_non_empty(self.summary, name="summary")
        if self.conflicting_provenance is not None:
            json_dumps(_as_json_object(self.conflicting_provenance))
        if self.evidence_doc_ids is not None:
            json_dumps(list(self.evidence_doc_ids))
        if self.participant_assertion_ids is not None:
            json_dumps(list(self.participant_assertion_ids))
        if self.intent_constraint_ids is not None:
            json_dumps(list(self.intent_constraint_ids))

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "conflict_id": self.conflict_id,
            "detected_at_ms": int(self.detected_at_ms),
            "severity": self.severity,
            "repo_id": self.repo_id,
            "plan_id": self.plan_id,
            "conflict_type": self.conflict_type,
            "entities": list(self.entities),
            "summary": self.summary,
            "suggested_actions": list(self.suggested_actions),
        }
        if self.conflicting_provenance is not None:
            obj["conflicting_provenance"] = _as_json_object(self.conflicting_provenance)
        if self.evidence_doc_ids is not None:
            obj["evidence_doc_ids"] = list(self.evidence_doc_ids)
        if self.participant_assertion_ids is not None:
            obj["participant_assertion_ids"] = list(self.participant_assertion_ids)
        if self.mediation_rule is not None and str(self.mediation_rule).strip():
            obj["mediation_rule"] = str(self.mediation_rule).strip()
        if self.intent_constraint_ids is not None:
            obj["intent_constraint_ids"] = list(self.intent_constraint_ids)
        json_dumps(obj)
        return obj
