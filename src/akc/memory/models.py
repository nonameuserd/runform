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

JSONValue: TypeAlias = None | bool | int | float | str | list["JSONValue"] | dict[str, "JSONValue"]

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


def now_ms() -> int:
    return int(time.time() * 1000)


def new_uuid() -> str:
    return str(uuid.uuid4())


def goal_fingerprint(goal: str) -> str:
    """Stable fingerprint for a plan goal (used to detect drift)."""

    require_non_empty(goal, name="goal")
    h = sha256(goal.encode("utf-8")).hexdigest()
    return h[:16]


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

    def __post_init__(self) -> None:
        require_non_empty(self.conflict_id, name="conflict_id")
        require_non_empty(self.repo_id, name="repo_id")
        require_non_empty(self.conflict_type, name="conflict_type")
        require_non_empty(self.summary, name="summary")

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
        json_dumps(obj)
        return obj
