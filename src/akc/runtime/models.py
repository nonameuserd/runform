from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, cast

from akc.ir.schema import IRDocument
from akc.memory.models import JSONValue, require_non_empty

RuntimePolicyMode = Literal["default", "enforce", "dry_run", "simulate", "canary"]
RuntimeActionStatus = Literal["pending", "running", "succeeded", "failed", "cancelled"]
HealthStatus = Literal["unknown", "healthy", "degraded", "failed"]
ReconcileConditionType = Literal["progressing", "stalled", "degraded"]
ReconcileConditionStatus = Literal["true", "false", "unknown"]
ObservedHealthConditionStatus = Literal["true", "false", "unknown"]
ReconcileHealthGateMode = Literal["strict", "permissive"]


def _require_non_negative_int(value: Any, *, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return cast(int, value)


def _resolve_system_ir_ref_path(*, bundle_path: Path, ref_path: str) -> Path:
    """Resolve ``system_ir_ref.path`` for a runtime bundle JSON file path.

    The compiler emits repo-relative paths such as ``.akc/ir/<run>.json``. Paths using
    ``..`` are resolved relative to the bundle file's directory (typically
    ``<repo>/.akc/runtime``).
    """
    raw = str(ref_path).strip()
    if not raw:
        return Path()
    p = Path(raw)
    if p.is_absolute():
        return p.resolve()
    norm = raw.replace("\\", "/")
    if norm.startswith(".akc/"):
        # bundle_path: <repo>/.akc/runtime/<file>.json → repo root is parents[2]
        repo_root = bundle_path.parent.parent.parent
        return (repo_root / Path(norm)).resolve()
    return (bundle_path.parent / Path(raw)).resolve()


def _validate_json_value(value: Any, *, what: str) -> JSONValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        return cast(JSONValue, value)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        return cast(JSONValue, [_validate_json_value(item, what=f"{what}[]") for item in value])
    if isinstance(value, Mapping):
        out: dict[str, JSONValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError(f"{what} keys must be strings")
            out[key] = _validate_json_value(item, what=f"{what}.{key}")
        return cast(JSONValue, out)
    raise ValueError(f"{what} must be JSONValue-compatible")


def _validate_json_mapping(value: Any, *, what: str) -> dict[str, JSONValue]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{what} must be an object")
    out: dict[str, JSONValue] = {}
    for key, item in value.items():
        if not isinstance(key, str):
            raise ValueError(f"{what} keys must be strings")
        out[key] = _validate_json_value(item, what=f"{what}.{key}")
    return out


def _optional_json_mapping(value: Any, *, what: str) -> dict[str, JSONValue] | None:
    if value is None:
        return None
    return _validate_json_mapping(value, what=what)


def _optional_non_empty(value: Any, *, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string when set")
    require_non_empty(value, name=name)
    return value.strip()


@dataclass(frozen=True, slots=True)
class RuntimeContext:
    tenant_id: str
    repo_id: str
    run_id: str
    runtime_run_id: str
    policy_mode: RuntimePolicyMode
    adapter_id: str

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="runtime_context.tenant_id")
        require_non_empty(self.repo_id, name="runtime_context.repo_id")
        require_non_empty(self.run_id, name="runtime_context.run_id")
        require_non_empty(self.runtime_run_id, name="runtime_context.runtime_run_id")
        require_non_empty(self.policy_mode, name="runtime_context.policy_mode")
        require_non_empty(self.adapter_id, name="runtime_context.adapter_id")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "tenant_id": self.tenant_id.strip(),
            "repo_id": self.repo_id.strip(),
            "run_id": self.run_id.strip(),
            "runtime_run_id": self.runtime_run_id.strip(),
            "policy_mode": self.policy_mode,
            "adapter_id": self.adapter_id.strip(),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeContext:
        return RuntimeContext(
            tenant_id=str(obj.get("tenant_id", "")).strip(),
            repo_id=str(obj.get("repo_id", "")).strip(),
            run_id=str(obj.get("run_id", "")).strip(),
            runtime_run_id=str(obj.get("runtime_run_id", "")).strip(),
            policy_mode=cast(RuntimePolicyMode, str(obj.get("policy_mode", "")).strip()),
            adapter_id=str(obj.get("adapter_id", "")).strip(),
        )


@dataclass(frozen=True, slots=True)
class RuntimeBundleRef:
    bundle_path: str
    manifest_hash: str
    created_at: int
    source_compile_run_id: str

    def __post_init__(self) -> None:
        require_non_empty(self.bundle_path, name="runtime_bundle_ref.bundle_path")
        require_non_empty(self.manifest_hash, name="runtime_bundle_ref.manifest_hash")
        require_non_empty(self.source_compile_run_id, name="runtime_bundle_ref.source_compile_run_id")
        _require_non_negative_int(self.created_at, name="runtime_bundle_ref.created_at")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "bundle_path": self.bundle_path.strip(),
            "manifest_hash": self.manifest_hash.strip(),
            "created_at": int(self.created_at),
            "source_compile_run_id": self.source_compile_run_id.strip(),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeBundleRef:
        return RuntimeBundleRef(
            bundle_path=str(obj.get("bundle_path", "")).strip(),
            manifest_hash=str(obj.get("manifest_hash", "")).strip(),
            created_at=_require_non_negative_int(obj.get("created_at", 0), name="runtime_bundle_ref.created_at"),
            source_compile_run_id=str(obj.get("source_compile_run_id", "")).strip(),
        )


@dataclass(frozen=True, slots=True)
class RuntimeNodeRef:
    node_id: str
    kind: str
    contract_id: str

    def __post_init__(self) -> None:
        require_non_empty(self.node_id, name="runtime_node_ref.node_id")
        require_non_empty(self.kind, name="runtime_node_ref.kind")
        require_non_empty(self.contract_id, name="runtime_node_ref.contract_id")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "node_id": self.node_id.strip(),
            "kind": self.kind.strip(),
            "contract_id": self.contract_id.strip(),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeNodeRef:
        return RuntimeNodeRef(
            node_id=str(obj.get("node_id", "")).strip(),
            kind=str(obj.get("kind", "")).strip(),
            contract_id=str(obj.get("contract_id", "")).strip(),
        )


@dataclass(frozen=True, slots=True)
class RuntimeTransition:
    from_state: str
    to_state: str
    trigger_id: str
    transition_id: str
    occurred_at: int

    def __post_init__(self) -> None:
        require_non_empty(self.from_state, name="runtime_transition.from_state")
        require_non_empty(self.to_state, name="runtime_transition.to_state")
        require_non_empty(self.trigger_id, name="runtime_transition.trigger_id")
        require_non_empty(self.transition_id, name="runtime_transition.transition_id")
        _require_non_negative_int(self.occurred_at, name="runtime_transition.occurred_at")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "from_state": self.from_state.strip(),
            "to_state": self.to_state.strip(),
            "trigger_id": self.trigger_id.strip(),
            "transition_id": self.transition_id.strip(),
            "occurred_at": int(self.occurred_at),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeTransition:
        return RuntimeTransition(
            from_state=str(obj.get("from_state", "")).strip(),
            to_state=str(obj.get("to_state", "")).strip(),
            trigger_id=str(obj.get("trigger_id", "")).strip(),
            transition_id=str(obj.get("transition_id", "")).strip(),
            occurred_at=_require_non_negative_int(obj.get("occurred_at", 0), name="runtime_transition.occurred_at"),
        )


@dataclass(frozen=True, slots=True)
class RuntimeAction:
    action_id: str
    action_type: str
    node_ref: RuntimeNodeRef
    inputs_fingerprint: str
    idempotency_key: str
    policy_context: Mapping[str, JSONValue] | None = field(default=None)

    def __post_init__(self) -> None:
        require_non_empty(self.action_id, name="runtime_action.action_id")
        require_non_empty(self.action_type, name="runtime_action.action_type")
        require_non_empty(self.inputs_fingerprint, name="runtime_action.inputs_fingerprint")
        require_non_empty(self.idempotency_key, name="runtime_action.idempotency_key")
        if self.policy_context is not None:
            _validate_json_mapping(self.policy_context, what="runtime_action.policy_context")

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "action_id": self.action_id.strip(),
            "action_type": self.action_type.strip(),
            "node_ref": self.node_ref.to_json_obj(),
            "inputs_fingerprint": self.inputs_fingerprint.strip(),
            "idempotency_key": self.idempotency_key.strip(),
        }
        if self.policy_context is not None:
            out["policy_context"] = dict(self.policy_context)
        return out

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeAction:
        node_ref_raw = obj.get("node_ref")
        if not isinstance(node_ref_raw, Mapping):
            raise ValueError("runtime_action.node_ref must be an object")
        pc = _optional_json_mapping(obj.get("policy_context"), what="runtime_action.policy_context")
        return RuntimeAction(
            action_id=str(obj.get("action_id", "")).strip(),
            action_type=str(obj.get("action_type", "")).strip(),
            node_ref=RuntimeNodeRef.from_json_obj(node_ref_raw),
            inputs_fingerprint=str(obj.get("inputs_fingerprint", "")).strip(),
            idempotency_key=str(obj.get("idempotency_key", "")).strip(),
            policy_context=pc,
        )


@dataclass(frozen=True, slots=True)
class RuntimeActionResult:
    status: RuntimeActionStatus
    outputs: Mapping[str, JSONValue] = field(default_factory=dict)
    error: str | None = None
    duration_ms: int | None = None
    cost: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.status, name="runtime_action_result.status")
        _validate_json_mapping(self.outputs, what="runtime_action_result.outputs")
        _optional_json_mapping(self.cost, what="runtime_action_result.cost")
        if self.error is not None:
            require_non_empty(self.error, name="runtime_action_result.error")
        if self.duration_ms is not None:
            _require_non_negative_int(self.duration_ms, name="runtime_action_result.duration_ms")

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "status": self.status,
            "outputs": dict(self.outputs),
            "error": self.error.strip() if self.error is not None else None,
            "duration_ms": int(self.duration_ms) if self.duration_ms is not None else None,
            "cost": dict(self.cost) if self.cost is not None else None,
        }
        return {key: value for key, value in out.items() if value is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeActionResult:
        return RuntimeActionResult(
            status=cast(RuntimeActionStatus, str(obj.get("status", "")).strip()),
            outputs=_validate_json_mapping(obj.get("outputs", {}), what="runtime_action_result.outputs"),
            error=_optional_non_empty(obj.get("error"), name="runtime_action_result.error"),
            duration_ms=(
                _require_non_negative_int(obj.get("duration_ms"), name="runtime_action_result.duration_ms")
                if obj.get("duration_ms") is not None
                else None
            ),
            cost=_optional_json_mapping(obj.get("cost"), what="runtime_action_result.cost"),
        )


@dataclass(frozen=True, slots=True)
class ReconcileOperation:
    operation_id: str
    operation_type: Literal["create", "update", "delete", "noop"]
    target: str
    payload: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_non_empty(self.operation_id, name="reconcile_operation.operation_id")
        require_non_empty(self.operation_type, name="reconcile_operation.operation_type")
        require_non_empty(self.target, name="reconcile_operation.target")
        _validate_json_mapping(self.payload, what="reconcile_operation.payload")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "operation_id": self.operation_id.strip(),
            "operation_type": self.operation_type,
            "target": self.target.strip(),
            "payload": dict(self.payload),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> ReconcileOperation:
        return ReconcileOperation(
            operation_id=str(obj.get("operation_id", "")).strip(),
            operation_type=cast(
                Literal["create", "update", "delete", "noop"],
                str(obj.get("operation_type", "")).strip(),
            ),
            target=str(obj.get("target", "")).strip(),
            payload=_validate_json_mapping(obj.get("payload", {}), what="reconcile_operation.payload"),
        )


@dataclass(frozen=True, slots=True)
class ReconcileCondition:
    """Kubernetes-style status condition for a single reconcile resource (stable JSON for export)."""

    type: ReconcileConditionType
    status: ReconcileConditionStatus
    reason: str | None = None
    message: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.type, name="reconcile_condition.type")
        require_non_empty(self.status, name="reconcile_condition.status")
        if self.reason is not None:
            require_non_empty(self.reason, name="reconcile_condition.reason")
        if self.message is not None:
            require_non_empty(self.message, name="reconcile_condition.message")

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "type": self.type,
            "status": self.status,
            "reason": self.reason.strip() if self.reason is not None else None,
            "message": self.message.strip() if self.message is not None else None,
        }
        return {key: value for key, value in out.items() if value is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> ReconcileCondition:
        return ReconcileCondition(
            type=cast(ReconcileConditionType, str(obj.get("type", "")).strip()),
            status=cast(ReconcileConditionStatus, str(obj.get("status", "")).strip()),
            reason=_optional_non_empty(obj.get("reason"), name="reconcile_condition.reason"),
            message=_optional_non_empty(obj.get("message"), name="reconcile_condition.message"),
        )


@dataclass(frozen=True, slots=True)
class ObservedHealthCondition:
    """Kubernetes-style condition row attached to an observed deployment resource (replay-stable JSON)."""

    type: str
    status: ObservedHealthConditionStatus
    reason: str | None = None
    message: str | None = None
    last_transition_time: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.type, name="observed_health_condition.type")
        require_non_empty(self.status, name="observed_health_condition.status")
        if self.reason is not None:
            require_non_empty(self.reason, name="observed_health_condition.reason")
        if self.message is not None:
            require_non_empty(self.message, name="observed_health_condition.message")
        if self.last_transition_time is not None:
            require_non_empty(self.last_transition_time, name="observed_health_condition.last_transition_time")

    def to_json_obj(self) -> dict[str, JSONValue]:
        ltt = self.last_transition_time.strip() if self.last_transition_time is not None else None
        out: dict[str, JSONValue] = {
            "type": self.type.strip(),
            "status": self.status,
            "reason": self.reason.strip() if self.reason is not None else None,
            "message": self.message.strip() if self.message is not None else None,
            "last_transition_time": ltt,
        }
        return {key: value for key, value in out.items() if value is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> ObservedHealthCondition:
        st_raw = str(obj.get("status", "")).strip().lower()
        if st_raw in {"true", "false", "unknown"}:
            st = cast(ObservedHealthConditionStatus, st_raw)
        else:
            up = str(obj.get("status", "")).strip()
            if up == "True":
                st = "true"
            elif up == "False":
                st = "false"
            elif up == "Unknown":
                st = "unknown"
            else:
                st = "unknown"
        return ObservedHealthCondition(
            type=str(obj.get("type", "")).strip(),
            status=st,
            reason=_optional_non_empty(obj.get("reason"), name="observed_health_condition.reason"),
            message=_optional_non_empty(obj.get("message"), name="observed_health_condition.message"),
            last_transition_time=_optional_non_empty(
                obj.get("last_transition_time"), name="observed_health_condition.last_transition_time"
            ),
        )


def build_reconcile_conditions(
    *,
    converged: bool,
    health_status: HealthStatus,
    last_error: str | None,
    rollback_triggered: bool,
) -> tuple[ReconcileCondition, ...]:
    """Derive standard progressing / stalled / degraded conditions from reconcile outcome."""
    err = (last_error or "").lower()
    degraded = health_status in ("degraded", "failed") or rollback_triggered
    policy_or_observe_block = (
        "policy denied" in err
        or "observe-only" in err
        or "observe_only" in err
        or "observe-only deployment provider" in err
    )
    stalled = bool(
        (not converged and policy_or_observe_block) or rollback_triggered or (not converged and "rolled back" in err)
    )
    progressing = not converged and not stalled and not rollback_triggered

    def _one(
        t: ReconcileConditionType,
        active: bool,
        *,
        reason: str | None,
    ) -> ReconcileCondition:
        st: ReconcileConditionStatus = "true" if active else "false"
        return ReconcileCondition(type=t, status=st, reason=reason if active else None, message=None)

    # Alphabetical by type for stable JSON ordering in exports.
    return (
        _one("degraded", degraded, reason="unhealthy_or_rollback"),
        _one("progressing", progressing, reason="reconciling"),
        _one("stalled", stalled, reason="blocked_or_rollback"),
    )


@dataclass(frozen=True, slots=True)
class ReconcilePlan:
    resource_id: str
    desired_hash: str
    operations: tuple[ReconcileOperation, ...]

    def __post_init__(self) -> None:
        require_non_empty(self.resource_id, name="reconcile_plan.resource_id")
        require_non_empty(self.desired_hash, name="reconcile_plan.desired_hash")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "resource_id": self.resource_id.strip(),
            "desired_hash": self.desired_hash.strip(),
            "operations": [operation.to_json_obj() for operation in self.operations],
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> ReconcilePlan:
        operations_raw = obj.get("operations", [])
        if not isinstance(operations_raw, Sequence) or isinstance(operations_raw, (str, bytes)):
            raise ValueError("reconcile_plan.operations must be an array")
        return ReconcilePlan(
            resource_id=str(obj.get("resource_id", "")).strip(),
            desired_hash=str(obj.get("desired_hash", "")).strip(),
            operations=tuple(
                ReconcileOperation.from_json_obj(operation)
                for operation in operations_raw
                if isinstance(operation, Mapping)
            ),
        )


@dataclass(frozen=True, slots=True)
class ReconcileStatus:
    """Per-resource reconcile evaluation.

    **Convergence (measurable):** ``converged`` is true only when ``hash_matched`` is true **and**
    ``health_gate_passed`` is true. With bundle ``reconcile_health_gate=permissive`` (default),
    the health gate accepts ``healthy`` or ``unknown``. With ``strict``, the gate requires
    ``healthy`` unless an observed condition with ``type`` **Ready** has ``status: true``, or
    ``unknown`` is still within ``reconcile_health_unknown_grace_ms`` relative to resync wait
    (see :func:`akc.runtime.reconciler.health_gate_passes`). Hash match compares observed
    provider state to ``desired_hash`` (or absent delete).
    """

    resource_id: str
    observed_hash: str
    health_status: HealthStatus
    converged: bool
    desired_hash: str = ""
    hash_matched: bool = False
    health_gate_passed: bool = False
    last_error: str | None = None
    conditions: tuple[ReconcileCondition, ...] = ()
    observed_health_conditions: tuple[ObservedHealthCondition, ...] = ()
    reconcile_health_gate: ReconcileHealthGateMode = "permissive"

    def __post_init__(self) -> None:
        require_non_empty(self.resource_id, name="reconcile_status.resource_id")
        require_non_empty(self.observed_hash, name="reconcile_status.observed_hash")
        require_non_empty(self.health_status, name="reconcile_status.health_status")
        if self.last_error is not None:
            require_non_empty(self.last_error, name="reconcile_status.last_error")

    def to_json_obj(self) -> dict[str, JSONValue]:
        conds = [c.to_json_obj() for c in self.conditions]
        ohc = [c.to_json_obj() for c in self.observed_health_conditions]
        out: dict[str, JSONValue] = {
            "resource_id": self.resource_id.strip(),
            "observed_hash": self.observed_hash.strip(),
            "health_status": self.health_status,
            "converged": bool(self.converged),
            "desired_hash": self.desired_hash.strip(),
            "hash_matched": bool(self.hash_matched),
            "health_gate_passed": bool(self.health_gate_passed),
            "last_error": self.last_error.strip() if self.last_error is not None else None,
            "conditions": cast(JSONValue, conds),
            "observed_health_conditions": cast(JSONValue, ohc),
            "reconcile_health_gate": self.reconcile_health_gate,
        }
        return {key: value for key, value in out.items() if value is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> ReconcileStatus:
        cond_raw = obj.get("conditions", [])
        conditions: tuple[ReconcileCondition, ...] = ()
        if isinstance(cond_raw, Sequence) and not isinstance(cond_raw, (str, bytes)):
            parsed: list[ReconcileCondition] = []
            for item in cond_raw:
                if isinstance(item, Mapping):
                    parsed.append(ReconcileCondition.from_json_obj(item))
            conditions = tuple(parsed)
        ohc_raw = obj.get("observed_health_conditions", [])
        observed_health: tuple[ObservedHealthCondition, ...] = ()
        if isinstance(ohc_raw, Sequence) and not isinstance(ohc_raw, (str, bytes)):
            oparsed: list[ObservedHealthCondition] = []
            for item in ohc_raw:
                if isinstance(item, Mapping):
                    oparsed.append(ObservedHealthCondition.from_json_obj(item))
            observed_health = tuple(oparsed)
        gate_raw = str(obj.get("reconcile_health_gate", "permissive")).strip().lower()
        gate: ReconcileHealthGateMode = "strict" if gate_raw == "strict" else "permissive"
        desired = str(obj.get("desired_hash", "")).strip()
        return ReconcileStatus(
            resource_id=str(obj.get("resource_id", "")).strip(),
            observed_hash=str(obj.get("observed_hash", "")).strip(),
            health_status=cast(HealthStatus, str(obj.get("health_status", "")).strip()),
            converged=bool(obj.get("converged", False)),
            desired_hash=desired,
            hash_matched=bool(obj.get("hash_matched", False)),
            health_gate_passed=bool(obj.get("health_gate_passed", False)),
            last_error=_optional_non_empty(obj.get("last_error"), name="reconcile_status.last_error"),
            conditions=conditions,
            observed_health_conditions=observed_health,
            reconcile_health_gate=gate,
        )


@dataclass(frozen=True, slots=True)
class RuntimeCheckpoint:
    checkpoint_id: str
    cursor: str
    pending_queue: tuple[RuntimeAction, ...]
    node_states: Mapping[str, JSONValue]
    replay_token: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.checkpoint_id, name="runtime_checkpoint.checkpoint_id")
        require_non_empty(self.cursor, name="runtime_checkpoint.cursor")
        _validate_json_mapping(self.node_states, what="runtime_checkpoint.node_states")
        if self.replay_token is not None:
            require_non_empty(self.replay_token, name="runtime_checkpoint.replay_token")

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "checkpoint_id": self.checkpoint_id.strip(),
            "cursor": self.cursor.strip(),
            "pending_queue": [action.to_json_obj() for action in self.pending_queue],
            "node_states": dict(self.node_states),
            "replay_token": self.replay_token.strip() if self.replay_token is not None else None,
        }
        return {key: value for key, value in out.items() if value is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeCheckpoint:
        pending_queue_raw = obj.get("pending_queue", [])
        if not isinstance(pending_queue_raw, Sequence) or isinstance(pending_queue_raw, (str, bytes)):
            raise ValueError("runtime_checkpoint.pending_queue must be an array")
        return RuntimeCheckpoint(
            checkpoint_id=str(obj.get("checkpoint_id", "")).strip(),
            cursor=str(obj.get("cursor", "")).strip(),
            pending_queue=tuple(
                RuntimeAction.from_json_obj(action) for action in pending_queue_raw if isinstance(action, Mapping)
            ),
            node_states=_validate_json_mapping(obj.get("node_states", {}), what="runtime_checkpoint.node_states"),
            replay_token=_optional_non_empty(obj.get("replay_token"), name="runtime_checkpoint.replay_token"),
        )


@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    """Structured runtime bus event.

    ``payload`` may include optional coordination / observability keys (additive, backward
    compatible): ``coordination_spec_sha256``, ``role_id``, ``graph_step_id``,
    ``parent_event_id``, ``input_sha256``, ``output_sha256``, ``policy_envelope_sha256``,
    ``orchestration_spec_sha256``, ``coordination_spec_version``, ``otel_trace``.
    See :mod:`akc.runtime.coordination.audit`.
    """

    event_id: str
    event_type: str
    timestamp: int
    context: RuntimeContext
    payload: Mapping[str, JSONValue]

    def __post_init__(self) -> None:
        require_non_empty(self.event_id, name="runtime_event.event_id")
        require_non_empty(self.event_type, name="runtime_event.event_type")
        _require_non_negative_int(self.timestamp, name="runtime_event.timestamp")
        _validate_json_mapping(self.payload, what="runtime_event.payload")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "event_id": self.event_id.strip(),
            "event_type": self.event_type.strip(),
            "timestamp": int(self.timestamp),
            "context": self.context.to_json_obj(),
            "payload": dict(self.payload),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeEvent:
        context_raw = obj.get("context")
        if not isinstance(context_raw, Mapping):
            raise ValueError("runtime_event.context must be an object")
        return RuntimeEvent(
            event_id=str(obj.get("event_id", "")).strip(),
            event_type=str(obj.get("event_type", "")).strip(),
            timestamp=_require_non_negative_int(obj.get("timestamp", 0), name="runtime_event.timestamp"),
            context=RuntimeContext.from_json_obj(context_raw),
            payload=_validate_json_mapping(obj.get("payload", {}), what="runtime_event.payload"),
        )


@dataclass(frozen=True, slots=True)
class RuntimeBundle:
    context: RuntimeContext
    ref: RuntimeBundleRef
    nodes: tuple[RuntimeNodeRef, ...]
    contract_ids: tuple[str, ...]
    policy_envelope: Mapping[str, JSONValue] = field(default_factory=dict)
    metadata: Mapping[str, JSONValue] = field(default_factory=dict)
    # Parsed system IR when embedded, loaded from system_ir_ref, or injected by tests.
    ir_document: IRDocument | None = None

    def __post_init__(self) -> None:
        for contract_id in self.contract_ids:
            require_non_empty(contract_id, name="runtime_bundle.contract_ids[]")
        _validate_json_mapping(self.policy_envelope, what="runtime_bundle.policy_envelope")
        _validate_json_mapping(self.metadata, what="runtime_bundle.metadata")
        if self.ir_document is not None:
            validate_runtime_ir_bundle_alignment(ir=self.ir_document, bundle=self)

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "context": self.context.to_json_obj(),
            "ref": self.ref.to_json_obj(),
            "nodes": [node.to_json_obj() for node in self.nodes],
            "contract_ids": [contract_id.strip() for contract_id in self.contract_ids],
            "policy_envelope": dict(self.policy_envelope),
            "metadata": dict(self.metadata),
            # ir_document is runtime-only; omit from JSON round-trip
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> RuntimeBundle:
        context_raw = obj.get("context")
        ref_raw = obj.get("ref")
        nodes_raw = obj.get("nodes", [])
        contract_ids_raw = obj.get("contract_ids", [])
        if not isinstance(context_raw, Mapping):
            raise ValueError("runtime_bundle.context must be an object")
        if not isinstance(ref_raw, Mapping):
            raise ValueError("runtime_bundle.ref must be an object")
        if not isinstance(nodes_raw, Sequence) or isinstance(nodes_raw, (str, bytes)):
            raise ValueError("runtime_bundle.nodes must be an array")
        if not isinstance(contract_ids_raw, Sequence) or isinstance(contract_ids_raw, (str, bytes)):
            raise ValueError("runtime_bundle.contract_ids must be an array")
        return RuntimeBundle(
            context=RuntimeContext.from_json_obj(context_raw),
            ref=RuntimeBundleRef.from_json_obj(ref_raw),
            nodes=tuple(RuntimeNodeRef.from_json_obj(node) for node in nodes_raw if isinstance(node, Mapping)),
            contract_ids=tuple(str(contract_id).strip() for contract_id in contract_ids_raw),
            policy_envelope=_validate_json_mapping(
                obj.get("policy_envelope", {}), what="runtime_bundle.policy_envelope"
            ),
            metadata=_validate_json_mapping(obj.get("metadata", {}), what="runtime_bundle.metadata"),
            ir_document=None,
        )


def load_ir_document_from_bundle_payload(
    *,
    payload: Mapping[str, Any],
    bundle_ref: RuntimeBundleRef,
    context: RuntimeContext,
) -> IRDocument | None:
    """Load IR from embedded ``system_ir`` or ``system_ir_ref`` relative to ``bundle_ref`` (optional-but-preferred)."""
    embedded = payload.get("system_ir")
    if isinstance(embedded, Mapping):
        ir_doc = IRDocument.from_json_obj(embedded)
        _validate_ir_document_scope(ir_doc=ir_doc, context=context)
        ref_raw = payload.get("system_ir_ref")
        if isinstance(ref_raw, Mapping):
            expected_fp = str(ref_raw.get("fingerprint", "")).strip()
            if expected_fp and ir_doc.fingerprint() != expected_fp:
                raise ValueError("embedded system_ir fingerprint mismatch")
        return ir_doc
    ref_raw = payload.get("system_ir_ref")
    if not isinstance(ref_raw, Mapping):
        return None
    path = str(ref_raw.get("path", "")).strip()
    if not path:
        return None
    bundle_path = Path(bundle_ref.bundle_path).expanduser()
    ir_path = _resolve_system_ir_ref_path(bundle_path=bundle_path, ref_path=path)
    if not ir_path.is_file():
        return None
    ir_doc = IRDocument.from_json_file(ir_path)
    _validate_ir_document_scope(ir_doc=ir_doc, context=context)
    expected_fp = str(ref_raw.get("fingerprint", "")).strip()
    if expected_fp and ir_doc.fingerprint() != expected_fp:
        raise ValueError("system IR file fingerprint mismatch")
    return ir_doc


def _validate_ir_document_scope(*, ir_doc: IRDocument, context: RuntimeContext) -> None:
    if ir_doc.tenant_id.strip() != context.tenant_id.strip():
        raise ValueError("runtime bundle IR tenant_id does not match runtime context (tenant isolation)")
    if ir_doc.repo_id.strip() != context.repo_id.strip():
        raise ValueError("runtime bundle IR repo_id does not match runtime context")


def validate_runtime_ir_bundle_alignment(*, ir: IRDocument, bundle: RuntimeBundle) -> None:
    """Ensure IR scope matches the bundle runtime context.

    Use this anywhere ``IRDocument`` is interpreted alongside a ``RuntimeBundle``
    (reconciler IR desired-state, future provider hooks) so tenant/repo rules stay
    identical to bundle load and ``load_ir_document_from_bundle_payload``.
    """
    _validate_ir_document_scope(ir_doc=ir, context=bundle.context)


def load_ir_document_for_bundle(bundle: RuntimeBundle) -> IRDocument | None:
    """Return parsed IR for this bundle: embedded field, else resolve `metadata.system_ir_ref` next to the bundle file.

    Validates tenant/repo scope and optional fingerprint from the ref. Returns None when no ref or file is missing.
    """
    if bundle.ir_document is not None:
        return bundle.ir_document
    ref_raw = bundle.metadata.get("system_ir_ref")
    if not isinstance(ref_raw, Mapping):
        return None
    path = str(ref_raw.get("path", "")).strip()
    if not path:
        return None
    bundle_path = Path(bundle.ref.bundle_path).expanduser()
    ir_path = _resolve_system_ir_ref_path(bundle_path=bundle_path, ref_path=path)
    if not ir_path.is_file():
        return None
    ir_doc = IRDocument.from_json_file(ir_path)
    _validate_ir_document_scope(ir_doc=ir_doc, context=bundle.context)
    expected_fp = str(ref_raw.get("fingerprint", "")).strip()
    if expected_fp and ir_doc.fingerprint() != expected_fp:
        raise ValueError("system IR fingerprint mismatch for runtime bundle")
    return ir_doc
