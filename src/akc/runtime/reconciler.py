from __future__ import annotations

import math
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import ClassVar, Literal, Protocol, cast

from akc.ir.schema import IRDocument, IRNode
from akc.memory.models import JSONValue
from akc.runtime.models import (
    HealthStatus,
    ObservedHealthCondition,
    ReconcileCondition,
    ReconcileHealthGateMode,
    ReconcileOperation,
    ReconcilePlan,
    ReconcileStatus,
    RuntimeBundle,
    build_reconcile_conditions,
    validate_runtime_ir_bundle_alignment,
)
from akc.runtime.policy import RuntimePolicyRuntime, ensure_runtime_context_match
from akc.utils.fingerprint import stable_json_fingerprint

ReconcileMode = Literal["simulate", "enforce", "canary"]
RollbackOutcome = Literal["rollback_applied", "rollback_failed", "rollback_unsupported"]

# Matches deployable kinds enumerated in :func:`akc.compile.artifact_passes.run_runtime_bundle_pass`.
DEPLOYABLE_IR_NODE_KINDS: frozenset[str] = frozenset({"service", "integration", "infrastructure", "agent"})


@dataclass(frozen=True, slots=True)
class CanaryProgressiveStrategy:
    """Progressive canary rollout parameters for reconciler promotion/abort.

    This is an opt-in behavior layered on top of the existing canary reconcile
    "hold" semantics. When enabled, the reconciler will apply canary steps,
    run analysis classifications, and only promote (apply the remaining steps)
    when analysis succeeds.
    """

    enabled: bool
    # Step endpoints expressed as fractions (0 < x <= 1) of total resources, or counts (x > 1).
    weight_steps: tuple[float, ...]
    # Pause windows between steps (ms). Length should be len(weight_steps) - 1.
    pause_windows_ms: tuple[int, ...]
    # If any canary resource health_status is in these values, abort + rollback.
    abort_on_health_status: tuple[HealthStatus, ...]
    # If any canary resource health_status is in these values OR any resource did not converge,
    # the step result is inconclusive (no promotion, no rollback).
    inconclusive_on_health_status: tuple[HealthStatus, ...]


def _resolve_canary_progressive_strategy(
    metadata: Mapping[str, JSONValue], *, canary_limit: int
) -> CanaryProgressiveStrategy | None:
    """Resolve opt-in progressive canary strategy from runtime bundle metadata."""

    raw = metadata.get("reconcile_canary_strategy")
    if not isinstance(raw, Mapping):
        return None
    enabled = raw.get("enabled", True)
    if enabled is not True:
        return None

    # User-supplied step endpoints. Actual defaulting happens in reconcile.
    weight_steps: tuple[float, ...] = ()
    weight_steps_raw = raw.get("weight_steps")
    if isinstance(weight_steps_raw, Sequence) and not isinstance(weight_steps_raw, (str, bytes)):
        tmp: list[float] = []
        for item in weight_steps_raw:
            if isinstance(item, bool):
                continue
            if isinstance(item, (int, float)) and not isinstance(item, bool):
                tmp.append(float(item))
        weight_steps = tuple(tmp)

    pause_windows_ms: tuple[int, ...] = ()
    pause_raw = raw.get("pause_windows_ms")
    if isinstance(pause_raw, Sequence) and not isinstance(pause_raw, (str, bytes)):
        tmp2: list[int] = []
        for item in pause_raw:
            if isinstance(item, bool):
                continue
            if isinstance(item, int):
                tmp2.append(int(max(0, item)))
        pause_windows_ms = tuple(tmp2)

    abort_on_health_status: tuple[HealthStatus, ...] = ("degraded", "failed")
    abort_raw = raw.get("abort_on_health_status")
    if isinstance(abort_raw, Sequence) and not isinstance(abort_raw, (str, bytes)):
        tmp3: list[HealthStatus] = []
        for item in abort_raw:
            st = str(item).strip().lower()
            if st in {"unknown", "healthy", "degraded", "failed"}:
                tmp3.append(cast(HealthStatus, st))
        if tmp3:
            abort_on_health_status = tuple(tmp3)

    inconclusive_on_health_status: tuple[HealthStatus, ...] = ("unknown",)
    inconclusive_raw = raw.get("inconclusive_on_health_status")
    if isinstance(inconclusive_raw, Sequence) and not isinstance(inconclusive_raw, (str, bytes)):
        tmp4: list[HealthStatus] = []
        for item in inconclusive_raw:
            st = str(item).strip().lower()
            if st in {"unknown", "healthy", "degraded", "failed"}:
                tmp4.append(cast(HealthStatus, st))
        if tmp4:
            inconclusive_on_health_status = tuple(tmp4)

    return CanaryProgressiveStrategy(
        enabled=True,
        weight_steps=weight_steps,
        pause_windows_ms=pause_windows_ms,
        abort_on_health_status=abort_on_health_status,
        inconclusive_on_health_status=inconclusive_on_health_status,
    )


def resolve_reconcile_health_gate(metadata: Mapping[str, JSONValue]) -> ReconcileHealthGateMode:
    raw = str(metadata.get("reconcile_health_gate", "permissive")).strip().lower()
    return "strict" if raw == "strict" else "permissive"


def resolve_reconcile_unknown_grace_ms(metadata: Mapping[str, JSONValue]) -> int:
    raw = metadata.get("reconcile_health_unknown_grace_ms")
    if isinstance(raw, int) and not isinstance(raw, bool) and raw >= 0:
        return int(raw)
    return 0


def ready_condition_true(conditions: tuple[ObservedHealthCondition, ...]) -> bool:
    return any(c.type.strip().lower() == "ready" and c.status == "true" for c in conditions)


def health_gate_passes(
    *,
    gate: ReconcileHealthGateMode,
    health_status: HealthStatus,
    observed_conditions: tuple[ObservedHealthCondition, ...],
    unknown_grace_ms: int,
    resync_elapsed_wait_ms: int,
) -> bool:
    """Evaluate whether observation passes the bundle-configured health gate.

    * ``permissive`` (default): ``healthy`` and ``unknown`` pass (legacy stub-friendly).
    * ``strict``: ``healthy`` passes; ``degraded`` / ``failed`` fail; ``unknown`` passes only
      if a **Ready** observed condition is true or unknown grace has not expired.
    """
    if gate == "permissive":
        return health_status in {"healthy", "unknown"}
    if health_status == "healthy":
        return True
    if health_status in {"degraded", "failed"}:
        return False
    if ready_condition_true(observed_conditions):
        return True
    return unknown_grace_ms > 0 and resync_elapsed_wait_ms < unknown_grace_ms


def _ir_deployable_nodes_for_reconcile(*, ir: IRDocument, bundle: RuntimeBundle) -> tuple[IRNode, ...]:
    """Deployable IR nodes, optionally restricted to ``referenced_ir_nodes`` when non-empty."""

    raw = bundle.metadata.get("referenced_ir_nodes")
    restrict: set[str] | None = None
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        restrict = set()
        for item in raw:
            if isinstance(item, Mapping):
                nid = str(item.get("id", "")).strip()
                if nid:
                    restrict.add(nid)
    out: list[IRNode] = []
    for n in sorted(ir.nodes, key=lambda x: x.id):
        if str(n.kind) not in DEPLOYABLE_IR_NODE_KINDS:
            continue
        if restrict is not None and restrict and n.id not in restrict:
            continue
        out.append(n)
    return tuple(out)


def _deployment_intent_row_from_ir_node(n: IRNode) -> dict[str, JSONValue]:
    return {
        "node_id": n.id,
        "kind": n.kind,
        "name": n.name,
        "depends_on": list(n.depends_on),
        "effects": n.effects.to_json_obj() if n.effects is not None else None,
        "contract_id": n.contract.contract_id if n.contract is not None else None,
    }


def ir_projection_deployment_intents(*, ir: IRDocument, bundle: RuntimeBundle) -> list[dict[str, JSONValue]]:
    """Canonical deployable projection from IR + bundle referenced-node slice (for strict alignment checks)."""

    return [_deployment_intent_row_from_ir_node(n) for n in _ir_deployable_nodes_for_reconcile(ir=ir, bundle=bundle)]


def validate_strict_deployment_intents_align_with_ir(*, ir: IRDocument, bundle: RuntimeBundle) -> None:
    """Fail closed when ``deployment_intents_ir_alignment=strict`` and the bundle drifts from IR."""

    alignment = str(bundle.metadata.get("deployment_intents_ir_alignment", "off")).strip().lower()
    if alignment != "strict":
        return
    expected = ir_projection_deployment_intents(ir=ir, bundle=bundle)
    raw = bundle.metadata.get("deployment_intents")
    actual: list[dict[str, JSONValue]] = []
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        for item in raw:
            if isinstance(item, Mapping):
                actual.append(dict(item))
    if stable_json_fingerprint({"deployment_intents": expected}) != stable_json_fingerprint(
        {"deployment_intents": actual}
    ):
        raise ValueError(
            "deployment_intents_ir_alignment=strict but deployment_intents do not match IR deployable projection"
        )


@dataclass(frozen=True, slots=True)
class ObservedResource:
    resource_id: str
    resource_class: str
    observed_hash: str
    health_status: Literal["unknown", "healthy", "degraded", "failed"] = "unknown"
    payload: Mapping[str, JSONValue] = field(default_factory=dict)
    health_conditions: tuple[ObservedHealthCondition, ...] = ()


@dataclass(frozen=True, slots=True)
class ProviderOperationResult:
    operation: ReconcileOperation
    applied: bool
    observed_hash: str
    health_status: Literal["unknown", "healthy", "degraded", "failed"]
    error: str | None = None
    evidence: Mapping[str, JSONValue] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ReconcileEvidence:
    resource_id: str
    mode: ReconcileMode
    operations: tuple[ProviderOperationResult, ...]
    converged: bool
    health_status: Literal["unknown", "healthy", "degraded", "failed"]
    rollback_triggered: bool
    rollback_target_hash: str | None
    rollback_outcome: RollbackOutcome | None = None
    conditions: tuple[ReconcileCondition, ...] = ()
    # Bounded resync loop (CLI / bundle metadata): attempt index and backoff between attempts.
    resync_attempt: int = 1
    resync_max_attempts: int = 1
    resync_interval_ms: int = 0


class DeploymentProviderClient(Protocol):
    def observe(self, *, resource_id: str, resource_class: str) -> ObservedResource | None: ...

    def list_observed(self) -> tuple[ObservedResource, ...]: ...

    def apply(self, *, operation: ReconcileOperation) -> ProviderOperationResult: ...

    def rollback(self, *, resource_id: str, resource_class: str, target_hash: str) -> ProviderOperationResult: ...


@dataclass(slots=True)
class InMemoryDeploymentProviderClient(DeploymentProviderClient):
    """In-memory provider: supports real apply/rollback for tests and default runs."""

    observe_only: ClassVar[bool] = False
    observed: dict[str, ObservedResource] = field(default_factory=dict)

    def observe(self, *, resource_id: str, resource_class: str) -> ObservedResource | None:
        _ = resource_class
        current = self.observed.get(resource_id)
        if current is None:
            return None
        return current

    def list_observed(self) -> tuple[ObservedResource, ...]:
        return tuple(self.observed[resource_id] for resource_id in sorted(self.observed))

    def apply(self, *, operation: ReconcileOperation) -> ProviderOperationResult:
        payload = dict(operation.payload)
        target = str(payload.get("target", operation.target)).strip()
        desired_hash = str(payload.get("desired_hash", "")).strip() or stable_json_fingerprint(payload)
        resource_class = str(payload.get("resource_class", "service")).strip() or "service"
        health: HealthStatus = "healthy" if operation.operation_type != "delete" else "unknown"
        applied = operation.operation_type != "noop"
        if operation.operation_type == "delete":
            self.observed.pop(target, None)
        else:
            self.observed[target] = ObservedResource(
                resource_id=target,
                resource_class=resource_class,
                observed_hash=desired_hash,
                health_status=health,
                payload=payload,
                health_conditions=(),
            )
        return ProviderOperationResult(
            operation=operation,
            applied=applied,
            observed_hash=desired_hash,
            health_status=health,
            evidence={
                "resource_id": target,
                "resource_class": resource_class,
                "operation_type": operation.operation_type,
            },
        )

    def rollback(self, *, resource_id: str, resource_class: str, target_hash: str) -> ProviderOperationResult:
        operation = ReconcileOperation(
            operation_id=f"{resource_id}:rollback:{target_hash}",
            operation_type="update",
            target=resource_id,
            payload={
                "resource_id": resource_id,
                "resource_class": resource_class,
                "desired_hash": target_hash,
                "rollback": True,
                "target": resource_id,
            },
        )
        self.observed[resource_id] = ObservedResource(
            resource_id=resource_id,
            resource_class=resource_class,
            observed_hash=target_hash,
            health_status="healthy",
            payload={"rollback": True},
            health_conditions=(),
        )
        return ProviderOperationResult(
            operation=operation,
            applied=True,
            observed_hash=target_hash,
            health_status="healthy",
            evidence={"rollback_target_hash": target_hash},
        )


@dataclass(slots=True)
class DeploymentReconciler:
    """Reconcile desired vs observed state.

    If the provider sets ``observe_only=True`` (read-only adapters), then even when
    ``mode`` is ``enforce`` or ``canary`` the reconciler **does not** call
    ``provider.apply`` / ``provider.rollback`` or authorize those mutations: it only
    observes external truth and reports drift (same code path as ``simulate`` for
    mutating operations).
    """

    mode: ReconcileMode = "simulate"
    provider: DeploymentProviderClient = field(default_factory=InMemoryDeploymentProviderClient)
    canary_limit: int = 1
    rollback_on_failure: bool = True
    policy_runtime: RuntimePolicyRuntime | None = None
    desired_hash_history: dict[str, list[str]] = field(default_factory=dict)
    operation_evidence: dict[str, list[ProviderOperationResult]] = field(default_factory=dict)

    @staticmethod
    def provider_is_observe_only(provider: DeploymentProviderClient) -> bool:
        return bool(getattr(provider, "observe_only", False))

    @staticmethod
    def classify_rollback_outcome(result: ProviderOperationResult) -> RollbackOutcome:
        ev = result.evidence if isinstance(result.evidence, Mapping) else {}
        explicit = str(ev.get("rollback_outcome", "")).strip().lower()
        if explicit in {"rollback_applied", "rollback_failed", "rollback_unsupported"}:
            return cast(RollbackOutcome, explicit)
        err = str(result.error or "").strip().lower()
        if "rollback_unsupported" in err or "unsupported_capability" in err or ev.get("rollback_supported") is False:
            return "rollback_unsupported"
        if bool(result.applied) and not err:
            return "rollback_applied"
        return "rollback_failed"

    def build_desired_state(self, *, bundle: RuntimeBundle) -> dict[str, dict[str, JSONValue]]:
        source = str(bundle.metadata.get("reconcile_desired_state_source", "")).strip()
        if bundle.ir_document is not None and source == "ir":
            return self.build_desired_state_from_ir(ir=bundle.ir_document, bundle=bundle)
        intents_raw = bundle.metadata.get("deployment_intents", [])
        desired: dict[str, dict[str, JSONValue]] = {}
        if isinstance(intents_raw, Sequence) and not isinstance(intents_raw, (str, bytes)):
            for raw_intent in intents_raw:
                if not isinstance(raw_intent, Mapping):
                    continue
                resource_id = str(raw_intent.get("node_id", "")).strip()
                if not resource_id:
                    continue
                resource_class = str(raw_intent.get("kind", "service")).strip() or "service"
                payload: dict[str, JSONValue] = {
                    "resource_id": resource_id,
                    "resource_class": resource_class,
                    "target": resource_id,
                    "desired_hash": stable_json_fingerprint(dict(raw_intent)),
                    "intent": dict(raw_intent),
                    "bundle_manifest_hash": bundle.ref.manifest_hash,
                    "desired_state_source": "deployment_intents",
                }
                desired[resource_id] = payload
        return desired

    def build_desired_state_from_ir(self, *, ir: IRDocument, bundle: RuntimeBundle) -> dict[str, dict[str, JSONValue]]:
        """Desired hashes from canonical IR node fingerprints (tenant-scoped)."""
        validate_runtime_ir_bundle_alignment(ir=ir, bundle=bundle)
        validate_strict_deployment_intents_align_with_ir(ir=ir, bundle=bundle)
        nodes_by_id = {n.id: n for n in ir.nodes}
        desired: dict[str, dict[str, JSONValue]] = {}
        ir_only = bool(bundle.metadata.get("reconcile_deploy_targets_from_ir_only")) is True
        if ir_only:
            for deploy_node in _ir_deployable_nodes_for_reconcile(ir=ir, bundle=bundle):
                resource_id = str(deploy_node.id).strip()
                if not resource_id:
                    continue
                resource_class = str(deploy_node.kind).strip() or "service"
                payload_ir: dict[str, JSONValue] = {
                    "resource_id": resource_id,
                    "resource_class": resource_class,
                    "target": resource_id,
                    "desired_hash": deploy_node.fingerprint(),
                    "intent": deploy_node.to_json_obj(),
                    "bundle_manifest_hash": bundle.ref.manifest_hash,
                    "desired_state_source": "ir",
                }
                desired[resource_id] = payload_ir
            return desired

        intents_raw = bundle.metadata.get("deployment_intents", [])
        if isinstance(intents_raw, Sequence) and not isinstance(intents_raw, (str, bytes)):
            for raw_intent in intents_raw:
                if not isinstance(raw_intent, Mapping):
                    continue
                resource_id = str(raw_intent.get("node_id", "")).strip()
                if not resource_id:
                    continue
                matched = nodes_by_id.get(resource_id)
                resource_class = str(raw_intent.get("kind", "service")).strip() or "service"
                if matched is not None:
                    desired_hash = matched.fingerprint()
                    intent_payload: JSONValue = matched.to_json_obj()
                else:
                    desired_hash = stable_json_fingerprint(dict(raw_intent))
                    intent_payload = cast(JSONValue, dict(raw_intent))
                payload_legacy: dict[str, JSONValue] = {
                    "resource_id": resource_id,
                    "resource_class": resource_class,
                    "target": resource_id,
                    "desired_hash": desired_hash,
                    "intent": intent_payload,
                    "bundle_manifest_hash": bundle.ref.manifest_hash,
                    "desired_state_source": "ir",
                }
                desired[resource_id] = payload_legacy
        return desired

    def inspect_observed_state(
        self, *, bundle: RuntimeBundle, desired_state: Mapping[str, Mapping[str, JSONValue]]
    ) -> dict[str, ObservedResource | None]:
        _ = bundle
        observed: dict[str, ObservedResource | None] = {}
        for resource_id, desired in desired_state.items():
            observed[resource_id] = self.provider.observe(
                resource_id=resource_id,
                resource_class=str(desired.get("resource_class", "service")),
            )
        return observed

    def build_plan(self, *, bundle: RuntimeBundle) -> tuple[ReconcilePlan, ...]:
        desired_state = self.build_desired_state(bundle=bundle)
        observed_state = self.inspect_observed_state(bundle=bundle, desired_state=desired_state)
        plans: list[ReconcilePlan] = []
        for resource_id, desired in sorted(desired_state.items()):
            desired_hash = str(desired.get("desired_hash", "")).strip()
            resource_class = str(desired.get("resource_class", "service")).strip() or "service"
            observed = observed_state.get(resource_id)
            if observed is None:
                op_type: Literal["create", "update", "delete", "noop"] = "create"
            elif observed.observed_hash != desired_hash:
                op_type = "update"
            else:
                op_type = "noop"
            operations = (
                ReconcileOperation(
                    operation_id=f"{resource_id}:{op_type}:{desired_hash[:12]}",
                    operation_type=op_type,
                    target=resource_id,
                    payload={
                        "resource_id": resource_id,
                        "resource_class": resource_class,
                        "desired_hash": desired_hash,
                        "target": resource_id,
                        "intent": desired.get("intent"),
                    },
                ),
            )
            plans.append(
                ReconcilePlan(
                    resource_id=resource_id,
                    desired_hash=desired_hash,
                    operations=operations,
                )
            )
        observed_only = {
            observed.resource_id: observed
            for observed in self.provider.list_observed()
            if observed.resource_id not in desired_state
        }
        for resource_id, observed in sorted(observed_only.items()):
            assert observed is not None
            plans.append(
                ReconcilePlan(
                    resource_id=resource_id,
                    desired_hash="absent",
                    operations=(
                        ReconcileOperation(
                            operation_id=f"{resource_id}:delete:{observed.observed_hash[:12]}",
                            operation_type="delete",
                            target=resource_id,
                            payload={
                                "resource_id": resource_id,
                                "resource_class": observed.resource_class,
                                "desired_hash": "absent",
                                "target": resource_id,
                            },
                        ),
                    ),
                )
            )
        return tuple(plans)

    def reconcile(self, *, bundle: RuntimeBundle) -> tuple[ReconcileStatus, ...]:
        statuses, _ = self.reconcile_with_evidence(bundle=bundle)
        return statuses

    def reconcile_with_evidence(
        self,
        *,
        bundle: RuntimeBundle,
        resync_attempt: int = 1,
        resync_max_attempts: int = 1,
        resync_interval_ms: int = 0,
        resync_elapsed_wait_ms: int = 0,
    ) -> tuple[tuple[ReconcileStatus, ...], tuple[ReconcileEvidence, ...]]:
        if self.policy_runtime is not None:
            ensure_runtime_context_match(expected=self.policy_runtime.context, actual=bundle.context)
        plans = self.build_plan(bundle=bundle)
        if self.mode == "canary":
            strat = _resolve_canary_progressive_strategy(bundle.metadata, canary_limit=int(self.canary_limit))
            if strat is not None:
                return self._reconcile_with_evidence_progressive_canary(
                    bundle=bundle,
                    plans=plans,
                    resync_attempt=resync_attempt,
                    resync_max_attempts=resync_max_attempts,
                    resync_interval_ms=resync_interval_ms,
                    resync_elapsed_wait_ms=resync_elapsed_wait_ms,
                    strategy=strat,
                )
        statuses: list[ReconcileStatus] = []
        evidence_records: list[ReconcileEvidence] = []
        for index, plan in enumerate(plans):
            resource_class = self._resource_class_from_plan(plan)
            self._seed_desired_hash_history_from_observation_if_needed(
                resource_id=plan.resource_id,
                resource_class=resource_class,
                resync_elapsed_wait_ms=int(resync_elapsed_wait_ms),
            )
            rollback_target = self._rollback_target_hash(
                resource_id=plan.resource_id,
                current_desired_hash=plan.desired_hash,
            )
            applied_results: list[ProviderOperationResult] = []
            operations = self._operations_for_mode(plan=plan, index=index)
            policy_denied = not self._policy_allows(plan=plan)
            if policy_denied:
                operations = tuple(
                    ReconcileOperation(
                        operation_id=f"{operation.operation_id}:policy_denied",
                        operation_type="noop",
                        target=operation.target,
                        payload={**dict(operation.payload), "policy_denied": True},
                    )
                    for operation in operations
                )
            observe_only = self.provider_is_observe_only(self.provider)
            for operation in operations:
                simulate_op = self.mode == "simulate" or operation.operation_type == "noop" or observe_only
                if (
                    not simulate_op
                    and operation.operation_type in {"create", "update", "delete"}
                    and self.policy_runtime is not None
                ):
                    self._authorize_reconcile_operation(
                        action="service.reconcile.apply",
                        bundle=bundle,
                        operation=operation,
                    )
                if simulate_op:
                    observed = self.provider.observe(resource_id=plan.resource_id, resource_class=resource_class)
                    ev: dict[str, JSONValue] = {"mode": self.mode, "simulated": True}
                    if observe_only and self.mode in {"enforce", "canary"}:
                        ev["observe_only_provider"] = True
                        ev["mutations_skipped"] = "observe_only"
                    applied_results.append(
                        ProviderOperationResult(
                            operation=operation,
                            applied=False,
                            observed_hash=(observed.observed_hash if observed is not None else plan.desired_hash),
                            health_status=(observed.health_status if observed is not None else "unknown"),
                            error="policy denied" if policy_denied else None,
                            evidence=ev,
                        )
                    )
                    continue
                applied_results.append(self.provider.apply(operation=operation))
            status = self._evaluate_convergence(
                bundle=bundle,
                plan=plan,
                resource_class=resource_class,
                applied_results=tuple(applied_results),
                resync_elapsed_wait_ms=int(resync_elapsed_wait_ms),
            )
            rollback_triggered = False
            rollback_outcome: RollbackOutcome | None = None
            if (
                self.mode == "enforce"
                and not self.provider_is_observe_only(self.provider)
                and self.rollback_on_failure
                and not status.converged
                and rollback_target is not None
            ):
                if self.policy_runtime is not None:
                    self._authorize_reconcile_operation(
                        action="service.reconcile.rollback",
                        bundle=bundle,
                        operation=ReconcileOperation(
                            operation_id=f"{plan.resource_id}:rollback",
                            operation_type="update",
                            target=plan.resource_id,
                            payload={"resource_id": plan.resource_id, "desired_hash": rollback_target},
                        ),
                    )
                rollback_triggered = True
                rollback_result = self.provider.rollback(
                    resource_id=plan.resource_id,
                    resource_class=resource_class,
                    target_hash=rollback_target,
                )
                applied_results.append(rollback_result)
                rollback_outcome = self.classify_rollback_outcome(rollback_result)
                rb_err = status.last_error or "rolled back to previous desired hash"
                rb_health = rollback_result.health_status
                if rollback_outcome == "rollback_failed":
                    rb_health = "failed"
                    rb_err = rollback_result.error or "rollback_failed: provider reported rollback failure"
                elif rollback_outcome == "rollback_unsupported":
                    rb_err = rollback_result.error or "rollback_unsupported: provider cannot rollback to target hash"
                rb_conds = build_reconcile_conditions(
                    converged=False,
                    health_status=rb_health,
                    last_error=rb_err,
                    rollback_triggered=True,
                )
                gate_rb = resolve_reconcile_health_gate(bundle.metadata)
                grace_rb = resolve_reconcile_unknown_grace_ms(bundle.metadata)
                rb_hc: tuple[ObservedHealthCondition, ...] = ()
                status = ReconcileStatus(
                    resource_id=plan.resource_id,
                    observed_hash=rollback_target,
                    health_status=rb_health,
                    converged=False,
                    desired_hash=plan.desired_hash,
                    hash_matched=False,
                    health_gate_passed=health_gate_passes(
                        gate=gate_rb,
                        health_status=rb_health,
                        observed_conditions=rb_hc,
                        unknown_grace_ms=grace_rb,
                        resync_elapsed_wait_ms=int(resync_elapsed_wait_ms),
                    ),
                    last_error=rb_err,
                    conditions=rb_conds,
                    observed_health_conditions=rb_hc,
                    reconcile_health_gate=gate_rb,
                )
            self._record_history(plan=plan, status=status)
            self.operation_evidence.setdefault(plan.resource_id, []).extend(applied_results)
            statuses.append(status)
            evidence_records.append(
                ReconcileEvidence(
                    resource_id=plan.resource_id,
                    mode=self.mode,
                    operations=tuple(applied_results),
                    converged=status.converged,
                    health_status=status.health_status,
                    rollback_triggered=rollback_triggered,
                    rollback_target_hash=rollback_target if rollback_triggered else None,
                    rollback_outcome=rollback_outcome,
                    conditions=status.conditions,
                    resync_attempt=int(resync_attempt),
                    resync_max_attempts=int(resync_max_attempts),
                    resync_interval_ms=int(resync_interval_ms),
                )
            )
        return tuple(statuses), tuple(evidence_records)

    def _seed_desired_hash_history_from_observation_if_needed(
        self,
        *,
        resource_id: str,
        resource_class: str,
        resync_elapsed_wait_ms: int,
    ) -> None:
        """Seed rollback history from current provider observation when empty.

        Deterministic rollback requires a prior successful desired-hash chain.
        When no history exists yet, we conservatively seed from *healthy* observed state.
        """

        history = self.desired_hash_history.get(resource_id)
        if history:
            return
        observed = self.provider.observe(resource_id=resource_id, resource_class=resource_class)
        if observed is None:
            return
        if observed.health_status != "healthy":
            return
        observed_hash = str(observed.observed_hash).strip()
        if not observed_hash:
            return
        # Seed exactly one value: "latest known good". Record history only on convergence later.
        self.desired_hash_history[resource_id] = [observed_hash]
        _ = resync_elapsed_wait_ms  # kept for future strict seeding based on grace windows

    def _rollback_target_hash(self, *, resource_id: str, current_desired_hash: str) -> str | None:
        """Select deterministic rollback target from a successful desired-hash chain.

        The returned value is the most recent successful desired hash *different* from the
        current desired hash (so "rollback after success" targets the predecessor).
        """

        history = self.desired_hash_history.get(resource_id, [])
        if not history:
            return None
        for candidate in reversed(history):
            if candidate != current_desired_hash:
                return candidate
        return None

    def _reconcile_with_evidence_progressive_canary(
        self,
        *,
        bundle: RuntimeBundle,
        plans: tuple[ReconcilePlan, ...],
        resync_attempt: int,
        resync_max_attempts: int,
        resync_interval_ms: int,
        resync_elapsed_wait_ms: int,
        strategy: CanaryProgressiveStrategy,
    ) -> tuple[tuple[ReconcileStatus, ...], tuple[ReconcileEvidence, ...]]:
        total = len(plans)
        if total == 0:
            return (), ()

        weight_steps: tuple[float, ...]
        if not strategy.weight_steps:
            first_count = min(max(1, int(self.canary_limit)), total)
            weight_steps = (1.0,) if first_count >= total else (float(first_count) / float(total), 1.0)
        else:
            weight_steps = strategy.weight_steps

        # Convert step endpoints to monotonic counts and then to end indices.
        step_counts: list[int] = []
        prev = 0
        for raw_w in weight_steps:
            if raw_w <= 0:
                continue
            cnt = int(math.ceil(total * float(raw_w))) if raw_w <= 1.0 else int(raw_w)
            cnt = max(1, min(total, cnt))
            cnt = max(prev, cnt)
            prev = cnt
            step_counts.append(cnt)

        if not step_counts:
            step_counts = [total]
        if step_counts[-1] != total:
            step_counts.append(total)

        # Unique while preserving order.
        unique_step_counts: list[int] = []
        seen: set[int] = set()
        for c in step_counts:
            if c not in seen:
                unique_step_counts.append(c)
                seen.add(c)
        step_end_indices = [c - 1 for c in unique_step_counts if c > 0]

        if not step_end_indices:
            return (), ()

        pause_windows_ms = list(strategy.pause_windows_ms)
        if len(pause_windows_ms) < len(step_end_indices) - 1:
            pause_windows_ms.extend([0] * ((len(step_end_indices) - 1) - len(pause_windows_ms)))
        pause_windows_ms = pause_windows_ms[: max(0, len(step_end_indices) - 1)]

        def _hold_operations(*, plan: ReconcilePlan) -> tuple[ReconcileOperation, ...]:
            return tuple(
                ReconcileOperation(
                    operation_id=f"{operation.operation_id}:canary_hold",
                    operation_type="noop",
                    target=operation.target,
                    payload={**dict(operation.payload), "canary_hold": True},
                )
                for operation in plan.operations
            )

        def _classify_statuses(status_rows: Sequence[ReconcileStatus]) -> Literal["success", "failure", "inconclusive"]:
            abort_health = set(strategy.abort_on_health_status)
            inconclusive_health = set(strategy.inconclusive_on_health_status)
            if any(s.health_status in abort_health for s in status_rows):
                return "failure"
            # Inconclusive includes "unknown" plus non-converged resources.
            if any(s.health_status in inconclusive_health or not s.converged for s in status_rows):
                return "inconclusive"
            # Success is "converged and healthy".
            if any(s.health_status != "healthy" for s in status_rows):
                return "inconclusive"
            return "success"

        statuses: list[ReconcileStatus] = []
        evidence_records: list[ReconcileEvidence] = []
        applied_results_by_idx: list[list[ProviderOperationResult]] = []

        can_apply_until_idx = step_end_indices[0]
        progressive_stop = False  # stops promotion; can still evaluate held resources.
        canary_step_idx = 0
        for index, plan in enumerate(plans):
            resource_class = self._resource_class_from_plan(plan)
            can_apply = (not progressive_stop) and index <= can_apply_until_idx
            if can_apply:
                self._seed_desired_hash_history_from_observation_if_needed(
                    resource_id=plan.resource_id,
                    resource_class=resource_class,
                    resync_elapsed_wait_ms=int(resync_elapsed_wait_ms),
                )
            operations = plan.operations if can_apply else _hold_operations(plan=plan)
            policy_denied = not self._policy_allows(plan=plan)
            if policy_denied:
                operations = tuple(
                    ReconcileOperation(
                        operation_id=f"{operation.operation_id}:policy_denied",
                        operation_type="noop",
                        target=operation.target,
                        payload={**dict(operation.payload), "policy_denied": True},
                    )
                    for operation in operations
                )

            observe_only = self.provider_is_observe_only(self.provider)
            applied_results: list[ProviderOperationResult] = []
            for operation in operations:
                simulate_op = self.mode == "simulate" or operation.operation_type == "noop" or observe_only
                if (
                    not simulate_op
                    and operation.operation_type in {"create", "update", "delete"}
                    and self.policy_runtime is not None
                ):
                    self._authorize_reconcile_operation(
                        action="service.reconcile.apply",
                        bundle=bundle,
                        operation=operation,
                    )
                if simulate_op:
                    observed = self.provider.observe(resource_id=plan.resource_id, resource_class=resource_class)
                    ev: dict[str, JSONValue] = {"mode": self.mode, "simulated": True}
                    if observe_only and self.mode in {"enforce", "canary"}:
                        ev["observe_only_provider"] = True
                        ev["mutations_skipped"] = "observe_only"
                    applied_results.append(
                        ProviderOperationResult(
                            operation=operation,
                            applied=False,
                            observed_hash=(observed.observed_hash if observed is not None else plan.desired_hash),
                            health_status=(observed.health_status if observed is not None else "unknown"),
                            error="policy denied" if policy_denied else None,
                            evidence=ev,
                        )
                    )
                    continue
                applied_results.append(self.provider.apply(operation=operation))

            status = self._evaluate_convergence(
                bundle=bundle,
                plan=plan,
                resource_class=resource_class,
                applied_results=tuple(applied_results),
                resync_elapsed_wait_ms=int(resync_elapsed_wait_ms),
            )

            rollback_triggered = False
            rollback_target_hash = None
            rollback_outcome: RollbackOutcome | None = None
            self._record_history(plan=plan, status=status)
            self.operation_evidence.setdefault(plan.resource_id, []).extend(applied_results)
            statuses.append(status)
            evidence_records.append(
                ReconcileEvidence(
                    resource_id=plan.resource_id,
                    mode=self.mode,
                    operations=tuple(applied_results),
                    converged=status.converged,
                    health_status=status.health_status,
                    rollback_triggered=rollback_triggered,
                    rollback_target_hash=rollback_target_hash,
                    rollback_outcome=rollback_outcome,
                    conditions=status.conditions,
                    resync_attempt=int(resync_attempt),
                    resync_max_attempts=int(resync_max_attempts),
                    resync_interval_ms=int(resync_interval_ms),
                )
            )
            applied_results_by_idx.append(applied_results)

            # Step analysis after the step end index is processed.
            if progressive_stop:
                continue
            if index != can_apply_until_idx:
                continue

            status_rows_for_step = statuses[: index + 1]
            classification = _classify_statuses(status_rows_for_step)
            if classification == "success":
                if canary_step_idx < len(step_end_indices) - 1:
                    pause_ms = pause_windows_ms[canary_step_idx] if canary_step_idx < len(pause_windows_ms) else 0
                    if pause_ms > 0:
                        time.sleep(pause_ms / 1000.0)
                    canary_step_idx += 1
                    can_apply_until_idx = step_end_indices[canary_step_idx]
                continue

            # Failure: abort promotion and rollback canary resources back to prior success hash.
            if classification == "failure":
                progressive_stop = True
                step_end_idx = index
                for idx in range(0, step_end_idx + 1):
                    prior_status = statuses[idx]
                    if prior_status.converged:
                        # Only roll back non-converged resources to keep the rollback target deterministic
                        # (history predecessor vs. the current successful desired-hash).
                        continue
                    rollback_target = self._rollback_target_hash(
                        resource_id=plans[idx].resource_id,
                        current_desired_hash=plans[idx].desired_hash,
                    )
                    if rollback_target is None:
                        continue
                    rc_class = self._resource_class_from_plan(plans[idx])
                    if self.policy_runtime is not None:
                        self._authorize_reconcile_operation(
                            action="service.reconcile.rollback",
                            bundle=bundle,
                            operation=ReconcileOperation(
                                operation_id=f"{plans[idx].resource_id}:rollback",
                                operation_type="update",
                                target=plans[idx].resource_id,
                                payload={"resource_id": plans[idx].resource_id, "desired_hash": rollback_target},
                            ),
                        )
                    rollback_result = self.provider.rollback(
                        resource_id=plans[idx].resource_id,
                        resource_class=rc_class,
                        target_hash=rollback_target,
                    )
                    rollback_outcome = self.classify_rollback_outcome(rollback_result)
                    applied_results_by_idx[idx].append(rollback_result)
                    self.operation_evidence.setdefault(plans[idx].resource_id, []).append(rollback_result)

                    rb_err = prior_status.last_error or "rolled back to previous desired hash"
                    rb_health = rollback_result.health_status
                    if rollback_outcome == "rollback_failed":
                        rb_health = "failed"
                        rb_err = rollback_result.error or "rollback_failed: provider reported rollback failure"
                    elif rollback_outcome == "rollback_unsupported":
                        rb_err = (
                            rollback_result.error or "rollback_unsupported: provider cannot rollback to target hash"
                        )
                    rb_conds = build_reconcile_conditions(
                        converged=False,
                        health_status=rb_health,
                        last_error=rb_err,
                        rollback_triggered=True,
                    )
                    gate_rb = resolve_reconcile_health_gate(bundle.metadata)
                    grace_rb = resolve_reconcile_unknown_grace_ms(bundle.metadata)
                    rb_hc: tuple[ObservedHealthCondition, ...] = ()
                    new_status = ReconcileStatus(
                        resource_id=plans[idx].resource_id,
                        observed_hash=rollback_target,
                        health_status=rb_health,
                        converged=False,
                        desired_hash=plans[idx].desired_hash,
                        hash_matched=False,
                        health_gate_passed=health_gate_passes(
                            gate=gate_rb,
                            health_status=rb_health,
                            observed_conditions=rb_hc,
                            unknown_grace_ms=grace_rb,
                            resync_elapsed_wait_ms=int(resync_elapsed_wait_ms),
                        ),
                        last_error=rb_err,
                        conditions=rb_conds,
                        observed_health_conditions=rb_hc,
                        reconcile_health_gate=gate_rb,
                    )
                    statuses[idx] = new_status
                    evidence_records[idx] = ReconcileEvidence(
                        resource_id=plans[idx].resource_id,
                        mode=self.mode,
                        operations=tuple(applied_results_by_idx[idx]),
                        converged=False,
                        health_status=new_status.health_status,
                        rollback_triggered=True,
                        rollback_target_hash=rollback_target,
                        rollback_outcome=rollback_outcome,
                        conditions=new_status.conditions,
                        resync_attempt=int(resync_attempt),
                        resync_max_attempts=int(resync_max_attempts),
                        resync_interval_ms=int(resync_interval_ms),
                    )
                continue

            # Inconclusive: abort promotion without rollback.
            progressive_stop = True
            continue

        return tuple(statuses), tuple(evidence_records)

    def _operations_for_mode(self, *, plan: ReconcilePlan, index: int) -> tuple[ReconcileOperation, ...]:
        if self.mode != "canary":
            return plan.operations
        if index >= int(self.canary_limit):
            return tuple(
                ReconcileOperation(
                    operation_id=f"{operation.operation_id}:canary_hold",
                    operation_type="noop",
                    target=operation.target,
                    payload={**dict(operation.payload), "canary_hold": True},
                )
                for operation in plan.operations
            )
        return plan.operations

    def _policy_allows(self, *, plan: ReconcilePlan) -> bool:
        _ = plan
        return self.mode in {"simulate", "enforce", "canary"}

    def _resource_class_from_plan(self, plan: ReconcilePlan) -> str:
        if not plan.operations:
            return "service"
        payload = plan.operations[0].payload
        return str(payload.get("resource_class", "service")).strip() or "service"

    def _evaluate_convergence(
        self,
        *,
        bundle: RuntimeBundle,
        plan: ReconcilePlan,
        resource_class: str,
        applied_results: tuple[ProviderOperationResult, ...],
        resync_elapsed_wait_ms: int,
    ) -> ReconcileStatus:
        observed = self.provider.observe(resource_id=plan.resource_id, resource_class=resource_class)
        observed_hash = observed.observed_hash if observed is not None else "absent"
        health_status = observed.health_status if observed is not None else "unknown"
        observed_hc = observed.health_conditions if observed is not None else ()
        gate = resolve_reconcile_health_gate(bundle.metadata)
        grace_ms = resolve_reconcile_unknown_grace_ms(bundle.metadata)
        hash_matched = observed_hash == plan.desired_hash or (plan.desired_hash == "absent" and observed is None)
        absent_satisfied = plan.desired_hash == "absent" and observed is None
        health_gate_passed = bool(
            absent_satisfied
            or health_gate_passes(
                gate=gate,
                health_status=health_status,
                observed_conditions=observed_hc,
                unknown_grace_ms=grace_ms,
                resync_elapsed_wait_ms=int(resync_elapsed_wait_ms),
            )
        )
        last_error = next(
            (result.error for result in reversed(applied_results) if result.error is not None and result.error.strip()),
            None,
        )
        if not hash_matched and last_error is None:
            last_error = "desired state not converged within current observation window"
        elif hash_matched and not health_gate_passed and last_error is None:
            if gate == "strict" and health_status == "unknown":
                last_error = (
                    "hash matched but strict health gate requires healthy, Ready=true, or unknown within grace window"
                )
            else:
                last_error = "hash matched but health probe reported degraded or failed state"
        converged_effective = hash_matched and health_gate_passed
        conds = build_reconcile_conditions(
            converged=converged_effective,
            health_status=health_status,
            last_error=last_error,
            rollback_triggered=False,
        )
        return ReconcileStatus(
            resource_id=plan.resource_id,
            observed_hash=observed_hash,
            health_status=health_status,
            converged=converged_effective,
            desired_hash=plan.desired_hash,
            hash_matched=hash_matched,
            health_gate_passed=health_gate_passed,
            last_error=last_error,
            conditions=conds,
            observed_health_conditions=observed_hc,
            reconcile_health_gate=gate,
        )

    def _record_history(self, *, plan: ReconcilePlan, status: ReconcileStatus) -> None:
        if plan.desired_hash == "absent":
            return
        if not status.converged:
            return
        history = self.desired_hash_history.setdefault(plan.resource_id, [])
        if not history or history[-1] != plan.desired_hash:
            history.append(plan.desired_hash)
        if len(history) > 16:
            del history[:-16]

    def _previous_desired_hash(self, resource_id: str) -> str | None:
        history = self.desired_hash_history.get(resource_id, [])
        if len(history) < 1:
            return None
        return history[-1]

    def _authorize_reconcile_operation(
        self, *, action: str, bundle: RuntimeBundle, operation: ReconcileOperation
    ) -> None:
        if self.policy_runtime is None:
            return
        decision = self.policy_runtime.authorize(
            action=action,
            context=bundle.context,
            extra_context={
                "resource_id": str(operation.payload.get("resource_id", operation.target)),
                "operation_type": operation.operation_type,
            },
        )
        if bool(decision.block):
            raise PermissionError(f"runtime policy blocked action={action!r} reason={decision.reason!r}")
