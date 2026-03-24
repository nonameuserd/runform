from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal, cast

from akc.control.policy import (
    CapabilityIssuer,
    DefaultDenyPolicyEngine,
    PolicyDecision,
    PolicyEngine,
    ToolAuthorizationPolicy,
    ToolAuthorizationRequest,
)
from akc.ir.schema import IRDocument
from akc.memory.models import JSONValue, require_non_empty
from akc.runtime.models import RuntimeContext, RuntimeEvent

if TYPE_CHECKING:
    from akc.compile.interfaces import TenantRepoScope
else:

    @dataclass(frozen=True, slots=True)
    class TenantRepoScope:
        tenant_id: str
        repo_id: str


RUNTIME_POLICY_ACTIONS: tuple[str, ...] = (
    "runtime.event.consume",
    "runtime.action.dispatch",
    "runtime.action.retry",
    "runtime.action.execute.subprocess",
    "runtime.action.execute.http",
    "runtime.state.checkpoint.write",
    "service.reconcile.apply",
    "service.reconcile.rollback",
)


class RuntimeScopeMismatchError(ValueError):
    pass


ReconcileDesiredStateSource = Literal["ir", "deployment_intents"]


def intent_bundle_has_acceptance_criteria(
    *, intent_ref: Mapping[str, Any], intent_projection: Mapping[str, Any]
) -> bool:
    """True when the bundle carries a correlated intent identity plus acceptance/success-criteria signals."""
    if not str(intent_ref.get("intent_id", "")).strip():
        return False
    if not str(intent_ref.get("stable_intent_sha256", "")).strip():
        return False
    summary = intent_projection.get("success_criteria_summary")
    if not isinstance(summary, Mapping):
        return False
    modes = summary.get("evaluation_modes")
    if isinstance(modes, Sequence) and not isinstance(modes, (str, bytes)) and any(str(m).strip() for m in modes):
        return True
    count = summary.get("count")
    if isinstance(count, int) and not isinstance(count, bool) and count > 0:
        return True
    criteria = summary.get("criteria")
    return bool(isinstance(criteria, Sequence) and not isinstance(criteria, (str, bytes)) and len(criteria) > 0)


def _payload_declares_system_ir_handoff(payload: Mapping[str, Any]) -> bool:
    embedded = payload.get("system_ir")
    if isinstance(embedded, Mapping) and embedded:
        return True
    ref = payload.get("system_ir_ref")
    return isinstance(ref, Mapping) and bool(str(ref.get("path", "")).strip())


def resolve_reconcile_desired_state_source(
    *,
    ir_document: IRDocument | None,
    payload: Mapping[str, Any],
) -> ReconcileDesiredStateSource:
    """Pick reconciler desired-state lane.

    - Declared ``ir`` requires a loaded :class:`~akc.ir.schema.IRDocument`.
    - Declared ``deployment_intents`` skips IR even when a ref is present.
    - When undeclared, a loaded IR wins; otherwise a ``system_ir`` / ``system_ir_ref`` handoff without a load
      raises (do not silently use ``deployment_intents`` when the bundle implied IR authority).
    """
    declared = str(payload.get("reconcile_desired_state_source", "")).strip().lower()
    if declared == "ir":
        if ir_document is None:
            raise ValueError(
                "runtime bundle declares reconcile_desired_state_source=ir but system IR could not be loaded"
            )
        return "ir"
    if declared == "deployment_intents":
        return "deployment_intents"
    if ir_document is not None:
        return "ir"
    if _payload_declares_system_ir_handoff(payload):
        raise ValueError(
            "runtime bundle references system IR (system_ir_ref or system_ir) but IR could not be loaded; "
            "fix the path or embed system_ir, or set reconcile_desired_state_source to deployment_intents "
            "to reconcile from deployment_intents only"
        )
    return "deployment_intents"


def derive_runtime_evidence_expectations(
    *,
    projection: Mapping[str, Any],
    policy_envelope: Mapping[str, Any],
) -> tuple[str, ...]:
    """Compile-time and runtime bundle metadata use the same derivation as :class:`RuntimePolicyRuntime`."""
    return _derive_evidence_expectations(projection=projection, policy_envelope=policy_envelope)


def merge_runtime_evidence_expectations(
    *,
    derived: tuple[str, ...],
    bundle_declared: Sequence[str] | None,
    allow_widen: bool = False,
) -> tuple[str, ...]:
    """Merge bundle-stamped expectations with projection/envelope-derived expectations.

    When ``allow_widen`` is false (default), bundle-declared tokens cannot add requirements beyond
    ``derived`` (intent + envelope derivation). This prevents a tampered bundle from demanding
    extra evidence types unless ``runtime_policy_envelope.allow_runtime_evidence_expectation_widening``
    is set for the same tenant/repo scope.
    """

    derived_set = {str(x).strip() for x in derived if str(x).strip()}
    extras: set[str] = set()
    raw = bundle_declared
    if isinstance(raw, Sequence) and not isinstance(raw, (str, bytes)):
        for item in raw:
            token = str(item).strip()
            if token:
                extras.add(token)
    if allow_widen:
        return tuple(sorted(derived_set | extras))
    return tuple(sorted(derived_set | (extras & derived_set)))


def runtime_evidence_expectation_violations(
    *,
    expectations: Sequence[str],
    evidence_types_present: set[str],
) -> tuple[str, ...]:
    """Return expectation tokens that are not satisfied by the post-run evidence stream."""
    missing: list[str] = []
    for raw in expectations:
        token = str(raw).strip()
        if not token:
            continue
        if _evidence_expectation_satisfied(token=token, evidence_types_present=evidence_types_present):
            continue
        missing.append(token)
    return tuple(missing)


def _evidence_expectation_satisfied(*, token: str, evidence_types_present: set[str]) -> bool:
    if token == "reconciler.health_check":
        return bool(
            evidence_types_present
            & {"terminal_health", "reconcile_outcome", "reconcile_resource_status", "rollback_chain"}
        )
    if token == "metric_threshold":
        if not _evidence_expectation_satisfied(
            token="reconciler.health_check", evidence_types_present=evidence_types_present
        ):
            return False
        return bool(evidence_types_present & {"reconcile_outcome", "transition_application"})
    if token == "operational_spec":
        if not _evidence_expectation_satisfied(
            token="reconciler.health_check", evidence_types_present=evidence_types_present
        ):
            return False
        return bool(evidence_types_present & {"reconcile_outcome", "transition_application"})
    return token in evidence_types_present


def runtime_scope(context: RuntimeContext) -> TenantRepoScope:
    require_runtime_context(context)
    return TenantRepoScope(tenant_id=context.tenant_id, repo_id=context.repo_id)


def require_runtime_context(context: RuntimeContext) -> None:
    require_non_empty(context.tenant_id, name="runtime_context.tenant_id")
    require_non_empty(context.repo_id, name="runtime_context.repo_id")
    require_non_empty(context.run_id, name="runtime_context.run_id")
    require_non_empty(context.runtime_run_id, name="runtime_context.runtime_run_id")


def ensure_runtime_context_match(*, expected: RuntimeContext, actual: RuntimeContext) -> None:
    require_runtime_context(expected)
    require_runtime_context(actual)
    if (
        expected.tenant_id != actual.tenant_id
        or expected.repo_id != actual.repo_id
        or expected.run_id != actual.run_id
        or expected.runtime_run_id != actual.runtime_run_id
    ):
        raise RuntimeScopeMismatchError("runtime context scope mismatch")


def ensure_event_scope(*, expected: RuntimeContext, event: RuntimeEvent) -> None:
    ensure_runtime_context_match(expected=expected, actual=event.context)


@dataclass(frozen=True, slots=True)
class ScopedRuntimeEnvironment:
    working_directory: str
    secret_keys: tuple[str, ...]


def derive_scoped_runtime_environment(
    *, context: RuntimeContext, policy_envelope: Mapping[str, Any] | None
) -> ScopedRuntimeEnvironment:
    require_runtime_context(context)
    envelope = dict(policy_envelope or {})
    workdir = str(
        envelope.get(
            "scoped_workdir",
            f".akc/runtime/{context.tenant_id}/{context.repo_id}/{context.run_id}/{context.runtime_run_id}",
        )
    ).strip()
    if not workdir:
        raise ValueError("runtime scoped working directory must be non-empty")
    secret_values = envelope.get("scoped_secret_keys", ())
    if isinstance(secret_values, Sequence) and not isinstance(secret_values, (str, bytes)):
        secret_keys = tuple(str(item).strip() for item in secret_values if str(item).strip())
    else:
        secret_keys = ()
    return ScopedRuntimeEnvironment(working_directory=workdir, secret_keys=secret_keys)


@dataclass(slots=True)
class RuntimePolicyRuntime:
    context: RuntimeContext
    policy_engine: PolicyEngine
    issuer: CapabilityIssuer
    decision_log: list[dict[str, JSONValue]]
    effective_allow_actions: tuple[str, ...] = ()
    effective_deny_actions: tuple[str, ...] = ()
    unresolved_policy_ids: tuple[str, ...] = ()
    evidence_expectations: tuple[str, ...] = ()

    @classmethod
    def default(
        cls,
        *,
        context: RuntimeContext,
        policy_mode: str = "enforce",
        allow_actions: Sequence[str] = RUNTIME_POLICY_ACTIONS,
    ) -> RuntimePolicyRuntime:
        resolved_actions = tuple(str(action).strip() for action in allow_actions if str(action).strip())
        engine = DefaultDenyPolicyEngine(
            issuer=CapabilityIssuer(),
            policy=ToolAuthorizationPolicy(
                mode=cast(
                    Literal["audit_only", "enforce"],
                    policy_mode if policy_mode in ("audit_only", "enforce") else "enforce",
                ),
                allow_actions=resolved_actions,
            ),
        )
        return cls(
            context=context,
            policy_engine=engine,
            issuer=engine.issuer,
            decision_log=[],
            effective_allow_actions=resolved_actions,
            effective_deny_actions=(),
            unresolved_policy_ids=(),
            evidence_expectations=(),
        )

    @classmethod
    def from_bundle(
        cls,
        *,
        context: RuntimeContext,
        policy_envelope: Mapping[str, Any] | None = None,
        intent_projection: Mapping[str, Any] | None = None,
        ir_document: IRDocument | None = None,
        bundle_evidence_expectations: Sequence[str] | None = None,
    ) -> RuntimePolicyRuntime:
        envelope = dict(policy_envelope or {})
        projection = dict(intent_projection or {})
        allow_actions = set(RUNTIME_POLICY_ACTIONS)
        deny_actions: set[str] = set()

        explicit_allow_actions = _coerce_action_set(
            envelope.get("allow_actions"),
            envelope.get("allow_runtime_actions"),
        )
        if explicit_allow_actions:
            allow_actions &= explicit_allow_actions
        deny_actions |= _coerce_action_set(
            envelope.get("deny_actions"),
            envelope.get("deny_runtime_actions"),
        )

        unresolved_policy_ids: list[str] = []
        for raw_policy in _projection_policies(projection):
            matched = _apply_intent_policy_to_runtime_actions(
                raw_policy=raw_policy,
                allow_actions=allow_actions,
                deny_actions=deny_actions,
            )
            if not matched:
                policy_id = str(raw_policy.get("id", "")).strip()
                if policy_id:
                    unresolved_policy_ids.append(policy_id)

        _apply_ir_policy_nodes_to_runtime_actions(
            ir_document=ir_document,
            allow_actions=allow_actions,
            deny_actions=deny_actions,
        )

        if bool(envelope.get("knowledge_network_egress_forbidden", False)):
            deny_actions |= {
                "service.reconcile.apply",
                "service.reconcile.rollback",
                "runtime.action.execute.http",
            }

        deny_actions |= _coerce_action_set(envelope.get("knowledge_derived_deny_actions"))

        allow_actions -= deny_actions
        mode = _runtime_policy_mode(context=context)
        if unresolved_policy_ids and mode == "enforce":
            allow_actions.clear()

        resolved_allow_actions = tuple(sorted(allow_actions))
        resolved_deny_actions = tuple(sorted(deny_actions))
        derived_expectations = _derive_evidence_expectations(
            projection=projection,
            policy_envelope=envelope,
        )
        allow_widen = bool(envelope.get("allow_runtime_evidence_expectation_widening", False))
        envelope_expectations = envelope.get("runtime_evidence_expectations")
        evidence_expectations = merge_runtime_evidence_expectations(
            derived=merge_runtime_evidence_expectations(
                derived=derived_expectations,
                bundle_declared=bundle_evidence_expectations,
                allow_widen=allow_widen,
            ),
            bundle_declared=(
                envelope_expectations
                if isinstance(envelope_expectations, Sequence) and not isinstance(envelope_expectations, (str, bytes))
                else None
            ),
            allow_widen=allow_widen,
        )

        engine = DefaultDenyPolicyEngine(
            issuer=CapabilityIssuer(),
            policy=ToolAuthorizationPolicy(
                mode=mode,
                allow_actions=resolved_allow_actions,
            ),
        )
        return cls(
            context=context,
            policy_engine=engine,
            issuer=engine.issuer,
            decision_log=[],
            effective_allow_actions=resolved_allow_actions,
            effective_deny_actions=resolved_deny_actions,
            unresolved_policy_ids=tuple(sorted(set(unresolved_policy_ids))),
            evidence_expectations=evidence_expectations,
        )

    def authorize(
        self,
        *,
        action: str,
        context: RuntimeContext,
        extra_context: Mapping[str, JSONValue] | None = None,
    ) -> PolicyDecision:
        require_non_empty(action, name="runtime_policy.action")
        ensure_runtime_context_match(expected=self.context, actual=context)
        scope = runtime_scope(context)
        token = self.issuer.issue(
            scope=scope,
            action=action,
            constraints={
                "run_id": context.run_id,
                "runtime_run_id": context.runtime_run_id,
            },
        )
        decision = self.policy_engine.authorize(
            req=ToolAuthorizationRequest(
                scope=scope,
                action=action,
                capability=token,
                context=dict(extra_context or {}),
            )
        )
        self.decision_log.append(
            {
                "action": action,
                "token_id": token.token_id,
                "tenant_id": context.tenant_id,
                "repo_id": context.repo_id,
                "run_id": context.run_id,
                "runtime_run_id": context.runtime_run_id,
                "context": dict(extra_context or {}),
                "allowed": bool(decision.allowed),
                "reason": decision.reason,
                "mode": decision.mode,
            }
        )
        return decision


def _runtime_policy_mode(*, context: RuntimeContext) -> Literal["audit_only", "enforce"]:
    if context.policy_mode in {"simulate", "dry_run", "canary"}:
        return "audit_only"
    return "enforce"


def _coerce_action_set(*values: Any) -> set[str]:
    actions: set[str] = set()
    for value in values:
        if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
            continue
        for item in value:
            action = str(item).strip()
            if action:
                actions.add(action)
    return actions


def _projection_policies(projection: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    raw = projection.get("policies")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    return tuple(item for item in raw if isinstance(item, Mapping))


def _apply_ir_policy_nodes_to_runtime_actions(
    *,
    ir_document: IRDocument | None,
    allow_actions: set[str],
    deny_actions: set[str],
) -> None:
    """Narrow runtime actions using structured fields on IR ``policy`` nodes when present."""
    if ir_document is None:
        return
    for node in ir_document.nodes:
        if node.kind != "policy":
            continue
        props = dict(node.properties)
        meta: dict[str, Any] | None = None
        raw_meta = props.get("metadata")
        if isinstance(raw_meta, Mapping):
            meta = dict(raw_meta)
        else:
            direct_keys = (
                "runtime_allow_actions",
                "runtime_deny_actions",
                "allow_runtime_actions",
                "deny_runtime_actions",
            )
            if any(k in props for k in direct_keys):
                meta = {k: props[k] for k in direct_keys if k in props}
        if not meta:
            continue
        raw_policy = {
            "id": str(props.get("policy_id") or node.id),
            "requirement": str(props.get("requirement", "")),
            "metadata": meta,
        }
        _apply_intent_policy_to_runtime_actions(
            raw_policy=raw_policy,
            allow_actions=allow_actions,
            deny_actions=deny_actions,
        )


def _apply_intent_policy_to_runtime_actions(
    *,
    raw_policy: Mapping[str, Any],
    allow_actions: set[str],
    deny_actions: set[str],
) -> bool:
    metadata = raw_policy.get("metadata")
    policy_id = str(raw_policy.get("id", "")).strip().lower()
    requirement = str(raw_policy.get("requirement", "")).strip().lower()

    matched = False
    if isinstance(metadata, Mapping):
        metadata_allow = _coerce_action_set(
            metadata.get("runtime_allow_actions"),
            metadata.get("allow_runtime_actions"),
        )
        if metadata_allow:
            allow_actions &= metadata_allow
            matched = True
        metadata_deny = _coerce_action_set(
            metadata.get("runtime_deny_actions"),
            metadata.get("deny_runtime_actions"),
        )
        if metadata_deny:
            deny_actions |= metadata_deny
            matched = True

    builtin_allow, builtin_deny = _builtin_runtime_policy_actions(
        policy_id=policy_id,
        requirement=requirement,
    )
    if builtin_allow:
        allow_actions &= builtin_allow
        matched = True
    if builtin_deny:
        deny_actions |= builtin_deny
        matched = True
    return matched


def _builtin_runtime_policy_actions(*, policy_id: str, requirement: str) -> tuple[set[str], set[str]]:
    policy_text = " ".join(part for part in (policy_id, requirement) if part).strip()
    if not policy_text:
        return set(), set()

    if "checkpoint" in policy_text:
        return {"runtime.state.checkpoint.write"}, set()
    if "retry" in policy_text:
        return {"runtime.action.retry"}, set()
    if "dispatch" in policy_text:
        return {"runtime.action.dispatch"}, set()
    if "event" in policy_text:
        return {"runtime.event.consume"}, set()
    if "reconcile" in policy_text or "deploy" in policy_text:
        return {"service.reconcile.apply", "service.reconcile.rollback"}, set()
    if "network" in policy_text or "egress" in policy_text:
        return set(), {
            "service.reconcile.apply",
            "service.reconcile.rollback",
            "runtime.action.execute.http",
        }
    return set(), set()


def _derive_evidence_expectations(
    *,
    projection: Mapping[str, Any],
    policy_envelope: Mapping[str, Any],
) -> tuple[str, ...]:
    expectations = set()
    raw_summary = projection.get("success_criteria_summary")
    if isinstance(raw_summary, Mapping):
        modes = raw_summary.get("evaluation_modes")
        if isinstance(modes, Sequence) and not isinstance(modes, (str, bytes)):
            normalized_modes = {str(mode).strip() for mode in modes if str(mode).strip()}
            if "metric_threshold" in normalized_modes:
                expectations.add("metric_threshold")
                expectations.add("reconciler.health_check")
            if "operational_spec" in normalized_modes:
                expectations.add("operational_spec")
                expectations.add("reconciler.health_check")
    if bool(policy_envelope.get("require_reconcile_evidence")):
        expectations.add("reconciler.health_check")
    return tuple(sorted(expectations))
