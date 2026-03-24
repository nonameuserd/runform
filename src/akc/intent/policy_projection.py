from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from akc.compile.controller_config import Budget
from akc.intent.models import (
    IntentSpec,
    OperatingBound,
    OperationalValidityParamsError,
    intent_goal_text_fingerprint,
    intent_semantic_fingerprint,
    parse_operational_validity_params,
    stable_intent_sha256,
)
from akc.memory.models import JSONValue, json_value_as_float, json_value_as_int

# Mirrors ``akc.intent.models`` operational_signal stub validation (opaque id for operator-side binding).
_OTEL_QUERY_STUB_PATTERN = re.compile(r"^[-A-Za-z0-9_.]{1,128}$")


@dataclass(frozen=True, slots=True)
class OperatingBoundsPolicyProjection:
    """Project/normalize intent operating bounds into authorization-ready context.

    The projection enforces "no widening" relative to:
    - controller runtime budgets (where configured)
    - hard executor/network defaults (expressed as `hard_allow_network`)

    It also records every narrowing decision for auditability.
    """

    requested: dict[str, JSONValue]
    baseline: dict[str, JSONValue]
    effective: dict[str, JSONValue]
    narrowing_decisions: tuple[dict[str, JSONValue], ...]

    def to_policy_context(self) -> dict[str, JSONValue]:
        """Return a JSON-serializable policy context mapping."""
        return {
            "intent_operating_bounds_requested": dict(self.requested),
            "intent_operating_bounds_baseline": dict(self.baseline),
            "intent_operating_bounds_effective": dict(self.effective),
        }

    def to_audit_artifact(self) -> dict[str, JSONValue]:
        """Return a JSON-serializable audit record."""
        return {
            "requested": dict(self.requested),
            "baseline": dict(self.baseline),
            "effective": dict(self.effective),
            "narrowing_decisions": [dict(d) for d in self.narrowing_decisions],
        }


@dataclass(frozen=True, slots=True)
class RuntimeIntentProjection:
    intent_id: str
    spec_version: int
    intent_semantic_fingerprint: str
    intent_goal_text_fingerprint: str
    stable_intent_sha256: str
    operating_bounds_effective: dict[str, JSONValue]
    policies: tuple[dict[str, JSONValue], ...]
    success_criteria_summary: dict[str, JSONValue] | None = None

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "intent_id": self.intent_id,
            "spec_version": int(self.spec_version),
            "intent_semantic_fingerprint": self.intent_semantic_fingerprint,
            "intent_goal_text_fingerprint": self.intent_goal_text_fingerprint,
            "stable_intent_sha256": self.stable_intent_sha256,
            "operating_bounds_effective": dict(self.operating_bounds_effective),
            "policies": [dict(policy) for policy in self.policies],
            "success_criteria_summary": (
                dict(self.success_criteria_summary) if self.success_criteria_summary is not None else None
            ),
        }
        return {key: value for key, value in obj.items() if value is not None}


@dataclass(frozen=True, slots=True)
class DeploymentIntentProjection:
    intent_id: str
    spec_version: int
    intent_semantic_fingerprint: str
    intent_goal_text_fingerprint: str
    stable_intent_sha256: str
    constraint_ids: tuple[str, ...]
    policy_ids: tuple[str, ...]
    success_criteria_modes: tuple[str, ...]
    trace_tags: tuple[str, ...]

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "intent_id": self.intent_id,
            "spec_version": int(self.spec_version),
            "intent_semantic_fingerprint": self.intent_semantic_fingerprint,
            "intent_goal_text_fingerprint": self.intent_goal_text_fingerprint,
            "stable_intent_sha256": self.stable_intent_sha256,
            "constraint_ids": list(self.constraint_ids),
            "policy_ids": list(self.policy_ids),
            "success_criteria_modes": list(self.success_criteria_modes),
            "trace_tags": list(self.trace_tags),
        }


def _json_mapping_or_empty(value: Mapping[str, JSONValue] | None) -> dict[str, JSONValue]:
    if value is None:
        return {}
    return {str(key): item for key, item in value.items()}


def _sorted_unique_strings(values: list[str]) -> tuple[str, ...]:
    return tuple(sorted({value for value in values if value.strip()}))


def _observability_slice_from_success_criteria(*, intent: IntentSpec) -> dict[str, JSONValue] | None:
    """Collect opaque OTEL/metric binding stubs from success criteria for runtime export tags."""

    stubs: list[str] = []
    tags: list[str] = []
    for criterion in intent.normalized().success_criteria:
        if criterion.evaluation_mode == "operational_spec":
            try:
                parsed = parse_operational_validity_params(
                    dict(criterion.params) if criterion.params is not None else None
                )
            except OperationalValidityParamsError:
                parsed = None
            if parsed is not None:
                for sig in parsed.signals:
                    if sig.otel_query_stub:
                        stubs.append(sig.otel_query_stub)
                        tags.append(f"intent.oteld_stub:{sig.otel_query_stub}")
        elif criterion.evaluation_mode == "metric_threshold" and isinstance(criterion.params, Mapping):
            raw_stub = criterion.params.get("otel_query_stub")
            if isinstance(raw_stub, str):
                stub = raw_stub.strip()
                if stub and _OTEL_QUERY_STUB_PATTERN.fullmatch(stub) is not None:
                    stubs.append(stub)
                    tags.append(f"intent.metric_oteld_stub:{stub}")
    if not stubs and not tags:
        return None
    return {
        "otel_query_stubs": list(_sorted_unique_strings(stubs)),
        "intent_trace_tags": list(_sorted_unique_strings(tags)),
    }


def build_handoff_intent_ref(*, intent: IntentSpec) -> dict[str, JSONValue]:
    """Correlation block shared by ``runtime_bundle.intent_ref`` and deployment handoff.

    This is the canonical identity lane for compile outputs: normalized intent bytes
    (``stable_intent_sha256``) plus semantic/goal fingerprints. It must not be derived
    from IR node ids, plan-step blobs, or deployment-specific projections alone.
    """

    normalized_intent = intent.normalized()
    return {
        "intent_id": normalized_intent.intent_id,
        "stable_intent_sha256": stable_intent_sha256(intent=normalized_intent),
        "semantic_fingerprint": intent_semantic_fingerprint(intent=normalized_intent),
        "goal_text_fingerprint": intent_goal_text_fingerprint(goal_statement=normalized_intent.goal_statement),
    }


def project_runtime_intent_projection(
    *,
    intent: IntentSpec,
    operating_bounds_effective: Mapping[str, JSONValue] | None = None,
    include_success_criteria_summary: bool = True,
) -> RuntimeIntentProjection:
    normalized_intent = intent.normalized()
    effective_bounds = _json_mapping_or_empty(operating_bounds_effective)
    if not effective_bounds and normalized_intent.operating_bounds is not None:
        effective_bounds = normalized_intent.operating_bounds.to_json_obj()

    success_criteria_summary: dict[str, JSONValue] | None = None
    if include_success_criteria_summary and normalized_intent.success_criteria:
        criteria = sorted(normalized_intent.success_criteria, key=lambda criterion: criterion.id)
        modes = tuple(sorted({criterion.evaluation_mode for criterion in normalized_intent.success_criteria}))
        success_criteria_summary = {
            "count": len(criteria),
            "evaluation_modes": list(modes),
            "criteria": [criterion.to_summary_obj() for criterion in criteria],
        }
        obs = _observability_slice_from_success_criteria(intent=normalized_intent)
        if obs is not None:
            success_criteria_summary["observability"] = obs

    return RuntimeIntentProjection(
        intent_id=normalized_intent.intent_id,
        spec_version=int(normalized_intent.spec_version),
        intent_semantic_fingerprint=intent_semantic_fingerprint(intent=normalized_intent),
        intent_goal_text_fingerprint=intent_goal_text_fingerprint(goal_statement=normalized_intent.goal_statement),
        stable_intent_sha256=stable_intent_sha256(intent=normalized_intent),
        operating_bounds_effective=effective_bounds,
        policies=tuple(policy.to_json_obj() for policy in sorted(normalized_intent.policies, key=lambda p: p.id)),
        success_criteria_summary=success_criteria_summary,
    )


def project_deployment_intent_projection(*, intent: IntentSpec) -> DeploymentIntentProjection:
    normalized_intent = intent.normalized()
    constraint_ids = tuple(constraint.id for constraint in sorted(normalized_intent.constraints, key=lambda c: c.id))
    policy_ids = tuple(policy.id for policy in sorted(normalized_intent.policies, key=lambda p: p.id))
    success_criteria_modes = tuple(
        sorted({criterion.evaluation_mode for criterion in normalized_intent.success_criteria})
    )

    trace_tags = _sorted_unique_strings(
        [f"constraint:{constraint_id}" for constraint_id in constraint_ids]
        + [f"policy:{policy_id}" for policy_id in policy_ids]
        + [f"success_mode:{mode}" for mode in success_criteria_modes]
    )

    return DeploymentIntentProjection(
        intent_id=normalized_intent.intent_id,
        spec_version=int(normalized_intent.spec_version),
        intent_semantic_fingerprint=intent_semantic_fingerprint(intent=normalized_intent),
        intent_goal_text_fingerprint=intent_goal_text_fingerprint(goal_statement=normalized_intent.goal_statement),
        stable_intent_sha256=stable_intent_sha256(intent=normalized_intent),
        constraint_ids=constraint_ids,
        policy_ids=policy_ids,
        success_criteria_modes=success_criteria_modes,
        trace_tags=trace_tags,
    )


def _operating_bounds_projection_field_values(
    *,
    intent_bounds: OperatingBound | None,
    controller_budget: Budget,
    hard_allow_network: bool,
) -> tuple[dict[str, JSONValue], dict[str, JSONValue]]:
    # Requested values:
    # - max_seconds / max_input_tokens / max_output_tokens are optional in the model.
    # - max_steps is also optional; when omitted, we treat it as the controller baseline.
    # - allow_network defaults to False when an intent explicitly provides bounds; when
    #   operating_bounds is omitted entirely, we treat it as "unspecified" and keep
    #   hard_allow_network as the requested value.
    if intent_bounds is None:
        requested_allow_network: bool = bool(hard_allow_network)
        requested_max_seconds: float | None = (
            float(controller_budget.max_wall_time_s) if controller_budget.max_wall_time_s is not None else None
        )
        requested_max_steps: int | None = int(controller_budget.max_iterations_total)
        requested_max_input_tokens: int | None = (
            int(controller_budget.max_input_tokens) if controller_budget.max_input_tokens is not None else None
        )
        requested_max_output_tokens: int | None = (
            int(controller_budget.max_output_tokens) if controller_budget.max_output_tokens is not None else None
        )
    else:
        requested_allow_network = bool(intent_bounds.allow_network)
        requested_max_seconds = float(intent_bounds.max_seconds) if intent_bounds.max_seconds is not None else None
        requested_max_steps = int(intent_bounds.max_steps) if intent_bounds.max_steps is not None else None
        requested_max_input_tokens = (
            int(intent_bounds.max_input_tokens) if intent_bounds.max_input_tokens is not None else None
        )
        requested_max_output_tokens = (
            int(intent_bounds.max_output_tokens) if intent_bounds.max_output_tokens is not None else None
        )

    requested: dict[str, JSONValue] = {
        "max_seconds": requested_max_seconds,
        "max_steps": requested_max_steps,
        "max_input_tokens": requested_max_input_tokens,
        "max_output_tokens": requested_max_output_tokens,
        "allow_network": requested_allow_network,
    }

    baseline: dict[str, JSONValue] = {
        "max_seconds": (
            float(controller_budget.max_wall_time_s) if controller_budget.max_wall_time_s is not None else None
        ),
        # Budget.max_iterations_total is always configured in this Phase 3 controller.
        "max_steps": int(controller_budget.max_iterations_total),
        "max_input_tokens": (
            int(controller_budget.max_input_tokens) if controller_budget.max_input_tokens is not None else None
        ),
        "max_output_tokens": (
            int(controller_budget.max_output_tokens) if controller_budget.max_output_tokens is not None else None
        ),
        "allow_network": bool(hard_allow_network),
    }

    return requested, baseline


def _min_optional_float(*, requested: float | None, baseline: float | None) -> float | None:
    if requested is None:
        return baseline
    if baseline is None:
        return requested
    return min(float(requested), float(baseline))


def _min_optional_int(*, requested: int | None, baseline: int | None) -> int | None:
    if requested is None:
        return baseline
    if baseline is None:
        return requested
    return min(int(requested), int(baseline))


def project_intent_operating_bounds_to_policy_context(
    *,
    intent_bounds: OperatingBound | None,
    controller_budget: Budget,
    hard_allow_network: bool,
) -> OperatingBoundsPolicyProjection:
    """Project intent operating bounds into policy context + audit decisions."""

    requested, baseline = _operating_bounds_projection_field_values(
        intent_bounds=intent_bounds,
        controller_budget=controller_budget,
        hard_allow_network=hard_allow_network,
    )

    # Effective values:
    req_max_seconds = json_value_as_float(requested.get("max_seconds"), default=0.0) if requested.get("max_seconds") is not None else None
    base_max_seconds = json_value_as_float(baseline.get("max_seconds"), default=0.0) if baseline.get("max_seconds") is not None else None
    effective_max_seconds = _min_optional_float(
        requested=req_max_seconds,
        baseline=base_max_seconds,
    )
    base_max_steps = json_value_as_int(baseline.get("max_steps"), default=0)
    req_max_steps = json_value_as_int(requested.get("max_steps"), default=base_max_steps)
    effective_max_steps = base_max_steps if requested.get("max_steps") is None else min(req_max_steps, base_max_steps)
    effective_max_input_tokens = _min_optional_int(
        requested=(json_value_as_int(requested.get("max_input_tokens"), default=0) if requested.get("max_input_tokens") is not None else None),
        baseline=(json_value_as_int(baseline.get("max_input_tokens"), default=0) if baseline.get("max_input_tokens") is not None else None),
    )
    effective_max_output_tokens = _min_optional_int(
        requested=(json_value_as_int(requested.get("max_output_tokens"), default=0) if requested.get("max_output_tokens") is not None else None),
        baseline=(json_value_as_int(baseline.get("max_output_tokens"), default=0) if baseline.get("max_output_tokens") is not None else None),
    )
    effective_allow_network = bool(requested["allow_network"]) and bool(baseline["allow_network"])

    effective: dict[str, JSONValue] = {
        "max_seconds": effective_max_seconds,
        "max_steps": effective_max_steps,
        "max_input_tokens": effective_max_input_tokens,
        "max_output_tokens": effective_max_output_tokens,
        "allow_network": effective_allow_network,
    }

    narrowing_decisions: list[dict[str, JSONValue]] = []

    def _maybe_add_field_decision(*, field: str, reason_code: str) -> None:
        base_val = baseline.get(field)
        eff_val = effective.get(field)
        if base_val == eff_val:
            return
        narrowing_decisions.append(
            {
                "field": field,
                "requested": requested.get(field),
                "baseline": base_val,
                "effective": eff_val,
                "reason_code": reason_code,
            }
        )

    # Numeric/time narrowing.
    if baseline["max_seconds"] != effective["max_seconds"]:
        if baseline["max_seconds"] is None:
            _maybe_add_field_decision(field="max_seconds", reason_code="intent.applied")
        else:
            _maybe_add_field_decision(field="max_seconds", reason_code="intent.min_with_controller_budget")
    if baseline["max_steps"] != effective["max_steps"]:
        _maybe_add_field_decision(field="max_steps", reason_code="intent.min_with_controller_budget")
    if baseline["max_input_tokens"] != effective["max_input_tokens"]:
        if baseline["max_input_tokens"] is None:
            _maybe_add_field_decision(field="max_input_tokens", reason_code="intent.applied")
        else:
            _maybe_add_field_decision(field="max_input_tokens", reason_code="intent.min_with_controller_budget")
    if baseline["max_output_tokens"] != effective["max_output_tokens"]:
        if baseline["max_output_tokens"] is None:
            _maybe_add_field_decision(field="max_output_tokens", reason_code="intent.applied")
        else:
            _maybe_add_field_decision(field="max_output_tokens", reason_code="intent.min_with_controller_budget")

    # Network hardening.
    #
    # For allow_network we record when the intent request differs from the hard
    # baseline, even if the effective value ends up equal to the baseline
    # (e.g. intent requested `true` but the hard default is already `false`).
    if requested["allow_network"] != baseline["allow_network"]:
        if bool(requested["allow_network"]) and not bool(baseline["allow_network"]):
            narrowing_decisions.append(
                {
                    "field": "allow_network",
                    "requested": requested.get("allow_network"),
                    "baseline": baseline.get("allow_network"),
                    "effective": effective.get("allow_network"),
                    "reason_code": "hard_policy.denied_network",
                }
            )
        else:
            narrowing_decisions.append(
                {
                    "field": "allow_network",
                    "requested": requested.get("allow_network"),
                    "baseline": baseline.get("allow_network"),
                    "effective": effective.get("allow_network"),
                    "reason_code": "intent.denied_network",
                }
            )

    return OperatingBoundsPolicyProjection(
        requested=requested,
        baseline=baseline,
        effective=effective,
        narrowing_decisions=tuple(narrowing_decisions),
    )


def project_stage_timeout_s(
    *,
    stage_timeout_s: float | None,
    intent_max_seconds: float | None,
) -> tuple[float | None, dict[str, JSONValue] | None]:
    """Narrow a stage timeout using intent's `max_seconds`.

    Returns (effective_timeout_s, decision_or_none).
    """

    if intent_max_seconds is None:
        # No intent stage cap; keep controller-configured timeout unchanged.
        return stage_timeout_s, None

    if stage_timeout_s is None:
        effective = float(intent_max_seconds)
        return effective, {
            "field": "stage_timeout_s",
            "requested": None,
            "baseline": None,
            "effective": effective,
            "reason_code": "intent.applied_stage_timeout",
        }

    effective = min(float(stage_timeout_s), float(intent_max_seconds))
    if effective == float(stage_timeout_s):
        return stage_timeout_s, None

    return effective, {
        "field": "stage_timeout_s",
        "requested": float(stage_timeout_s),
        "baseline": float(stage_timeout_s),
        "effective": effective,
        "reason_code": "intent.min_with_stage_timeout",
    }
