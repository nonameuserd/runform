"""Single compile-time resolution path for normalized intent (IntentSpecV1).

Precedence:
1. ``intent_ref`` + :class:`~akc.intent.store.IntentStore.load_intent`` + stable hash verify
2. Explicit :class:`~akc.intent.models.IntentSpecV1` from the controller / session
3. Controlled legacy read from plan step ``inputs`` (deprecated)
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from akc.intent.models import (
    Constraint,
    ConstraintKind,
    ConstraintLink,
    EvaluationMode,
    IntentSpecV1,
    Objective,
    OperatingBound,
    SuccessCriterion,
    SuccessCriterionLink,
    stable_intent_sha256,
)
from akc.intent.plan_step_intent import INTENT_REF_INPUT_KEY, load_intent_verified_from_plan_step_inputs
from akc.intent.store import IntentStore
from akc.memory.models import normalize_repo_id, require_non_empty
from akc.utils.fingerprint import stable_json_fingerprint

logger = logging.getLogger(__name__)


class IntentResolutionError(ValueError):
    """Raised when intent_ref is present but cannot be verified or conflicts with controller intent."""


ResolvedIntentSource = Literal["store_ref", "controller", "legacy_step"]


@dataclass(frozen=True, slots=True)
class ResolvedIntentContext:
    """Normalized intent plus resolution provenance for compile subsystems."""

    spec: IntentSpecV1
    source: ResolvedIntentSource
    stable_intent_sha256: str


def _normalize_stable_sha(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    s = value.strip().lower()
    if len(s) != 64 or any(c not in "0123456789abcdef" for c in s):
        return None
    return s


def intent_ref_requires_store_resolution(*, inputs: Mapping[str, Any]) -> bool:
    """True when ``inputs`` carries a structurally complete ``intent_ref`` (id + stable sha)."""

    ref = inputs.get(INTENT_REF_INPUT_KEY)
    if not isinstance(ref, dict):
        return False
    intent_id_raw = ref.get("intent_id")
    if not isinstance(intent_id_raw, str) or not intent_id_raw.strip():
        return False
    return _normalize_stable_sha(ref.get("stable_intent_sha256")) is not None


def intent_link_summaries_for_prompts(
    *,
    spec: IntentSpecV1,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """JSON-shaped summaries used by generate/repair prompts (tenant-scoped contract slice)."""

    n = spec.normalized()
    active_objectives = [o.to_summary_obj() for o in n.objectives]
    linked_constraints = [ConstraintLink.from_constraint(constraint=c).to_json_obj() for c in n.constraints]
    active_success_criteria = [
        SuccessCriterionLink.from_success_criterion(sc=sc).to_json_obj() for sc in n.success_criteria
    ]
    return active_objectives, linked_constraints, active_success_criteria


def intent_reference_summaries_for_prompts(
    *,
    spec: IntentSpecV1,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Id + fingerprint rows for prompts when IR + IntentStore anchor intent (no full text blobs)."""

    n = spec.normalized()
    active_objectives: list[dict[str, Any]] = []
    for o in n.objectives:
        so = o.to_summary_obj()
        active_objectives.append(
            {
                "id": str(so["id"]),
                "fingerprint": stable_json_fingerprint(so),
            }
        )
    linked_constraints: list[dict[str, Any]] = []
    for c in n.constraints:
        lj = ConstraintLink.from_constraint(constraint=c).to_json_obj()
        linked_constraints.append(
            {
                "constraint_id": str(lj["constraint_id"]),
                "kind": str(lj["kind"]),
                "fingerprint": stable_json_fingerprint(lj),
            }
        )
    active_success_criteria: list[dict[str, Any]] = []
    for sc in n.success_criteria:
        sj = SuccessCriterionLink.from_success_criterion(sc=sc).to_json_obj()
        active_success_criteria.append(
            {
                "success_criterion_id": str(sj["success_criterion_id"]),
                "evaluation_mode": str(sj["evaluation_mode"]),
                "fingerprint": stable_json_fingerprint(sj),
            }
        )
    return active_objectives, linked_constraints, active_success_criteria


def _legacy_intent_spec_from_inputs(
    *,
    tenant_id: str,
    repo_id: str,
    inputs: Mapping[str, Any],
    fallback_goal_statement: str | None = None,
) -> IntentSpecV1 | None:
    """Best-effort IntentSpecV1 from duplicated step blobs (deprecated)."""

    raw_id = inputs.get("intent_id")
    intent_id = str(raw_id).strip() if isinstance(raw_id, str) else ""
    ref = inputs.get(INTENT_REF_INPUT_KEY)
    if not intent_id and isinstance(ref, dict):
        rid = ref.get("intent_id")
        if isinstance(rid, str) and rid.strip():
            intent_id = rid.strip()
    if not intent_id:
        return None

    repo = normalize_repo_id(repo_id)

    objectives: list[Objective] = []
    ao = inputs.get("active_objectives")
    if isinstance(ao, list):
        for item in ao:
            if isinstance(item, dict):
                try:
                    objectives.append(Objective.from_json_obj(item))
                except Exception:
                    continue

    constraints: list[Constraint] = []
    lc = inputs.get("linked_constraints")
    if isinstance(lc, list):
        for item in lc:
            if not isinstance(item, dict):
                continue
            cid = str(item.get("constraint_id") or item.get("id") or "").strip()
            kind = str(item.get("kind") or "hard").strip()
            summ = str(item.get("summary") or item.get("statement") or "").strip()
            if cid and summ:
                try:
                    ck: ConstraintKind = cast(ConstraintKind, kind) if kind in ("hard", "soft") else "hard"
                    constraints.append(Constraint(id=cid, kind=ck, statement=summ))
                except Exception:
                    continue

    success_criteria: list[SuccessCriterion] = []
    asc = inputs.get("active_success_criteria")
    if isinstance(asc, list):
        for item in asc:
            if not isinstance(item, dict):
                continue
            sid = str(item.get("success_criterion_id") or item.get("id") or "").strip()
            mode_raw = str(item.get("evaluation_mode") or "human_gate").strip()
            summ = str(item.get("summary") or item.get("description") or "").strip()
            if sid and mode_raw:
                try:
                    emode = cast(EvaluationMode, mode_raw)
                    success_criteria.append(SuccessCriterion(id=sid, evaluation_mode=emode, description=summ or sid))
                except Exception:
                    continue

    ob_raw = inputs.get("operating_bounds")
    operating_bounds = None
    if isinstance(ob_raw, dict) and ob_raw:
        try:
            operating_bounds = OperatingBound.from_json_obj(ob_raw)
        except Exception:
            operating_bounds = None

    gs = inputs.get("goal_statement")
    goal_statement = str(gs).strip() if isinstance(gs, str) else None
    if goal_statement is None and fallback_goal_statement is not None:
        g2 = str(fallback_goal_statement).strip()
        goal_statement = g2 if g2 else None

    return IntentSpecV1(
        intent_id=intent_id,
        tenant_id=tenant_id.strip(),
        repo_id=repo,
        goal_statement=goal_statement,
        objectives=tuple(objectives),
        constraints=tuple(constraints),
        success_criteria=tuple(success_criteria),
        operating_bounds=operating_bounds,
    )


def resolve_compile_intent_context(
    *,
    tenant_id: str,
    repo_id: str,
    inputs: Mapping[str, Any],
    intent_store: IntentStore | None = None,
    controller_intent_spec: IntentSpecV1 | None = None,
    fallback_goal_statement: str | None = None,
    warn_legacy_step_blobs_without_intent_ref_under_outputs_root: bool = False,
) -> ResolvedIntentContext:
    """Resolve authoritative :class:`IntentSpecV1` for compile-time consumers.

    When ``intent_ref`` is structurally present, resolution requires a tenant-scoped
    ``intent_store`` and a successful hash-verified load. Otherwise this fails
    closed with :class:`IntentResolutionError` (tamper / stale plan / missing artifact).

    When a verified store intent is loaded and ``controller_intent_spec`` is provided,
    the stable hash must match; otherwise :class:`IntentResolutionError` is raised.

    Legacy step blobs are only used when neither store resolution nor controller
    intent applies.

    When ``warn_legacy_step_blobs_without_intent_ref_under_outputs_root`` is true
    (outputs_root-backed compile sessions), falling back to duplicated step fields
    without a structurally complete ``intent_ref`` emits a deprecation warning.
    """

    require_non_empty(tenant_id, name="tenant_id")

    ref_needs_store = intent_ref_requires_store_resolution(inputs=inputs)
    if ref_needs_store:
        if intent_store is None:
            raise IntentResolutionError(
                "intent_ref is present on plan step inputs but no IntentStore was provided for verification"
            )
        loaded = load_intent_verified_from_plan_step_inputs(
            tenant_id=tenant_id,
            repo_id=repo_id,
            inputs=inputs,
            intent_store=intent_store,
        )
        if loaded is not None:
            normalized = loaded.normalized()
            sha = stable_intent_sha256(intent=normalized)
            # Verified bytes match intent_ref; the store is authoritative for this path.
            return ResolvedIntentContext(spec=normalized, source="store_ref", stable_intent_sha256=sha)

        # intent_ref is present but bytes/hash could not be verified (missing file, stale ref, etc.).
        # Prefer explicit controller context when available; otherwise fail closed.
        if controller_intent_spec is not None:
            ctrl = controller_intent_spec.normalized()
            sha = stable_intent_sha256(intent=ctrl)
            return ResolvedIntentContext(spec=ctrl, source="controller", stable_intent_sha256=sha)

        raise IntentResolutionError(
            "intent_ref could not be verified against the IntentStore "
            "(missing artifact or stable_intent_sha256 mismatch)"
        )

    if controller_intent_spec is not None:
        ctrl = controller_intent_spec.normalized()
        sha = stable_intent_sha256(intent=ctrl)
        return ResolvedIntentContext(spec=ctrl, source="controller", stable_intent_sha256=sha)

    legacy = _legacy_intent_spec_from_inputs(
        tenant_id=tenant_id,
        repo_id=repo_id,
        inputs=inputs,
        fallback_goal_statement=fallback_goal_statement,
    )
    if legacy is None:
        raise IntentResolutionError(
            "no intent_ref, no controller IntentSpecV1, and plan step inputs lack a usable legacy intent contract"
        )
    n = legacy.normalized()
    sha = stable_intent_sha256(intent=n)
    if warn_legacy_step_blobs_without_intent_ref_under_outputs_root:
        logger.warning(
            "Deprecated: intent was resolved from duplicated plan-step fields "
            "(e.g. active_objectives, linked_constraints, active_success_criteria) "
            "without a structurally complete intent_ref while using an outputs_root-backed "
            "session. Persist normalized intent under the tenant IntentStore and attach "
            "intent_ref to plan steps; duplicated step blobs are scheduled for removal."
        )
    return ResolvedIntentContext(spec=n, source="legacy_step", stable_intent_sha256=sha)
