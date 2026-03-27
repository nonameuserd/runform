from __future__ import annotations

import json
import os
import subprocess
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_artifact_json
from akc.control.otel_export import (
    StdoutOtelExportSink,
    autopilot_scope_event_to_export_obj,
    export_obj_to_json_line,
    otel_export_extra_callbacks_from_env,
)
from akc.living.automation_profile import (
    LivingAutomationProfile,
    living_automation_includes_runtime_bridge,
    resolve_living_automation_profile,
)
from akc.living.runtime_bridge import default_living_runtime_bridge
from akc.living.safe_recompile import safe_recompile_on_drift
from akc.memory.models import JSONValue, normalize_repo_id
from akc.run.loader import find_latest_run_manifest, load_run_manifest
from akc.runtime.compile_apply_attestation import (
    compile_apply_attestation_denial_for_rollout,
    summarize_compile_apply_from_control_plane,
)
from akc.runtime.living_bridge import LivingRuntimeBridge
from akc.runtime.models import RuntimeEvent

RuntimeTerminalStatus = Literal[
    "succeeded",
    "failed",
    "terminal",
    "idle",
    "stopped",
    "max_iterations_exceeded",
    "unknown",
]

EventKind = Literal["runtime_rollout", "promotion_prevented"]
LeaseBackend = Literal["filesystem", "k8s"]
EnvProfile = Literal["dev", "staging", "prod"]

SLOGateStatus = Literal["allowed", "prevented", "insufficient_history", "error"]


@dataclass(frozen=True, slots=True)
class AutonomyBudgetConfig:
    """Autopilot-level safety limits (bounded blast radius).

    These budgets are enforced by the always-on controller loop before starting
    any mutating runtime rollout (no policy bypass).
    """

    max_mutations_per_day: int
    max_concurrent_rollouts: int
    rollback_budget_per_day: int

    # Escalation thresholds (controller stops mutating for this scope).
    max_consecutive_rollout_failures: int
    max_rollbacks_per_day_before_escalation: int

    # Loop pacing / backoff.
    cooldown_after_failure_ms: int
    cooldown_after_policy_deny_ms: int

    # Rolling budget window size for daily counters (defaults to 24h).
    budget_window_ms: int = 24 * 60 * 60 * 1000


@dataclass(frozen=True, slots=True)
class BudgetState:
    """Persisted per tenant/repo."""

    window_start_ms: int
    mutations_count: int = 0
    rollbacks_count: int = 0
    consecutive_failures: int = 0
    active_rollouts: int = 0
    human_escalation_required: bool = False
    cooldown_until_ms: int = 0

    @staticmethod
    def reset_for_new_window(*, window_start_ms: int) -> BudgetState:
        return BudgetState(window_start_ms=window_start_ms)


@dataclass(frozen=True, slots=True)
class AutopilotHistoryEntry:
    event_kind: EventKind
    attempt_id: str
    started_at_ms: int

    # Promotion policy signal (only meaningful for runtime_rollout events).
    promotion_policy_allow: bool | None = None

    # Runtime rollout outcomes (only meaningful for runtime_rollout events).
    runtime_terminal_status: RuntimeTerminalStatus | None = None
    rollback_count: int = 0
    rollback_success_count: int = 0
    convergence_latency_ms: float | None = None
    intent_to_healthy_runtime_ms: float | None = None
    compile_to_healthy_runtime_ms: float | None = None
    compression_factor_vs_baseline: float | None = None
    intent_to_staging_ms: float | None = None
    intent_to_prod_ms: float | None = None
    staging_to_prod_ms: float | None = None
    approval_wait_ms: float | None = None
    manual_touch_count: int | None = None

    # Prevention metadata.
    prevented_reason: str | None = None

    # Compile realization (promotion packet / manifest compile_apply_attestation).
    compile_realization_mode: str | None = None
    compile_apply_applied: bool | None = None
    compile_apply_policy_allowed: bool | None = None
    compile_apply_denial_reason: str | None = None

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "event_kind": self.event_kind,
            "attempt_id": self.attempt_id,
            "started_at_ms": int(self.started_at_ms),
            "promotion_policy_allow": self.promotion_policy_allow,
            "runtime_terminal_status": self.runtime_terminal_status,
            "rollback_count": int(self.rollback_count),
            "rollback_success_count": int(self.rollback_success_count),
            "convergence_latency_ms": self.convergence_latency_ms,
            "intent_to_healthy_runtime_ms": self.intent_to_healthy_runtime_ms,
            "compile_to_healthy_runtime_ms": self.compile_to_healthy_runtime_ms,
            "compression_factor_vs_baseline": self.compression_factor_vs_baseline,
            "intent_to_staging_ms": self.intent_to_staging_ms,
            "intent_to_prod_ms": self.intent_to_prod_ms,
            "staging_to_prod_ms": self.staging_to_prod_ms,
            "approval_wait_ms": self.approval_wait_ms,
            "manual_touch_count": self.manual_touch_count,
            "prevented_reason": self.prevented_reason,
            "compile_realization_mode": self.compile_realization_mode,
            "compile_apply_applied": self.compile_apply_applied,
            "compile_apply_policy_allowed": self.compile_apply_policy_allowed,
            "compile_apply_denial_reason": self.compile_apply_denial_reason,
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> AutopilotHistoryEntry:
        ek = str(obj.get("event_kind", "")).strip()
        if ek not in {"runtime_rollout", "promotion_prevented"}:
            raise ValueError(f"invalid event_kind: {ek!r}")
        attempt_id = str(obj.get("attempt_id", "")).strip()
        if not attempt_id:
            raise ValueError("attempt_id must be non-empty")
        started_at_ms = int(obj.get("started_at_ms", 0) or 0)
        promotion_allow_raw = obj.get("promotion_policy_allow")
        promotion_policy_allow: bool | None = None
        if promotion_allow_raw is True:
            promotion_policy_allow = True
        elif promotion_allow_raw is False:
            promotion_policy_allow = False
        term_raw = obj.get("runtime_terminal_status")
        runtime_terminal_status: RuntimeTerminalStatus | None = None
        if isinstance(term_raw, str) and term_raw.strip():
            runtime_terminal_status = term_raw.strip()  # type: ignore[assignment]
        rollback_count = int(obj.get("rollback_count", 0) or 0)
        rollback_success_count = int(obj.get("rollback_success_count", 0) or 0)
        cl = obj.get("convergence_latency_ms")
        convergence_latency_ms: float | None = None
        if isinstance(cl, (int, float)) and not isinstance(cl, bool):
            convergence_latency_ms = float(cl)
        i2h_raw = obj.get("intent_to_healthy_runtime_ms")
        intent_to_healthy_runtime_ms: float | None = None
        if isinstance(i2h_raw, (int, float)) and not isinstance(i2h_raw, bool):
            intent_to_healthy_runtime_ms = float(i2h_raw)
        c2h_raw = obj.get("compile_to_healthy_runtime_ms")
        compile_to_healthy_runtime_ms: float | None = None
        if isinstance(c2h_raw, (int, float)) and not isinstance(c2h_raw, bool):
            compile_to_healthy_runtime_ms = float(c2h_raw)
        cf_raw = obj.get("compression_factor_vs_baseline")
        compression_factor_vs_baseline: float | None = None
        if isinstance(cf_raw, (int, float)) and not isinstance(cf_raw, bool):
            compression_factor_vs_baseline = float(cf_raw)

        def _opt_metric_float(raw: Any) -> float | None:
            if isinstance(raw, (int, float)) and not isinstance(raw, bool):
                return float(raw)
            return None

        def _opt_metric_int(raw: Any) -> int | None:
            if isinstance(raw, int) and not isinstance(raw, bool):
                return int(raw)
            if isinstance(raw, float) and not isinstance(raw, bool) and raw >= 0:
                return int(raw)
            return None

        intent_to_staging_ms = _opt_metric_float(obj.get("intent_to_staging_ms"))
        intent_to_prod_ms = _opt_metric_float(obj.get("intent_to_prod_ms"))
        staging_to_prod_ms = _opt_metric_float(obj.get("staging_to_prod_ms"))
        approval_wait_ms = _opt_metric_float(obj.get("approval_wait_ms"))
        manual_touch_count = _opt_metric_int(obj.get("manual_touch_count"))
        prevented_reason = obj.get("prevented_reason")
        crm_raw = obj.get("compile_realization_mode")
        compile_realization_mode: str | None = (
            str(crm_raw).strip() if isinstance(crm_raw, str) and str(crm_raw).strip() else None
        )
        cap_raw = obj.get("compile_apply_applied")
        compile_apply_applied: bool | None = None
        if cap_raw is True:
            compile_apply_applied = True
        elif cap_raw is False:
            compile_apply_applied = False
        cppa_raw = obj.get("compile_apply_policy_allowed")
        compile_apply_policy_allowed: bool | None = None
        if cppa_raw is True:
            compile_apply_policy_allowed = True
        elif cppa_raw is False:
            compile_apply_policy_allowed = False
        cad_raw = obj.get("compile_apply_denial_reason")
        compile_apply_denial_reason = (
            str(cad_raw).strip() if isinstance(cad_raw, str) and str(cad_raw).strip() else None
        )
        return AutopilotHistoryEntry(
            event_kind=ek,  # type: ignore[arg-type]
            attempt_id=attempt_id,
            started_at_ms=started_at_ms,
            promotion_policy_allow=promotion_policy_allow,
            runtime_terminal_status=runtime_terminal_status,
            rollback_count=rollback_count,
            rollback_success_count=rollback_success_count,
            convergence_latency_ms=convergence_latency_ms,
            intent_to_healthy_runtime_ms=intent_to_healthy_runtime_ms,
            compile_to_healthy_runtime_ms=compile_to_healthy_runtime_ms,
            compression_factor_vs_baseline=compression_factor_vs_baseline,
            intent_to_staging_ms=intent_to_staging_ms,
            intent_to_prod_ms=intent_to_prod_ms,
            staging_to_prod_ms=staging_to_prod_ms,
            approval_wait_ms=approval_wait_ms,
            manual_touch_count=manual_touch_count,
            prevented_reason=str(prevented_reason).strip() if prevented_reason is not None else None,
            compile_realization_mode=compile_realization_mode,
            compile_apply_applied=compile_apply_applied,
            compile_apply_policy_allowed=compile_apply_policy_allowed,
            compile_apply_denial_reason=compile_apply_denial_reason,
        )


def _day_window_start_ms(*, now_ms: int, window_ms: int) -> int:
    return int(now_ms - (now_ms % int(window_ms)))


def budget_guard_for_start(
    *,
    now_ms: int,
    state: BudgetState,
    config: AutonomyBudgetConfig,
    proposed_rollout_rollback_count: int = 0,
) -> tuple[bool, str, BudgetState]:
    """Return (allowed, reason, updated_state).

    The updated state includes window reset/cooldown changes but does not
    increment counters for rollouts that are not starting.
    """

    if config.budget_window_ms <= 0:
        raise ValueError("budget_window_ms must be > 0")

    current_window_start = _day_window_start_ms(now_ms=now_ms, window_ms=config.budget_window_ms)
    next_state = state
    if state.window_start_ms != current_window_start:
        next_state = BudgetState.reset_for_new_window(window_start_ms=current_window_start)

    if next_state.human_escalation_required:
        return False, "human_escalation_required", next_state

    if now_ms < next_state.cooldown_until_ms:
        return False, "cooldown_active", next_state

    if next_state.active_rollouts >= int(config.max_concurrent_rollouts):
        return False, "max_concurrent_rollouts_exceeded", next_state

    if next_state.mutations_count >= int(config.max_mutations_per_day):
        return False, "max_mutations_per_day_exceeded", next_state

    if next_state.rollbacks_count + int(proposed_rollout_rollback_count) > int(config.rollback_budget_per_day):
        return False, "rollback_budget_exceeded", next_state

    # Reserve an active slot.
    return (
        True,
        "allowed",
        BudgetState(
            window_start_ms=next_state.window_start_ms,
            mutations_count=next_state.mutations_count,
            rollbacks_count=next_state.rollbacks_count,
            consecutive_failures=next_state.consecutive_failures,
            active_rollouts=next_state.active_rollouts + 1,
            human_escalation_required=next_state.human_escalation_required,
            cooldown_until_ms=next_state.cooldown_until_ms,
        ),
    )


def budget_guard_after_runtime_outcome(
    *,
    now_ms: int,
    state: BudgetState,
    config: AutonomyBudgetConfig,
    rollout_started_allowed: bool,
    runtime_terminal_status: RuntimeTerminalStatus,
    rollback_count: int,
) -> BudgetState:
    """Update state after a runtime rollout attempt completes."""

    if not rollout_started_allowed:
        return state

    current_window_start = _day_window_start_ms(now_ms=now_ms, window_ms=config.budget_window_ms)
    next_state = state
    if state.window_start_ms != current_window_start:
        next_state = BudgetState.reset_for_new_window(window_start_ms=current_window_start)

    active_rollouts = max(0, int(next_state.active_rollouts) - 1)

    succeeded = runtime_terminal_status == "succeeded"
    consecutive_failures = 0 if succeeded else int(next_state.consecutive_failures) + 1

    mutations_count = int(next_state.mutations_count) + 1
    rollbacks_count = int(next_state.rollbacks_count) + int(rollback_count)

    human_escalation_required = bool(next_state.human_escalation_required)
    cooldown_until_ms = int(next_state.cooldown_until_ms)

    if consecutive_failures >= int(config.max_consecutive_rollout_failures):
        human_escalation_required = True
    if rollbacks_count >= int(config.max_rollbacks_per_day_before_escalation):
        human_escalation_required = True

    # Cooldown after failure helps avoid thrashing.
    cooldown_until_ms = 0 if succeeded else int(now_ms + int(config.cooldown_after_failure_ms))

    return BudgetState(
        window_start_ms=next_state.window_start_ms,
        mutations_count=mutations_count,
        rollbacks_count=rollbacks_count,
        consecutive_failures=consecutive_failures,
        active_rollouts=active_rollouts,
        human_escalation_required=human_escalation_required,
        cooldown_until_ms=cooldown_until_ms,
    )


@dataclass(frozen=True, slots=True)
class ReliabilityScoreboard:
    tenant_id: str
    repo_id: str
    window_start_ms: int
    window_end_ms: int
    kpi: dict[str, Any]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "tenant_id": self.tenant_id,
            "repo_id": self.repo_id,
            "window_start_ms": int(self.window_start_ms),
            "window_end_ms": int(self.window_end_ms),
            "kpi": dict(self.kpi),
        }


@dataclass(frozen=True, slots=True)
class ReliabilitySLOGateConfig:
    """Fail-closed reliability gate for progressive takeover (Level 3).

    The intent is to block *scope expansion* (starting new live rollouts) until
    measured reliability KPIs are above minimum thresholds for this scope.
    """

    min_rollouts_total: int = 5
    min_policy_compliance_rate: float = 0.98
    min_rollback_success_rate: float = 0.95
    max_delivery_change_instability_proxy: float = 0.25


def _slo_gate_allows_rollout(
    *,
    now_ms: int,
    scoreboard_window_ms: int,
    tenant_id: str,
    repo_id: str,
    history: list[AutopilotHistoryEntry],
    gate: ReliabilitySLOGateConfig,
) -> tuple[bool, SLOGateStatus, str, ReliabilityScoreboard]:
    """Evaluate the SLO gate over the trailing window (fail-closed)."""

    window_start_ms = int(max(0, int(now_ms) - int(scoreboard_window_ms)))
    window_end_ms = int(now_ms)
    scoreboard = compute_reliability_scoreboard(
        tenant_id=tenant_id,
        repo_id=repo_id,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        history=history,
    )
    kpi = scoreboard.kpi
    rollouts_total = int(kpi.get("rollouts_total", 0) or 0)
    if rollouts_total < int(gate.min_rollouts_total):
        return False, "insufficient_history", "min_rollouts_total_not_met", scoreboard

    def _num(key: str) -> float:
        v = kpi.get(key)
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        return 0.0

    if _num("policy_compliance_rate") < float(gate.min_policy_compliance_rate):
        return False, "prevented", "policy_compliance_rate_below_threshold", scoreboard
    if _num("rollback_success_rate") < float(gate.min_rollback_success_rate):
        return False, "prevented", "rollback_success_rate_below_threshold", scoreboard
    if _num("delivery_change_instability_proxy") > float(gate.max_delivery_change_instability_proxy):
        return False, "prevented", "delivery_change_instability_proxy_above_threshold", scoreboard
    return True, "allowed", "allowed", scoreboard


def _percentile_sorted(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = int(round((len(s) - 1) * q))
    idx = max(0, min(idx, len(s) - 1))
    return float(s[idx])


def compute_reliability_scoreboard(
    *,
    tenant_id: str,
    repo_id: str,
    window_start_ms: int,
    window_end_ms: int,
    history: list[AutopilotHistoryEntry],
) -> ReliabilityScoreboard:
    if window_end_ms < window_start_ms:
        raise ValueError("window_end_ms must be >= window_start_ms")

    in_window = [
        h for h in history if int(h.started_at_ms) >= int(window_start_ms) and int(h.started_at_ms) < int(window_end_ms)
    ]

    runtime_rollouts = [h for h in in_window if h.event_kind == "runtime_rollout"]
    prevented = [h for h in in_window if h.event_kind == "promotion_prevented"]

    rollouts_total = len(runtime_rollouts)
    rollouts_with_rollback = sum(1 for h in runtime_rollouts if int(h.rollback_count) > 0)
    rollbacks_total = sum(int(h.rollback_count) for h in runtime_rollouts)
    rollbacks_succeeded_total = sum(int(h.rollback_success_count) for h in runtime_rollouts)
    rollback_success_rate = float(rollbacks_succeeded_total) / float(rollbacks_total) if rollbacks_total > 0 else 1.0

    allowed_rollouts = sum(1 for h in runtime_rollouts if h.promotion_policy_allow is True)
    policy_compliance_rate = (float(allowed_rollouts) / float(rollouts_total)) if rollouts_total > 0 else 1.0

    convergence_latencies = [h.convergence_latency_ms for h in runtime_rollouts if h.convergence_latency_ms is not None]
    convergence_latency_ms_avg = (
        float(sum(convergence_latencies) / float(len(convergence_latencies))) if convergence_latencies else 0.0
    )

    repair_latencies = [
        h.convergence_latency_ms
        for h in runtime_rollouts
        if int(h.rollback_count) > 0 and h.convergence_latency_ms is not None
    ]
    mttr_like_repair_latency_ms_avg = (
        float(sum(repair_latencies) / float(len(repair_latencies))) if repair_latencies else 0.0
    )
    intent_to_healthy_values = [
        float(h.intent_to_healthy_runtime_ms)
        for h in runtime_rollouts
        if isinstance(h.intent_to_healthy_runtime_ms, (int, float))
    ]
    compile_to_healthy_values = [
        float(h.compile_to_healthy_runtime_ms)
        for h in runtime_rollouts
        if isinstance(h.compile_to_healthy_runtime_ms, (int, float))
    ]
    compression_values = [
        float(h.compression_factor_vs_baseline)
        for h in runtime_rollouts
        if isinstance(h.compression_factor_vs_baseline, (int, float))
    ]
    intent_to_healthy_runtime_ms_p50 = (
        sorted(intent_to_healthy_values)[len(intent_to_healthy_values) // 2] if intent_to_healthy_values else 0.0
    )
    intent_to_healthy_runtime_ms_p90 = (
        sorted(intent_to_healthy_values)[int(round((len(intent_to_healthy_values) - 1) * 0.9))]
        if intent_to_healthy_values
        else 0.0
    )
    compile_to_healthy_runtime_ms_p50 = (
        sorted(compile_to_healthy_values)[len(compile_to_healthy_values) // 2] if compile_to_healthy_values else 0.0
    )
    compression_factor_vs_baseline_avg = (
        float(sum(compression_values) / float(len(compression_values))) if compression_values else 0.0
    )

    failed_promotions_prevented = len(prevented)
    mutation_attempts_blocked_by_policy = sum(
        1
        for h in prevented
        if isinstance(h.prevented_reason, str)
        and h.prevented_reason.strip().lower() in {"policy_denied", "promotion_policy_not_allowed"}
    )
    compile_scoped_apply_rollouts_total = sum(
        1 for h in runtime_rollouts if (h.compile_realization_mode or "") == "scoped_apply"
    )
    compile_apply_gate_prevented_total = sum(
        1 for h in prevented if isinstance(h.prevented_reason, str) and "compile_apply_denied:" in h.prevented_reason
    )

    window_ms = max(1, int(window_end_ms) - int(window_start_ms))
    weeks = float(window_ms) / float(7 * 24 * 60 * 60 * 1000)
    delivery_throughput_rollouts_per_week_est = float(rollouts_total) / weeks if weeks > 0 else 0.0
    delivery_change_instability_proxy = (
        float(rollouts_with_rollback) / float(rollouts_total) if rollouts_total > 0 else 0.0
    )

    intent_to_staging_vals = [
        float(h.intent_to_staging_ms)
        for h in runtime_rollouts
        if isinstance(h.intent_to_staging_ms, (int, float)) and not isinstance(h.intent_to_staging_ms, bool)
    ]
    intent_to_prod_vals = [
        float(h.intent_to_prod_ms)
        for h in runtime_rollouts
        if isinstance(h.intent_to_prod_ms, (int, float)) and not isinstance(h.intent_to_prod_ms, bool)
    ]
    staging_to_prod_vals = [
        float(h.staging_to_prod_ms)
        for h in runtime_rollouts
        if isinstance(h.staging_to_prod_ms, (int, float)) and not isinstance(h.staging_to_prod_ms, bool)
    ]
    approval_wait_vals = [
        float(h.approval_wait_ms)
        for h in runtime_rollouts
        if isinstance(h.approval_wait_ms, (int, float)) and not isinstance(h.approval_wait_ms, bool)
    ]
    manual_touch_total = 0
    for h in runtime_rollouts:
        mtc = h.manual_touch_count
        if isinstance(mtc, int) and not isinstance(mtc, bool) and mtc >= 0:
            manual_touch_total += int(mtc)

    return ReliabilityScoreboard(
        tenant_id=tenant_id,
        repo_id=repo_id,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        kpi={
            "decision_window_id": f"{int(window_start_ms)}-{int(window_end_ms)}",
            "policy_compliance_rate": policy_compliance_rate,
            "rollouts_total": int(rollouts_total),
            "rollouts_with_rollback": int(rollouts_with_rollback),
            "rollbacks_total": int(rollbacks_total),
            "rollbacks_succeeded_total": int(rollbacks_succeeded_total),
            "rollback_success_rate": rollback_success_rate,
            "convergence_latency_ms_avg": convergence_latency_ms_avg,
            "mttr_like_repair_latency_ms_avg": mttr_like_repair_latency_ms_avg,
            "intent_to_healthy_runtime_ms_p50": intent_to_healthy_runtime_ms_p50,
            "intent_to_healthy_runtime_ms_p90": intent_to_healthy_runtime_ms_p90,
            "compile_to_healthy_runtime_ms_p50": compile_to_healthy_runtime_ms_p50,
            "compression_factor_vs_baseline_avg": compression_factor_vs_baseline_avg,
            "failed_promotions_prevented": int(failed_promotions_prevented),
            "mutation_attempts_blocked_by_policy": int(mutation_attempts_blocked_by_policy),
            "compile_scoped_apply_rollouts_total": int(compile_scoped_apply_rollouts_total),
            "compile_apply_gate_prevented_total": int(compile_apply_gate_prevented_total),
            "delivery_throughput_rollouts_per_week_est": delivery_throughput_rollouts_per_week_est,
            "delivery_change_instability_proxy": delivery_change_instability_proxy,
            "delivery_lead_time_ms_p50": _percentile_sorted(intent_to_prod_vals, 0.5),
            "delivery_lead_time_ms_p90": _percentile_sorted(intent_to_prod_vals, 0.9),
            "intent_to_staging_ms_p50": _percentile_sorted(intent_to_staging_vals, 0.5),
            "staging_to_prod_latency_ms_p50": _percentile_sorted(staging_to_prod_vals, 0.5),
            "approval_wait_ms_p50": _percentile_sorted(approval_wait_vals, 0.5),
            "manual_touch_count_total": int(manual_touch_total),
            "delivery_improvement_signals_note": (
                "DORA-shaped service delivery proxies for trends only; pair with reliability KPIs and "
                "avoid single-score gates."
            ),
        },
    )


def emit_reliability_scoreboard_artifact(
    *,
    scope_root: Path,
    tenant_id: str,
    repo_id: str,
    window_start_ms: int,
    window_end_ms: int,
    history: list[AutopilotHistoryEntry],
) -> Path:
    """Write a schema-validated reliability scoreboard JSON artifact."""

    scoreboard = compute_reliability_scoreboard(
        tenant_id=tenant_id,
        repo_id=repo_id,
        window_start_ms=window_start_ms,
        window_end_ms=window_end_ms,
        history=history,
    )

    out_dir = scope_root / ".akc" / "autopilot" / "scoreboards"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{int(window_start_ms)}-{int(window_end_ms)}.reliability_scoreboard.v1.json"

    payload = apply_schema_envelope(obj=scoreboard.to_json_obj(), kind="reliability_scoreboard", version=1)
    validate_artifact_json(obj=payload, kind="reliability_scoreboard", version=1)

    out_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def _load_history_jsonl(path: Path) -> list[AutopilotHistoryEntry]:
    if not path.is_file():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    out: list[AutopilotHistoryEntry] = []
    for line in lines:
        if not line.strip():
            continue
        raw = json.loads(line)
        if isinstance(raw, dict):
            out.append(AutopilotHistoryEntry.from_json_obj(raw))
    return out


def _append_history_jsonl(*, path: Path, entry: AutopilotHistoryEntry) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry.to_json_obj(), sort_keys=True) + "\n")


def _latest_runtime_evidence_summary(*, runtime_evidence_path: Path) -> tuple[int, int, float | None]:
    """Return (rollback_count, rollback_success_count, convergence_latency_ms_avg)."""

    if not runtime_evidence_path.is_file():
        return 0, 0, None

    raw = json.loads(runtime_evidence_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        return 0, 0, None

    rollback_chain_count = 0
    rollback_result_count = 0
    rollback_success_count = 0
    convergence_latencies: list[float] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        et = str(item.get("evidence_type", "")).strip()
        payload = item.get("payload")
        if et == "rollback_chain":
            rollback_chain_count += 1
        if et == "rollback_result" and isinstance(payload, dict):
            rollback_result_count += 1
            if str(payload.get("rollback_outcome", "")).strip().lower() == "rollback_applied":
                rollback_success_count += 1
        if et == "convergence_certificate" and isinstance(payload, dict) and payload.get("aggregate") is True:
            win_ms = payload.get("window_ms")
            if isinstance(win_ms, (int, float)) and not isinstance(win_ms, bool):
                convergence_latencies.append(float(win_ms))
    avg = float(sum(convergence_latencies) / float(len(convergence_latencies))) if convergence_latencies else None
    rollback_count = rollback_result_count if rollback_result_count > 0 else rollback_chain_count
    if rollback_result_count == 0:
        rollback_success_count = max(0, rollback_success_count)
    return rollback_count, rollback_success_count, avg


def make_attempt_id() -> str:
    return str(uuid.uuid4())


def _compile_fields_for_history(compile_summary: dict[str, Any]) -> dict[str, Any]:
    """Map summarize_compile_apply_from_control_plane output into AutopilotHistoryEntry kwargs."""

    return {
        "compile_realization_mode": compile_summary.get("compile_realization_mode"),
        "compile_apply_applied": compile_summary.get("compile_apply_applied"),
        "compile_apply_policy_allowed": compile_summary.get("compile_apply_policy_allowed"),
        "compile_apply_denial_reason": compile_summary.get("compile_apply_denial_reason"),
    }


def _history_tcm_float(tcm: dict[str, Any], key: str) -> float | None:
    v = tcm.get(key)
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    return None


def _history_tcm_int(tcm: dict[str, Any], key: str) -> int | None:
    v = tcm.get(key)
    if isinstance(v, int) and not isinstance(v, bool):
        return int(v)
    if isinstance(v, float) and not isinstance(v, bool) and v >= 0:
        return int(v)
    return None


def _now_ms() -> int:
    return int(time.time() * 1000)


def _budget_state_to_json(*, state: BudgetState) -> dict[str, Any]:
    return {
        "window_start_ms": int(state.window_start_ms),
        "mutations_count": int(state.mutations_count),
        "rollbacks_count": int(state.rollbacks_count),
        "consecutive_failures": int(state.consecutive_failures),
        "active_rollouts": int(state.active_rollouts),
        "human_escalation_required": bool(state.human_escalation_required),
        "cooldown_until_ms": int(state.cooldown_until_ms),
    }


def _budget_state_from_json(*, obj: dict[str, Any]) -> BudgetState:
    return BudgetState(
        window_start_ms=int(obj.get("window_start_ms", 0) or 0),
        mutations_count=int(obj.get("mutations_count", 0) or 0),
        rollbacks_count=int(obj.get("rollbacks_count", 0) or 0),
        consecutive_failures=int(obj.get("consecutive_failures", 0) or 0),
        active_rollouts=int(obj.get("active_rollouts", 0) or 0),
        human_escalation_required=bool(obj.get("human_escalation_required", False)),
        cooldown_until_ms=int(obj.get("cooldown_until_ms", 0) or 0),
    )


def _load_scope_state_file(*, state_path: Path, now_ms: int, config: AutonomyBudgetConfig) -> dict[str, Any]:
    if not state_path.is_file():
        window_start_ms = _day_window_start_ms(now_ms=now_ms, window_ms=config.budget_window_ms)
        return {
            "budget_state": _budget_state_to_json(
                state=BudgetState(window_start_ms=window_start_ms),
            ),
            "last_drift_check_at_ms": 0,
            "last_scoreboard_emitted_window_start_ms": 0,
            "lease_denied_streak": 0,
        }
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        # If state is corrupted, fail safe by resetting budgets for this window.
        window_start_ms = _day_window_start_ms(now_ms=now_ms, window_ms=config.budget_window_ms)
        return {
            "budget_state": _budget_state_to_json(state=BudgetState(window_start_ms=window_start_ms)),
            "last_drift_check_at_ms": 0,
            "last_scoreboard_emitted_window_start_ms": 0,
            "lease_denied_streak": 0,
        }
    if not isinstance(raw, dict):
        raise ValueError("autopilot state file must be a JSON object")
    budget_state_raw = raw.get("budget_state")
    if not isinstance(budget_state_raw, dict):
        return {
            "budget_state": _budget_state_to_json(
                state=BudgetState(
                    window_start_ms=_day_window_start_ms(now_ms=now_ms, window_ms=config.budget_window_ms),
                )
            ),
            "last_drift_check_at_ms": int(raw.get("last_drift_check_at_ms", 0) or 0),
            "last_scoreboard_emitted_window_start_ms": int(raw.get("last_scoreboard_emitted_window_start_ms", 0) or 0),
            "lease_denied_streak": int(raw.get("lease_denied_streak", 0) or 0),
        }
    # Ensure required keys exist.
    raw.setdefault("last_drift_check_at_ms", 0)
    raw.setdefault("last_scoreboard_emitted_window_start_ms", 0)
    raw.setdefault("lease_denied_streak", 0)
    return raw


def _save_scope_state_file(*, state_path: Path, scope_state: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(scope_state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _scope_root(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    return outputs_root.expanduser().resolve() / tenant_id.strip() / normalize_repo_id(repo_id)


def _scope_key(*, tenant_id: str, repo_id: str) -> str:
    digest = sha256(f"{tenant_id.strip()}::{repo_id.strip()}".encode()).hexdigest()
    return digest[:20]


def _scope_registry_scopes(*, scope_registry_path: Path) -> list[tuple[str, str]]:
    if not scope_registry_path.is_file():
        return []
    raw = json.loads(scope_registry_path.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise ValueError("scope registry must be a JSON array")
    out: list[tuple[str, str]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        tenant = str(item.get("tenant_id", "")).strip()
        repo = str(item.get("repo_id", "")).strip()
        if tenant and repo:
            out.append((tenant, repo))
    return sorted(set(out))


def _iter_scopes(*, outputs_root: Path, tenant_id: str | None, repo_id: str | None) -> list[tuple[str, str]]:
    out: list[tuple[str, str]] = []
    root = outputs_root.expanduser().resolve()
    if tenant_id is not None and repo_id is not None:
        out.append((tenant_id, repo_id))
        return out
    if tenant_id is not None:
        tenant_dir = root / tenant_id.strip()
        if tenant_dir.is_dir():
            for child in tenant_dir.iterdir():
                if child.is_dir() and not child.name.startswith("."):
                    out.append((tenant_dir.name, child.name))
        return sorted(out)
    # Default: discover scopes by scanning for `.akc/living/baseline.json`.
    for tchild in root.iterdir():
        if not tchild.is_dir() or tchild.name.startswith("."):
            continue
        for rchild in tchild.iterdir():
            if not rchild.is_dir() or rchild.name.startswith("."):
                continue
            candidate = rchild / ".akc" / "living" / "baseline.json"
            if candidate.is_file():
                out.append((tchild.name, rchild.name))
    # Stable ordering.
    return sorted(out)


def _read_cursors_file(*, scope_root: Path) -> dict[str, Any]:
    path = scope_root / ".akc" / "autopilot" / "cursors.json"
    if not path.is_file():
        return {
            "runtime": {
                "last_runtime_run_started_at_ms": 0,
                "last_event_timestamp": 0,
                "last_event_id": "",
                "seen_event_ids": [],
            },
            "source": {
                "last_drift_check_at_ms": 0,
            },
        }
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError("autopilot cursors must be a JSON object")
    runtime = raw.get("runtime")
    source = raw.get("source")
    if not isinstance(runtime, dict):
        runtime = {}
    if not isinstance(source, dict):
        source = {}
    return {
        "runtime": {
            "last_runtime_run_started_at_ms": int(runtime.get("last_runtime_run_started_at_ms", 0) or 0),
            "last_event_timestamp": int(runtime.get("last_event_timestamp", 0) or 0),
            "last_event_id": str(runtime.get("last_event_id", "")).strip(),
            "seen_event_ids": [
                str(item).strip() for item in cast(list[Any], runtime.get("seen_event_ids", [])) if str(item).strip()
            ][-1000:],
        },
        "source": {
            "last_drift_check_at_ms": int(source.get("last_drift_check_at_ms", 0) or 0),
        },
    }


def _write_cursors_file(*, scope_root: Path, cursors: dict[str, Any]) -> None:
    path = scope_root / ".akc" / "autopilot" / "cursors.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(cursors, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _env_truthy_autopilot(raw: str | None) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "on"}


def _autopilot_otel_mirror_callbacks() -> tuple[Callable[[str], None], ...]:
    sinks: list[Callable[[str], None]] = list(otel_export_extra_callbacks_from_env())
    if _env_truthy_autopilot(os.environ.get("AKC_AUTOPILOT_OTEL_STDOUT")):
        sinks.append(StdoutOtelExportSink().write_line)
    return tuple(sinks)


def _maybe_emit_autopilot_otel_for_decision(
    *,
    tenant_id: str,
    repo_id: str,
    decision: str,
    budget_state: BudgetState,
    now_ms: int,
    lease_denied_streak: int | None = None,
) -> None:
    callbacks = _autopilot_otel_mirror_callbacks()
    if not callbacks:
        return
    attrs: dict[str, JSONValue] = {
        "akc.autopilot.decision": decision,
        "akc.autopilot.human_escalation_required": budget_state.human_escalation_required,
        "akc.autopilot.consecutive_failures": int(budget_state.consecutive_failures),
        "akc.autopilot.mutations_count": int(budget_state.mutations_count),
        "akc.autopilot.rollbacks_count": int(budget_state.rollbacks_count),
    }
    if lease_denied_streak is not None:
        attrs["akc.autopilot.lease_denied_streak"] = int(lease_denied_streak)
    rec = autopilot_scope_event_to_export_obj(
        tenant_id=tenant_id,
        repo_id=repo_id,
        span_name=f"akc.autopilot.{decision}",
        attributes=attrs,
        now_ms=now_ms,
    )
    line = export_obj_to_json_line(rec)
    for cb in callbacks:
        cb(line)


def _maybe_emit_autopilot_otel_human_escalation(
    *,
    tenant_id: str,
    repo_id: str,
    reason: str,
    budget_state: BudgetState,
    now_ms: int,
) -> None:
    callbacks = _autopilot_otel_mirror_callbacks()
    if not callbacks:
        return
    attrs: dict[str, JSONValue] = {
        "akc.autopilot.reason": reason,
        "akc.autopilot.human_escalation_required": True,
        "akc.autopilot.consecutive_failures": int(budget_state.consecutive_failures),
    }
    rec = autopilot_scope_event_to_export_obj(
        tenant_id=tenant_id,
        repo_id=repo_id,
        span_name="akc.autopilot.human_escalation",
        attributes=attrs,
        now_ms=now_ms,
    )
    line = export_obj_to_json_line(rec)
    for cb in callbacks:
        cb(line)


def _emit_decision_artifact(
    *,
    scope_root: Path,
    now_ms: int,
    attempt_id: str,
    tenant_id: str,
    repo_id: str,
    controller_id: str,
    env_profile: str,
    decision: str,
    budget_state: BudgetState,
    extra: dict[str, Any] | None = None,
    mirror_otel: bool = True,
    lease_denied_streak: int | None = None,
) -> Path:
    body: dict[str, Any] = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "controller_id": controller_id,
        "env_profile": env_profile,
        "decision_at_ms": int(now_ms),
        "attempt_id": attempt_id,
        "decision": decision,
        "budget_state": _budget_state_to_json(state=budget_state),
    }
    if extra:
        body.update(extra)
    apply_schema_envelope(obj=body, kind="autopilot_decision", version=1)
    validate_artifact_json(obj=body, kind="autopilot_decision", version=1)
    out_dir = scope_root / ".akc" / "autopilot" / "decisions"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{int(now_ms)}.{attempt_id}.decision.json"
    out_path.write_text(json.dumps(body, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if mirror_otel:
        _maybe_emit_autopilot_otel_for_decision(
            tenant_id=tenant_id,
            repo_id=repo_id,
            decision=decision,
            budget_state=budget_state,
            now_ms=now_ms,
            lease_denied_streak=lease_denied_streak,
        )
    return out_path


def _filesystem_lease_acquire_or_renew(
    *,
    scope_root: Path,
    scope_name: str,
    controller_id: str,
    now_ms: int,
    lease_ttl_ms: int,
) -> tuple[bool, dict[str, Any]]:
    lease_dir = scope_root / ".akc" / "autopilot" / "leases"
    lease_dir.mkdir(parents=True, exist_ok=True)
    lease_path = lease_dir / f"{scope_name}.json"
    lock_path = lease_dir / f"{scope_name}.lock"
    lock_fd = os.open(str(lock_path), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        # Import locally to keep module import-time side effects minimal.
        import fcntl

        fcntl.flock(lock_fd, fcntl.LOCK_EX)
        current: dict[str, Any] = {}
        if lease_path.is_file():
            try:
                raw = json.loads(lease_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    current = raw
            except Exception:
                current = {}
        holder = str(current.get("holder_controller_id", "")).strip()
        expires_at_ms = int(current.get("expires_at_ms", 0) or 0)
        allowed = holder == controller_id or holder == "" or now_ms >= expires_at_ms
        if not allowed:
            return False, current
        next_lease = {
            "scope": scope_name,
            "holder_controller_id": controller_id,
            "acquired_at_ms": int(
                now_ms if holder != controller_id else int(current.get("acquired_at_ms", now_ms) or now_ms)
            ),
            "renewed_at_ms": int(now_ms),
            "expires_at_ms": int(now_ms + int(lease_ttl_ms)),
            "backend": "filesystem",
        }
        lease_path.write_text(json.dumps(next_lease, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return True, next_lease
    finally:
        os.close(lock_fd)


def _now_iso_utc_from_ms(*, ts_ms: int) -> str:
    return datetime.fromtimestamp(float(ts_ms) / 1000.0, tz=UTC).isoformat().replace("+00:00", "Z")


def _ms_from_iso_utc(raw: str) -> int:
    v = str(raw).strip()
    if not v:
        return 0
    try:
        if v.endswith("Z"):
            v = v[:-1] + "+00:00"
        dt = datetime.fromisoformat(v)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)
    except Exception:
        return 0


def _run_kubectl(*, args: list[str], stdin_json_obj: dict[str, Any] | None = None) -> tuple[int, str, str]:
    stdin_text = None
    if stdin_json_obj is not None:
        stdin_text = json.dumps(stdin_json_obj, sort_keys=True)
    proc = subprocess.run(
        ["kubectl", *args],
        input=stdin_text,
        text=True,
        capture_output=True,
        check=False,
    )
    return int(proc.returncode), str(proc.stdout), str(proc.stderr)


def _k8s_lease_acquire_or_renew(
    *,
    scope_name: str,
    controller_id: str,
    now_ms: int,
    lease_ttl_ms: int,
    lease_namespace: str | None,
) -> tuple[bool, dict[str, Any]]:
    namespace = str(lease_namespace or "").strip()
    if not namespace:
        raise ValueError("lease_namespace is required when lease_backend='k8s'")

    duration_s = max(1, int((int(lease_ttl_ms) + 999) / 1000))
    get_rc, get_out, get_err = _run_kubectl(
        args=["get", "lease", scope_name, "-n", namespace, "-o", "json"],
        stdin_json_obj=None,
    )
    current: dict[str, Any] = {}
    exists = get_rc == 0
    if exists:
        try:
            raw = json.loads(get_out)
            if isinstance(raw, dict):
                current = raw
        except Exception:
            current = {}
    elif "NotFound" in get_err:
        exists = False
    else:
        return False, {
            "backend": "k8s",
            "namespace": namespace,
            "name": scope_name,
            "error": f"kubectl_get_failed:{get_err.strip()}",
        }

    spec = cast(dict[str, Any], current.get("spec", {}) if isinstance(current.get("spec"), dict) else {})
    holder = str(spec.get("holderIdentity", "")).strip()
    renew_at_ms = _ms_from_iso_utc(str(spec.get("renewTime", "")).strip())
    duration_s_current = int(spec.get("leaseDurationSeconds", 0) or 0)
    expires_at_ms = renew_at_ms + max(0, duration_s_current) * 1000
    allowed = holder == controller_id or holder == "" or now_ms >= expires_at_ms
    if not allowed:
        return False, {
            "backend": "k8s",
            "namespace": namespace,
            "name": scope_name,
            "holder_controller_id": holder,
            "expires_at_ms": int(expires_at_ms),
        }

    acquire_ms = now_ms
    if holder == controller_id:
        acquire_ms = _ms_from_iso_utc(str(spec.get("acquireTime", "")).strip()) or now_ms
    obj: dict[str, Any] = {
        "apiVersion": "coordination.k8s.io/v1",
        "kind": "Lease",
        "metadata": {
            "name": scope_name,
            "namespace": namespace,
        },
        "spec": {
            "holderIdentity": controller_id,
            "acquireTime": _now_iso_utc_from_ms(ts_ms=acquire_ms),
            "renewTime": _now_iso_utc_from_ms(ts_ms=now_ms),
            "leaseDurationSeconds": duration_s,
        },
    }

    if exists:
        metadata = current.get("metadata")
        if isinstance(metadata, dict):
            rv = str(metadata.get("resourceVersion", "")).strip()
            if rv:
                cast(dict[str, Any], obj["metadata"])["resourceVersion"] = rv
        put_rc, _put_out, put_err = _run_kubectl(
            args=["replace", "-f", "-"],
            stdin_json_obj=obj,
        )
        if put_rc != 0:
            # Optimistic race fallback: re-read and return not-holder if someone else won.
            check_rc, check_out, check_err = _run_kubectl(
                args=["get", "lease", scope_name, "-n", namespace, "-o", "json"],
                stdin_json_obj=None,
            )
            if check_rc == 0:
                try:
                    chk = json.loads(check_out)
                    if isinstance(chk, dict):
                        chk_spec = chk.get("spec")
                        if isinstance(chk_spec, dict):
                            chk_holder = str(chk_spec.get("holderIdentity", "")).strip()
                            chk_renew = _ms_from_iso_utc(str(chk_spec.get("renewTime", "")).strip())
                            chk_dur = int(chk_spec.get("leaseDurationSeconds", 0) or 0)
                            chk_expires = chk_renew + max(0, chk_dur) * 1000
                            if chk_holder and chk_holder != controller_id and now_ms < chk_expires:
                                return False, {
                                    "backend": "k8s",
                                    "namespace": namespace,
                                    "name": scope_name,
                                    "holder_controller_id": chk_holder,
                                    "expires_at_ms": int(chk_expires),
                                }
                except Exception:
                    pass
            return False, {
                "backend": "k8s",
                "namespace": namespace,
                "name": scope_name,
                "error": f"kubectl_replace_failed:{put_err.strip() or check_err.strip()}",
            }
    else:
        create_rc, _create_out, create_err = _run_kubectl(
            args=["create", "-f", "-"],
            stdin_json_obj=obj,
        )
        if create_rc != 0:
            return False, {
                "backend": "k8s",
                "namespace": namespace,
                "name": scope_name,
                "error": f"kubectl_create_failed:{create_err.strip()}",
            }

    return True, {
        "backend": "k8s",
        "namespace": namespace,
        "name": scope_name,
        "holder_controller_id": controller_id,
        "acquired_at_ms": int(acquire_ms),
        "renewed_at_ms": int(now_ms),
        "expires_at_ms": int(now_ms + duration_s * 1000),
    }


def _acquire_or_renew_lease(
    *,
    scope_root: Path,
    scope_name: str,
    backend: LeaseBackend,
    controller_id: str,
    now_ms: int,
    lease_ttl_ms: int,
    lease_namespace: str | None,
) -> tuple[bool, dict[str, Any]]:
    if backend not in {"filesystem", "k8s"}:
        raise ValueError(f"unsupported lease backend: {backend}")
    if backend == "filesystem":
        return _filesystem_lease_acquire_or_renew(
            scope_root=scope_root,
            scope_name=scope_name,
            controller_id=controller_id,
            now_ms=now_ms,
            lease_ttl_ms=lease_ttl_ms,
        )
    return _k8s_lease_acquire_or_renew(
        scope_name=scope_name,
        controller_id=controller_id,
        now_ms=now_ms,
        lease_ttl_ms=lease_ttl_ms,
        lease_namespace=lease_namespace,
    )


def _load_incremental_runtime_events_for_scope(
    *,
    scope_root: Path,
    bridge: LivingRuntimeBridge,
    cursors: dict[str, Any],
    living_automation_enabled: bool = True,
) -> tuple[tuple[dict[str, Any], ...], bool, dict[str, Any]]:
    """Return (events_dicts, should_include_events_for_recompile, next_cursors).

    When ``living_automation_enabled`` is False (automation profile ``off``), events are still
    consumed for cursor advancement but are not passed to safe recompile.
    """

    runtime_dir = scope_root / ".akc" / "runtime"
    if not runtime_dir.is_dir():
        return (), False, cursors

    candidates = list(runtime_dir.rglob("runtime_run.json"))
    if not candidates:
        return (), False, cursors

    def _sort_key(p: Path) -> int:
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            ts = raw.get("started_at_ms")
            if isinstance(ts, int):
                return ts
        except Exception:
            pass
        try:
            return int(p.stat().st_mtime * 1000)
        except OSError:
            return 0

    runtime_cursor = cast(dict[str, Any], cursors.get("runtime", {}))
    last_runtime_run_started_at_ms = int(runtime_cursor.get("last_runtime_run_started_at_ms", 0) or 0)
    seen_event_ids_ordered = [
        str(item).strip() for item in cast(list[Any], runtime_cursor.get("seen_event_ids", [])) if str(item).strip()
    ]
    seen_event_ids = set(seen_event_ids_ordered)
    events_accum: list[dict[str, Any]] = []
    next_last_runtime_run_started_at_ms = int(last_runtime_run_started_at_ms)
    next_last_event_timestamp = int(runtime_cursor.get("last_event_timestamp", 0) or 0)
    next_last_event_id = str(runtime_cursor.get("last_event_id", "")).strip()

    for runtime_run in sorted(candidates, key=_sort_key):
        try:
            record = json.loads(runtime_run.read_text(encoding="utf-8"))
            started_at_ms = int(record.get("started_at_ms", 0) or 0)
            if started_at_ms < last_runtime_run_started_at_ms:
                continue
            events_path = Path(str(record.get("events_path", ""))).expanduser()
            if not events_path.is_file():
                continue
            raw_events = json.loads(events_path.read_text(encoding="utf-8"))
            if not isinstance(raw_events, list):
                continue
            for item in raw_events:
                if not isinstance(item, dict):
                    continue
                try:
                    ev = RuntimeEvent.from_json_obj(item)
                except Exception:
                    continue
                if ev.context.tenant_id.strip() != str(scope_root.parent.name).strip():
                    continue
                if ev.context.repo_id.strip() != str(scope_root.name).strip():
                    continue
                if ev.event_id in seen_event_ids:
                    continue
                if ev.timestamp < next_last_event_timestamp:
                    continue
                seen_event_ids.add(ev.event_id)
                seen_event_ids_ordered.append(ev.event_id)
                events_accum.append(item)
                next_last_event_timestamp = max(next_last_event_timestamp, int(ev.timestamp))
                next_last_event_id = ev.event_id
            next_last_runtime_run_started_at_ms = max(next_last_runtime_run_started_at_ms, started_at_ms)
        except Exception:
            continue

    events_dicts = tuple(events_accum)

    # Living bridge gate: only include runtime events when a mapped health signal exists.
    # This avoids over-triggering compilations on benign/noisy events.
    try:
        has_signal = False
        for item in events_dicts:
            ev = RuntimeEvent.from_json_obj(item)
            if bridge.derive_signal(event=ev) is not None:
                has_signal = True
                break
    except Exception:
        has_signal = True  # fail safe: include runtime events

    has_signal = bool(living_automation_enabled and has_signal)

    next_cursors = {
        "runtime": {
            "last_runtime_run_started_at_ms": int(next_last_runtime_run_started_at_ms),
            "last_event_timestamp": int(next_last_event_timestamp),
            "last_event_id": next_last_event_id,
            "seen_event_ids": seen_event_ids_ordered[-1000:],
        },
        "source": dict(cast(dict[str, Any], cursors.get("source", {}))),
    }
    return events_dicts, has_signal, next_cursors


def _latest_runtime_evidence_for_scope_after_start(
    *,
    scope_root: Path,
    compile_run_id_hint: str | None,
) -> tuple[RuntimeTerminalStatus, int, int, float | None]:
    """Return (terminal_status, rollback_count, rollback_success_count, convergence_latency_avg_ms)."""

    runtime_dir = scope_root / ".akc" / "runtime"
    if not runtime_dir.is_dir():
        return "unknown", 0, 0, None

    if compile_run_id_hint:
        base = runtime_dir / compile_run_id_hint.strip()
        if base.is_dir():
            candidates = list(base.rglob("runtime_run.json"))
        else:
            candidates = list(runtime_dir.rglob("runtime_run.json"))
    else:
        candidates = list(runtime_dir.rglob("runtime_run.json"))

    if not candidates:
        return "unknown", 0, 0, None

    def _sort_key(p: Path) -> int:
        try:
            raw = json.loads(p.read_text(encoding="utf-8"))
            ts = raw.get("started_at_ms")
            if isinstance(ts, int):
                return ts
        except Exception:
            pass
        try:
            return int(p.stat().st_mtime * 1000)
        except OSError:
            return 0

    latest = max(candidates, key=_sort_key)
    try:
        record = json.loads(latest.read_text(encoding="utf-8"))
        terminal_status = str(record.get("status", "unknown")).strip() or "unknown"
        evidence_path = Path(str(record.get("runtime_evidence_path", ""))).expanduser()
        rollback_count, rollback_success_count, conv_avg = _latest_runtime_evidence_summary(
            runtime_evidence_path=evidence_path
        )
        return cast(RuntimeTerminalStatus, terminal_status.strip()), rollback_count, rollback_success_count, conv_avg
    except Exception:
        return "unknown", 0, 0, None


def run_runtime_autopilot(
    *,
    outputs_root: str | Path,
    ingest_state_path: str | Path,
    tenant_id: str | None = None,
    repo_id: str | None = None,
    eval_suite_path: str | Path = "configs/evals/intent_system_v1.json",
    policy_mode: Literal["audit_only", "enforce"] = "enforce",
    canary_mode: Literal["quick", "thorough"] = "quick",
    accept_mode: Literal["quick", "thorough"] = "thorough",
    living_check_interval_s: float = 3600.0,
    scoreboard_window_ms: int = 7 * 24 * 60 * 60 * 1000,
    budgets: AutonomyBudgetConfig,
    max_iterations: int | None = None,
    goal: str = "Compile repository",
    verbose: bool = False,
    controller_id: str | None = None,
    lease_backend: LeaseBackend = "filesystem",
    lease_name: str | None = None,
    lease_namespace: str | None = None,
    scope_registry_path: str | Path | None = None,
    env_profile: EnvProfile = "staging",
    lease_ttl_ms: int = 15_000,
    living_automation_profile: LivingAutomationProfile | None = None,
    reliability_slo_gate: ReliabilitySLOGateConfig | None = None,
) -> int:
    """Always-on runtime controller loop (Plan 3).

    This controller:
    - periodically safe-recompiles when a runtime living-bridge health signal exists
      (and always includes source drift checks via `safe_recompile_on_drift` fast-path)
    - starts mutating runtime rollouts only when autonomy budgets allow
    - emits reliability-scoreboard artifacts per KPI window

    Notes:
    - Runtime execution uses the existing CLI `akc runtime start` implementation
      to keep evidence + attestation artifacts consistent.
    """

    from argparse import Namespace

    outputs_root_p = Path(outputs_root).expanduser().resolve()
    ingest_state_path_p = Path(ingest_state_path).expanduser().resolve()
    if not ingest_state_path_p.is_file():
        raise ValueError(f"ingest_state_path does not exist: {ingest_state_path_p}")

    if scoreboard_window_ms <= 0:
        raise ValueError("scoreboard_window_ms must be > 0")

    profile = (
        living_automation_profile
        if living_automation_profile is not None
        else resolve_living_automation_profile(cli_value=None, env=os.environ, project_value=None)
    )

    bridge = default_living_runtime_bridge()

    controller_id_value = (
        controller_id.strip() if isinstance(controller_id, str) and controller_id.strip() else make_attempt_id()
    )

    # Lazy import to avoid cli/runtime import cost until we actually run.
    from akc.cli.runtime import cmd_runtime_start

    check_interval_ms = int(float(living_check_interval_s) * 1000)
    now_ms = _now_ms()

    iterations = 0
    while True:
        if max_iterations is not None and iterations >= int(max_iterations):
            return 0
        iterations += 1

        scope_pairs = (
            _scope_registry_scopes(scope_registry_path=Path(scope_registry_path).expanduser().resolve())
            if scope_registry_path is not None
            else _iter_scopes(outputs_root=outputs_root_p, tenant_id=tenant_id, repo_id=repo_id)
        )
        if not scope_pairs:
            if verbose:
                print("autopilot: no tenant/repo scopes found (missing baseline.json?)")
            time.sleep(0.25)
            continue

        now_ms = _now_ms()
        for tid, rid in scope_pairs:
            scope_r = _scope_root(outputs_root=outputs_root_p, tenant_id=tid, repo_id=rid)
            scope_name = (
                lease_name.strip()
                if isinstance(lease_name, str) and lease_name.strip()
                else _scope_key(tenant_id=tid, repo_id=rid)
            )
            state_path = scope_r / ".akc" / "autopilot" / "state.json"
            cursors = _read_cursors_file(scope_root=scope_r)
            scope_state = _load_scope_state_file(state_path=state_path, now_ms=now_ms, config=budgets)

            lease_ok, lease_diag = _acquire_or_renew_lease(
                scope_root=scope_r,
                scope_name=scope_name,
                backend=lease_backend,
                controller_id=controller_id_value,
                now_ms=now_ms,
                lease_ttl_ms=lease_ttl_ms,
                lease_namespace=lease_namespace,
            )
            if not lease_ok:
                budget_state_lease = _budget_state_from_json(
                    obj=cast(dict[str, Any], scope_state["budget_state"]),
                )
                streak = int(scope_state.get("lease_denied_streak", 0) or 0) + 1
                scope_state["lease_denied_streak"] = int(streak)
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
                _emit_decision_artifact(
                    scope_root=scope_r,
                    now_ms=now_ms,
                    attempt_id=make_attempt_id(),
                    tenant_id=tid,
                    repo_id=rid,
                    controller_id=controller_id_value,
                    env_profile=env_profile,
                    decision="skip_not_lease_holder",
                    budget_state=budget_state_lease,
                    extra={
                        "lease_backend": lease_backend,
                        "lease_namespace": lease_namespace,
                        "scope_name": scope_name,
                        "lease": lease_diag,
                    },
                    lease_denied_streak=streak,
                )
                continue

            if int(scope_state.get("lease_denied_streak", 0) or 0) != 0:
                scope_state["lease_denied_streak"] = 0
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
            last_drift_check_at_ms = int(scope_state.get("last_drift_check_at_ms", 0) or 0)
            last_scoreboard_emitted_window_start_ms = int(
                scope_state.get("last_scoreboard_emitted_window_start_ms", 0) or 0
            )

            if now_ms - last_drift_check_at_ms < check_interval_ms:
                continue

            # Persist the drift-check clock early to avoid tight loops on exceptions.
            scope_state["last_drift_check_at_ms"] = int(now_ms)
            cast(dict[str, Any], cursors["source"])["last_drift_check_at_ms"] = int(now_ms)
            _write_cursors_file(scope_root=scope_r, cursors=cursors)
            _save_scope_state_file(state_path=state_path, scope_state=scope_state)

            budget_state = _budget_state_from_json(obj=cast(dict[str, Any], scope_state["budget_state"]))

            if budget_state.human_escalation_required:
                _emit_decision_artifact(
                    scope_root=scope_r,
                    now_ms=now_ms,
                    attempt_id=make_attempt_id(),
                    tenant_id=tid,
                    repo_id=rid,
                    controller_id=controller_id_value,
                    env_profile=env_profile,
                    decision="escalation_hold",
                    budget_state=budget_state,
                    extra={
                        "note": "human_escalation_required; controller skips mutating work until state is cleared",
                    },
                )
                continue

            # Living-bridge gate for including runtime transcript as part of recompile policy.
            (
                runtime_events_dicts,
                should_include_runtime_events,
                next_cursors,
            ) = _load_incremental_runtime_events_for_scope(
                scope_root=scope_r,
                bridge=bridge,
                cursors=cursors,
                living_automation_enabled=living_automation_includes_runtime_bridge(profile=profile),
            )
            _write_cursors_file(scope_root=scope_r, cursors=next_cursors)
            runtime_events = runtime_events_dicts if should_include_runtime_events else None

            # Detect whether acceptance compile produced a new manifest.
            prev_manifest_path = find_latest_run_manifest(
                outputs_root=outputs_root_p,
                tenant_id=tid,
                repo_id=rid,
            )

            try:
                safe_recompile_on_drift(
                    tenant_id=tid,
                    repo_id=rid,
                    outputs_root=outputs_root_p,
                    ingest_state_path=ingest_state_path_p,
                    baseline_path=None,
                    eval_suite_path=eval_suite_path,
                    goal=goal,
                    policy_mode=policy_mode,
                    canary_mode=canary_mode,
                    accept_mode=accept_mode,
                    allow_network=False,
                    update_baseline_on_accept=True,
                    skip_other_pending=True,
                    runtime_events=runtime_events,
                    living_automation_profile=profile,
                )
            except Exception:
                # Compile failure should still back off to avoid tight failure loops.
                consecutive_failures = int(budget_state.consecutive_failures) + 1
                human_escalation_required = bool(consecutive_failures >= int(budgets.max_consecutive_rollout_failures))
                budget_state = BudgetState(
                    window_start_ms=budget_state.window_start_ms,
                    mutations_count=budget_state.mutations_count,
                    rollbacks_count=budget_state.rollbacks_count,
                    consecutive_failures=consecutive_failures,
                    active_rollouts=budget_state.active_rollouts,
                    human_escalation_required=human_escalation_required,
                    cooldown_until_ms=int(now_ms + int(budgets.cooldown_after_failure_ms)),
                )
                scope_state["budget_state"] = _budget_state_to_json(state=budget_state)
                scope_state["last_scoreboard_emitted_window_start_ms"] = int(last_scoreboard_emitted_window_start_ms)
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
                _emit_decision_artifact(
                    scope_root=scope_r,
                    now_ms=now_ms,
                    attempt_id=make_attempt_id(),
                    tenant_id=tid,
                    repo_id=rid,
                    controller_id=controller_id_value,
                    env_profile=env_profile,
                    decision="compile_failed",
                    budget_state=budget_state,
                    extra={
                        "compile_inputs": {
                            "runtime_event_count": len(runtime_events or ()),
                            "policy_mode": policy_mode,
                            "canary_mode": canary_mode,
                            "accept_mode": accept_mode,
                        },
                    },
                )
                continue

            next_manifest_path = find_latest_run_manifest(
                outputs_root=outputs_root_p,
                tenant_id=tid,
                repo_id=rid,
            )
            if next_manifest_path is None or prev_manifest_path == next_manifest_path:
                # Nothing accepted; update scoreboard window if needed.
                _maybe_emit_scoreboard(
                    scope_root=scope_r,
                    tenant_id=tid,
                    repo_id=rid,
                    now_ms=now_ms,
                    scoreboard_window_ms=scoreboard_window_ms,
                    scope_state=scope_state,
                    state_path=state_path,
                )
                _emit_decision_artifact(
                    scope_root=scope_r,
                    now_ms=now_ms,
                    attempt_id=make_attempt_id(),
                    tenant_id=tid,
                    repo_id=rid,
                    controller_id=controller_id_value,
                    env_profile=env_profile,
                    decision="no_new_manifest",
                    budget_state=budget_state,
                    extra={
                        "compile_inputs": {
                            "runtime_event_count": len(runtime_events or ()),
                            "policy_mode": policy_mode,
                            "canary_mode": canary_mode,
                            "accept_mode": accept_mode,
                        },
                    },
                )
                continue

            # Decide whether a new rollout should start based on promotion packet metadata.
            try:
                manifest = load_run_manifest(
                    path=next_manifest_path,
                    expected_tenant_id=tid,
                    expected_repo_id=rid,
                )
            except Exception:
                continue

            cp = manifest.control_plane if isinstance(manifest.control_plane, dict) else {}
            compile_summary = summarize_compile_apply_from_control_plane(cp if isinstance(cp, dict) else None)
            promotion_mode = str(cp.get("promotion_mode", "")).strip()
            promotion_policy_allow = bool(cp.get("promotion_policy_allow", False))

            prevented_reason: str | None = None
            if promotion_mode != "live_apply":
                prevented_reason = f"promotion_mode_not_live_apply:{promotion_mode}"
            elif isinstance(cp, dict):
                compile_denial = compile_apply_attestation_denial_for_rollout(
                    scope_root=scope_r,
                    manifest_control_plane=cp,
                )
                if compile_denial is not None:
                    prevented_reason = f"compile_apply_denied:{compile_denial}"
            if prevented_reason is None and not promotion_policy_allow:
                prevented_reason = "promotion_policy_not_allowed"

            history_path = scope_r / ".akc" / "autopilot" / "history.jsonl"
            if prevented_reason is not None:
                entry = AutopilotHistoryEntry(
                    event_kind="promotion_prevented",
                    attempt_id=make_attempt_id(),
                    started_at_ms=now_ms,
                    prevented_reason=prevented_reason,
                    **_compile_fields_for_history(compile_summary),
                )
                _append_history_jsonl(path=history_path, entry=entry)

                # Policy denies should cool down a bit to avoid repeated denies.
                cooldown_until = int(now_ms + int(budgets.cooldown_after_policy_deny_ms))
                budget_state = BudgetState(
                    window_start_ms=budget_state.window_start_ms,
                    mutations_count=budget_state.mutations_count,
                    rollbacks_count=budget_state.rollbacks_count,
                    consecutive_failures=budget_state.consecutive_failures,
                    active_rollouts=budget_state.active_rollouts,
                    human_escalation_required=budget_state.human_escalation_required,
                    cooldown_until_ms=cooldown_until,
                )
                scope_state["budget_state"] = _budget_state_to_json(state=budget_state)
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
                _maybe_emit_scoreboard(
                    scope_root=scope_r,
                    tenant_id=tid,
                    repo_id=rid,
                    now_ms=now_ms,
                    scoreboard_window_ms=scoreboard_window_ms,
                    scope_state=scope_state,
                    state_path=state_path,
                )
                _emit_decision_artifact(
                    scope_root=scope_r,
                    now_ms=now_ms,
                    attempt_id=entry.attempt_id,
                    tenant_id=tid,
                    repo_id=rid,
                    controller_id=controller_id_value,
                    env_profile=env_profile,
                    decision="promotion_prevented",
                    budget_state=budget_state,
                    extra={
                        "reason": prevented_reason,
                        "compile_apply": compile_summary,
                    },
                )
                continue

            # Budgets guard: start runtime rollout only if allowed.
            if reliability_slo_gate is not None:
                try:
                    history_for_gate = _load_history_jsonl(path=history_path)
                    gate_allowed, gate_status, gate_reason, scoreboard = _slo_gate_allows_rollout(
                        now_ms=now_ms,
                        scoreboard_window_ms=scoreboard_window_ms,
                        tenant_id=tid,
                        repo_id=rid,
                        history=history_for_gate,
                        gate=reliability_slo_gate,
                    )
                except Exception:
                    gate_allowed = False
                    gate_status = "error"
                    gate_reason = "slo_gate_evaluation_error"
                    scoreboard = compute_reliability_scoreboard(
                        tenant_id=tid,
                        repo_id=rid,
                        window_start_ms=int(max(0, int(now_ms) - int(scoreboard_window_ms))),
                        window_end_ms=int(now_ms),
                        history=[],
                    )
                if not gate_allowed:
                    entry = AutopilotHistoryEntry(
                        event_kind="promotion_prevented",
                        attempt_id=make_attempt_id(),
                        started_at_ms=now_ms,
                        prevented_reason=f"slo_gate:{gate_status}:{gate_reason}",
                        **_compile_fields_for_history(compile_summary),
                    )
                    _append_history_jsonl(path=history_path, entry=entry)
                    _emit_decision_artifact(
                        scope_root=scope_r,
                        now_ms=now_ms,
                        attempt_id=entry.attempt_id,
                        tenant_id=tid,
                        repo_id=rid,
                        controller_id=controller_id_value,
                        env_profile=env_profile,
                        decision="slo_gate_prevented",
                        budget_state=budget_state,
                        extra={
                            "reason": gate_reason,
                            "gate_status": gate_status,
                            "gate_config": reliability_slo_gate.__dict__,
                            "scoreboard_kpi": dict(scoreboard.kpi),
                        },
                    )
                    continue

            allowed, reason, budget_state2 = budget_guard_for_start(
                now_ms=now_ms,
                state=budget_state,
                config=budgets,
                proposed_rollout_rollback_count=0,
            )
            if not allowed:
                entry = AutopilotHistoryEntry(
                    event_kind="promotion_prevented",
                    attempt_id=make_attempt_id(),
                    started_at_ms=now_ms,
                    prevented_reason=f"budget_prevented:{reason}",
                    **_compile_fields_for_history(compile_summary),
                )
                _append_history_jsonl(path=history_path, entry=entry)
                scope_state["budget_state"] = _budget_state_to_json(state=budget_state2)
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
                _maybe_emit_scoreboard(
                    scope_root=scope_r,
                    tenant_id=tid,
                    repo_id=rid,
                    now_ms=now_ms,
                    scoreboard_window_ms=scoreboard_window_ms,
                    scope_state=scope_state,
                    state_path=state_path,
                )
                _emit_decision_artifact(
                    scope_root=scope_r,
                    now_ms=now_ms,
                    attempt_id=entry.attempt_id,
                    tenant_id=tid,
                    repo_id=rid,
                    controller_id=controller_id_value,
                    env_profile=env_profile,
                    decision="budget_prevented",
                    budget_state=budget_state2,
                    extra={
                        "reason": reason,
                        "compile_apply": compile_summary,
                    },
                )
                continue

            # Load promotion packet to resolve runtime bundle path.
            cp_ref = cp.get("promotion_packet_ref")
            if not isinstance(cp_ref, dict):
                entry = AutopilotHistoryEntry(
                    event_kind="promotion_prevented",
                    attempt_id=make_attempt_id(),
                    started_at_ms=now_ms,
                    prevented_reason="missing_promotion_packet_ref",
                    **_compile_fields_for_history(compile_summary),
                )
                _append_history_jsonl(path=history_path, entry=entry)
                scope_state["budget_state"] = _budget_state_to_json(state=budget_state2)
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
                continue
            pp_path_rel = str(cp_ref.get("path", "")).strip()
            if not pp_path_rel:
                entry = AutopilotHistoryEntry(
                    event_kind="promotion_prevented",
                    attempt_id=make_attempt_id(),
                    started_at_ms=now_ms,
                    prevented_reason="empty_promotion_packet_ref.path",
                    **_compile_fields_for_history(compile_summary),
                )
                _append_history_jsonl(path=history_path, entry=entry)
                scope_state["budget_state"] = _budget_state_to_json(state=budget_state2)
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
                continue
            pp_path = (scope_r / pp_path_rel).expanduser().resolve()
            pp_obj = json.loads(pp_path.read_text(encoding="utf-8"))
            apply_target_metadata = pp_obj.get("apply_target_metadata")
            runtime_bundle_relpath = ""
            if isinstance(apply_target_metadata, dict):
                rb = apply_target_metadata.get("runtime_bundle_path")
                if isinstance(rb, str):
                    runtime_bundle_relpath = rb.strip()
            if not runtime_bundle_relpath:
                entry = AutopilotHistoryEntry(
                    event_kind="promotion_prevented",
                    attempt_id=make_attempt_id(),
                    started_at_ms=now_ms,
                    prevented_reason="missing_runtime_bundle_path_in_promotion_packet",
                    **_compile_fields_for_history(compile_summary),
                )
                _append_history_jsonl(path=history_path, entry=entry)
                scope_state["budget_state"] = _budget_state_to_json(state=budget_state2)
                _save_scope_state_file(state_path=state_path, scope_state=scope_state)
                continue

            bundle_path = (scope_r / runtime_bundle_relpath).expanduser().resolve()

            # Start mutating runtime.
            runtime_started_at = _now_ms()
            attempt_id = make_attempt_id()
            try:
                cmd_runtime_start(
                    Namespace(
                        bundle=str(bundle_path),
                        mode="enforce",
                        outputs_root=str(outputs_root_p),
                        strict_intent_authority=False,
                        verbose=verbose,
                    )
                )
            except SystemExit:
                pass
            except Exception:
                pass

            # Compute runtime outcome metrics for budget + scoreboard.
            (
                terminal_status,
                rollback_count,
                rollback_success_count,
                conv_avg,
            ) = _latest_runtime_evidence_for_scope_after_start(
                scope_root=scope_r,
                compile_run_id_hint=manifest.run_id,
            )

            # Update budget state.
            budget_state3 = budget_guard_after_runtime_outcome(
                now_ms=now_ms,
                state=budget_state2,
                config=budgets,
                rollout_started_allowed=True,
                runtime_terminal_status=terminal_status,
                rollback_count=rollback_count,
            )
            scope_state["budget_state"] = _budget_state_to_json(state=budget_state3)
            _save_scope_state_file(state_path=state_path, scope_state=scope_state)

            # Append rollout attempt to history.
            manifest_post_runtime = manifest
            try:
                manifest_post_runtime = load_run_manifest(
                    path=next_manifest_path,
                    expected_tenant_id=tid,
                    expected_repo_id=rid,
                )
            except Exception:
                manifest_post_runtime = manifest
            cp_post = (
                manifest_post_runtime.control_plane if isinstance(manifest_post_runtime.control_plane, dict) else {}
            )
            raw_tcm = cp_post.get("time_compression_metrics") if isinstance(cp_post, dict) else None
            tcm: dict[str, Any] = dict(raw_tcm) if isinstance(raw_tcm, dict) else {}

            history_entry = AutopilotHistoryEntry(
                event_kind="runtime_rollout",
                attempt_id=attempt_id,
                started_at_ms=runtime_started_at,
                promotion_policy_allow=True,
                runtime_terminal_status=terminal_status if isinstance(terminal_status, str) else "unknown",
                rollback_count=rollback_count,
                rollback_success_count=rollback_success_count,
                convergence_latency_ms=conv_avg,
                intent_to_healthy_runtime_ms=_history_tcm_float(tcm, "intent_to_healthy_runtime_ms"),
                compile_to_healthy_runtime_ms=_history_tcm_float(tcm, "compile_to_healthy_runtime_ms"),
                compression_factor_vs_baseline=_history_tcm_float(tcm, "compression_factor_vs_baseline"),
                intent_to_staging_ms=_history_tcm_float(tcm, "intent_to_staging_ms"),
                intent_to_prod_ms=_history_tcm_float(tcm, "intent_to_prod_ms"),
                staging_to_prod_ms=_history_tcm_float(tcm, "staging_to_prod_ms"),
                approval_wait_ms=_history_tcm_float(tcm, "approval_wait_ms"),
                manual_touch_count=_history_tcm_int(tcm, "manual_touch_count"),
                **_compile_fields_for_history(compile_summary),
            )
            _append_history_jsonl(path=history_path, entry=history_entry)
            _emit_decision_artifact(
                scope_root=scope_r,
                now_ms=now_ms,
                attempt_id=attempt_id,
                tenant_id=tid,
                repo_id=rid,
                controller_id=controller_id_value,
                env_profile=env_profile,
                decision="runtime_rollout",
                budget_state=budget_state3,
                extra={
                    "runtime_outcome": {
                        "terminal_status": terminal_status,
                        "rollback_count": rollback_count,
                        "rollback_success_count": rollback_success_count,
                        "convergence_latency_ms_avg": conv_avg,
                    },
                    "compile_apply": compile_summary,
                },
            )

            if budget_state3.human_escalation_required:
                _emit_human_escalation(
                    scope_root=scope_r,
                    tenant_id=tid,
                    repo_id=rid,
                    now_ms=now_ms,
                    budget_state=budget_state3,
                    reason="autonomy_budget_escalation",
                )

            _maybe_emit_scoreboard(
                scope_root=scope_r,
                tenant_id=tid,
                repo_id=rid,
                now_ms=now_ms,
                scoreboard_window_ms=scoreboard_window_ms,
                scope_state=scope_state,
                state_path=state_path,
            )

        # Avoid CPU spin.
        time.sleep(0.25)


def _maybe_emit_scoreboard(
    *,
    scope_root: Path,
    tenant_id: str,
    repo_id: str,
    now_ms: int,
    scoreboard_window_ms: int,
    scope_state: dict[str, Any],
    state_path: Path,
) -> None:
    last_emitted_start = int(scope_state.get("last_scoreboard_emitted_window_start_ms", 0) or 0)
    current_window_start = _day_window_start_ms(now_ms=now_ms, window_ms=scoreboard_window_ms)
    if last_emitted_start == 0:
        scope_state["last_scoreboard_emitted_window_start_ms"] = int(current_window_start)
        _save_scope_state_file(state_path=state_path, scope_state=scope_state)
        return
    if current_window_start <= last_emitted_start:
        return

    history_path = scope_root / ".akc" / "autopilot" / "history.jsonl"
    history = _load_history_jsonl(path=history_path)
    prev_start = last_emitted_start
    prev_end = current_window_start

    emit_reliability_scoreboard_artifact(
        scope_root=scope_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        window_start_ms=prev_start,
        window_end_ms=prev_end,
        history=history,
    )

    scope_state["last_scoreboard_emitted_window_start_ms"] = int(current_window_start)
    _save_scope_state_file(state_path=state_path, scope_state=scope_state)


def _emit_human_escalation(
    *,
    scope_root: Path,
    tenant_id: str,
    repo_id: str,
    now_ms: int,
    budget_state: BudgetState,
    reason: str,
) -> None:
    out_dir = scope_root / ".akc" / "autopilot" / "escalations"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{int(now_ms)}.{reason}.json"
    payload = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "generated_at_ms": int(now_ms),
        "reason": reason,
        "budget_state": _budget_state_to_json(state=budget_state),
    }
    apply_schema_envelope(obj=payload, kind="autopilot_human_escalation", version=1)
    validate_artifact_json(obj=payload, kind="autopilot_human_escalation", version=1)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    _maybe_emit_autopilot_otel_human_escalation(
        tenant_id=tenant_id,
        repo_id=repo_id,
        reason=reason,
        budget_state=budget_state,
        now_ms=now_ms,
    )
