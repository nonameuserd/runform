from __future__ import annotations

from akc.runtime.autopilot import AutopilotHistoryEntry, compute_reliability_scoreboard


def test_compute_reliability_scoreboard_kpis() -> None:
    tenant_id = "t1"
    repo_id = "r1"
    start_ms = 1_000
    end_ms = 2_000

    history = [
        AutopilotHistoryEntry(
            event_kind="runtime_rollout",
            attempt_id="rollout-allowed-no-rollback",
            started_at_ms=1_100,
            promotion_policy_allow=True,
            runtime_terminal_status="succeeded",
            rollback_count=0,
            convergence_latency_ms=100.0,
        ),
        AutopilotHistoryEntry(
            event_kind="runtime_rollout",
            attempt_id="rollout-allowed-with-rollback",
            started_at_ms=1_200,
            promotion_policy_allow=True,
            runtime_terminal_status="succeeded",
            rollback_count=2,
            convergence_latency_ms=300.0,
        ),
        AutopilotHistoryEntry(
            event_kind="promotion_prevented",
            attempt_id="prevented-policy-deny",
            started_at_ms=1_300,
            prevented_reason="policy_denied",
        ),
    ]

    sb = compute_reliability_scoreboard(
        tenant_id=tenant_id,
        repo_id=repo_id,
        window_start_ms=start_ms,
        window_end_ms=end_ms,
        history=history,
    )

    assert sb.tenant_id == tenant_id
    assert sb.repo_id == repo_id
    assert sb.window_start_ms == start_ms
    assert sb.window_end_ms == end_ms

    assert sb.kpi["rollouts_total"] == 2
    assert sb.kpi["rollouts_with_rollback"] == 1
    assert sb.kpi["rollbacks_total"] == 2
    assert sb.kpi["rollback_success_rate"] == 0.0
    assert sb.kpi["failed_promotions_prevented"] == 1
    assert sb.kpi["mutation_attempts_blocked_by_policy"] == 1
    assert sb.kpi["compile_scoped_apply_rollouts_total"] == 0
    assert sb.kpi["compile_apply_gate_prevented_total"] == 0

    assert sb.kpi["policy_compliance_rate"] == 1.0
    assert sb.kpi["convergence_latency_ms_avg"] == 200.0  # (100 + 300) / 2
    assert sb.kpi["mttr_like_repair_latency_ms_avg"] == 300.0  # only rollback rollout
    assert sb.kpi["intent_to_healthy_runtime_ms_p50"] == 0.0
    assert sb.kpi["compression_factor_vs_baseline_avg"] == 0.0
    assert sb.kpi["delivery_change_instability_proxy"] == 0.5
    assert sb.kpi["manual_touch_count_total"] == 0
    assert "delivery_improvement_signals_note" in sb.kpi
