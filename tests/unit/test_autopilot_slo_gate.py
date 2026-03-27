from __future__ import annotations

from akc.runtime.autopilot import (
    AutopilotHistoryEntry,
    ReliabilitySLOGateConfig,
    _slo_gate_allows_rollout,
)


def _rollout(
    *,
    started_at_ms: int,
    promotion_policy_allow: bool,
    rollback_count: int = 0,
    rollback_success_count: int = 0,
) -> AutopilotHistoryEntry:
    return AutopilotHistoryEntry(
        event_kind="runtime_rollout",
        attempt_id=f"att-{started_at_ms}",
        started_at_ms=started_at_ms,
        promotion_policy_allow=promotion_policy_allow,
        runtime_terminal_status="succeeded",
        rollback_count=rollback_count,
        rollback_success_count=rollback_success_count,
    )


def test_slo_gate_insufficient_history_prevents() -> None:
    gate = ReliabilitySLOGateConfig(min_rollouts_total=5)
    ok, status, reason, scoreboard = _slo_gate_allows_rollout(
        now_ms=10_000,
        scoreboard_window_ms=10_000,
        tenant_id="t",
        repo_id="r",
        history=[_rollout(started_at_ms=9_000, promotion_policy_allow=True)],
        gate=gate,
    )
    assert ok is False
    assert status == "insufficient_history"
    assert reason == "min_rollouts_total_not_met"
    assert int(scoreboard.kpi["rollouts_total"]) == 1


def test_slo_gate_policy_compliance_blocks() -> None:
    gate = ReliabilitySLOGateConfig(min_rollouts_total=5, min_policy_compliance_rate=1.0)
    hist = [
        _rollout(started_at_ms=1_000, promotion_policy_allow=True),
        _rollout(started_at_ms=2_000, promotion_policy_allow=True),
        _rollout(started_at_ms=3_000, promotion_policy_allow=True),
        _rollout(started_at_ms=4_000, promotion_policy_allow=True),
        _rollout(started_at_ms=5_000, promotion_policy_allow=False),
    ]
    ok, status, reason, _scoreboard = _slo_gate_allows_rollout(
        now_ms=10_000,
        scoreboard_window_ms=10_000,
        tenant_id="t",
        repo_id="r",
        history=hist,
        gate=gate,
    )
    assert ok is False
    assert status == "prevented"
    assert reason == "policy_compliance_rate_below_threshold"


def test_slo_gate_allows_when_thresholds_met() -> None:
    gate = ReliabilitySLOGateConfig(
        min_rollouts_total=5,
        min_policy_compliance_rate=0.9,
        min_rollback_success_rate=0.9,
        max_delivery_change_instability_proxy=0.3,
    )
    hist = [
        _rollout(started_at_ms=1_000, promotion_policy_allow=True),
        _rollout(started_at_ms=2_000, promotion_policy_allow=True),
        _rollout(started_at_ms=3_000, promotion_policy_allow=True),
        _rollout(started_at_ms=4_000, promotion_policy_allow=True),
        _rollout(started_at_ms=5_000, promotion_policy_allow=True, rollback_count=1, rollback_success_count=1),
    ]
    ok, status, reason, _scoreboard = _slo_gate_allows_rollout(
        now_ms=10_000,
        scoreboard_window_ms=10_000,
        tenant_id="t",
        repo_id="r",
        history=hist,
        gate=gate,
    )
    assert ok is True
    assert status == "allowed"
    assert reason == "allowed"
