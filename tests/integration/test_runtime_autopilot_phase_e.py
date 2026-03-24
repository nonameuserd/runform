"""Phase E — autopilot integration: mocked safe recompile, drift interval, profile resolution, escalation.

Lease contention (two controllers, filesystem) is covered in
``test_runtime_autopilot_lease_single_writer.py``; this module adds end-to-end checks
for drift pacing, ``living_automation_profile`` wiring, profile-off transcript gating,
and compile-failure escalation.
"""

from __future__ import annotations

import json
from hashlib import sha256
from pathlib import Path
from typing import Any

from akc.living.automation_profile import PROFILE_LIVING_LOOP_V1, PROFILE_OFF
from akc.runtime.autopilot import AutonomyBudgetConfig, run_runtime_autopilot


def _budgets(**overrides: Any) -> AutonomyBudgetConfig:
    base: dict[str, Any] = {
        "max_mutations_per_day": 10,
        "max_concurrent_rollouts": 1,
        "rollback_budget_per_day": 10,
        "max_consecutive_rollout_failures": 3,
        "max_rollbacks_per_day_before_escalation": 3,
        "cooldown_after_failure_ms": 1000,
        "cooldown_after_policy_deny_ms": 1000,
    }
    base.update(overrides)
    return AutonomyBudgetConfig(**base)


def _scope_fixture(tmp_path: Path) -> tuple[Path, Path, Path]:
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    ingest_state_path = tmp_path / "ingest_state.json"
    ingest_state_path.write_text("{}\n", encoding="utf-8")
    scope_registry_path = tmp_path / "scope_registry.json"
    scope_registry_path.write_text(
        json.dumps([{"tenant_id": "tenant-a", "repo_id": "repo-a"}], indent=2) + "\n",
        encoding="utf-8",
    )
    return outputs_root, ingest_state_path, scope_registry_path


def _write_failure_runtime_event(scope_root: Path) -> None:
    """Emit one bridge-visible failure event under ``.akc/runtime`` (see unit autopilot tests)."""
    runtime_dir = scope_root / ".akc" / "runtime" / "run1"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    events_path = runtime_dir / "events.json"
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    fail_event = {
        "event_id": "evt-fail",
        "event_type": "runtime.action.failed",
        "timestamp": 100,
        "context": {
            "tenant_id": tenant_id,
            "repo_id": repo_id,
            "run_id": "run-1",
            "runtime_run_id": "rt-1",
            "policy_mode": "enforce",
            "adapter_id": "native",
        },
        "payload": {"reason": "boom"},
    }
    events_path.write_text(json.dumps([fail_event], indent=2) + "\n", encoding="utf-8")
    (runtime_dir / "runtime_run.json").write_text(
        json.dumps(
            {
                "started_at_ms": 101,
                "events_path": str(events_path),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def test_runtime_autopilot_e2e_mocked_recompile_profile_lease_and_drift_interval(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """Small ``max_iterations``, mocked ``safe_recompile_on_drift``, lease holder, drift pacing, profile."""
    outputs_root, ingest_state_path, scope_registry_path = _scope_fixture(tmp_path)

    compile_calls: list[dict[str, Any]] = []

    def _fake_safe_recompile_on_drift(**kwargs: Any) -> None:
        compile_calls.append(
            {
                "tenant_id": kwargs.get("tenant_id"),
                "repo_id": kwargs.get("repo_id"),
                "living_automation_profile": getattr(kwargs.get("living_automation_profile"), "id", None),
                "runtime_events": kwargs.get("runtime_events"),
            }
        )

    monkeypatch.setattr("akc.runtime.autopilot.safe_recompile_on_drift", _fake_safe_recompile_on_drift)

    clock_ms = [1_000_000_000]

    def _fake_now_ms() -> int:
        return int(clock_ms[0])

    def _fake_sleep(seconds: float) -> None:
        clock_ms[0] += int(float(seconds) * 1000)

    monkeypatch.setattr("akc.runtime.autopilot._now_ms", _fake_now_ms)
    monkeypatch.setattr("akc.runtime.autopilot.time.sleep", _fake_sleep)

    rc = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="filesystem",
        env_profile="staging",
        budgets=_budgets(),
        living_check_interval_s=1.0,
        max_iterations=5,
        living_automation_profile=PROFILE_LIVING_LOOP_V1,
    )
    assert rc == 0

    assert len(compile_calls) == 2, "drift should run twice once 1s elapses across sleeps"
    assert all(c["living_automation_profile"] == "living_loop_v1" for c in compile_calls)
    assert all(c["tenant_id"] == "tenant-a" and c["repo_id"] == "repo-a" for c in compile_calls)

    scope_root = outputs_root / "tenant-a" / "repo-a"
    scope_name = sha256(b"tenant-a::repo-a").hexdigest()[:20]
    lease_path = scope_root / ".akc" / "autopilot" / "leases" / f"{scope_name}.json"
    assert lease_path.is_file()
    lease_obj = json.loads(lease_path.read_text(encoding="utf-8"))
    assert lease_obj.get("holder_controller_id") == "controller-a"

    state_path = scope_root / ".akc" / "autopilot" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    assert int(state.get("last_drift_check_at_ms", 0) or 0) > 0
    assert int(state.get("last_drift_check_at_ms", 0) or 0) <= clock_ms[0]


def test_runtime_autopilot_profile_off_runtime_events_not_passed_to_safe_recompile(
    tmp_path: Path, monkeypatch: Any
) -> None:
    """With automation profile ``off``, incremental runtime events are not fed into drift recompile."""
    outputs_root, ingest_state_path, scope_registry_path = _scope_fixture(tmp_path)
    scope_root = outputs_root / "tenant-a" / "repo-a"
    _write_failure_runtime_event(scope_root)

    captured: list[Any] = []

    def _fake_safe_recompile_on_drift(**kwargs: Any) -> None:
        captured.append(kwargs.get("runtime_events"))

    monkeypatch.setattr("akc.runtime.autopilot.safe_recompile_on_drift", _fake_safe_recompile_on_drift)

    rc = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="filesystem",
        env_profile="staging",
        budgets=_budgets(),
        living_check_interval_s=0.0,
        max_iterations=1,
        living_automation_profile=PROFILE_OFF,
    )
    assert rc == 0
    assert len(captured) == 1
    assert captured[0] is None


def test_runtime_autopilot_escalation_after_max_consecutive_compile_failures(tmp_path: Path, monkeypatch: Any) -> None:
    """Repeated ``safe_recompile_on_drift`` failures set ``human_escalation_required`` and hold."""
    outputs_root, ingest_state_path, scope_registry_path = _scope_fixture(tmp_path)

    def _boom(**_kwargs: Any) -> None:
        raise RuntimeError("compile failed")

    monkeypatch.setattr("akc.runtime.autopilot.safe_recompile_on_drift", _boom)

    rc = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="filesystem",
        env_profile="staging",
        budgets=_budgets(max_consecutive_rollout_failures=3),
        living_check_interval_s=0.0,
        max_iterations=4,
        living_automation_profile=PROFILE_LIVING_LOOP_V1,
    )
    assert rc == 0

    scope_root = outputs_root / "tenant-a" / "repo-a"
    state_path = scope_root / ".akc" / "autopilot" / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    budget = state.get("budget_state")
    assert isinstance(budget, dict)
    assert budget.get("human_escalation_required") is True

    decisions_dir = scope_root / ".akc" / "autopilot" / "decisions"
    decisions = [json.loads(p.read_text(encoding="utf-8")) for p in sorted(decisions_dir.glob("*.decision.json"))]
    kinds = {d.get("decision") for d in decisions}
    assert "compile_failed" in kinds
    assert "escalation_hold" in kinds
