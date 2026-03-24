from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from akc.runtime.autopilot import AutonomyBudgetConfig, run_runtime_autopilot


def test_runtime_autopilot_single_writer_lease_holder_only(tmp_path: Path, monkeypatch: Any) -> None:
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    ingest_state_path = tmp_path / "ingest_state.json"
    ingest_state_path.write_text("{}\n", encoding="utf-8")

    scope_registry_path = tmp_path / "scope_registry.json"
    scope_registry_path.write_text(
        json.dumps([{"tenant_id": "tenant-a", "repo_id": "repo-a"}], indent=2) + "\n",
        encoding="utf-8",
    )

    compile_calls: list[tuple[str, str]] = []

    def _fake_safe_recompile_on_drift(**kwargs: Any) -> None:
        compile_calls.append((str(kwargs.get("tenant_id", "")), str(kwargs.get("repo_id", ""))))

    monkeypatch.setattr(
        "akc.runtime.autopilot.safe_recompile_on_drift",
        _fake_safe_recompile_on_drift,
    )

    budgets = AutonomyBudgetConfig(
        max_mutations_per_day=10,
        max_concurrent_rollouts=1,
        rollback_budget_per_day=10,
        max_consecutive_rollout_failures=3,
        max_rollbacks_per_day_before_escalation=3,
        cooldown_after_failure_ms=1000,
        cooldown_after_policy_deny_ms=1000,
    )

    # First controller acquires lease and executes one control iteration.
    rc1 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="filesystem",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc1 == 0

    # Second controller should be blocked by active lease and not execute compile.
    rc2 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-b",
        lease_backend="filesystem",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc2 == 0
    assert len(compile_calls) == 1

    scope_root = outputs_root / "tenant-a" / "repo-a"
    decisions_dir = scope_root / ".akc" / "autopilot" / "decisions"
    decision_files = sorted(decisions_dir.glob("*.decision.json"))
    assert decision_files, "expected decision artifacts"
    decisions = [json.loads(path.read_text(encoding="utf-8")) for path in decision_files]

    assert any(item.get("decision") == "no_new_manifest" for item in decisions)
    assert any(item.get("decision") == "skip_not_lease_holder" for item in decisions)


def test_runtime_autopilot_single_writer_k8s_lease_holder_only(tmp_path: Path, monkeypatch: Any) -> None:
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    ingest_state_path = tmp_path / "ingest_state.json"
    ingest_state_path.write_text("{}\n", encoding="utf-8")

    scope_registry_path = tmp_path / "scope_registry.json"
    scope_registry_path.write_text(
        json.dumps([{"tenant_id": "tenant-a", "repo_id": "repo-a"}], indent=2) + "\n",
        encoding="utf-8",
    )

    compile_calls: list[tuple[str, str]] = []
    lease_state: dict[str, Any] = {}

    def _fake_safe_recompile_on_drift(**kwargs: Any) -> None:
        compile_calls.append((str(kwargs.get("tenant_id", "")), str(kwargs.get("repo_id", ""))))

    def _fake_run_kubectl(*, args: list[str], stdin_json_obj: dict[str, Any] | None = None) -> tuple[int, str, str]:
        _ = stdin_json_obj
        if args[:3] == ["get", "lease", "scope-a"]:
            if lease_state:
                return 0, json.dumps(lease_state), ""
            return 1, "", 'Error from server (NotFound): leases.coordination.k8s.io "scope-a" not found'
        if args[:2] == ["create", "-f"]:
            assert isinstance(stdin_json_obj, dict)
            lease_state.clear()
            lease_state.update(stdin_json_obj)
            lease_state.setdefault("metadata", {})
            lease_state["metadata"]["resourceVersion"] = "1"
            return 0, "", ""
        if args[:2] == ["replace", "-f"]:
            assert isinstance(stdin_json_obj, dict)
            lease_state.clear()
            lease_state.update(stdin_json_obj)
            lease_state.setdefault("metadata", {})
            lease_state["metadata"]["resourceVersion"] = str(
                int(str(lease_state["metadata"].get("resourceVersion", "1"))) + 1
            )
            return 0, "", ""
        raise AssertionError(f"unexpected kubectl call: {args!r}")

    monkeypatch.setattr(
        "akc.runtime.autopilot.safe_recompile_on_drift",
        _fake_safe_recompile_on_drift,
    )
    monkeypatch.setattr("akc.runtime.autopilot._run_kubectl", _fake_run_kubectl)

    budgets = AutonomyBudgetConfig(
        max_mutations_per_day=10,
        max_concurrent_rollouts=1,
        rollback_budget_per_day=10,
        max_consecutive_rollout_failures=3,
        max_rollbacks_per_day_before_escalation=3,
        cooldown_after_failure_ms=1000,
        cooldown_after_policy_deny_ms=1000,
    )

    # First controller acquires k8s lease and executes one control iteration.
    rc1 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="k8s",
        lease_name="scope-a",
        lease_namespace="autopilot-system",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc1 == 0

    # Second controller should be blocked by active k8s lease and not execute compile.
    rc2 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-b",
        lease_backend="k8s",
        lease_name="scope-a",
        lease_namespace="autopilot-system",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc2 == 0
    assert len(compile_calls) == 1

    scope_root = outputs_root / "tenant-a" / "repo-a"
    decisions_dir = scope_root / ".akc" / "autopilot" / "decisions"
    decision_files = sorted(decisions_dir.glob("*.decision.json"))
    assert decision_files, "expected decision artifacts"
    decisions = [json.loads(path.read_text(encoding="utf-8")) for path in decision_files]

    assert any(item.get("decision") == "no_new_manifest" for item in decisions)
    assert any(item.get("decision") == "skip_not_lease_holder" for item in decisions)


def test_runtime_autopilot_k8s_lease_renews_for_same_controller(tmp_path: Path, monkeypatch: Any) -> None:
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    ingest_state_path = tmp_path / "ingest_state.json"
    ingest_state_path.write_text("{}\n", encoding="utf-8")
    scope_registry_path = tmp_path / "scope_registry.json"
    scope_registry_path.write_text(
        json.dumps([{"tenant_id": "tenant-a", "repo_id": "repo-a"}], indent=2) + "\n",
        encoding="utf-8",
    )

    compile_calls: list[tuple[str, str]] = []
    lease_state: dict[str, Any] = {}
    kubectl_calls: list[list[str]] = []

    def _fake_safe_recompile_on_drift(**kwargs: Any) -> None:
        compile_calls.append((str(kwargs.get("tenant_id", "")), str(kwargs.get("repo_id", ""))))

    def _fake_run_kubectl(*, args: list[str], stdin_json_obj: dict[str, Any] | None = None) -> tuple[int, str, str]:
        kubectl_calls.append(list(args))
        if args[:3] == ["get", "lease", "scope-a"]:
            if lease_state:
                return 0, json.dumps(lease_state), ""
            return 1, "", 'Error from server (NotFound): leases.coordination.k8s.io "scope-a" not found'
        if args[:2] == ["create", "-f"]:
            assert isinstance(stdin_json_obj, dict)
            lease_state.clear()
            lease_state.update(stdin_json_obj)
            lease_state.setdefault("metadata", {})
            lease_state["metadata"]["resourceVersion"] = "1"
            return 0, "", ""
        if args[:2] == ["replace", "-f"]:
            assert isinstance(stdin_json_obj, dict)
            lease_state.clear()
            lease_state.update(stdin_json_obj)
            lease_state.setdefault("metadata", {})
            prev_rv = int(str(lease_state["metadata"].get("resourceVersion", "1")))
            lease_state["metadata"]["resourceVersion"] = str(prev_rv + 1)
            return 0, "", ""
        raise AssertionError(f"unexpected kubectl call: {args!r}")

    monkeypatch.setattr("akc.runtime.autopilot.safe_recompile_on_drift", _fake_safe_recompile_on_drift)
    monkeypatch.setattr("akc.runtime.autopilot._run_kubectl", _fake_run_kubectl)

    budgets = AutonomyBudgetConfig(
        max_mutations_per_day=10,
        max_concurrent_rollouts=1,
        rollback_budget_per_day=10,
        max_consecutive_rollout_failures=3,
        max_rollbacks_per_day_before_escalation=3,
        cooldown_after_failure_ms=1000,
        cooldown_after_policy_deny_ms=1000,
    )

    rc1 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="k8s",
        lease_name="scope-a",
        lease_namespace="autopilot-system",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc1 == 0

    rc2 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="k8s",
        lease_name="scope-a",
        lease_namespace="autopilot-system",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc2 == 0
    assert compile_calls == [("tenant-a", "repo-a"), ("tenant-a", "repo-a")]
    assert any(call[:2] == ["create", "-f"] for call in kubectl_calls)
    assert any(call[:2] == ["replace", "-f"] for call in kubectl_calls)
    assert lease_state.get("spec", {}).get("holderIdentity") == "controller-a"


def test_runtime_autopilot_k8s_lease_expiry_handoff_between_controllers(tmp_path: Path, monkeypatch: Any) -> None:
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    ingest_state_path = tmp_path / "ingest_state.json"
    ingest_state_path.write_text("{}\n", encoding="utf-8")
    scope_registry_path = tmp_path / "scope_registry.json"
    scope_registry_path.write_text(
        json.dumps([{"tenant_id": "tenant-a", "repo_id": "repo-a"}], indent=2) + "\n",
        encoding="utf-8",
    )

    compile_calls: list[tuple[str, str]] = []
    lease_state: dict[str, Any] = {}

    def _fake_safe_recompile_on_drift(**kwargs: Any) -> None:
        compile_calls.append((str(kwargs.get("tenant_id", "")), str(kwargs.get("repo_id", ""))))

    def _fake_run_kubectl(*, args: list[str], stdin_json_obj: dict[str, Any] | None = None) -> tuple[int, str, str]:
        if args[:3] == ["get", "lease", "scope-a"]:
            if lease_state:
                return 0, json.dumps(lease_state), ""
            return 1, "", 'Error from server (NotFound): leases.coordination.k8s.io "scope-a" not found'
        if args[:2] == ["create", "-f"]:
            assert isinstance(stdin_json_obj, dict)
            lease_state.clear()
            lease_state.update(stdin_json_obj)
            lease_state.setdefault("metadata", {})
            lease_state["metadata"]["resourceVersion"] = "1"
            return 0, "", ""
        if args[:2] == ["replace", "-f"]:
            assert isinstance(stdin_json_obj, dict)
            lease_state.clear()
            lease_state.update(stdin_json_obj)
            lease_state.setdefault("metadata", {})
            prev_rv = int(str(lease_state["metadata"].get("resourceVersion", "1")))
            lease_state["metadata"]["resourceVersion"] = str(prev_rv + 1)
            return 0, "", ""
        raise AssertionError(f"unexpected kubectl call: {args!r}")

    monkeypatch.setattr("akc.runtime.autopilot.safe_recompile_on_drift", _fake_safe_recompile_on_drift)
    monkeypatch.setattr("akc.runtime.autopilot._run_kubectl", _fake_run_kubectl)

    budgets = AutonomyBudgetConfig(
        max_mutations_per_day=10,
        max_concurrent_rollouts=1,
        rollback_budget_per_day=10,
        max_consecutive_rollout_failures=3,
        max_rollbacks_per_day_before_escalation=3,
        cooldown_after_failure_ms=1000,
        cooldown_after_policy_deny_ms=1000,
    )

    rc1 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-a",
        lease_backend="k8s",
        lease_name="scope-a",
        lease_namespace="autopilot-system",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc1 == 0
    assert lease_state.get("spec", {}).get("holderIdentity") == "controller-a"

    # Force the lease to look expired so controller-b can take over.
    lease_state.setdefault("spec", {})
    lease_state["spec"]["renewTime"] = "1970-01-01T00:00:00Z"
    lease_state["spec"]["leaseDurationSeconds"] = 1

    rc2 = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        scope_registry_path=scope_registry_path,
        controller_id="controller-b",
        lease_backend="k8s",
        lease_name="scope-a",
        lease_namespace="autopilot-system",
        env_profile="staging",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=1,
    )
    assert rc2 == 0
    assert compile_calls == [("tenant-a", "repo-a"), ("tenant-a", "repo-a")]
    assert lease_state.get("spec", {}).get("holderIdentity") == "controller-b"
