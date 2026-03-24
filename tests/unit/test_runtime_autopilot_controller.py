from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from akc.runtime.autopilot import (
    AutonomyBudgetConfig,
    _acquire_or_renew_lease,
    _load_incremental_runtime_events_for_scope,
    _scope_registry_scopes,
    run_runtime_autopilot,
)
from akc.runtime.living_bridge import DefaultLivingRuntimeBridge


def _write_runtime_run(scope_root: Path, *, run_dir: str, started_at_ms: int, events: list[dict[str, object]]) -> None:
    runtime_dir = scope_root / ".akc" / "runtime" / run_dir
    runtime_dir.mkdir(parents=True, exist_ok=True)
    events_path = runtime_dir / "events.json"
    runtime_run_path = runtime_dir / "runtime_run.json"
    events_path.write_text(json.dumps(events, indent=2) + "\n", encoding="utf-8")
    runtime_run_path.write_text(
        json.dumps(
            {
                "started_at_ms": started_at_ms,
                "events_path": str(events_path),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def test_filesystem_lease_acquire_renew_and_steal(tmp_path: Path) -> None:
    scope_root = tmp_path / "tenant1" / "repo1"
    now_ms = 10_000

    ok_a, lease_a = _acquire_or_renew_lease(
        scope_root=scope_root,
        scope_name="scope-a",
        backend="filesystem",
        controller_id="controller-a",
        now_ms=now_ms,
        lease_ttl_ms=1_000,
        lease_namespace=None,
    )
    assert ok_a is True
    assert lease_a["holder_controller_id"] == "controller-a"

    ok_b, _lease_b = _acquire_or_renew_lease(
        scope_root=scope_root,
        scope_name="scope-a",
        backend="filesystem",
        controller_id="controller-b",
        now_ms=now_ms + 100,
        lease_ttl_ms=1_000,
        lease_namespace=None,
    )
    assert ok_b is False

    ok_c, lease_c = _acquire_or_renew_lease(
        scope_root=scope_root,
        scope_name="scope-a",
        backend="filesystem",
        controller_id="controller-b",
        now_ms=now_ms + 1_500,
        lease_ttl_ms=1_000,
        lease_namespace=None,
    )
    assert ok_c is True
    assert lease_c["holder_controller_id"] == "controller-b"


def test_incremental_runtime_events_respects_automation_disabled(tmp_path: Path) -> None:
    scope_root = tmp_path / "tenant1" / "repo1"
    tenant_id = "tenant1"
    repo_id = "repo1"
    bridge = DefaultLivingRuntimeBridge()
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
    _write_runtime_run(scope_root, run_dir="run1", started_at_ms=101, events=[fail_event])

    initial_cursors = {
        "runtime": {
            "last_runtime_run_started_at_ms": 0,
            "last_event_timestamp": 0,
            "last_event_id": "",
            "seen_event_ids": [],
        },
        "source": {"last_drift_check_at_ms": 0},
    }

    events1, include1, _c1 = _load_incremental_runtime_events_for_scope(
        scope_root=scope_root,
        bridge=bridge,
        cursors=initial_cursors,
        living_automation_enabled=False,
    )
    assert len(events1) == 1
    assert include1 is False


def test_incremental_runtime_events_cursor_and_dedupe(tmp_path: Path) -> None:
    scope_root = tmp_path / "tenant1" / "repo1"
    tenant_id = "tenant1"
    repo_id = "repo1"
    bridge = DefaultLivingRuntimeBridge()
    event1 = {
        "event_id": "evt-1",
        "event_type": "runtime.status",
        "timestamp": 100,
        "context": {
            "tenant_id": tenant_id,
            "repo_id": repo_id,
            "run_id": "run-1",
            "runtime_run_id": "rt-1",
            "policy_mode": "enforce",
            "adapter_id": "native",
        },
        "payload": {"status": "ok"},
    }
    event2 = {
        "event_id": "evt-2",
        "event_type": "runtime.status",
        "timestamp": 200,
        "context": {
            "tenant_id": tenant_id,
            "repo_id": repo_id,
            "run_id": "run-1",
            "runtime_run_id": "rt-1",
            "policy_mode": "enforce",
            "adapter_id": "native",
        },
        "payload": {"status": "warn"},
    }
    _write_runtime_run(scope_root, run_dir="run1", started_at_ms=101, events=[event1, event2])
    _write_runtime_run(scope_root, run_dir="run2", started_at_ms=202, events=[event2])

    initial_cursors = {
        "runtime": {
            "last_runtime_run_started_at_ms": 0,
            "last_event_timestamp": 0,
            "last_event_id": "",
            "seen_event_ids": [],
        },
        "source": {"last_drift_check_at_ms": 0},
    }

    events1, _include1, cursors1 = _load_incremental_runtime_events_for_scope(
        scope_root=scope_root,
        bridge=bridge,
        cursors=initial_cursors,
    )
    assert len(events1) == 2
    assert cursors1["runtime"]["last_event_timestamp"] == 200
    assert "evt-1" in set(cursors1["runtime"]["seen_event_ids"])
    assert "evt-2" in set(cursors1["runtime"]["seen_event_ids"])

    events2, _include2, cursors2 = _load_incremental_runtime_events_for_scope(
        scope_root=scope_root,
        bridge=bridge,
        cursors=cursors1,
    )
    assert events2 == ()
    assert cursors2["runtime"]["last_event_timestamp"] == 200


def test_scope_registry_scopes(tmp_path: Path) -> None:
    registry = tmp_path / "scopes.json"
    registry.write_text(
        json.dumps(
            [
                {"tenant_id": "t2", "repo_id": "r2"},
                {"tenant_id": "t1", "repo_id": "r1"},
                {"tenant_id": "t1", "repo_id": "r1"},
            ]
        ),
        encoding="utf-8",
    )
    assert _scope_registry_scopes(scope_registry_path=registry) == [("t1", "r1"), ("t2", "r2")]


def test_incremental_runtime_events_keeps_recent_seen_event_ids_order(tmp_path: Path) -> None:
    scope_root = tmp_path / "tenant1" / "repo1"
    tenant_id = "tenant1"
    repo_id = "repo1"
    bridge = DefaultLivingRuntimeBridge()

    events: list[dict[str, object]] = []
    for idx in range(1_005):
        events.append(
            {
                "event_id": f"evt-{idx}",
                "event_type": "runtime.status",
                "timestamp": idx,
                "context": {
                    "tenant_id": tenant_id,
                    "repo_id": repo_id,
                    "run_id": "run-1",
                    "runtime_run_id": "rt-1",
                    "policy_mode": "enforce",
                    "adapter_id": "native",
                },
                "payload": {"status": "ok"},
            }
        )
    _write_runtime_run(scope_root, run_dir="run1", started_at_ms=101, events=events)

    initial_cursors = {
        "runtime": {
            "last_runtime_run_started_at_ms": 0,
            "last_event_timestamp": 0,
            "last_event_id": "",
            "seen_event_ids": [],
        },
        "source": {"last_drift_check_at_ms": 0},
    }
    _events, _include, cursors = _load_incremental_runtime_events_for_scope(
        scope_root=scope_root,
        bridge=bridge,
        cursors=initial_cursors,
    )
    seen = cursors["runtime"]["seen_event_ids"]
    assert len(seen) == 1000
    assert seen[0] == "evt-5"
    assert seen[-1] == "evt-1004"


def test_runtime_autopilot_refreshes_scope_registry_each_iteration(tmp_path: Path, monkeypatch: Any) -> None:
    outputs_root = tmp_path / "outputs"
    outputs_root.mkdir(parents=True, exist_ok=True)
    ingest_state = tmp_path / "ingest_state.json"
    ingest_state.write_text("{}\n", encoding="utf-8")
    scope_registry = tmp_path / "scope_registry.json"
    scope_registry.write_text("[]\n", encoding="utf-8")

    calls: list[tuple[str, str]] = []
    iterations = {"n": 0}

    def _fake_scope_registry_scopes(*, scope_registry_path: Path) -> list[tuple[str, str]]:
        _ = scope_registry_path
        iterations["n"] += 1
        if iterations["n"] == 1:
            return []
        return [("tenant-a", "repo-a")]

    def _fake_safe_recompile_on_drift(**kwargs: Any) -> None:
        calls.append((str(kwargs.get("tenant_id", "")), str(kwargs.get("repo_id", ""))))

    monkeypatch.setattr("akc.runtime.autopilot._scope_registry_scopes", _fake_scope_registry_scopes)
    monkeypatch.setattr("akc.runtime.autopilot.safe_recompile_on_drift", _fake_safe_recompile_on_drift)

    budgets = AutonomyBudgetConfig(
        max_mutations_per_day=10,
        max_concurrent_rollouts=1,
        rollback_budget_per_day=10,
        max_consecutive_rollout_failures=3,
        max_rollbacks_per_day_before_escalation=3,
        cooldown_after_failure_ms=1000,
        cooldown_after_policy_deny_ms=1000,
    )

    rc = run_runtime_autopilot(
        outputs_root=outputs_root,
        ingest_state_path=ingest_state,
        scope_registry_path=scope_registry,
        controller_id="controller-a",
        budgets=budgets,
        living_check_interval_s=0.0,
        max_iterations=2,
    )
    assert rc == 0
    assert calls == [("tenant-a", "repo-a")]


def test_k8s_lease_requires_namespace(tmp_path: Path) -> None:
    scope_root = tmp_path / "tenant1" / "repo1"
    try:
        _acquire_or_renew_lease(
            scope_root=scope_root,
            scope_name="scope-a",
            backend="k8s",
            controller_id="controller-a",
            now_ms=10_000,
            lease_ttl_ms=1_000,
            lease_namespace=None,
        )
    except ValueError as exc:
        assert "lease_namespace is required" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing lease namespace")


def test_k8s_lease_not_holder_when_unexpired(tmp_path: Path, monkeypatch: Any) -> None:
    scope_root = tmp_path / "tenant1" / "repo1"

    def _fake_run_kubectl(*, args: list[str], stdin_json_obj: dict[str, Any] | None = None) -> tuple[int, str, str]:
        _ = (args, stdin_json_obj)
        lease = {
            "metadata": {"resourceVersion": "7"},
            "spec": {
                "holderIdentity": "other-controller",
                "renewTime": "2026-01-01T00:00:10Z",
                "leaseDurationSeconds": 30,
            },
        }
        return 0, json.dumps(lease), ""

    monkeypatch.setattr("akc.runtime.autopilot._run_kubectl", _fake_run_kubectl)
    ok, diag = _acquire_or_renew_lease(
        scope_root=scope_root,
        scope_name="scope-a",
        backend="k8s",
        controller_id="controller-a",
        now_ms=20_000,
        lease_ttl_ms=10_000,
        lease_namespace="autopilot",
    )
    assert ok is False
    assert diag.get("holder_controller_id") == "other-controller"
    assert diag.get("backend") == "k8s"


def test_k8s_lease_acquire_and_replace(tmp_path: Path, monkeypatch: Any) -> None:
    scope_root = tmp_path / "tenant1" / "repo1"
    calls: list[tuple[list[str], dict[str, Any] | None]] = []

    def _fake_run_kubectl(*, args: list[str], stdin_json_obj: dict[str, Any] | None = None) -> tuple[int, str, str]:
        calls.append((list(args), stdin_json_obj))
        if args[:3] == ["get", "lease", "scope-a"]:
            lease = {
                "metadata": {"resourceVersion": "11"},
                "spec": {
                    "holderIdentity": "controller-a",
                    "acquireTime": "2026-01-01T00:00:01Z",
                    "renewTime": "2026-01-01T00:00:02Z",
                    "leaseDurationSeconds": 5,
                },
            }
            return 0, json.dumps(lease), ""
        if args[:2] == ["replace", "-f"]:
            return 0, "", ""
        raise AssertionError(f"unexpected kubectl call: {args!r}")

    monkeypatch.setattr("akc.runtime.autopilot._run_kubectl", _fake_run_kubectl)
    ok, diag = _acquire_or_renew_lease(
        scope_root=scope_root,
        scope_name="scope-a",
        backend="k8s",
        controller_id="controller-a",
        now_ms=10_000,
        lease_ttl_ms=5_000,
        lease_namespace="autopilot",
    )
    assert ok is True
    assert diag.get("holder_controller_id") == "controller-a"
    assert any(call[0][:2] == ["replace", "-f"] for call in calls)
