from __future__ import annotations

from akc.runtime.living_bridge import DefaultLivingRuntimeBridge
from akc.runtime.models import RuntimeContext, RuntimeEvent


def _ctx() -> RuntimeContext:
    return RuntimeContext(
        tenant_id="t1",
        repo_id="r1",
        run_id="run-1",
        runtime_run_id="rt-1",
        policy_mode="enforce",
        adapter_id="native",
    )


def test_living_bridge_kernel_max_iterations_emits_signal() -> None:
    bridge = DefaultLivingRuntimeBridge()
    ev = RuntimeEvent(
        event_id="e1",
        event_type="runtime.kernel.loop_finished",
        timestamp=1,
        context=_ctx(),
        payload={"terminal_status": "max_iterations_exceeded", "iterations": 100},
    )
    sig = bridge.derive_signal(event=ev)
    assert sig is not None
    assert "max_iterations" in sig.reason


def test_living_bridge_completed_failed_action_emits_signal() -> None:
    bridge = DefaultLivingRuntimeBridge()
    ev = RuntimeEvent(
        event_id="e2",
        event_type="runtime.action.completed",
        timestamp=2,
        context=_ctx(),
        payload={"result": {"status": "failed", "outputs": {}}},
    )
    sig = bridge.derive_signal(event=ev)
    assert sig is not None
    assert "failed" in sig.reason


def test_living_bridge_adapter_outputs_health_degraded_emits_signal() -> None:
    bridge = DefaultLivingRuntimeBridge()
    ev = RuntimeEvent(
        event_id="e3",
        event_type="runtime.action.completed",
        timestamp=3,
        context=_ctx(),
        payload={"result": {"status": "succeeded", "outputs": {"health_status": "degraded"}}},
    )
    sig = bridge.derive_signal(event=ev)
    assert sig is not None
    assert "adapter_health" in sig.reason


def test_living_bridge_dead_letter_emits_signal() -> None:
    bridge = DefaultLivingRuntimeBridge()
    ev = RuntimeEvent(
        event_id="e4",
        event_type="runtime.action.dead_lettered",
        timestamp=4,
        context=_ctx(),
        payload={},
    )
    sig = bridge.derive_signal(event=ev)
    assert sig is not None
    assert sig.reason == "runtime.action.dead_lettered"


def test_living_bridge_reconcile_resource_status_hash_matched_not_converged() -> None:
    bridge = DefaultLivingRuntimeBridge()
    ev = RuntimeEvent(
        event_id="e-rs",
        event_type="runtime.reconcile.resource_status",
        timestamp=7,
        context=_ctx(),
        payload={"converged": False, "hash_matched": True, "resource_id": "svc-a"},
    )
    sig = bridge.derive_signal(event=ev)
    assert sig is not None
    assert "hash_matched_not_converged" in sig.reason


def test_living_bridge_operational_validity_attested_failed_emits_signal() -> None:
    bridge = DefaultLivingRuntimeBridge()
    ev_pass = RuntimeEvent(
        event_id="e-ov-pass",
        event_type="runtime.operational_validity.attested",
        timestamp=5,
        context=_ctx(),
        payload={"passed": True},
    )
    assert bridge.derive_signal(event=ev_pass) is None
    ev_fail = RuntimeEvent(
        event_id="e-ov-fail",
        event_type="runtime.operational_validity.attested",
        timestamp=6,
        context=_ctx(),
        payload={"passed": False, "attestation_fingerprint_sha256": "a" * 64},
    )
    sig = bridge.derive_signal(event=ev_fail)
    assert sig is not None
    assert "operational_validity" in sig.reason
    assert "passed=false" in sig.reason
