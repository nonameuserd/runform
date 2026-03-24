from __future__ import annotations

from akc.runtime.living_bridge import DefaultLivingRuntimeBridge
from akc.runtime.models import RuntimeContext, RuntimeEvent


def _context() -> RuntimeContext:
    return RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )


def test_runtime_living_recompile_bridge_derives_signal_for_failure() -> None:
    bridge = DefaultLivingRuntimeBridge()
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="runtime.action.failed",
        timestamp=1,
        context=_context(),
        payload={"reason": "boom"},
    )

    signal = bridge.derive_signal(event=event)

    assert signal is not None
    assert signal.impact_class == "workflow"
    assert signal.reason == "runtime.action.failed"
    assert signal.source_event_id == "evt-1"


def test_runtime_living_recompile_bridge_ignores_non_failure_event() -> None:
    bridge = DefaultLivingRuntimeBridge()
    event = RuntimeEvent(
        event_id="evt-2",
        event_type="runtime.action.completed",
        timestamp=1,
        context=_context(),
        payload={},
    )

    assert bridge.derive_signal(event=event) is None
