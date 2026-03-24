from __future__ import annotations

from dataclasses import dataclass

from akc.runtime.adapters import HybridRuntimeAdapter, NativeRuntimeAdapter
from akc.runtime.adapters.base import RuntimeAdapterCapabilities
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef


def _context() -> RuntimeContext:
    return RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="custom",
    )


def _bundle() -> RuntimeBundle:
    context = _context()
    return RuntimeBundle(
        context=context,
        ref=RuntimeBundleRef(
            bundle_path=".akc/runtime/runtime_bundle.json",
            manifest_hash="a" * 64,
            created_at=1,
            source_compile_run_id="compile-1",
        ),
        nodes=(RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1"),),
        contract_ids=("contract-1",),
    )


@dataclass
class LimitedAdapter:
    adapter_id: str = "limited"

    def capabilities(self) -> RuntimeAdapterCapabilities:
        return RuntimeAdapterCapabilities()

    def prepare(self, *, context, bundle) -> None:
        _ = (context, bundle)

    def execute_action(self, *, context, action):
        return NativeRuntimeAdapter().execute_action(context=context, action=action)

    def wait_signal(self, *, context, signal_spec):
        _ = (context, signal_spec)
        raise AssertionError("expected native fallback")

    def checkpoint(self, *, context):
        _ = context
        raise AssertionError("expected native fallback")

    def restore(self, *, context, checkpoint_token):
        _ = (context, checkpoint_token)
        raise AssertionError("expected native fallback")

    def cancel(self, *, context, action_id):
        _ = (context, action_id)
        raise AssertionError("expected native fallback")


@dataclass
class DurableAdapter(LimitedAdapter):
    adapter_id: str = "durable"

    def capabilities(self) -> RuntimeAdapterCapabilities:
        return RuntimeAdapterCapabilities(supports_durable_waits=True)

    def wait_signal(self, *, context, signal_spec):
        _ = context
        return {"adapter": self.adapter_id, **dict(signal_spec)}


def test_hybrid_adapter_reports_fallback_details() -> None:
    observed: list[tuple[str, str, str, str, str]] = []
    hybrid = HybridRuntimeAdapter(
        primary=LimitedAdapter(),
        native_fallback=NativeRuntimeAdapter(),
        fallback_observer=lambda context, primary, fallback, capability, reason: observed.append(
            (context.runtime_run_id, primary, fallback, capability, reason)
        ),
    )

    result = hybrid.wait_signal(
        context=_context(),
        signal_spec={"requires_external_signal": True, "signal_id": "sig-1"},
    )

    assert result == {"requires_external_signal": True, "signal_id": "sig-1"}
    assert observed == [
        (
            "runtime-1",
            "limited",
            "native",
            "external_signals",
            "adapter lacks external_signals",
        )
    ]


def test_hybrid_adapter_uses_primary_when_capability_is_supported() -> None:
    observed: list[object] = []
    hybrid = HybridRuntimeAdapter(
        primary=DurableAdapter(),
        native_fallback=NativeRuntimeAdapter(),
        fallback_observer=lambda *_args: observed.append("fallback"),
    )

    result = hybrid.wait_signal(
        context=_context(),
        signal_spec={"requires_external_signal": False, "signal_id": "sig-2"},
    )

    assert result == {"adapter": "durable", "requires_external_signal": False, "signal_id": "sig-2"}
    assert observed == []
