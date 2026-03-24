from __future__ import annotations

from dataclasses import dataclass

from akc.runtime.adapters.base import RuntimeAdapterCapabilities
from akc.runtime.init import create_hybrid_runtime
from akc.runtime.manifest_bridge import InMemoryRuntimeEvidenceWriter
from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef


def _bundle() -> RuntimeBundle:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="limited",
    )
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
        _ = (context, action)
        raise AssertionError("this integration only exercises fallback-only capabilities")

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


def test_runtime_e2e_adapter_fallback_records_runtime_event() -> None:
    writer = InMemoryRuntimeEvidenceWriter()
    kernel = create_hybrid_runtime(_bundle(), adapter=LimitedAdapter(), evidence_writer=writer)

    checkpoint_token = kernel.adapter.checkpoint(context=kernel.context)  # type: ignore[union-attr]

    assert checkpoint_token == "runtime-1:native"
    assert writer.events[-1].event_type == "runtime.adapter.fallback"
    assert writer.events[-1].payload["fallback_adapter_id"] == "native"
