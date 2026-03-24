from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from akc.runtime.adapters import (
    HybridRuntimeAdapter,
    NativeRuntimeAdapter,
    RuntimeAdapterRegistry,
    register_default_runtime_adapters,
)
from akc.runtime.adapters.base import RuntimeAdapterCapabilities
from akc.runtime.adapters.local_depth import LocalDepthRuntimeAdapter
from akc.runtime.init import create_hybrid_runtime
from akc.runtime.manifest_bridge import InMemoryRuntimeEvidenceWriter
from akc.runtime.models import (
    RuntimeAction,
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeContext,
    RuntimeNodeRef,
)


def _context(policy_mode: str = "enforce") -> RuntimeContext:
    return RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode=policy_mode,  # type: ignore[arg-type]
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
    adapter_id: str = "custom"

    def capabilities(self) -> RuntimeAdapterCapabilities:
        return RuntimeAdapterCapabilities()

    def prepare(self, *, context, bundle) -> None:
        _ = (context, bundle)

    def execute_action(self, *, context, action):
        _ = context
        return NativeRuntimeAdapter().execute_action(context=context, action=action)

    def wait_signal(self, *, context, signal_spec):
        _ = (context, signal_spec)
        raise AssertionError("should have fallen back to native")

    def checkpoint(self, *, context):
        _ = context
        raise AssertionError("should have fallen back to native")

    def restore(self, *, context, checkpoint_token):
        _ = (context, checkpoint_token)
        raise AssertionError("should have fallen back to native")

    def cancel(self, *, context, action_id):
        _ = (context, action_id)
        raise AssertionError("should have fallen back to native")


def test_hybrid_adapter_falls_back_and_emits_evidence_event() -> None:
    writer = InMemoryRuntimeEvidenceWriter()
    kernel = create_hybrid_runtime(_bundle(), adapter=LimitedAdapter(), evidence_writer=writer)

    result = kernel.adapter.wait_signal(  # type: ignore[union-attr]
        context=kernel.context,
        signal_spec={"requires_external_signal": True, "signal_id": "sig-1"},
    )

    assert result == {"requires_external_signal": True, "signal_id": "sig-1"}
    assert writer.events
    assert writer.events[-1].event_type == "runtime.adapter.fallback"


def test_registry_enforces_policy_allowlist_and_mode() -> None:
    registry = RuntimeAdapterRegistry()
    registry.register(
        adapter_id="custom",
        factory=LimitedAdapter,
        allowed_policy_modes=("enforce",),
    )

    adapter = registry.create(
        adapter_id="custom",
        context=_context("enforce"),
        policy_allowlist=("custom",),
    )
    assert adapter.adapter_id == "custom"

    with pytest.raises(PermissionError, match="not allowed by policy"):
        registry.create(
            adapter_id="custom",
            context=_context("enforce"),
            policy_allowlist=("native",),
        )

    with pytest.raises(PermissionError, match="policy_mode"):
        registry.create(
            adapter_id="custom",
            context=_context("simulate"),
            policy_allowlist=("custom",),
        )


def test_hybrid_adapter_checkpoint_restore_cancel_fall_back_to_native() -> None:
    hybrid = HybridRuntimeAdapter(primary=LimitedAdapter(), native_fallback=NativeRuntimeAdapter())
    context = _context()

    checkpoint_token = hybrid.checkpoint(context=context)
    hybrid.restore(context=context, checkpoint_token=str(checkpoint_token))
    hybrid.cancel(context=context, action_id="action-1")

    assert checkpoint_token == f"{context.runtime_run_id}:native"


def test_hybrid_adapter_primary_execute_action_is_preserved() -> None:
    hybrid = HybridRuntimeAdapter(primary=LimitedAdapter(), native_fallback=NativeRuntimeAdapter())
    action = RuntimeAction(
        action_id="action-1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1"),
        inputs_fingerprint="fp-1",
        idempotency_key="idem-1",
    )

    result = hybrid.execute_action(context=_context(), action=action)

    assert result.outputs["adapter_id"] == "native"


def test_hybrid_adapter_forwards_execute_action_with_graph_node() -> None:
    base = _bundle()
    tmp_bundle = RuntimeBundle(
        context=base.context,
        ref=base.ref,
        nodes=base.nodes,
        contract_ids=base.contract_ids,
        metadata={
            **dict(base.metadata),
            "runtime_action_routes": {"workflow.execute": "noop"},
        },
    )
    primary = LocalDepthRuntimeAdapter(outputs_root=Path("/tmp"))
    hybrid = HybridRuntimeAdapter(primary=primary, native_fallback=NativeRuntimeAdapter())
    action = RuntimeAction(
        action_id="action-1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1"),
        inputs_fingerprint="fp-1",
        idempotency_key="idem-1",
    )
    ctx = _context()
    primary.prepare(context=ctx, bundle=tmp_bundle)

    result = hybrid.execute_action_with_graph_node(
        context=ctx,
        action=action,
        bundle=tmp_bundle,
        graph_node=None,
    )

    assert result.outputs["adapter_id"] == "local_depth"
    assert result.outputs.get("route") == "noop"


def test_register_default_runtime_adapters_registers_native() -> None:
    registry = RuntimeAdapterRegistry()
    register_default_runtime_adapters(registry)
    assert "native" in registry.list_registered()


def test_native_runtime_adapter_honest_capabilities_default() -> None:
    caps = NativeRuntimeAdapter().capabilities()
    assert not caps.supports_durable_waits
    assert not caps.supports_external_signals
    assert not caps.supports_compensation_hooks
    assert not caps.supports_external_checkpointing


def test_native_runtime_adapter_legacy_stub_capabilities() -> None:
    caps = NativeRuntimeAdapter(honest_capabilities=False).capabilities()
    assert caps.supports_durable_waits
    assert caps.supports_external_signals
    assert caps.supports_compensation_hooks
    assert caps.supports_external_checkpointing


def test_hybrid_capabilities_match_primary_only() -> None:
    hybrid = HybridRuntimeAdapter(primary=LimitedAdapter(), native_fallback=NativeRuntimeAdapter())
    assert hybrid.capabilities() == LimitedAdapter().capabilities()
    primary_depth = LocalDepthRuntimeAdapter(outputs_root=Path("/tmp"))
    hybrid_depth = HybridRuntimeAdapter(primary=primary_depth, native_fallback=NativeRuntimeAdapter())
    assert hybrid_depth.capabilities() == primary_depth.capabilities()
