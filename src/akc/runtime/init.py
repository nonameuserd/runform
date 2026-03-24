from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from akc.runtime.adapters.base import HybridRuntimeAdapter, RuntimeAdapter
from akc.runtime.adapters.local_depth import LocalDepthRuntimeAdapter
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.events import RuntimeEventBus
from akc.runtime.kernel import RuntimeKernel
from akc.runtime.manifest_bridge import InMemoryRuntimeEvidenceWriter, RuntimeEvidenceWriter
from akc.runtime.models import RuntimeBundle
from akc.runtime.policy import RuntimePolicyRuntime
from akc.runtime.scheduler import InMemoryRuntimeScheduler
from akc.runtime.state_store import InMemoryRuntimeStateStore


def _intent_projection_map(metadata: Mapping[str, Any]) -> Mapping[str, Any]:
    raw = metadata.get("intent_policy_projection", {})
    return dict(raw) if isinstance(raw, Mapping) else {}


def create_local_depth_runtime(
    bundle: RuntimeBundle,
    *,
    outputs_root: str | Path,
    delegate: RuntimeAdapter | None = None,
    evidence_writer: RuntimeEvidenceWriter | None = None,
) -> RuntimeKernel:
    """Kernel with :class:`~akc.runtime.adapters.local_depth.LocalDepthRuntimeAdapter` (routing + opt-in subprocess).

    ``outputs_root`` must match the :class:`~akc.runtime.state_store.FileSystemRuntimeStateStore` root when
    using on-disk state so subprocess ``cwd`` aligns with persisted runtime artifacts.
    """
    writer = evidence_writer or InMemoryRuntimeEvidenceWriter()
    adapter = LocalDepthRuntimeAdapter(
        outputs_root=Path(outputs_root).expanduser().resolve(),
        delegate=delegate or NativeRuntimeAdapter(),
    )
    return RuntimeKernel(
        context=bundle.context,
        bundle=bundle,
        adapter=adapter,
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
        evidence_writer=writer,
        policy_runtime=RuntimePolicyRuntime.from_bundle(
            context=bundle.context,
            policy_envelope=bundle.policy_envelope,
            intent_projection=_intent_projection_map(bundle.metadata),
            ir_document=bundle.ir_document,
        ),
    )


def create_native_runtime(bundle: RuntimeBundle) -> RuntimeKernel:
    evidence_writer = InMemoryRuntimeEvidenceWriter()
    return RuntimeKernel(
        context=bundle.context,
        bundle=bundle,
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
        evidence_writer=evidence_writer,
        policy_runtime=RuntimePolicyRuntime.from_bundle(
            context=bundle.context,
            policy_envelope=bundle.policy_envelope,
            intent_projection=_intent_projection_map(bundle.metadata),
            ir_document=bundle.ir_document,
        ),
    )


def create_hybrid_runtime(
    bundle: RuntimeBundle,
    *,
    adapter: RuntimeAdapter,
    evidence_writer: RuntimeEvidenceWriter | None = None,
) -> RuntimeKernel:
    """Supported extension point for custom adapters (see ``docs/runtime-execution.md``).

    The kernel's ``adapter`` slot is replaced with :class:`~akc.runtime.adapters.base.HybridRuntimeAdapter`,
    which forwards ``execute_action`` to ``adapter`` and falls back to :class:`NativeRuntimeAdapter` for
    call paths the primary adapter does not implement. Advertised capabilities come from the **primary**
    only so telemetry and policy do not treat native stub fallbacks as production durability.
    """
    writer = evidence_writer or InMemoryRuntimeEvidenceWriter()
    kernel = RuntimeKernel(
        context=bundle.context,
        bundle=bundle,
        adapter=NativeRuntimeAdapter(),
        scheduler=InMemoryRuntimeScheduler(),
        state_store=InMemoryRuntimeStateStore(),
        event_bus=RuntimeEventBus(),
        evidence_writer=writer,
        policy_runtime=RuntimePolicyRuntime.from_bundle(
            context=bundle.context,
            policy_envelope=bundle.policy_envelope,
            intent_projection=_intent_projection_map(bundle.metadata),
            ir_document=bundle.ir_document,
        ),
    )
    kernel.adapter = HybridRuntimeAdapter(
        primary=adapter,
        native_fallback=NativeRuntimeAdapter(),
        fallback_observer=kernel.observe_adapter_fallback,
    )
    return kernel
