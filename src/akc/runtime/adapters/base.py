from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

from akc.runtime.models import RuntimeAction, RuntimeActionResult, RuntimeBundle, RuntimeContext

AdapterCapability = Literal[
    "durable_waits",
    "external_signals",
    "compensation_hooks",
    "external_checkpointing",
]
FallbackObserver = Callable[[RuntimeContext, str, str, AdapterCapability, str], None]


@dataclass(frozen=True, slots=True)
class RuntimeAdapterCapabilities:
    supports_durable_waits: bool = False
    supports_external_signals: bool = False
    supports_compensation_hooks: bool = False
    supports_external_checkpointing: bool = False

    def supports(self, capability: AdapterCapability) -> bool:
        return {
            "durable_waits": self.supports_durable_waits,
            "external_signals": self.supports_external_signals,
            "compensation_hooks": self.supports_compensation_hooks,
            "external_checkpointing": self.supports_external_checkpointing,
        }[capability]


class RuntimeAdapter(Protocol):
    @property
    def adapter_id(self) -> str: ...

    def capabilities(self) -> RuntimeAdapterCapabilities: ...

    def prepare(self, *, context: RuntimeContext, bundle: RuntimeBundle) -> None: ...

    def execute_action(self, *, context: RuntimeContext, action: RuntimeAction) -> RuntimeActionResult: ...

    def wait_signal(self, *, context: RuntimeContext, signal_spec: Mapping[str, object]) -> object: ...

    def checkpoint(self, *, context: RuntimeContext) -> str | None: ...

    def restore(self, *, context: RuntimeContext, checkpoint_token: str) -> None: ...

    def cancel(self, *, context: RuntimeContext, action_id: str) -> None: ...


@dataclass(slots=True)
class HybridRuntimeAdapter:
    primary: RuntimeAdapter
    native_fallback: RuntimeAdapter
    fallback_observer: FallbackObserver | None = None

    @property
    def adapter_id(self) -> str:
        return self.primary.adapter_id

    def capabilities(self) -> RuntimeAdapterCapabilities:
        """Return the **primary** adapter's capabilities only.

        The native fallback may still execute ``wait_signal`` / ``checkpoint`` / etc.
        when the primary lacks a capability, but those paths are not durable workload
        semantics unless the **primary** advertises them. OR-merging with the
        fallback previously caused hybrids to overclaim durability (for example
        ``LocalDepthRuntimeAdapter`` + ``NativeRuntimeAdapter``).
        """

        return self.primary.capabilities()

    def prepare(self, *, context: RuntimeContext, bundle: RuntimeBundle) -> None:
        self.primary.prepare(context=context, bundle=bundle)
        self.native_fallback.prepare(context=context, bundle=bundle)

    def execute_action(self, *, context: RuntimeContext, action: RuntimeAction) -> RuntimeActionResult:
        return self.primary.execute_action(context=context, action=action)

    def execute_action_with_graph_node(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        bundle: RuntimeBundle,
        graph_node: object | None,
    ) -> RuntimeActionResult:
        primary_fn = getattr(self.primary, "execute_action_with_graph_node", None)
        if callable(primary_fn):
            return primary_fn(  # type: ignore[no-any-return]
                context=context,
                action=action,
                bundle=bundle,
                graph_node=graph_node,
            )
        return self.primary.execute_action(context=context, action=action)

    @property
    def respects_runtime_action_routing(self) -> bool:
        return bool(getattr(self.primary, "respects_runtime_action_routing", False))

    def wait_signal(self, *, context: RuntimeContext, signal_spec: Mapping[str, object]) -> object:
        capability: AdapterCapability = (
            "external_signals" if bool(signal_spec.get("requires_external_signal", False)) else "durable_waits"
        )
        return self._call_with_fallback(
            context=context,
            capability=capability,
            reason=f"adapter lacks {capability}",
            primary_call=lambda: self.primary.wait_signal(context=context, signal_spec=signal_spec),
            fallback_call=lambda: self.native_fallback.wait_signal(context=context, signal_spec=signal_spec),
        )

    def checkpoint(self, *, context: RuntimeContext) -> str | None:
        return self._call_with_fallback(  # type: ignore[return-value]
            context=context,
            capability="external_checkpointing",
            reason="adapter lacks external checkpointing",
            primary_call=lambda: self.primary.checkpoint(context=context),
            fallback_call=lambda: self.native_fallback.checkpoint(context=context),
        )

    def restore(self, *, context: RuntimeContext, checkpoint_token: str) -> None:
        self._call_with_fallback(
            context=context,
            capability="external_checkpointing",
            reason="adapter lacks external checkpointing",
            primary_call=lambda: self.primary.restore(context=context, checkpoint_token=checkpoint_token),
            fallback_call=lambda: self.native_fallback.restore(context=context, checkpoint_token=checkpoint_token),
        )

    def cancel(self, *, context: RuntimeContext, action_id: str) -> None:
        self._call_with_fallback(
            context=context,
            capability="compensation_hooks",
            reason="adapter lacks compensation hooks",
            primary_call=lambda: self.primary.cancel(context=context, action_id=action_id),
            fallback_call=lambda: self.native_fallback.cancel(context=context, action_id=action_id),
        )

    def _call_with_fallback(
        self,
        *,
        context: RuntimeContext,
        capability: AdapterCapability,
        reason: str,
        primary_call: Callable[[], object],
        fallback_call: Callable[[], object],
    ) -> object:
        if self.primary.capabilities().supports(capability):
            return primary_call()
        self._observe_fallback(context=context, capability=capability, reason=reason)
        return fallback_call()

    def _observe_fallback(self, *, context: RuntimeContext, capability: AdapterCapability, reason: str) -> None:
        if self.fallback_observer is None:
            return
        self.fallback_observer(
            context,
            self.primary.adapter_id,
            self.native_fallback.adapter_id,
            capability,
            reason,
        )
