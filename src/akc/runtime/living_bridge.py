from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal, Protocol

from akc.memory.models import JSONValue
from akc.runtime.models import RuntimeContext, RuntimeEvent

RuntimeImpactClass = Literal["service", "agent", "workflow"]


@dataclass(frozen=True, slots=True)
class RuntimeHealthSignal:
    context: RuntimeContext
    impact_class: RuntimeImpactClass
    reason: str
    source_event_id: str


class LivingRuntimeBridge(Protocol):
    def derive_signal(self, *, event: RuntimeEvent) -> RuntimeHealthSignal | None: ...


def _payload_result_health_degraded(payload: Mapping[str, JSONValue]) -> str | None:
    result = payload.get("result")
    if not isinstance(result, Mapping):
        return None
    outputs = result.get("outputs")
    if not isinstance(outputs, Mapping):
        return None
    hs = str(outputs.get("health_status", "")).strip().lower()
    if hs in {"degraded", "failed"}:
        return hs
    return None


@dataclass(slots=True)
class DefaultLivingRuntimeBridge(LivingRuntimeBridge):
    """Map runtime transcript events to living-drift / safe-recompile health signals.

    Used by :func:`akc.living.runtime_bridge.default_living_runtime_bridge` when the
    ``living_loop_v1`` automation profile is enabled (see ``docs/runtime-execution.md``).

    Covers reconcile failures, **kernel loop terminal outcomes** (via
    ``runtime.kernel.loop_finished``), **action failures** (including completed actions whose
    ``result.status`` is not ``succeeded``), **dead letters**, **adapter fallback**, and
    adapter-reported ``outputs.health_status`` degradation.

    **Operational validity:** ``runtime.operational_validity.attested`` with ``passed: false`` maps to a
    workflow health signal (post-runtime attestation did not satisfy intent operational criteria).
    """

    def derive_signal(self, *, event: RuntimeEvent) -> RuntimeHealthSignal | None:
        et = event.event_type.strip()
        if et == "runtime.operational_validity.attested":
            if event.payload.get("passed") is False:
                return RuntimeHealthSignal(
                    context=event.context,
                    impact_class="workflow",
                    reason="runtime.operational_validity.attested:passed=false",
                    source_event_id=event.event_id,
                )
            return None
        if et == "runtime.kernel.loop_finished":
            ts = str(event.payload.get("terminal_status", "")).strip()
            if ts == "max_iterations_exceeded":
                return RuntimeHealthSignal(
                    context=event.context,
                    impact_class="workflow",
                    reason="runtime.kernel.loop_finished:max_iterations_exceeded",
                    source_event_id=event.event_id,
                )
            return None
        if et == "runtime.action.completed":
            payload = event.payload
            result = payload.get("result")
            if isinstance(result, dict):
                st = str(result.get("status", "")).strip()
                if st in {"failed", "cancelled"}:
                    return RuntimeHealthSignal(
                        context=event.context,
                        impact_class="workflow",
                        reason=f"runtime.action.completed:{st}",
                        source_event_id=event.event_id,
                    )
            adapter_hs = _payload_result_health_degraded(payload)
            if adapter_hs is not None:
                return RuntimeHealthSignal(
                    context=event.context,
                    impact_class="service",
                    reason=f"adapter_health_status:{adapter_hs}",
                    source_event_id=event.event_id,
                )
            return None
        if et in {"runtime.action.dead_lettered", "runtime.adapter.fallback"}:
            return RuntimeHealthSignal(
                context=event.context,
                impact_class="workflow",
                reason=et,
                source_event_id=event.event_id,
            )
        if et in {"runtime.action.failed", "runtime.reconcile.failed"}:
            return RuntimeHealthSignal(
                context=event.context,
                impact_class="workflow",
                reason=et,
                source_event_id=event.event_id,
            )
        if et == "runtime.reconcile.resource_status":
            payload = event.payload
            if payload.get("converged") is False and payload.get("hash_matched") is True:
                return RuntimeHealthSignal(
                    context=event.context,
                    impact_class="service",
                    reason="runtime.reconcile.resource_status:hash_matched_not_converged",
                    source_event_id=event.event_id,
                )
            return None
        return None
