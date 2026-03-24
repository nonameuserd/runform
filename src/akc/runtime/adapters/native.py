from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from akc.runtime.adapters.base import RuntimeAdapter, RuntimeAdapterCapabilities
from akc.runtime.coordination.worker import (
    AgentWorkerAdapter,
    TimeoutEnforcingAgentWorker,
    agent_worker_from_env,
    build_role_worker_context,
    coordination_step_runtime_result,
)
from akc.runtime.models import RuntimeAction, RuntimeActionResult, RuntimeBundle, RuntimeContext


@dataclass(slots=True)
class NativeRuntimeAdapter(RuntimeAdapter):
    """In-process stub adapter used for coordination steps and hybrid fallbacks.

    With ``honest_capabilities=True`` (default), durability-related flags are **False**:
    ``wait_signal`` only echoes the spec, ``checkpoint`` returns a symbolic token, and
    ``restore`` / ``cancel`` are no-ops. Kernel-level replay and filesystem checkpoints
    remain the real durability story (see ``docs/runtime-execution.md``).

    Set ``honest_capabilities=False`` only for legacy control-plane tests that expect
    the adapter to *advertise* broad capability support while still implementing the
    same stub behavior.
    """

    adapter_id: str = "native"
    honest_capabilities: bool = True
    agent_worker: AgentWorkerAdapter | None = None
    _bundle: RuntimeBundle | None = field(default=None, init=False, repr=False)

    def capabilities(self) -> RuntimeAdapterCapabilities:
        if self.honest_capabilities:
            return RuntimeAdapterCapabilities()
        return RuntimeAdapterCapabilities(
            supports_durable_waits=True,
            supports_external_signals=True,
            supports_compensation_hooks=True,
            supports_external_checkpointing=True,
        )

    def prepare(self, *, context: RuntimeContext, bundle: RuntimeBundle) -> None:
        _ = context
        self._bundle = bundle

    def _resolve_agent_worker(self, *, action: RuntimeAction) -> AgentWorkerAdapter:
        if self.agent_worker is not None:
            return TimeoutEnforcingAgentWorker(inner=self.agent_worker)
        return agent_worker_from_env(bundle=self._bundle, action=action)

    def execute_action(self, *, context: RuntimeContext, action: RuntimeAction) -> RuntimeActionResult:
        if action.action_type == "coordination.step":
            rwc = build_role_worker_context(context=context, action=action, bundle=self._bundle)
            turn = self._resolve_agent_worker(action=action).execute_role_turn(context=rwc)
            return coordination_step_runtime_result(adapter_id=self.adapter_id, action=action, turn=turn)
        return RuntimeActionResult(
            status="succeeded",
            outputs={
                "action_id": action.action_id,
                "action_type": action.action_type,
                "adapter_id": self.adapter_id,
            },
            duration_ms=0,
        )

    def wait_signal(self, *, context: RuntimeContext, signal_spec: Mapping[str, object]) -> object:
        _ = context
        return dict(signal_spec)

    def checkpoint(self, *, context: RuntimeContext) -> str | None:
        return f"{context.runtime_run_id}:native"

    def restore(self, *, context: RuntimeContext, checkpoint_token: str) -> None:
        _ = (context, checkpoint_token)

    def cancel(self, *, context: RuntimeContext, action_id: str) -> None:
        _ = (context, action_id)
