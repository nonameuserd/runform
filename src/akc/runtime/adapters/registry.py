from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field

from akc.memory.models import require_non_empty
from akc.runtime.adapters.base import RuntimeAdapter
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.models import RuntimeContext

RuntimeAdapterFactory = Callable[[], RuntimeAdapter]


@dataclass(frozen=True, slots=True)
class RegisteredRuntimeAdapter:
    adapter_id: str
    factory: RuntimeAdapterFactory
    allowed_policy_modes: tuple[str, ...] = ()


@dataclass(slots=True)
class RuntimeAdapterRegistry:
    _factories: dict[str, RegisteredRuntimeAdapter] = field(default_factory=dict)

    def register(
        self,
        *,
        adapter_id: str,
        factory: RuntimeAdapterFactory,
        allowed_policy_modes: Sequence[str] = (),
    ) -> None:
        require_non_empty(adapter_id, name="adapter_id")
        self._factories[adapter_id.strip()] = RegisteredRuntimeAdapter(
            adapter_id=adapter_id.strip(),
            factory=factory,
            allowed_policy_modes=tuple(str(mode).strip() for mode in allowed_policy_modes if str(mode).strip()),
        )

    def create(
        self,
        *,
        adapter_id: str,
        context: RuntimeContext | None = None,
        policy_allowlist: Sequence[str] | None = None,
    ) -> RuntimeAdapter:
        require_non_empty(adapter_id, name="adapter_id")
        registration = self._factories.get(adapter_id.strip())
        if registration is None:
            raise KeyError(f"unknown runtime adapter: {adapter_id}")
        if policy_allowlist is not None:
            allowlist = {str(item).strip() for item in policy_allowlist if str(item).strip()}
            if registration.adapter_id not in allowlist:
                raise PermissionError(f"runtime adapter not allowed by policy: {registration.adapter_id}")
        if (
            context is not None
            and registration.allowed_policy_modes
            and context.policy_mode not in set(registration.allowed_policy_modes)
        ):
            raise PermissionError(
                f"runtime adapter {registration.adapter_id} not allowed for policy_mode={context.policy_mode}"
            )
        return registration.factory()

    def list_registered(self) -> Mapping[str, RegisteredRuntimeAdapter]:
        return dict(self._factories)


def register_default_runtime_adapters(registry: RuntimeAdapterRegistry) -> None:
    """Register built-in adapters (today: ``native`` only).

    Adapters that need constructor arguments (for example ``local_depth`` with ``outputs_root``) must be
    registered by callers with ``RuntimeAdapterRegistry.register`` and a closure factory.
    """
    registry.register(adapter_id="native", factory=NativeRuntimeAdapter)
