from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field

from akc.runtime.models import RuntimeEvent

RuntimeEventHandler = Callable[[RuntimeEvent], None]


@dataclass(slots=True)
class RuntimeEventBus:
    _handlers: list[RuntimeEventHandler] = field(default_factory=list)
    _events: list[RuntimeEvent] = field(default_factory=list)

    def publish(self, event: RuntimeEvent) -> None:
        self._events.append(event)
        for handler in tuple(self._handlers):
            handler(event)

    def subscribe(self, handler: RuntimeEventHandler) -> None:
        self._handlers.append(handler)

    def snapshot(self) -> tuple[RuntimeEvent, ...]:
        return tuple(self._events)
