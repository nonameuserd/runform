from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from akc.runtime.models import RuntimeBundleRef, RuntimeEvent


class RuntimeEvidenceWriter(Protocol):
    def write_bundle_ref(self, *, bundle_ref: RuntimeBundleRef) -> None: ...

    def write_event(self, *, event: RuntimeEvent) -> None: ...


@dataclass(slots=True)
class InMemoryRuntimeEvidenceWriter(RuntimeEvidenceWriter):
    bundle_refs: list[RuntimeBundleRef] = field(default_factory=list)
    events: list[RuntimeEvent] = field(default_factory=list)

    def write_bundle_ref(self, *, bundle_ref: RuntimeBundleRef) -> None:
        self.bundle_refs.append(bundle_ref)

    def write_event(self, *, event: RuntimeEvent) -> None:
        self.events.append(event)
