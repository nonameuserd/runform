from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from akc.memory.models import JSONValue, require_non_empty

ProvenanceKind = Literal["doc_chunk", "message", "openapi_operation", "file", "other"]
ALLOWED_PROVENANCE_KINDS: tuple[str, ...] = (
    "doc_chunk",
    "message",
    "openapi_operation",
    "file",
    "other",
)


@dataclass(frozen=True, slots=True)
class ProvenancePointer:
    """Pointer to source material used to derive an IR node.

    Tenant isolation note:
    - `tenant_id` is explicit and validated, so provenance cannot be mixed across
      tenants without an explicit transform.
    """

    tenant_id: str
    kind: ProvenanceKind
    source_id: str
    locator: str | None = None
    sha256: str | None = None
    metadata: dict[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="provenance.tenant_id")
        require_non_empty(self.kind, name="provenance.kind")
        require_non_empty(self.source_id, name="provenance.source_id")
        if self.kind not in ALLOWED_PROVENANCE_KINDS:
            raise ValueError(f"provenance.kind must be one of {ALLOWED_PROVENANCE_KINDS}; got {self.kind!r}")
        if self.sha256 is not None:
            s = self.sha256.strip().lower()
            if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
                raise ValueError("provenance.sha256 must be a 64-char hex string when set")
            object.__setattr__(self, "sha256", s)

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "tenant_id": self.tenant_id.strip(),
            "kind": self.kind,
            "source_id": self.source_id.strip(),
            "locator": self.locator.strip() if isinstance(self.locator, str) else None,
            "sha256": self.sha256,
            "metadata": dict(self.metadata) if self.metadata else None,
        }
        return {k: v for k, v in out.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> ProvenancePointer:
        metadata = obj.get("metadata")
        if metadata is not None and not isinstance(metadata, dict):
            raise ValueError("provenance.metadata must be an object when set")
        return ProvenancePointer(
            tenant_id=str(obj.get("tenant_id", "")),
            kind=str(obj.get("kind", "other")),  # type: ignore[arg-type]
            source_id=str(obj.get("source_id", "")),
            locator=str(obj.get("locator")) if obj.get("locator") is not None else None,
            sha256=str(obj.get("sha256")) if obj.get("sha256") is not None else None,
            metadata=metadata,
        )
