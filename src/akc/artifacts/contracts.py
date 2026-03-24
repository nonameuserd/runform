from __future__ import annotations

from dataclasses import dataclass
from typing import Any

# Phase 3: artifact contract versioning
#
# - The schema version is a single integer that changes only on intentional,
#   backward-incompatible schema changes.
# - Additive, backward-compatible changes must keep this constant and rely on
#   optional fields + schemas that allow additional properties where needed.
ARTIFACT_SCHEMA_VERSION: int = 1


def _require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def schema_id_for(*, kind: str, version: int = ARTIFACT_SCHEMA_VERSION) -> str:
    """Return a stable schema identifier string for emitted artifacts."""

    _require_non_empty(kind, name="kind")
    if int(version) <= 0:
        raise ValueError("version must be > 0")
    return f"akc:{kind}:v{int(version)}"


def is_runtime_bundle_schema_id(value: str) -> bool:
    """Return True if ``value`` is a known ``runtime_bundle`` :func:`schema_id_for` string."""

    s = str(value).strip()
    return s in (
        schema_id_for(kind="runtime_bundle", version=1),
        schema_id_for(kind="runtime_bundle", version=2),
        schema_id_for(kind="runtime_bundle", version=3),
        schema_id_for(kind="runtime_bundle", version=4),
    )


@dataclass(frozen=True, slots=True)
class SchemaEnvelope:
    """Common envelope for versioned artifacts.

    Many artifacts embed this envelope directly at the top-level. Some legacy
    artifacts remain envelope-less; their schemas treat schema metadata as
    optional for compatibility.
    """

    schema_version: int = ARTIFACT_SCHEMA_VERSION
    schema_id: str | None = None

    def to_json_obj(self) -> dict[str, object]:
        obj: dict[str, object] = {"schema_version": int(self.schema_version)}
        if self.schema_id is not None:
            _require_non_empty(self.schema_id, name="schema_id")
            obj["schema_id"] = self.schema_id
        return obj


def apply_schema_envelope(
    *,
    obj: dict[str, Any],
    kind: str,
    version: int = ARTIFACT_SCHEMA_VERSION,
) -> dict[str, Any]:
    """Add schema metadata fields to an artifact object (in-place safe).

    The envelope is additive: existing keys are preserved unless they conflict.
    """

    if not isinstance(obj, dict):
        raise TypeError("obj must be a dict")
    if "schema_version" not in obj:
        obj["schema_version"] = int(version)
    if "schema_id" not in obj:
        obj["schema_id"] = schema_id_for(kind=kind, version=version)
    return obj
