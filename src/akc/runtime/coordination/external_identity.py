"""Optional external identity metadata for coordination (SPIFFE / OPA) — stub contract.

Full sidecar/SVID issuance is a deployment adapter concern; the coordinator records
stable metadata fields when an adapter populates them.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from akc.memory.models import JSONValue


def stub_external_identity_metadata(
    *,
    spiffe_id: str | None = None,
    opa_policy_bundle_version: str | None = None,
    opa_policy_bundle_digest_sha256: str | None = None,
) -> dict[str, JSONValue]:
    """No-op default metadata shape for audit linkage (tests assert keys/types)."""

    return {
        "spiffe_id": spiffe_id,
        "opa_policy_bundle_version": opa_policy_bundle_version,
        "opa_policy_bundle_digest_sha256": opa_policy_bundle_digest_sha256,
        "integration": "stub",
    }


def validate_external_identity_metadata_shape(obj: Mapping[str, Any]) -> tuple[str, ...]:
    """Return validation issue strings for required keys when present."""

    issues: list[str] = []
    if not isinstance(obj, dict):
        return ("external_identity_metadata must be an object",)
    if "spiffe_id" in obj and obj["spiffe_id"] is not None and not isinstance(obj["spiffe_id"], str):
        issues.append("spiffe_id must be a string or null")
    if (
        "opa_policy_bundle_version" in obj
        and obj["opa_policy_bundle_version"] is not None
        and not isinstance(obj["opa_policy_bundle_version"], str)
    ):
        issues.append("opa_policy_bundle_version must be a string or null")
    if (
        "opa_policy_bundle_digest_sha256" in obj
        and obj["opa_policy_bundle_digest_sha256"] is not None
        and not isinstance(obj["opa_policy_bundle_digest_sha256"], str)
    ):
        issues.append("opa_policy_bundle_digest_sha256 must be a string or null")
    return tuple(issues)
