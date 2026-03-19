"""Artifact contracts and schemas for AKC outputs.

This package defines stability rules and machine-checkable schemas for artifacts
emitted under a tenant/repo scope (e.g. manifest.json, .akc/tests/*.json).
"""

from __future__ import annotations

from .contracts import (
    ARTIFACT_SCHEMA_VERSION,
    apply_schema_envelope,
    schema_id_for,
)

__all__ = ["ARTIFACT_SCHEMA_VERSION", "apply_schema_envelope", "schema_id_for"]
