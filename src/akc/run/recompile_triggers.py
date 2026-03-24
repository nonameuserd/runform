from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from akc.memory.models import JSONValue


def normalized_success_criterion_ids_from_runtime_payload(payload: Mapping[str, Any]) -> tuple[str, ...]:
    """Extract intent success criterion identifiers from runtime telemetry payloads.

    Used when operational validity attestation (or similar) events include
    ``success_criterion_id`` / ``success_criterion_ids`` for living drift and
    recompile triggers. Compile-time acceptance checks already attach
    ``success_criterion_id`` on each ``per_criterion`` row in :mod:`akc.intent.acceptance`.

    Tenant-safe: read-only over JSON-shaped mappings; ignores unknown keys.
    """

    ordered: list[str] = []
    seen: set[str] = set()
    raw_ids = payload.get("success_criterion_ids")
    if isinstance(raw_ids, list):
        for x in raw_ids:
            if isinstance(x, str):
                sx = x.strip()
                if sx and sx not in seen:
                    seen.add(sx)
                    ordered.append(sx)
    raw_one = payload.get("success_criterion_id")
    if isinstance(raw_one, str):
        sx2 = raw_one.strip()
        if sx2 and sx2 not in seen:
            seen.add(sx2)
            ordered.append(sx2)
    return tuple(ordered)


RecompileTriggerKind = Literal[
    "intent_semantic_changed",
    "intent_stable_changed",
    "knowledge_semantic_changed",
    "knowledge_provenance_changed",
    "operational_validity_failed",
    "acceptance_criterion_failed",
]

# Deterministic evaluation order for :func:`evaluate_recompile_triggers` (stable drift artifacts).
RECOMPILE_TRIGGER_EVAL_ORDER: tuple[RecompileTriggerKind, ...] = (
    "intent_semantic_changed",
    "intent_stable_changed",
    "knowledge_semantic_changed",
    "knowledge_provenance_changed",
    "operational_validity_failed",
    "acceptance_criterion_failed",
)

OperationalValidityFailedTriggerSeverity = Literal["block", "advisory"]


def _norm_sha256(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    s = value.strip().lower()
    if len(s) != 64 or any(ch not in "0123456789abcdef" for ch in s):
        return None
    return s


def _intent_aligned_for_operational_attestation_triggers(
    *,
    manifest_intent_semantic_fingerprint: str | None,
    current_intent_semantic_fingerprint: str | None,
    manifest_stable_intent_sha256: str | None,
    current_stable_intent_sha256: str | None,
) -> bool:
    """Same intent-equality gating as ``operational_validity_failed`` (semantic + optional stable hash)."""

    if not current_intent_semantic_fingerprint:
        return False
    if not manifest_intent_semantic_fingerprint:
        return False
    if manifest_intent_semantic_fingerprint != current_intent_semantic_fingerprint:
        return False
    cur_s = _norm_sha256(current_stable_intent_sha256)
    man_s = _norm_sha256(manifest_stable_intent_sha256)
    return not (cur_s is not None and man_s is not None and cur_s != man_s)


@dataclass(frozen=True, slots=True)
class RecompileTrigger:
    """Explicit reason that forces recompilation instead of replay.

    This is used to ensure Phase 6 “living / replay” logic can distinguish
    semantic intent changes (real compiler contract changes) from other drift.
    """

    kind: RecompileTriggerKind
    details: Mapping[str, JSONValue]

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "kind": self.kind,
            "details": dict(self.details),
        }


def compute_intent_semantic_changed_trigger(
    *,
    manifest_intent_semantic_fingerprint: str | None,
    current_intent_semantic_fingerprint: str | None,
) -> RecompileTrigger | None:
    """Return a trigger when semantic intent differs between replay baseline and current run."""

    if not current_intent_semantic_fingerprint:
        return None
    if not manifest_intent_semantic_fingerprint:
        # Unknown baseline (older manifests, missing fields): do not force recompilation.
        return None
    if manifest_intent_semantic_fingerprint == current_intent_semantic_fingerprint:
        return None

    return RecompileTrigger(
        kind="intent_semantic_changed",
        details={
            "manifest_intent_semantic_fingerprint": manifest_intent_semantic_fingerprint,
            "current_intent_semantic_fingerprint": current_intent_semantic_fingerprint,
        },
    )


def compute_intent_stable_changed_trigger(
    *,
    manifest_stable_intent_sha256: str | None,
    current_stable_intent_sha256: str | None,
) -> RecompileTrigger | None:
    """Return a trigger when canonical intent bytes differ (includes goal text and metadata)."""

    current = _norm_sha256(current_stable_intent_sha256)
    baseline = _norm_sha256(manifest_stable_intent_sha256)
    if current is None:
        return None
    if baseline is None:
        return None
    if baseline == current:
        return None

    return RecompileTrigger(
        kind="intent_stable_changed",
        details={
            "manifest_stable_intent_sha256": baseline,
            "current_stable_intent_sha256": current,
        },
    )


def compute_knowledge_semantic_changed_trigger(
    *,
    manifest_knowledge_semantic_fingerprint: str | None,
    current_knowledge_semantic_fingerprint: str | None,
) -> RecompileTrigger | None:
    """Return a trigger when knowledge semantic fingerprint differs."""
    if not current_knowledge_semantic_fingerprint:
        return None
    if not manifest_knowledge_semantic_fingerprint:
        return None
    if manifest_knowledge_semantic_fingerprint == current_knowledge_semantic_fingerprint:
        return None

    return RecompileTrigger(
        kind="knowledge_semantic_changed",
        details={
            "manifest_knowledge_semantic_fingerprint": manifest_knowledge_semantic_fingerprint,
            "current_knowledge_semantic_fingerprint": current_knowledge_semantic_fingerprint,
        },
    )


def compute_knowledge_provenance_changed_trigger(
    *,
    manifest_knowledge_provenance_fingerprint: str | None,
    current_knowledge_provenance_fingerprint: str | None,
) -> RecompileTrigger | None:
    """Return a trigger when knowledge provenance fingerprint differs."""
    if not current_knowledge_provenance_fingerprint:
        return None
    if not manifest_knowledge_provenance_fingerprint:
        return None
    if manifest_knowledge_provenance_fingerprint == current_knowledge_provenance_fingerprint:
        return None

    return RecompileTrigger(
        kind="knowledge_provenance_changed",
        details={
            "manifest_knowledge_provenance_fingerprint": manifest_knowledge_provenance_fingerprint,
            "current_knowledge_provenance_fingerprint": current_knowledge_provenance_fingerprint,
        },
    )


def compute_operational_validity_failed_trigger(
    *,
    operational_validity_failed: bool,
    operational_validity_failed_trigger_severity: OperationalValidityFailedTriggerSeverity = "block",
    manifest_intent_semantic_fingerprint: str | None,
    current_intent_semantic_fingerprint: str | None,
    manifest_stable_intent_sha256: str | None,
    current_stable_intent_sha256: str | None,
    operational_validity_success_criterion_ids: tuple[str, ...] | None = None,
) -> RecompileTrigger | None:
    """When attestation fails but semantic intent matches baseline, record an explicit recompile reason."""

    if not operational_validity_failed:
        return None
    if operational_validity_failed_trigger_severity != "block":
        return None
    if not _intent_aligned_for_operational_attestation_triggers(
        manifest_intent_semantic_fingerprint=manifest_intent_semantic_fingerprint,
        current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
        manifest_stable_intent_sha256=manifest_stable_intent_sha256,
        current_stable_intent_sha256=current_stable_intent_sha256,
    ):
        return None

    details: dict[str, JSONValue] = {
        "reason": "operational_validity_attestation_failed_while_intent_fingerprints_matched_baseline",
    }
    sc_ids = tuple(str(x).strip() for x in (operational_validity_success_criterion_ids or ()) if str(x).strip())
    if sc_ids:
        details["success_criterion_ids"] = list(sc_ids)
        if len(sc_ids) == 1:
            details["success_criterion_id"] = sc_ids[0]

    return RecompileTrigger(
        kind="operational_validity_failed",
        details=details,
    )


def compute_acceptance_criterion_failed_triggers(
    *,
    enable_granular_acceptance_triggers: bool,
    operational_validity_failed: bool,
    operational_validity_failed_trigger_severity: OperationalValidityFailedTriggerSeverity = "block",
    operational_validity_success_criterion_ids: tuple[str, ...] | None,
    manifest_intent_semantic_fingerprint: str | None,
    current_intent_semantic_fingerprint: str | None,
    manifest_stable_intent_sha256: str | None,
    current_stable_intent_sha256: str | None,
) -> tuple[RecompileTrigger, ...]:
    """Optional per-criterion triggers when operational attestation fails (policy-gated)."""

    if not enable_granular_acceptance_triggers:
        return ()
    if not operational_validity_failed:
        return ()
    if operational_validity_failed_trigger_severity != "block":
        return ()
    if not _intent_aligned_for_operational_attestation_triggers(
        manifest_intent_semantic_fingerprint=manifest_intent_semantic_fingerprint,
        current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
        manifest_stable_intent_sha256=manifest_stable_intent_sha256,
        current_stable_intent_sha256=current_stable_intent_sha256,
    ):
        return ()
    ids = tuple(str(x).strip() for x in (operational_validity_success_criterion_ids or ()) if str(x).strip())
    if not ids:
        return ()
    return tuple(
        RecompileTrigger(
            kind="acceptance_criterion_failed",
            details={
                "success_criterion_id": cid,
                "reason": "operational_validity_attestation_failed",
            },
        )
        for cid in ids
    )


def is_known_recompile_trigger_kind(kind: str) -> bool:
    """Return True when ``kind`` is a documented :class:`RecompileTriggerKind` value."""

    return kind in set(RECOMPILE_TRIGGER_EVAL_ORDER)


def evaluate_recompile_triggers(
    *,
    manifest_intent_semantic_fingerprint: str | None,
    current_intent_semantic_fingerprint: str | None,
    manifest_knowledge_semantic_fingerprint: str | None,
    current_knowledge_semantic_fingerprint: str | None,
    manifest_knowledge_provenance_fingerprint: str | None,
    current_knowledge_provenance_fingerprint: str | None,
    manifest_stable_intent_sha256: str | None = None,
    current_stable_intent_sha256: str | None = None,
    operational_validity_failed: bool = False,
    operational_validity_failed_trigger_severity: OperationalValidityFailedTriggerSeverity = "block",
    operational_validity_success_criterion_ids: tuple[str, ...] | None = None,
    enable_granular_acceptance_triggers: bool = False,
) -> tuple[RecompileTrigger, ...]:
    triggers = [
        compute_intent_semantic_changed_trigger(
            manifest_intent_semantic_fingerprint=manifest_intent_semantic_fingerprint,
            current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
        ),
        compute_intent_stable_changed_trigger(
            manifest_stable_intent_sha256=manifest_stable_intent_sha256,
            current_stable_intent_sha256=current_stable_intent_sha256,
        ),
        compute_knowledge_semantic_changed_trigger(
            manifest_knowledge_semantic_fingerprint=manifest_knowledge_semantic_fingerprint,
            current_knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
        ),
        compute_knowledge_provenance_changed_trigger(
            manifest_knowledge_provenance_fingerprint=manifest_knowledge_provenance_fingerprint,
            current_knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
        ),
        compute_operational_validity_failed_trigger(
            operational_validity_failed=operational_validity_failed,
            operational_validity_failed_trigger_severity=operational_validity_failed_trigger_severity,
            manifest_intent_semantic_fingerprint=manifest_intent_semantic_fingerprint,
            current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
            manifest_stable_intent_sha256=manifest_stable_intent_sha256,
            current_stable_intent_sha256=current_stable_intent_sha256,
            operational_validity_success_criterion_ids=operational_validity_success_criterion_ids,
        ),
    ]
    triggers.extend(
        compute_acceptance_criterion_failed_triggers(
            enable_granular_acceptance_triggers=enable_granular_acceptance_triggers,
            operational_validity_failed=operational_validity_failed,
            operational_validity_failed_trigger_severity=operational_validity_failed_trigger_severity,
            operational_validity_success_criterion_ids=operational_validity_success_criterion_ids,
            manifest_intent_semantic_fingerprint=manifest_intent_semantic_fingerprint,
            current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
            manifest_stable_intent_sha256=manifest_stable_intent_sha256,
            current_stable_intent_sha256=current_stable_intent_sha256,
        )
    )
    return tuple(trigger for trigger in triggers if trigger is not None)
