"""Map intent success-criterion evaluation modes to mandatory partial-replay passes.

Kept under ``akc.run`` (not ``akc.intent``) to avoid import cycles: loading
``akc.intent`` must not pull the full ``akc.run`` package during package init.
Success criteria are accepted as duck-typed objects with ``evaluation_mode: str``.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

from akc.run.manifest import REPLAYABLE_PASSES

_REPLAYABLE: frozenset[str] = frozenset(REPLAYABLE_PASSES)

# Keys match ``EvaluationMode`` in ``akc.intent.models`` (stable string literals).
_MODE_TO_PASSES: dict[str, frozenset[str]] = {
    "tests": frozenset({"execute", "verify", "intent_acceptance"}),
    "manifest_check": frozenset({"verify", "intent_acceptance"}),
    "artifact_check": frozenset({"generate", "execute", "intent_acceptance"}),
    "metric_threshold": frozenset({"intent_acceptance", "runtime_bundle", "execute", "repair"}),
    # Operational criteria need a refreshed runtime bundle, execution/repair signals, and acceptance.
    "operational_spec": frozenset({"intent_acceptance", "runtime_bundle", "execute", "repair"}),
    "quality_contract": frozenset({"intent_acceptance", "execute", "verify"}),
    "human_gate": frozenset({"intent_acceptance"}),
}


def mandatory_partial_replay_passes_for_evaluation_modes(*, modes: Iterable[str]) -> frozenset[str]:
    """Return the union of mandatory passes for the given evaluation mode strings."""
    out: set[str] = set()
    for mode in modes:
        if isinstance(mode, str) and mode:
            out.update(_MODE_TO_PASSES.get(mode, frozenset()))
    return frozenset(p for p in out if p in _REPLAYABLE)


def mandatory_partial_replay_passes_for_success_criteria(*, success_criteria: Sequence[object]) -> frozenset[str]:
    """Mandatory partial-replay passes implied by intent success criteria (duck-typed)."""
    if not success_criteria:
        return frozenset()
    modes = (m for sc in success_criteria for m in (getattr(sc, "evaluation_mode", None),) if isinstance(m, str) and m)
    return mandatory_partial_replay_passes_for_evaluation_modes(modes=modes)
