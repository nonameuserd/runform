from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

AdoptionLevel: TypeAlias = Literal["observer", "advisor", "copilot", "compiler", "autonomy"]


def parse_adoption_level(raw: str | int | None) -> AdoptionLevel | None:
    """Parse `.akc/project.json` adoption_level tokens.

    Accepts string tokens (observer/advisor/...) or numeric 0..4 strings/ints.
    """

    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, int):
        if raw == 0:
            return "observer"
        if raw == 1:
            return "advisor"
        if raw == 2:
            return "copilot"
        if raw == 3:
            return "compiler"
        if raw == 4:
            return "autonomy"
        return None
    s = str(raw).strip().lower()
    if not s:
        return None
    if s in {"0", "level0", "l0"}:
        return "observer"
    if s in {"1", "level1", "l1"}:
        return "advisor"
    if s in {"2", "level2", "l2"}:
        return "copilot"
    if s in {"3", "level3", "l3"}:
        return "compiler"
    if s in {"4", "level4", "l4"}:
        return "autonomy"
    if s in {"observer", "read_only", "readonly", "read-only"}:
        return "observer"
    if s in {"advisor", "artifact_only", "artifact-only"}:
        return "advisor"
    if s in {"copilot", "co_pilot", "co-pilot", "scoped_apply", "scoped-apply"}:
        return "copilot"
    if s in {"compiler"}:
        return "compiler"
    if s in {"autonomy", "full_autonomy", "full-autonomy"}:
        return "autonomy"
    return None


def adoption_level_index(level: AdoptionLevel) -> int:
    if level == "observer":
        return 0
    if level == "advisor":
        return 1
    if level == "copilot":
        return 2
    if level == "compiler":
        return 3
    return 4


def recommended_compile_realization_mode(level: AdoptionLevel) -> Literal["artifact_only", "scoped_apply"]:
    """Map ladder level to the safest default compile realization."""

    if adoption_level_index(level) <= 1:
        return "artifact_only"
    return "scoped_apply"


@dataclass(frozen=True, slots=True)
class AdvisorEvidence:
    """Evidence row for Level 1 (advisor) runs.

    Intended to be serialized/aggregated by higher-level control-plane tooling.
    """

    run_id: str
    manifest_relpath: str
    approved: bool | None = None
