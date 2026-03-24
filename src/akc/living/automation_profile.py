"""Living systems automation profile (Phase E).

One named profile bundles:
- recompile trigger defaults (granular acceptance, operational validity severity),
- time-compression baseline for manifest evidence,
- runtime→living bridge participation (autopilot includes transcript events only when enabled).

Default is ``off`` (fail-closed for observe→recompile). Opt in with env
``AKC_LIVING_AUTOMATION_PROFILE=living_loop_v1`` (interactive / bridge) or
``living_loop_unattended_v1`` (same bridge + documented unattended autopilot defaults),
or ``.akc/project.json`` field ``living_automation_profile``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

LIVING_AUTOMATION_PROFILE_ENV = "AKC_LIVING_AUTOMATION_PROFILE"

LivingAutomationProfileId = Literal["off", "living_loop_v1", "living_loop_unattended_v1"]

LIVING_LOOP_V1_DEFAULT_BASELINE_HOURS = 8.0


@dataclass(frozen=True, slots=True)
class LivingAutomationProfile:
    """Resolved automation posture for living loop + evidence defaults."""

    id: LivingAutomationProfileId
    granular_acceptance_default: bool
    operational_validity_failed_trigger_severity_default: Literal["block", "advisory"]
    baseline_duration_hours: float | None


PROFILE_OFF = LivingAutomationProfile(
    id="off",
    granular_acceptance_default=False,
    operational_validity_failed_trigger_severity_default="block",
    baseline_duration_hours=None,
)

PROFILE_LIVING_LOOP_V1 = LivingAutomationProfile(
    id="living_loop_v1",
    granular_acceptance_default=True,
    operational_validity_failed_trigger_severity_default="block",
    baseline_duration_hours=LIVING_LOOP_V1_DEFAULT_BASELINE_HOURS,
)

PROFILE_LIVING_LOOP_UNATTENDED_V1 = LivingAutomationProfile(
    id="living_loop_unattended_v1",
    granular_acceptance_default=True,
    operational_validity_failed_trigger_severity_default="block",
    baseline_duration_hours=LIVING_LOOP_V1_DEFAULT_BASELINE_HOURS,
)


def parse_living_automation_profile_token(raw: object) -> LivingAutomationProfileId:
    """Map a single token to a profile id; unknown values resolve to ``off`` (fail-closed)."""

    s = str(raw).strip().lower()
    if not s or s in {"0", "false", "none", "no", "disabled", "off"}:
        return "off"
    if s in {"living_loop_v1", "living-loop-v1"}:
        return "living_loop_v1"
    if s in {"living_loop_unattended_v1", "living-loop-unattended-v1", "unattended_v1"}:
        return "living_loop_unattended_v1"
    return "off"


def living_automation_profile_from_id(profile_id: LivingAutomationProfileId) -> LivingAutomationProfile:
    if profile_id == "living_loop_v1":
        return PROFILE_LIVING_LOOP_V1
    if profile_id == "living_loop_unattended_v1":
        return PROFILE_LIVING_LOOP_UNATTENDED_V1
    return PROFILE_OFF


def resolve_living_automation_profile(
    *,
    cli_value: str | None,
    env: Mapping[str, str],
    project_value: str | None,
    env_key: str = LIVING_AUTOMATION_PROFILE_ENV,
) -> LivingAutomationProfile:
    """Resolve profile with precedence: CLI > env > project > off.

    Tenant-safe: only reads string keys from the provided env mapping; unknown
    profile strings fall back to ``off``.
    """

    for candidate in (cli_value, env.get(env_key), project_value):
        if candidate is None:
            continue
        if str(candidate).strip() == "":
            continue
        return living_automation_profile_from_id(parse_living_automation_profile_token(candidate))
    return PROFILE_OFF


def living_automation_includes_runtime_bridge(*, profile: LivingAutomationProfile) -> bool:
    """Whether autopilot / living flows may feed runtime transcript into safe recompile."""

    return profile.id in {"living_loop_v1", "living_loop_unattended_v1"}
