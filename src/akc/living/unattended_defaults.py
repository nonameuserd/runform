"""Documented defaults for ``living_loop_unattended_v1`` + ``env_profile`` matrix.

Staging favors **more frequent** drift checks and looser daily budgets so issues surface
before production. Production uses **longer** intervals, **tighter** mutation/rollback
budgets, and **stricter** escalation (lower rollback/day ceiling before human escalation).

All resolution remains explicit per tenant/repo (CLI/env/project); these are **defaults
only** for ``akc runtime autopilot --unattended-defaults`` — never cross-tenant.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from akc.runtime.autopilot import AutonomyBudgetConfig

# Keep in sync with ``EnvProfile`` in ``akc.runtime.autopilot`` (avoid importing autopilot at
# module load: ``akc.living`` package imports this module and autopilot imports living).
EnvProfile = Literal["dev", "staging", "prod"]

LeaseBackendHint = Literal["filesystem", "k8s"]


@dataclass(frozen=True, slots=True)
class UnattendedAutopilotDefaults:
    """Bundle of autopilot parameters keyed by ``--env-profile`` for unattended operation."""

    env_profile: EnvProfile
    living_check_interval_s: float
    lease_backend: LeaseBackendHint
    lease_namespace: str | None
    budgets: AutonomyBudgetConfig


def default_lease_namespace(*, lease_backend: LeaseBackendHint, env_profile: EnvProfile) -> str | None:
    """Optional K8s lease namespace hint; filesystem backend ignores this."""

    if lease_backend != "k8s":
        return None
    if env_profile == "prod":
        return "akc-living"
    return "akc-living-staging"


def unattended_autopilot_defaults_for_env(env_profile: str | EnvProfile) -> UnattendedAutopilotDefaults:
    """Return recommended autopilot defaults for the given environment label."""

    from akc.runtime.autopilot import AutonomyBudgetConfig

    raw = str(env_profile).strip().lower()
    ep: EnvProfile
    if raw in {"prod", "production"}:
        ep = "prod"
    elif raw in {"dev", "development", "local"}:
        ep = "dev"
    else:
        ep = "staging"

    lease_backend: LeaseBackendHint = "filesystem"

    if ep == "dev":
        return UnattendedAutopilotDefaults(
            env_profile=ep,
            living_check_interval_s=300.0,
            lease_backend=lease_backend,
            lease_namespace=default_lease_namespace(lease_backend=lease_backend, env_profile=ep),
            budgets=AutonomyBudgetConfig(
                max_mutations_per_day=100,
                max_concurrent_rollouts=3,
                rollback_budget_per_day=40,
                max_consecutive_rollout_failures=3,
                max_rollbacks_per_day_before_escalation=10,
                cooldown_after_failure_ms=30_000,
                cooldown_after_policy_deny_ms=60_000,
            ),
        )
    if ep == "staging":
        return UnattendedAutopilotDefaults(
            env_profile=ep,
            living_check_interval_s=600.0,
            lease_backend=lease_backend,
            lease_namespace=default_lease_namespace(lease_backend=lease_backend, env_profile=ep),
            budgets=AutonomyBudgetConfig(
                max_mutations_per_day=40,
                max_concurrent_rollouts=2,
                rollback_budget_per_day=20,
                max_consecutive_rollout_failures=2,
                max_rollbacks_per_day_before_escalation=8,
                cooldown_after_failure_ms=60_000,
                cooldown_after_policy_deny_ms=120_000,
            ),
        )
    return UnattendedAutopilotDefaults(
        env_profile="prod",
        living_check_interval_s=3600.0,
        lease_backend=lease_backend,
        lease_namespace=default_lease_namespace(lease_backend=lease_backend, env_profile="prod"),
        budgets=AutonomyBudgetConfig(
            max_mutations_per_day=8,
            max_concurrent_rollouts=1,
            rollback_budget_per_day=4,
            max_consecutive_rollout_failures=2,
            max_rollbacks_per_day_before_escalation=3,
            cooldown_after_failure_ms=300_000,
            cooldown_after_policy_deny_ms=600_000,
        ),
    )
