"""Shared entry to run `safe_recompile_on_drift` with explicit parameters (CLI, webhooks, CI)."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

from akc.compile.interfaces import LLMBackend
from akc.living.automation_profile import LivingAutomationProfile
from akc.living.safe_recompile import safe_recompile_on_drift


def living_recompile_execute(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    ingest_state_path: Path,
    baseline_path: Path | None,
    eval_suite_path: Path,
    goal: str,
    policy_mode: Literal["audit_only", "enforce"],
    canary_mode: Literal["quick", "thorough"],
    accept_mode: Literal["quick", "thorough"],
    canary_test_mode: Literal["smoke", "full"],
    allow_network: bool,
    llm_backend: LLMBackend | None,
    update_baseline_on_accept: bool,
    skip_other_pending: bool,
    opa_policy_path: str | None,
    opa_decision_path: str,
    living_automation_profile: LivingAutomationProfile,
) -> int:
    """One-shot living recompile; same core path as ``akc living-recompile`` and autopilot drift handling."""

    return int(
        safe_recompile_on_drift(
            tenant_id=str(tenant_id),
            repo_id=str(repo_id),
            outputs_root=outputs_root,
            ingest_state_path=ingest_state_path,
            baseline_path=baseline_path,
            eval_suite_path=eval_suite_path,
            goal=str(goal),
            policy_mode=policy_mode,
            canary_mode=canary_mode,
            accept_mode=accept_mode,
            canary_test_mode=canary_test_mode,
            allow_network=allow_network,
            llm_backend=llm_backend,
            update_baseline_on_accept=update_baseline_on_accept,
            skip_other_pending=skip_other_pending,
            opa_policy_path=opa_policy_path,
            opa_decision_path=opa_decision_path,
            living_automation_profile=living_automation_profile,
        )
    )
