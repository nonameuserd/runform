"""Phase 3 controller configuration (tiers + budgets).

This is a lightweight, dependency-free configuration model for an ARCS-style
tiered controller. The controller can choose different tiers per stage and
enforce per-run budgets (calls, tokens, wall time, etc.).
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, TypeAlias

from akc.compile.interfaces import Stage
from akc.memory.models import JSONValue, require_non_empty

TierName: TypeAlias = Literal["small", "medium", "large"]
TestMode: TypeAlias = Literal["smoke", "full"]


@dataclass(frozen=True, slots=True)
class CostRates:
    """Provider/tool billing rates used for deterministic cost accounting."""

    input_per_1k_tokens_usd: float = 0.0
    output_per_1k_tokens_usd: float = 0.0
    tool_call_usd: float = 0.0

    def __post_init__(self) -> None:
        if float(self.input_per_1k_tokens_usd) < 0:
            raise ValueError("cost_rates.input_per_1k_tokens_usd must be >= 0")
        if float(self.output_per_1k_tokens_usd) < 0:
            raise ValueError("cost_rates.output_per_1k_tokens_usd must be >= 0")
        if float(self.tool_call_usd) < 0:
            raise ValueError("cost_rates.tool_call_usd must be >= 0")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "input_per_1k_tokens_usd": float(self.input_per_1k_tokens_usd),
            "output_per_1k_tokens_usd": float(self.output_per_1k_tokens_usd),
            "tool_call_usd": float(self.tool_call_usd),
        }


@dataclass(frozen=True, slots=True)
class Budget:
    """A bounded budget for controller operations.

    Budgets are conservative and intended to be enforced by the controller.
    """

    max_llm_calls: int = 10
    # Deprecated (back-compat): older name used by early Phase 3 controller.
    # Prefer `max_repairs_per_step`. This may be removed in a future major version.
    max_repair_iterations: int | None = None
    # Maximum repair iterations within a single plan step
    # (not counting the initial generate attempt).
    max_repairs_per_step: int = 3
    # Total (generate+repair) iterations allowed for a single plan step.
    max_iterations_total: int = 5
    max_tool_calls: int | None = None
    max_input_tokens: int | None = None
    max_total_tokens: int | None = None
    max_output_tokens: int | None = None
    max_wall_time_s: float | None = None
    max_cost_usd: float | None = None

    def __post_init__(self) -> None:
        if int(self.max_llm_calls) <= 0:
            raise ValueError("max_llm_calls must be > 0")
        if self.max_repair_iterations is not None and int(self.max_repair_iterations) < 0:
            raise ValueError("max_repair_iterations must be >= 0 when set")
        if self.max_repair_iterations is not None:
            warnings.warn(
                "Budget.max_repair_iterations is deprecated; use max_repairs_per_step instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        if int(self.max_repairs_per_step) < 0:
            raise ValueError("max_repairs_per_step must be >= 0")
        if int(self.max_iterations_total) <= 0:
            raise ValueError("max_iterations_total must be > 0")
        if self.max_tool_calls is not None and int(self.max_tool_calls) < 0:
            raise ValueError("max_tool_calls must be >= 0 when set")
        if self.max_input_tokens is not None and int(self.max_input_tokens) <= 0:
            raise ValueError("max_input_tokens must be > 0 when set")
        if self.max_total_tokens is not None and int(self.max_total_tokens) <= 0:
            raise ValueError("max_total_tokens must be > 0 when set")
        if self.max_output_tokens is not None and int(self.max_output_tokens) <= 0:
            raise ValueError("max_output_tokens must be > 0 when set")
        if self.max_wall_time_s is not None and float(self.max_wall_time_s) <= 0:
            raise ValueError("max_wall_time_s must be > 0 when set")
        if self.max_cost_usd is not None and float(self.max_cost_usd) <= 0:
            raise ValueError("max_cost_usd must be > 0 when set")

    def effective_max_repairs_per_step(self) -> int:
        """Resolve the effective per-step repair budget (backward compatible)."""

        if self.max_repair_iterations is not None:
            return int(self.max_repair_iterations)
        return int(self.max_repairs_per_step)

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "max_llm_calls": int(self.max_llm_calls),
            "max_repair_iterations": int(self.max_repair_iterations)
            if self.max_repair_iterations is not None
            else None,
            "max_repairs_per_step": int(self.max_repairs_per_step),
            "max_iterations_total": int(self.max_iterations_total),
            "max_tool_calls": int(self.max_tool_calls) if self.max_tool_calls is not None else None,
            "max_input_tokens": int(self.max_input_tokens)
            if self.max_input_tokens is not None
            else None,
            "max_total_tokens": int(self.max_total_tokens)
            if self.max_total_tokens is not None
            else None,
            "max_output_tokens": int(self.max_output_tokens)
            if self.max_output_tokens is not None
            else None,
            "max_wall_time_s": float(self.max_wall_time_s)
            if self.max_wall_time_s is not None
            else None,
            "max_cost_usd": float(self.max_cost_usd) if self.max_cost_usd is not None else None,
        }
        return obj


@dataclass(frozen=True, slots=True)
class TierConfig:
    """Configuration for an individual tier (model routing, knobs, metadata)."""

    name: TierName
    llm_model: str
    temperature: float = 0.2
    default_max_output_tokens: int | None = None
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.name, name="tier.name")
        require_non_empty(self.llm_model, name="tier.llm_model")
        t = float(self.temperature)
        if t < 0.0 or t > 2.0:
            raise ValueError("tier.temperature must be within [0.0, 2.0]")
        if self.default_max_output_tokens is not None and int(self.default_max_output_tokens) <= 0:
            raise ValueError("default_max_output_tokens must be > 0 when set")


@dataclass(frozen=True, slots=True)
class ControllerConfig:
    """Tiered controller configuration for Phase 3 compile loop orchestration."""

    tiers: Mapping[TierName, TierConfig]
    stage_tiers: Mapping[Stage, TierName] | None = None
    budget: Budget = Budget()
    # Phase 5.3 (optional): tests generated by default.
    #
    # When enabled, the controller will instruct the Generate/Repair stages to
    # include relevant test changes in the patch. Additionally, promotion can be
    # gated on the patch touching test files when non-test code is modified.
    generate_tests_by_default: bool = True
    require_tests_for_non_test_changes: bool = True
    # Tests-by-default: each candidate evaluation runs tests in the isolated workdir.
    #
    # Back-compat: older code used `metadata["execute_command"]` / `metadata["execute_timeout_s"]`.
    # If these fields are unset, the controller will fall back to those metadata keys.
    test_command: tuple[str, ...] | None = None
    test_timeout_s: float | None = None
    test_mode: TestMode = "smoke"
    # When test_mode="smoke", control how often we run the full test gate.
    #
    # - None: run full tests whenever smoke passes (promotion gate every time).
    # - N>=1: run full tests only when (iteration_idx % N == 0) OR on the last allowed iteration.
    full_test_every_n_iterations: int | None = None
    # Phase 5: verifier gate (ProofWright-style). Runs after tests pass and can veto promotion.
    verifier_enabled: bool = True
    # If strict, any verifier finding vetoes. If relaxed, only errors veto.
    verifier_strict: bool = True
    # Phase hardening: default-deny tool authorization + explicit allowlist.
    policy_mode: Literal["audit_only", "enforce"] = "enforce"
    tool_allowlist: tuple[str, ...] = ()
    # First-class rates for deterministic cost accounting.
    cost_rates: CostRates = CostRates()
    # Optional OPA/Rego policy integration.
    opa_policy_path: str | None = None
    opa_decision_path: str = "data.akc.allow"
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        if not self.tiers:
            raise ValueError("tiers must be non-empty")
        # Ensure tier mapping is consistent with TierConfig.name.
        for name, cfg in self.tiers.items():
            if cfg.name != name:
                raise ValueError("tiers key must match TierConfig.name")
        if self.stage_tiers is not None:
            for _stage, tier_name in self.stage_tiers.items():
                if tier_name not in self.tiers:
                    raise ValueError(f"stage_tiers references unknown tier: {tier_name}")
        if self.test_command is not None and len(self.test_command) == 0:
            raise ValueError("test_command must be non-empty when set")
        if self.test_timeout_s is not None and float(self.test_timeout_s) <= 0:
            raise ValueError("test_timeout_s must be > 0 when set")
        if (
            self.full_test_every_n_iterations is not None
            and int(self.full_test_every_n_iterations) <= 0
        ):
            raise ValueError("full_test_every_n_iterations must be > 0 when set")
        if not isinstance(self.generate_tests_by_default, bool):
            raise ValueError("generate_tests_by_default must be a bool")
        if not isinstance(self.require_tests_for_non_test_changes, bool):
            raise ValueError("require_tests_for_non_test_changes must be a bool")
        if not isinstance(self.verifier_enabled, bool):
            raise ValueError("verifier_enabled must be a bool")
        if not isinstance(self.verifier_strict, bool):
            raise ValueError("verifier_strict must be a bool")
        if self.policy_mode not in {"audit_only", "enforce"}:
            raise ValueError("policy_mode must be one of: audit_only, enforce")
        if any(not isinstance(a, str) or not a.strip() for a in self.tool_allowlist):
            raise ValueError("tool_allowlist must contain non-empty action names")
        if self.opa_policy_path is not None and not str(self.opa_policy_path).strip():
            raise ValueError("opa_policy_path must be non-empty when set")
        if not isinstance(self.opa_decision_path, str) or not self.opa_decision_path.strip():
            raise ValueError("opa_decision_path must be non-empty")

    def tier_for_stage(self, *, stage: Stage) -> TierConfig:
        """Resolve the tier config for a stage, defaulting conservatively."""

        if self.stage_tiers is None:
            # Default policy: use medium if available, else smallest key.
            if "medium" in self.tiers:
                return self.tiers["medium"]
            return self.tiers[sorted(self.tiers.keys())[0]]
        name = self.stage_tiers.get(stage)
        if name is None:
            if "medium" in self.tiers:
                return self.tiers["medium"]
            return self.tiers[sorted(self.tiers.keys())[0]]
        return self.tiers[name]
