"""Phase 3 controller configuration (tiers + budgets).

This is a lightweight, dependency-free configuration model for an ARCS-style
tiered controller. The controller can choose different tiers per stage and
enforce per-run budgets (calls, tokens, wall time, etc.).
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, TypeAlias, cast

from akc.compile.interfaces import Stage
from akc.control.policy import KnowledgeUnresolvedConflictPolicy
from akc.memory.models import JSONValue, require_non_empty

TierName: TypeAlias = Literal["small", "medium", "large"]
CompileMcpToolStage: TypeAlias = Literal["generate", "repair"]
TestMode: TypeAlias = Literal["smoke", "full"]
KnowledgeExtractionMode: TypeAlias = Literal["deterministic", "llm", "hybrid"]
DocDerivedAssertionsMode: TypeAlias = Literal["off", "limited"]
StoredAssertionIndexMode: TypeAlias = Literal["off", "merge"]
CompilePromptIntentContractPolicy: TypeAlias = Literal["auto", "full", "reference_first"]
OperationalValidityFailedTriggerSeverity: TypeAlias = Literal["block", "advisory"]
CompileRealizationMode: TypeAlias = Literal["artifact_only", "scoped_apply"]
CompileSkillsMode: TypeAlias = Literal["off", "default_only", "explicit", "auto"]


@dataclass(frozen=True, slots=True)
class CompileMcpToolSpec:
    """One MCP ``tools/call`` invocation during compile (generate/repair), under policy + budgets."""

    tool_name: str
    arguments: dict[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_non_empty(self.tool_name, name="compile_mcp_tool.tool_name")


@dataclass(frozen=True, slots=True)
class DocDerivedPatternOptions:
    """Optional high-precision doc-derived assertion patterns (A2/A4).

    All default to False so compile-time ``limited`` mode matches legacy behavior.
    Ingest may pass a preset with flags enabled for broader indexing under the same cap.
    """

    rfc2119_bcp14: bool = False
    numbered_requirements: bool = False
    table_normative_rows: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.rfc2119_bcp14, bool):
            raise ValueError("rfc2119_bcp14 must be a bool")
        if not isinstance(self.numbered_requirements, bool):
            raise ValueError("numbered_requirements must be a bool")
        if not isinstance(self.table_normative_rows, bool):
            raise ValueError("table_normative_rows must be a bool")


@dataclass(frozen=True, slots=True)
class KnowledgeEvidenceWeighting:
    """Optional document-metadata boosts for knowledge conflict mediation (B1).

    When all retrieved chunks omit the relevant metadata fields, scores match the
    legacy overlap/cardinality baseline. Tenant isolation: only numeric weights
    and chunk-local metadata are read; no cross-tenant lookups.
    """

    trust_tier_bonus: Mapping[str, float] | None = None
    pinned_bonus: float = 6.0
    recency_halflife_days: float = 120.0
    recency_max_bonus: float = 4.0
    doc_version_step_bonus: float = 0.5

    def __post_init__(self) -> None:
        if self.trust_tier_bonus is not None:
            for k, v in self.trust_tier_bonus.items():
                if not isinstance(k, str) or not k.strip():
                    raise ValueError("trust_tier_bonus keys must be non-empty strings")
                if not isinstance(v, (int, float)) or not float("-inf") < float(v) < float("inf"):
                    raise ValueError(f"trust_tier_bonus[{k!r}] must be a finite float")
        if not float("-inf") < float(self.pinned_bonus) < float("inf"):
            raise ValueError("pinned_bonus must be finite")
        if float(self.recency_halflife_days) < 0.0:
            raise ValueError("recency_halflife_days must be >= 0")
        if float(self.recency_max_bonus) < 0.0:
            raise ValueError("recency_max_bonus must be >= 0")
        if not float("-inf") < float(self.doc_version_step_bonus) < float("inf"):
            raise ValueError("doc_version_step_bonus must be finite")

    def resolved_trust_tier_bonus(self) -> dict[str, float]:
        base = {
            "default": 0.0,
            "trusted": 8.0,
            "high": 8.0,
            "low": -4.0,
            "untrusted": -4.0,
        }
        if self.trust_tier_bonus is None:
            return dict(base)
        merged = dict(base)
        merged.update({str(k).strip().lower(): float(v) for k, v in self.trust_tier_bonus.items()})
        return merged

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "trust_tier_bonus": (
                {k: float(v) for k, v in sorted(self.resolved_trust_tier_bonus().items())}
                if self.trust_tier_bonus is not None
                else None
            ),
            "pinned_bonus": float(self.pinned_bonus),
            "recency_halflife_days": float(self.recency_halflife_days),
            "recency_max_bonus": float(self.recency_max_bonus),
            "doc_version_step_bonus": float(self.doc_version_step_bonus),
        }


@dataclass(frozen=True, slots=True)
class KnowledgeConflictNormalization:
    """Deterministic grouping keys before knowledge-layer conflict mediation (Phase 2).

    Does **not** change ``CanonicalConstraint.assertion_id`` (semantic fingerprint); it only
    affects how contradictions are grouped and compared.
    """

    # Maps normalized lookup key (whitespace-collapsed, lowercased) -> canonical subject token.
    subject_synonyms: Mapping[str, str] | None = None
    # Same for optional object alignment on ``must_use`` / ``must_not_use`` rows.
    object_synonyms: Mapping[str, str] | None = None
    lowercase_subjects: bool = True

    def __post_init__(self) -> None:
        if self.subject_synonyms is not None:
            for k, v in self.subject_synonyms.items():
                if not isinstance(k, str) or not k.strip():
                    raise ValueError("subject_synonyms keys must be non-empty strings")
                if not isinstance(v, str) or not v.strip():
                    raise ValueError("subject_synonyms values must be non-empty strings")
        if self.object_synonyms is not None:
            for k, v in self.object_synonyms.items():
                if not isinstance(k, str) or not k.strip():
                    raise ValueError("object_synonyms keys must be non-empty strings")
                if not isinstance(v, str) or not v.strip():
                    raise ValueError("object_synonyms values must be non-empty strings")
        if not isinstance(self.lowercase_subjects, bool):
            raise ValueError("lowercase_subjects must be a bool")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "subject_synonyms": (
                {str(k).strip(): str(v).strip() for k, v in sorted(self.subject_synonyms.items())}
                if self.subject_synonyms is not None
                else None
            ),
            "object_synonyms": (
                {str(k).strip(): str(v).strip() for k, v in sorted(self.object_synonyms.items())}
                if self.object_synonyms is not None
                else None
            ),
            "lowercase_subjects": bool(self.lowercase_subjects),
        }


@dataclass(frozen=True, slots=True)
class CostRates:
    """Provider/tool billing rates used for deterministic cost accounting."""

    input_per_1k_tokens_usd: float = 0.0
    output_per_1k_tokens_usd: float = 0.0
    tool_call_usd: float = 0.0
    mcp_call_usd: float = 0.0
    currency: str = "USD"
    pricing_version: str = "static-v1"

    def __post_init__(self) -> None:
        if float(self.input_per_1k_tokens_usd) < 0:
            raise ValueError("cost_rates.input_per_1k_tokens_usd must be >= 0")
        if float(self.output_per_1k_tokens_usd) < 0:
            raise ValueError("cost_rates.output_per_1k_tokens_usd must be >= 0")
        if float(self.tool_call_usd) < 0:
            raise ValueError("cost_rates.tool_call_usd must be >= 0")
        if float(self.mcp_call_usd) < 0:
            raise ValueError("cost_rates.mcp_call_usd must be >= 0")
        require_non_empty(self.currency, name="cost_rates.currency")
        require_non_empty(self.pricing_version, name="cost_rates.pricing_version")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "input_per_1k_tokens_usd": float(self.input_per_1k_tokens_usd),
            "output_per_1k_tokens_usd": float(self.output_per_1k_tokens_usd),
            "tool_call_usd": float(self.tool_call_usd),
            "mcp_call_usd": float(self.mcp_call_usd),
            "currency": str(self.currency),
            "pricing_version": str(self.pricing_version),
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
    max_mcp_calls: int | None = None
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
        if self.max_mcp_calls is not None and int(self.max_mcp_calls) < 0:
            raise ValueError("max_mcp_calls must be >= 0 when set")
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
            "max_mcp_calls": int(self.max_mcp_calls) if self.max_mcp_calls is not None else None,
            "max_input_tokens": int(self.max_input_tokens) if self.max_input_tokens is not None else None,
            "max_total_tokens": int(self.max_total_tokens) if self.max_total_tokens is not None else None,
            "max_output_tokens": int(self.max_output_tokens) if self.max_output_tokens is not None else None,
            "max_wall_time_s": float(self.max_wall_time_s) if self.max_wall_time_s is not None else None,
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
    # Living/runtime policy: failed operational validity can be recompile-blocking
    # (`block`) or advisory-only (`advisory`).
    operational_validity_failed_trigger_severity: OperationalValidityFailedTriggerSeverity = "block"
    governance_assurance_mode: Literal["hybrid", "artifact_only"] = "hybrid"
    governance_verifier_enforcement: Literal["auto", "advisory", "blocking"] = "auto"
    governance_provider_allowlist: tuple[str, ...] = ()
    governance_max_errors_before_block: int = 1
    # First-class rates for deterministic cost accounting.
    cost_rates: CostRates = CostRates()
    # Optional OPA/Rego policy integration.
    opa_policy_path: str | None = None
    opa_decision_path: str = "data.akc.allow"
    # Knowledge layer: deterministic regex/heuristic extraction (default, CI-stable) vs
    # LLM-assisted JSON extraction (`llm` / `hybrid` require a backend on the compile loop).
    # `hybrid` currently uses the same LLM path as `llm` with deterministic fail-closed fallback.
    knowledge_extraction_mode: KnowledgeExtractionMode = "deterministic"
    # A2: propose soft CanonicalConstraints from retrieved chunks (MUST/SHALL, etc.) under a hard cap.
    # Default ``limited`` keeps extraction bounded (see ``doc_derived_max_assertions``); use ``off`` to disable.
    doc_derived_assertions_mode: DocDerivedAssertionsMode = "limited"
    doc_derived_max_assertions: int = 12
    doc_derived_patterns: DocDerivedPatternOptions = field(default_factory=DocDerivedPatternOptions)
    # B1: optional evidence doc metadata (trust tier, recency, pinned) merged into conflict scores.
    knowledge_evidence_weighting: KnowledgeEvidenceWeighting | None = None
    # Phase 2: optional synonym maps + casing for conflict grouping (does not alter assertion_id hashes).
    knowledge_conflict_normalization: KnowledgeConflictNormalization | None = None
    # Phase 2: optional deterministic embedding similarity to merge near-duplicate subjects (tenant-local).
    knowledge_embedding_clustering_enabled: bool = False
    knowledge_embedding_clustering_threshold: float = 0.92
    # B2: behavior when score+hard ties persist within a contradiction group.
    knowledge_unresolved_conflict_policy: KnowledgeUnresolvedConflictPolicy = "warn_and_continue"
    # A4: merge pre-indexed doc-derived assertions from `.akc/knowledge/assertions.sqlite`.
    stored_assertion_index_mode: StoredAssertionIndexMode = "off"
    stored_assertion_index_max_rows: int = 64
    # B3: merge `.akc/knowledge/decisions.json` after automated mediation.
    apply_operator_knowledge_decisions: bool = True
    # Compile prompts: ``auto`` uses reference-first intent slices when IntentStore + intent_ref apply;
    # ``full`` always embeds full objective/constraint/criterion summaries in prompts + retrieval.
    compile_prompt_intent_contract_policy: CompilePromptIntentContractPolicy = "auto"
    # IR spine: validate workflow runtime contracts + intent acceptance contracts on emitted IR.
    ir_operational_structure_policy: Literal["off", "warn", "error"] = "warn"
    # Optional override for graph checks (depends_on, knowledge hub payloads, deployable metadata).
    # When None, uses ir_operational_structure_policy.
    ir_graph_integrity_policy: Literal["off", "warn", "error"] | None = None
    # Cross-artifact consistency gates (session applies when validators are wired).
    artifact_consistency_policy: Literal["off", "warn", "error"] = "warn"
    # When set, validates ``deployment_intents`` rows against deployable ``referenced_ir_nodes``.
    # When None, inherits ``warn``/``error`` from ``ir_operational_structure_policy`` (off when IR policy is off).
    deployment_intents_ir_alignment_policy: Literal["off", "warn", "error"] | None = None
    # Runtime bundle artifact envelope (see docs/ir-schema.md). Use v1/v2 only for legacy tests.
    runtime_bundle_schema_version: int = 4
    # Embed full IR JSON in the bundle (larger artifact; use for air-gapped / debugging only).
    runtime_bundle_embed_system_ir: bool = False
    # When True, emitted v4+ bundles set ``reconcile_deploy_targets_from_ir_only`` so the reconciler
    # enumerates deployable resources from IR (same kinds as ``run_runtime_bundle_pass``), not from
    # ``deployment_intents`` membership (hashes still match IR nodes when rows are present).
    reconcile_deploy_targets_from_ir_only: bool = False
    # Working-tree apply after a passing compile candidate (fail-closed preflight); set
    # ``artifact_only`` to skip apply and emit patches as artifacts only.
    compile_realization_mode: CompileRealizationMode = "scoped_apply"
    # Absolute path to the repo/working tree allowed to receive patches (tenant/repo scope root).
    apply_scope_root: str | None = None
    metadata: Mapping[str, Any] | None = None
    # Shallow-merged into the controller accounting dict after initialization (tests, controlled bridges).
    # Keys such as ``operational_compile_bundle`` / ``operational_verifier_findings`` must already be tenant-scoped.
    accounting_overlay: Mapping[str, Any] | None = None
    # Optional compile-time MCP (live retrieval): same trust model as ingest-mcp; gated by policy + budgets.
    compile_mcp_enabled: bool = False
    compile_mcp_config_path: str | None = None
    compile_mcp_server: str | None = None
    compile_mcp_resource_uris: tuple[str, ...] = ()
    compile_mcp_session_timeout_s: float | None = 60.0
    compile_mcp_tools: tuple[CompileMcpToolSpec, ...] = ()
    compile_mcp_tool_stages: tuple[CompileMcpToolStage, ...] = ("generate", "repair")
    # Agent Skills (SKILL.md) injected into patch LLM system message (tenant-local paths only).
    compile_skills_mode: CompileSkillsMode = "default_only"
    compile_skill_allowlist: tuple[str, ...] = ()
    compile_skill_relative_roots: tuple[str, ...] = ()
    compile_skill_extra_roots: tuple[Path, ...] = ()
    # UTF-8 byte budgets: per-SKILL.md read from disk and total injected system preamble.
    compile_skill_max_total_bytes: int = 98_304
    compile_skill_max_file_bytes: int = 393_216

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
        if self.full_test_every_n_iterations is not None and int(self.full_test_every_n_iterations) <= 0:
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
        if self.operational_validity_failed_trigger_severity not in {"block", "advisory"}:
            raise ValueError("operational_validity_failed_trigger_severity must be one of: block, advisory")
        if self.governance_assurance_mode not in {"hybrid", "artifact_only"}:
            raise ValueError("governance_assurance_mode must be one of: hybrid, artifact_only")
        if self.governance_verifier_enforcement not in {"auto", "advisory", "blocking"}:
            raise ValueError("governance_verifier_enforcement must be one of: auto, advisory, blocking")
        if any(not isinstance(p, str) or not p.strip() for p in self.governance_provider_allowlist):
            raise ValueError("governance_provider_allowlist must contain non-empty provider names")
        if int(self.governance_max_errors_before_block) < 0:
            raise ValueError("governance_max_errors_before_block must be >= 0")
        if any(not isinstance(a, str) or not a.strip() for a in self.tool_allowlist):
            raise ValueError("tool_allowlist must contain non-empty action names")
        if self.opa_policy_path is not None and not str(self.opa_policy_path).strip():
            raise ValueError("opa_policy_path must be non-empty when set")
        if not isinstance(self.opa_decision_path, str) or not self.opa_decision_path.strip():
            raise ValueError("opa_decision_path must be non-empty")
        if self.knowledge_extraction_mode not in {"deterministic", "llm", "hybrid"}:
            raise ValueError("knowledge_extraction_mode must be one of: deterministic, llm, hybrid")
        if self.doc_derived_assertions_mode not in {"off", "limited"}:
            raise ValueError("doc_derived_assertions_mode must be one of: off, limited")
        if int(self.doc_derived_max_assertions) < 0:
            raise ValueError("doc_derived_max_assertions must be >= 0")
        if self.knowledge_unresolved_conflict_policy not in {"fail_closed", "warn_and_continue", "defer_to_intent"}:
            raise ValueError(
                "knowledge_unresolved_conflict_policy must be one of: fail_closed, warn_and_continue, defer_to_intent"
            )
        if self.knowledge_conflict_normalization is not None and not isinstance(
            self.knowledge_conflict_normalization, KnowledgeConflictNormalization
        ):
            raise ValueError("knowledge_conflict_normalization must be a KnowledgeConflictNormalization or None")
        if not isinstance(self.knowledge_embedding_clustering_enabled, bool):
            raise ValueError("knowledge_embedding_clustering_enabled must be a bool")
        t = float(self.knowledge_embedding_clustering_threshold)
        if not 0.0 <= t <= 1.0:
            raise ValueError("knowledge_embedding_clustering_threshold must be within [0.0, 1.0]")
        if self.stored_assertion_index_mode not in {"off", "merge"}:
            raise ValueError("stored_assertion_index_mode must be one of: off, merge")
        if int(self.stored_assertion_index_max_rows) < 0:
            raise ValueError("stored_assertion_index_max_rows must be >= 0")
        if not isinstance(self.apply_operator_knowledge_decisions, bool):
            raise ValueError("apply_operator_knowledge_decisions must be a bool")
        if self.compile_prompt_intent_contract_policy not in {"auto", "full", "reference_first"}:
            raise ValueError("compile_prompt_intent_contract_policy must be one of: auto, full, reference_first")
        if self.ir_operational_structure_policy not in {"off", "warn", "error"}:
            raise ValueError("ir_operational_structure_policy must be one of: off, warn, error")
        if self.ir_graph_integrity_policy is not None and self.ir_graph_integrity_policy not in {
            "off",
            "warn",
            "error",
        }:
            raise ValueError("ir_graph_integrity_policy must be one of: off, warn, error, or None")
        if self.artifact_consistency_policy not in {"off", "warn", "error"}:
            raise ValueError("artifact_consistency_policy must be one of: off, warn, error")
        if (
            self.deployment_intents_ir_alignment_policy is not None
            and self.deployment_intents_ir_alignment_policy not in {"off", "warn", "error"}
        ):
            raise ValueError("deployment_intents_ir_alignment_policy must be one of: off, warn, error, or None")
        if int(self.runtime_bundle_schema_version) not in (1, 2, 3, 4):
            raise ValueError("runtime_bundle_schema_version must be 1, 2, 3, or 4")
        if not isinstance(self.runtime_bundle_embed_system_ir, bool):
            raise ValueError("runtime_bundle_embed_system_ir must be a bool")
        if not isinstance(self.reconcile_deploy_targets_from_ir_only, bool):
            raise ValueError("reconcile_deploy_targets_from_ir_only must be a bool")
        if self.compile_realization_mode not in {"artifact_only", "scoped_apply"}:
            raise ValueError("compile_realization_mode must be one of: artifact_only, scoped_apply")
        if self.apply_scope_root is not None:
            s = str(self.apply_scope_root).strip()
            if not s:
                raise ValueError("apply_scope_root must be non-empty when set")
            if not Path(s).expanduser().is_absolute():
                raise ValueError("apply_scope_root must be an absolute path")
        if self.accounting_overlay is not None and not isinstance(self.accounting_overlay, Mapping):
            raise ValueError("accounting_overlay must be a mapping when set")
        if self.compile_mcp_enabled:
            if self.compile_mcp_config_path is None or not str(self.compile_mcp_config_path).strip():
                raise ValueError("compile_mcp_config_path must be set when compile_mcp_enabled")
            p = Path(str(self.compile_mcp_config_path).strip()).expanduser()
            if not p.is_absolute():
                raise ValueError("compile_mcp_config_path must be an absolute path")
        if self.compile_mcp_server is not None and not str(self.compile_mcp_server).strip():
            raise ValueError("compile_mcp_server must be non-empty when set")
        if any(not isinstance(u, str) or not u.strip() for u in self.compile_mcp_resource_uris):
            raise ValueError("compile_mcp_resource_uris must contain only non-empty strings")
        if self.compile_mcp_session_timeout_s is not None and float(self.compile_mcp_session_timeout_s) <= 0:
            raise ValueError("compile_mcp_session_timeout_s must be > 0 when set")
        for spec in self.compile_mcp_tools:
            if not isinstance(spec, CompileMcpToolSpec):
                raise ValueError("compile_mcp_tools must contain only CompileMcpToolSpec entries")
        for st in self.compile_mcp_tool_stages:
            if st not in ("generate", "repair"):
                raise ValueError("compile_mcp_tool_stages entries must be 'generate' or 'repair'")
        if self.compile_skills_mode not in {"off", "default_only", "explicit", "auto"}:
            raise ValueError("compile_skills_mode must be one of: off, default_only, explicit, auto")
        if int(self.compile_skill_max_total_bytes) <= 0:
            raise ValueError("compile_skill_max_total_bytes must be > 0")
        if int(self.compile_skill_max_file_bytes) <= 0:
            raise ValueError("compile_skill_max_file_bytes must be > 0")
        if any(not isinstance(a, str) or not a.strip() for a in self.compile_skill_allowlist):
            raise ValueError("compile_skill_allowlist must contain only non-empty strings")
        if any(not isinstance(r, str) or not r.strip() for r in self.compile_skill_relative_roots):
            raise ValueError("compile_skill_relative_roots must contain only non-empty strings")
        for p in self.compile_skill_extra_roots:
            if not isinstance(p, Path):
                raise ValueError("compile_skill_extra_roots must contain only pathlib.Path entries")
            exp = p.expanduser()
            if not str(exp).strip():
                raise ValueError("compile_skill_extra_roots must not contain empty paths")
            if not exp.is_absolute():
                raise ValueError("compile_skill_extra_roots entries must be absolute paths")

    def effective_deployment_intents_ir_alignment_policy(self) -> Literal["off", "warn", "error"]:
        """Policy for ``deployment_intents`` vs IR deployable nodes (runtime bundle projection check)."""

        if self.deployment_intents_ir_alignment_policy is not None:
            return self.deployment_intents_ir_alignment_policy
        if self.ir_operational_structure_policy in ("warn", "error"):
            return self.ir_operational_structure_policy
        return "off"

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

    def operational_validity_failed_trigger_blocks(self) -> bool:
        """Whether failed operational validity should emit blocking recompile triggers."""

        return self.operational_validity_failed_trigger_severity == "block"

    def with_governance_profile(
        self,
        *,
        assurance_mode: str | None,
        verifier_enforcement: str | None,
        provider_allowlist: tuple[str, ...] | None,
        max_errors_before_block: int | None,
        rollout_stage: str | None,
    ) -> ControllerConfig:
        """Apply tenant governance profile values with fail-closed normalization."""

        am = str(assurance_mode or self.governance_assurance_mode).strip().lower()
        if am not in {"hybrid", "artifact_only"}:
            am = self.governance_assurance_mode
        ve = str(verifier_enforcement or self.governance_verifier_enforcement).strip().lower()
        if ve not in {"auto", "advisory", "blocking"}:
            ve = self.governance_verifier_enforcement
        allow = (
            tuple(sorted({str(x).strip() for x in provider_allowlist if str(x).strip()}))
            if provider_allowlist is not None
            else self.governance_provider_allowlist
        )
        max_err = (
            int(max_errors_before_block)
            if max_errors_before_block is not None
            else self.governance_max_errors_before_block
        )
        if max_err < 0:
            max_err = 0
        sev = self.operational_validity_failed_trigger_severity
        stage = str(rollout_stage or "").strip().lower()
        if ve == "advisory" or (ve == "auto" and stage == "observe"):
            sev = "advisory"
        elif ve == "blocking" or (ve == "auto" and stage == "enforce"):
            sev = "block"
        return ControllerConfig(
            tiers=self.tiers,
            stage_tiers=self.stage_tiers,
            budget=self.budget,
            generate_tests_by_default=self.generate_tests_by_default,
            require_tests_for_non_test_changes=self.require_tests_for_non_test_changes,
            test_command=self.test_command,
            test_timeout_s=self.test_timeout_s,
            test_mode=self.test_mode,
            full_test_every_n_iterations=self.full_test_every_n_iterations,
            verifier_enabled=self.verifier_enabled,
            verifier_strict=self.verifier_strict,
            policy_mode=self.policy_mode,
            tool_allowlist=self.tool_allowlist,
            operational_validity_failed_trigger_severity=sev,
            governance_assurance_mode=cast(Literal["hybrid", "artifact_only"], am),
            governance_verifier_enforcement=cast(Literal["auto", "advisory", "blocking"], ve),
            governance_provider_allowlist=allow,
            governance_max_errors_before_block=max_err,
            cost_rates=self.cost_rates,
            opa_policy_path=self.opa_policy_path,
            opa_decision_path=self.opa_decision_path,
            knowledge_extraction_mode=self.knowledge_extraction_mode,
            doc_derived_assertions_mode=self.doc_derived_assertions_mode,
            doc_derived_max_assertions=self.doc_derived_max_assertions,
            doc_derived_patterns=self.doc_derived_patterns,
            knowledge_evidence_weighting=self.knowledge_evidence_weighting,
            knowledge_conflict_normalization=self.knowledge_conflict_normalization,
            knowledge_embedding_clustering_enabled=self.knowledge_embedding_clustering_enabled,
            knowledge_embedding_clustering_threshold=self.knowledge_embedding_clustering_threshold,
            knowledge_unresolved_conflict_policy=self.knowledge_unresolved_conflict_policy,
            stored_assertion_index_mode=self.stored_assertion_index_mode,
            stored_assertion_index_max_rows=self.stored_assertion_index_max_rows,
            apply_operator_knowledge_decisions=self.apply_operator_knowledge_decisions,
            compile_prompt_intent_contract_policy=self.compile_prompt_intent_contract_policy,
            ir_operational_structure_policy=self.ir_operational_structure_policy,
            ir_graph_integrity_policy=self.ir_graph_integrity_policy,
            artifact_consistency_policy=self.artifact_consistency_policy,
            deployment_intents_ir_alignment_policy=self.deployment_intents_ir_alignment_policy,
            runtime_bundle_schema_version=self.runtime_bundle_schema_version,
            runtime_bundle_embed_system_ir=self.runtime_bundle_embed_system_ir,
            reconcile_deploy_targets_from_ir_only=self.reconcile_deploy_targets_from_ir_only,
            compile_realization_mode=self.compile_realization_mode,
            apply_scope_root=self.apply_scope_root,
            metadata=self.metadata,
            accounting_overlay=self.accounting_overlay,
            compile_mcp_enabled=self.compile_mcp_enabled,
            compile_mcp_config_path=self.compile_mcp_config_path,
            compile_mcp_server=self.compile_mcp_server,
            compile_mcp_resource_uris=self.compile_mcp_resource_uris,
            compile_mcp_session_timeout_s=self.compile_mcp_session_timeout_s,
            compile_mcp_tools=self.compile_mcp_tools,
            compile_mcp_tool_stages=self.compile_mcp_tool_stages,
            compile_skills_mode=self.compile_skills_mode,
            compile_skill_allowlist=self.compile_skill_allowlist,
            compile_skill_relative_roots=self.compile_skill_relative_roots,
            compile_skill_extra_roots=self.compile_skill_extra_roots,
            compile_skill_max_total_bytes=self.compile_skill_max_total_bytes,
            compile_skill_max_file_bytes=self.compile_skill_max_file_bytes,
        )
