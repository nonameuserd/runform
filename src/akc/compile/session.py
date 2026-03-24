"""Compile boundary session (Phase 2).

This module intentionally provides a small "boundary object" that binds a
tenant+repo scope with the Memory layer stores, so the eventual Phase 3
controller can call a cohesive API without redesigning store interfaces.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.artifacts.contracts import ARTIFACT_SCHEMA_VERSION, apply_schema_envelope
from akc.artifacts.schemas import RUNTIME_BUNDLE_SCHEMA_VERSION
from akc.artifacts.validate import validate_artifact_json
from akc.compile.artifact_consistency import (
    collect_cross_artifact_consistency_issues,
    validate_deployment_intents_align_with_ir,
)
from akc.compile.artifact_passes import (
    run_agent_coordination_pass,
    run_delivery_plan_pass,
    run_deployment_config_pass,
    run_orchestration_spec_pass,
    run_runtime_bundle_pass,
    run_system_design_pass,
)
from akc.compile.controller import ControllerResult, run_compile_loop
from akc.compile.controller_config import ControllerConfig
from akc.compile.controller_policy_runtime import COMPILE_PATCH_APPLY_ACTION
from akc.compile.delivery_projection import parse_json_artifact_text, render_delivery_summary_markdown
from akc.compile.executors import SubprocessExecutor
from akc.compile.interfaces import Executor, Index, LLMBackend, TenantRepoScope
from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.ir_operational_validate import validate_ir_graph_integrity, validate_ir_operational_contracts
from akc.compile.patch_emitter import patch_sha256_hex
from akc.compile.planner import advance_plan, create_or_resume_plan
from akc.compile.retriever import retrieve_context
from akc.control.cost_index import CostIndex, RunCostRecord
from akc.control.operations_index import try_upsert_operations_index_from_manifest
from akc.control.otel_export import build_compile_trace_export_text
from akc.control.policy_provenance import merge_policy_provenance_for_compile_control_plane
from akc.control.tracing import TraceSpan, new_span_id, new_trace_id, now_unix_nano
from akc.intent import (
    IntentSpecV1,
    compile_intent_spec,
    compute_intent_fingerprint,
    intent_acceptance_slice_fingerprint,
    stable_intent_sha256,
)
from akc.intent.compiler import IntentCompilerError
from akc.intent.store import IntentStore, JsonFileIntentStore
from akc.ir import IRDocument, diff_ir
from akc.knowledge.models import KnowledgeSnapshot
from akc.knowledge.persistence import (
    KNOWLEDGE_MEDIATION_RELPATH,
    KNOWLEDGE_SNAPSHOT_FINGERPRINT_RELPATH,
    KNOWLEDGE_SNAPSHOT_RELPATH,
    write_knowledge_snapshot_artifacts,
)
from akc.memory.facade import Memory, MemoryBackend, build_memory
from akc.memory.models import (
    JSONValue,
    PlanState,
    json_value_as_float,
    json_value_as_int,
    normalize_repo_id,
    require_non_empty,
)
from akc.memory.why_graph import WhyGraphStore
from akc.outputs.emitters import Emitter, JsonManifestEmitter
from akc.outputs.models import OutputArtifact, OutputBundle
from akc.pass_registry import CONTROLLER_LOOP_PASS_ORDER, assert_expected_artifact_pass_order
from akc.promotion import (
    canonical_sha256,
    latest_allow_decision_for_action,
    latest_policy_allow_decision,
    normalize_promotion_mode,
    validate_promotion_transition,
)
from akc.run import (
    ArtifactPointer,
    PassRecord,
    ReplayMode,
    RetrievalSnapshot,
    RunManifest,
    build_recompile_triggers_payload,
    build_replay_decisions_payload,
    decide_replay_for_pass,
)
from akc.run.intent_replay_mandates import mandatory_partial_replay_passes_for_success_criteria
from akc.run.loader import find_latest_run_manifest, load_run_manifest
from akc.run.time_compression import derive_time_compression_metrics
from akc.utils.fingerprint import stable_json_fingerprint

logger = logging.getLogger(__name__)


def _compile_apply_attestation_from_parts(
    *,
    policy_trace: list[dict[str, Any]],
    csa_raw: dict[str, Any] | None,
    patch_fingerprint_sha256: str,
    config_realization_mode: str,
    best_touched_paths: tuple[str, ...],
) -> dict[str, Any]:
    """Promotion packet + control-plane attestation for compile scoped apply."""

    apply_dec = latest_allow_decision_for_action(policy_trace, action=COMPILE_PATCH_APPLY_ACTION)
    csa = csa_raw if isinstance(csa_raw, dict) else {}
    mode = str(csa.get("compile_realization_mode") or config_realization_mode)
    if mode not in ("artifact_only", "scoped_apply"):
        mode = (
            str(config_realization_mode)
            if str(config_realization_mode)
            in (
                "artifact_only",
                "scoped_apply",
            )
            else "scoped_apply"
        )
    touched: list[str] = []
    files = csa.get("files")
    if isinstance(files, list):
        for row in files:
            if isinstance(row, dict) and row.get("path"):
                touched.append(str(row["path"]))
    if not touched:
        touched = [str(x) for x in best_touched_paths]
    patch_fp = str(csa.get("patch_sha256") or patch_fingerprint_sha256)
    if len(patch_fp) != 64 or any(c not in "0123456789abcdef" for c in patch_fp.lower()):
        patch_fp = patch_fingerprint_sha256
    apply_tok = str(apply_dec.get("token_id", "")) if bool(apply_dec.get("allowed")) else ""
    return {
        "compile_realization_mode": mode,
        "applied": bool(csa.get("applied")),
        "apply_decision_token_id": apply_tok,
        "policy_allow_decision": apply_dec,
        "patch_fingerprint_sha256": patch_fp,
        "scope_root": csa.get("scope_root"),
        "touched_paths": sorted(set(touched)),
    }


@dataclass(frozen=True, slots=True)
class CompileSession:
    """Tenant+repo scoped compile boundary wrapper."""

    tenant_id: str
    repo_id: str
    memory: Memory
    index: Index | None = None

    @classmethod
    def from_backend(
        cls,
        *,
        tenant_id: str,
        repo_id: str,
        backend: MemoryBackend,
        sqlite_path: str | None = None,
        index: Index | None = None,
    ) -> CompileSession:
        """Create a session by selecting a memory backend."""

        mem = build_memory(backend=backend, sqlite_path=sqlite_path)
        return cls(tenant_id=tenant_id, repo_id=repo_id, memory=mem, index=index)

    @classmethod
    def from_memory(
        cls,
        *,
        tenant_id: str,
        repo_id: str,
        index: Index | None = None,
    ) -> CompileSession:
        """Create a session with in-memory stores (fast, test-friendly)."""

        return cls.from_backend(
            tenant_id=tenant_id,
            repo_id=repo_id,
            backend="memory",
            index=index,
        )

    @classmethod
    def from_sqlite(
        cls,
        *,
        tenant_id: str,
        repo_id: str,
        sqlite_path: str,
        index: Index | None = None,
    ) -> CompileSession:
        """Create a session backed by a single SQLite file."""

        return cls.from_backend(
            tenant_id=tenant_id,
            repo_id=repo_id,
            backend="sqlite",
            sqlite_path=sqlite_path,
            index=index,
        )

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        # Normalize once for stable downstream keying.
        object.__setattr__(self, "repo_id", normalize_repo_id(self.repo_id))

    @property
    def why_graph(self) -> WhyGraphStore:
        return self.memory.why_graph

    def plan(self, *, goal: str) -> PlanState:
        """Create or resume the active plan for this tenant+repo."""

        return create_or_resume_plan(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            goal=goal,
            plan_store=self.memory.plan_state,
        )

    def advance(self, *, plan_id: str, feedback: dict[str, Any] | None = None) -> PlanState:
        """Persist feedback and advance next_step_id deterministically (Phase 2 policy)."""

        return advance_plan(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            plan_id=plan_id,
            plan_store=self.memory.plan_state,
            feedback=feedback,
        )

    def retrieve(
        self,
        *,
        plan: PlanState,
        limit: int = 20,
        ir_document: IRDocument | None = None,
        knowledge_snapshot_for_query: KnowledgeSnapshot | None = None,
        knowledge_query_budget_chars: int = 1200,
    ) -> dict[str, Any]:
        """Retrieve context from code memory (+ optionally why-graph) for this session."""

        return retrieve_context(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            plan=plan,
            code_memory=self.memory.code_memory,
            why_graph=self.memory.why_graph,
            index=self.index,
            limit=limit,
            ir_document=ir_document,
            knowledge_snapshot_for_query=knowledge_snapshot_for_query,
            knowledge_query_budget_chars=knowledge_query_budget_chars,
        )

    def run(
        self,
        *,
        goal: str,
        llm: LLMBackend,
        executor: Executor | None = None,
        config: ControllerConfig,
        outputs_root: str | Path | None = None,
        schema_version: int = ARTIFACT_SCHEMA_VERSION,
        emitter: Emitter | None = None,
        intent_file: str | Path | None = None,
        replay_mode: ReplayMode = "live",
        replay_manifest_path: str | Path | None = None,
        partial_replay_passes: tuple[str, ...] | None = None,
        developer_role_profile: str = "classic",
        developer_profile_decisions: Mapping[str, JSONValue] | None = None,
    ) -> ControllerResult:
        """Run the Phase 3 compile loop for this tenant+repo scope."""
        compile_started_at_ms = int(time.time() * 1000)
        loaded_replay_manifest: RunManifest | None = None
        effective_replay_manifest: RunManifest | None = None
        if replay_mode != "live":
            source_path = (
                Path(replay_manifest_path).expanduser()
                if replay_manifest_path is not None
                else (
                    find_latest_run_manifest(
                        outputs_root=outputs_root,
                        tenant_id=self.tenant_id,
                        repo_id=self.repo_id,
                    )
                    if outputs_root is not None
                    else None
                )
            )
            if source_path is not None:
                loaded_replay_manifest = load_run_manifest(
                    path=source_path,
                    expected_tenant_id=self.tenant_id,
                    expected_repo_id=self.repo_id,
                )
            if loaded_replay_manifest is not None:
                # Enforce caller-selected replay mode even when loading a manifest
                # produced under a different mode.
                effective_replay_manifest = RunManifest(
                    run_id=loaded_replay_manifest.run_id,
                    tenant_id=loaded_replay_manifest.tenant_id,
                    repo_id=loaded_replay_manifest.repo_id,
                    ir_sha256=loaded_replay_manifest.ir_sha256,
                    intent_semantic_fingerprint=loaded_replay_manifest.intent_semantic_fingerprint,
                    intent_goal_text_fingerprint=loaded_replay_manifest.intent_goal_text_fingerprint,
                    stable_intent_sha256=loaded_replay_manifest.stable_intent_sha256,
                    knowledge_semantic_fingerprint=loaded_replay_manifest.knowledge_semantic_fingerprint,
                    knowledge_provenance_fingerprint=loaded_replay_manifest.knowledge_provenance_fingerprint,
                    replay_mode=replay_mode,
                    retrieval_snapshots=loaded_replay_manifest.retrieval_snapshots,
                    passes=loaded_replay_manifest.passes,
                    model=loaded_replay_manifest.model,
                    model_params=loaded_replay_manifest.model_params,
                    tool_params=loaded_replay_manifest.tool_params,
                    partial_replay_passes=loaded_replay_manifest.partial_replay_passes,
                    llm_vcr=loaded_replay_manifest.llm_vcr,
                    budgets=loaded_replay_manifest.budgets,
                    output_hashes=loaded_replay_manifest.output_hashes,
                    trace_spans=loaded_replay_manifest.trace_spans,
                    control_plane=loaded_replay_manifest.control_plane,
                    cost_attribution=loaded_replay_manifest.cost_attribution,
                    manifest_version=loaded_replay_manifest.manifest_version,
                )
            if replay_mode == "partial_replay" and partial_replay_passes is not None:
                cleaned = tuple(str(p).strip() for p in partial_replay_passes if str(p).strip())
                if effective_replay_manifest is not None:
                    effective_replay_manifest = RunManifest(
                        run_id=effective_replay_manifest.run_id,
                        tenant_id=effective_replay_manifest.tenant_id,
                        repo_id=effective_replay_manifest.repo_id,
                        ir_sha256=effective_replay_manifest.ir_sha256,
                        intent_semantic_fingerprint=effective_replay_manifest.intent_semantic_fingerprint,
                        intent_goal_text_fingerprint=effective_replay_manifest.intent_goal_text_fingerprint,
                        stable_intent_sha256=effective_replay_manifest.stable_intent_sha256,
                        knowledge_semantic_fingerprint=effective_replay_manifest.knowledge_semantic_fingerprint,
                        knowledge_provenance_fingerprint=effective_replay_manifest.knowledge_provenance_fingerprint,
                        replay_mode=replay_mode,
                        retrieval_snapshots=effective_replay_manifest.retrieval_snapshots,
                        passes=effective_replay_manifest.passes,
                        model=effective_replay_manifest.model,
                        model_params=effective_replay_manifest.model_params,
                        tool_params=effective_replay_manifest.tool_params,
                        partial_replay_passes=cleaned,
                        llm_vcr=effective_replay_manifest.llm_vcr,
                        budgets=effective_replay_manifest.budgets,
                        output_hashes=effective_replay_manifest.output_hashes,
                        trace_spans=effective_replay_manifest.trace_spans,
                        control_plane=effective_replay_manifest.control_plane,
                        cost_attribution=effective_replay_manifest.cost_attribution,
                        manifest_version=effective_replay_manifest.manifest_version,
                    )
                else:
                    effective_replay_manifest = RunManifest(
                        run_id="partial-replay-no-manifest",
                        tenant_id=self.tenant_id,
                        repo_id=self.repo_id,
                        ir_sha256="0" * 64,
                        replay_mode="partial_replay",
                        partial_replay_passes=cleaned,
                    )

        # Phase 2: resolve the intent contract used for planning + acceptance.
        #
        # Important for correctness + test stability:
        # - In goal-only compatibility (live + no intent-file), keep controller-side
        #   compilation so controller-level monkeypatching/mocking remains effective.
        # - For manifest replay (and when we load an intent artifact), pass the resolved
        #   intent spec into the controller so acceptance criteria + bounds match.
        intent_spec: IntentSpecV1 | None = None  # used for output emission + IR nodes
        controller_intent_spec: IntentSpecV1 | None = None  # used for controller acceptance
        derived_goal: str = goal
        profile_mode = str(developer_role_profile or "classic").strip().lower()
        profile_intent_bootstrap_enabled = bool(
            dict(config.metadata or {}).get("developer_profile_intent_bootstrap_from_store", False)
        )

        if (
            profile_mode == "emerging"
            and profile_intent_bootstrap_enabled
            and intent_file is None
            and replay_mode == "live"
            and outputs_root is not None
        ):
            try:
                bootstrap_store = JsonFileIntentStore(base_dir=Path(outputs_root).expanduser())
                active_id = bootstrap_store.get_active_intent_id(tenant_id=self.tenant_id, repo_id=self.repo_id)
                if active_id is not None:
                    loaded = bootstrap_store.load_intent(
                        tenant_id=self.tenant_id,
                        repo_id=self.repo_id,
                        intent_id=active_id,
                    )
                    if loaded is not None:
                        intent_spec = loaded.normalized()
                        controller_intent_spec = intent_spec
                        derived_goal = (
                            intent_spec.goal_statement
                            if intent_spec.goal_statement is not None
                            else (intent_spec.objectives[0].statement if intent_spec.objectives else derived_goal)
                        )
            except Exception:
                # Keep compile fail-open for bootstrap convenience; normal intent compilation path still runs.
                intent_spec = None
                controller_intent_spec = None

        if intent_file is not None:
            # Validate + derive a planning goal from the explicit intent file, but let
            # the controller compile from the file (so it can be overridden/mocked).
            intent_spec = compile_intent_spec(
                tenant_id=self.tenant_id,
                repo_id=self.repo_id,
                intent_file=intent_file,
                controller_budget=config.budget,
            )
            derived_goal = (
                intent_spec.goal_statement
                if intent_spec.goal_statement is not None
                else (intent_spec.objectives[0].statement if intent_spec.objectives else derived_goal)
            )
        elif replay_mode != "live" and loaded_replay_manifest is not None and outputs_root is not None:
            intent_path: Path | None = None
            try:
                scope_root = Path(outputs_root).expanduser() / self.tenant_id / self.repo_id
                intent_path = scope_root / ".akc" / "intent" / f"{loaded_replay_manifest.run_id}.json"
                raw = json.loads(intent_path.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    loaded_intent = IntentSpecV1.from_json_obj(raw)
                    if (
                        loaded_intent.tenant_id == self.tenant_id
                        and normalize_repo_id(loaded_intent.repo_id) == self.repo_id
                    ):
                        intent_spec = loaded_intent.normalized()
                        controller_intent_spec = intent_spec
                        derived_goal = (
                            intent_spec.goal_statement
                            if intent_spec.goal_statement is not None
                            else (intent_spec.objectives[0].statement if intent_spec.objectives else derived_goal)
                        )
            except Exception as exc:
                # Fail-closed for any replay that advertises intent fingerprints.
                # Otherwise, older manifests may not have the intent artifact.
                should_fail_closed = bool(
                    loaded_replay_manifest.intent_semantic_fingerprint
                    or loaded_replay_manifest.intent_goal_text_fingerprint
                )
                if should_fail_closed:
                    intent_path_str = str(intent_path) if intent_path is not None else "<unknown>"
                    raise IntentCompilerError(
                        f"intent artifact missing/corrupt for intent-backed replay (intent_path={intent_path_str})"
                    ) from exc
                # Backward compatibility: if intent artifact is missing/corrupt,
                # fall back to goal-only compilation.
                intent_spec = None

        # If we didn't resolve intent spec for planning (goal-only compatibility),
        # keep derived_goal as provided by the caller.

        # Replay should be self-contained: don't resume a potentially-completed
        # plan from a prior live run, since the controller will early-exit when
        # `next_step_id` is None (skipping intent acceptance gates).
        if replay_mode != "live":
            plan = self.memory.plan_state.create_plan(
                tenant_id=self.tenant_id,
                repo_id=self.repo_id,
                goal=derived_goal,
                initial_steps=["Implement goal"],
            )
        else:
            plan = self.plan(goal=derived_goal)

        # Ensure the plan exists and is active before running.
        self.memory.plan_state.set_active_plan(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            plan_id=plan.id,
        )

        intent_store_for_controller: IntentStore | None = None
        if outputs_root is not None:
            intent_store_for_controller = JsonFileIntentStore(base_dir=Path(outputs_root).expanduser())

        knowledge_artifact_root = (
            Path(outputs_root).expanduser() / self.tenant_id / self.repo_id if outputs_root is not None else None
        )
        result = run_compile_loop(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            goal=derived_goal,
            plan_store=self.memory.plan_state,
            code_memory=self.memory.code_memory,
            why_graph=self.memory.why_graph,
            index=self.index,
            llm=llm,
            executor=executor or SubprocessExecutor(),
            config=config,
            replay_mode=replay_mode,
            replay_manifest=effective_replay_manifest,
            intent_spec=controller_intent_spec,
            intent_file=intent_file,
            intent_store=intent_store_for_controller,
            knowledge_artifact_root=knowledge_artifact_root,
        )

        # Phase 4 Outputs integration (minimal): on success, emit a scoped manifest
        # containing the best patch candidate (and optional test output when available).
        #
        # This keeps the controller focused on the ARCS loop and makes emission opt-in
        # by requiring `outputs_root`.
        if outputs_root is not None:
            # Phase 6: emit intent artifacts + fingerprints for replay/audit.
            if intent_spec is None:
                intent_spec = compile_intent_spec(
                    tenant_id=self.tenant_id,
                    repo_id=self.repo_id,
                    goal_statement=derived_goal,
                    controller_budget=config.budget,
                )
            intent_fingerprint = compute_intent_fingerprint(intent=intent_spec)
            stable_intent_hash = stable_intent_sha256(intent=intent_spec.normalized())
            # Persist intent under the IntentStore namespace so future replay can
            # recover the active intent without relying on run_id-named artifacts.
            intent_store = JsonFileIntentStore(base_dir=Path(outputs_root).expanduser())
            intent_store.create_intent(intent=intent_spec)

            scope = TenantRepoScope(tenant_id=result.plan.tenant_id, repo_id=result.plan.repo_id)
            step_id = result.plan.last_feedback.get("step_id") if isinstance(result.plan.last_feedback, dict) else None
            step_id_s = str(step_id) if step_id is not None else "unknown_step"
            step = next((s for s in result.plan.steps if s.id == step_id_s), None)
            step_outputs = dict(step.outputs or {}) if step is not None else {}
            proj = result.accounting.get("policy_operating_bounds_projection")
            effective_operating_bounds: dict[str, Any] | None = None
            if isinstance(proj, dict):
                eff = proj.get("effective")
                if isinstance(eff, dict):
                    effective_operating_bounds = eff

            intent_node_properties: dict[str, Any] = {
                "intent_id": intent_spec.intent_id,
                "spec_version": int(intent_spec.spec_version),
                "goal_statement": intent_spec.goal_statement,
                "operating_bounds": effective_operating_bounds
                if effective_operating_bounds is not None
                else (intent_spec.operating_bounds.to_json_obj() if intent_spec.operating_bounds is not None else None),
                "intent_semantic_fingerprint": intent_fingerprint.semantic,
                "intent_goal_text_fingerprint": intent_fingerprint.goal_text,
            }
            ir_doc = self._ir_from_plan(
                plan=result.plan,
                intent_node_properties=intent_node_properties,
                intent_store=intent_store_for_controller,
                controller_intent_spec=intent_spec,
            )
            op_issues = validate_ir_operational_contracts(ir_doc)
            graph_issues = validate_ir_graph_integrity(ir_doc)
            op_pol = config.ir_operational_structure_policy
            graph_pol = config.ir_graph_integrity_policy or config.ir_operational_structure_policy

            def _apply_ir_policy(*, issues: tuple[str, ...], policy: str, label: str) -> None:
                if not issues:
                    return
                if policy == "error":
                    raise ValueError(f"{label} validation failed: " + "; ".join(issues))
                if policy == "warn":
                    logger.warning("%s: %s", label, "; ".join(issues))

            _apply_ir_policy(issues=op_issues, policy=op_pol, label="IR operational structure")
            _apply_ir_policy(issues=graph_issues, policy=graph_pol, label="IR graph integrity")

            retrieval_snapshots = self._build_retrieval_snapshots(step_outputs=step_outputs)
            passes = self._build_pass_records(
                result=result,
                step_outputs=step_outputs,
            )
            artifact_pass_records: list[PassRecord] = []
            artifact_pass_artifacts: list[OutputArtifact] = []
            artifact_pass_hashes: dict[str, str] = {}
            artifact_bundle_md: dict[str, JSONValue] = {}
            artifact_trace_spans: list[dict[str, JSONValue]] = []
            if bool(getattr(result, "compile_succeeded", False)):
                (
                    artifact_pass_records,
                    artifact_pass_artifacts,
                    artifact_pass_hashes,
                    artifact_bundle_md,
                    artifact_trace_spans,
                ) = self._execute_artifact_passes(
                    run_id=result.plan.id,
                    ir_doc=ir_doc,
                    intent_spec=intent_spec,
                    step_outputs=step_outputs,
                    outputs_root=outputs_root,
                    replay_manifest=effective_replay_manifest,
                    current_intent_semantic_fingerprint=intent_fingerprint.semantic,
                    current_stable_intent_sha256=stable_intent_hash,
                    controller_accounting=result.accounting,
                    runtime_bundle_schema_version=int(config.runtime_bundle_schema_version),
                    runtime_bundle_embed_system_ir=bool(config.runtime_bundle_embed_system_ir),
                    artifact_consistency_policy=config.artifact_consistency_policy,
                    deployment_intents_ir_alignment_policy=config.effective_deployment_intents_ir_alignment_policy(),
                    reconcile_deploy_targets_from_ir_only=bool(config.reconcile_deploy_targets_from_ir_only),
                )
                passes.extend(artifact_pass_records)
            combined_trace_spans = [
                dict(x) for x in result.accounting.get("trace_spans", []) if isinstance(x, dict)
            ] + [dict(x) for x in artifact_trace_spans if isinstance(x, dict)]
            output_hashes = self._build_output_hashes(
                result=result,
                plan_id=result.plan.id,
                step_id=step_id_s,
                ir_doc=ir_doc,
            )
            output_hashes.update(artifact_pass_hashes)
            compile_md = dict(config.metadata or {})
            promotion_mode = normalize_promotion_mode(str(compile_md.get("promotion_mode") or "artifact_only"))
            if promotion_mode is None:
                promotion_mode = "artifact_only"
            if not validate_promotion_transition(src="artifact_only", dst=promotion_mode):
                raise ValueError("invalid promotion state transition from artifact_only to requested promotion_mode")
            promotion_packet_obj: dict[str, Any] | None = None
            promotion_packet_path = f".akc/promotion/{result.plan.id}_{step_id_s}.packet.json"
            if bool(getattr(result, "compile_succeeded", False)) and result.best_candidate is not None:
                policy_trace = [dict(x) for x in result.accounting.get("policy_decisions", []) if isinstance(x, dict)]
                csa_for_promotion = result.accounting.get("compile_scoped_apply")
                required_tests: list[dict[str, JSONValue]] = []
                for key in ("last_tests_smoke", "last_tests_full"):
                    raw = step_outputs.get(key)
                    if not isinstance(raw, dict):
                        continue
                    exit_code = raw.get("exit_code")
                    passed = (int(exit_code) == 0) if isinstance(exit_code, int) else None
                    required_tests.append(
                        {
                            "stage": str(raw.get("stage", "")),
                            "command": [str(x) for x in (raw.get("command") or [])]
                            if isinstance(raw.get("command"), list)
                            else [],
                            "exit_code": int(exit_code) if isinstance(exit_code, int) else None,
                            "passed": passed,
                        }
                    )
                verifier_result = step_outputs.get("last_verification")
                runtime_bundle_md = artifact_bundle_md.get("runtime_bundle")
                apply_target_metadata = (
                    dict(runtime_bundle_md) if isinstance(runtime_bundle_md, dict) else {"runtime_bundle_path": None}
                )
                patch_text_for_hash = str(result.best_candidate.llm_text)
                patch_hash = patch_sha256_hex(patch_text=patch_text_for_hash)
                compile_apply_attestation = _compile_apply_attestation_from_parts(
                    policy_trace=policy_trace,
                    csa_raw=csa_for_promotion if isinstance(csa_for_promotion, dict) else None,
                    patch_fingerprint_sha256=patch_hash,
                    config_realization_mode=str(config.compile_realization_mode),
                    best_touched_paths=tuple(result.best_candidate.touched_paths),
                )
                promotion_packet_obj = {
                    "packet_version": 1,
                    "run_ref": {
                        "tenant_id": self.tenant_id,
                        "repo_id": self.repo_id,
                        "run_id": result.plan.id,
                        "step_id": step_id_s,
                    },
                    "intent_ref": {
                        "intent_id": intent_spec.intent_id,
                        "stable_intent_sha256": stable_intent_hash,
                        "semantic_fingerprint": intent_fingerprint.semantic,
                        "goal_text_fingerprint": intent_fingerprint.goal_text,
                    },
                    "promotion_mode": promotion_mode,
                    "promotion_state": promotion_mode,
                    "patch_hash_sha256": patch_hash,
                    "touched_paths": sorted({str(x) for x in result.best_candidate.touched_paths}),
                    "required_tests": required_tests,
                    "verifier_result": dict(verifier_result) if isinstance(verifier_result, dict) else None,
                    "policy_decision_trace": policy_trace,
                    "policy_allow_decision": latest_policy_allow_decision(policy_trace),
                    "compile_apply_attestation": compile_apply_attestation,
                    "apply_target_metadata": apply_target_metadata,
                    "issued_at_ms": int(time.time() * 1000),
                }
                apply_schema_envelope(obj=promotion_packet_obj, kind="promotion_packet", version=int(schema_version))
                promotion_packet_obj["packet_signature_sha256"] = canonical_sha256(
                    {k: v for k, v in promotion_packet_obj.items() if k != "packet_signature_sha256"}
                )
                validate_artifact_json(
                    obj=promotion_packet_obj,
                    kind="promotion_packet",
                    version=1,
                )
                output_hashes[promotion_packet_path] = canonical_sha256(promotion_packet_obj)
            knowledge_snapshot_pointer: ArtifactPointer | None = None
            knowledge_mediation_pointer: ArtifactPointer | None = None
            persisted_knowledge_artifacts: list[OutputArtifact] = []
            ks_raw = step_outputs.get("knowledge_snapshot")
            if outputs_root is not None and isinstance(ks_raw, dict):
                try:
                    ks_obj = KnowledgeSnapshot.from_json_obj(ks_raw)
                    scope_root_p = Path(outputs_root).expanduser() / self.tenant_id / self.repo_id
                    intent_ids: frozenset[str] | None = None
                    raw_intent = step_outputs.get("knowledge_intent_assertion_ids")
                    if isinstance(raw_intent, list):
                        intent_ids = frozenset(
                            str(x).strip() for x in raw_intent if isinstance(x, str) and str(x).strip()
                        )
                    snap_sha, fp_sha = write_knowledge_snapshot_artifacts(
                        scope_root_p,
                        tenant_id=self.tenant_id,
                        repo_id=self.repo_id,
                        snapshot=ks_obj,
                        run_id=result.plan.id,
                        intent_assertion_ids=intent_ids,
                    )
                    output_hashes[KNOWLEDGE_SNAPSHOT_RELPATH] = snap_sha
                    output_hashes[KNOWLEDGE_SNAPSHOT_FINGERPRINT_RELPATH] = fp_sha
                    knowledge_snapshot_pointer = ArtifactPointer(
                        path=KNOWLEDGE_SNAPSHOT_RELPATH,
                        sha256=snap_sha,
                    )
                    snap_path = scope_root_p / KNOWLEDGE_SNAPSHOT_RELPATH
                    fp_path = scope_root_p / KNOWLEDGE_SNAPSHOT_FINGERPRINT_RELPATH
                    snap_obj = json.loads(snap_path.read_text(encoding="utf-8"))
                    fp_obj = json.loads(fp_path.read_text(encoding="utf-8"))
                    persisted_knowledge_artifacts = [
                        OutputArtifact.from_json(
                            path=KNOWLEDGE_SNAPSHOT_RELPATH,
                            obj=snap_obj,
                            metadata={"plan_id": result.plan.id, "kind": "knowledge_snapshot"},
                        ),
                        OutputArtifact.from_json(
                            path=KNOWLEDGE_SNAPSHOT_FINGERPRINT_RELPATH,
                            obj=fp_obj,
                            metadata={"plan_id": result.plan.id, "kind": "knowledge_snapshot_fingerprint"},
                        ),
                    ]
                except (OSError, ValueError, TypeError, KeyError, json.JSONDecodeError):
                    knowledge_snapshot_pointer = None
                    persisted_knowledge_artifacts = []
            if outputs_root is not None and output_hashes is not None:
                scope_root_for_knowledge = Path(outputs_root).expanduser() / self.tenant_id / self.repo_id
                km_fp_raw = step_outputs.get("knowledge_mediation_fingerprint")
                med_path = scope_root_for_knowledge / KNOWLEDGE_MEDIATION_RELPATH
                if (
                    isinstance(km_fp_raw, str)
                    and len(km_fp_raw.strip()) == 64
                    and all(c in "0123456789abcdef" for c in km_fp_raw.strip().lower())
                    and med_path.is_file()
                ):
                    try:
                        med_loaded = json.loads(med_path.read_text(encoding="utf-8"))
                        if not isinstance(med_loaded, dict):
                            raise ValueError("mediation artifact must be a JSON object")
                        digest = stable_json_fingerprint(med_loaded)
                        if digest != km_fp_raw.strip().lower():
                            raise ValueError("knowledge_mediation_fingerprint does not match mediation.json content")
                        output_hashes[KNOWLEDGE_MEDIATION_RELPATH] = digest
                        knowledge_mediation_pointer = ArtifactPointer(
                            path=KNOWLEDGE_MEDIATION_RELPATH,
                            sha256=digest,
                        )
                    except (OSError, ValueError, json.JSONDecodeError):
                        knowledge_mediation_pointer = None
            current_knowledge_semantic_fingerprint = (
                step_outputs.get("knowledge_semantic_fingerprint")
                if isinstance(step_outputs.get("knowledge_semantic_fingerprint"), str)
                else None
            )
            current_knowledge_provenance_fingerprint = (
                step_outputs.get("knowledge_provenance_fingerprint")
                if isinstance(step_outputs.get("knowledge_provenance_fingerprint"), str)
                else None
            )
            cost_attribution = self._build_cost_attribution(
                run_id=result.plan.id,
                outputs_root=outputs_root,
                config=config,
                accounting=result.accounting,
                passes=passes,
                trace_spans=combined_trace_spans,
            )
            developer_profile_decisions_obj: dict[str, Any] | None = None
            developer_profile_decisions_path = f".akc/run/{result.plan.id}.developer_profile_decisions.json"
            if profile_mode == "emerging" and developer_profile_decisions is not None:
                developer_profile_decisions_obj = dict(developer_profile_decisions)
                if "fingerprint_sha256" not in developer_profile_decisions_obj:
                    developer_profile_decisions_obj["fingerprint_sha256"] = stable_json_fingerprint(
                        {k: v for k, v in developer_profile_decisions_obj.items() if k != "fingerprint_sha256"}
                    )
                output_hashes[developer_profile_decisions_path] = stable_json_fingerprint(
                    developer_profile_decisions_obj
                )
            scoped_apply_path = f".akc/run/{result.plan.id}.scoped_apply.json"
            scoped_apply_obj: dict[str, Any] | None = None
            csa_raw = result.accounting.get("compile_scoped_apply")
            if isinstance(csa_raw, dict):
                scoped_apply_obj = apply_schema_envelope(
                    obj={
                        **dict(csa_raw),
                        "run_id": result.plan.id,
                        "tenant_id": self.tenant_id,
                        "repo_id": self.repo_id,
                    },
                    kind="compile_scoped_apply",
                    version=int(schema_version),
                )
                output_hashes[scoped_apply_path] = stable_json_fingerprint(scoped_apply_obj)
            control_plane_obj: dict[str, Any] = {
                "stable_intent_sha256": stable_intent_hash,
                "policy_decisions": [
                    dict(x) for x in result.accounting.get("policy_decisions", []) if isinstance(x, dict)
                ],
                "promotion_mode": promotion_mode,
                "promotion_state": promotion_mode,
                "developer_role_profile": profile_mode,
            }
            control_plane_obj["lifecycle_timestamps"] = {
                # Current intent authority enters at compile invocation.
                "intent_received_at": int(compile_started_at_ms),
                "compile_started_at": int(compile_started_at_ms),
                "compile_completed_at": int(time.time() * 1000),
            }
            cfg_meta = dict(config.metadata) if config.metadata else {}
            prof_id = str(cfg_meta.get("living_automation_profile_id", "")).strip().lower()
            bh_raw = cfg_meta.get("baseline_duration_hours")
            baseline_hours: float | None = None
            if isinstance(bh_raw, (int, float)) and not isinstance(bh_raw, bool):
                baseline_hours = float(bh_raw)
            elif prof_id in {"living_loop_v1", "living_loop_unattended_v1"}:
                from akc.living.automation_profile import LIVING_LOOP_V1_DEFAULT_BASELINE_HOURS

                baseline_hours = float(LIVING_LOOP_V1_DEFAULT_BASELINE_HOURS)
            if baseline_hours is not None:
                control_plane_obj["baseline_duration_hours"] = baseline_hours
            if prof_id in {"living_loop_v1", "living_loop_unattended_v1"}:
                control_plane_obj["living_automation_profile_id"] = prof_id
            control_plane_obj["time_compression_metrics"] = derive_time_compression_metrics(
                lifecycle_timestamps=dict(control_plane_obj["lifecycle_timestamps"]),
                baseline_duration_hours=baseline_hours,
            )
            if developer_profile_decisions_obj is not None:
                control_plane_obj["developer_profile_decisions_ref"] = {
                    "path": developer_profile_decisions_path,
                    "sha256": output_hashes.get(developer_profile_decisions_path),
                }
            if promotion_packet_obj is not None:
                control_plane_obj["promotion_packet_ref"] = {
                    "path": promotion_packet_path,
                    "sha256": output_hashes.get(promotion_packet_path),
                }
                control_plane_obj["promotion_policy_allow"] = bool(
                    dict(promotion_packet_obj.get("policy_allow_decision") or {}).get("allowed", False)
                )
                ca_att = promotion_packet_obj.get("compile_apply_attestation")
                if isinstance(ca_att, dict):
                    control_plane_obj["compile_apply_attestation"] = dict(ca_att)
            merge_policy_provenance_for_compile_control_plane(control_plane_obj, opa_policy_path=config.opa_policy_path)
            if scoped_apply_obj is not None:
                control_plane_obj["compile_scoped_apply_ref"] = {
                    "path": scoped_apply_path,
                    "sha256": output_hashes.get(scoped_apply_path),
                }
            success_eval_modes = tuple(sorted({str(sc.evaluation_mode) for sc in intent_spec.success_criteria}))
            intent_acceptance_fp = intent_acceptance_slice_fingerprint(success_criteria=intent_spec.success_criteria)
            run_manifest = RunManifest(
                run_id=result.plan.id,
                tenant_id=result.plan.tenant_id,
                repo_id=result.plan.repo_id,
                ir_sha256=ir_doc.fingerprint(),
                ir_document=ArtifactPointer(
                    path=f".akc/ir/{result.plan.id}.json",
                    sha256=ir_doc.fingerprint(),
                ),
                ir_format_version=ir_doc.format_version,
                intent_semantic_fingerprint=intent_fingerprint.semantic,
                intent_goal_text_fingerprint=intent_fingerprint.goal_text,
                stable_intent_sha256=stable_intent_hash,
                success_criteria_evaluation_modes=success_eval_modes,
                intent_acceptance_fingerprint=intent_acceptance_fp,
                knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
                knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
                knowledge_snapshot=knowledge_snapshot_pointer,
                knowledge_mediation=knowledge_mediation_pointer,
                replay_mode=replay_mode,
                retrieval_snapshots=tuple(retrieval_snapshots),
                passes=tuple(passes),
                model=llm.__class__.__name__,
                model_params={"mode": str((config.metadata or {}).get("mode") or "default")},
                tool_params={
                    "test_mode": config.test_mode,
                    "test_command": list(config.test_command or ()),
                    "full_test_every_n_iterations": config.full_test_every_n_iterations,
                },
                partial_replay_passes=(
                    effective_replay_manifest.partial_replay_passes if effective_replay_manifest is not None else ()
                ),
                llm_vcr=self._build_llm_vcr(passes=passes),
                budgets=config.budget.to_json_obj(),
                output_hashes=output_hashes,
                trace_spans=tuple(combined_trace_spans),
                control_plane=apply_schema_envelope(
                    obj=control_plane_obj,
                    kind="control_plane_envelope",
                    version=int(schema_version),
                ),
                cost_attribution=cost_attribution,
            )
            intent_mandates = mandatory_partial_replay_passes_for_success_criteria(
                success_criteria=intent_spec.success_criteria
            )
            replay_decisions_obj = build_replay_decisions_payload(
                run_id=result.plan.id,
                tenant_id=self.tenant_id,
                repo_id=self.repo_id,
                replay_mode=replay_mode,
                decision_manifest=effective_replay_manifest,
                baseline_manifest=loaded_replay_manifest,
                replay_source_run_id=(loaded_replay_manifest.run_id if loaded_replay_manifest is not None else None),
                current_intent_semantic_fingerprint=intent_fingerprint.semantic,
                current_knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
                current_knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
                current_stable_intent_sha256=stable_intent_hash,
                intent_mandatory_partial_replay_passes=intent_mandates,
                audit_manifest=run_manifest,
            )
            recompile_triggers_obj = build_recompile_triggers_payload(
                tenant_id=self.tenant_id,
                repo_id=self.repo_id,
                run_id=result.plan.id,
                checked_at_ms=int(time.time() * 1000),
                source="compile_session",
                manifest=(
                    effective_replay_manifest
                    if effective_replay_manifest is not None and effective_replay_manifest.run_id != result.plan.id
                    else None
                ),
                current_intent_semantic_fingerprint=intent_fingerprint.semantic,
                current_knowledge_semantic_fingerprint=current_knowledge_semantic_fingerprint,
                current_knowledge_provenance_fingerprint=current_knowledge_provenance_fingerprint,
                current_stable_intent_sha256=stable_intent_hash,
            )
            control_plane = run_manifest.control_plane
            if control_plane is None:
                raise ValueError("run manifest missing control_plane envelope")
            control_plane["replay_decisions_ref"] = {
                "path": f".akc/run/{result.plan.id}.replay_decisions.json",
                "sha256": stable_json_fingerprint(replay_decisions_obj),
            }
            control_plane["recompile_triggers_ref"] = {
                "path": f".akc/run/{result.plan.id}.recompile_triggers.json",
                "sha256": stable_json_fingerprint(recompile_triggers_obj),
            }
            validate_artifact_json(
                obj=control_plane,
                kind="control_plane_envelope",
                version=int(schema_version),
            )
            validate_artifact_json(
                obj=replay_decisions_obj,
                kind="replay_decisions",
                version=int(schema_version),
            )
            validate_artifact_json(
                obj=recompile_triggers_obj,
                kind="recompile_triggers",
                version=int(schema_version),
            )

            artifacts: list[OutputArtifact] = []
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/ir/{result.plan.id}.json",
                    obj=ir_doc.to_json_obj(),
                    metadata={
                        "plan_id": result.plan.id,
                        "kind": "ir_document",
                        "fingerprint": ir_doc.fingerprint(),
                        "format_version": ir_doc.format_version,
                        "schema_version": int(ir_doc.schema_version),
                    },
                )
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/intent/{result.plan.id}.json",
                    obj=intent_spec.to_json_obj(),
                    metadata={
                        "plan_id": result.plan.id,
                        "kind": "intent_spec",
                        "intent_id": intent_spec.intent_id,
                        "intent_semantic_fingerprint": intent_fingerprint.semantic,
                        "intent_goal_text_fingerprint": intent_fingerprint.goal_text,
                    },
                )
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/run/{result.plan.id}.spans.json",
                    obj=apply_schema_envelope(
                        obj={
                            "run_id": result.plan.id,
                            "tenant_id": self.tenant_id,
                            "repo_id": self.repo_id,
                            "spans": [dict(x) for x in run_manifest.trace_spans],
                        },
                        kind="run_trace_spans",
                        version=int(schema_version),
                    ),
                    metadata={"plan_id": result.plan.id, "kind": "trace_spans"},
                )
            )
            if developer_profile_decisions_obj is not None:
                artifacts.append(
                    OutputArtifact.from_json(
                        path=developer_profile_decisions_path,
                        obj=developer_profile_decisions_obj,
                        metadata={
                            "plan_id": result.plan.id,
                            "kind": "developer_profile_decisions",
                            "developer_role_profile": profile_mode,
                        },
                    )
                )
            if scoped_apply_obj is not None:
                artifacts.append(
                    OutputArtifact.from_json(
                        path=scoped_apply_path,
                        obj=scoped_apply_obj,
                        metadata={"plan_id": result.plan.id, "kind": "compile_scoped_apply"},
                    )
                )
            validate_artifact_json(
                obj=apply_schema_envelope(
                    obj={
                        "run_id": result.plan.id,
                        "tenant_id": self.tenant_id,
                        "repo_id": self.repo_id,
                        "spans": [dict(x) for x in run_manifest.trace_spans],
                    },
                    kind="run_trace_spans",
                    version=int(schema_version),
                ),
                kind="run_trace_spans",
                version=int(schema_version),
            )
            otel_export_text = build_compile_trace_export_text(
                spans=[dict(x) for x in run_manifest.trace_spans],
                tenant_id=self.tenant_id,
                repo_id=self.repo_id,
                run_id=result.plan.id,
                stable_intent_sha256=stable_intent_hash,
            )
            if str(otel_export_text).strip():
                artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/run/{result.plan.id}.otel.jsonl",
                        text=otel_export_text,
                        media_type="application/x-ndjson; charset=utf-8",
                        metadata={"plan_id": result.plan.id, "kind": "otel_trace_export"},
                    )
                )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/run/{result.plan.id}.costs.json",
                    obj=apply_schema_envelope(
                        obj=(dict(run_manifest.cost_attribution or {})),
                        kind="run_cost_attribution",
                        version=int(schema_version),
                    ),
                    metadata={"plan_id": result.plan.id, "kind": "cost_attribution"},
                )
            )
            validate_artifact_json(
                obj=apply_schema_envelope(
                    obj=dict(run_manifest.cost_attribution or {}),
                    kind="run_cost_attribution",
                    version=int(schema_version),
                ),
                kind="run_cost_attribution",
                version=int(schema_version),
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/run/{result.plan.id}.replay_decisions.json",
                    obj=replay_decisions_obj,
                    metadata={"plan_id": result.plan.id, "kind": "replay_decisions"},
                )
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/run/{result.plan.id}.recompile_triggers.json",
                    obj=recompile_triggers_obj,
                    metadata={"plan_id": result.plan.id, "kind": "recompile_triggers"},
                )
            )
            artifacts.append(
                OutputArtifact.from_text(
                    path=f".akc/run/{result.plan.id}.log.txt",
                    text=self._build_run_log(run_manifest=run_manifest),
                    media_type="text/plain; charset=utf-8",
                    metadata={"plan_id": result.plan.id, "kind": "run_log"},
                )
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/policy/{result.plan.id}_{step_id_s}.decisions.json",
                    obj={
                        "plan_id": result.plan.id,
                        "step_id": step_id_s,
                        "decisions": list(result.accounting.get("policy_decisions", [])),
                    },
                    metadata={"plan_id": result.plan.id, "step_id": step_id_s, "kind": "policy"},
                )
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/policy/{result.plan.id}_{step_id_s}.operating_bounds_projection.json",
                    obj={
                        "plan_id": result.plan.id,
                        "step_id": step_id_s,
                        "projection": result.accounting.get("policy_operating_bounds_projection", {}),
                    },
                    metadata={
                        "plan_id": result.plan.id,
                        "step_id": step_id_s,
                        "kind": "policy_operating_bounds_projection",
                    },
                )
            )

            ir_diff = self._build_ir_diff_artifact(
                outputs_root=outputs_root,
                plan_id=result.plan.id,
                ir_doc=ir_doc,
            )
            if ir_diff is not None:
                artifacts.append(ir_diff)
            artifacts.extend(persisted_knowledge_artifacts)
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/run/{result.plan.id}.manifest.json",
                    obj=run_manifest.to_json_obj(),
                    metadata={
                        "plan_id": result.plan.id,
                        "stable_hash": run_manifest.stable_hash(),
                        "replay_mode": replay_mode,
                    },
                )
            )
            if promotion_packet_obj is not None:
                artifacts.append(
                    OutputArtifact.from_json(
                        path=promotion_packet_path,
                        obj=promotion_packet_obj,
                        metadata={
                            "plan_id": result.plan.id,
                            "step_id": step_id_s,
                            "kind": "promotion_packet",
                            "promotion_mode": promotion_mode,
                        },
                    )
                )
            artifacts.extend(artifact_pass_artifacts)

            def _emit_stage(*, name: str, payload: dict[str, Any]) -> None:
                stdout = str(payload.get("stdout") or "")
                stderr = str(payload.get("stderr") or "")
                combined = (stdout + ("\n" + stderr if stderr else "")) or "(no output)"
                artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/tests/{result.plan.id}_{step_id_s}.{name}.stdout.txt",
                        text=stdout or "(no stdout)",
                        media_type="text/plain; charset=utf-8",
                        metadata={
                            "plan_id": result.plan.id,
                            "step_id": step_id_s,
                            "stage": name,
                            "stream": "stdout",
                        },
                    )
                )
                artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/tests/{result.plan.id}_{step_id_s}.{name}.stderr.txt",
                        text=stderr or "(no stderr)",
                        media_type="text/plain; charset=utf-8",
                        metadata={
                            "plan_id": result.plan.id,
                            "step_id": step_id_s,
                            "stage": name,
                            "stream": "stderr",
                        },
                    )
                )
                artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/tests/{result.plan.id}_{step_id_s}.{name}.txt",
                        text=combined,
                        media_type="text/plain; charset=utf-8",
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s, "stage": name},
                    )
                )
                # Keep a structured record, too.
                cmd_raw = payload.get("command")
                cmd_list: list[str] = [str(x) for x in cmd_raw] if isinstance(cmd_raw, list) else []
                stage_obj: dict[str, Any] = {
                    "plan_id": result.plan.id,
                    "step_id": step_id_s,
                    "stage": payload.get("stage"),
                    "command": cmd_list,
                    "exit_code": payload.get("exit_code"),
                    "duration_ms": payload.get("duration_ms"),
                    "stdout": stdout,
                    "stderr": stderr,
                }
                apply_schema_envelope(obj=stage_obj, kind="execution_stage", version=int(schema_version))
                artifacts.append(
                    OutputArtifact.from_json(
                        path=f".akc/tests/{result.plan.id}_{step_id_s}.{name}.json",
                        obj=stage_obj,
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s, "stage": name},
                    )
                )

            if result.best_candidate is not None:
                artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/patches/{result.plan.id}_{step_id_s}.diff",
                        text=result.best_candidate.llm_text,
                        media_type="text/x-diff; charset=utf-8",
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s},
                    )
                )

            if result.best_candidate is not None and result.best_candidate.execution is not None:
                # Prefer step outputs when available (smoke+full);
                # otherwise fall back to best_candidate.
                last_smoke = step_outputs.get("last_tests_smoke")
                last_full = step_outputs.get("last_tests_full")
                if isinstance(last_smoke, dict):
                    _emit_stage(name="smoke", payload=dict(last_smoke))
                if isinstance(last_full, dict):
                    _emit_stage(name="full", payload=dict(last_full))

                if not isinstance(last_smoke, dict) and not isinstance(last_full, dict):
                    stdout = result.best_candidate.execution.stdout or ""
                    stderr = result.best_candidate.execution.stderr or ""
                    combined = stdout + ("\n" + stderr if stderr else "")
                    artifacts.append(
                        OutputArtifact.from_text(
                            path=f".akc/tests/{result.plan.id}_{step_id_s}.stdout.txt",
                            text=stdout or "(no stdout)",
                            media_type="text/plain; charset=utf-8",
                            metadata={
                                "plan_id": result.plan.id,
                                "step_id": step_id_s,
                                "stream": "stdout",
                            },
                        )
                    )
                    artifacts.append(
                        OutputArtifact.from_text(
                            path=f".akc/tests/{result.plan.id}_{step_id_s}.stderr.txt",
                            text=stderr or "(no stderr)",
                            media_type="text/plain; charset=utf-8",
                            metadata={
                                "plan_id": result.plan.id,
                                "step_id": step_id_s,
                                "stream": "stderr",
                            },
                        )
                    )
                    artifacts.append(
                        OutputArtifact.from_text(
                            path=f".akc/tests/{result.plan.id}_{step_id_s}.txt",
                            text=combined or "(no output)",
                            media_type="text/plain; charset=utf-8",
                            metadata={"plan_id": result.plan.id, "step_id": step_id_s},
                        )
                    )
                    artifacts.append(
                        OutputArtifact.from_json(
                            path=f".akc/tests/{result.plan.id}_{step_id_s}.json",
                            obj=apply_schema_envelope(
                                obj={
                                    "plan_id": result.plan.id,
                                    "step_id": step_id_s,
                                    "stage": getattr(result.best_candidate, "execution_stage", None),
                                    "command": list(
                                        getattr(
                                            result.best_candidate,
                                            "execution_command",
                                            None,
                                        )
                                        or []
                                    ),
                                    "exit_code": int(result.best_candidate.execution.exit_code),
                                    "duration_ms": result.best_candidate.execution.duration_ms,
                                    "stdout": stdout,
                                    "stderr": stderr,
                                },
                                kind="execution_stage",
                                version=int(schema_version),
                            ),
                            metadata={"plan_id": result.plan.id, "step_id": step_id_s},
                        )
                    )

            last_ver = step_outputs.get("last_verification")
            if isinstance(last_ver, dict):
                artifacts.append(
                    OutputArtifact.from_json(
                        path=f".akc/verification/{result.plan.id}_{step_id_s}.json",
                        obj=apply_schema_envelope(
                            obj=dict(last_ver),
                            kind="verifier_result",
                            version=int(schema_version),
                        ),
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s},
                    )
                )

            bundle = OutputBundle(
                scope=scope,
                name="compile_session",
                artifacts=tuple(artifacts),
                metadata={
                    "run_id": result.plan.id,
                    "replay_mode": replay_mode,
                    **(
                        {"runtime_bundle_artifact": artifact_bundle_md.get("runtime_bundle")}
                        if artifact_bundle_md.get("runtime_bundle") is not None
                        else {}
                    ),
                    **({"artifact_passes": artifact_bundle_md} if artifact_bundle_md else {}),
                },
            )
            written_paths = (emitter or JsonManifestEmitter()).emit(bundle=bundle, root=outputs_root)
            resolved_root = Path(outputs_root).expanduser().resolve()
            for wp in written_paths:
                wp_r = wp.resolve()
                if not wp_r.name.endswith(".manifest.json"):
                    continue
                parts = wp_r.parts
                if ".akc" not in parts:
                    continue
                akc_i = parts.index(".akc")
                if akc_i + 1 < len(parts) and parts[akc_i + 1] == "run":
                    try_upsert_operations_index_from_manifest(wp_r, outputs_root=resolved_root)
                    break

        return result

    def _ir_from_plan(
        self,
        *,
        plan: PlanState,
        intent_node_properties: dict[str, Any] | None = None,
        intent_store: IntentStore | None = None,
        controller_intent_spec: IntentSpecV1 | None = None,
    ) -> IRDocument:
        # Keep session IR emission consistent with the controller and recompile
        # logic by delegating to the shared builder.
        return build_ir_document_from_plan(
            plan=plan,
            intent_node_properties=intent_node_properties or None,
            intent_store=intent_store,
            controller_intent_spec=controller_intent_spec,
            warn_legacy_step_blobs_without_intent_ref_under_outputs_root=True,
        )

    def _build_retrieval_snapshots(self, *, step_outputs: dict[str, Any]) -> list[RetrievalSnapshot]:
        payload = step_outputs.get("retrieval_snapshot")
        if not isinstance(payload, dict):
            return []
        query = str(payload.get("query") or "").strip()
        source = str(payload.get("source") or "compile_retriever")
        top_k = int(payload.get("top_k") or 20)
        item_ids_raw = payload.get("item_ids")
        item_ids = tuple(str(x) for x in item_ids_raw) if isinstance(item_ids_raw, list) else ()
        if not query or top_k <= 0:
            return []
        return [RetrievalSnapshot(source=source, query=query, top_k=top_k, item_ids=item_ids)]

    def _build_pass_records(self, *, result: ControllerResult, step_outputs: dict[str, Any]) -> list[PassRecord]:
        compile_status: Literal["succeeded", "failed"] = (
            "succeeded" if bool(getattr(result, "compile_succeeded", False)) else "failed"
        )
        best_hash: str | None = None
        if result.best_candidate is not None and result.best_candidate.llm_text.strip():
            best_hash = stable_json_fingerprint({"patch": result.best_candidate.llm_text})
        out: list[PassRecord] = [
            PassRecord(name="plan", status="succeeded"),
            PassRecord(name="retrieve", status="succeeded"),
            PassRecord(
                name="generate",
                status=(
                    "succeeded"
                    if int(result.accounting.get("llm_calls", 0)) > 0 or result.best_candidate is not None
                    else "skipped"
                ),
                output_sha256=best_hash,
                metadata=(
                    {
                        "llm_text": result.best_candidate.llm_text,
                        "prompt_key": str(step_outputs.get("last_prompt_key") or ""),
                    }
                    if result.best_candidate is not None
                    else None
                ),
            ),
        ]
        exec_payload = step_outputs.get("last_tests_full") or step_outputs.get("last_tests_smoke")
        exec_md: dict[str, Any] | None = None
        if isinstance(exec_payload, dict):
            duration_raw = exec_payload.get("duration_ms")
            duration_ms: int | None = int(duration_raw) if duration_raw is not None else None
            exec_md = {
                "stage": str(exec_payload.get("stage") or "tests_full"),
                "command": (
                    [str(x) for x in exec_payload.get("command", [])]
                    if isinstance(exec_payload.get("command"), list)
                    else []
                ),
                "exit_code": int(exec_payload.get("exit_code", 1)),
                "stdout": str(exec_payload.get("stdout") or ""),
                "stderr": str(exec_payload.get("stderr") or ""),
                "duration_ms": duration_ms,
            }
        out.append(PassRecord(name="execute", status=compile_status, metadata=exec_md))
        if int(result.accounting.get("repair_iterations", 0)) > 0:
            out.append(PassRecord(name="repair", status=compile_status))
        else:
            out.append(PassRecord(name="repair", status="skipped"))
        last_ver = step_outputs.get("last_verification")
        if isinstance(last_ver, dict):
            out.append(
                PassRecord(
                    name="verify",
                    status="succeeded" if bool(last_ver.get("passed")) else "failed",
                )
            )
        else:
            # Replay contracts: always emit `verify` (cf. `intent_acceptance` below).
            out.append(PassRecord(name="verify", status="skipped"))
        last_intent_accept = step_outputs.get("last_intent_acceptance")
        if isinstance(last_intent_accept, dict):
            passed = bool(last_intent_accept.get("passed"))
            out.append(
                PassRecord(
                    name="intent_acceptance",
                    status="succeeded" if passed else "failed",
                    metadata={
                        "evaluated_success_criteria": json_value_as_int(
                            last_intent_accept.get("evaluated_success_criteria"), default=0
                        ),
                        "failures": list(last_intent_accept.get("failures", []) or []),
                    },
                )
            )
        else:
            # Keep replay contracts stable: always emit an `intent_acceptance` pass
            # record so consumers can rely on its presence (even when the gate
            # never ran due to earlier budget/policy failures).
            out.append(PassRecord(name="intent_acceptance", status="skipped"))
        names = tuple(rec.name for rec in out)
        if names != CONTROLLER_LOOP_PASS_ORDER:
            raise RuntimeError(
                f"controller pass record order mismatch: got {list(names)!r}, "
                f"expected {list(CONTROLLER_LOOP_PASS_ORDER)!r}"
            )
        return out

    def _build_llm_vcr(self, *, passes: list[PassRecord]) -> dict[str, str]:
        out: dict[str, str] = {}
        for rec in passes:
            md = dict(rec.metadata or {})
            key = md.get("prompt_key")
            text = md.get("llm_text")
            if isinstance(key, str) and key and isinstance(text, str) and text:
                out[key] = text
        return out

    def _build_output_hashes(
        self,
        *,
        result: ControllerResult,
        plan_id: str,
        step_id: str,
        ir_doc: IRDocument,
    ) -> dict[str, str]:
        hashes: dict[str, str] = {f".akc/ir/{plan_id}.json": ir_doc.fingerprint()}
        if result.best_candidate is not None and result.best_candidate.llm_text.strip():
            hashes[f".akc/patches/{plan_id}_{step_id}.diff"] = stable_json_fingerprint(
                {"patch": result.best_candidate.llm_text}
            )
        return hashes

    def _execute_artifact_passes(
        self,
        *,
        run_id: str,
        ir_doc: IRDocument,
        intent_spec: IntentSpecV1,
        step_outputs: dict[str, Any],
        outputs_root: str | Path,
        replay_manifest: RunManifest | None,
        current_intent_semantic_fingerprint: str | None,
        current_stable_intent_sha256: str | None = None,
        controller_accounting: dict[str, Any] | None = None,
        runtime_bundle_schema_version: int = RUNTIME_BUNDLE_SCHEMA_VERSION,
        runtime_bundle_embed_system_ir: bool = False,
        artifact_consistency_policy: Literal["off", "warn", "error"] = "warn",
        deployment_intents_ir_alignment_policy: Literal["off", "warn", "error"] = "off",
        reconcile_deploy_targets_from_ir_only: bool = False,
    ) -> tuple[
        list[PassRecord],
        list[OutputArtifact],
        dict[str, str],
        dict[str, JSONValue],
        list[dict[str, JSONValue]],
    ]:
        """Run deterministic artifact passes in stable order and collect replay metadata."""
        intent_mandates = mandatory_partial_replay_passes_for_success_criteria(
            success_criteria=intent_spec.success_criteria
        )
        pass_records: list[PassRecord] = []
        trace_spans: list[dict[str, JSONValue]] = []
        artifacts_by_group: dict[str, list[OutputArtifact]] = {
            "specs": [],
            "code_stubs": [],
            "runtime": [],
            "deployment_configs": [],
        }
        output_hashes: dict[str, str] = {}
        pass_order: list[str] = []
        groups: dict[str, list[str]] = {
            "specs": [],
            "code_stubs": [],
            "runtime": [],
            "deployment_configs": [],
        }
        scope_root = Path(outputs_root).expanduser() / self.tenant_id / self.repo_id
        linked_trace_id = (
            str(controller_accounting.get("trace_id")).strip()
            if isinstance(controller_accounting, dict) and str(controller_accounting.get("trace_id", "")).strip()
            else new_trace_id()
        )
        linked_run_span_id = (
            str(controller_accounting.get("run_span_id")).strip()
            if isinstance(controller_accounting, dict) and str(controller_accounting.get("run_span_id", "")).strip()
            else None
        )

        def _clone_replay_artifact(*, artifact: OutputArtifact, prior_run_id: str) -> OutputArtifact:
            new_path = artifact.path.replace(prior_run_id, run_id)
            text = artifact.text()
            cloned_text = text.replace(prior_run_id, run_id)
            return OutputArtifact.from_text(
                path=new_path,
                text=cloned_text,
                media_type=artifact.media_type,
                metadata=artifact.metadata,
            )

        def _load_replayed_pass_artifacts(*, name: str) -> tuple[PassRecord, list[OutputArtifact]] | None:
            if replay_manifest is None:
                return None
            previous = next((record for record in replay_manifest.passes if record.name == name), None)
            if previous is None:
                return None
            metadata = dict(previous.metadata or {})
            artifact_paths = metadata.get("artifact_paths")
            if not isinstance(artifact_paths, list) or not artifact_paths:
                return None
            loaded_artifacts: list[OutputArtifact] = []
            for artifact_path in artifact_paths:
                if not isinstance(artifact_path, str) or not artifact_path.strip():
                    return None
                full_path = scope_root / artifact_path
                if not full_path.exists():
                    return None
                loaded_artifacts.append(
                    _clone_replay_artifact(
                        artifact=OutputArtifact(
                            path=artifact_path,
                            content=full_path.read_bytes(),
                            media_type=(
                                "application/json; charset=utf-8"
                                if artifact_path.endswith(".json")
                                else "application/yaml; charset=utf-8"
                                if artifact_path.endswith((".yml", ".yaml"))
                                else "text/markdown; charset=utf-8"
                                if artifact_path.endswith(".md")
                                else "text/plain; charset=utf-8"
                            ),
                            metadata=None,
                        ),
                        prior_run_id=replay_manifest.run_id,
                    )
                )
            return previous, loaded_artifacts

        def _append_artifact_trace_span(
            *,
            pass_name: str,
            start_ns: int,
            metadata: dict[str, JSONValue],
            reused_from_replay_manifest: bool,
        ) -> None:
            attrs: dict[str, JSONValue] = {
                "stage": "artifact_pass",
                "akc.pass_name": pass_name,
                "akc.linked_run_id": run_id,
                "reused_from_replay_manifest": bool(reused_from_replay_manifest),
            }
            if linked_run_span_id is not None:
                attrs["akc.linked_parent_span_id"] = linked_run_span_id
            artifact_group = metadata.get("artifact_group")
            if isinstance(artifact_group, str) and artifact_group.strip():
                attrs["artifact_group"] = artifact_group
            artifact_paths = metadata.get("artifact_paths")
            if isinstance(artifact_paths, list):
                attrs["artifact_count"] = len(artifact_paths)
            trace_spans.append(
                TraceSpan(
                    trace_id=linked_trace_id,
                    span_id=new_span_id(),
                    parent_span_id=None,
                    name=f"compile.artifact.{pass_name}",
                    kind="internal",
                    start_time_unix_nano=int(start_ns),
                    end_time_unix_nano=max(int(now_unix_nano()), int(start_ns)),
                    attributes=attrs,
                ).to_json_obj()
            )

        def _register_pass(
            *,
            name: str,
            pass_artifacts: list[OutputArtifact],
            group: str,
            base_metadata: dict[str, JSONValue] | None = None,
            replay_source_run_id: str | None = None,
            span_start_ns: int,
        ) -> None:
            def _artifact_group_for_path(path: str) -> str:
                if path.startswith(".akc/runtime/"):
                    return "runtime"
                if path.startswith(".akc/deployment/") or path.startswith(".github/workflows/"):
                    return "deployment_configs"
                if path.endswith(".py") or path.endswith(".ts"):
                    return "code_stubs"
                return "specs"

            artifact_paths = [artifact.path for artifact in pass_artifacts]
            artifact_hashes = {artifact.path: artifact.sha256_hex() for artifact in pass_artifacts}
            for artifact in pass_artifacts:
                artifacts_by_group[_artifact_group_for_path(artifact.path)].append(artifact)
                output_hashes[artifact.path] = artifact.sha256_hex()
            pass_order.append(name)
            groups[group].append(name)
            # Base metadata first (replay markers, counts, …); paths/hashes must match `pass_artifacts`
            # so baselines cannot clobber expanded artifact lists (e.g. delivery summary companion).
            metadata: dict[str, JSONValue] = dict(base_metadata) if base_metadata else {}
            if replay_source_run_id is not None:
                metadata["replay_source_run_id"] = replay_source_run_id
                metadata["reused_from_replay_manifest"] = True
            metadata["artifact_group"] = group
            metadata["artifact_paths"] = list(artifact_paths)
            metadata["artifact_hashes"] = dict(artifact_hashes)
            if name == "runtime_bundle":
                rb_path = next((p for p in artifact_paths if str(p).endswith(".runtime_bundle.json")), None)
                if rb_path is not None:
                    metadata["runtime_bundle_path"] = rb_path
            if name == "delivery_plan":
                dp_path = next((p for p in artifact_paths if str(p).endswith(".delivery_plan.json")), None)
                if dp_path is not None:
                    metadata["delivery_plan_path"] = dp_path
                ds_path = next((p for p in artifact_paths if str(p).endswith(".delivery_summary.md")), None)
                if ds_path is not None:
                    metadata["delivery_summary_path"] = ds_path
            pass_records.append(
                PassRecord(
                    name=name,
                    status="succeeded",
                    output_sha256=stable_json_fingerprint(
                        {
                            "pass_name": name,
                            "artifacts": [{"path": p, "sha256": artifact_hashes[p]} for p in sorted(artifact_hashes)],
                        }
                    ),
                    metadata=metadata,
                )
            )
            _append_artifact_trace_span(
                pass_name=name,
                start_ns=span_start_ns,
                metadata=metadata,
                reused_from_replay_manifest=replay_source_run_id is not None,
            )

        def _maybe_load_replay_pass(
            pass_name: str,
        ) -> tuple[PassRecord, list[OutputArtifact]] | None:
            """If replay policy says reuse and baseline has loadable artifacts, return them.

            Otherwise return None so the caller runs the pass fresh (e.g. minimal seed
            manifests that only record generate/execute).
            """
            if replay_manifest is None:
                return None
            decision = decide_replay_for_pass(
                manifest=replay_manifest,
                pass_name=pass_name,
                current_intent_semantic_fingerprint=current_intent_semantic_fingerprint,
                current_stable_intent_sha256=current_stable_intent_sha256,
                current_knowledge_semantic_fingerprint=(
                    step_outputs.get("knowledge_semantic_fingerprint")
                    if isinstance(step_outputs.get("knowledge_semantic_fingerprint"), str)
                    else None
                ),
                current_knowledge_provenance_fingerprint=(
                    step_outputs.get("knowledge_provenance_fingerprint")
                    if isinstance(step_outputs.get("knowledge_provenance_fingerprint"), str)
                    else None
                ),
                intent_mandatory_partial_replay_passes=intent_mandates,
            )
            if decision.should_call_tools is not False:
                return None
            return _load_replayed_pass_artifacts(name=pass_name)

        orchestration_json_text: str
        coordination_json_text: str

        system_replayed = _maybe_load_replay_pass("system_design")
        if system_replayed is not None:
            system_design_span_start_ns = now_unix_nano()
            previous, system_design_artifacts = system_replayed
            _register_pass(
                name="system_design",
                pass_artifacts=system_design_artifacts,
                group="specs",
                base_metadata={
                    **dict(previous.metadata or {}),
                    **({"replay_mode": replay_manifest.replay_mode} if replay_manifest else {}),
                },
                replay_source_run_id=replay_manifest.run_id if replay_manifest else None,
                span_start_ns=system_design_span_start_ns,
            )
        else:
            system_design_span_start_ns = now_unix_nano()
            system_design_result = run_system_design_pass(
                run_id=run_id,
                ir_document=ir_doc,
                intent_spec=intent_spec,
                knowledge_snapshot=(
                    step_outputs.get("knowledge_snapshot")
                    if isinstance(step_outputs.get("knowledge_snapshot"), dict)
                    else None
                ),
                emit_markdown_companion=True,
            )
            system_design_artifacts = [system_design_result.artifact_json]
            if system_design_result.artifact_md is not None:
                system_design_artifacts.append(system_design_result.artifact_md)
            _register_pass(
                name="system_design",
                pass_artifacts=system_design_artifacts,
                group="specs",
                base_metadata=dict(system_design_result.metadata),
                span_start_ns=system_design_span_start_ns,
            )

        orchestration_replayed = _maybe_load_replay_pass("orchestration_spec")
        if orchestration_replayed is not None:
            orchestration_span_start_ns = now_unix_nano()
            previous, orchestration_artifacts = orchestration_replayed
            orchestration_json_artifact = next(
                artifact for artifact in orchestration_artifacts if artifact.path.endswith(".json")
            )
            orchestration_json_text = orchestration_json_artifact.text()
            _register_pass(
                name="orchestration_spec",
                pass_artifacts=orchestration_artifacts,
                group="specs",
                base_metadata={
                    **dict(previous.metadata or {}),
                    **({"replay_mode": replay_manifest.replay_mode} if replay_manifest else {}),
                },
                replay_source_run_id=replay_manifest.run_id if replay_manifest else None,
                span_start_ns=orchestration_span_start_ns,
            )
        else:
            orchestration_span_start_ns = now_unix_nano()
            orchestration_result = run_orchestration_spec_pass(
                run_id=run_id,
                ir_document=ir_doc,
                intent_spec=intent_spec,
            )
            orchestration_json_text = orchestration_result.artifact_json.text()
            _register_pass(
                name="orchestration_spec",
                pass_artifacts=[
                    orchestration_result.artifact_json,
                    orchestration_result.artifact_python_stub,
                    orchestration_result.artifact_typescript_stub,
                ],
                group="specs",
                base_metadata=dict(orchestration_result.metadata),
                span_start_ns=orchestration_span_start_ns,
            )

        coordination_replayed = _maybe_load_replay_pass("agent_coordination")
        if coordination_replayed is not None:
            coordination_span_start_ns = now_unix_nano()
            previous, coordination_artifacts = coordination_replayed
            coordination_json_artifact = next(
                artifact for artifact in coordination_artifacts if artifact.path.endswith(".json")
            )
            coordination_json_text = coordination_json_artifact.text()
            _register_pass(
                name="agent_coordination",
                pass_artifacts=coordination_artifacts,
                group="specs",
                base_metadata={
                    **dict(previous.metadata or {}),
                    **({"replay_mode": replay_manifest.replay_mode} if replay_manifest else {}),
                },
                replay_source_run_id=replay_manifest.run_id if replay_manifest else None,
                span_start_ns=coordination_span_start_ns,
            )
        else:
            coordination_span_start_ns = now_unix_nano()
            coordination_result = run_agent_coordination_pass(
                run_id=run_id,
                ir_document=ir_doc,
                intent_spec=intent_spec,
            )
            coordination_json_text = coordination_result.artifact_json.text()
            _register_pass(
                name="agent_coordination",
                pass_artifacts=[
                    coordination_result.artifact_json,
                    coordination_result.artifact_python_stub,
                    coordination_result.artifact_typescript_stub,
                ],
                group="specs",
                base_metadata=dict(coordination_result.metadata),
                span_start_ns=coordination_span_start_ns,
            )

        delivery_plan_json_text: str
        delivery_plan_replayed = _maybe_load_replay_pass("delivery_plan")
        if delivery_plan_replayed is not None:
            delivery_plan_span_start_ns = now_unix_nano()
            previous, delivery_plan_artifacts = delivery_plan_replayed
            delivery_plan_artifacts = list(delivery_plan_artifacts)
            delivery_plan_json_artifact = next(
                artifact for artifact in delivery_plan_artifacts if artifact.path.endswith(".json")
            )
            delivery_plan_json_text = delivery_plan_json_artifact.text()
            if not any(str(a.path).endswith(".delivery_summary.md") for a in delivery_plan_artifacts):
                plan_obj = parse_json_artifact_text(delivery_plan_json_text)
                summary_text = render_delivery_summary_markdown(run_id=run_id, delivery_plan=plan_obj)
                delivery_plan_artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/design/{run_id}.delivery_summary.md",
                        text=summary_text,
                        media_type="text/markdown; charset=utf-8",
                        metadata={
                            "run_id": run_id,
                            "kind": "delivery_summary_markdown",
                            "companion_of": delivery_plan_json_artifact.path,
                            "synthesized_from_replay": True,
                        },
                    )
                )
            _register_pass(
                name="delivery_plan",
                pass_artifacts=delivery_plan_artifacts,
                group="deployment_configs",
                base_metadata={
                    **dict(previous.metadata or {}),
                    **({"replay_mode": replay_manifest.replay_mode} if replay_manifest else {}),
                },
                replay_source_run_id=replay_manifest.run_id if replay_manifest else None,
                span_start_ns=delivery_plan_span_start_ns,
            )
        else:
            delivery_plan_span_start_ns = now_unix_nano()
            delivery_plan_result = run_delivery_plan_pass(
                run_id=run_id,
                ir_document=ir_doc,
                intent_spec=intent_spec,
                orchestration_spec_text=orchestration_json_text,
                coordination_spec_text=coordination_json_text,
            )
            delivery_plan_json_text = delivery_plan_result.artifact_json.text()
            _register_pass(
                name="delivery_plan",
                pass_artifacts=[delivery_plan_result.artifact_json, delivery_plan_result.artifact_summary_md],
                group="deployment_configs",
                base_metadata=dict(delivery_plan_result.metadata),
                span_start_ns=delivery_plan_span_start_ns,
            )

        runtime_bundle_replayed = _maybe_load_replay_pass("runtime_bundle")
        if runtime_bundle_replayed is not None:
            runtime_bundle_span_start_ns = now_unix_nano()
            previous, runtime_bundle_artifacts = runtime_bundle_replayed
            _register_pass(
                name="runtime_bundle",
                pass_artifacts=runtime_bundle_artifacts,
                group="runtime",
                base_metadata={
                    **dict(previous.metadata or {}),
                    **({"replay_mode": replay_manifest.replay_mode} if replay_manifest else {}),
                },
                replay_source_run_id=replay_manifest.run_id if replay_manifest else None,
                span_start_ns=runtime_bundle_span_start_ns,
            )
        else:
            runtime_bundle_span_start_ns = now_unix_nano()
            bundle_dep_align: Literal["off", "strict"] = (
                "strict" if deployment_intents_ir_alignment_policy == "error" else "off"
            )
            runtime_bundle_result = run_runtime_bundle_pass(
                run_id=run_id,
                ir_document=ir_doc,
                intent_spec=intent_spec,
                orchestration_spec_text=orchestration_json_text,
                coordination_spec_text=coordination_json_text,
                delivery_plan_text=delivery_plan_json_text,
                embed_system_ir=bool(runtime_bundle_embed_system_ir),
                runtime_bundle_schema_version=int(runtime_bundle_schema_version),
                reconcile_deploy_targets_from_ir_only=bool(reconcile_deploy_targets_from_ir_only),
                deployment_intents_ir_alignment=bundle_dep_align,
            )
            runtime_bundle_artifacts = [runtime_bundle_result.artifact_json]
            _register_pass(
                name="runtime_bundle",
                pass_artifacts=runtime_bundle_artifacts,
                group="runtime",
                base_metadata=dict(runtime_bundle_result.metadata),
                span_start_ns=runtime_bundle_span_start_ns,
            )

        deployment_docker_compose_text: str | None = None
        deployment_replayed = _maybe_load_replay_pass("deployment_config")
        if deployment_replayed is not None:
            deployment_span_start_ns = now_unix_nano()
            previous, deployment_artifacts = deployment_replayed
            deployment_docker_compose_text = next(
                (a.text() for a in deployment_artifacts if str(a.path).endswith("docker-compose.yml")),
                None,
            )
            _register_pass(
                name="deployment_config",
                pass_artifacts=deployment_artifacts,
                group="deployment_configs",
                base_metadata={
                    **dict(previous.metadata or {}),
                    **({"replay_mode": replay_manifest.replay_mode} if replay_manifest else {}),
                },
                replay_source_run_id=replay_manifest.run_id if replay_manifest else None,
                span_start_ns=deployment_span_start_ns,
            )
        else:
            deployment_span_start_ns = now_unix_nano()
            deployment_result = run_deployment_config_pass(
                run_id=run_id,
                ir_document=ir_doc,
                intent_spec=intent_spec,
                orchestration_spec_text=orchestration_json_text,
                coordination_spec_text=coordination_json_text,
                delivery_plan_text=delivery_plan_json_text,
            )
            deployment_docker_compose_text = deployment_result.artifact_docker_compose.text()
            _register_pass(
                name="deployment_config",
                pass_artifacts=[
                    deployment_result.artifact_docker_compose,
                    deployment_result.artifact_k8s_deployment,
                    deployment_result.artifact_k8s_service,
                    deployment_result.artifact_k8s_configmap,
                    deployment_result.artifact_github_actions,
                    *deployment_result.additional_artifacts,
                ],
                group="deployment_configs",
                base_metadata=dict(deployment_result.metadata),
                span_start_ns=deployment_span_start_ns,
            )

        needs_bundle_validation = (
            artifact_consistency_policy != "off" or deployment_intents_ir_alignment_policy != "off"
        )
        if needs_bundle_validation:
            rb_json = next(
                (a for a in runtime_bundle_artifacts if str(a.path).endswith(".runtime_bundle.json")),
                None,
            )
            if rb_json is None:
                raise ValueError("artifact consistency: runtime bundle JSON artifact not found")
            bundle_raw = json.loads(rb_json.text())
            if not isinstance(bundle_raw, dict):
                raise ValueError("artifact consistency: runtime bundle must decode to a JSON object")
            if artifact_consistency_policy != "off":
                ac_issues = collect_cross_artifact_consistency_issues(
                    ir_document=ir_doc,
                    orchestration_json_text=orchestration_json_text,
                    coordination_json_text=coordination_json_text,
                    runtime_bundle_obj=bundle_raw,
                    intent_spec=intent_spec,
                    deployment_docker_compose_yaml=deployment_docker_compose_text,
                )
                if ac_issues:
                    if artifact_consistency_policy == "error":
                        raise ValueError("Artifact consistency validation failed: " + "; ".join(ac_issues))
                    logger.warning("Artifact consistency: %s", "; ".join(ac_issues))

            if deployment_intents_ir_alignment_policy != "off":
                dep_issues = validate_deployment_intents_align_with_ir(bundle_raw)
                if dep_issues:
                    msg = "Deployment intents vs IR alignment: " + "; ".join(dep_issues)
                    if deployment_intents_ir_alignment_policy == "error":
                        raise ValueError(msg)
                    logger.warning("%s", msg)

        assert_expected_artifact_pass_order(actual=pass_order)

        bundle_md: dict[str, JSONValue] = {
            "order": list(pass_order),
            "groups": {name: list(items) for name, items in groups.items()},
            "output_hashes": dict(output_hashes),
        }
        runtime_bundle_record = next(
            (record for record in pass_records if record.name == "runtime_bundle"),
            None,
        )
        if runtime_bundle_record is not None and runtime_bundle_record.metadata is not None:
            bundle_md["runtime_bundle"] = dict(runtime_bundle_record.metadata)
            # Runtime CLI appends per-run coordination audit JSONL; record the stable relative layout for manifests.
            bundle_md["runtime_coordination_audit_evidence_relative_path"] = (
                ".akc/runtime/<compile_run_id>/<runtime_run_id>/evidence/coordination_audit.jsonl"
            )
        ordered_artifacts = (
            artifacts_by_group["specs"]
            + artifacts_by_group["code_stubs"]
            + artifacts_by_group["runtime"]
            + artifacts_by_group["deployment_configs"]
        )
        return pass_records, ordered_artifacts, output_hashes, bundle_md, trace_spans

    def _build_cost_attribution(
        self,
        *,
        run_id: str,
        outputs_root: str | Path,
        config: ControllerConfig,
        accounting: dict[str, Any],
        passes: list[PassRecord],
        trace_spans: list[dict[str, JSONValue]],
    ) -> dict[str, JSONValue]:
        by_pass = self._rollup_costs_by_pass(
            passes=passes,
            trace_spans=trace_spans,
            config=config,
        )
        controller_pass_names = frozenset(CONTROLLER_LOOP_PASS_ORDER)
        controller_rollup = self._new_cost_rollup()
        artifact_rollup = self._new_cost_rollup()
        for pass_name, rollup in by_pass.items():
            target = controller_rollup if pass_name in controller_pass_names else artifact_rollup
            self._accumulate_cost_rollup(target=target, src=rollup)
        controller_rollup["estimated_cost_usd"] = float(accounting.get("estimated_cost_usd", 0.0))
        cost_attribution: dict[str, JSONValue] = {
            "tenant_id": self.tenant_id,
            "repo_id": self.repo_id,
            "run_id": run_id,
            "currency": str(config.cost_rates.currency),
            "pricing_version": str(config.cost_rates.pricing_version),
            "llm_calls": int(accounting.get("llm_calls", 0)),
            "tool_calls": int(accounting.get("tool_calls", 0)),
            "input_tokens": int(accounting.get("input_tokens", 0)),
            "output_tokens": int(accounting.get("output_tokens", 0)),
            "total_tokens": int(accounting.get("total_tokens", 0)),
            "estimated_cost_usd": float(accounting.get("estimated_cost_usd", 0.0)),
            "repair_iterations": int(accounting.get("repair_iterations", 0)),
            "wall_time_ms": int(accounting.get("wall_time_ms", 0)),
            "budget": config.budget.to_json_obj(),
            "cost_rates": config.cost_rates.to_json_obj(),
            "by_pass": {k: dict(v) for k, v in by_pass.items()},
            "by_component": {
                "controller": controller_rollup,
                "artifact_passes": artifact_rollup,
            },
        }
        cost_attribution["tenant_totals"] = self._write_and_read_tenant_cost_totals(
            outputs_root=outputs_root,
            run_id=run_id,
            repo_id=self.repo_id,
            current_run_costs=cost_attribution,
        )
        return cost_attribution

    def _rollup_costs_by_pass(
        self,
        *,
        passes: list[PassRecord],
        trace_spans: list[dict[str, JSONValue]],
        config: ControllerConfig,
    ) -> dict[str, dict[str, JSONValue]]:
        by_pass: dict[str, dict[str, JSONValue]] = {}
        for rec in passes:
            by_pass[rec.name] = self._new_cost_rollup(status=rec.status)
        for span in trace_spans:
            if not isinstance(span, dict):
                continue
            pass_name = self._pass_name_for_cost_span(span=span)
            if pass_name is None:
                continue
            rollup = by_pass.setdefault(pass_name, self._new_cost_rollup())
            rollup["wall_time_ms"] = json_value_as_int(rollup.get("wall_time_ms"), default=0) + self._span_duration_ms(
                span=span
            )
            attrs = span.get("attributes")
            attrs_dict = attrs if isinstance(attrs, dict) else {}
            span_name = str(span.get("name") or "")
            if span_name == "compile.llm.complete":
                input_tokens = json_value_as_int(attrs_dict.get("input_tokens", 0), default=0)
                output_tokens = json_value_as_int(attrs_dict.get("output_tokens", 0), default=0)
                rollup["llm_calls"] = json_value_as_int(rollup.get("llm_calls"), default=0) + 1
                rollup["input_tokens"] = json_value_as_int(rollup.get("input_tokens"), default=0) + input_tokens
                rollup["output_tokens"] = json_value_as_int(rollup.get("output_tokens"), default=0) + output_tokens
                rollup["total_tokens"] = (
                    json_value_as_int(rollup.get("total_tokens"), default=0) + input_tokens + output_tokens
                )
            elif span_name.startswith("compile.executor."):
                rollup["tool_calls"] = json_value_as_int(rollup.get("tool_calls"), default=0) + 1
            reused = attrs_dict.get("reused_from_replay_manifest")
            if isinstance(reused, bool):
                rollup["reused_from_replay_manifest"] = reused
        for rollup in by_pass.values():
            rollup["estimated_cost_usd"] = self._estimate_rollup_cost_usd(
                input_tokens=json_value_as_int(rollup.get("input_tokens"), default=0),
                output_tokens=json_value_as_int(rollup.get("output_tokens"), default=0),
                tool_calls=json_value_as_int(rollup.get("tool_calls"), default=0),
                config=config,
            )
        return by_pass

    def _pass_name_for_cost_span(self, *, span: dict[str, JSONValue]) -> str | None:
        name = str(span.get("name") or "")
        attrs = span.get("attributes")
        attrs_dict = attrs if isinstance(attrs, dict) else {}
        if name == "compile.retrieve":
            return "retrieve"
        if name == "compile.llm.complete":
            stage = str(attrs_dict.get("stage") or "").strip()
            return stage or "generate"
        if name.startswith("compile.executor."):
            return "execute"
        if name == "compile.verify":
            return "verify"
        if name == "compile.intent_acceptance":
            return "intent_acceptance"
        if name.startswith("compile.artifact."):
            pass_name = str(attrs_dict.get("akc.pass_name") or name.removeprefix("compile.artifact."))
            return pass_name.strip() or None
        return None

    def _estimate_rollup_cost_usd(
        self,
        *,
        input_tokens: int,
        output_tokens: int,
        tool_calls: int,
        config: ControllerConfig,
    ) -> float:
        return (
            (float(max(0, int(input_tokens))) / 1000.0) * float(config.cost_rates.input_per_1k_tokens_usd)
            + (float(max(0, int(output_tokens))) / 1000.0) * float(config.cost_rates.output_per_1k_tokens_usd)
            + float(max(0, int(tool_calls))) * float(config.cost_rates.tool_call_usd)
        )

    def _span_duration_ms(self, *, span: dict[str, JSONValue]) -> int:
        start_raw = span.get("start_time_unix_nano")
        end_raw = span.get("end_time_unix_nano")
        if start_raw is None or end_raw is None:
            return 0
        try:
            start_ns = json_value_as_int(start_raw, default=0)
            end_ns = json_value_as_int(end_raw, default=0)
        except (TypeError, ValueError):
            return 0
        return max(0, int((end_ns - start_ns) / 1_000_000))

    def _new_cost_rollup(self, *, status: str | None = None) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "llm_calls": 0,
            "tool_calls": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "total_tokens": 0,
            "estimated_cost_usd": 0.0,
            "wall_time_ms": 0,
        }
        if status is not None:
            obj["status"] = status
        return obj

    def _accumulate_cost_rollup(self, *, target: dict[str, JSONValue], src: dict[str, JSONValue]) -> None:
        for key in (
            "llm_calls",
            "tool_calls",
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "wall_time_ms",
        ):
            target[key] = json_value_as_int(target.get(key), default=0) + json_value_as_int(src.get(key), default=0)
        target["estimated_cost_usd"] = json_value_as_float(
            target.get("estimated_cost_usd"), default=0.0
        ) + json_value_as_float(src.get("estimated_cost_usd"), default=0.0)

    def _build_ir_diff_artifact(
        self,
        *,
        outputs_root: str | Path,
        plan_id: str,
        ir_doc: IRDocument,
    ) -> OutputArtifact | None:
        ir_root = Path(outputs_root).expanduser() / self.tenant_id / self.repo_id / ".akc" / "ir"
        if not ir_root.exists():
            return None
        candidates = sorted(
            [p for p in ir_root.glob("*.json") if p.name != f"{plan_id}.json"],
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for fp in candidates:
            try:
                import json

                raw = json.loads(fp.read_text(encoding="utf-8"))
                if not isinstance(raw, dict):
                    continue
                prev = IRDocument.from_json_obj(raw)
                d = diff_ir(before=prev, after=ir_doc)
                return OutputArtifact.from_json(
                    path=f".akc/ir/{plan_id}.diff.json",
                    obj={
                        "before_ir_path": f".akc/ir/{fp.stem}.json",
                        "after_ir_path": f".akc/ir/{plan_id}.json",
                        "diff": d.to_json_obj(),
                    },
                    metadata={"plan_id": plan_id, "kind": "ir_diff"},
                )
            except Exception:
                continue
        return None

    def _build_run_log(self, *, run_manifest: RunManifest) -> str:
        costs = dict(run_manifest.cost_attribution or {})
        cp = run_manifest.control_plane or {}
        raw_pd = cp.get("policy_decisions", [])
        pd_seq: list[object] = list(raw_pd) if isinstance(raw_pd, list) else []
        policy_decisions = [dict(x) for x in pd_seq if isinstance(x, dict)]
        denied = [d for d in policy_decisions if not bool(d.get("allowed", False))]
        lines: list[str] = [
            f"run_id={run_manifest.run_id}",
            f"tenant_id={run_manifest.tenant_id}",
            f"repo_id={run_manifest.repo_id}",
            f"replay_mode={run_manifest.replay_mode}",
            f"model={run_manifest.model or 'unknown'}",
            f"passes={','.join([p.name + ':' + p.status for p in run_manifest.passes])}",
            (
                "costs="
                f"llm_calls={costs.get('llm_calls', 0)} "
                f"tool_calls={costs.get('tool_calls', 0)} "
                f"input_tokens={costs.get('input_tokens', 0)} "
                f"output_tokens={costs.get('output_tokens', 0)} "
                f"total_tokens={costs.get('total_tokens', 0)} "
                f"estimated_cost_usd={costs.get('estimated_cost_usd', 0.0)} "
                f"wall_time_ms={costs.get('wall_time_ms', 0)}"
            ),
            f"trace_spans={len(run_manifest.trace_spans)}",
            f"policy_decisions={len(policy_decisions)} denied={len(denied)}",
        ]
        for item in denied[:5]:
            lines.append(
                "policy_deny="
                f"action={item.get('action', '')} "
                f"reason={item.get('reason', '')} "
                f"tenant_id={((item.get('scope') or {}).get('tenant_id', ''))} "
                f"repo_id={((item.get('scope') or {}).get('repo_id', ''))}"
            )
        return "\n".join(lines) + "\n"

    def _write_and_read_tenant_cost_totals(
        self,
        *,
        outputs_root: str | Path,
        run_id: str,
        repo_id: str,
        current_run_costs: dict[str, Any],
    ) -> dict[str, JSONValue]:
        """Persist current run costs and query tenant totals via control-plane index."""

        metrics_db = Path(outputs_root).expanduser() / self.tenant_id / ".akc" / "control" / "metrics.sqlite"
        index = CostIndex(sqlite_path=metrics_db)
        index.upsert_run_cost(
            record=RunCostRecord(
                tenant_id=self.tenant_id,
                repo_id=repo_id,
                run_id=run_id,
                llm_calls=int(current_run_costs.get("llm_calls", 0)),
                tool_calls=int(current_run_costs.get("tool_calls", 0)),
                input_tokens=int(current_run_costs.get("input_tokens", 0)),
                output_tokens=int(current_run_costs.get("output_tokens", 0)),
                total_tokens=int(current_run_costs.get("total_tokens", 0)),
                wall_time_ms=int(current_run_costs.get("wall_time_ms", 0)),
                estimated_cost_usd=float(current_run_costs.get("estimated_cost_usd", 0.0)),
                pricing_version=(
                    str(current_run_costs.get("pricing_version"))
                    if current_run_costs.get("pricing_version") is not None
                    else None
                ),
                cost_breakdown={
                    "currency": current_run_costs.get("currency"),
                    "pricing_version": current_run_costs.get("pricing_version"),
                    "by_pass": current_run_costs.get("by_pass"),
                    "by_component": current_run_costs.get("by_component"),
                },
            )
        )
        return index.tenant_totals(tenant_id=self.tenant_id)
