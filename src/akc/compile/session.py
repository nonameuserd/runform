"""Compile boundary session (Phase 2).

This module intentionally provides a small "boundary object" that binds a
tenant+repo scope with the Memory layer stores, so the eventual Phase 3
controller can call a cohesive API without redesigning store interfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.artifacts.contracts import ARTIFACT_SCHEMA_VERSION, apply_schema_envelope
from akc.compile.controller import ControllerResult, run_compile_loop
from akc.compile.controller_config import ControllerConfig
from akc.compile.executors import SubprocessExecutor
from akc.compile.interfaces import Executor, Index, LLMBackend, TenantRepoScope
from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.planner import advance_plan, create_or_resume_plan
from akc.compile.retriever import retrieve_context
from akc.control.cost_index import CostIndex, RunCostRecord
from akc.ir import IRDocument, diff_ir
from akc.memory.facade import Memory, MemoryBackend, build_memory
from akc.memory.models import JSONValue, PlanState, normalize_repo_id, require_non_empty
from akc.memory.why_graph import WhyGraphStore
from akc.outputs.emitters import Emitter, JsonManifestEmitter
from akc.outputs.models import OutputArtifact, OutputBundle
from akc.run import PassRecord, ReplayMode, RetrievalSnapshot, RunManifest
from akc.run.loader import find_latest_run_manifest, load_run_manifest
from akc.utils.fingerprint import stable_json_fingerprint


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

    def retrieve(self, *, plan: PlanState, limit: int = 20) -> dict[str, Any]:
        """Retrieve context from code memory (+ optionally why-graph) for this session."""

        return retrieve_context(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            plan=plan,
            code_memory=self.memory.code_memory,
            why_graph=self.memory.why_graph,
            index=self.index,
            limit=limit,
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
        replay_mode: ReplayMode = "live",
        replay_manifest_path: str | Path | None = None,
        partial_replay_passes: tuple[str, ...] | None = None,
    ) -> ControllerResult:
        """Run the Phase 3 compile loop for this tenant+repo scope."""

        plan = self.plan(goal=goal)
        # Ensure the plan exists and is active before running.
        self.memory.plan_state.set_active_plan(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            plan_id=plan.id,
        )
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
                        run_id=plan.id,
                        tenant_id=self.tenant_id,
                        repo_id=self.repo_id,
                        ir_sha256="0" * 64,
                        replay_mode="partial_replay",
                        partial_replay_passes=cleaned,
                    )

        result = run_compile_loop(
            tenant_id=self.tenant_id,
            repo_id=self.repo_id,
            goal=goal,
            plan_store=self.memory.plan_state,
            code_memory=self.memory.code_memory,
            why_graph=self.memory.why_graph,
            index=self.index,
            llm=llm,
            executor=executor or SubprocessExecutor(),
            config=config,
            replay_mode=replay_mode,
            replay_manifest=effective_replay_manifest,
        )

        # Phase 4 Outputs integration (minimal): on success, emit a scoped manifest
        # containing the best patch candidate (and optional test output when available).
        #
        # This keeps the controller focused on the ARCS loop and makes emission opt-in
        # by requiring `outputs_root`.
        if outputs_root is not None:
            scope = TenantRepoScope(tenant_id=result.plan.tenant_id, repo_id=result.plan.repo_id)
            step_id = (
                result.plan.last_feedback.get("step_id")
                if isinstance(result.plan.last_feedback, dict)
                else None
            )
            step_id_s = str(step_id) if step_id is not None else "unknown_step"
            step = next((s for s in result.plan.steps if s.id == step_id_s), None)
            step_outputs = dict(step.outputs or {}) if step is not None else {}
            ir_doc = self._ir_from_plan(plan=result.plan)

            retrieval_snapshots = self._build_retrieval_snapshots(step_outputs=step_outputs)
            passes = self._build_pass_records(
                result=result,
                step_outputs=step_outputs,
            )
            run_manifest = RunManifest(
                run_id=result.plan.id,
                tenant_id=result.plan.tenant_id,
                repo_id=result.plan.repo_id,
                ir_sha256=ir_doc.fingerprint(),
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
                    effective_replay_manifest.partial_replay_passes
                    if effective_replay_manifest is not None
                    else ()
                ),
                llm_vcr=self._build_llm_vcr(passes=passes),
                budgets=config.budget.to_json_obj(),
                output_hashes=self._build_output_hashes(
                    result=result,
                    plan_id=result.plan.id,
                    step_id=step_id_s,
                    ir_doc=ir_doc,
                ),
                trace_spans=tuple(
                    dict(x) for x in result.accounting.get("trace_spans", []) if isinstance(x, dict)
                ),
                control_plane={
                    "policy_decisions": [
                        dict(x)
                        for x in result.accounting.get("policy_decisions", [])
                        if isinstance(x, dict)
                    ]
                },
                cost_attribution={
                    "tenant_id": self.tenant_id,
                    "repo_id": self.repo_id,
                    "run_id": result.plan.id,
                    "llm_calls": int(result.accounting.get("llm_calls", 0)),
                    "tool_calls": int(result.accounting.get("tool_calls", 0)),
                    "input_tokens": int(result.accounting.get("input_tokens", 0)),
                    "output_tokens": int(result.accounting.get("output_tokens", 0)),
                    "total_tokens": int(result.accounting.get("total_tokens", 0)),
                    "estimated_cost_usd": float(result.accounting.get("estimated_cost_usd", 0.0)),
                    "repair_iterations": int(result.accounting.get("repair_iterations", 0)),
                    "wall_time_ms": int(result.accounting.get("wall_time_ms", 0)),
                    "budget": config.budget.to_json_obj(),
                    "cost_rates": config.cost_rates.to_json_obj(),
                    "tenant_totals": self._write_and_read_tenant_cost_totals(
                        outputs_root=outputs_root,
                        run_id=result.plan.id,
                        repo_id=self.repo_id,
                        current_run_costs=result.accounting,
                    ),
                },
            )

            artifacts: list[OutputArtifact] = []
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/ir/{result.plan.id}.json",
                    obj=ir_doc.to_json_obj(),
                    metadata={"plan_id": result.plan.id, "fingerprint": ir_doc.fingerprint()},
                )
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/run/{result.plan.id}.spans.json",
                    obj={
                        "run_id": result.plan.id,
                        "tenant_id": self.tenant_id,
                        "repo_id": self.repo_id,
                        "spans": [dict(x) for x in run_manifest.trace_spans],
                    },
                    metadata={"plan_id": result.plan.id, "kind": "trace_spans"},
                )
            )
            artifacts.append(
                OutputArtifact.from_json(
                    path=f".akc/run/{result.plan.id}.costs.json",
                    obj=(dict(run_manifest.cost_attribution or {})),
                    metadata={"plan_id": result.plan.id, "kind": "cost_attribution"},
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

            ir_diff = self._build_ir_diff_artifact(
                outputs_root=outputs_root,
                plan_id=result.plan.id,
                ir_doc=ir_doc,
            )
            if ir_diff is not None:
                artifacts.append(ir_diff)
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
                apply_schema_envelope(
                    obj=stage_obj, kind="execution_stage", version=int(schema_version)
                )
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
                                    "stage": getattr(
                                        result.best_candidate, "execution_stage", None
                                    ),
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

            bundle = OutputBundle(scope=scope, name="compile_session", artifacts=tuple(artifacts))
            (emitter or JsonManifestEmitter()).emit(bundle=bundle, root=outputs_root)

        return result

    def _ir_from_plan(self, *, plan: PlanState) -> IRDocument:
        # Keep session IR emission consistent with the controller and recompile
        # logic by delegating to the shared builder.
        return build_ir_document_from_plan(plan=plan)

    def _build_retrieval_snapshots(
        self, *, step_outputs: dict[str, Any]
    ) -> list[RetrievalSnapshot]:
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

    def _build_pass_records(
        self, *, result: ControllerResult, step_outputs: dict[str, Any]
    ) -> list[PassRecord]:
        status: Literal["succeeded", "failed"] = (
            "succeeded" if result.status == "succeeded" else "failed"
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
                    if int(result.accounting.get("llm_calls", 0)) > 0
                    or result.best_candidate is not None
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
        out.append(PassRecord(name="execute", status=status, metadata=exec_md))
        if int(result.accounting.get("repair_iterations", 0)) > 0:
            out.append(PassRecord(name="repair", status=status))
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

        metrics_db = (
            Path(outputs_root).expanduser() / self.tenant_id / ".akc" / "control" / "metrics.sqlite"
        )
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
            )
        )
        return index.tenant_totals(tenant_id=self.tenant_id)
