"""Compile boundary session (Phase 2).

This module intentionally provides a small "boundary object" that binds a
tenant+repo scope with the Memory layer stores, so the eventual Phase 3
controller can call a cohesive API without redesigning store interfaces.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.compile.planner import advance_plan, create_or_resume_plan
from akc.compile.retriever import retrieve_context
from akc.compile.controller import run_compile_loop, ControllerResult
from akc.compile.controller_config import ControllerConfig
from akc.compile.interfaces import Index
from akc.compile.interfaces import Executor, LLMBackend
from akc.compile.interfaces import TenantRepoScope
from akc.compile.executors import SubprocessExecutor
from akc.memory.facade import Memory, MemoryBackend, build_memory
from akc.memory.models import PlanState, normalize_repo_id, require_non_empty
from akc.memory.why_graph import WhyGraphStore
from akc.outputs.emitters import Emitter, JsonManifestEmitter
from akc.outputs.models import OutputArtifact, OutputBundle


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

        return cls.from_backend(tenant_id=tenant_id, repo_id=repo_id, backend="memory", index=index)

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
        emitter: Emitter | None = None,
    ) -> ControllerResult:
        """Run the Phase 3 compile loop for this tenant+repo scope."""

        plan = self.plan(goal=goal)
        # Ensure the plan exists and is active before running.
        self.memory.plan_state.set_active_plan(tenant_id=self.tenant_id, repo_id=self.repo_id, plan_id=plan.id)
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
        )

        # Phase 4 Outputs integration (minimal): on success, emit a scoped manifest
        # containing the best patch candidate (and optional test output when available).
        #
        # This keeps the controller focused on the ARCS loop and makes emission opt-in
        # by requiring `outputs_root`.
        if outputs_root is not None and result.status == "succeeded" and result.best_candidate is not None:
            scope = TenantRepoScope(tenant_id=result.plan.tenant_id, repo_id=result.plan.repo_id)
            step_id = result.plan.last_feedback.get("step_id") if isinstance(result.plan.last_feedback, dict) else None
            step_id_s = str(step_id) if step_id is not None else "unknown_step"
            step = next((s for s in result.plan.steps if s.id == step_id_s), None)
            step_outputs = dict(step.outputs or {}) if step is not None else {}

            artifacts: list[OutputArtifact] = [
                OutputArtifact.from_text(
                    path=f".akc/patches/{result.plan.id}_{step_id_s}.diff",
                    text=result.best_candidate.llm_text,
                    media_type="text/x-diff; charset=utf-8",
                    metadata={"plan_id": result.plan.id, "step_id": step_id_s},
                )
            ]

            def _emit_stage(*, name: str, payload: dict[str, object]) -> None:
                stdout = str(payload.get("stdout") or "")
                stderr = str(payload.get("stderr") or "")
                combined = (stdout + ("\n" + stderr if stderr else "")) or "(no output)"
                artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/tests/{result.plan.id}_{step_id_s}.{name}.stdout.txt",
                        text=stdout or "(no stdout)",
                        media_type="text/plain; charset=utf-8",
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s, "stage": name, "stream": "stdout"},
                    )
                )
                artifacts.append(
                    OutputArtifact.from_text(
                        path=f".akc/tests/{result.plan.id}_{step_id_s}.{name}.stderr.txt",
                        text=stderr or "(no stderr)",
                        media_type="text/plain; charset=utf-8",
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s, "stage": name, "stream": "stderr"},
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
                artifacts.append(
                    OutputArtifact.from_json(
                        path=f".akc/tests/{result.plan.id}_{step_id_s}.{name}.json",
                        obj={
                            "plan_id": result.plan.id,
                            "step_id": step_id_s,
                            "stage": payload.get("stage"),
                            "command": list(payload.get("command") or []),  # type: ignore[list-item]
                            "exit_code": payload.get("exit_code"),
                            "duration_ms": payload.get("duration_ms"),
                        },
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s, "stage": name},
                    )
                )

            if result.best_candidate.execution is not None:
                # Prefer step outputs when available (smoke+full); otherwise fall back to best_candidate.
                last_smoke = step_outputs.get("last_tests_smoke")
                last_full = step_outputs.get("last_tests_full")
                if isinstance(last_smoke, dict):
                    _emit_stage(name="smoke", payload=last_smoke)  # type: ignore[arg-type]
                if isinstance(last_full, dict):
                    _emit_stage(name="full", payload=last_full)  # type: ignore[arg-type]

                if not isinstance(last_smoke, dict) and not isinstance(last_full, dict):
                    stdout = result.best_candidate.execution.stdout or ""
                    stderr = result.best_candidate.execution.stderr or ""
                    combined = stdout + ("\n" + stderr if stderr else "")
                    artifacts.append(
                        OutputArtifact.from_text(
                            path=f".akc/tests/{result.plan.id}_{step_id_s}.stdout.txt",
                            text=stdout or "(no stdout)",
                            media_type="text/plain; charset=utf-8",
                            metadata={"plan_id": result.plan.id, "step_id": step_id_s, "stream": "stdout"},
                        )
                    )
                    artifacts.append(
                        OutputArtifact.from_text(
                            path=f".akc/tests/{result.plan.id}_{step_id_s}.stderr.txt",
                            text=stderr or "(no stderr)",
                            media_type="text/plain; charset=utf-8",
                            metadata={"plan_id": result.plan.id, "step_id": step_id_s, "stream": "stderr"},
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
                            obj={
                                "plan_id": result.plan.id,
                                "step_id": step_id_s,
                                "stage": getattr(result.best_candidate, "execution_stage", None),
                                "command": list(getattr(result.best_candidate, "execution_command", None) or []),
                                "exit_code": int(result.best_candidate.execution.exit_code),
                                "duration_ms": result.best_candidate.execution.duration_ms,
                            },
                            metadata={"plan_id": result.plan.id, "step_id": step_id_s},
                        )
                    )

            last_ver = step_outputs.get("last_verification")
            if isinstance(last_ver, dict):
                artifacts.append(
                    OutputArtifact.from_json(
                        path=f".akc/verification/{result.plan.id}_{step_id_s}.json",
                        obj=last_ver,  # already structured
                        metadata={"plan_id": result.plan.id, "step_id": step_id_s},
                    )
                )

            bundle = OutputBundle(scope=scope, name="compile_session", artifacts=tuple(artifacts))
            (emitter or JsonManifestEmitter()).emit(bundle=bundle, root=outputs_root)

        return result

