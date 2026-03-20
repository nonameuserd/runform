"""Phase 3 ARCS-style budgeted tiered controller.

This module orchestrates Plan → Retrieve → Generate → Execute → Repair.

Design goals:
- Enforce tenant+repo isolation by threading scope everywhere.
- Enforce a conservative budget (LLM calls, repair iterations, wall time).
- Persist progress into PlanState (step status + best candidate + accounting).
"""

from __future__ import annotations

import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Any, Literal, cast

from akc.compile.controller_config import ControllerConfig, TierConfig
from akc.compile.executors import StageRunResult, run_stage
from akc.compile.interfaces import (
    ExecutionResult,
    Executor,
    LLMBackend,
    TenantRepoScope,
)
from akc.compile.ir_builder import build_ir_document_from_plan
from akc.compile.ir_passes import DefaultIRGeneratePromptPass, DefaultIRRepairPromptPass
from akc.compile.patch_emitter import (
    ModelCallNeeded,
    ResolvedPatch,
    StageName,
    candidate_from_patch_text,
    patch_sha256_hex,
    resolve_patch_candidate_from_prompt,
)
from akc.compile.planner import advance_plan
from akc.compile.provenance_mapper import build_retrieval_documents_item_ids_and_provenance
from akc.compile.repair import parse_execution_failure
from akc.compile.retriever import retrieve_context
from akc.compile.rust_bridge import RustExecConfig
from akc.compile.verifier import DeterministicVerifier, VerifierPolicy
from akc.control.policy import (
    CapabilityAttenuator,
    CapabilityIssuer,
    DefaultDenyPolicyEngine,
    PolicyEngine,
    PolicyWrappedExecutor,
    PolicyWrappedLLMBackend,
    SubprocessOpaEvaluator,
    ToolAuthorizationError,
    ToolAuthorizationPolicy,
)
from akc.control.tracing import TraceSpan, new_span_id, new_trace_id, now_unix_nano
from akc.memory.code_memory import make_item
from akc.memory.models import (
    JSONValue,
    PlanState,
    PlanStep,
    PlanStepStatus,
    now_ms,
    require_non_empty,
)
from akc.memory.plan_state import PlanStateStore
from akc.run.manifest import ReplayMode, RunManifest
from akc.run.replay import decide_replay_for_pass

RunStatus = Literal["succeeded", "failed", "budget_exhausted"]


@dataclass(frozen=True, slots=True)
class Candidate:
    """A single generate→execute attempt."""

    tier: str
    stage: str
    llm_text: str
    touched_paths: tuple[str, ...]
    test_paths: tuple[str, ...]
    execution: ExecutionResult | None
    execution_stage: str | None
    execution_command: list[str] | None
    score: int
    attempt_idx: int
    created_at_ms: int

    def to_json_obj(self) -> dict[str, Any]:
        exe = None
        if self.execution is not None:
            exe = {
                "stage": self.execution_stage,
                "command": list(self.execution_command or []),
                "exit_code": int(self.execution.exit_code),
                "stdout": self.execution.stdout,
                "stderr": self.execution.stderr,
                "duration_ms": self.execution.duration_ms,
            }
        return {
            "tier": self.tier,
            "stage": self.stage,
            "llm_text": self.llm_text,
            "touched_paths": list(self.touched_paths),
            "test_paths": list(self.test_paths),
            "has_test_changes": bool(self.test_paths),
            "execution": exe,
            "score": int(self.score),
            "attempt_idx": int(self.attempt_idx),
            "created_at_ms": int(self.created_at_ms),
        }


@dataclass(frozen=True, slots=True)
class ControllerResult:
    status: RunStatus
    plan: PlanState
    best_candidate: Candidate | None
    accounting: dict[str, Any]


def _platform_label() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "darwin"
    if sys.platform in {"win32", "cygwin"}:
        return "windows"
    return sys.platform


def _docker_apparmor_available() -> bool:
    if _platform_label() != "linux":
        return False
    try:
        from pathlib import Path

        return (
            Path("/sys/module/apparmor/parameters/enabled")
            .read_text(encoding="utf-8")
            .strip()
            .lower()
            .startswith("y")
        )
    except OSError:
        return False


def _unwrap_executor_for_policy(executor: Executor) -> Any:
    underlying = getattr(executor, "underlying", None)
    return underlying if underlying is not None else executor


def _executor_backend_label(executor: Executor) -> str:
    effective = _unwrap_executor_for_policy(executor)
    rust_cfg = getattr(effective, "rust_cfg", None)
    if isinstance(rust_cfg, RustExecConfig):
        return "wasm" if rust_cfg.lane == "wasm" else "process"
    cls_name = effective.__class__.__name__.lower()
    if "docker" in cls_name:
        return "docker"
    if "subprocess" in cls_name:
        return "process"
    return "unknown"


def _wasm_policy_context(
    *,
    executor: Executor,
    timeout_s: float | None,
    policy_mode: str,
    metadata: dict[str, Any],
) -> dict[str, Any] | None:
    effective = _unwrap_executor_for_policy(executor)
    rust_cfg = getattr(effective, "rust_cfg", None)
    if not isinstance(rust_cfg, RustExecConfig) or rust_cfg.lane != "wasm":
        return None

    network_exception_raw = metadata.get("wasm_network_exception")
    if network_exception_raw is None:
        network_exception_raw = metadata.get("policy_wasm_network_exception")
    network_exception = str(network_exception_raw).strip() if network_exception_raw else ""
    strict_profile = policy_mode == "enforce" or bool(rust_cfg.wasm_normalization_strict)
    writable_preopen_dirs = list(rust_cfg.allowed_write_paths)
    writable_preopen_dir_set = set(writable_preopen_dirs)
    read_only_preopen_dirs = [
        path for path in rust_cfg.preopen_dirs if path not in writable_preopen_dir_set
    ]

    required_controls = [
        name
        for name, enabled in (
            ("wall_time_ms", timeout_s is not None),
            ("memory_bytes", rust_cfg.memory_bytes is not None),
            ("cpu_fuel", rust_cfg.cpu_fuel is not None),
            ("stdout_max_bytes", rust_cfg.stdout_max_bytes is not None),
            ("stderr_max_bytes", rust_cfg.stderr_max_bytes is not None),
        )
        if enabled
    ]
    unsupported_controls: list[str] = []
    if _platform_label() == "windows" and strict_profile and timeout_s is not None:
        unsupported_controls.append("wall_time_ms")

    limits = {
        "wall_time_ms": int(timeout_s * 1000.0) if timeout_s is not None else None,
        "memory_bytes": rust_cfg.memory_bytes,
        "cpu_fuel": rust_cfg.cpu_fuel,
        "stdout_max_bytes": rust_cfg.stdout_max_bytes,
        "stderr_max_bytes": rust_cfg.stderr_max_bytes,
    }
    return {
        "network_enabled": bool(rust_cfg.allow_network),
        "network_mode": "enabled" if bool(rust_cfg.allow_network) else "none",
        "network_exception": network_exception or None,
        "preopen_dirs": list(rust_cfg.preopen_dirs),
        "writable_preopen_dirs": writable_preopen_dirs,
        "read_only_preopen_dirs": read_only_preopen_dirs,
        "limits": limits,
        "limits_tuple": [
            limits["wall_time_ms"],
            limits["memory_bytes"],
            limits["cpu_fuel"],
            limits["stdout_max_bytes"],
            limits["stderr_max_bytes"],
        ],
        "platform_capability_profile": {
            "os": _platform_label(),
            "profile": "strict" if strict_profile else "relaxed",
            "required_controls": required_controls,
            "unsupported_controls": unsupported_controls,
        },
    }


def _docker_policy_context(
    *,
    executor: Executor,
    metadata: dict[str, Any],
) -> dict[str, Any] | None:
    effective = _unwrap_executor_for_policy(executor)
    if "docker" not in effective.__class__.__name__.lower():
        return None

    user_raw = getattr(effective, "user", None)
    user = str(user_raw).strip() if user_raw is not None else ""
    seccomp_raw = getattr(effective, "seccomp_profile", None)
    seccomp_profile = str(seccomp_raw).strip() if seccomp_raw is not None else ""
    apparmor_raw = getattr(effective, "apparmor_profile", None)
    apparmor_profile = str(apparmor_raw).strip() if apparmor_raw is not None else ""
    ulimit_nofile_raw = getattr(effective, "ulimit_nofile", None)
    ulimit_nofile = str(ulimit_nofile_raw).strip() if ulimit_nofile_raw is not None else ""
    ulimit_nproc_raw = getattr(effective, "ulimit_nproc", None)
    ulimit_nproc = str(ulimit_nproc_raw).strip() if ulimit_nproc_raw is not None else ""
    network_exception_raw = metadata.get("docker_network_exception")
    if network_exception_raw is None:
        network_exception_raw = metadata.get("policy_docker_network_exception")
    network_exception = str(network_exception_raw).strip() if network_exception_raw else ""

    network_enabled = not bool(getattr(effective, "disable_network", False))
    apparmor_available = _docker_apparmor_available()
    effective_seccomp_profile = seccomp_profile or "runtime/default"
    effective_apparmor_profile = apparmor_profile or (
        "docker-default" if apparmor_available else ""
    )
    return {
        "network_enabled": network_enabled,
        "network_mode": "enabled" if network_enabled else "none",
        "network_exception": network_exception or None,
        "read_only_rootfs": bool(getattr(effective, "read_only_rootfs", False)),
        "no_new_privileges": bool(getattr(effective, "no_new_privileges", False)),
        "cap_drop_all": bool(getattr(effective, "cap_drop_all", False)),
        "user": user or None,
        "user_present": bool(user),
        "user_is_non_root": bool(user) and user not in {"0", "0:0", "root", "root:root"},
        "security_profiles": {
            "seccomp": effective_seccomp_profile,
            "apparmor": effective_apparmor_profile or None,
        },
        "platform": {
            "os": _platform_label(),
            "apparmor_available": apparmor_available,
        },
        "limits": {
            "memory_bytes": getattr(effective, "memory_bytes", None),
            "pids_limit": getattr(effective, "pids_limit", None),
            "cpus": getattr(effective, "cpus", None),
            "ulimit_nofile": ulimit_nofile or None,
            "ulimit_nproc": ulimit_nproc or None,
        },
        "tmpfs_mounts": list(getattr(effective, "tmpfs_mounts", ()) or ()),
    }


def _build_executor_policy_context(
    *,
    executor: Executor,
    stage: str,
    command: list[str],
    timeout_s: float | None,
    replay_mode: ReplayMode,
    policy_mode: str,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    backend = _executor_backend_label(executor)
    ctx: dict[str, Any] = {
        "stage": stage,
        "command": list(command),
        "replay_mode": replay_mode,
        "backend": backend,
    }
    wasm_ctx = _wasm_policy_context(
        executor=executor,
        timeout_s=timeout_s,
        policy_mode=policy_mode,
        metadata=metadata,
    )
    if wasm_ctx is not None:
        ctx["wasm"] = wasm_ctx
    docker_ctx = _docker_policy_context(executor=executor, metadata=metadata)
    if docker_ctx is not None:
        ctx["docker"] = docker_ctx
    return ctx


def _tier_order(name: str) -> int:
    if name == "small":
        return 0
    if name == "medium":
        return 1
    if name == "large":
        return 2
    return 99


def _escalate_tier(*, current: TierConfig, config: ControllerConfig) -> TierConfig:
    """Escalate tier conservatively: small→medium→large (if available)."""

    tiers = sorted(config.tiers.values(), key=lambda t: _tier_order(t.name))
    for idx, t in enumerate(tiers):
        if t.name == current.name:
            if idx + 1 < len(tiers):
                return tiers[idx + 1]
            return current
    return current


def _best_of(a: Candidate | None, b: Candidate) -> Candidate:
    if a is None:
        return b
    # Higher score wins; tie-break deterministically on attempt_idx.
    if b.score > a.score:
        return b
    if b.score < a.score:
        return a
    return b if b.attempt_idx >= a.attempt_idx else a


def _extract_patch_paths(patch_text: str) -> list[str]:
    """Extract touched file paths from a unified diff.

    Best-effort and deterministic: returns stable sorted unique paths.
    """
    paths: set[str] = set()
    for raw in (patch_text or "").splitlines():
        line = raw.strip()
        # Common forms:
        # --- a/foo.py
        # +++ b/foo.py
        if line.startswith("+++ "):
            p = line[4:].strip()
            if p.startswith("b/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
        elif line.startswith("--- "):
            p = line[4:].strip()
            if p.startswith("a/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
    return sorted(paths)


def _is_test_path(p: str) -> bool:
    p2 = str(p or "").replace("\\", "/")
    parts = [seg for seg in p2.split("/") if seg]
    if not parts:
        return False
    if parts[0] in {"test", "tests"}:
        return True
    leaf = parts[-1]
    if leaf.startswith("test_") and leaf.endswith(".py"):
        return True
    return bool(leaf.endswith("_test.py"))


def _policy_requires_tests(
    *,
    touched_paths: list[str],
    require_tests_for_non_test_changes: bool,
) -> tuple[bool, dict[str, Any]]:
    """Return (ok, evidence) for the tests-generated-by-default heuristic."""

    tests = [p for p in touched_paths if _is_test_path(p)]
    non_tests = [p for p in touched_paths if not _is_test_path(p)]
    if not require_tests_for_non_test_changes:
        return True, {
            "touched_paths": touched_paths,
            "test_paths": tests,
            "non_test_paths": non_tests,
        }
    # Only require tests if the patch touches at least one non-test path.
    if non_tests and not tests:
        return False, {
            "touched_paths": touched_paths,
            "test_paths": tests,
            "non_test_paths": non_tests,
        }
    return True, {
        "touched_paths": touched_paths,
        "test_paths": tests,
        "non_test_paths": non_tests,
    }


def _score_execution(result: ExecutionResult | None) -> int:
    if result is None:
        return 0
    # Simple monotone scoring: pass >> fail. Keep it stable for tests.
    return 1000 if int(result.exit_code) == 0 else 10


def _score_candidate(
    *,
    execution: ExecutionResult | None,
    ok_tests_policy: bool,
    promotable: bool,
    verifier_passed: bool | None,
) -> int:
    """Deterministic scoring for monotonic repair progress.

    We intentionally incorporate policy + verifier outcomes so that a repair
    candidate can be considered "improving" even when the test exit code
    stays the same (e.g. verifier vetoes due to patch safety issues).
    """

    base = _score_execution(execution)
    if base != 1000:
        return base

    # Differentiate smoke-only pass vs full promotable pass.
    if not promotable:
        return 950

    if not ok_tests_policy:
        return 900

    # verifier_passed is only meaningful when promotable and ok_tests_policy.
    if verifier_passed is False:
        return 800

    return 1000


def _estimate_token_count(text: str) -> int:
    # Deterministic heuristic fallback when provider usage isn't available.
    s = str(text or "")
    if not s:
        return 0
    return max(1, len(s) // 4)


def _estimate_cost_usd(
    *,
    input_tokens: int,
    output_tokens: int,
    tool_calls: int,
    input_per_1k_tokens_usd: float,
    output_per_1k_tokens_usd: float,
    tool_call_usd: float,
) -> float:
    """Estimate run cost from explicit rate configuration."""

    return (
        (float(max(0, int(input_tokens))) / 1000.0) * float(input_per_1k_tokens_usd)
        + (float(max(0, int(output_tokens))) / 1000.0) * float(output_per_1k_tokens_usd)
        + float(max(0, int(tool_calls))) * float(tool_call_usd)
    )


def _derive_full_test_command(smoke_command: list[str]) -> list[str]:
    """Derive a 'full' test command from a smoke command deterministically.

    Policy:
    - If '-q' is present, drop it (common 'smoke' speed + noiseless mode).
    - Otherwise keep the command as-is.
    """

    if not smoke_command:
        raise ValueError("smoke_command must be non-empty")
    return [c for c in smoke_command if c != "-q"]


def _update_step(
    *,
    plan: PlanState,
    step_id: str,
    mutate: Callable[[PlanStep], PlanStep],
) -> PlanState:
    require_non_empty(step_id, name="step_id")
    steps2: list[PlanStep] = []
    found = False
    for s in plan.steps:
        if s.id != step_id:
            steps2.append(s)
            continue
        steps2.append(mutate(s))
        found = True
    if not found:
        raise ValueError("step not found")
    return replace(plan, steps=tuple(steps2), updated_at_ms=now_ms())


def _set_step_outputs(
    *,
    plan: PlanState,
    step_id: str,
    outputs_patch: dict[str, Any],
) -> PlanState:
    def _mutate(s: PlanStep) -> PlanStep:
        out = dict(s.outputs or {})
        out.update(dict(outputs_patch))
        return replace(s, outputs=out)

    return _update_step(plan=plan, step_id=step_id, mutate=_mutate)


def _set_step_status(
    *,
    plan: PlanState,
    step_id: str,
    status: PlanStepStatus,
    notes: str | None = None,
) -> PlanState:
    t = now_ms()

    def _mutate(s: PlanStep) -> PlanStep:
        started = s.started_at_ms
        finished = s.finished_at_ms
        if status == "in_progress" and started is None:
            started = t
        if status in {"done", "failed", "skipped"} and finished is None:
            finished = t
        return PlanStep(
            id=s.id,
            title=s.title,
            status=status,
            order_idx=s.order_idx,
            started_at_ms=started,
            finished_at_ms=finished,
            notes=notes if notes is not None else s.notes,
            inputs=s.inputs,
            outputs=s.outputs,
        )

    return _update_step(plan=plan, step_id=step_id, mutate=_mutate)


def _manifest_pass_metadata(
    *, replay_manifest: RunManifest | None, pass_name: str
) -> dict[str, Any] | None:
    if replay_manifest is None:
        return None
    for rec in replay_manifest.passes:
        if rec.name == pass_name and isinstance(rec.metadata, dict):
            return dict(rec.metadata)
    return None


def _cached_execution_stage(
    *, plan: PlanState, step_id: str
) -> tuple[str, ExecutionResult, list[str]] | None:
    step = next((s for s in plan.steps if s.id == step_id), None)
    if step is None:
        return None
    outputs = dict(step.outputs or {})
    for key in ("last_tests_full", "last_tests_smoke"):
        raw = outputs.get(key)
        if not isinstance(raw, dict):
            continue
        stage = str(raw.get("stage") or "")
        command_raw = raw.get("command")
        command = [str(x) for x in command_raw] if isinstance(command_raw, list) else []
        try:
            exit_code_raw = raw.get("exit_code")
            if exit_code_raw is None:
                continue
            duration_raw = raw.get("duration_ms")
            result = ExecutionResult(
                exit_code=int(exit_code_raw),
                stdout=str(raw.get("stdout") or ""),
                stderr=str(raw.get("stderr") or ""),
                duration_ms=(int(duration_raw) if duration_raw is not None else None),
            )
        except Exception:
            continue
        return stage, result, command
    return None


def _replayed_execution_stage(
    *, plan: PlanState, step_id: str, replay_manifest: RunManifest | None
) -> tuple[str, ExecutionResult, list[str]] | None:
    cached = _cached_execution_stage(plan=plan, step_id=step_id)
    if cached is not None:
        return cached
    md = _manifest_pass_metadata(replay_manifest=replay_manifest, pass_name="execute")
    if md is None:
        return None
    stage = str(md.get("stage") or "tests_full")
    cmd_raw = md.get("command")
    command = [str(x) for x in cmd_raw] if isinstance(cmd_raw, list) else []
    try:
        exit_code_raw = md.get("exit_code")
        if exit_code_raw is None:
            return None
        duration_raw = md.get("duration_ms")
        result = ExecutionResult(
            exit_code=int(exit_code_raw),
            stdout=str(md.get("stdout") or ""),
            stderr=str(md.get("stderr") or ""),
            duration_ms=(int(duration_raw) if duration_raw is not None else None),
        )
    except Exception:
        return None
    return stage, result, command


def _replayed_retrieval_context(
    *,
    replay_manifest: RunManifest | None,
    goal: str,
) -> dict[str, Any] | None:
    if replay_manifest is None:
        return None
    snapshots = tuple(replay_manifest.retrieval_snapshots or ())
    if not snapshots:
        return None

    documents: list[dict[str, Any]] = []
    item_ids: list[str] = []
    for idx, snap in enumerate(snapshots):
        snap_item_ids = [str(x).strip() for x in snap.item_ids if str(x).strip()]
        item_ids.extend(snap_item_ids)
        for rank, item_id in enumerate(snap_item_ids):
            documents.append(
                {
                    "doc_id": item_id,
                    "title": f"replayed:{snap.source}:{idx}:{rank}",
                    # Keep replay prompt construction manifest-self-contained.
                    "content": (
                        f"replayed retrieval snapshot source={snap.source} "
                        f"query={snap.query} rank={rank}"
                    ),
                    "score": float(max(int(snap.top_k) - rank, 0)),
                    "metadata": {
                        "source_type": "retrieval_snapshot",
                        "source": snap.source,
                        "query": snap.query,
                        "rank": rank,
                        "replay": True,
                    },
                }
            )

    return {
        "code_memory_items": [
            {
                "item_id": item_id,
                "kind": "retrieval_snapshot",
                "artifact_id": replay_manifest.run_id,
                "payload": {"goal": goal, "replay_mode": replay_manifest.replay_mode},
            }
            for item_id in item_ids
        ],
        "documents": documents,
        "why_graph": {
            "replayed_from_manifest": True,
            "run_id": replay_manifest.run_id,
            "query_count": len(snapshots),
        },
    }


def run_compile_loop(
    *,
    tenant_id: str,
    repo_id: str,
    goal: str,
    plan_store: PlanStateStore,
    code_memory: Any,  # CodeMemoryStore (kept untyped here to avoid import cycle)
    why_graph: Any,
    index: Any,
    llm: LLMBackend,
    executor: Executor,
    config: ControllerConfig,
    replay_mode: ReplayMode = "live",
    replay_manifest: RunManifest | None = None,
    policy_engine: PolicyEngine | None = None,
) -> ControllerResult:
    """Run the Phase 3 compile loop for the active plan step.

    This is intentionally conservative and dependency-free: it does not apply patches
    to a working tree yet; instead it treats the LLM output as the artifact and uses
    the executor to validate it (tests, linters, etc.) based on the provided config.
    """

    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    require_non_empty(goal, name="goal")
    require_non_empty(replay_mode, name="replay_mode")
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)

    plan = advance_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan_store.get_active_plan_id(tenant_id=tenant_id, repo_id=repo_id)
        or plan_store.create_plan(tenant_id=tenant_id, repo_id=repo_id, goal=goal).id,
        plan_store=plan_store,
        feedback=None,
    )

    step_id = plan.next_step_id
    if step_id is None:
        return ControllerResult(status="succeeded", plan=plan, best_candidate=None, accounting={})

    plan = _set_step_status(plan=plan, step_id=step_id, status="in_progress")
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

    budget = config.budget
    start = time.monotonic()
    accounting: dict[str, Any] = {
        "llm_calls": 0,
        "tool_calls": 0,
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "repair_iterations": 0,
        "iterations_total": 0,
        "started_at_ms": now_ms(),
        "tier_history": [],
        "best_score": 0,
        "estimated_cost_usd": 0.0,
        "policy_mode": config.policy_mode,
        "policy_decisions": [],
        "trace_spans": [],
    }
    trace_id = new_trace_id()
    run_span_id = new_span_id()
    step_span_id = new_span_id()
    run_started_ns = now_unix_nano()
    step_started_ns = now_unix_nano()

    def _append_span(
        *,
        span_id: str,
        parent_span_id: str | None,
        name: str,
        kind: str,
        start_ns: int,
        end_ns: int,
        attributes: dict[str, Any] | None = None,
        status: str = "ok",
    ) -> None:
        span_attrs: dict[str, JSONValue] | None = None
        if attributes:
            span_attrs = {str(k): cast(JSONValue, v) for k, v in attributes.items()}
        span = TraceSpan(
            trace_id=trace_id,
            span_id=span_id,
            parent_span_id=parent_span_id,
            name=name,
            kind=kind,
            start_time_unix_nano=int(start_ns),
            end_time_unix_nano=int(end_ns),
            attributes=span_attrs,
            status=status,
        )
        accounting["trace_spans"].append(span.to_json_obj())

    best: Candidate | None = None
    verifier = DeterministicVerifier()
    effective_policy_engine = policy_engine or DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(
            mode=config.policy_mode,
            allow_actions=tuple(config.tool_allowlist),
            opa=(
                SubprocessOpaEvaluator(
                    policy_path=config.opa_policy_path,
                    decision_path=config.opa_decision_path,
                )
                if config.opa_policy_path is not None
                else None
            ),
        ),
    )

    def _observe_policy_decision(
        action: str,
        token: Any,
        decision: Any,
        context: dict[str, Any] | None,
    ) -> None:
        # Persist a stable, manifest-friendly decision shape for auditing.
        accounting["policy_decisions"].append(
            {
                "action": action,
                "scope": {
                    "tenant_id": scope.tenant_id,
                    "repo_id": scope.repo_id,
                },
                "token_id": str(getattr(token, "token_id", "")),
                "constraints": dict(getattr(token, "constraints", {}) or {}),
                "context": dict(context or {}),
                "allowed": bool(getattr(decision, "allowed", False)),
                "reason": str(getattr(decision, "reason", "")),
                "source": str(getattr(decision, "source", "")),
                "mode": str(getattr(decision, "mode", "")),
                "block": bool(getattr(decision, "block", False)),
            }
        )

    def _record_policy_failure(
        *,
        action: str,
        reason: str,
        stage_name: str,
        context: dict[str, Any] | None,
    ) -> PlanState:
        plan2 = _set_step_outputs(
            plan=plan,
            step_id=step_id,
            outputs_patch={
                "policy_decisions": list(accounting["policy_decisions"]),
                "last_policy_failure": {
                    "code": "policy.authorization_denied",
                    "action": action,
                    "stage": stage_name,
                    "message": f"policy authorization denied for {action}: {reason}",
                    "reason": reason,
                    "context": dict(context or {}),
                    "scope": {
                        "tenant_id": tenant_id,
                        "repo_id": repo_id,
                    },
                },
            },
        )
        plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan2)
        return plan2

    policy_llm = PolicyWrappedLLMBackend(
        backend=llm,
        policy_engine=effective_policy_engine,
        issuer=effective_policy_engine.issuer,
        decision_observer=_observe_policy_decision,
    )
    attenuator = CapabilityAttenuator()
    exec_base_capability = effective_policy_engine.issuer.issue(
        scope=scope,
        action="executor.run",
        constraints={"plan_id": plan.id, "step_id": step_id},
    )
    policy_executor = PolicyWrappedExecutor(
        executor=executor,
        policy_engine=effective_policy_engine,
        issuer=effective_policy_engine.issuer,
        attenuator=attenuator,
        decision_observer=_observe_policy_decision,
    )

    # Retrieve once per step for now (keeps budgeting simple). In replay modes,
    # prefer the persisted retrieval snapshot over live memory/index access.
    retrieve_span_id = new_span_id()
    retrieve_start_ns = now_unix_nano()
    replayed_ctx = _replayed_retrieval_context(replay_manifest=replay_manifest, goal=goal)
    if replayed_ctx is not None:
        ctx = replayed_ctx
    else:
        ctx = retrieve_context(
            tenant_id=tenant_id,
            repo_id=repo_id,
            plan=plan,
            code_memory=code_memory,
            why_graph=why_graph,
            index=index,
            limit=20,
        )
    _append_span(
        span_id=retrieve_span_id,
        parent_span_id=step_span_id,
        name="compile.retrieve",
        kind="internal",
        start_ns=retrieve_start_ns,
        end_ns=now_unix_nano(),
        attributes={
            "top_k": 20,
            "stage": "retrieve",
            "replayed_from_manifest": replayed_ctx is not None,
        },
    )
    retrieval_item_ids, retrieval_provenance = build_retrieval_documents_item_ids_and_provenance(
        tenant_id=tenant_id,
        documents=ctx.get("documents") or [],
    )
    for item in ctx.get("code_memory_items") or []:
        if isinstance(item, dict):
            item_id = item.get("item_id")
            if isinstance(item_id, str) and item_id.strip():
                retrieval_item_ids.append(item_id.strip())
    plan = _set_step_outputs(
        plan=plan,
        step_id=step_id,
        outputs_patch={
            "retrieval_snapshot": {
                "source": "compile_retriever",
                "query": goal,
                "top_k": 20,
                "item_ids": sorted(set(retrieval_item_ids)),
                "provenance": list(retrieval_provenance),
                "replayed_from_manifest": replayed_ctx is not None,
            }
        },
    )
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

    gen_tier = config.tier_for_stage(stage="generate")
    # Tests-by-default: run tests for every candidate evaluation.
    # Prefer explicit config, but remain backward-compatible with older metadata keys.
    smoke_command = list(
        config.test_command
        if config.test_command is not None
        else (config.metadata or {}).get("execute_command") or ["python", "-m", "pytest", "-q"]
    )
    smoke_timeout_s_raw = (
        config.test_timeout_s
        if config.test_timeout_s is not None
        else (config.metadata or {}).get("execute_timeout_s")
    )
    smoke_timeout_s_f = float(smoke_timeout_s_raw) if smoke_timeout_s_raw is not None else None

    full_command_raw = (config.metadata or {}).get("full_test_command")
    full_command = (
        list(full_command_raw)
        if isinstance(full_command_raw, (list, tuple)) and full_command_raw
        else _derive_full_test_command(smoke_command)
    )
    full_timeout_s_raw = (config.metadata or {}).get("full_test_timeout_s")
    full_timeout_s_f = (
        float(full_timeout_s_raw) if full_timeout_s_raw is not None else smoke_timeout_s_f
    )

    smoke_stage_name = "tests_smoke"
    full_stage_name = "tests_full"
    policy_metadata = dict(config.metadata or {})

    def _wall_budget_ok() -> bool:
        if budget.max_wall_time_s is None:
            return True
        return (time.monotonic() - start) <= float(budget.max_wall_time_s)

    def _token_budget_ok() -> bool:
        if budget.max_input_tokens is not None and int(accounting["input_tokens"]) >= int(
            budget.max_input_tokens
        ):
            return False
        return not (
            budget.max_total_tokens is not None
            and int(accounting["total_tokens"]) >= int(budget.max_total_tokens)
        )

    def _tool_budget_ok() -> bool:
        if budget.max_tool_calls is None:
            return True
        return int(accounting["tool_calls"]) < int(budget.max_tool_calls)

    def _refresh_estimated_cost() -> None:
        md = dict(config.metadata or {})
        # Backward compatibility: allow legacy metadata keys when explicit cost rates are unset.
        in_rate = float(config.cost_rates.input_per_1k_tokens_usd)
        out_rate = float(config.cost_rates.output_per_1k_tokens_usd)
        tool_rate = float(config.cost_rates.tool_call_usd)
        if in_rate == 0.0:
            in_rate = float(md.get("cost_input_per_1k_tokens_usd", 0.0) or 0.0)
        if out_rate == 0.0:
            out_rate = float(md.get("cost_output_per_1k_tokens_usd", 0.0) or 0.0)
        if tool_rate == 0.0:
            tool_rate = float(md.get("cost_tool_call_usd", 0.0) or 0.0)
        accounting["estimated_cost_usd"] = float(
            _estimate_cost_usd(
                input_tokens=int(accounting["input_tokens"]),
                output_tokens=int(accounting["output_tokens"]),
                tool_calls=int(accounting["tool_calls"]),
                input_per_1k_tokens_usd=in_rate,
                output_per_1k_tokens_usd=out_rate,
                tool_call_usd=tool_rate,
            )
        )

    def _cost_budget_ok() -> bool:
        if budget.max_cost_usd is None:
            return True
        return float(accounting["estimated_cost_usd"]) <= float(budget.max_cost_usd)

    # Generate → Execute loop with bounded repairs.
    current_tier = gen_tier
    stage: StageName = "generate"
    last_exec: ExecutionResult | None = None
    max_repairs = int(budget.effective_max_repairs_per_step())
    max_iters_total = int(budget.max_iterations_total)
    repairs_used = 0

    # IR-first prompt passes (compiler pass contract).
    generate_prompt_pass = DefaultIRGeneratePromptPass()
    repair_prompt_pass = DefaultIRRepairPromptPass()

    while True:
        if accounting["iterations_total"] >= max_iters_total:
            break
        if (
            accounting["llm_calls"] >= int(budget.max_llm_calls)
            or not _wall_budget_ok()
            or not _token_budget_ok()
            or not _cost_budget_ok()
        ):
            break

        # ARCS-style: use a single tier knob for both generate and repair, escalating on failures.
        tier_cfg = current_tier
        accounting["tier_history"].append({"stage": stage, "tier": tier_cfg.name})

        max_out = budget.max_output_tokens
        if max_out is None:
            max_out = tier_cfg.default_max_output_tokens

        # Build IR for the current plan state so passes can consume IR.
        ir_doc = build_ir_document_from_plan(plan=plan)

        test_policy: dict[str, Any] | None = None
        step_title: str | None = None
        failure: Any | None = None
        verifier_fb: dict[str, Any] | None = None

        if stage == "generate":
            test_policy = {
                "tests_generated_by_default": bool(config.generate_tests_by_default),
                "require_tests_for_non_test_changes": bool(
                    config.require_tests_for_non_test_changes
                ),
                "smoke_test_command": list(smoke_command),
                "full_test_command": list(full_command),
            }
        else:
            # Repair stage: parse failure and build a more structured prompt.
            step = next(s for s in plan.steps if s.id == step_id)
            assert last_exec is not None
            failure = parse_execution_failure(result=last_exec)
            # If the previous iteration was vetoed by the verifier, the controller
            # stores its structured result in step outputs. Thread it into repair context.
            verifier_fb = None
            try:
                step_outputs = dict(step.outputs or {})
                last_ver = step_outputs.get("last_verification")
                if isinstance(last_ver, dict):
                    verifier_fb = last_ver
            except Exception:
                verifier_fb = None
            step_title = step.title

        effective_replay_manifest = replay_manifest or RunManifest(
            run_id=plan.id,
            tenant_id=tenant_id,
            repo_id=repo_id,
            ir_sha256="0" * 64,
            replay_mode=replay_mode,  # validated by RunManifest
        )
        pass_decision = decide_replay_for_pass(manifest=effective_replay_manifest, pass_name=stage)

        last_generation_text = best.llm_text if best is not None else ""

        patch_resolution: ModelCallNeeded | ResolvedPatch | None = (
            resolve_patch_candidate_from_prompt(
                stage=stage,
                plan=plan,
                ir_doc=ir_doc,
                step_id=step_id,
                tier_name=tier_cfg.name,
                tier_model=tier_cfg.llm_model,
                temperature=float(tier_cfg.temperature),
                max_output_tokens=int(max_out) if max_out is not None else None,
                goal=goal,
                retrieved_context=ctx,
                test_policy=test_policy,
                step_title=step_title,
                last_generation_text=last_generation_text,
                failure=failure,
                verifier_feedback=verifier_fb,
                replay_mode=replay_mode,
                replay_manifest=effective_replay_manifest,
                should_call_model=pass_decision.should_call_model,
                generate_prompt_pass=generate_prompt_pass,
                repair_prompt_pass=repair_prompt_pass,
            )
        )

        if patch_resolution is None:
            break

        prompt_key = patch_resolution.prompt_key
        patch_text: str
        patch_sha256: str
        if isinstance(patch_resolution, ModelCallNeeded):
            try:
                resp = policy_llm.complete(
                    scope=scope,
                    stage=patch_resolution.llm_stage,
                    request=patch_resolution.llm_request,
                    token_constraints={
                        "plan_id": plan.id,
                        "step_id": step_id,
                        "tier": tier_cfg.name,
                        "replay_mode": replay_mode,
                    },
                )
            except ToolAuthorizationError as exc:
                plan = _record_policy_failure(
                    action="llm.complete",
                    reason=exc.decision.reason,
                    stage_name=patch_resolution.llm_stage,
                    context={"stage": patch_resolution.llm_stage},
                )
                break
            accounting["llm_calls"] += 1
            usage = dict(resp.usage or {})
            in_tok = (
                int(usage.get("input_tokens", 0))
                if "input_tokens" in usage
                else _estimate_token_count(patch_resolution.user_prompt)
            )
            out_tok = (
                int(usage.get("output_tokens", 0))
                if "output_tokens" in usage
                else _estimate_token_count(resp.text)
            )
            accounting["input_tokens"] += in_tok
            accounting["output_tokens"] += out_tok
            accounting["total_tokens"] += in_tok + out_tok
            _refresh_estimated_cost()
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name="compile.llm.complete",
                kind="client",
                start_ns=now_unix_nano() - 1,
                end_ns=now_unix_nano(),
                attributes={
                    "stage": stage,
                    "tier": tier_cfg.name,
                    "input_tokens": in_tok,
                    "output_tokens": out_tok,
                    "prompt_key": prompt_key,
                },
            )
            patch_text = resp.text
            touched_paths = list(candidate_from_patch_text(patch_text=patch_text).touched_paths)
            patch_sha256 = patch_sha256_hex(patch_text=patch_text)
        else:
            patch_text = patch_resolution.candidate.patch_text
            touched_paths = list(patch_resolution.candidate.touched_paths)
            patch_sha256 = patch_resolution.patch_sha256

        accounting["iterations_total"] += 1
        ok_tests_policy, policy_evidence = _policy_requires_tests(
            touched_paths=touched_paths,
            require_tests_for_non_test_changes=bool(config.require_tests_for_non_test_changes),
        )

        # Execute the produced artifact (executor decides what it means).
        # Tests-by-default:
        # - full mode: run one test command per iteration
        # - smoke mode: run smoke each iteration, and only run full on smoke pass ("promotion gate")
        if pass_decision.should_call_tools:
            if not _tool_budget_ok():
                break
            try:
                smoke_res = run_stage(
                    executor=policy_executor,
                    scope=scope,
                    stage=smoke_stage_name if config.test_mode == "smoke" else full_stage_name,
                    command=list(smoke_command if config.test_mode == "smoke" else full_command),
                    timeout_s=smoke_timeout_s_f
                    if config.test_mode == "smoke"
                    else full_timeout_s_f,
                    run_id=plan.id,
                    policy_context=_build_executor_policy_context(
                        executor=executor,
                        stage=smoke_stage_name if config.test_mode == "smoke" else full_stage_name,
                        command=list(
                            smoke_command if config.test_mode == "smoke" else full_command
                        ),
                        timeout_s=(
                            smoke_timeout_s_f if config.test_mode == "smoke" else full_timeout_s_f
                        ),
                        replay_mode=replay_mode,
                        policy_mode=config.policy_mode,
                        metadata=policy_metadata,
                    ),
                    policy_base_capability=exec_base_capability,
                )
            except ToolAuthorizationError as exc:
                smoke_policy_ctx = _build_executor_policy_context(
                    executor=executor,
                    stage=smoke_stage_name if config.test_mode == "smoke" else full_stage_name,
                    command=list(smoke_command if config.test_mode == "smoke" else full_command),
                    timeout_s=(
                        smoke_timeout_s_f if config.test_mode == "smoke" else full_timeout_s_f
                    ),
                    replay_mode=replay_mode,
                    policy_mode=config.policy_mode,
                    metadata=policy_metadata,
                )
                plan = _record_policy_failure(
                    action="executor.run",
                    reason=exc.decision.reason,
                    stage_name=smoke_stage_name if config.test_mode == "smoke" else full_stage_name,
                    context=smoke_policy_ctx,
                )
                break
            accounting["tool_calls"] += 1
            _refresh_estimated_cost()
            exec_result = smoke_res.result
            last_exec = exec_result
            smoke_end_ns = now_unix_nano()
            smoke_start_ns = smoke_end_ns - max(1, int(exec_result.duration_ms or 0) * 1_000_000)
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name=f"compile.executor.{smoke_res.stage}",
                kind="client",
                start_ns=smoke_start_ns,
                end_ns=smoke_end_ns,
                attributes={
                    "command": list(smoke_res.command),
                    "exit_code": int(exec_result.exit_code),
                    "duration_ms": exec_result.duration_ms,
                },
                status="ok" if int(exec_result.exit_code) == 0 else "error",
            )
        else:
            cached_exec = _replayed_execution_stage(
                plan=plan, step_id=step_id, replay_manifest=effective_replay_manifest
            )
            if cached_exec is None:
                break
            cached_stage, cached_result, cached_command = cached_exec
            smoke_res = StageRunResult(
                stage=cached_stage,
                command=list(cached_command),
                result=cached_result,
            )
            exec_result = cached_result
            last_exec = exec_result

        full_res = None
        should_run_full = False
        if (
            pass_decision.should_call_tools
            and config.test_mode == "smoke"
            and int(exec_result.exit_code) == 0
        ):
            n = config.full_test_every_n_iterations
            if n is None:
                should_run_full = True
            else:
                it = int(accounting["iterations_total"])
                # Run full periodically and on the last allowed iteration so we don't
                # exit without a final full-gate signal.
                should_run_full = (it % int(n) == 0) or (it >= max_iters_total)
        if should_run_full:
            if not _tool_budget_ok():
                break
            try:
                full_res = run_stage(
                    executor=policy_executor,
                    scope=scope,
                    stage=full_stage_name,
                    command=list(full_command),
                    timeout_s=full_timeout_s_f,
                    run_id=plan.id,
                    policy_context=_build_executor_policy_context(
                        executor=executor,
                        stage=full_stage_name,
                        command=list(full_command),
                        timeout_s=full_timeout_s_f,
                        replay_mode=replay_mode,
                        policy_mode=config.policy_mode,
                        metadata=policy_metadata,
                    ),
                    policy_base_capability=exec_base_capability,
                )
            except ToolAuthorizationError as exc:
                full_policy_ctx = _build_executor_policy_context(
                    executor=executor,
                    stage=full_stage_name,
                    command=list(full_command),
                    timeout_s=full_timeout_s_f,
                    replay_mode=replay_mode,
                    policy_mode=config.policy_mode,
                    metadata=policy_metadata,
                )
                plan = _record_policy_failure(
                    action="executor.run",
                    reason=exc.decision.reason,
                    stage_name=full_stage_name,
                    context=full_policy_ctx,
                )
                break
            accounting["tool_calls"] += 1
            _refresh_estimated_cost()
            exec_result = full_res.result
            last_exec = exec_result
            full_end_ns = now_unix_nano()
            full_start_ns = full_end_ns - max(1, int(exec_result.duration_ms or 0) * 1_000_000)
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name=f"compile.executor.{full_res.stage}",
                kind="client",
                start_ns=full_start_ns,
                end_ns=full_end_ns,
                attributes={
                    "command": list(full_res.command),
                    "exit_code": int(exec_result.exit_code),
                    "duration_ms": exec_result.duration_ms,
                },
                status="ok" if int(exec_result.exit_code) == 0 else "error",
            )

        promotable = int(exec_result.exit_code) == 0 and (
            config.test_mode != "smoke" or full_res is not None
        )

        # Compute verifier result early so monotonic "improvement" can take it
        # into account. We only persist the verification output if we reach the
        # promotion gate (i.e. not vetoed earlier by monotonic/policy).
        verifier_result = None
        if promotable and ok_tests_policy:
            verifier_policy = VerifierPolicy(
                enabled=bool(config.verifier_enabled),
                strict=bool(config.verifier_strict),
            )
            verifier_result = verifier.verify(
                scope=scope,
                plan_id=plan.id,
                step_id=step_id,
                candidate_patch=patch_text,
                execution=exec_result,
                accounting=accounting,
                budget=config.budget,
                policy=verifier_policy,
            )

        previous_best = best
        cand = Candidate(
            tier=str(tier_cfg.name),
            stage=stage,
            llm_text=patch_text,
            touched_paths=tuple(touched_paths),
            test_paths=tuple([p for p in touched_paths if _is_test_path(p)]),
            execution=exec_result,
            execution_stage=(full_res.stage if full_res is not None else smoke_res.stage),
            execution_command=list(full_res.command if full_res is not None else smoke_res.command),
            score=_score_candidate(
                execution=exec_result,
                ok_tests_policy=ok_tests_policy,
                promotable=promotable,
                verifier_passed=(verifier_result.passed if verifier_result is not None else None),
            ),
            attempt_idx=int(accounting["iterations_total"]),
            created_at_ms=now_ms(),
        )
        best = _best_of(best, cand)
        accounting["best_score"] = int(best.score)
        # ARCS-style monotonicity: repair candidates must strictly improve score.
        improved = previous_best is None or cand.score > int(previous_best.score)

        # Persist "best so far" into plan step outputs (monotonic).
        failure_json = None
        if int(exec_result.exit_code) != 0:
            failure_json = parse_execution_failure(result=exec_result).to_json_obj()
        plan = _set_step_outputs(
            plan=plan,
            step_id=step_id,
            outputs_patch={
                "best_candidate": best.to_json_obj(),
                "last_tests_smoke": smoke_res.to_json_obj(),
                "last_tests_full": full_res.to_json_obj() if full_res is not None else None,
                "accounting": dict(accounting),
                "policy": {
                    "mode": config.policy_mode,
                    "allowlist": list(config.tool_allowlist),
                },
                "policy_decisions": list(accounting["policy_decisions"]),
                "last_prompt_key": prompt_key,
                "last_patch_sha256": patch_sha256,
                "last_failure": failure_json,
            },
        )
        plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)

        if promotable and not improved and stage == "repair":
            monotonic_msg = (
                "monotonic improvement violated: repair candidate did not improve candidate score"
            )
            monotonic_exec = ExecutionResult(
                exit_code=3, stdout="", stderr=monotonic_msg, duration_ms=0
            )
            last_exec = monotonic_exec
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={
                    "last_monotonic_failure": {
                        "code": "repair.non_improving",
                        "message": monotonic_msg,
                        "best_score": int(previous_best.score)
                        if previous_best is not None
                        else None,
                        "candidate_score": int(cand.score),
                    }
                },
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            if repairs_used >= max_repairs:
                break
            repairs_used += 1
            accounting["repair_iterations"] = repairs_used
            stage = "repair"
            current_tier = _escalate_tier(current=current_tier, config=config)
            continue
        if promotable and not ok_tests_policy:
            # Treat as a policy failure eligible for repair; store evidence on the step.
            policy_msg = (
                "policy violation: patch changes non-test code but does not add/update tests; "
                "include relevant tests in the patch (tests generated by default)"
            )
            policy_exec = ExecutionResult(exit_code=2, stdout="", stderr=policy_msg, duration_ms=0)
            last_exec = policy_exec
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={
                    "last_policy_failure": {
                        "code": "policy.missing_tests",
                        "message": policy_msg,
                        "evidence": policy_evidence,
                    }
                },
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            if repairs_used >= max_repairs:
                break
            repairs_used += 1
            accounting["repair_iterations"] = repairs_used
            stage = "repair"
            current_tier = _escalate_tier(current=current_tier, config=config)
            continue
        if promotable:
            # Phase 5 verifier gate: can veto promotion even after tests pass.
            assert verifier_result is not None  # ok_tests_policy ensured above
            vres = verifier_result
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={"last_verification": vres.to_json_obj()},
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            _append_span(
                span_id=new_span_id(),
                parent_span_id=step_span_id,
                name="compile.verify",
                kind="internal",
                start_ns=now_unix_nano() - 1,
                end_ns=now_unix_nano(),
                attributes={
                    "passed": bool(vres.passed),
                    "strict": bool(config.verifier_strict),
                },
                status="ok" if bool(vres.passed) else "error",
            )
            if not vres.passed:
                # Treat verifier veto as a failure eligible for repair (budgeted).
                last_exec = exec_result
                if repairs_used >= max_repairs:
                    break
                repairs_used += 1
                accounting["repair_iterations"] = repairs_used
                stage = "repair"
                current_tier = _escalate_tier(current=current_tier, config=config)
                continue

            # Success: persist artifacts into code memory and mark step done.
            tms = now_ms()
            patch_paths = touched_paths
            patch_item_id = f"{plan.id}:{step_id}:patch"
            test_item_id = f"{plan.id}:{step_id}:test_result"
            smoke_item_id = f"{plan.id}:{step_id}:test_smoke_result"
            full_item_id = f"{plan.id}:{step_id}:test_full_result"
            code_memory.upsert_items(
                tenant_id=tenant_id,
                repo_id=repo_id,
                artifact_id=plan.id,
                items=[
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=patch_item_id,
                        kind="patch",
                        content=patch_text,
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "tier": tier_cfg.name,
                            "stage": str(stage),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=smoke_item_id,
                        kind="test_result",
                        content=smoke_res.result.stdout
                        + ("\n" + smoke_res.result.stderr if smoke_res.result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": smoke_res.stage,
                            "exit_code": int(smoke_res.result.exit_code),
                            "duration_ms": smoke_res.result.duration_ms,
                            "command": list(smoke_res.command),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=full_item_id,
                        kind="test_full_result",
                        content=exec_result.stdout
                        + ("\n" + exec_result.stderr if exec_result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": (full_res.stage if full_res is not None else smoke_res.stage),
                            "exit_code": int(exec_result.exit_code),
                            "duration_ms": exec_result.duration_ms,
                            "command": list(
                                full_res.command if full_res is not None else smoke_res.command
                            ),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                    make_item(
                        tenant_id=tenant_id,
                        repo_id=repo_id,
                        artifact_id=plan.id,
                        item_id=test_item_id,
                        kind="test_result",
                        content=exec_result.stdout
                        + ("\n" + exec_result.stderr if exec_result.stderr else ""),
                        metadata={
                            "plan_id": plan.id,
                            "step_id": step_id,
                            "stage": (full_res.stage if full_res is not None else smoke_res.stage),
                            "exit_code": int(exec_result.exit_code),
                            "duration_ms": exec_result.duration_ms,
                            "command": list(
                                full_res.command if full_res is not None else smoke_res.command
                            ),
                            "paths": patch_paths,
                        },
                        created_at_ms=tms,
                        updated_at_ms=tms,
                    ),
                ],
            )

            plan = _set_step_status(plan=plan, step_id=step_id, status="done")
            plan = _set_step_outputs(
                plan=plan,
                step_id=step_id,
                outputs_patch={
                    "code_memory_item_ids": [patch_item_id, test_item_id],
                    "code_memory_test_item_ids": [smoke_item_id, full_item_id],
                },
            )
            plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
            plan = advance_plan(
                tenant_id=tenant_id,
                repo_id=repo_id,
                plan_id=plan.id,
                plan_store=plan_store,
                feedback={"status": "passed", "step_id": step_id},
            )
            accounting["finished_at_ms"] = now_ms()
            accounting["wall_time_ms"] = int((time.monotonic() - start) * 1000.0)
            run_end_ns = now_unix_nano()
            _append_span(
                span_id=step_span_id,
                parent_span_id=run_span_id,
                name="compile.step",
                kind="internal",
                start_ns=step_started_ns,
                end_ns=run_end_ns,
                attributes={"step_id": step_id, "status": "succeeded"},
            )
            _append_span(
                span_id=run_span_id,
                parent_span_id=None,
                name="compile.run",
                kind="internal",
                start_ns=run_started_ns,
                end_ns=run_end_ns,
                attributes={
                    "tenant_id": tenant_id,
                    "repo_id": repo_id,
                    "plan_id": plan.id,
                    "status": "succeeded",
                },
            )
            return ControllerResult(
                status="succeeded",
                plan=plan,
                best_candidate=best,
                accounting=accounting,
            )

        # Smoke-only pass without a full gate:
        # keep iterating without consuming a repair.
        if (
            config.test_mode == "smoke"
            and int(smoke_res.result.exit_code) == 0
            and full_res is None
        ):
            stage = "generate"
            continue

        # Failure: iterate repair if budget allows.
        if repairs_used >= max_repairs:
            break
        repairs_used += 1
        accounting["repair_iterations"] = repairs_used
        stage = "repair"
        # ARCS-style: escalate generation tier after a failed execute.
        current_tier = _escalate_tier(current=current_tier, config=config)

    # If we reach here we did not succeed.
    status: RunStatus = "budget_exhausted"
    if (
        accounting["llm_calls"] < int(budget.max_llm_calls)
        and _wall_budget_ok()
        and _cost_budget_ok()
    ):
        status = "failed"

    plan = _set_step_status(
        plan=plan,
        step_id=step_id,
        status="failed",
        notes="compile loop did not produce a passing candidate within budget",
    )
    plan = replace(
        plan,
        last_feedback={"status": str(status), "step_id": step_id},
        updated_at_ms=now_ms(),
    )
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan)
    plan = advance_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan.id,
        plan_store=plan_store,
        feedback={"status": str(status), "step_id": step_id},
    )
    accounting["finished_at_ms"] = now_ms()
    accounting["wall_time_ms"] = int((time.monotonic() - start) * 1000.0)
    run_end_ns = now_unix_nano()
    _append_span(
        span_id=step_span_id,
        parent_span_id=run_span_id,
        name="compile.step",
        kind="internal",
        start_ns=step_started_ns,
        end_ns=run_end_ns,
        attributes={"step_id": step_id, "status": str(status)},
        status="error",
    )
    _append_span(
        span_id=run_span_id,
        parent_span_id=None,
        name="compile.run",
        kind="internal",
        start_ns=run_started_ns,
        end_ns=run_end_ns,
        attributes={
            "tenant_id": tenant_id,
            "repo_id": repo_id,
            "plan_id": plan.id,
            "status": str(status),
        },
        status="error",
    )
    return ControllerResult(status=status, plan=plan, best_candidate=best, accounting=accounting)
