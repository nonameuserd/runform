from __future__ import annotations

import json
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

from akc.compile import (
    Budget,
    CompileSession,
    ControllerConfig,
    CostRates,
    SubprocessExecutor,
    TierConfig,
)
from akc.compile.controller import ControllerResult
from akc.compile.interfaces import Executor, LLMBackend, TenantRepoScope
from akc.evals import run_eval_suite
from akc.ir import IRDocument
from akc.memory.models import PlanState, normalize_repo_id, require_non_empty
from akc.outputs.drift import drift_report, write_baseline
from akc.outputs.fingerprints import IngestStateFingerprint, stable_json_fingerprint
from akc.run.loader import find_latest_run_manifest

_TenantKeySep = "::"


def _read_json_object(path: Path, *, what: str) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError(f"{what} must be a JSON object: {path}")
    return loaded


def parse_ingest_state_key(
    *,
    tenant_id: str,
    key: str,
) -> tuple[str, str] | None:
    """Parse an ingestion state key into (connector, source_id).

    Ingestion state keys are expected to follow:
    `<tenant_id>::<connector_name>::<source_id>`.
    """

    if not isinstance(key, str) or not key.startswith(f"{tenant_id}{_TenantKeySep}"):
        return None
    # Preserve any `::` inside source_id by limiting splits.
    parts = key.split(_TenantKeySep, 2)
    if len(parts) != 3:
        return None
    _, connector, source_id = parts
    connector = str(connector).strip()
    source_id = str(source_id).strip()
    if not connector or not source_id:
        return None
    return connector, source_id


def fq_source_id(connector: str, source_id: str) -> str:
    return f"{connector.strip()}::{source_id.strip()}"


def compute_changed_source_ids(
    *,
    tenant_id: str,
    baseline_sources_by_key: Mapping[str, Any] | None,
    current_state_by_key: Mapping[str, Any],
) -> set[str]:
    """Compute changed ingestion source ids by per-key fingerprint equality."""

    require_non_empty(tenant_id, name="tenant_id")
    changed: set[str] = set()
    baseline = dict(baseline_sources_by_key or {})

    # Union over keys so removals count as changes.
    keys = set(baseline.keys()) | set(current_state_by_key.keys())
    for k in keys:
        parsed = parse_ingest_state_key(tenant_id=tenant_id, key=str(k))
        if parsed is None:
            continue
        connector, source_id = parsed
        prev = baseline.get(k)
        curr = current_state_by_key.get(k, None)
        if isinstance(prev, dict) and isinstance(curr, dict):
            if dict(prev) != dict(curr):
                changed.add(fq_source_id(connector, source_id))
        else:
            if prev != curr:
                changed.add(fq_source_id(connector, source_id))
    return changed


def compute_impacted_workflow_step_ids(
    *,
    ir: IRDocument,
    changed_source_ids: set[str],
) -> set[str]:
    """Compute impacted workflow step ids from the IR provenance + dependency graph."""

    if not changed_source_ids:
        return set()

    nodes_by_id = {n.id: n for n in ir.nodes}
    dependents: dict[str, set[str]] = {}
    for n in ir.nodes:
        for dep in n.depends_on:
            dependents.setdefault(dep, set()).add(n.id)

    start_node_ids: set[str] = set()
    for n in ir.nodes:
        if n.kind != "workflow":
            continue
        for p in n.provenance:
            if p.source_id in changed_source_ids:
                start_node_ids.add(n.id)
                break

    if not start_node_ids:
        # No provenance hit -> conservative caller should recompile everything.
        return set()

    queue = list(start_node_ids)
    visited: set[str] = set(start_node_ids)
    while queue:
        cur = queue.pop(0)
        for nxt in dependents.get(cur, set()):
            if nxt in visited:
                continue
            visited.add(nxt)
            queue.append(nxt)

    impacted_steps: set[str] = set()
    for nid in visited:
        node = nodes_by_id.get(nid)
        if node is None or node.kind != "workflow":
            continue
        step_id = node.properties.get("step_id")
        if isinstance(step_id, str) and step_id.strip():
            impacted_steps.add(step_id.strip())
    return impacted_steps


def _latest_ir_document_path(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
) -> Path | None:
    base = outputs_root / tenant_id / normalize_repo_id(repo_id)
    ir_dir = base / ".akc" / "ir"
    if not ir_dir.exists():
        return None
    candidates = [
        p
        for p in ir_dir.glob("*.json")
        if p.is_file() and not p.name.endswith(".diff.json") and p.name != "diff.json"
    ]
    if not candidates:
        return None
    return max(candidates, key=lambda p: p.stat().st_mtime)


def _reset_steps_for_recompile(
    *,
    plan_store: Any,
    tenant_id: str,
    repo_id: str,
    plan_id: str,
    impacted_step_ids: set[str],
    skip_other_pending: bool,
) -> None:
    plan: PlanState | None = plan_store.load_plan(
        tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id
    )
    if plan is None:
        raise ValueError("active plan not found for recompile")

    new_steps = []
    for s in plan.steps:
        if s.id in impacted_step_ids:
            new_steps.append(
                type(s)(
                    id=s.id,
                    title=s.title,
                    status="pending",
                    order_idx=s.order_idx,
                    started_at_ms=None,
                    finished_at_ms=None,
                    notes=None,
                    inputs=s.inputs,
                    outputs={},
                )
            )
        elif skip_other_pending and s.status in {"pending", "in_progress"}:
            new_steps.append(
                type(s)(
                    id=s.id,
                    title=s.title,
                    status="skipped",
                    order_idx=s.order_idx,
                    started_at_ms=s.started_at_ms,
                    finished_at_ms=s.finished_at_ms,
                    notes=s.notes,
                    inputs=s.inputs,
                    outputs=s.outputs,
                )
            )
        else:
            new_steps.append(s)

    plan2 = type(plan)(
        id=plan.id,
        tenant_id=plan.tenant_id,
        repo_id=plan.repo_id,
        goal=plan.goal,
        status=plan.status,
        created_at_ms=plan.created_at_ms,
        updated_at_ms=int(time.time() * 1000),
        steps=tuple(new_steps),
        next_step_id=plan.next_step_id,
        budgets=plan.budgets,
        last_feedback=plan.last_feedback,
    )
    plan_store.save_plan(tenant_id=tenant_id, repo_id=repo_id, plan=plan2)


class _OfflineCompileLLM(LLMBackend):
    """Deterministic offline backend for safe recompile canaries."""

    def complete(
        self,
        *,
        scope: Any,
        stage: str,
        request: Any,
    ) -> Any:
        # Ignore request content; generate a patch that touches both code and tests.
        _ = (scope, request)
        text = "\n".join(
            [
                "--- a/src/akc_compiled.py",
                "+++ b/src/akc_compiled.py",
                "@@",
                f"+# compiled stage={stage}",
                "",
                "--- a/tests/test_akc_compiled.py",
                "+++ b/tests/test_akc_compiled.py",
                "@@",
                "+def test_compiled_smoke():",
                "+    assert True",
                "",
            ]
        )
        # Minimal compatibility with controller expectations (LLMResponse shape).
        from akc.compile.interfaces import LLMResponse  # local import to avoid cycles

        return LLMResponse(text=text, raw=None, usage=None)


def _mk_compile_config(
    *,
    mode: Literal["quick", "thorough"],
    test_mode: Literal["smoke", "full"],
    policy_mode: Literal["audit_only", "enforce"],
    opa_policy_path: str | None = None,
    opa_decision_path: str = "data.akc.allow",
    cost_rates: CostRates | None = None,
) -> ControllerConfig:
    from akc.compile.controller_config import ControllerConfig as CC

    tiers: dict[Literal["small", "medium", "large"], TierConfig] = {
        "small": TierConfig(name="small", llm_model="offline-small", temperature=0.0),
        "medium": TierConfig(name="medium", llm_model="offline-medium", temperature=0.2),
        "large": TierConfig(name="large", llm_model="offline-large", temperature=0.3),
    }

    if mode == "thorough":
        budget = Budget(max_llm_calls=8, max_repairs_per_step=3, max_iterations_total=6)
        full_every: int | None = None
    else:
        budget = Budget(max_llm_calls=4, max_repairs_per_step=2, max_iterations_total=4)
        full_every = 2

    return CC(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=budget,
        test_mode=test_mode,
        full_test_every_n_iterations=full_every if test_mode == "smoke" else None,
        policy_mode=policy_mode,
        tool_allowlist=("llm.complete", "executor.run"),
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
        cost_rates=cost_rates or CostRates(),
        metadata={"mode": mode},
    )


def safe_recompile_on_drift(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: str | Path,
    ingest_state_path: str | Path,
    baseline_path: str | Path | None = None,
    eval_suite_path: str | Path,
    goal: str = "Compile repository",
    policy_mode: Literal["audit_only", "enforce"] = "enforce",
    canary_mode: Literal["quick", "thorough"] = "quick",
    accept_mode: Literal["quick", "thorough"] = "thorough",
    canary_test_mode: Literal["smoke", "full"] = "smoke",
    allow_network: bool = False,
    llm_backend: LLMBackend | None = None,
    update_baseline_on_accept: bool = True,
    skip_other_pending: bool = True,
    opa_policy_path: str | None = None,
    opa_decision_path: str = "data.akc.allow",
) -> int:
    """Living systems safe recompile entrypoint.

    - Detect changed ingestion sources (from baseline + current ingest state).
    - Compute impacted IR subgraph using IR provenance.
    - Canary compile only impacted plan steps, then run an eval subset on canary manifests.
    - If canary passes, accept changes by running full compile for impacted steps.
    - Uses a caller-provided LLM backend when supplied, otherwise falls back to
      a deterministic offline backend.
    """

    tenant_id = str(tenant_id).strip()
    require_non_empty(tenant_id, name="tenant_id")
    repo_id = normalize_repo_id(str(repo_id))
    outputs_root_p = Path(outputs_root).expanduser().resolve()
    ingest_state_p = Path(ingest_state_path).expanduser().resolve()
    if baseline_path is None:
        baseline_path_p = outputs_root_p / tenant_id / repo_id / ".akc" / "living" / "baseline.json"
    else:
        baseline_path_p = Path(baseline_path).expanduser().resolve()

    standard_base = outputs_root_p / tenant_id / repo_id
    memory_db = standard_base / ".akc" / "memory.sqlite"
    memory_db.parent.mkdir(parents=True, exist_ok=True)

    baseline_exists = baseline_path_p.exists()
    baseline_sources_by_key: Mapping[str, Any] | None = None
    if baseline_exists:
        try:
            baseline_loaded = _read_json_object(baseline_path_p, what="baseline")
            maybe = baseline_loaded.get("sources_by_key")
            baseline_sources_by_key = maybe if isinstance(maybe, dict) else None
        except Exception:
            baseline_sources_by_key = None

    # Load current ingestion state and filter to tenant.
    current_state = _read_json_object(ingest_state_p, what="ingest_state")
    current_tenant_by_key: dict[str, Any] = {}
    tenant_prefix = f"{tenant_id}::"
    for k, v in current_state.items():
        if isinstance(k, str) and k.startswith(tenant_prefix):
            current_tenant_by_key[k] = v

    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    ingest_fp = IngestStateFingerprint(
        tenant_id=tenant_id,
        state_path=str(ingest_state_p),
        sha256=stable_json_fingerprint(current_tenant_by_key),
        keys_included=len(current_tenant_by_key),
    )
    drift = drift_report(
        scope=scope,
        outputs_root=outputs_root_p,
        ingest_fingerprint=ingest_fp,
        baseline_path=baseline_path_p if baseline_exists else None,
    )
    # Fast-path: when we have an existing baseline contract and it matches
    # current sources+outputs, no safe action is required.
    if baseline_exists and not drift.has_drift():
        return 0

    # If baseline_sources_by_key is present we can compute a precise changed
    # source set; otherwise we conservatively recompile everything.
    changed_source_ids: set[str] = set()
    if baseline_sources_by_key is not None:
        changed_source_ids = compute_changed_source_ids(
            tenant_id=tenant_id,
            baseline_sources_by_key=baseline_sources_by_key,
            current_state_by_key=current_tenant_by_key,
        )

    # Find latest accepted IR doc to compute an impacted workflow subgraph.
    latest_ir_path = _latest_ir_document_path(
        outputs_root=outputs_root_p, tenant_id=tenant_id, repo_id=repo_id
    )
    latest_ir: IRDocument | None = None
    if latest_ir_path is not None:
        latest_ir = IRDocument.from_json_obj(_read_json_object(latest_ir_path, what="ir"))

    plan_step_ids_to_reset: set[str] = set()
    if latest_ir is not None and changed_source_ids:
        plan_step_ids_to_reset = compute_impacted_workflow_step_ids(
            ir=latest_ir, changed_source_ids=changed_source_ids
        )

    # Decide how conservative we need to be when mapping changed sources into
    # a recompile subgraph. When provenance mapping is missing/unreliable, we
    # recompile all steps (not just non-done) to restore safe output state.
    sources_drifted = any(f.kind == "changed_sources" for f in drift.findings)
    outputs_drifted = any(f.kind in {"missing_manifest", "changed_outputs"} for f in drift.findings)
    provenance_failed_to_map = (
        bool(changed_source_ids) and latest_ir is not None and not plan_step_ids_to_reset
    )

    # If we don't have a baseline, we can't safely narrow to impacted outputs.
    need_all_steps = (not baseline_exists) or outputs_drifted or provenance_failed_to_map
    no_ir_mapping = sources_drifted and bool(changed_source_ids) and latest_ir is None
    if no_ir_mapping:
        need_all_steps = True
    if sources_drifted and baseline_sources_by_key is None:
        need_all_steps = True
    if sources_drifted and baseline_sources_by_key is not None and not changed_source_ids:
        need_all_steps = True

    session = CompileSession.from_sqlite(
        tenant_id=tenant_id,
        repo_id=repo_id,
        sqlite_path=str(memory_db),
        index=None,
    )
    plan = session.plan(goal=goal)
    session.memory.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
    loaded_plan = session.memory.plan_state.load_plan(
        tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id
    )
    if loaded_plan is None:
        raise ValueError("active plan missing after planning")

    if not plan_step_ids_to_reset:
        if need_all_steps:
            plan_step_ids_to_reset = {s.id for s in loaded_plan.steps}
        else:
            # Conservative default when we can map drift but only need to
            # re-run steps that aren't already finalized.
            plan_step_ids_to_reset = {s.id for s in loaded_plan.steps if s.status != "done"}

    if not plan_step_ids_to_reset:
        # Nothing to do: plan is already done for all steps.
        return 0

    _reset_steps_for_recompile(
        plan_store=session.memory.plan_state,
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan.id,
        impacted_step_ids=plan_step_ids_to_reset,
        skip_other_pending=skip_other_pending,
    )

    llm: LLMBackend = llm_backend if llm_backend is not None else _OfflineCompileLLM()
    work_root = standard_base
    executor: Executor = SubprocessExecutor(
        work_root=work_root,
        disable_network=not allow_network,
    )

    canary_config = _mk_compile_config(
        mode=canary_mode,
        test_mode=canary_test_mode,
        policy_mode=policy_mode,
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
    )

    canary_outputs_root = outputs_root_p / ".akc" / "living" / "canary"
    canary_outputs_root.mkdir(parents=True, exist_ok=True)

    # Canary compile: iteratively run compile loop until impacted steps are done.
    while True:
        active = session.memory.plan_state.load_plan(
            tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id
        )
        if active is None:
            raise ValueError("plan disappeared during canary compile")
        remaining = [
            s.id for s in active.steps if s.id in plan_step_ids_to_reset and s.status != "done"
        ]
        if not remaining:
            break
        cres: ControllerResult = session.run(
            goal=goal,
            llm=llm,
            executor=executor,
            config=canary_config,
            outputs_root=canary_outputs_root,
            replay_mode="live",
        )
        if cres.status != "succeeded":
            return 2

    # Canary eval subset: deterministic checks against the emitted canary manifest.
    canary_manifest_path = find_latest_run_manifest(
        outputs_root=canary_outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    if canary_manifest_path is None:
        return 2

    canary_manifest_abs = Path(canary_manifest_path).expanduser().resolve()
    regression_thresholds: dict[str, Any] = {}
    try:
        eval_suite_raw = _read_json_object(
            Path(eval_suite_path).expanduser().resolve(), what="eval_suite_path"
        )
        maybe = eval_suite_raw.get("regression_thresholds")
        if isinstance(maybe, dict):
            regression_thresholds = maybe
    except Exception:
        regression_thresholds = {}
    canary_suite: dict[str, Any] = {
        "suite_version": "living-canary-v1",
        "tasks": [
            {
                "id": "living-canary",
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "manifest_path": str(canary_manifest_abs),
                "checks": {
                    "require_success": True,
                    "required_passes": ["plan", "retrieve", "generate", "execute"],
                    "max_repair_iterations": 2,
                    "max_total_tokens": 100000,
                    "max_wall_time_ms": 60000,
                    "require_trace_spans": True,
                },
                "judge": {"enabled": False},
            }
        ],
        "regression_thresholds": regression_thresholds,
    }
    canary_suite_path = (
        canary_outputs_root / tenant_id / repo_id / ".akc" / "living" / "canary_eval_suite.json"
    )
    canary_suite_path.parent.mkdir(parents=True, exist_ok=True)
    canary_suite_path.write_text(
        json.dumps(canary_suite, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    eval_report = run_eval_suite(
        suite_path=canary_suite_path,
        outputs_root=canary_outputs_root,
        baseline_report_path=None,
    )
    if not eval_report.passed:
        return 2

    # Acceptance compile: reset impacted steps and run full compilation (live).
    _reset_steps_for_recompile(
        plan_store=session.memory.plan_state,
        tenant_id=tenant_id,
        repo_id=repo_id,
        plan_id=plan.id,
        impacted_step_ids=plan_step_ids_to_reset,
        skip_other_pending=False,
    )

    accept_config = _mk_compile_config(
        mode=accept_mode,
        test_mode="full",
        policy_mode=policy_mode,
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
    )

    while True:
        active2 = session.memory.plan_state.load_plan(
            tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id
        )
        if active2 is None:
            raise ValueError("plan disappeared during acceptance compile")
        remaining2 = [
            s.id for s in active2.steps if s.id in plan_step_ids_to_reset and s.status != "done"
        ]
        if not remaining2:
            break
        cres2: ControllerResult = session.run(
            goal=goal,
            llm=llm,
            executor=executor,
            config=accept_config,
            outputs_root=outputs_root_p,
            replay_mode="live",
        )
        if cres2.status != "succeeded":
            return 2

    # Acceptance succeeded: update baseline contract after the new outputs exist.
    if update_baseline_on_accept:
        write_baseline(
            scope=TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id),
            outputs_root=outputs_root_p,
            ingest_fingerprint=ingest_fp,
            baseline_path=baseline_path_p,
        )

    return 0
