"""Validate the hardening roadmap success criteria end to end.

These tests cover the measurable criteria from the AKC hardening roadmap:
- Replayable
- Auditable
- Bounded
- Isolated
- Regressed-proof
- Living
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from akc.cli import main
from akc.compile import Budget, CompileSession, ControllerConfig, TierConfig, run_compile_loop
from akc.compile.executors import SubprocessExecutor
from akc.compile.interfaces import (
    ExecutionRequest,
    ExecutionResult,
    Executor,
    Index,
    IndexDocument,
    IndexQuery,
    LLMBackend,
    LLMRequest,
    LLMResponse,
    TenantRepoScope,
)
from akc.execute.dev import DevSandboxConfig
from akc.execute.secrets import SecretsScopeConfig
from akc.execute.strong import SandboxStrongConfig
from akc.living.safe_recompile import safe_recompile_on_drift
from akc.memory.facade import build_memory
from akc.memory.models import normalize_repo_id
from akc.outputs.drift import write_baseline
from akc.outputs.emitters import JsonManifestEmitter
from akc.outputs.fingerprints import fingerprint_ingestion_state
from akc.outputs.models import OutputArtifact, OutputBundle
from akc.run import (
    PassRecord,
    RetrievalSnapshot,
    RunManifest,
    find_latest_run_manifest,
    load_run_manifest,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _write_minimal_repo(root: Path) -> None:
    pkg = root / "src"
    tests = root / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tests / "test_module.py").write_text(
        "from src import module\n\ndef test_smoke() -> None:\n    assert module.VALUE == 1\n",
        encoding="utf-8",
    )


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    base = outputs_root / tenant_id / normalize_repo_id(repo_id)
    return base / tenant_id / normalize_repo_id(repo_id)


def _seed_plan_with_one_step(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    goal: str = "Compile repository",
    step_status: str | None = None,
) -> str:
    base = outputs_root / tenant_id / normalize_repo_id(repo_id)
    memory_db = base / ".akc" / "memory.sqlite"
    memory_db.parent.mkdir(parents=True, exist_ok=True)

    mem = build_memory(backend="sqlite", sqlite_path=str(memory_db))
    plan = mem.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal=goal,
        initial_steps=["Implement goal"],
    )
    mem.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)
    if step_status is not None:
        mem.plan_state.mark_step(
            tenant_id=tenant_id,
            repo_id=repo_id,
            plan_id=plan.id,
            step_id=plan.steps[0].id,
            status=step_status,
        )
    return plan.id


def _latest_manifest(outputs_root: Path, tenant_id: str, repo_id: str) -> RunManifest:
    manifest_path = find_latest_run_manifest(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    assert manifest_path is not None
    return load_run_manifest(
        path=manifest_path,
        expected_tenant_id=tenant_id,
        expected_repo_id=repo_id,
    )


@dataclass(frozen=True)
class _OfflineLLM(LLMBackend):
    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = (scope, stage, request)
        return LLMResponse(
            text="\n".join(
                [
                    "--- a/src/generated.py",
                    "+++ b/src/generated.py",
                    "@@",
                    "+VALUE = 2",
                    "",
                    "--- a/tests/test_generated.py",
                    "+++ b/tests/test_generated.py",
                    "@@",
                    "+def test_generated() -> None:",
                    "+    assert True",
                    "",
                ]
            ),
            raw=None,
            usage=None,
        )


@dataclass(frozen=True)
class _FakeIndex(Index):
    def query(self, *, scope: TenantRepoScope, query: IndexQuery) -> list[IndexDocument]:
        _ = query
        return [
            IndexDocument(
                doc_id=f"{scope.tenant_id}-doc-1",
                title="Requirements",
                content="Implement the requested change safely.",
                score=0.99,
                metadata={
                    "source_type": "docs",
                    "source": "/docs/requirements.md",
                    "path": "/docs/requirements.md",
                    "chunk_index": 0,
                },
            )
        ]


@dataclass
class _AlwaysFailExecutor(Executor):
    calls: int = 0

    def run(
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        _ = (scope, request)
        self.calls += 1
        return ExecutionResult(exit_code=1, stdout="", stderr="boom", duration_ms=1)


def _mk_controller_config(*, max_llm_calls: int = 3) -> ControllerConfig:
    tiers = {
        "small": TierConfig(name="small", llm_model="fake-small", temperature=0.0),
        "medium": TierConfig(name="medium", llm_model="fake-medium", temperature=0.0),
        "large": TierConfig(name="large", llm_model="fake-large", temperature=0.0),
    }
    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=Budget(max_llm_calls=max_llm_calls, max_repairs_per_step=1, max_iterations_total=2),
        test_mode="full",
        test_command=("python", "-c", "print('ok')"),
        test_timeout_s=2.0,
        tool_allowlist=("llm.complete", "executor.run"),
    )


def _emit_manifest_and_baseline(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
    ingest_state_path: Path,
    baseline_path: Path,
) -> None:
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    bundle = OutputBundle(
        scope=scope,
        name="accepted",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=outputs_root)
    ingest_fp = fingerprint_ingestion_state(tenant_id=tenant_id, state_path=ingest_state_path)
    write_baseline(
        scope=scope,
        outputs_root=outputs_root,
        ingest_fingerprint=ingest_fp,
        baseline_path=baseline_path,
    )


def test_success_criteria_replayable_manifest_replay_is_self_contained(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = "replay-tenant"
    repo_id = "replay-repo"
    outputs_root = tmp_path
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    replay_manifest = RunManifest(
        run_id="prior-replay-run",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="a" * 64,
        replay_mode="full_replay",
        retrieval_snapshots=(
            RetrievalSnapshot(
                source="compile_retriever",
                query="Compile repository",
                top_k=1,
                item_ids=("snapshot-doc-1",),
            ),
        ),
        passes=(
            PassRecord(
                name="generate",
                status="succeeded",
                metadata={
                    "llm_text": "\n".join(
                        [
                            "--- a/src/replayed.py",
                            "+++ b/src/replayed.py",
                            "@@",
                            "+VALUE = 3",
                            "",
                            "--- a/tests/test_replayed.py",
                            "+++ b/tests/test_replayed.py",
                            "@@",
                            "+def test_replayed() -> None:",
                            "+    assert True",
                            "",
                        ]
                    )
                },
            ),
            PassRecord(
                name="execute",
                status="succeeded",
                metadata={
                    "stage": "tests_full",
                    "command": ["pytest", "-q"],
                    "exit_code": 0,
                    "stdout": "replayed",
                    "stderr": "",
                    "duration_ms": 1,
                },
            ),
        ),
        control_plane={"policy_decisions": [{"allowed": True, "reason": "seeded"}]},
    )
    replay_manifest_path = tmp_path / "seed.manifest.json"
    replay_manifest_path.write_text(json.dumps(replay_manifest.to_json_obj()), encoding="utf-8")

    def _fail_live_retrieval(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("live retrieve_context must not run in manifest replay")

    monkeypatch.setattr("akc.compile.controller.retrieve_context", _fail_live_retrieval)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "thorough",
                "--replay-mode",
                "full_replay",
                "--replay-manifest-path",
                str(replay_manifest_path),
            ]
        )
    assert excinfo.value.code == 0

    emitted = _latest_manifest(outputs_root, tenant_id, repo_id)
    assert emitted.retrieval_snapshots
    assert emitted.retrieval_snapshots[0].item_ids == ("snapshot-doc-1",)


def test_success_criteria_auditable_policy_trace_and_provenance(tmp_path: Path) -> None:
    tenant_id = "audit-tenant"
    repo_id = "audit-repo"
    outputs_root = tmp_path
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    session = CompileSession.from_memory(
        tenant_id=tenant_id,
        repo_id=repo_id,
        index=_FakeIndex(),
    )
    plan = session.memory.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Compile repository",
        initial_steps=["Implement goal"],
    )
    session.memory.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)

    result = session.run(
        goal="Compile repository",
        llm=_OfflineLLM(),
        executor=SubprocessExecutor(work_root=outputs_root),
        config=_mk_controller_config(),
        outputs_root=outputs_root,
    )
    assert result.status == "succeeded"

    manifest = _latest_manifest(outputs_root, tenant_id, repo_id)
    assert manifest.trace_spans
    assert isinstance((manifest.control_plane or {}).get("policy_decisions"), list)
    assert manifest.retrieval_snapshots

    ir_dir = outputs_root / tenant_id / repo_id / ".akc" / "ir"
    ir_paths = [p for p in ir_dir.glob("*.json") if not p.name.endswith(".diff.json")]
    assert ir_paths
    latest_ir = max(ir_paths, key=lambda p: p.stat().st_mtime)
    ir_payload = json.loads(latest_ir.read_text(encoding="utf-8"))
    workflow_nodes = [n for n in ir_payload.get("nodes", []) if n.get("kind") == "workflow"]
    assert workflow_nodes
    assert workflow_nodes[0].get("provenance"), "expected replay/audit provenance on workflow node"


def test_success_criteria_bounded_run_halts_on_budget(tmp_path: Path) -> None:
    mem = build_memory(backend="memory")
    plan = mem.plan_state.create_plan(
        tenant_id="budget-tenant",
        repo_id="budget-repo",
        goal="Compile repository",
        initial_steps=["Implement goal"],
    )
    mem.plan_state.set_active_plan(
        tenant_id="budget-tenant",
        repo_id="budget-repo",
        plan_id=plan.id,
    )

    res = run_compile_loop(
        tenant_id="budget-tenant",
        repo_id="budget-repo",
        goal="Compile repository",
        plan_store=mem.plan_state,
        code_memory=mem.code_memory,
        why_graph=mem.why_graph,
        index=None,
        llm=_OfflineLLM(),
        executor=_AlwaysFailExecutor(),
        config=_mk_controller_config(max_llm_calls=1),
    )

    assert res.status in {"budget_exhausted", "failed"}
    assert int(res.accounting.get("llm_calls", 0)) <= 1
    assert int(res.accounting.get("repair_iterations", 0)) <= 1


def test_success_criteria_isolated_network_defaults_secrets_and_artifacts(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert DevSandboxConfig().allow_network is False
    assert SandboxStrongConfig().allow_network is False

    monkeypatch.setenv("AKC_SECRET_tenant_a_TOKEN", "aaa")
    monkeypatch.setenv("AKC_SECRET_tenant_b_TOKEN", "bbb")
    secrets = SecretsScopeConfig(allowed_secret_names=("TOKEN",))
    tenant_a = secrets.resolve_env_for_scope(scope=TenantRepoScope("tenant_a", "repo"))
    assert tenant_a == {"AKC_SECRET_TOKEN": "aaa"}

    outputs_root = tmp_path
    _write_minimal_repo(_executor_cwd(outputs_root, "tenant_a", "repo1"))
    _seed_plan_with_one_step(tenant_id="tenant_a", repo_id="repo1", outputs_root=outputs_root)
    _write_minimal_repo(_executor_cwd(outputs_root, "tenant_b", "repo1"))
    _seed_plan_with_one_step(tenant_id="tenant_b", repo_id="repo1", outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo_a:
        main(
            [
                "compile",
                "--tenant-id",
                "tenant_a",
                "--repo-id",
                "repo1",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    with pytest.raises(SystemExit) as excinfo_b:
        main(
            [
                "compile",
                "--tenant-id",
                "tenant_b",
                "--repo-id",
                "repo1",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo_a.value.code == 0
    assert excinfo_b.value.code == 0
    assert (outputs_root / "tenant_a" / "repo1" / "manifest.json").exists()
    assert (outputs_root / "tenant_b" / "repo1" / "manifest.json").exists()
    assert not (outputs_root / "manifest.json").exists()


def test_success_criteria_regressed_proof_ci_runs_eval_regression_gates() -> None:
    ci = (_repo_root() / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "Eval regression gates" in ci
    assert "uv run akc eval" in ci
    assert "--baseline-report-path" in ci


def test_success_criteria_living_drift_triggers_safe_recompile_with_gates(tmp_path: Path) -> None:
    tenant_id = "living-tenant"
    repo_id = "living-repo"
    outputs_root = tmp_path
    goal = "Compile repository"

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(
        tenant_id=tenant_id,
        repo_id=repo_id,
        outputs_root=outputs_root,
        goal=goal,
        step_status="done",
    )

    ingest_state_path = tmp_path / "ingest_state.json"
    ingest_state_path.write_text(
        json.dumps(
            {
                f"{tenant_id}::docs::/x/a.md": {
                    "kind": "docs",
                    "path": "/x/a.md",
                    "mtime_ns": 1,
                    "size": 10,
                }
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    baseline_path = outputs_root / tenant_id / repo_id / ".akc" / "living" / "baseline.json"
    _emit_manifest_and_baseline(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ingest_state_path=ingest_state_path,
        baseline_path=baseline_path,
    )

    ingest_state_path.write_text(
        json.dumps(
            {
                f"{tenant_id}::docs::/x/a.md": {
                    "kind": "docs",
                    "path": "/x/a.md",
                    "mtime_ns": 2,
                    "size": 10,
                }
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    eval_suite_path = tmp_path / "eval_suite.json"
    eval_suite_path.write_text(
        json.dumps({"regression_thresholds": {}}, indent=2),
        encoding="utf-8",
    )

    code = safe_recompile_on_drift(
        tenant_id=tenant_id,
        repo_id=repo_id,
        outputs_root=outputs_root,
        ingest_state_path=ingest_state_path,
        baseline_path=baseline_path,
        eval_suite_path=eval_suite_path,
        goal=goal,
        canary_mode="quick",
        accept_mode="quick",
    )
    assert code == 0
    assert (outputs_root / ".akc" / "living" / "canary" / tenant_id / repo_id / ".akc" / "living").exists()
