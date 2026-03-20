from __future__ import annotations

import json
from pathlib import Path

from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.living.safe_recompile import safe_recompile_on_drift
from akc.memory.facade import build_memory
from akc.memory.models import normalize_repo_id
from akc.outputs.drift import write_baseline
from akc.outputs.emitters import JsonManifestEmitter
from akc.outputs.fingerprints import fingerprint_ingestion_state
from akc.outputs.models import OutputArtifact, OutputBundle


class _MarkerLLM(LLMBackend):
    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = (scope, request)
        text = "\n".join(
            [
                "--- a/src/marker_compiled.py",
                "+++ b/src/marker_compiled.py",
                "@@",
                f"+# marker stage={stage}",
                "",
                "--- a/tests/test_marker_compiled.py",
                "+++ b/tests/test_marker_compiled.py",
                "@@",
                "+def test_marker_compiled() -> None:",
                "+    assert True",
                "",
            ]
        )
        return LLMResponse(text=text, raw=None, usage=None)


def _write_minimal_repo(root: Path) -> None:
    """Minimal Python package with a passing pytest suite."""

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
    """Path where `SubprocessExecutor` runs tests."""

    base = outputs_root / tenant_id / normalize_repo_id(repo_id)
    return base / tenant_id / normalize_repo_id(repo_id)


def _seed_plan_with_one_step(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    goal: str,
    step_status: str,
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
    if step_status == "done":
        mem.plan_state.mark_step(
            tenant_id=tenant_id,
            repo_id=repo_id,
            plan_id=plan.id,
            step_id=plan.steps[0].id,
            status="done",
        )
    return plan.id


def _emit_manifest_and_baseline(
    *,
    outputs_root: Path,
    tenant_id: str,
    repo_id: str,
    ingest_state_path: Path,
    baseline_path: Path,
) -> None:
    scope_root = outputs_root / tenant_id / normalize_repo_id(repo_id)
    scope_root.mkdir(parents=True, exist_ok=True)

    # Emit a minimal output bundle + manifest contract.
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=outputs_root)

    fp = fingerprint_ingestion_state(tenant_id=tenant_id, state_path=ingest_state_path)
    write_baseline(
        scope=scope,
        outputs_root=outputs_root,
        ingest_fingerprint=fp,
        baseline_path=baseline_path,
    )


def _read_baseline(path: Path) -> dict[str, object]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(raw, dict)
    return raw


def test_safe_recompile_returns_zero_when_no_drift(tmp_path: Path) -> None:
    tenant_id = "t1"
    repo_id = "r1"
    goal = "Compile repository"
    outputs_root = tmp_path

    # Place a minimal repo in the executor's workdir.
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    plan_id = _seed_plan_with_one_step(
        tenant_id=tenant_id,
        repo_id=repo_id,
        outputs_root=outputs_root,
        goal=goal,
        step_status="done",
    )
    _ = plan_id  # explicit: we don't assert plan progress in the no-drift case

    # Create ingestion state and baseline.
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

    baseline_path = (
        outputs_root / tenant_id / normalize_repo_id(repo_id) / ".akc" / "living" / "baseline.json"
    )

    _emit_manifest_and_baseline(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ingest_state_path=ingest_state_path,
        baseline_path=baseline_path,
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
        baseline_path=None,
        eval_suite_path=eval_suite_path,
        goal=goal,
        canary_mode="quick",
        accept_mode="quick",
    )
    assert code == 0

    canary_dir = (
        outputs_root / ".akc" / "living" / "canary" / tenant_id / normalize_repo_id(repo_id)
    )
    assert not canary_dir.exists()


def test_safe_recompile_recompiles_on_output_drift_even_if_steps_done(tmp_path: Path) -> None:
    tenant_id = "t1"
    repo_id = "r1"
    goal = "Compile repository"
    outputs_root = tmp_path

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    plan_id = _seed_plan_with_one_step(
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

    baseline_path = (
        outputs_root / tenant_id / normalize_repo_id(repo_id) / ".akc" / "living" / "baseline.json"
    )

    _emit_manifest_and_baseline(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ingest_state_path=ingest_state_path,
        baseline_path=baseline_path,
    )
    baseline_before = _read_baseline(baseline_path)
    prev_manifest_sha = baseline_before.get("manifest_sha256")
    assert isinstance(prev_manifest_sha, str) and prev_manifest_sha

    # Mutate an emitted artifact after baseline acceptance.
    out_txt = outputs_root / tenant_id / normalize_repo_id(repo_id) / "out.txt"
    out_txt.write_text("mutated", encoding="utf-8")

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
        baseline_path=None,
        eval_suite_path=eval_suite_path,
        goal=goal,
        canary_mode="quick",
        accept_mode="quick",
    )
    assert code == 0

    canary_run = (
        outputs_root
        / ".akc"
        / "living"
        / "canary"
        / tenant_id
        / normalize_repo_id(repo_id)
        / ".akc"
        / "run"
        / f"{plan_id}.manifest.json"
    )
    assert canary_run.exists()

    baseline_after = _read_baseline(baseline_path)
    next_manifest_sha = baseline_after.get("manifest_sha256")
    assert isinstance(next_manifest_sha, str) and next_manifest_sha
    assert next_manifest_sha != prev_manifest_sha


def test_safe_recompile_recompiles_on_source_drift_and_updates_baseline(tmp_path: Path) -> None:
    tenant_id = "t1"
    repo_id = "r1"
    goal = "Compile repository"
    outputs_root = tmp_path

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    plan_id = _seed_plan_with_one_step(
        tenant_id=tenant_id,
        repo_id=repo_id,
        outputs_root=outputs_root,
        goal=goal,
        step_status="done",
    )

    baseline_ingest_state = tmp_path / "ingest_state_baseline.json"
    baseline_ingest_state.write_text(
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

    current_ingest_state = tmp_path / "ingest_state_current.json"
    current_ingest_state.write_text(
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

    baseline_path = (
        outputs_root / tenant_id / normalize_repo_id(repo_id) / ".akc" / "living" / "baseline.json"
    )

    _emit_manifest_and_baseline(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ingest_state_path=baseline_ingest_state,
        baseline_path=baseline_path,
    )
    baseline_before = _read_baseline(baseline_path)
    prev_sources_sha = baseline_before.get("sources_sha256")
    assert isinstance(prev_sources_sha, str) and prev_sources_sha

    eval_suite_path = tmp_path / "eval_suite.json"
    eval_suite_path.write_text(
        json.dumps({"regression_thresholds": {}}, indent=2),
        encoding="utf-8",
    )

    code = safe_recompile_on_drift(
        tenant_id=tenant_id,
        repo_id=repo_id,
        outputs_root=outputs_root,
        ingest_state_path=current_ingest_state,
        baseline_path=None,
        eval_suite_path=eval_suite_path,
        goal=goal,
        canary_mode="quick",
        accept_mode="quick",
    )
    assert code == 0

    canary_run = (
        outputs_root
        / ".akc"
        / "living"
        / "canary"
        / tenant_id
        / normalize_repo_id(repo_id)
        / ".akc"
        / "run"
        / f"{plan_id}.manifest.json"
    )
    assert canary_run.exists()

    baseline_after = _read_baseline(baseline_path)
    next_sources_sha = baseline_after.get("sources_sha256")
    assert isinstance(next_sources_sha, str) and next_sources_sha
    assert next_sources_sha != prev_sources_sha


def test_safe_recompile_uses_injected_llm_backend_for_canary(tmp_path: Path) -> None:
    tenant_id = "t1"
    repo_id = "r1"
    goal = "Compile repository"
    outputs_root = tmp_path

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    _seed_plan_with_one_step(
        tenant_id=tenant_id,
        repo_id=repo_id,
        outputs_root=outputs_root,
        goal=goal,
        step_status="done",
    )

    baseline_ingest_state = tmp_path / "ingest_state_baseline.json"
    baseline_ingest_state.write_text(
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
    current_ingest_state = tmp_path / "ingest_state_current.json"
    current_ingest_state.write_text(
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
    baseline_path = (
        outputs_root / tenant_id / normalize_repo_id(repo_id) / ".akc" / "living" / "baseline.json"
    )
    _emit_manifest_and_baseline(
        outputs_root=outputs_root,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ingest_state_path=baseline_ingest_state,
        baseline_path=baseline_path,
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
        ingest_state_path=current_ingest_state,
        baseline_path=None,
        eval_suite_path=eval_suite_path,
        goal=goal,
        canary_mode="quick",
        accept_mode="quick",
        llm_backend=_MarkerLLM(),
    )
    assert code == 0

    canary_patch_dir = (
        outputs_root
        / ".akc"
        / "living"
        / "canary"
        / tenant_id
        / normalize_repo_id(repo_id)
        / ".akc"
        / "patches"
    )
    patch_files = sorted(canary_patch_dir.glob("*.diff"))
    assert patch_files
    patch_content = patch_files[-1].read_text(encoding="utf-8")
    assert "marker stage=" in patch_content
