"""Unit tests for the `akc compile` CLI path.

Tests invoke the CLI entrypoint via akc.cli.main([...]) with offline/fake
backends and assert exit codes, output layout, and tenant isolation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from akc.cli import main
from akc.memory.facade import build_memory
from akc.run import PassRecord, RetrievalSnapshot, RunManifest


def _write_minimal_repo(root: Path) -> None:
    """Write a minimal Python package with passing pytest."""
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


def _write_failing_repo(root: Path) -> None:
    """Write a minimal repo whose tests always fail (for exit code 2 tests)."""
    pkg = root / "src"
    tests = root / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (tests / "test_fail.py").write_text("def test_always_fails(): assert False\n", encoding="utf-8")


def _seed_plan_with_one_step(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    goal: str = "Compile repository",
) -> None:
    """Pre-seed SQLite memory with an active plan that has one step.

    The CLI uses <outputs_root>/<tenant_id>/<repo_id>/.akc/memory.sqlite.
    Seeding a plan with one step causes the compile loop to run and emit
    manifest + .akc/tests when tests pass.
    """
    base = outputs_root / tenant_id / repo_id
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


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    """Path where the executor runs tests: work_root/tenant_id/repo_id (CLI sets work_root=base)."""
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


def test_cli_compile_quick_mode_emits_manifest_and_tests(tmp_path: Path) -> None:
    """With a pre-seeded plan and passing tests, compile exits 0 and emits manifest + .akc/tests."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

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
                "quick",
            ]
        )
    assert excinfo.value.code == 0

    manifest = base / "manifest.json"
    assert manifest.exists(), "expected manifest.json to be emitted"
    assert (base / ".akc" / "run").is_dir()
    assert any((base / ".akc" / "run").rglob("*.manifest.json"))
    assert (base / ".akc" / "ir").is_dir()
    assert any((base / ".akc" / "ir").rglob("*.json"))

    tests_dir = base / ".akc" / "tests"
    assert tests_dir.is_dir()
    assert any(tests_dir.rglob("*.json")), "expected structured test artifacts under .akc/tests"


def test_cli_compile_empty_plan_exits_success_and_emits_run_contracts(tmp_path: Path) -> None:
    """When the plan has no steps, compile exits 0 and still emits IR/run contracts."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    # Do not seed a plan; create_or_resume_plan will create an empty plan.

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
                "quick",
            ]
        )
    assert excinfo.value.code == 0

    manifest = base / "manifest.json"
    assert manifest.exists(), "empty plan should still emit manifest"
    assert any((base / ".akc" / "run").rglob("*.manifest.json"))
    assert any((base / ".akc" / "ir").rglob("*.json"))


def test_cli_compile_failing_tests_exit_code_2(tmp_path: Path) -> None:
    """When tests always fail, compile exits with code 2."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    _write_failing_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

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
                "quick",
            ]
        )
    assert excinfo.value.code == 2


def test_cli_compile_missing_required_args_exits_non_zero() -> None:
    """Missing --tenant-id or --outputs-root results in non-zero exit (usage error)."""
    with pytest.raises(SystemExit) as excinfo:
        main(["compile", "--repo-id", "r1", "--outputs-root", "/tmp/out"])
    assert excinfo.value.code != 0

    with pytest.raises(SystemExit) as excinfo:
        main(["compile", "--tenant-id", "t1", "--repo-id", "r1"])
    assert excinfo.value.code != 0


def test_cli_compile_tenant_isolation(tmp_path: Path) -> None:
    """Outputs and artifacts are scoped under <outputs_root>/<tenant>/<repo>."""
    outputs_root = tmp_path
    t1_base = outputs_root / "tenant_a" / "repo1"
    t2_base = outputs_root / "tenant_b" / "repo1"
    _write_minimal_repo(_executor_cwd(outputs_root, "tenant_a", "repo1"))
    _seed_plan_with_one_step(tenant_id="tenant_a", repo_id="repo1", outputs_root=outputs_root)
    _write_minimal_repo(_executor_cwd(outputs_root, "tenant_b", "repo1"))
    _seed_plan_with_one_step(tenant_id="tenant_b", repo_id="repo1", outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "tenant_a",
                "--repo-id",
                "repo1",
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0
    with pytest.raises(SystemExit) as excinfo2:
        main(
            [
                "compile",
                "--tenant-id",
                "tenant_b",
                "--repo-id",
                "repo1",
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo2.value.code == 0

    assert (t1_base / "manifest.json").exists()
    assert (t2_base / "manifest.json").exists()
    assert (t1_base / ".akc" / "tests").is_dir()
    assert (t2_base / ".akc" / "tests").is_dir()
    # No cross-contamination: no manifest at outputs root.
    assert not (outputs_root / "manifest.json").exists()


def test_cli_compile_thorough_mode_emits_manifest(tmp_path: Path) -> None:
    """--mode thorough runs and emits manifest when tests pass."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

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
            ]
        )
    assert excinfo.value.code == 0
    assert (base / "manifest.json").exists()


def test_cli_compile_custom_goal_used(tmp_path: Path) -> None:
    """--goal is passed through and compile succeeds with it."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

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
                "--goal",
                "Implement feature X",
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0
    assert (base / "manifest.json").exists()


def test_cli_compile_full_replay_with_manifest_path_succeeds(tmp_path: Path) -> None:
    """Compile can run in full_replay using an explicit prior run manifest."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    replay_patch = "\n".join(
        [
            "--- a/src/replayed_cli.py",
            "+++ b/src/replayed_cli.py",
            "@@",
            "+VALUE = 3",
            "",
            "--- a/tests/test_replayed_cli.py",
            "+++ b/tests/test_replayed_cli.py",
            "@@",
            "+def test_replayed_cli():",
            "+    assert True",
            "",
        ]
    )
    replay_manifest = RunManifest(
        run_id="prior-run",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="a" * 64,
        replay_mode="full_replay",
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
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
    )
    replay_manifest_path = tmp_path / "replay.manifest.json"
    replay_manifest_path.write_text(
        json.dumps(replay_manifest.to_json_obj()),
        encoding="utf-8",
    )

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
    assert (base / "manifest.json").exists()
    run_dir = base / ".akc" / "run"
    assert run_dir.is_dir()
    assert any(run_dir.rglob("*.manifest.json"))


def test_cli_compile_full_replay_uses_manifest_snapshot_without_live_retrieval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    replay_patch = "\n".join(
        [
            "--- a/src/replayed_manifest_only.py",
            "+++ b/src/replayed_manifest_only.py",
            "@@",
            "+VALUE = 8",
            "",
            "--- a/tests/test_replayed_manifest_only.py",
            "+++ b/tests/test_replayed_manifest_only.py",
            "@@",
            "+def test_replayed_manifest_only():",
            "+    assert True",
            "",
        ]
    )
    replay_manifest = RunManifest(
        run_id="prior-run-manifest-only",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="f" * 64,
        replay_mode="full_replay",
        retrieval_snapshots=(
            RetrievalSnapshot(
                source="compile_retriever",
                query="Compile repository",
                top_k=1,
                item_ids=("snap-1",),
            ),
        ),
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
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
    )
    replay_manifest_path = tmp_path / "replay-manifest-only.manifest.json"
    replay_manifest_path.write_text(json.dumps(replay_manifest.to_json_obj()), encoding="utf-8")

    def _fail_live_retrieval(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("live retrieve_context must not run during manifest replay")

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


def test_cli_compile_llm_vcr_with_manifest_path_succeeds(tmp_path: Path) -> None:
    """Compile can run in llm_vcr using replayed model output while still running tests."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    replay_patch = "\n".join(
        [
            "--- a/src/replayed_llm_vcr.py",
            "+++ b/src/replayed_llm_vcr.py",
            "@@",
            "+VALUE = 4",
            "",
            "--- a/tests/test_replayed_llm_vcr.py",
            "+++ b/tests/test_replayed_llm_vcr.py",
            "@@",
            "+def test_replayed_llm_vcr():",
            "+    assert True",
            "",
        ]
    )
    replay_manifest = RunManifest(
        run_id="prior-run-vcr",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="b" * 64,
        replay_mode="llm_vcr",
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
        ),
    )
    replay_manifest_path = tmp_path / "replay-llm-vcr.manifest.json"
    replay_manifest_path.write_text(
        json.dumps(replay_manifest.to_json_obj()),
        encoding="utf-8",
    )

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
                "llm_vcr",
                "--replay-manifest-path",
                str(replay_manifest_path),
            ]
        )
    assert excinfo.value.code == 0
    assert (base / "manifest.json").exists()
    tests_dir = base / ".akc" / "tests"
    assert tests_dir.is_dir()
    assert any(tests_dir.rglob("*.json")), "expected structured test artifacts under .akc/tests"


def test_cli_compile_partial_replay_with_manifest_path_succeeds(tmp_path: Path) -> None:
    """Compile can run in partial_replay using replayed model output while still running tests."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    replay_patch = "\n".join(
        [
            "--- a/src/replayed_partial.py",
            "+++ b/src/replayed_partial.py",
            "@@",
            "+VALUE = 5",
            "",
            "--- a/tests/test_replayed_partial.py",
            "+++ b/tests/test_replayed_partial.py",
            "@@",
            "+def test_replayed_partial():",
            "+    assert True",
            "",
        ]
    )
    replay_manifest = RunManifest(
        run_id="prior-run-partial",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="c" * 64,
        replay_mode="partial_replay",
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
        ),
    )
    replay_manifest_path = tmp_path / "replay-partial.manifest.json"
    replay_manifest_path.write_text(
        json.dumps(replay_manifest.to_json_obj()),
        encoding="utf-8",
    )

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
                "partial_replay",
                "--replay-manifest-path",
                str(replay_manifest_path),
            ]
        )
    # Under strict monotonic repair policy, partial replay without explicit pass
    # selection may fail if replayed generate/repair candidates tie in score.
    assert excinfo.value.code in {0, 2}
    assert (base / "manifest.json").exists()
    run_dir = base / ".akc" / "run"
    assert run_dir.is_dir()
    assert any(run_dir.rglob("*.manifest.json"))
    tests_dir = base / ".akc" / "tests"
    if tests_dir.is_dir():
        assert any(tests_dir.rglob("*.json")), (
            "expected structured test artifacts under .akc/tests when tests run"
        )


def test_cli_compile_partial_replay_with_selected_passes_succeeds(tmp_path: Path) -> None:
    """Partial replay accepts explicit pass list and preserves tenant-scoped outputs."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    replay_patch = "\n".join(
        [
            "--- a/src/replayed_selected.py",
            "+++ b/src/replayed_selected.py",
            "@@",
            "+VALUE = 6",
            "",
            "--- a/tests/test_replayed_selected.py",
            "+++ b/tests/test_replayed_selected.py",
            "@@",
            "+def test_replayed_selected():",
            "+    assert True",
            "",
        ]
    )
    replay_manifest = RunManifest(
        run_id="prior-run-partial-selected",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="d" * 64,
        replay_mode="partial_replay",
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
        ),
    )
    replay_manifest_path = tmp_path / "replay-partial-selected.manifest.json"
    replay_manifest_path.write_text(
        json.dumps(replay_manifest.to_json_obj()),
        encoding="utf-8",
    )

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
                "partial_replay",
                "--replay-manifest-path",
                str(replay_manifest_path),
                "--partial-replay-passes",
                "execute",
            ]
        )
    assert excinfo.value.code == 0
    assert (base / "manifest.json").exists()


def test_cli_compile_partial_replay_overrides_manifest_mode(tmp_path: Path) -> None:
    """Requested replay mode should be honored even if manifest mode differs."""
    tenant_id = "t1"
    repo_id = "repo1"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id
    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    replay_patch = "\n".join(
        [
            "--- a/src/replayed_override.py",
            "+++ b/src/replayed_override.py",
            "@@",
            "+VALUE = 7",
            "",
            "--- a/tests/test_replayed_override.py",
            "+++ b/tests/test_replayed_override.py",
            "@@",
            "+def test_replayed_override():",
            "+    assert True",
            "",
        ]
    )
    # Intentionally provide a full_replay manifest while requesting partial_replay.
    replay_manifest = RunManifest(
        run_id="prior-run-mode-mismatch",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="e" * 64,
        replay_mode="full_replay",
        passes=(
            PassRecord(name="generate", status="succeeded", metadata={"llm_text": replay_patch}),
        ),
    )
    replay_manifest_path = tmp_path / "replay-mode-mismatch.manifest.json"
    replay_manifest_path.write_text(
        json.dumps(replay_manifest.to_json_obj()),
        encoding="utf-8",
    )

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
                "partial_replay",
                "--replay-manifest-path",
                str(replay_manifest_path),
                "--partial-replay-passes",
                "execute",
            ]
        )
    assert excinfo.value.code == 0
    assert (base / "manifest.json").exists()


@dataclass
class _FakeControllerResult:
    status: str = "succeeded"


@dataclass
class _FakeSession:
    executor_seen: Any = None

    def run(self, **kwargs: Any) -> _FakeControllerResult:
        self.executor_seen = kwargs.get("executor")
        return _FakeControllerResult(status="succeeded")


@dataclass
class _FakeDockerExecutor:
    work_root: Any
    image: str
    disable_network: bool
    memory_bytes: int | None = None
    pids_limit: int | None = None
    cpus: float | None = None
    user: str | None = None
    tmpfs_mounts: tuple[str, ...] = ()
    seccomp_profile: str | None = None
    apparmor_profile: str | None = None
    ulimit_nofile: str | None = None
    ulimit_nproc: str | None = None
    stdout_max_bytes: int | None = None
    stderr_max_bytes: int | None = None


def test_cli_compile_strong_sandbox_defaults_to_docker_when_available(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`--sandbox strong` should default to Docker even when Rust is available."""
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    @dataclass
    class _FakeRustExecutor:
        rust_cfg: Any
        work_root: Any = None

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    import akc.execute.strong as strong_mod

    monkeypatch.setattr(strong_mod, "RustExecutor", _FakeRustExecutor)
    monkeypatch.setattr(strong_mod, "DockerExecutor", _FakeDockerExecutor)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--rust-exec-mode",
                "cli",
            ]
        )
    assert excinfo.value.code == 0
    from akc.execute.strong import StrongSandboxExecutor

    assert isinstance(fake_session.executor_seen, StrongSandboxExecutor)
    assert isinstance(fake_session.executor_seen.underlying, _FakeDockerExecutor)


def test_cli_compile_strong_sandbox_allows_explicit_wasm_preference(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`--sandbox strong --strong-lane-preference wasm` should use Rust WASM."""
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    @dataclass
    class _FakeRustExecutor:
        rust_cfg: Any
        work_root: Any = None

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    import akc.execute.strong as strong_mod

    monkeypatch.setattr(strong_mod, "RustExecutor", _FakeRustExecutor)
    monkeypatch.setattr(strong_mod, "DockerExecutor", _FakeDockerExecutor)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "wasm",
            ]
        )
    assert excinfo.value.code == 0
    from akc.execute.strong import StrongSandboxExecutor

    assert isinstance(fake_session.executor_seen, StrongSandboxExecutor)
    assert isinstance(fake_session.executor_seen.underlying, _FakeRustExecutor)
    assert fake_session.executor_seen.underlying.rust_cfg.lane == "wasm"


def test_cli_compile_use_rust_exec_allows_explicit_wasm_lane(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`--use-rust-exec --rust-exec-lane wasm` should select the Rust WASM lane directly."""
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    @dataclass
    class _FakeRustExecutor:
        rust_cfg: Any
        work_root: Any = None

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    monkeypatch.setattr(compile_mod, "RustExecutor", _FakeRustExecutor)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--use-rust-exec",
                "--rust-exec-lane",
                "wasm",
            ]
        )

    assert excinfo.value.code == 0
    assert isinstance(fake_session.executor_seen, _FakeRustExecutor)
    assert fake_session.executor_seen.rust_cfg.lane == "wasm"


def test_cli_compile_strong_wasm_normalization_flags_propagate_to_rust_cfg(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """WASM normalization CLI flags should flow into strong-lane Rust config."""
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    @dataclass
    class _FakeRustExecutor:
        rust_cfg: Any
        work_root: Any = None

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    import akc.execute.strong as strong_mod

    monkeypatch.setattr(strong_mod, "RustExecutor", _FakeRustExecutor)
    monkeypatch.setattr(strong_mod, "DockerExecutor", _FakeDockerExecutor)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "wasm",
                "--wasm-fs-normalize-existing-paths",
                "--wasm-fs-normalization-profile",
                "relaxed",
            ]
        )
    assert excinfo.value.code == 0
    from akc.execute.strong import StrongSandboxExecutor

    assert isinstance(fake_session.executor_seen, StrongSandboxExecutor)
    assert isinstance(fake_session.executor_seen.underlying, _FakeRustExecutor)
    rust_cfg = fake_session.executor_seen.underlying.rust_cfg
    assert rust_cfg.wasm_normalize_existing_paths is True
    assert rust_cfg.wasm_normalization_strict is False


def test_cli_compile_strong_wasm_fs_flags_propagate_to_rust_cfg(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    @dataclass
    class _FakeRustExecutor:
        rust_cfg: Any
        work_root: Any = None

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    import akc.execute.strong as strong_mod

    monkeypatch.setattr(strong_mod, "RustExecutor", _FakeRustExecutor)
    monkeypatch.setattr(strong_mod, "DockerExecutor", _FakeDockerExecutor)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "wasm",
                "--wasm-preopen-dir",
                "/tmp/work",
                "--wasm-preopen-dir",
                "/tmp/cache",
                "--wasm-allow-write-dir",
                "/tmp/work",
            ]
        )
    assert excinfo.value.code == 0
    from akc.execute.strong import StrongSandboxExecutor

    assert isinstance(fake_session.executor_seen, StrongSandboxExecutor)
    rust_cfg = fake_session.executor_seen.underlying.rust_cfg
    assert rust_cfg.preopen_dirs == ("/tmp/work", "/tmp/cache")
    assert rust_cfg.allowed_write_paths == ("/tmp/work",)


def test_cli_compile_strong_wasm_cpu_fuel_propagates_to_rust_cfg(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """WASM CPU fuel flag should flow into strong-lane Rust config."""
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    @dataclass
    class _FakeRustExecutor:
        rust_cfg: Any
        work_root: Any = None

    @dataclass
    class _FakeDockerExecutor:
        work_root: Any
        image: str
        disable_network: bool
        memory_bytes: int | None = None
        pids_limit: int | None = None
        cpus: float | None = None
        stdout_max_bytes: int | None = None
        stderr_max_bytes: int | None = None

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    import akc.execute.strong as strong_mod

    monkeypatch.setattr(strong_mod, "RustExecutor", _FakeRustExecutor)
    monkeypatch.setattr(strong_mod, "DockerExecutor", _FakeDockerExecutor)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "wasm",
                "--sandbox-cpu-fuel",
                "12000",
            ]
        )
    assert excinfo.value.code == 0
    from akc.execute.strong import StrongSandboxExecutor

    assert isinstance(fake_session.executor_seen, StrongSandboxExecutor)
    assert isinstance(fake_session.executor_seen.underlying, _FakeRustExecutor)
    assert fake_session.executor_seen.underlying.rust_cfg.cpu_fuel == 12000


def test_cli_compile_rust_exec_wasm_fs_flags_propagate_to_rust_cfg(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    @dataclass
    class _FakeRustExecutor:
        rust_cfg: Any
        work_root: Any = None

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    monkeypatch.setattr(compile_mod, "RustExecutor", _FakeRustExecutor)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--use-rust-exec",
                "--rust-exec-lane",
                "wasm",
                "--wasm-preopen-dir",
                "/tmp/work",
                "--wasm-allow-write-dir",
                "/tmp/work",
            ]
        )

    assert excinfo.value.code == 0
    assert isinstance(fake_session.executor_seen, _FakeRustExecutor)
    assert fake_session.executor_seen.rust_cfg.preopen_dirs == ("/tmp/work",)
    assert fake_session.executor_seen.rust_cfg.allowed_write_paths == ("/tmp/work",)


def test_cli_compile_strict_wasm_fails_fast_when_rust_surface_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import akc.cli.compile as compile_mod

    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: False)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "wasm",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "WASM preflight failed" in out
    assert "requested WASM backend is unavailable" in out
    assert "Install the Rust WASM execution surface" in out


def test_cli_compile_strong_docker_hardening_flags_propagate(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    import akc.cli.compile as compile_mod

    fake_session = _FakeSession()

    class _FakeCompileSession:
        @classmethod
        def from_sqlite(cls, **kwargs: Any) -> _FakeSession:
            _ = cls
            _ = kwargs
            return fake_session

    monkeypatch.setattr(compile_mod, "CompileSession", _FakeCompileSession)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)

    def _which(cmd: str) -> str | None:
        return "/usr/bin/docker" if cmd == "docker" else None

    monkeypatch.setattr(compile_mod.shutil, "which", _which)
    import akc.execute.strong as strong_mod

    monkeypatch.setattr(strong_mod, "DockerExecutor", _FakeDockerExecutor)

    seccomp_profile = tmp_path / "seccomp.json"
    seccomp_profile.write_text("{}", encoding="utf-8")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--docker-user",
                "1234:1234",
                "--docker-tmpfs",
                "/tmp",
                "--docker-tmpfs",
                "/var/tmp",
                "--docker-seccomp-profile",
                str(seccomp_profile),
                "--docker-ulimit-nofile",
                "1024:2048",
                "--docker-ulimit-nproc",
                "512",
            ]
        )

    assert excinfo.value.code == 0
    from akc.execute.strong import StrongSandboxExecutor

    assert isinstance(fake_session.executor_seen, StrongSandboxExecutor)
    underlying = fake_session.executor_seen.underlying
    assert isinstance(underlying, _FakeDockerExecutor)
    assert underlying.user == "1234:1234"
    assert underlying.tmpfs_mounts == ("/tmp", "/var/tmp")
    assert underlying.seccomp_profile == str(seccomp_profile)
    assert underlying.ulimit_nofile == "1024:2048"
    assert underlying.ulimit_nproc == "512"


def test_cli_compile_invalid_docker_hardening_exits_non_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--docker-ulimit-nofile",
                "10:5",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "Docker preflight failed" in out
    assert "soft limit cannot exceed hard limit" in out


def test_cli_compile_missing_seccomp_profile_fails_closed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = tmp_path / "missing-seccomp.json"

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--docker-seccomp-profile",
                str(missing),
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "Docker preflight failed" in out
    assert "configured seccomp profile path is unavailable" in out


def test_cli_compile_invalid_docker_apparmor_profile_fails_closed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--docker-apparmor-profile",
                "bad profile",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "Docker preflight failed" in out
    assert "docker apparmor profile cannot contain whitespace" in out


def test_cli_compile_docker_apparmor_requires_supported_host(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import akc.cli.compile as compile_mod

    monkeypatch.setattr(compile_mod, "_docker_apparmor_available", lambda: False)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--docker-apparmor-profile",
                "akc-default",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "Docker preflight failed" in out
    assert "configured AppArmor profile is unavailable on this host" in out


def test_cli_compile_malformed_docker_ulimit_nproc_exits_non_zero(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--docker-ulimit-nproc",
                "1:2:3",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "Docker preflight failed" in out
    assert "docker ulimit nproc must be '<soft>' or '<soft>:<hard>'" in out


def test_cli_compile_auto_lane_with_docker_hardening_fails_when_docker_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import akc.cli.compile as compile_mod

    monkeypatch.setattr(compile_mod.shutil, "which", lambda _cmd: None)
    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "auto",
                "--docker-user",
                "1234:1234",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "Docker preflight failed" in out
    assert "Docker hardening would be dropped" in out


def test_cli_compile_opa_policy_requires_opa_cli(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import akc.cli.compile as compile_mod

    def _which(cmd: str) -> str | None:
        if cmd == "docker":
            return "/usr/bin/docker"
        return None

    monkeypatch.setattr(compile_mod.shutil, "which", _which)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo-prod",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "docker",
                "--policy-mode",
                "enforce",
                "--opa-policy-path",
                "./configs/policy/compile_tools_prod.rego",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "Policy preflight failed" in out
    assert "configured OPA policy requires the `opa` CLI" in out


def test_cli_compile_rust_exec_wasm_fails_closed_when_surface_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import akc.cli.compile as compile_mod

    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: False)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--use-rust-exec",
                "--rust-exec-lane",
                "wasm",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "WASM preflight failed" in out
    assert "requested WASM backend is unavailable" in out
    assert "requested_backend=rust-exec-wasm" in out
    assert "--rust-exec-lane process" in out


def test_cli_compile_strict_wasm_fails_fast_on_windows_timeout_gap(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    import akc.cli.compile as compile_mod

    monkeypatch.setattr(compile_mod, "_rust_exec_available", lambda **_: True)
    monkeypatch.setattr(compile_mod.sys, "platform", "win32")

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox",
                "strong",
                "--strong-lane-preference",
                "wasm",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "WASM preflight failed" in out
    assert "strict WASM compile runs on Windows cannot guarantee wall-time enforcement" in out
    assert "Use Linux/macOS for strict WASM runs" in out


def test_cli_compile_rejects_wasm_fs_flags_without_explicit_wasm_lane(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--wasm-preopen-dir",
                "/tmp/work",
            ]
        )

    assert excinfo.value.code == 2
    out = capsys.readouterr().out
    assert "WASM preflight failed" in out
    assert "WASM filesystem flags require explicit WASM lane selection" in out


def test_cli_compile_rejects_non_positive_cpu_fuel(tmp_path: Path) -> None:
    """`--sandbox-cpu-fuel` must be strictly positive when provided."""
    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                "t1",
                "--repo-id",
                "r1",
                "--outputs-root",
                str(tmp_path),
                "--sandbox-cpu-fuel",
                "0",
            ]
        )
    assert excinfo.value.code == 2
