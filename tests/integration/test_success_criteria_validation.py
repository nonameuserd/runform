"""Validate the OSS plan Success criteria (Section 7).

These tests re-check the five criteria after implementation:
- Reproducible: documented one-liner / short block runs ingest → compile (and optionally verify).
- Documented: README and docs exist, links and key phrases are correct.
- Testable: CLI compile unit and integration tests exist and run in CI.
- Extensible: No hard-coded assumptions in compile that block new connectors or outputs.
- Transparent: Default flows do not require API keys; proprietary options are opt-in only.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from akc.cli import main
from akc.memory.facade import build_memory


def _repo_root() -> Path:
    """Repository root (parent of tests/)."""
    return Path(__file__).resolve().parent.parent.parent


def _write_minimal_repo(root: Path) -> None:
    """Minimal Python package with passing pytest."""
    pkg = root / "src"
    tests = root / "tests"
    pkg.mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)
    (pkg / "__init__.py").write_text("", encoding="utf-8")
    (pkg / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (tests / "test_module.py").write_text(
        "from src import module\n\n"
        "def test_smoke() -> None:\n"
        "    assert module.VALUE == 1\n",
        encoding="utf-8",
    )


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    """Path where the executor runs tests (work_root/tenant_id/repo_id)."""
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


def _seed_plan_with_one_step(
    *,
    tenant_id: str,
    repo_id: str,
    outputs_root: Path,
    goal: str = "Compile repository",
) -> None:
    """Pre-seed SQLite memory with an active plan that has one step."""
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


# --- Reproducible --------------------------------------------------------------


def test_success_criteria_reproducible_documented_compile_one_liner(tmp_path: Path) -> None:
    """Reproducible: documented compile one-liner runs and produces expected layout.

    The README/getting-started promise:
      uv run akc compile --tenant-id my-tenant --repo-id my-repo --outputs-root ./out

    With pre-seeded plan and minimal repo (as after a prior ingest/setup), compile
    must exit 0 and emit manifest + .akc/tests under <outputs-root>/<tenant>/<repo>.
    """
    tenant_id = "my-tenant"
    repo_id = "my-repo"
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
            ]
        )
    assert excinfo.value.code == 0, "documented compile one-liner must exit 0"

    assert (base / "manifest.json").exists(), "manifest.json must be emitted"
    tests_dir = base / ".akc" / "tests"
    assert tests_dir.is_dir(), ".akc/tests must exist"
    assert any(tests_dir.rglob("*.json")), "structured test artifacts under .akc/tests"


def test_success_criteria_reproducible_ingest_then_compile_offline(tmp_path: Path) -> None:
    """Reproducible: ingest (offline) then compile runs without API keys.

    Run the documented 2-step block with --embedder hash and --no-index,
    then set up compile state and run compile. Validates full offline path.
    """
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "readme.md").write_text("# Test doc\n", encoding="utf-8")

    tenant_id = "sc-tenant"
    repo_id = "sc-repo"
    outputs_root = tmp_path / "out"
    outputs_root.mkdir()

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "ingest",
                "--tenant-id",
                tenant_id,
                "--connector",
                "docs",
                "--input",
                str(docs_dir),
                "--embedder",
                "hash",
                "--no-index",
            ]
        )
    assert excinfo.value.code == 0, "ingest with --embedder hash --no-index must exit 0 (offline)"

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo2:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo2.value.code == 0, "compile after offline ingest must exit 0"

    base = outputs_root / tenant_id / repo_id
    assert (base / "manifest.json").exists()
    assert (base / ".akc" / "tests").is_dir()


# --- Documented ----------------------------------------------------------------


def test_success_criteria_documented_key_files_exist() -> None:
    """Documented: README and docs exist and are in place."""
    root = _repo_root()
    assert (root / "README.md").exists(), "README.md must exist"
    assert (root / "docs" / "getting-started.md").exists(), "docs/getting-started.md must exist"
    assert (root / "docs" / "architecture.md").exists(), "docs/architecture.md must exist"


def test_success_criteria_documented_readme_contains_one_liner_and_offline() -> None:
    """Documented: README contains reproducible one-liner and offline-first wording."""
    readme = (_repo_root() / "README.md").read_text(encoding="utf-8")
    assert "akc compile" in readme, "README must mention akc compile"
    assert "outputs-root" in readme or "outputs_root" in readme, "README must show outputs-root"
    assert "offline" in readme.lower() or "no API keys" in readme, "README must state offline/no API keys"
    assert "opt-in" in readme.lower(), "README must state cloud/opt-in"


def test_success_criteria_documented_getting_started_has_e2e_section() -> None:
    """Documented: getting-started has end-to-end run (ingest → compile → verify)."""
    gs = (_repo_root() / "docs" / "getting-started.md").read_text(encoding="utf-8")
    assert "ingest" in gs and "compile" in gs and "verify" in gs
    assert "End-to-end" in gs or "end-to-end" in gs
    assert "akc compile" in gs


def test_success_criteria_documented_architecture_design_principles() -> None:
    """Documented: architecture lists Reproducible and Transparent design principles."""
    arch = (_repo_root() / "docs" / "architecture.md").read_text(encoding="utf-8")
    assert "Reproducible" in arch
    assert "Transparent" in arch
    assert "opt-in" in arch.lower() or "offline" in arch.lower()


# --- Testable ------------------------------------------------------------------


def test_success_criteria_testable_cli_compile_tests_exist() -> None:
    """Testable: Unit and integration tests for CLI compile exist and are discoverable."""
    root = _repo_root()
    unit_cli_compile = root / "tests" / "unit" / "test_cli_compile.py"
    integration_cli_compile = root / "tests" / "integration" / "test_cli_compile_docs.py"
    assert unit_cli_compile.exists(), "tests/unit/test_cli_compile.py must exist"
    assert integration_cli_compile.exists(), "tests/integration/test_cli_compile_docs.py must exist"


# --- Extensible ----------------------------------------------------------------


def _get_command_subparsers(parser: object) -> dict:
    """Return the 'command' subparser choices (name -> subparser)."""
    for action in getattr(parser, "_actions", []):
        if getattr(action, "dest", None) == "command" and hasattr(action, "choices"):
            return action.choices or {}
    return {}


def test_success_criteria_extensible_ingest_connector_pluggable() -> None:
    """Extensible: Ingest exposes pluggable connector and index-backend via CLI choices."""
    from akc.cli import _build_parser

    parser = _build_parser()
    subparsers = _get_command_subparsers(parser)
    ingest_parser = subparsers["ingest"]
    connector_action = next(a for a in ingest_parser._actions if a.dest == "connector")
    assert connector_action.choices is not None
    assert "docs" in connector_action.choices and "openapi" in connector_action.choices
    index_action = next(a for a in ingest_parser._actions if a.dest == "index_backend")
    assert index_action.choices is not None
    assert "memory" in index_action.choices


def test_success_criteria_extensible_compile_no_connector_required() -> None:
    """Extensible: Compile does not require a specific connector; uses tenant/repo/outputs-root only."""
    from akc.cli import _build_parser

    parser = _build_parser()
    subparsers = _get_command_subparsers(parser)
    compile_parser = subparsers["compile"]
    dests = {a.dest for a in compile_parser._actions if a.dest}
    assert "tenant_id" in dests and "repo_id" in dests and "outputs_root" in dests
    assert "connector" not in dests, "compile must not hard-code a connector"


# --- Transparent --------------------------------------------------------------


def test_success_criteria_transparent_default_embedder_offline() -> None:
    """Transparent: Default embedder is offline (none)."""
    from akc.cli import _build_parser

    parser = _build_parser()
    ingest_parser = _get_command_subparsers(parser)["ingest"]
    embedder_action = next(a for a in ingest_parser._actions if a.dest == "embedder")
    assert embedder_action.default == "none", "default embedder must be none (offline)"


def test_success_criteria_transparent_compile_help_offline_default() -> None:
    """Transparent: Compile has no API-key options; CLI help advertises offline default."""
    from akc.cli import _build_parser

    parser = _build_parser()
    compile_parser = _get_command_subparsers(parser)["compile"]
    dests = {a.dest for a in compile_parser._actions if getattr(a, "dest", None)}
    api_key_like = {d for d in dests if d and ("api_key" in d or "token" in d)}
    assert not api_key_like, (
        "compile must not require API keys; found options: " + ", ".join(sorted(api_key_like))
    )
    # Main parser help (akc -h) includes compile description with "offline"
    full_help = parser.format_help()
    assert "offline" in full_help.lower() or "no API" in full_help
