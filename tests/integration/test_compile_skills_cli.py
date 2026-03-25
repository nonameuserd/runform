"""Integration: compile injects project Agent Skills into the patch LLM system prompt."""

from __future__ import annotations

from pathlib import Path

import pytest

from akc.cli import compile as compile_cli
from akc.cli import main
from akc.compile.interfaces import LLMRequest, LLMResponse, TenantRepoScope
from akc.memory.facade import build_memory

_SKILL_BODY_MARKER = "INTEGRATION_AGENT_SKILLS_BODY_MARKER_7e4a91c2"


def _executor_cwd(outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    base = outputs_root / tenant_id / repo_id
    return base / tenant_id / repo_id


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


def test_compile_cli_generate_system_prompt_includes_skill_body(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured_system: list[str] = []

    class _CapturingOfflineLLM(compile_cli._OfflineLLM):
        def complete(
            self,
            *,
            scope: TenantRepoScope,
            stage: str,
            request: LLMRequest,
        ) -> LLMResponse:
            if stage == "generate":
                for msg in request.messages:
                    if msg.role == "system":
                        captured_system.append(msg.content)
                        break
            return super().complete(scope=scope, stage=stage, request=request)

    monkeypatch.setattr(compile_cli, "_OfflineLLM", _CapturingOfflineLLM)

    tenant_id = "skill-cli-tenant"
    repo_id = "skill-cli-repo"
    outputs_root = tmp_path
    base = outputs_root / tenant_id / repo_id

    skill_dir = base / ".cursor" / "skills" / "test-skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        f"---\nname: test-skill\ndescription: integration fixture skill\n---\n\n{_SKILL_BODY_MARKER}\n",
        encoding="utf-8",
    )

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))

    memory_db = base / ".akc" / "memory.sqlite"
    memory_db.parent.mkdir(parents=True, exist_ok=True)
    mem = build_memory(backend="sqlite", sqlite_path=str(memory_db))
    plan = mem.plan_state.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="Compile repository",
        initial_steps=["Implement goal"],
    )
    mem.plan_state.set_active_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan.id)

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
                "--compile-skill",
                "test-skill",
            ]
        )
    assert excinfo.value.code == 0, "compile should exit 0 on success"

    assert captured_system, "expected at least one generate-stage LLM call with a system message"
    joined = "\n".join(captured_system)
    assert _SKILL_BODY_MARKER in joined
    assert "### SKILL: test-skill" in joined
