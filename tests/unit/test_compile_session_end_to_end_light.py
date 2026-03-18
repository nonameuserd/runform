from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from akc.compile import Budget, CompileSession, ControllerConfig, TierConfig
from akc.compile.executors import SubprocessExecutor
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope


@dataclass(frozen=True)
class _FakeLLM(LLMBackend):
    """Minimal deterministic backend for integration-light compile runs."""

    def complete(  # type: ignore[override]
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        _ = scope
        _ = stage
        _ = request
        # Controller expects "unified diff only" text; default policy requires test paths
        # when non-test paths are touched. Include both a code path and a test path so
        # the loop can promote without policy veto (executor does not apply patches yet).
        return LLMResponse(
            text=(
                "--- a/README.md\n"
                "+++ b/README.md\n"
                "@@ -1 +1 @@\n"
                "-old\n"
                "+new\n"
                "\n"
                "--- a/tests/test_readme.py\n"
                "+++ b/tests/test_readme.py\n"
                "@@\n"
                "+def test_ok():\n"
                "+    assert True\n"
            ),
            raw=None,
            usage=None,
        )


def _mk_config(*, command: list[str]) -> ControllerConfig:
    tiers = {
        "small": TierConfig(name="small", llm_model="fake-small", temperature=0.0),
        "medium": TierConfig(name="medium", llm_model="fake-medium", temperature=0.0),
        "large": TierConfig(name="large", llm_model="fake-large", temperature=0.0),
    }
    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=Budget(max_llm_calls=3, max_repairs_per_step=1, max_iterations_total=2),
        # Use explicit test fields to exercise tests-by-default plumbing.
        test_command=tuple(command),
        test_timeout_s=2.0,
        test_mode="full",
    )


def test_compile_session_run_integration_light_uses_isolated_workdir(tmp_path: Path) -> None:
    # Use an actual subprocess-backed executor, but keep command trivial and portable.
    ex = SubprocessExecutor(work_root=tmp_path)
    # Ensure stdout is non-empty so the controller can persist a non-empty test_result.
    cfg = _mk_config(command=["python", "-c", "print('ok')"])

    s1 = CompileSession.from_memory(tenant_id="t1", repo_id="repo1")
    # Seed a plan with at least one step so the controller actually executes.
    p1 = s1.memory.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    s1.memory.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=p1.id)
    res1 = s1.run(goal="Goal", llm=_FakeLLM(), executor=ex, config=cfg, outputs_root=tmp_path)
    assert res1.status == "succeeded"

    # Run a second session with a different tenant to ensure per-tenant segregation.
    s2 = CompileSession.from_memory(tenant_id="t2", repo_id="repo1")
    p2 = s2.memory.plan_state.create_plan(
        tenant_id="t2",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    s2.memory.plan_state.set_active_plan(tenant_id="t2", repo_id="repo1", plan_id=p2.id)
    res2 = s2.run(goal="Goal", llm=_FakeLLM(), executor=ex, config=cfg, outputs_root=tmp_path)
    assert res2.status == "succeeded"

    # Workdirs should be created under tmp_path/<tenant>/<repo>.
    assert (tmp_path / "t1" / "repo1").exists()
    assert (tmp_path / "t2" / "repo1").exists()

    # Manifests and emitted artifacts should also be scoped per-tenant/repo under tmp_path.
    assert (tmp_path / "t1" / "repo1" / "manifest.json").exists()
    assert (tmp_path / "t2" / "repo1" / "manifest.json").exists()
    # Tests + verification artifacts should live under the correct tenant/repo only.
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "tests").rglob("*.json"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "tests").rglob("*.json"))
