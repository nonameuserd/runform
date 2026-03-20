from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.compile import Budget, CompileSession, ControllerConfig, TierConfig
from akc.compile.controller import ControllerResult
from akc.compile.executors import SubprocessExecutor
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.run import RunManifest


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
        tool_allowlist=("llm.complete", "executor.run"),
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
    # Observability artifacts should be emitted per run and scope.
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.spans.json"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "run").glob("*.costs.json"))
    t1_cost_files = list((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.costs.json"))
    assert t1_cost_files
    t1_cost_payload = json.loads(t1_cost_files[0].read_text(encoding="utf-8"))
    assert "estimated_cost_usd" in t1_cost_payload
    assert isinstance(t1_cost_payload.get("tenant_totals"), dict)
    assert (tmp_path / "t1" / ".akc" / "control" / "metrics.sqlite").exists()


def test_compile_session_preserves_control_plane_when_loading_replay_manifest(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    captured: dict[str, Any] = {}
    replay_manifest = RunManifest(
        run_id="run_replay",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256="a" * 64,
        replay_mode="full_replay",
        control_plane={"policy_decisions": [{"allowed": True, "reason": "seeded"}]},
    )
    replay_manifest_path = tmp_path / "seed.manifest.json"
    replay_manifest_path.write_text(json.dumps(replay_manifest.to_json_obj()), encoding="utf-8")

    session = CompileSession.from_memory(tenant_id="t1", repo_id="repo1")
    plan = session.memory.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    session.memory.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    def _fake_run_compile_loop(*, replay_manifest: RunManifest | None = None, **kwargs: Any) -> Any:
        captured["replay_manifest"] = replay_manifest
        active_id = kwargs["plan_store"].get_active_plan_id(tenant_id="t1", repo_id="repo1")
        loaded_plan = kwargs["plan_store"].load_plan(
            tenant_id="t1",
            repo_id="repo1",
            plan_id=active_id,
        )
        assert loaded_plan is not None
        return ControllerResult(
            status="succeeded",
            plan=loaded_plan,
            best_candidate=None,
            accounting={},
        )

    monkeypatch.setattr("akc.compile.session.run_compile_loop", _fake_run_compile_loop)

    session.run(
        goal="Goal",
        llm=_FakeLLM(),
        config=_mk_config(command=["python", "-c", "print('ok')"]),
        outputs_root=None,
        replay_mode="full_replay",
        replay_manifest_path=replay_manifest_path,
    )

    effective = captured.get("replay_manifest")
    assert isinstance(effective, RunManifest)
    assert effective.control_plane == replay_manifest.control_plane
