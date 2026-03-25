from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.compile import Budget, CompileSession, ControllerConfig, TierConfig
from akc.compile.controller import ControllerResult
from akc.compile.executors import SubprocessExecutor
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.pass_registry import ARTIFACT_PASS_ORDER
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
        _ = request
        if stage in {
            "system_design",
            "orchestration_spec",
            "agent_coordination",
            "deployment_config",
        }:
            payload = {
                "spec_version": 1,
                "tenant_id": scope.tenant_id,
                "repo_id": scope.repo_id,
                "stage": stage,
            }
            return LLMResponse(text=json.dumps(payload, sort_keys=True), raw=None, usage=None)
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
    for st in res1.plan.steps:
        ins = st.inputs or {}
        assert isinstance(ins.get("intent_ref"), dict)
        assert ins["intent_ref"].get("stable_intent_sha256")
        assert "active_objectives" not in ins
    llm_span = next(span for span in res1.accounting["trace_spans"] if span.get("name") == "compile.llm.complete")
    llm_attrs = llm_span.get("attributes", {})
    assert llm_attrs["gen_ai.request.model"] == "fake-small"
    assert "gen_ai.input_tokens" in llm_attrs
    assert "gen_ai.output_tokens" in llm_attrs

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
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "intent").glob("*.json"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "intent").glob("*.json"))
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "design").glob("*.system_design.json"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "design").glob("*.system_design.json"))
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "orchestration").glob("*.orchestration.json"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "orchestration").glob("*.orchestration.json"))
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "orchestration").glob("*.orchestrator.py"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "orchestration").glob("*.orchestrator.ts"))
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "agents").glob("*.coordination.json"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "agents").glob("*.coordination.json"))
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "agents").glob("*.coordination_protocol.py"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "agents").glob("*.coordination_protocol.ts"))
    assert (tmp_path / "t1" / "repo1" / ".akc" / "deployment" / "docker-compose.yml").exists()
    assert (tmp_path / "t2" / "repo1" / ".akc" / "deployment" / "docker-compose.yml").exists()
    assert (tmp_path / "t1" / "repo1" / ".akc" / "deployment" / "k8s" / "deployment.yml").exists()
    assert (tmp_path / "t1" / "repo1" / ".akc" / "deployment" / "k8s" / "service.yml").exists()
    assert (tmp_path / "t1" / "repo1" / ".akc" / "deployment" / "k8s" / "configmap.yml").exists()
    assert any((tmp_path / "t1" / "repo1" / ".github" / "workflows").glob("akc_deploy_*.yml"))
    t1_run_manifest_files = list((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.manifest.json"))
    assert t1_run_manifest_files
    t1_run_manifest_obj = json.loads(t1_run_manifest_files[0].read_text(encoding="utf-8"))
    pass_names = [str(p.get("name", "")) for p in t1_run_manifest_obj.get("passes", [])]
    assert "deployment_config" in pass_names
    artifact_pass_names = [name for name in pass_names if name in ARTIFACT_PASS_ORDER]
    assert artifact_pass_names == list(ARTIFACT_PASS_ORDER)
    pass_by_name = {str(pass_obj.get("name", "")): pass_obj for pass_obj in t1_run_manifest_obj.get("passes", [])}
    for artifact_pass in artifact_pass_names:
        md = pass_by_name[artifact_pass].get("metadata", {})
        assert isinstance(md, dict)
        assert isinstance(md.get("artifact_paths"), list)
        assert isinstance(md.get("artifact_hashes"), dict)
        for artifact_path, digest in md["artifact_hashes"].items():
            assert t1_run_manifest_obj["output_hashes"][artifact_path] == digest
    trace_span_names = {str(span.get("name", "")) for span in t1_run_manifest_obj.get("trace_spans", [])}
    assert {f"compile.artifact.{n}" for n in ARTIFACT_PASS_ORDER}.issubset(trace_span_names)

    bundle_manifest_obj = json.loads((tmp_path / "t1" / "repo1" / "manifest.json").read_text(encoding="utf-8"))
    artifact_pass_md = bundle_manifest_obj.get("metadata", {}).get("artifact_passes", {})
    assert artifact_pass_md.get("order") == list(ARTIFACT_PASS_ORDER)
    assert artifact_pass_md.get("groups", {}).get("specs") == [
        "system_design",
        "orchestration_spec",
        "agent_coordination",
    ]
    assert artifact_pass_md.get("groups", {}).get("deployment_configs") == ["delivery_plan", "deployment_config"]
    assert isinstance(artifact_pass_md.get("output_hashes"), dict)

    # IntentStore integration: active intent pointers should be scoped tenant+repo.
    assert (tmp_path / ".akc" / "intent" / "t1" / "repo1" / "active.json").exists()
    assert (tmp_path / ".akc" / "intent" / "t2" / "repo1" / "active.json").exists()
    # Observability artifacts should be emitted per run and scope.
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.spans.json"))
    assert any((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.otel.jsonl"))
    assert any((tmp_path / "t2" / "repo1" / ".akc" / "run").glob("*.costs.json"))
    t1_cost_files = list((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.costs.json"))
    assert t1_cost_files
    t1_cost_payload = json.loads(t1_cost_files[0].read_text(encoding="utf-8"))
    assert "estimated_cost_usd" in t1_cost_payload
    assert t1_cost_payload["currency"] == "USD"
    assert t1_cost_payload["pricing_version"] == "static-v1"
    assert isinstance(t1_cost_payload.get("by_pass"), dict)
    assert "generate" in t1_cost_payload["by_pass"]
    assert isinstance(t1_cost_payload.get("by_component"), dict)
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
            compile_succeeded=True,
            intent_satisfied=True,
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


def test_compile_session_skips_artifact_passes_for_failed_compile(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    session = CompileSession.from_memory(tenant_id="t1", repo_id="repo1")
    plan = session.memory.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    session.memory.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=plan.id)

    def _fake_run_compile_loop(**kwargs: Any) -> ControllerResult:
        active_id = kwargs["plan_store"].get_active_plan_id(tenant_id="t1", repo_id="repo1")
        loaded_plan = kwargs["plan_store"].load_plan(
            tenant_id="t1",
            repo_id="repo1",
            plan_id=active_id,
        )
        assert loaded_plan is not None
        return ControllerResult(
            status="failed",
            plan=loaded_plan,
            best_candidate=None,
            accounting={},
            compile_succeeded=False,
            intent_satisfied=False,
        )

    monkeypatch.setattr("akc.compile.session.run_compile_loop", _fake_run_compile_loop)

    result = session.run(
        goal="Goal",
        llm=_FakeLLM(),
        config=_mk_config(command=["python", "-c", "print('ok')"]),
        outputs_root=tmp_path,
    )

    assert result.compile_succeeded is False
    assert not any((tmp_path / "t1" / "repo1" / ".akc" / "design").glob("*.system_design.json"))
    manifest_files = list((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.manifest.json"))
    assert manifest_files
    manifest_obj = json.loads(manifest_files[0].read_text(encoding="utf-8"))
    pass_names = [str(p.get("name", "")) for p in manifest_obj.get("passes", [])]
    assert "system_design" not in pass_names
    assert "orchestration_spec" not in pass_names
    assert "agent_coordination" not in pass_names
    assert "deployment_config" not in pass_names


def test_compile_session_full_replay_reuses_artifact_pass_outputs(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    ex = SubprocessExecutor(work_root=tmp_path)
    cfg = _mk_config(command=["python", "-c", "print('ok')"])

    live = CompileSession.from_memory(tenant_id="t1", repo_id="repo1")
    live_plan = live.memory.plan_state.create_plan(
        tenant_id="t1",
        repo_id="repo1",
        goal="Goal",
        initial_steps=["step1"],
    )
    live.memory.plan_state.set_active_plan(tenant_id="t1", repo_id="repo1", plan_id=live_plan.id)
    live_result = live.run(goal="Goal", llm=_FakeLLM(), executor=ex, config=cfg, outputs_root=tmp_path)
    assert live_result.compile_succeeded is True

    seed_manifest_path = next((tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.manifest.json"))

    replay = CompileSession.from_memory(tenant_id="t1", repo_id="repo1")

    def _fake_run_compile_loop(**kwargs: Any) -> ControllerResult:
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
            compile_succeeded=True,
            intent_satisfied=True,
        )

    def _unexpected(*args: Any, **kwargs: Any) -> Any:
        raise AssertionError("artifact pass should have been reused from replay manifest")

    monkeypatch.setattr("akc.compile.session.run_compile_loop", _fake_run_compile_loop)
    monkeypatch.setattr("akc.compile.session.run_system_design_pass", _unexpected)
    monkeypatch.setattr("akc.compile.session.run_orchestration_spec_pass", _unexpected)
    monkeypatch.setattr("akc.compile.session.run_agent_coordination_pass", _unexpected)
    monkeypatch.setattr("akc.compile.session.run_delivery_plan_pass", _unexpected)
    monkeypatch.setattr("akc.compile.session.run_runtime_bundle_pass", _unexpected)
    monkeypatch.setattr("akc.compile.session.run_deployment_config_pass", _unexpected)

    replay_result = replay.run(
        goal="Goal",
        llm=_FakeLLM(),
        config=cfg,
        outputs_root=tmp_path,
        replay_mode="full_replay",
        replay_manifest_path=seed_manifest_path,
    )
    assert replay_result.compile_succeeded is True

    manifest_files = sorted(
        (tmp_path / "t1" / "repo1" / ".akc" / "run").glob("*.manifest.json"),
        key=lambda path: path.stat().st_mtime,
    )
    replay_manifest_obj = json.loads(manifest_files[-1].read_text(encoding="utf-8"))
    by_name = {str(p.get("name", "")): p for p in replay_manifest_obj.get("passes", [])}
    for pass_name in (
        "system_design",
        "orchestration_spec",
        "agent_coordination",
        "delivery_plan",
        "runtime_bundle",
        "deployment_config",
    ):
        metadata = by_name[pass_name].get("metadata", {})
        assert metadata.get("reused_from_replay_manifest") is True
        assert metadata.get("replay_source_run_id")

    run_dir = tmp_path / "t1" / "repo1" / ".akc" / "run"
    seed_manifest_obj = json.loads(seed_manifest_path.read_text(encoding="utf-8"))
    replay_decisions_path = run_dir / f"{replay_manifest_obj['run_id']}.replay_decisions.json"
    replay_decisions_obj = json.loads(replay_decisions_path.read_text(encoding="utf-8"))
    assert replay_decisions_obj["replay_source_run_id"] == seed_manifest_obj["run_id"]
    first_record = replay_decisions_obj["decisions"][0]
    assert first_record["inputs_snapshot"]["baseline_present"] is True
    assert first_record["inputs_snapshot"]["baseline_run_id"] == replay_decisions_obj["replay_source_run_id"]

    control_plane = replay_manifest_obj.get("control_plane", {})
    ref = control_plane.get("replay_decisions_ref", {})
    assert ref.get("path") == f".akc/run/{replay_manifest_obj['run_id']}.replay_decisions.json"
