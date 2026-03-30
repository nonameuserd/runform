from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.delivery import packaging_adapters
from akc.delivery import store as delivery_store
from akc.delivery.compile_handoff import run_manifest_path
from akc.delivery.orchestrate import run_delivery_build_and_package
from akc.run.manifest import PassRecord, RunManifest


def _seed_execution_workspace(project_dir: Path, *, rid: str) -> None:
    (project_dir / ".akc" / "run").mkdir(parents=True, exist_ok=True)
    edir = project_dir / ".akc" / "execution"
    edir.mkdir(parents=True, exist_ok=True)
    workspace_root = project_dir / ".akc" / "execution" / rid / "workspace" / "apps" / "universal" / "web"
    workspace_root.mkdir(parents=True, exist_ok=True)
    (workspace_root / "index.html").write_text("<html><body>ok</body></html>", encoding="utf-8")
    manifest = {
        "run_id": rid,
        "tenant_id": "t1",
        "repo_id": "r1",
        "workspace_root": f".akc/execution/{rid}/workspace",
        "package_manager": "npm",
        "toolchain": {"node": "node", "eas_cli": "eas", "package_manager": "npm"},
        "runtime_profile": "typescript_node",
        "expo": {
            "project_id": "expo-project-1",
            "ios_bundle_identifier": "com.akc.repo1",
            "android_package": "com.akc.repo1",
        },
        "build_profiles": {"beta": "preview", "store": "production"},
        "build_entrypoints": {
            "web": {"cwd": f".akc/execution/{rid}/workspace/apps/universal"},
            "ios": {"cwd": f".akc/execution/{rid}/workspace/apps/universal"},
            "android": {"cwd": f".akc/execution/{rid}/workspace/apps/universal"},
        },
        "expected_outputs": {},
        "targets": [],
        "generated_files": [
            {
                "path": f".akc/execution/{rid}/workspace/apps/universal/web/index.html",
                "sha256": "a" * 64,
                "size_bytes": 28,
            }
        ],
        "workspace_fingerprint": "b" * 64,
    }
    (edir / f"{rid}.execution_workspace_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    mpath = run_manifest_path(project_dir=project_dir, compile_run_id=rid)
    if mpath.is_file():
        raw = json.loads(mpath.read_text(encoding="utf-8"))
        passes = list(raw.get("passes") or [])
        passes.append(
            {
                "name": "execution_workspace",
                "status": "succeeded",
                "metadata": {
                    "execution_workspace_manifest_path": f".akc/execution/{rid}.execution_workspace_manifest.json"
                },
            }
        )
        raw["passes"] = passes
        mpath.write_text(json.dumps(raw), encoding="utf-8")
    else:
        run_manifest = RunManifest(
            run_id=rid,
            tenant_id="t1",
            repo_id="r1",
            ir_sha256="a" * 64,
            replay_mode="live",
            stable_intent_sha256="b" * 64,
            intent_semantic_fingerprint="c" * 16,
            intent_goal_text_fingerprint="d" * 16,
            passes=(
                PassRecord(
                    name="execution_workspace",
                    status="succeeded",
                    metadata={
                        "execution_workspace_manifest_path": f".akc/execution/{rid}.execution_workspace_manifest.json"
                    },
                ),
            ),
        )
        mpath.write_text(json.dumps(run_manifest.to_json_obj()), encoding="utf-8")


def _create_delivery(tmp_path: Path, *, rid: str, platforms: list[str], release_mode: str = "beta") -> str:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="build an app",
        recipients=["a@example.com"],
        platforms=platforms,
        release_mode=release_mode,
        delivery_version="1.2.3",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    delivery_store.update_session_compile_stage(
        project_dir=tmp_path,
        delivery_id=did,
        run_id=rid,
        succeeded=True,
    )
    return did


def _fake_packaging_runner_factory(tmp_path: Path) -> object:
    def _runner(*, argv: list[str], cwd: Path, env: dict[str, str] | None = None) -> dict[str, object]:
        _ = env
        if argv[:3] == ["npx", "expo", "export"]:
            out_dir = Path(argv[-1])
            out_dir.mkdir(parents=True, exist_ok=True)
            (out_dir / "index.html").write_text("<html>exported</html>", encoding="utf-8")
            return {"argv": argv, "cwd": str(cwd), "exit_code": 0, "stdout": "", "stderr": ""}
        if argv[:2] == ["vercel", "deploy"]:
            return {
                "argv": argv,
                "cwd": str(cwd),
                "exit_code": 0,
                "stdout": json.dumps({"id": "dep-1", "url": "https://invite.example.org"}),
                "stderr": "",
            }
        if argv[:3] == ["eas", "build", "--platform"]:
            platform = argv[3]
            if platform == "ios":
                return {
                    "argv": argv,
                    "cwd": str(cwd),
                    "exit_code": 0,
                    "stdout": json.dumps(
                        {"id": "build-ios-1", "artifacts": {"buildUrl": "https://builds.example.org/ios"}}
                    ),
                    "stderr": "",
                }
            return {
                "argv": argv,
                "cwd": str(cwd),
                "exit_code": 0,
                "stdout": json.dumps(
                    {"id": "build-android-1", "artifacts": {"buildUrl": "https://builds.example.org/android"}}
                ),
                "stderr": "",
            }
        return {"argv": argv, "cwd": str(cwd), "exit_code": 0, "stdout": "", "stderr": ""}

    return _runner


def test_run_delivery_build_and_package_plan_mode_skips_distribution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    monkeypatch.setenv("AKC_DELIVERY_WEB_INVITE_BASE_URL", "https://beta.example.org")
    rid = "run-plan-1"
    _seed_execution_workspace(tmp_path, rid=rid)
    did = _create_delivery(tmp_path, rid=rid, platforms=["web", "ios"])

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["web", "ios"],
        release_mode="beta",
        delivery_version="1.2.3",
        compile_run_id=rid,
        packaging_mode="plan",
        store_submit_mode="manual",
        tenant_id="t1",
        repo_id="r1",
    )

    assert out["ok"] is True
    assert out["summary"]["mode"] == "plan"
    assert out["summary"]["planned_only_platforms"] == ["web", "ios"]
    dist = out.get("distribution") or {}
    assert dist.get("skipped") is True
    assert dist.get("reason") == "plan_only"

    web = out["per_platform"]["web"]["outputs"]
    ios = out["per_platform"]["ios"]["outputs"]
    assert web["execution_mode"] == "plan"
    assert web["artifact_authority"] == "planned"
    assert web["distribution_ready"] is False
    assert web["distribution_inputs"]["beta"]["ready"] is False
    assert web["distribution_inputs"]["store"]["ready"] is False
    assert web["distribution_results"] == {}
    assert ios["artifact_authority"] == "planned"
    assert ios["distribution_ready"] is False
    assert ios["distribution_inputs"]["beta"]["provider_kind"] == "testflight"
    assert ios["distribution_inputs"]["store"]["provider_kind"] == "app_store_release"

    sess = delivery_store.load_session(tmp_path, did)
    assert sess["pipeline"]["distribution"]["status"] == "skipped"
    assert sess["session_phase"] == "packaging"


def test_execute_mode_records_authoritative_web_and_mobile_outputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    monkeypatch.setenv("AKC_DELIVERY_WEB_INVITE_BASE_URL", "https://invite.example.org")
    monkeypatch.setattr(packaging_adapters, "_run_packaging_command", _fake_packaging_runner_factory(tmp_path))
    (tmp_path / "vercel.json").write_text("{}", encoding="utf-8")
    rid = "run-exec-1"
    _seed_execution_workspace(tmp_path, rid=rid)
    did = _create_delivery(tmp_path, rid=rid, platforms=["web", "ios"])

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["web", "ios"],
        release_mode="beta",
        delivery_version="1.2.3",
        compile_run_id=rid,
        packaging_mode="execute",
        store_submit_mode="manual",
        tenant_id="t1",
        repo_id="r1",
    )

    assert out["ok"] is True
    assert out["summary"]["mode"] == "execute"
    assert out["summary"]["ready_platforms"] == ["web", "ios"]
    dist = out.get("distribution") or {}
    assert dist.get("ok") is True
    jobs = dist.get("jobs") or {}
    assert jobs["web:beta"]["ok"] is True
    assert jobs["ios:beta"]["ok"] is True

    web = out["per_platform"]["web"]["outputs"]
    ios = out["per_platform"]["ios"]["outputs"]
    assert web["artifact_authority"] == "authoritative"
    assert web["distribution_ready"] is True
    assert web["deploy_result"]["hosting_url"] == "https://invite.example.org"
    assert web["distribution_inputs"]["beta"]["hosting_url"] == "https://invite.example.org"
    assert web["distribution_inputs"]["beta"]["ready"] is True
    assert ios["distribution_ready"] is True
    assert ios["build_result"]["build_id"] == "build-ios-1"
    assert ios["build_result"]["artifact_url"] == "https://builds.example.org/ios"
    assert ios["distribution_inputs"]["beta"]["eas_build_id"] == "build-ios-1"
    assert ios["distribution_inputs"]["store"]["ready"] is True

    sess = delivery_store.load_session(tmp_path, did)
    assert sess["per_platform"]["web"]["channels"]["beta"]["status"] == "completed"
    assert sess["per_platform"]["ios"]["channels"]["beta"]["status"] == "completed"
    assert sess["pipeline"]["package"]["outputs"]["summary"]["distribution_ready"] is True


def test_execute_mode_fails_closed_when_authoritative_outputs_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")

    def _runner(*, argv: list[str], cwd: Path, env: dict[str, str] | None = None) -> dict[str, object]:
        _ = (cwd, env)
        if argv[:3] == ["eas", "build", "--platform"]:
            return {"argv": argv, "cwd": str(cwd), "exit_code": 0, "stdout": "{}", "stderr": ""}
        return {"argv": argv, "cwd": str(cwd), "exit_code": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(packaging_adapters, "_run_packaging_command", _runner)
    rid = "run-exec-missing"
    _seed_execution_workspace(tmp_path, rid=rid)
    did = _create_delivery(tmp_path, rid=rid, platforms=["ios"])

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["ios"],
        release_mode="beta",
        delivery_version="1.2.3",
        compile_run_id=rid,
        packaging_mode="execute",
        store_submit_mode="manual",
        tenant_id="t1",
        repo_id="r1",
    )

    assert out["ok"] is False
    assert "authoritative build outputs" in str(out["error"] or "")
    assert out["per_platform"]["ios"]["outputs"]["distribution_ready"] is False


def test_execute_mode_preserves_mobile_packaging_failure_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")

    def _runner(*, argv: list[str], cwd: Path, env: dict[str, str] | None = None) -> dict[str, object]:
        _ = (cwd, env)
        if argv[:2] == ["npm", "install"]:
            return {"argv": argv, "cwd": str(cwd), "exit_code": 0, "stdout": "", "stderr": ""}
        if argv[:3] == ["eas", "build", "--platform"]:
            return {
                "argv": argv,
                "cwd": str(cwd),
                "exit_code": 1,
                "stdout": "",
                "stderr": "transient upstream 503",
            }
        return {"argv": argv, "cwd": str(cwd), "exit_code": 0, "stdout": "", "stderr": ""}

    monkeypatch.setattr(packaging_adapters, "_run_packaging_command", _runner)
    rid = "run-exec-flaky-ios"
    _seed_execution_workspace(tmp_path, rid=rid)
    did = _create_delivery(tmp_path, rid=rid, platforms=["ios"])

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["ios"],
        release_mode="beta",
        delivery_version="1.2.3",
        compile_run_id=rid,
        packaging_mode="execute",
        store_submit_mode="manual",
        tenant_id="t1",
        repo_id="r1",
    )

    assert out["ok"] is False
    assert "ios packaging command failed" in str(out["error"] or "")
    outputs = out["per_platform"]["ios"]["outputs"]
    assert outputs["execution_mode"] == "execute"
    assert outputs["artifact_authority"] == "authoritative"
    assert outputs["distribution_ready"] is False
    assert outputs["failed_command"][:3] == ["eas", "build", "--platform"]
    assert outputs["failed_command_index"] == 1
    assert len(outputs["command_logs"]) == 2
    assert outputs["command_logs"][1]["exit_code"] == 1
    assert outputs["build_result"]["planned"] is False


def test_default_execute_can_fallback_to_inspectable_plan_when_delivery_prereqs_are_missing(
    tmp_path: Path,
) -> None:
    rid = "run-fallback-1"
    _seed_execution_workspace(tmp_path, rid=rid)
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="build an app",
        recipients=["a@example.com"],
        platforms=["web", "ios"],
        release_mode="both",
        delivery_version="1.2.3",
        tenant_id="t1",
        repo_id="r1",
    )
    did = str(summary["delivery_id"])
    delivery_store.update_session_compile_stage(
        project_dir=tmp_path,
        delivery_id=did,
        run_id=rid,
        succeeded=True,
    )

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["web", "ios"],
        release_mode="both",
        delivery_version="1.2.3",
        compile_run_id=rid,
        packaging_mode="execute",
        store_submit_mode="auto",
        tenant_id="t1",
        repo_id="r1",
        allow_plan_fallback=True,
    )

    assert out["ok"] is True
    assert out["summary"]["mode"] == "plan"
    assert out["summary"]["mode_resolution"]["outcome"] == "inspectable_plan"
    assert out["summary"]["mode_resolution"]["reason"] == "delivery_preflight_blocked"
    assert out["summary"]["mode_resolution"]["auto_fallback"] is True
    assert out["requested_preflight_issues"]
    dist = out.get("distribution") or {}
    assert dist.get("skipped") is True
    assert dist.get("reason") == "plan_only"

    sess = delivery_store.load_session(tmp_path, did)
    pkg_summary = sess["pipeline"]["package"]["outputs"]["summary"]
    assert pkg_summary["mode"] == "plan"
    assert pkg_summary["mode_resolution"]["reason"] == "delivery_preflight_blocked"


def test_packaging_preflight_defaults_strict_for_store_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    monkeypatch.delenv("AKC_PACKAGING_ENFORCE_PREFLIGHT", raising=False)
    _seed_execution_workspace(tmp_path, rid="run-1")
    did = _create_delivery(tmp_path, rid="run-1", platforms=["ios"], release_mode="store")

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="run-1",
        packaging_mode="execute",
        store_submit_mode="auto",
        tenant_id="t1",
        repo_id="r1",
    )
    assert out["ok"] is False
    assert out["error"] == "packaging preflight blocked"
    assert out["preflight_issues"]
    sess = delivery_store.load_session(tmp_path, did)
    assert sess["pipeline"]["build"]["status"] == "blocked"
    assert sess["pipeline"]["package"]["status"] == "blocked"


def test_plan_mode_skips_execute_only_prereqs_even_when_store_preflight_is_strict(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    monkeypatch.delenv("AKC_PACKAGING_ENFORCE_PREFLIGHT", raising=False)
    _seed_execution_workspace(tmp_path, rid="run-1")
    did = _create_delivery(tmp_path, rid="run-1", platforms=["ios"], release_mode="store")

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="run-1",
        packaging_mode="plan",
        store_submit_mode="auto",
        tenant_id="t1",
        repo_id="r1",
    )
    assert out["ok"] is True
    assert out["preflight_issues"] == []
    dist = out.get("distribution") or {}
    assert dist.get("skipped") is True
    assert out["summary"]["mode"] == "plan"
    assert out["summary"]["mode_resolution"]["reason"] == "explicit_plan"
