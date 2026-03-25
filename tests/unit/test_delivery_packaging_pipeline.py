from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.delivery import store as delivery_store
from akc.delivery.compile_handoff import run_manifest_path
from akc.delivery.orchestrate import run_delivery_build_and_package
from akc.run.manifest import PassRecord, RunManifest


def test_run_delivery_build_and_package_after_compile(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="build an app",
        recipients=["a@example.com"],
        platforms=["web", "ios"],
        release_mode="beta",
        delivery_version="1.2.3",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    delivery_store.update_session_compile_stage(
        project_dir=tmp_path,
        delivery_id=did,
        run_id="run-1",
        succeeded=True,
    )
    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["web", "ios"],
        release_mode="beta",
        delivery_version="1.2.3",
        compile_run_id="run-1",
        tenant_id="t1",
        repo_id="r1",
    )
    assert out["ok"] is True
    assert out["provider_versions"]["ios_marketing_version"] == "1.2.3"
    dist = out.get("distribution")
    assert isinstance(dist, dict), "packaging must run distribution dispatch; missing distribution summary"
    assert dist.get("ok") is True, dist
    jobs = dist.get("jobs") or {}
    # If the dispatch loop body is accidentally skipped, jobs stay empty while ok_all stays True — assert real work.
    assert "web:beta" in jobs and "ios:beta" in jobs, jobs
    assert jobs["web:beta"].get("ok") is True
    assert jobs["ios:beta"].get("ok") is True

    sess = delivery_store.load_session(tmp_path, did)
    assert sess["per_platform"]["web"]["channels"]["beta"]["status"] == "completed"
    assert sess["per_platform"]["ios"]["channels"]["beta"]["status"] == "completed"
    assert sess["pipeline"]["build"]["status"] == "completed"
    assert sess["pipeline"]["package"]["status"] == "completed"
    assert sess["session_phase"] == "distributing"
    assert sess["delivery_version"] == "1.2.3"
    ev = delivery_store.load_events(tmp_path, did)
    assert any(e.get("event_type") == "delivery.build.packaged" for e in ev)
    assert any(e.get("event_type") == "delivery.invite.sent" for e in ev)
    contract = tmp_path / ".akc" / "delivery" / did / "activation_client_contract.v1.json"
    assert contract.is_file()


def test_packaging_web_deployed_url_from_delivery_plan_handoff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    rid = "run-with-plan"
    (tmp_path / ".akc" / "run").mkdir(parents=True)
    manifest = RunManifest(
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
                name="delivery_plan",
                status="succeeded",
                metadata={"delivery_plan_path": f".akc/deployment/{rid}.delivery_plan.json"},
            ),
        ),
    )
    run_manifest_path(project_dir=tmp_path, compile_run_id=rid).write_text(
        json.dumps(manifest.to_json_obj()),
        encoding="utf-8",
    )
    plan = {
        "targets": [{"target_class": "web_app", "target_id": "w", "name": "x", "domain": "invite.example.org"}],
        "promotion_readiness": {"status": "ready"},
    }
    dd = tmp_path / ".akc" / "deployment"
    dd.mkdir(parents=True, exist_ok=True)
    (dd / f"{rid}.delivery_plan.json").write_text(json.dumps(plan), encoding="utf-8")

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="build an app",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        delivery_version="1.0.0",
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
    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["web"],
        release_mode="beta",
        delivery_version="1.0.0",
        compile_run_id=rid,
        tenant_id="t1",
        repo_id="r1",
    )
    assert out["ok"] is True
    dist = out.get("distribution")
    assert isinstance(dist, dict)
    jobs = dist.get("jobs") or {}
    assert "web:beta" in jobs and jobs["web:beta"].get("ok") is True, jobs
    sess = delivery_store.load_session(tmp_path, did)
    assert sess["per_platform"]["web"]["channels"]["beta"]["status"] == "completed"

    web = out["per_platform"]["web"]
    assert web["outputs"]["deployed_url"] == "https://invite.example.org"


def test_packaging_preflight_defaults_strict_for_store_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    monkeypatch.delenv("AKC_PACKAGING_ENFORCE_PREFLIGHT", raising=False)
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="build and ship",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    delivery_store.update_session_compile_stage(
        project_dir=tmp_path,
        delivery_id=did,
        run_id="run-1",
        succeeded=True,
    )

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="run-1",
        tenant_id="t1",
        repo_id="r1",
    )
    assert out["ok"] is False
    assert out["error"] == "packaging preflight blocked"
    assert out["preflight_issues"]
    sess = delivery_store.load_session(tmp_path, did)
    assert sess["pipeline"]["build"]["status"] == "blocked"
    assert sess["pipeline"]["package"]["status"] == "blocked"


def test_packaging_preflight_can_be_relaxed_for_store_mode(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    monkeypatch.setenv("AKC_PACKAGING_ENFORCE_PREFLIGHT", "0")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="build and ship",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    delivery_store.update_session_compile_stage(
        project_dir=tmp_path,
        delivery_id=did,
        run_id="run-1",
        succeeded=True,
    )

    out = run_delivery_build_and_package(
        project_dir=tmp_path,
        delivery_id=did,
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="run-1",
        tenant_id="t1",
        repo_id="r1",
    )
    assert out["ok"] is True
    dist = out.get("distribution")
    assert isinstance(dist, dict)
    assert dist.get("ok") is True
