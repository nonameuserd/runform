from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from akc.delivery import provider_clients
from akc.delivery import store as delivery_store
from akc.delivery.distribution_dispatch import _lane_binding, run_delivery_distribution


def _prime_package_outputs(
    *,
    project_dir: Path,
    delivery_id: str,
    per_platform_outputs: dict[str, Any],
) -> None:
    normalized: dict[str, Any] = {}
    for platform, raw in per_platform_outputs.items():
        row = dict(raw) if isinstance(raw, dict) else {}
        outputs = dict(row.get("outputs") or {})
        outputs.setdefault("execution_mode", "execute")
        outputs.setdefault("artifact_authority", "authoritative")
        outputs.setdefault("distribution_ready", True)
        row["outputs"] = outputs
        normalized[platform] = row
    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="package",
        status="completed",
        outputs={"summary": {"mode": "execute", "distribution_ready": True}, "per_platform": normalized},
    )


def test_ios_store_without_ipa_is_not_treated_as_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)
    monkeypatch.setattr(
        provider_clients,
        "asc_verify_api_token",
        lambda **_kwargs: {"ok": True, "http_status": 200},
    )

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="store",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={"ios": {"ok": True, "outputs": {"ipa_path": None}}},
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("store",),
    )
    assert dist.get("ok") is False
    job = (dist.get("jobs") or {}).get("ios:store") or {}
    assert job.get("submitted") is False
    assert job.get("blocked") is True
    assert "ipa" in str(job.get("error") or "").lower()

    events = delivery_store.load_events(tmp_path, did)
    etypes = [str(e.get("event_type")) for e in events]
    assert "delivery.failed" in etypes
    assert "delivery.store.submitted" not in etypes
    sess = delivery_store.load_session(tmp_path, did)
    assert sess["per_platform"]["ios"]["channels"]["store"]["status"] == "blocked"


def test_ios_store_ipa_upload_success_emits_store_submitted(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)
    monkeypatch.setattr(
        provider_clients,
        "asc_verify_api_token",
        lambda **_kwargs: {"ok": True, "http_status": 200},
    )
    monkeypatch.setattr(
        provider_clients,
        "asc_upload_ipa_to_app_store_connect",
        lambda **kwargs: {"ok": True, "ipa_path": str(kwargs.get("ipa_path") or "")},
    )

    ipa = tmp_path / "App.ipa"
    ipa.write_bytes(b"fake-ipa")

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="store",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={"ios": {"ok": True, "outputs": {"ipa_path": str(ipa)}}},
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("store",),
    )
    assert dist.get("ok") is True
    job = (dist.get("jobs") or {}).get("ios:store") or {}
    assert job.get("submitted") is True

    events = delivery_store.load_events(tmp_path, did)
    store_events = [e for e in events if str(e.get("event_type")) == "delivery.store.submitted"]
    assert len(store_events) == 1
    jobs = store_events[0].get("payload", {}).get("jobs") or {}
    assert "ios:store" in jobs
    sess = delivery_store.load_session(tmp_path, did)
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["ios"]["outputs"]["distribution_results"]["store"]
    assert result["submission_id"] is None
    assert result["artifact_provenance"] == "packaged_ipa"


def test_android_store_without_aab_is_not_treated_as_submission(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.setenv("AKC_DELIVERY_PLAY_PACKAGE_NAME", "com.example.app")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)
    monkeypatch.setattr(
        provider_clients,
        "play_validate_edits_session",
        lambda **kwargs: {"ok": True, "package_name": str(kwargs.get("package_name") or "")},
    )

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["android"],
        release_mode="store",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={"android": {"ok": True, "outputs": {"aab_path": None}}},
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["android"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("store",),
    )
    assert dist.get("ok") is False
    job = (dist.get("jobs") or {}).get("android:store") or {}
    assert job.get("submitted") is False
    assert job.get("blocked") is True

    events = delivery_store.load_events(tmp_path, did)
    etypes = [str(e.get("event_type")) for e in events]
    assert "delivery.failed" in etypes
    assert "delivery.store.submitted" not in etypes
    sess = delivery_store.load_session(tmp_path, did)
    assert sess["per_platform"]["android"]["channels"]["store"]["status"] == "blocked"


def test_android_store_committed_submission_emits_store_submitted_event(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.setenv("AKC_DELIVERY_PLAY_PACKAGE_NAME", "com.example.app")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)

    aab = tmp_path / "app-release.aab"
    aab.write_bytes(b"fake-aab")
    monkeypatch.setattr(
        provider_clients,
        "play_upload_aab_and_commit_production",
        lambda **kwargs: {
            "ok": True,
            "package_name": str(kwargs.get("package_name") or ""),
            "aab_path": str(kwargs.get("aab_path") or ""),
            "edit_id": "edit-1",
        },
    )

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["android"],
        release_mode="store",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={"android": {"ok": True, "outputs": {"aab_path": str(aab)}}},
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["android"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("store",),
    )
    assert dist.get("ok") is True
    job = (dist.get("jobs") or {}).get("android:store") or {}
    assert job.get("submitted") is True

    events = delivery_store.load_events(tmp_path, did)
    store_events = [e for e in events if str(e.get("event_type")) == "delivery.store.submitted"]
    assert len(store_events) == 1
    jobs = store_events[0].get("payload", {}).get("jobs") or {}
    assert "android:store" in jobs
    sess = delivery_store.load_session(tmp_path, did)
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["android"]["outputs"]["distribution_results"][
        "store"
    ]
    assert result["edit_id"] == "edit-1"
    assert result["artifact_provenance"] == "packaged_aab"


def test_store_auto_submit_uses_eas_submit_without_legacy_env_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)
    monkeypatch.delenv("AKC_DELIVERY_EAS_AUTO_SUBMIT", raising=False)

    workspace_root = tmp_path / ".akc" / "execution" / "run-1" / "workspace" / "apps" / "universal"
    workspace_root.mkdir(parents=True, exist_ok=True)

    from akc.delivery import distribution_dispatch

    monkeypatch.setattr(
        distribution_dispatch,
        "_run_local_command",
        lambda *, argv, cwd: {
            "argv": argv,
            "cwd": str(cwd),
            "exit_code": 0,
            "stdout": json.dumps({"id": "submission-1"}),
            "stderr": "",
        },
    )

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="store",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={
            "ios": {
                "ok": True,
                "outputs": {
                    "eas_build_id": "build-ios-1",
                    "build_profile": "production",
                    "artifact_paths": {
                        "execution_workspace_root": str(tmp_path / ".akc" / "execution" / "run-1" / "workspace")
                    },
                    "build_result": {"build_id": "build-ios-1"},
                    "store_submit_mode": "auto",
                },
            }
        },
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("store",),
    )
    assert dist.get("ok") is True
    job = (dist.get("jobs") or {}).get("ios:store") or {}
    assert job.get("submitted") is True
    assert job.get("submission_id") == "submission-1"

    sess = delivery_store.load_session(tmp_path, did)
    assert sess["store_release"]["ios"]["status"] == "submitted"
    assert sess["store_release"]["ios"]["external_ref"] == "submission-1"
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["ios"]["outputs"]["distribution_results"]["store"]
    assert result["submission_id"] == "submission-1"
    assert result["artifact_provenance"] == "packaged_eas_build"


def test_store_manual_submit_is_recorded_as_blocked_not_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)

    workspace_root = tmp_path / ".akc" / "execution" / "run-manual" / "workspace" / "apps" / "universal"
    workspace_root.mkdir(parents=True, exist_ok=True)

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="store",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={
            "ios": {
                "ok": True,
                "outputs": {
                    "eas_build_id": "build-ios-1",
                    "build_profile": "production",
                    "artifact_paths": {
                        "execution_workspace_root": str(tmp_path / ".akc" / "execution" / "run-manual" / "workspace")
                    },
                    "build_result": {"build_id": "build-ios-1"},
                    "store_submit_mode": "manual",
                },
            }
        },
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["ios"],
        release_mode="store",
        delivery_version="1.0.0",
        compile_run_id="run-manual",
        lanes=("store",),
    )
    assert dist.get("ok") is False
    job = (dist.get("jobs") or {}).get("ios:store") or {}
    assert job.get("blocked") is True
    assert job.get("submitted") is False

    sess = delivery_store.load_session(tmp_path, did)
    assert sess["pipeline"]["distribution"]["status"] == "blocked"
    assert sess["per_platform"]["ios"]["channels"]["store"]["status"] == "blocked"
    assert sess["store_release"]["ios"]["status"] == "blocked"
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["ios"]["outputs"]["distribution_results"]["store"]
    assert result["status"] == "blocked"
    provider_state = delivery_store.load_provider_state_sidecar(tmp_path, did)
    assert provider_state["platforms"]["ios"]["status"] == "blocked"


def test_lane_binding_backfills_legacy_packaging_outputs() -> None:
    packaging = {
        "artifact_authority": "authoritative",
        "execution_mode": "execute",
        "build_profile": "production",
        "artifact_paths": {
            "execution_workspace_root": "/tmp/workspace",
            "ipa_path": "/tmp/App.ipa",
            "aab_path": "/tmp/app.aab",
        },
        "artifact_urls": {"build_url": "https://builds.example.org/ios"},
        "hosting_url": "https://app.example.org",
        "eas_build_id": "build-1",
    }
    web_beta = _lane_binding(platform="web", lane="beta", packaging=packaging)
    ios_store = _lane_binding(platform="ios", lane="store", packaging=packaging)
    android_beta = _lane_binding(platform="android", lane="beta", packaging=packaging)

    assert web_beta["hosting_url"] == "https://app.example.org"
    assert web_beta["ready"] is True
    assert ios_store["ipa_path"] == "/tmp/App.ipa"
    assert ios_store["ready"] is True
    assert android_beta["aab_path"] == "/tmp/app.aab"
    assert android_beta["ready"] is True


def test_web_distribution_persists_hosting_url_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={"web": {"ok": True, "outputs": {"hosting_url": "https://app.example.org"}}},
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["web"],
        release_mode="beta",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("beta",),
    )
    assert dist.get("ok") is True
    job = (dist.get("jobs") or {}).get("web:beta") or {}
    assert job.get("hosting_url") == "https://app.example.org"

    sess = delivery_store.load_session(tmp_path, did)
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["web"]["outputs"]["distribution_results"]["beta"]
    assert result["hosting_url"] == "https://app.example.org"
    provider_state = delivery_store.load_provider_state_sidecar(tmp_path, did)
    assert provider_state["platforms"]["web"]["external_refs"]["hosting_url"] == "https://app.example.org"


def test_ios_beta_submits_once_then_reuses_persisted_result(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.setenv("AKC_DELIVERY_ASC_BETA_GROUP_ID", "beta-group-1")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)

    from akc.delivery import distribution_dispatch

    submits: list[list[str]] = []
    invites: list[list[str]] = []
    monkeypatch.setattr(
        distribution_dispatch,
        "_run_local_command",
        lambda *, argv, cwd: (
            submits.append(list(argv))
            or {
                "argv": argv,
                "cwd": str(cwd),
                "exit_code": 0,
                "stdout": json.dumps({"id": "submission-beta-1"}),
                "stderr": "",
            }
        ),
    )
    monkeypatch.setattr(
        provider_clients,
        "asc_invite_emails_to_beta_group",
        lambda **kwargs: (
            invites.append(list(kwargs.get("emails") or [])) or {"ok": True, "invited": kwargs.get("emails")}
        ),
    )

    workspace_root = tmp_path / ".akc" / "execution" / "run-1" / "workspace" / "apps" / "universal"
    workspace_root.mkdir(parents=True, exist_ok=True)
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["ios"],
        release_mode="beta",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={
            "ios": {
                "ok": True,
                "outputs": {
                    "eas_build_id": "build-ios-beta-1",
                    "build_profile": "preview",
                    "artifact_paths": {
                        "execution_workspace_root": str(tmp_path / ".akc" / "execution" / "run-1" / "workspace")
                    },
                    "build_result": {"build_id": "build-ios-beta-1"},
                },
            }
        },
    )

    first = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["ios"],
        release_mode="beta",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("beta",),
    )
    second = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["ios"],
        release_mode="beta",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("beta",),
    )

    assert first["jobs"]["ios:beta"]["submitted"] is True
    assert second["jobs"]["ios:beta"]["reused_submission"] is True
    assert len(submits) == 1
    assert len(invites) == 2

    sess = delivery_store.load_session(tmp_path, did)
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["ios"]["outputs"]["distribution_results"]["beta"]
    assert result["submission_id"] == "submission-beta-1"
    assert result["artifact_provenance"] == "packaged_eas_build"


def test_android_beta_uploads_packaged_artifact_and_reuses_release_name(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.setenv("AKC_DELIVERY_FIREBASE_APP_ID", "1:123:android:abc")
    monkeypatch.setenv("AKC_DELIVERY_FIREBASE_APP_DIST_GROUPS", "qa")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)

    uploads: list[str] = []
    redistributes: list[str] = []
    monkeypatch.setattr(
        provider_clients,
        "firebase_upload_and_distribute_artifact",
        lambda **kwargs: (
            uploads.append(str(kwargs.get("binary_path")))
            or {
                "ok": True,
                "release_name": "projects/p1/apps/a1/releases/r1",
                "artifact_url": "https://firebase.example.org/r1",
            }
        ),
    )
    monkeypatch.setattr(
        provider_clients,
        "firebase_distribute_release",
        lambda **kwargs: redistributes.append(str(kwargs.get("release_name"))) or {"ok": True, "response": {}},
    )

    apk = tmp_path / "app.apk"
    apk.write_bytes(b"apk")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["android"],
        release_mode="beta",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={"android": {"ok": True, "outputs": {"apk_path": str(apk)}}},
    )

    first = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["android"],
        release_mode="beta",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("beta",),
    )
    second = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["android"],
        release_mode="beta",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("beta",),
    )

    assert first["jobs"]["android:beta"]["release_name"] == "projects/p1/apps/a1/releases/r1"
    assert second["jobs"]["android:beta"]["reused_release"] is True
    assert uploads == [str(apk)]
    assert redistributes == ["projects/p1/apps/a1/releases/r1"]

    sess = delivery_store.load_session(tmp_path, did)
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["android"]["outputs"]["distribution_results"][
        "beta"
    ]
    assert result["release_name"] == "projects/p1/apps/a1/releases/r1"
    assert result["artifact_provenance"] == "packaged_apk"


def test_android_beta_falls_back_to_external_release_name_when_no_artifact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "true")
    monkeypatch.setenv("AKC_DELIVERY_FIREBASE_RELEASE_NAME", "projects/p1/apps/a1/releases/external-1")
    monkeypatch.setenv("AKC_DELIVERY_FIREBASE_APP_DIST_GROUPS", "qa")
    monkeypatch.delenv("AKC_DELIVERY_PROVIDER_DRY_RUN", raising=False)
    monkeypatch.setattr(
        provider_clients,
        "firebase_distribute_release",
        lambda **_kwargs: {"ok": True, "response": {}},
    )

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["a@example.com"],
        platforms=["android"],
        release_mode="beta",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    _prime_package_outputs(
        project_dir=tmp_path,
        delivery_id=did,
        per_platform_outputs={"android": {"ok": True, "outputs": {}}},
    )

    dist = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["android"],
        release_mode="beta",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("beta",),
    )
    assert dist["jobs"]["android:beta"]["artifact_provenance"] == "external_release_name_fallback"

    sess = delivery_store.load_session(tmp_path, did)
    result = sess["pipeline"]["package"]["outputs"]["per_platform"]["android"]["outputs"]["distribution_results"][
        "beta"
    ]
    assert result["release_name"] == "projects/p1/apps/a1/releases/external-1"
    assert result["artifact_provenance"] == "external_release_name_fallback"
