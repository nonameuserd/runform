from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from akc.delivery import provider_clients
from akc.delivery import store as delivery_store
from akc.delivery.distribution_dispatch import run_delivery_distribution


def _prime_package_outputs(
    *,
    project_dir: Path,
    delivery_id: str,
    per_platform_outputs: dict[str, Any],
) -> None:
    delivery_store.update_session_pipeline_stage(
        project_dir=project_dir,
        delivery_id=delivery_id,
        stage_name="package",
        status="completed",
        outputs={"per_platform": per_platform_outputs},
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
    assert sess["per_platform"]["ios"]["channels"]["store"]["status"] == "failed"


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
    assert sess["per_platform"]["android"]["channels"]["store"]["status"] == "failed"


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
