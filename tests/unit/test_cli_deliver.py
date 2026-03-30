from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from akc.cli import deliver as cli_deliver
from akc.cli import main
from akc.delivery import orchestrate as delivery_orchestrate
from akc.delivery.compile_handoff import run_manifest_path
from akc.run.manifest import PassRecord, RunManifest


def test_cli_deliver_submit_status_events_resend_promote(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "deliver",
                "--project-dir",
                str(tmp_path),
                "--request",
                "build an app and send it to these 3 users",
                "--recipient",
                "alice@example.com",
                "--recipient",
                "bob@example.com",
                "--recipient",
                "carol@example.com",
                "--platforms",
                "web,ios,android",
                "--release-mode",
                "both",
            ]
        )
    assert exc.value.code == 0
    out = json.loads(capsys.readouterr().out)
    delivery_id = out["delivery_id"]
    assert out["parsed"]["requested_platforms"] == ["web", "ios", "android"]
    assert out["parsed"]["delivery_mode"] == "both"
    assert out["required_human_inputs_count"] >= 1
    assert out["preflight_ok"] is False
    assert out["session_phase"] == "blocked"
    base = tmp_path / ".akc" / "delivery" / delivery_id
    assert (base / "request.json").is_file()
    assert (base / "session.json").is_file()
    assert (base / "recipients.json").is_file()
    assert (base / "events.json").is_file()
    assert (base / "provider_state.json").is_file()
    assert (base / "activation_evidence.json").is_file()

    capsys.readouterr()
    with pytest.raises(SystemExit) as exc2:
        main(["deliver", "status", "--project-dir", str(tmp_path), "--delivery-id", delivery_id])
    assert exc2.value.code == 0
    status_doc = json.loads(capsys.readouterr().out)
    assert status_doc["request"]["recipients"] == [
        "alice@example.com",
        "bob@example.com",
        "carol@example.com",
    ]
    assert "metrics" in status_doc
    assert "request_to_invite_sent_ms" in status_doc["metrics"]
    assert "activation_rate" in status_doc["metrics"]
    assert isinstance(status_doc["request"].get("required_accounts"), list)

    capsys.readouterr()
    with pytest.raises(SystemExit) as exc3:
        main(["deliver", "events", "--project-dir", str(tmp_path), "--delivery-id", delivery_id])
    assert exc3.value.code == 0
    ev_doc = json.loads(capsys.readouterr().out)
    assert any(e.get("event_type") == "delivery.request.accepted" for e in ev_doc["events"])
    assert any(e.get("event_type") == "delivery.request.parsed" for e in ev_doc["events"])

    capsys.readouterr()
    with pytest.raises(SystemExit) as exc4:
        main(
            [
                "deliver",
                "resend",
                "--project-dir",
                str(tmp_path),
                "--delivery-id",
                delivery_id,
                "--recipient",
                "alice@example.com",
            ]
        )
    assert exc4.value.code == 0
    resend_out = json.loads(capsys.readouterr().out)
    assert resend_out["event"]["event_type"] == "delivery.invite.resend_requested"

    capsys.readouterr()
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")
    with pytest.raises(SystemExit) as exc_gate:
        main(
            [
                "deliver",
                "gate-pass",
                "--project-dir",
                str(tmp_path),
                "--delivery-id",
                delivery_id,
            ]
        )
    assert exc_gate.value.code == 0

    capsys.readouterr()
    with pytest.raises(SystemExit) as exc5:
        main(
            [
                "deliver",
                "promote",
                "--project-dir",
                str(tmp_path),
                "--delivery-id",
                delivery_id,
                "--lane",
                "store",
            ]
        )
    assert exc5.value.code == 2
    prom_out = json.loads(capsys.readouterr().out)
    assert prom_out["event"]["event_type"] == "delivery.store.promotion_requested"
    assert prom_out["event"]["payload"]["lane"] == "store"
    assert prom_out["distribution"]["ok"] is False


def test_cli_deliver_accepts_recipients_file_only(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    rfile = tmp_path / "recipients.txt"
    rfile.write_text("dana@example.com\n", encoding="utf-8")
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "deliver",
                "--project-dir",
                str(tmp_path),
                "--request",
                "beta for my team",
                "--recipients-file",
                str(rfile),
                "--platforms",
                "web",
            ]
        )
    assert exc.value.code == 0
    out = json.loads(capsys.readouterr().out)
    with pytest.raises(SystemExit) as exc2:
        main(
            [
                "deliver",
                "status",
                "--project-dir",
                str(tmp_path),
                "--delivery-id",
                str(out["delivery_id"]),
            ]
        )
    assert exc2.value.code == 0
    loaded = json.loads(capsys.readouterr().out)
    assert loaded["request"]["recipients"] == ["dana@example.com"]
    assert loaded["request"]["parsed"]["recipient_set"] == ["dana@example.com"]


def test_cli_deliver_resend_rejects_unknown_recipient(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "deliver",
                "--project-dir",
                str(tmp_path),
                "--request",
                "x",
                "--recipient",
                "alice@example.com",
            ]
        )
    assert exc.value.code == 0
    delivery_id = json.loads(capsys.readouterr().out)["delivery_id"]

    capsys.readouterr()
    with pytest.raises(SystemExit) as exc2:
        main(
            [
                "deliver",
                "resend",
                "--project-dir",
                str(tmp_path),
                "--delivery-id",
                delivery_id,
                "--recipient",
                "not-in-list@example.com",
            ]
        )
    assert exc2.value.code == 2
    assert "not part of this delivery" in capsys.readouterr().err


def test_cli_deliver_compile_outputs_structured_packaging_summary(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setattr(delivery_orchestrate, "run_delivery_compile", lambda **_kwargs: (0, "run-1"))
    monkeypatch.setattr(
        cli_deliver,
        "load_compile_handoff",
        lambda **_kwargs: {
            "compile_run_id": "run-1",
            "manifest_present": True,
            "manifest_rel_path": ".akc/run/run-1.manifest.json",
            "delivery_plan_rel_path": ".akc/deployment/run-1.delivery_plan.json",
            "delivery_plan_loaded": True,
            "delivery_plan_ref": {"path": ".akc/deployment/run-1.delivery_plan.json", "fingerprint": "a" * 64},
            "promotion_readiness": {"status": "ready"},
            "runtime_bundle_rel_path": ".akc/runtime/run-1.runtime_bundle.json",
        },
    )
    monkeypatch.setattr(
        delivery_orchestrate,
        "run_delivery_build_and_package",
        lambda **_kwargs: {
            "ok": True,
            "preflight_issues": [],
            "requested_preflight_issues": [],
            "provider_versions": {"delivery_version": "1.0.0"},
            "mode_resolution": {
                "requested_mode": "plan",
                "resolved_mode": "plan",
                "mode_source": "explicit",
                "reason": "explicit_plan",
                "outcome": "inspectable_plan",
                "auto_fallback": False,
                "fallback_issues": [],
            },
            "distribution": {"ok": True, "skipped": True, "reason": "plan_only"},
            "summary": {
                "mode": "plan",
                "store_submit_mode": "auto",
                "ready_platforms": [],
                "planned_only_platforms": ["web"],
                "distribution_ready": False,
                "mode_resolution": {
                    "requested_mode": "plan",
                    "resolved_mode": "plan",
                    "mode_source": "explicit",
                    "reason": "explicit_plan",
                    "outcome": "inspectable_plan",
                    "auto_fallback": False,
                    "fallback_issues": [],
                },
            },
        },
    )

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "deliver",
                "--project-dir",
                str(tmp_path),
                "--request",
                "build a web app",
                "--recipient",
                "alice@example.com",
                "--platforms",
                "web",
                "--compile",
                "--packaging-mode",
                "plan",
                "--store-submit",
                "auto",
            ]
        )
    assert exc.value.code == 0
    out = json.loads(capsys.readouterr().out)
    assert out["packaging_ok"] is True
    assert out["packaging"]["mode"] == "plan"
    assert out["packaging"]["store_submit_mode"] == "auto"
    assert out["packaging"]["planned_only_platforms"] == ["web"]
    assert out["packaging"]["distribution_ready"] is False
    assert out["journey"]["outcome"] == "inspectable_plan"
    assert out["journey"]["resolved_packaging_mode"] == "plan"
    assert out["compile_outputs"]["delivery_plan_ref"]["path"] == ".akc/deployment/run-1.delivery_plan.json"


def test_cli_deliver_compile_default_mode_enables_auto_plan_fallback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}

    monkeypatch.setattr(delivery_orchestrate, "run_delivery_compile", lambda **_kwargs: (0, "run-1"))
    monkeypatch.setattr(
        cli_deliver,
        "load_compile_handoff",
        lambda **_kwargs: {
            "compile_run_id": "run-1",
            "manifest_present": True,
            "manifest_rel_path": ".akc/run/run-1.manifest.json",
            "delivery_plan_loaded": True,
            "delivery_plan_ref": {"path": ".akc/deployment/run-1.delivery_plan.json", "fingerprint": "a" * 64},
            "runtime_bundle_rel_path": ".akc/runtime/run-1.runtime_bundle.json",
            "promotion_readiness": {"status": "blocked"},
        },
    )

    def _fake_build_and_package(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return {
            "ok": True,
            "preflight_issues": [],
            "requested_preflight_issues": [{"platform": "ios", "lane": "ios_build", "reason": "missing token"}],
            "provider_versions": {"delivery_version": "1.0.0"},
            "mode_resolution": {
                "requested_mode": "execute",
                "resolved_mode": "plan",
                "mode_source": "default",
                "reason": "packaging_preflight_blocked",
                "outcome": "inspectable_plan",
                "auto_fallback": True,
                "fallback_issues": [{"platform": "ios", "lane": "ios_build", "reason": "missing token"}],
            },
            "distribution": {"ok": True, "skipped": True, "reason": "plan_only"},
            "summary": {
                "mode": "plan",
                "store_submit_mode": "auto",
                "ready_platforms": [],
                "planned_only_platforms": ["ios"],
                "distribution_ready": False,
                "mode_resolution": {
                    "requested_mode": "execute",
                    "resolved_mode": "plan",
                    "mode_source": "default",
                    "reason": "packaging_preflight_blocked",
                    "outcome": "inspectable_plan",
                    "auto_fallback": True,
                    "fallback_issues": [{"platform": "ios", "lane": "ios_build", "reason": "missing token"}],
                },
            },
        }

    monkeypatch.setattr(delivery_orchestrate, "run_delivery_build_and_package", _fake_build_and_package)

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "deliver",
                "--project-dir",
                str(tmp_path),
                "--request",
                "build an ios app",
                "--recipient",
                "alice@example.com",
                "--platforms",
                "ios",
                "--compile",
                "--store-submit",
                "auto",
            ]
        )
    assert exc.value.code == 0
    assert captured["packaging_mode"] == "execute"
    assert captured["allow_plan_fallback"] is True
    out = json.loads(capsys.readouterr().out)
    assert out["journey"]["outcome"] == "inspectable_plan"
    assert out["journey"]["reason"] == "packaging_preflight_blocked"
    assert out["packaging"]["mode_resolution"]["auto_fallback"] is True


def test_cli_deliver_preflight_reports_missing_credentials(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "deliver",
                "preflight",
                "--project-dir",
                str(tmp_path),
                "--platforms",
                "web,ios,android",
                "--release-mode",
                "both",
            ]
        )
    assert exc.value.code == 2
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] is False
    ids = {str(row.get("id")) for row in out["required_human_inputs"]}
    assert "app_store_connect_api_credentials" in ids
    assert "testflight_beta_group_id" in ids
    assert "firebase_distribution_credentials" in ids
    assert "google_play_publisher_credentials" in ids
    assert out["surfaces"]["expo_eas_build_hosting"]["ok"] is False
    assert out["surfaces"]["firebase_play_upload"]["ok"] is False
    assert any(
        "AKC_DELIVERY_ASC_BETA_GROUP_ID" in str(row.get("reason") or "") for row in out["distribution"]["issues"]
    )
    assert any(
        "execution workspace manifest missing from compile handoff" in str(row.get("reason") or "")
        for row in out["packaging"]["execute"]["issues"]
    )


def test_cli_deliver_preflight_uses_compile_run_id_for_packaging_specific_gaps(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    rid = "run-preflight-1"
    (tmp_path / ".akc" / "run").mkdir(parents=True, exist_ok=True)
    manifest = RunManifest(
        run_id=rid,
        tenant_id="local",
        repo_id="local",
        ir_sha256="a" * 64,
        replay_mode="live",
        passes=(
            PassRecord(
                name="execution_workspace",
                status="succeeded",
                metadata={
                    "execution_workspace_manifest_path": f".akc/execution/{rid}.execution_workspace_manifest.json",
                },
            ),
        ),
    )
    run_manifest_path(project_dir=tmp_path, compile_run_id=rid).write_text(
        json.dumps(manifest.to_json_obj()),
        encoding="utf-8",
    )
    execution_dir = tmp_path / ".akc" / "execution"
    execution_dir.mkdir(parents=True, exist_ok=True)
    (execution_dir / f"{rid}.execution_workspace_manifest.json").write_text(
        json.dumps(
            {
                "run_id": rid,
                "tenant_id": "local",
                "repo_id": "local",
                "workspace_root": f".akc/execution/{rid}/workspace",
                "package_manager": "npm",
                "expo": {
                    "project_id": "expo-project-1",
                    "ios_bundle_identifier": "com.example.app",
                },
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "deliver",
                "preflight",
                "--project-dir",
                str(tmp_path),
                "--platforms",
                "ios",
                "--release-mode",
                "beta",
                "--compile-run-id",
                rid,
            ]
        )
    assert exc.value.code == 2
    out = json.loads(capsys.readouterr().out)
    reasons = [str(row.get("reason") or "") for row in out["packaging"]["execute"]["issues"]]
    assert all("execution workspace manifest missing from compile handoff" not in reason for reason in reasons)
    assert "expo_access_token" in {str(row.get("id")) for row in out["required_human_inputs"]}
