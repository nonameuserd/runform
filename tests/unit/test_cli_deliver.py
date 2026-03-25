from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main


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
    assert exc5.value.code == 0
    prom_out = json.loads(capsys.readouterr().out)
    assert prom_out["event"]["event_type"] == "delivery.store.promotion_requested"
    assert prom_out["event"]["payload"]["lane"] == "store"


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
