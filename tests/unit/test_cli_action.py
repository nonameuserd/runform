from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main


def test_action_group_hidden_without_flag(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("AKC_ACTION_PLANE", raising=False)
    with pytest.raises(SystemExit):
        main(["action", "status", "--intent-id", "intent_x"])


def test_action_submit_and_status_roundtrip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("AKC_ACTION_PLANE", "1")
    monkeypatch.setenv("AKC_ACTION_MEDIUM_ALLOWLIST", "action.call.place,action.calendar.write")
    monkeypatch.chdir(tmp_path)
    consent_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / "consents"
    consent_root.mkdir(parents=True, exist_ok=True)
    (consent_root / "user_1.json").write_text(
        json.dumps({"allow_actions": ["action.call.place"]}, sort_keys=True),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="0"):
        main(
            [
                "action",
                "submit",
                "--text",
                "call +14155550123",
                "--tenant-id",
                "tenant_a",
                "--repo-id",
                "repo_a",
                "--channel",
                "cli",
                "--actor-id",
                "user_1",
            ]
        )
    out = capsys.readouterr().out
    submit_payload = json.loads(out)
    intent_id = str(submit_payload["intent_id"])
    assert intent_id.startswith("intent_")
    action_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / intent_id
    intent_obj = json.loads((action_root / "intent.json").read_text(encoding="utf-8"))
    plan_obj = json.loads((action_root / "plan.json").read_text(encoding="utf-8"))
    result_obj = json.loads((action_root / "result.json").read_text(encoding="utf-8"))
    execution_lines = (action_root / "execution.jsonl").read_text(encoding="utf-8").splitlines()
    assert intent_obj["schema_kind"] == "action_intent"
    assert plan_obj["schema_kind"] == "action_plan"
    assert "artifact_refs" in result_obj
    first_exec = json.loads(execution_lines[0])
    assert first_exec["schema_kind"] == "action_execution_record"
    assert isinstance(first_exec["decision_token_refs"], list)
    assert isinstance(first_exec["external_ids"], list)
    assert (tmp_path / ".akc" / "run" / "action_runs.jsonl").exists()
    assert (tmp_path / ".akc" / "control" / "action_runs.jsonl").exists()

    with pytest.raises(SystemExit, match="0"):
        main(["action", "status", "--intent-id", intent_id])
    status_out = capsys.readouterr().out
    status_payload = json.loads(status_out)
    assert status_payload["intent_id"] == intent_id
    assert status_payload["status"] in {"completed", "pending_approval", "failed"}


def test_action_submit_denied_emits_narrative(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("AKC_ACTION_PLANE", "1")
    monkeypatch.delenv("AKC_ACTION_MEDIUM_ALLOWLIST", raising=False)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit, match="0"):
        main(
            [
                "action",
                "submit",
                "--text",
                "call +14155550123",
                "--tenant-id",
                "tenant_a",
                "--repo-id",
                "repo_a",
                "--channel",
                "cli",
                "--actor-id",
                "user_1",
            ]
        )
    out = capsys.readouterr().out
    submit_payload = json.loads(out)
    intent_id = str(submit_payload["intent_id"])
    action_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / intent_id
    policy_obj = json.loads((action_root / "policy_decisions.json").read_text(encoding="utf-8"))
    result_obj = json.loads((action_root / "result.json").read_text(encoding="utf-8"))
    assert policy_obj["schema_kind"] == "action_policy_decisions"
    denied = [row for row in policy_obj["decisions"] if row["allowed"] is False]
    assert denied
    assert "narrative" in denied[0]
    denied_steps = [step for step in result_obj["steps"] if step["status"] == "denied"]
    assert denied_steps
    assert "narrative" in denied_steps[0]


def test_action_dispatch_channel_accepts_normalized_envelope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("AKC_ACTION_PLANE", "1")
    monkeypatch.setenv("AKC_ACTION_MEDIUM_ALLOWLIST", "action.call.place")
    monkeypatch.chdir(tmp_path)
    payload_file = tmp_path / "payload.json"
    payload_file.write_text(
        json.dumps(
            {
                "schema_kind": "action_inbound_message_envelope",
                "schema_version": 1,
                "channel": "cli",
                "tenant_id": "tenant_a",
                "repo_id": "repo_a",
                "text": "call +14155550123",
                "actor_id": "user_1",
                "message_id": "msg_1",
                "metadata": {"source": "test"},
            }
        ),
        encoding="utf-8",
    )
    consent_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / "consents"
    consent_root.mkdir(parents=True, exist_ok=True)
    (consent_root / "user_1.json").write_text(
        json.dumps({"allow_actions": ["action.call.place"]}, sort_keys=True),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="0"):
        main(["action", "dispatch-channel", "--channel", "cli", "--payload-file", str(payload_file)])
    submit_payload = json.loads(capsys.readouterr().out)
    assert submit_payload["notification"]["intent_id"] == submit_payload["intent_id"]
    assert submit_payload["notification"]["status"] in {"completed", "pending_approval", "failed"}
    assert submit_payload["notification"]["summary"]


def test_action_dispatch_channel_slack_adapter_ready(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("AKC_ACTION_PLANE", "1")
    monkeypatch.chdir(tmp_path)
    payload_file = tmp_path / "slack_payload.json"
    payload_file.write_text(
        json.dumps(
            {
                "tenant_id": "tenant_a",
                "repo_id": "repo_a",
                "text": "find my mom cellphone number",
                "actor_id": "user_2",
                "metadata": {"thread_ts": "1.2"},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="0"):
        main(["action", "dispatch-channel", "--channel", "slack", "--payload-file", str(payload_file)])
    submit_payload = json.loads(capsys.readouterr().out)
    assert submit_payload["notification"]["channel"] == "slack"


def test_action_submit_dry_run_persists_plan_without_execution(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("AKC_ACTION_PLANE", "1")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit, match="0"):
        main(
            [
                "action",
                "submit",
                "--text",
                "schedule flights on 2026-05-10",
                "--tenant-id",
                "tenant_a",
                "--repo-id",
                "repo_a",
                "--actor-id",
                "user_1",
                "--dry-run",
            ]
        )
    payload = json.loads(capsys.readouterr().out)
    intent_id = str(payload["intent_id"])
    action_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / intent_id
    result = json.loads((action_root / "result.json").read_text(encoding="utf-8"))
    assert result["status"] == "dry_run"
    assert (action_root / "execution.jsonl").exists() is False


def test_action_submit_simulate_marks_steps_simulated(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("AKC_ACTION_PLANE", "1")
    monkeypatch.setenv("AKC_ACTION_MEDIUM_ALLOWLIST", "action.call.place")
    monkeypatch.chdir(tmp_path)
    consent_root = tmp_path / ".akc" / "actions" / "tenant_a" / "repo_a" / "consents"
    consent_root.mkdir(parents=True, exist_ok=True)
    (consent_root / "user_1.json").write_text(
        json.dumps({"allow_actions": ["action.call.place"]}, sort_keys=True),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit, match="0"):
        main(
            [
                "action",
                "submit",
                "--text",
                "call my dad",
                "--tenant-id",
                "tenant_a",
                "--repo-id",
                "repo_a",
                "--actor-id",
                "user_1",
                "--simulate",
            ]
        )
    payload = json.loads(capsys.readouterr().out)
    assert payload["result"]["mode"] == "simulate"
    assert all(step["status"] == "simulated" for step in payload["result"]["steps"])
