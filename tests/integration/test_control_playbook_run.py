"""Integration: `akc control playbook run` writes report, audit, and optional stdout JSON."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _seed_two_runs(outputs_root: Path) -> None:
    scope = outputs_root / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    m1 = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("a"),
        intent_semantic_fingerprint="a" * 16,
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("a"),
        },
    )
    m2 = RunManifest(
        run_id="r2",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("2"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("b"),
        intent_semantic_fingerprint="d" * 16,
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("b"),
        },
    )
    (rd / "r1.manifest.json").write_text(json.dumps(m1.to_json_obj()), encoding="utf-8")
    (rd / "r2.manifest.json").write_text(json.dumps(m2.to_json_obj()), encoding="utf-8")
    replay = {
        "run_id": "r2",
        "tenant_id": "t1",
        "repo_id": "repo1",
        "replay_mode": "partial_replay",
        "decisions": [],
    }
    (rd / "r2.replay_decisions.json").write_text(json.dumps(replay), encoding="utf-8")


def test_cli_control_playbook_run_writes_report_and_audit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_two_runs(tmp_path)
    monkeypatch.chdir(tmp_path)
    ts = "20990102T000000Z"
    with pytest.raises(SystemExit) as ei:
        main(
            [
                "control",
                "playbook",
                "run",
                "--outputs-root",
                str(tmp_path),
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--run-id-a",
                "r1",
                "--run-id-b",
                "r2",
                "--timestamp-utc",
                ts,
                "--format",
                "json",
            ]
        )
    assert int(ei.value.code) == 0

    report_path = tmp_path / "t1" / ".akc" / "control" / "playbooks" / f"{ts}.json"
    assert report_path.is_file()
    saved = json.loads(report_path.read_text(encoding="utf-8"))
    assert saved["schema_kind"] == "akc_operator_playbook_report"
    assert saved["manifest_diff"]["stable_intent_sha256"]["match"] is False
    assert saved["replay_plan_artifact"] is not None
    assert saved["replay_plan_artifact"]["sha256"]
    rp = tmp_path / "t1" / ".akc" / "control" / "playbooks" / f"{ts}.replay_plan.json"
    assert rp.is_file()
    rp_doc = json.loads(rp.read_text(encoding="utf-8"))
    assert rp_doc["schema_kind"] == "akc_replay_plan"
    step_names = [s["name"] for s in saved["steps"] if isinstance(s, dict)]
    assert step_names.index("replay_plan") > step_names.index("manifest_diff")

    audit_path = tmp_path / "t1" / ".akc" / "control" / "control_audit.jsonl"
    lines = audit_path.read_text(encoding="utf-8").strip().splitlines()
    assert lines
    last = json.loads(lines[-1])
    assert last["action"] == "playbook_run"
    assert last["details"]["report_sha256"]
