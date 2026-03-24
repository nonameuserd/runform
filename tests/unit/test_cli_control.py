from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.run.manifest import PassRecord, RunManifest
from akc.run.replay_decisions import build_replay_decisions_payload


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def test_cli_control_runs_list_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
    )
    scope = tmp_path / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    mp = rd / "run-1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "control",
                "runs",
                "list",
                "--tenant-id",
                "t1",
                "--outputs-root",
                str(tmp_path),
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["tenant_id"] == "t1"
    assert len(payload["runs"]) == 1
    assert payload["runs"][0]["run_id"] == "run-1"


def test_cli_control_runs_label_set(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
    )
    scope = tmp_path / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    mp = rd / "run-1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "control",
                "runs",
                "label",
                "set",
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--run-id",
                "run-1",
                "--label-key",
                "env",
                "--label-value",
                "staging",
                "--outputs-root",
                str(tmp_path),
            ]
        )
    assert exc.value.code == 0
    assert "env" in capsys.readouterr().out

    idx = OperationsIndex(sqlite_path=operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    row = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-1")
    assert row is not None
    assert row["labels"] == {"env": "staging"}

    audit_path = tmp_path / "t1" / ".akc" / "control" / "control_audit.jsonl"
    audit_lines = [ln for ln in audit_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(audit_lines) == 1
    rec0 = json.loads(audit_lines[0])
    assert rec0["action"] == "runs.label.set"
    assert rec0["tenant_id"] == "t1"
    assert rec0["details"]["label_key"] == "env"
    assert rec0["details"]["before"]["label_value"] is None
    assert rec0["details"]["after"]["label_value"] == "staging"
    assert "request_id" in rec0

    with pytest.raises(SystemExit) as exc2:
        main(
            [
                "control",
                "runs",
                "label",
                "set",
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--run-id",
                "run-1",
                "--label-key",
                "env",
                "--label-value",
                "prod",
                "--outputs-root",
                str(tmp_path),
            ]
        )
    assert exc2.value.code == 0
    audit_lines2 = [ln for ln in audit_path.read_text(encoding="utf-8").splitlines() if ln.strip()]
    assert len(audit_lines2) == 2
    rec1 = json.loads(audit_lines2[1])
    assert rec1["details"]["before"]["label_value"] == "staging"
    assert rec1["details"]["after"]["label_value"] == "prod"


def test_cli_control_manifest_diff_run_ids_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def _hex64(c: str = "0") -> str:
        return (c * 64)[:64]

    a = RunManifest(
        run_id="run-a",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("a"),
        passes=(PassRecord(name="verify", status="succeeded"),),
    )
    b = RunManifest(
        run_id="run-b",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
        passes=(PassRecord(name="verify", status="failed"),),
    )
    scope = tmp_path / "t1" / "repo1" / ".akc" / "run"
    scope.mkdir(parents=True)
    (scope / "run-a.manifest.json").write_text(json.dumps(a.to_json_obj()), encoding="utf-8")
    (scope / "run-b.manifest.json").write_text(json.dumps(b.to_json_obj()), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "control",
                "manifest",
                "diff",
                "--outputs-root",
                str(tmp_path),
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--run-id-a",
                "run-a",
                "--run-id-b",
                "run-b",
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    diff = json.loads(capsys.readouterr().out)
    assert diff["stable_intent_sha256"]["match"] is False
    assert any(x.get("pass") == "verify" for x in diff["pass_status_changes"])


def test_cli_control_replay_forensics_json(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def _hex64(c: str = "a") -> str:
        return (c * 64)[:64]

    payload = build_replay_decisions_payload(
        run_id="run-x",
        tenant_id="t1",
        repo_id="repo1",
        replay_mode="live",
        decision_manifest=None,
        baseline_manifest=None,
        replay_source_run_id=None,
        current_intent_semantic_fingerprint="f" * 16,
        current_knowledge_semantic_fingerprint="e" * 16,
        current_knowledge_provenance_fingerprint="d" * 16,
    )
    p = tmp_path / "rd.json"
    p.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "control",
                "replay",
                "forensics",
                "--replay-decisions",
                str(p),
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    report = json.loads(capsys.readouterr().out)
    assert report.get("schema_kind") == "akc_replay_forensics"


def test_cli_control_runs_show_missing(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "control",
                "runs",
                "show",
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--run-id",
                "nope",
                "--outputs-root",
                str(tmp_path),
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["run"] is None
