"""Integration: `akc control replay plan` emits schema-valid replay_plan JSON."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _write_manifest(scope: Path) -> Path:
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("a"),
        intent_semantic_fingerprint="a" * 16,
        partial_replay_passes=("generate",),
        success_criteria_evaluation_modes=("tests",),
    )
    mp = rd / "r1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    return mp


def test_cli_control_replay_plan_stdout_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    scope = tmp_path / "t1" / "repo1"
    _write_manifest(scope)
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as ei:
        main(
            [
                "control",
                "replay",
                "plan",
                "--manifest",
                str(scope / ".akc" / "run" / "r1.manifest.json"),
            ]
        )
    assert int(ei.value.code) == 0
    out = capsys.readouterr().out
    doc = json.loads(out)
    assert doc["schema_kind"] == "akc_replay_plan"
    assert "generate" in doc["intent_replay_context"]["effective_partial_replay_passes"]


def test_cli_control_replay_plan_writes_out(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    scope = tmp_path / "t1" / "repo1"
    _write_manifest(scope)
    out_file = tmp_path / "plan.json"
    monkeypatch.chdir(tmp_path)
    with pytest.raises(SystemExit) as ei:
        main(
            [
                "control",
                "replay",
                "plan",
                "--manifest",
                str(scope / ".akc" / "run" / "r1.manifest.json"),
                "--evaluation-modes",
                "manifest_check",
                "--out",
                str(out_file),
            ]
        )
    assert int(ei.value.code) == 0
    assert out_file.is_file()
    doc = json.loads(out_file.read_text(encoding="utf-8"))
    assert doc["intent_replay_context"]["evaluation_modes_source"] == "cli"
    assert doc["intent_replay_context"]["cli_evaluation_modes"] == ["manifest_check"]
