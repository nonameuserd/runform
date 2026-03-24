from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

from akc.control.operator_workflows import (
    build_forensics_bundle,
    validate_forensics_bundle,
)
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _base_manifest() -> RunManifest:
    return RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("1"),
        intent_semantic_fingerprint="a" * 16,
    )


def test_tail_coordination_audit_respects_line_limit(tmp_path: Path) -> None:
    outputs_root = tmp_path
    scope = outputs_root / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    ca_rel = ".akc/run/r1.coordination_audit.jsonl"
    lines = [json.dumps({"i": n}) for n in range(120)]
    (scope / ca_rel).write_text("\n".join(lines) + "\n", encoding="utf-8")

    manifest = replace(
        _base_manifest(),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "coordination_audit_ref": {"path": ca_rel, "sha256": _hex64("c")},
        },
    )
    man_path = rd / "r1.manifest.json"
    man_path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    out_dir = tmp_path / "bundle"
    res = build_forensics_bundle(
        outputs_root=outputs_root,
        scope_root=scope,
        manifest=manifest,
        manifest_source_path=man_path,
        out_dir=out_dir,
        make_zip=False,
        coordination_audit_tail_lines=40,
        coordination_audit_max_scan_bytes=256 * 1024,
    )
    summary = res["summary"]
    assert summary["coordination_audit"]["included"] is True
    assert summary["coordination_audit"]["tail_line_count"] == 40
    tail_path = out_dir / "data" / "coordination_audit.tail.jsonl"
    assert tail_path.is_file()
    tail_lines = tail_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(tail_lines) == 40
    assert json.loads(tail_lines[-1])["i"] == 119

    issues = validate_forensics_bundle(summary)
    assert issues == []


def test_coordination_audit_omitted_on_path_escape(tmp_path: Path) -> None:
    outputs_root = tmp_path
    scope = outputs_root / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)

    manifest = replace(
        _base_manifest(),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "coordination_audit_ref": {
                "path": ".akc/run/../../../outside.jsonl",
                "sha256": _hex64("c"),
            },
        },
    )
    man_path = rd / "r1.manifest.json"
    man_path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    out_dir = tmp_path / "bundle2"
    summary = build_forensics_bundle(
        outputs_root=outputs_root,
        scope_root=scope,
        manifest=manifest,
        manifest_source_path=man_path,
        out_dir=out_dir,
        make_zip=False,
    )["summary"]
    assert summary["coordination_audit"]["included"] is False
    assert summary["coordination_audit"]["omitted_reason"] == "path_escape"


def test_replay_forensics_summary_embedded(tmp_path: Path) -> None:
    outputs_root = tmp_path
    scope = outputs_root / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)

    replay = {
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "repo1",
        "replay_mode": "partial_replay",
        "decisions": [
            {
                "pass_name": "generate",
                "replay_mode": "partial_replay",
                "should_call_model": True,
                "should_call_tools": False,
                "trigger_reason": "intent_semantic_changed",
                "inputs_snapshot": {},
            }
        ],
    }
    (rd / "r1.replay_decisions.json").write_text(json.dumps(replay), encoding="utf-8")

    manifest = _base_manifest()
    man_path = rd / "r1.manifest.json"
    man_path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    out_dir = tmp_path / "bundle3"
    summary = build_forensics_bundle(
        outputs_root=outputs_root,
        scope_root=scope,
        manifest=manifest,
        manifest_source_path=man_path,
        out_dir=out_dir,
        make_zip=False,
    )["summary"]
    fs = summary["replay"].get("forensics_summary")
    assert isinstance(fs, dict)
    assert fs.get("schema_kind") == "akc_replay_forensics"
    assert fs.get("trigger_reason_histogram", {}).get("intent_semantic_changed") == 1
