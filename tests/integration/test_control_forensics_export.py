"""Integration: `akc control forensics export` writes schema-valid FORENSICS.json and artifacts."""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path

import pytest

from akc.cli import main
from akc.control.operations_index import OperationsIndex
from akc.control.operator_workflows import validate_forensics_bundle
from akc.run.manifest import ArtifactPointer, RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _seed_run(outputs_root: Path) -> None:
    scope = outputs_root / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    know = scope / ".akc" / "knowledge"
    know.mkdir(parents=True, exist_ok=True)

    snap_rel = ".akc/knowledge/snapshot.json"
    (scope / snap_rel).write_text('{"snap": true}\n', encoding="utf-8")

    ca_rel = ".akc/run/r1.coordination_audit.jsonl"
    (scope / ca_rel).write_text(
        "\n".join(json.dumps({"step": n, "event": "coord"}) for n in range(50)) + "\n",
        encoding="utf-8",
    )

    otel_rel = ".akc/run/r1.otel.jsonl"
    (scope / otel_rel).write_text('{"resourceSpans":[]}\n', encoding="utf-8")

    replay = {
        "run_id": "r1",
        "tenant_id": "t1",
        "repo_id": "repo1",
        "replay_mode": "partial_replay",
        "decisions": [
            {
                "pass_name": "verify",
                "replay_mode": "partial_replay",
                "should_call_model": False,
                "should_call_tools": True,
                "trigger_reason": "knowledge_semantic_changed",
                "inputs_snapshot": {},
            }
        ],
    }
    (rd / "r1.replay_decisions.json").write_text(json.dumps(replay), encoding="utf-8")

    manifest = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("e"),
        intent_semantic_fingerprint="b" * 16,
        output_hashes={otel_rel: _hex64("4")},
        knowledge_snapshot=ArtifactPointer(path=snap_rel, sha256=_hex64("d")),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "replay_decisions_ref": {"path": ".akc/run/r1.replay_decisions.json", "sha256": _hex64("2")},
            "coordination_audit_ref": {"path": ca_rel, "sha256": _hex64("3")},
        },
    )
    man_path = rd / "r1.manifest.json"
    man_path.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(man_path, outputs_root=outputs_root)


def test_cli_control_forensics_export_json_stdout_and_bundle(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _seed_run(tmp_path)
    monkeypatch.chdir(tmp_path)
    out_dir = tmp_path / "forensics_out"

    with pytest.raises(SystemExit) as ei:
        main(
            [
                "control",
                "forensics",
                "export",
                "--outputs-root",
                str(tmp_path),
                "--tenant-id",
                "t1",
                "--repo-id",
                "repo1",
                "--run-id",
                "r1",
                "--no-zip",
                "--out-dir",
                str(out_dir),
                "--coordination-audit-tail-lines",
                "12",
                "--signer-identity",
                "ops-signer@tenant.t1",
                "--signature",
                "sig-v1:abc123",
                "--format",
                "json",
            ]
        )
    assert int(ei.value.code) == 0

    # Stdout is the CLI result envelope (paths + summary), not FORENSICS.json alone.
    # We validate the on-disk manifest.
    forensics_path = out_dir / "FORENSICS.json"
    assert forensics_path.is_file()
    doc = json.loads(forensics_path.read_text(encoding="utf-8"))
    assert doc["schema_kind"] == "akc_forensics_bundle"
    assert validate_forensics_bundle(doc) == []

    assert doc["operations_index"]["row_found"] is True
    assert doc["operations_index"]["run"] is not None
    assert doc["operations_index"]["run"]["stable_intent_sha256"] == _hex64("e")

    assert doc["replay"]["included"] is True
    assert doc["replay"]["forensics_summary"]["trigger_reason_histogram"].get("knowledge_semantic_changed") == 1

    assert doc["coordination_audit"]["included"] is True
    assert doc["coordination_audit"]["tail_line_count"] == 12

    otel_exports = doc["otel"]["exports"]
    assert len(otel_exports) >= 1
    assert any(e.get("source_relpath", "").endswith(".otel.jsonl") and e.get("bundle_relpath") for e in otel_exports)

    assert doc["knowledge_snapshot"]["included"] is True
    assert doc["export_metadata"]["signature"] == {
        "identity": "ops-signer@tenant.t1",
        "signature": "sig-v1:abc123",
    }

    assert (out_dir / "data" / "replay_decisions.json").is_file()
    assert (out_dir / "data" / "coordination_audit.tail.jsonl").is_file()


def test_forensics_bundle_picks_up_otel_from_output_hashes_only(tmp_path: Path) -> None:
    """When the default ``.akc/run/<id>.otel.jsonl`` is absent, still index OTel from ``output_hashes``."""

    from akc.control.operator_workflows import build_forensics_bundle

    outputs_root = tmp_path
    scope = outputs_root / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)

    alt = ".akc/run/r1.extra.otel.jsonl"
    (scope / alt).write_text("{}\n", encoding="utf-8")

    manifest = replace(
        RunManifest(
            run_id="r1",
            tenant_id="t1",
            repo_id="repo1",
            ir_sha256=_hex64("1"),
            replay_mode="partial_replay",
            stable_intent_sha256=_hex64("f"),
            intent_semantic_fingerprint="c" * 16,
        ),
        output_hashes={alt: _hex64("5")},
    )
    mp = rd / "r1.manifest.json"
    mp.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")

    out_dir = tmp_path / "fb2"
    summary = build_forensics_bundle(
        outputs_root=outputs_root,
        scope_root=scope,
        manifest=manifest,
        manifest_source_path=mp,
        out_dir=out_dir,
        make_zip=False,
    )["summary"]
    paths = [e["source_relpath"] for e in summary["otel"]["exports"]]
    assert alt in paths
