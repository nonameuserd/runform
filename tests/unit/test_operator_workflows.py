from __future__ import annotations

import json
from pathlib import Path

from akc.control.operator_workflows import (
    _ensure_under_scope,
    build_replay_plan_document,
    compute_manifest_intent_diff,
    export_incident_bundle,
    format_replay_forensics_markdown,
    partial_replay_effective_for_manifest,
    read_repo_relative_file,
    replay_decisions_payload_to_forensics,
    try_load_replay_decisions,
    validate_replay_plan_document,
)
from akc.run.manifest import PassRecord, RunManifest
from akc.run.replay_decisions import build_replay_decisions_payload


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def test_partial_replay_effective_matches_manifest_diff_cli_modes() -> None:
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("a"),
        partial_replay_passes=("generate",),
        success_criteria_evaluation_modes=("human_gate",),
    )
    eff = partial_replay_effective_for_manifest(m, evaluation_modes_override=("tests",))
    assert eff["modes_source"] == "cli"
    assert "generate" in eff["effective"]
    assert "execute" in eff["effective"]


def test_build_replay_plan_document_validates_and_suggests_argv(tmp_path: Path) -> None:
    mp = tmp_path / "m.manifest.json"
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("a"),
        partial_replay_passes=(),
        success_criteria_evaluation_modes=("tests",),
    )
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    doc = build_replay_plan_document(manifest=m, manifest_source_path=mp, generated_at_ms=1)
    assert validate_replay_plan_document(doc) == []
    assert doc["suggested_compile"]["replay_mode"] == "partial_replay"
    assert "execute" in doc["intent_replay_context"]["effective_partial_replay_passes"]
    assert doc["manifest"]["path"] == str(mp.resolve())
    assert doc["manifest"]["developer_role_profile_resolution"]["source"] == "default"
    argv = doc["suggested_compile"]["argv_template"]
    assert argv[0] == "akc" and argv[1] == "compile"


def test_build_replay_plan_document_emerging_inserts_profile_argv(tmp_path: Path) -> None:
    mp = tmp_path / "m.manifest.json"
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("a"),
        partial_replay_passes=("generate",),
        success_criteria_evaluation_modes=("tests",),
        control_plane={"developer_role_profile": "emerging"},
    )
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    doc = build_replay_plan_document(manifest=m, manifest_source_path=mp, generated_at_ms=1)
    assert validate_replay_plan_document(doc) == []
    assert doc["manifest"]["developer_role_profile_resolution"]["source"] == "manifest"
    argv = doc["suggested_compile"]["argv_template"]
    assert argv[:4] == ["akc", "--developer-role-profile", "emerging", "compile"]


def test_build_replay_plan_document_cli_overrides_manifest_profile(tmp_path: Path) -> None:
    mp = tmp_path / "m.manifest.json"
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("a"),
        partial_replay_passes=("generate",),
        success_criteria_evaluation_modes=("tests",),
        control_plane={"developer_role_profile": "emerging"},
    )
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    doc = build_replay_plan_document(
        manifest=m,
        manifest_source_path=mp,
        generated_at_ms=1,
        developer_role_profile_cli="classic",
    )
    assert doc["manifest"]["developer_role_profile_resolution"]["source"] == "cli"
    assert doc["manifest"]["developer_role_profile"] == "classic"
    argv = doc["suggested_compile"]["argv_template"]
    assert argv[1] == "compile"


def test_compute_manifest_intent_diff_passes_and_control_plane() -> None:
    left = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("b"),
        partial_replay_passes=("generate",),
        passes=(PassRecord(name="verify", status="succeeded"),),
        control_plane={"replay_decisions_ref": {"path": ".akc/run/r1.replay_decisions.json", "sha256": _hex64("c")}},
    )
    right = RunManifest(
        run_id="r2",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="full_replay",
        stable_intent_sha256=_hex64("d"),
        partial_replay_passes=("execute",),
        passes=(PassRecord(name="verify", status="failed"),),
        control_plane={"replay_decisions_ref": {"path": ".akc/run/r2.replay_decisions.json", "sha256": _hex64("e")}},
    )
    diff = compute_manifest_intent_diff(left=left, right=right, evaluation_modes=("tests",))
    assert diff["stable_intent_sha256"]["match"] is False
    assert "replay_decisions_ref" in diff["control_plane_delta"]
    assert any(p.get("pass") == "verify" for p in diff["pass_status_changes"])
    mp = diff["mandatory_partial_replay_passes"]
    assert isinstance(mp, dict)
    assert mp.get("evaluation_modes_source") == "cli"
    assert mp.get("evaluation_modes") == ["tests"]


def test_replay_forensics_markdown_contains_trigger_histogram() -> None:
    manifest = RunManifest(
        run_id="run_x",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="partial_replay",
        partial_replay_passes=("generate",),
        intent_semantic_fingerprint="a" * 16,
        stable_intent_sha256=_hex64("f"),
        knowledge_semantic_fingerprint="b" * 16,
        knowledge_provenance_fingerprint="c" * 16,
    )
    mandates = frozenset({"intent_acceptance"})
    payload = build_replay_decisions_payload(
        run_id="run_x",
        tenant_id="t1",
        repo_id="repo1",
        replay_mode="partial_replay",
        decision_manifest=manifest,
        baseline_manifest=manifest,
        replay_source_run_id="baseline",
        current_intent_semantic_fingerprint="a" * 16,
        current_knowledge_semantic_fingerprint="b" * 16,
        current_knowledge_provenance_fingerprint="c" * 16,
        current_stable_intent_sha256=_hex64("f"),
        intent_mandatory_partial_replay_passes=mandates,
    )
    report = replay_decisions_payload_to_forensics(payload)
    assert report.get("trigger_reason_histogram")
    md = format_replay_forensics_markdown(report)
    assert "Replay forensics" in md
    assert "per-pass" in md.lower() or "Per-pass" in md


def test_try_load_replay_decisions_via_control_plane_ref(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    run_dir = scope / ".akc" / "run"
    run_dir.mkdir(parents=True)
    rd_path = run_dir / "run-1.replay_decisions.json"
    payload = {"schema_kind": "replay_decisions", "decisions": []}
    rd_path.write_text(json.dumps(payload), encoding="utf-8")
    manifest = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={"replay_decisions_ref": {"path": ".akc/run/run-1.replay_decisions.json", "sha256": _hex64("x")}},
    )
    loaded = try_load_replay_decisions(scope_root=scope, manifest=manifest)
    assert loaded is not None
    assert loaded.get("schema_kind") == "replay_decisions"


def test_export_incident_bundle_writes_summary(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    run_dir = scope / ".akc" / "run"
    run_dir.mkdir(parents=True)
    costs_rel = ".akc/run/run-1.costs.json"
    costs_path = scope / ".akc" / "run" / "run-1.costs.json"
    costs_path.parent.mkdir(parents=True, exist_ok=True)
    costs_path.write_text(json.dumps({"totals": {}}), encoding="utf-8")
    manifest = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        output_hashes={costs_rel: _hex64("c")},
    )
    mp = run_dir / "run-1.manifest.json"
    mp.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")
    out = tmp_path / "bundle"
    res = export_incident_bundle(
        scope_root=scope,
        manifest=manifest,
        manifest_source_path=mp,
        out_dir=out,
        make_zip=False,
        signer_identity="ops@tenant.t1",
        signature="sig-v1:incident",
    )
    summary_path = out / "SUMMARY.json"
    assert summary_path.is_file()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["schema_kind"] == "akc_incident_bundle_summary"
    assert "manifest" in summary["included"]
    assert "knowledge_governance" in summary
    assert "redaction_applied" in summary
    assert "export_metadata" in summary
    meta = summary["export_metadata"]
    assert meta["schema_kind"] == "akc_bundle_export_metadata"
    assert isinstance(meta["hash_manifest"], list)
    assert any(row.get("bundle_relpath") == "data/run.manifest.json" for row in meta["hash_manifest"])
    assert meta["signature"] == {"identity": "ops@tenant.t1", "signature": "sig-v1:incident"}
    kg = summary["knowledge_governance"]
    assert "unresolved_knowledge_conflicts_count" in kg
    assert "knowledge_paths" in kg
    assert kg["knowledge_paths"]["mediation"] == ".akc/knowledge/mediation.json"
    assert res["zip_path"] is None


def test_read_repo_relative_file_blocks_traversal_prefix(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    scope.mkdir(parents=True)
    bad = "../outside.txt"
    try:
        read_repo_relative_file(scope_root=scope, rel_path=bad)
    except ValueError as exc:
        assert "unsafe artifact path" in str(exc)
    else:  # pragma: no cover - explicit regression guard
        raise AssertionError("expected ValueError for traversal path")


def test_read_repo_relative_file_blocks_symlink_escape(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    scope.mkdir(parents=True)
    outside = tmp_path / "outside.secret"
    outside.write_text("top-secret", encoding="utf-8")
    link = scope / "leak.txt"
    link.symlink_to(outside)
    try:
        read_repo_relative_file(scope_root=scope, rel_path="leak.txt")
    except ValueError as exc:
        assert "path escapes repo scope" in str(exc)
    else:  # pragma: no cover - explicit regression guard
        raise AssertionError("expected ValueError for symlink escape")


def test_ensure_under_scope_allows_internal_symlink(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    scope.mkdir(parents=True)
    target = scope / "safe.json"
    target.write_text("{}", encoding="utf-8")
    link = scope / "safe-link.json"
    link.symlink_to(target)
    _ensure_under_scope(scope_root=scope, target=link)
