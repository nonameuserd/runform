from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.control.operations_index import (
    OperationsIndex,
    infer_outputs_root_from_run_manifest_path,
    operations_sqlite_path,
    try_upsert_operations_index_from_manifest,
)
from akc.run.manifest import RunManifest, RuntimeEvidenceRecord


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _write_manifest(
    *,
    root: Path,
    manifest: RunManifest,
    tenant: str = "t1",
    repo: str = "repo1",
) -> Path:
    scope = root / tenant / repo
    run_dir = scope / ".akc" / "run"
    run_dir.mkdir(parents=True, exist_ok=True)
    mp = run_dir / f"{manifest.run_id}.manifest.json"
    mp.write_text(json.dumps(manifest.to_json_obj()), encoding="utf-8")
    return mp


def test_infer_outputs_root_from_manifest_path(tmp_path: Path) -> None:
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="r1",
        ir_sha256=_hex64(),
        replay_mode="live",
    )
    mp = _write_manifest(root=tmp_path, manifest=m, tenant="t1", repo="r1")
    inferred = infer_outputs_root_from_run_manifest_path(mp)
    assert inferred == tmp_path.resolve()


def test_operations_index_upsert_list_get(tmp_path: Path) -> None:
    trig_path = ".akc/run/run-1.recompile_triggers.json"
    m = RunManifest(
        run_id="run-1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
        passes=(),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("b"),
            "recompile_triggers_ref": {"path": trig_path, "sha256": _hex64("c")},
            "runtime_evidence_ref": {"path": ".akc/runtime/x/evidence.json", "sha256": _hex64("d")},
        },
    )
    mp = _write_manifest(root=tmp_path, manifest=m)
    scope = tmp_path / "t1" / "repo1"
    triggers_obj = {
        "tenant_id": "t1",
        "repo_id": "repo1",
        "checked_at_ms": 1,
        "triggers": [{"kind": "intent_stable_changed"}],
    }
    (scope / ".akc" / "run" / "run-1.recompile_triggers.json").write_text(json.dumps(triggers_obj), encoding="utf-8")

    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    db = operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1")
    assert db.is_file()

    idx = OperationsIndex(sqlite_path=db)
    rows = idx.list_runs(tenant_id="t1", limit=10)
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run-1"
    assert rows[0]["recompile_trigger_count"] == 1
    assert rows[0]["runtime_evidence_present"] is True
    assert rows[0]["stable_intent_sha256"] == _hex64("b")

    full = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-1")
    assert full is not None
    kinds = {str(s["kind"]) for s in full["sidecars"]}  # type: ignore[index]
    assert "recompile_triggers_ref" in kinds
    assert "runtime_evidence_ref" in kinds


def test_operations_index_indexes_quality_fields_and_sidecar(tmp_path: Path) -> None:
    quality_rel = ".akc/run/run-q.quality.json"
    m = RunManifest(
        run_id="run-q",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "quality_contract_fingerprint": "1234abcd5678ef90",
            "quality_overall_score": 0.82,
            "quality_gate_failed_dimensions": ["judgment"],
            "quality_advisory_dimensions": ["taste", "user_empathy"],
            "quality_dimension_scores": {"judgment": 0.52, "taste": 0.71},
            "quality_sidecar_ref": {"path": quality_rel, "sha256": _hex64("f")},
        },
    )
    mp = _write_manifest(root=tmp_path, manifest=m)
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    db = operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1")
    idx = OperationsIndex(sqlite_path=db)

    rows = idx.list_runs(tenant_id="t1", limit=10)
    assert len(rows) == 1
    assert rows[0]["quality_contract_fingerprint"] == "1234abcd5678ef90"
    assert rows[0]["quality_gate_failed_count"] == 1
    assert rows[0]["quality_advisory_count"] == 2
    qscores = rows[0]["quality_dimension_scores"]
    assert isinstance(qscores, dict)
    assert qscores["judgment"] == 0.52

    full = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-q")
    assert full is not None
    assert full["quality_overall_score"] == 0.82
    kinds = {str(s["kind"]) for s in full["sidecars"]}  # type: ignore[index]
    assert "quality_sidecar_ref" in kinds


def test_operations_index_policy_bundle_artifact(tmp_path: Path) -> None:
    m = RunManifest(
        run_id="run-pb",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
    )
    mp = _write_manifest(root=tmp_path, manifest=m)
    scope = tmp_path / "t1" / "repo1"
    ctrl = scope / ".akc" / "control"
    ctrl.mkdir(parents=True, exist_ok=True)
    bundle = {
        "schema_kind": "akc_policy_bundle",
        "version": 1,
        "rollout_stage": "enforce",
        "revision_id": "rev-a",
        "pins": {"opa_bundle_sha256": "b" * 64},
    }
    (ctrl / "policy_bundle.json").write_text(
        json.dumps(bundle, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    idx = OperationsIndex(operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    rows = idx.list_runs(tenant_id="t1", limit=5)
    assert len(rows) == 1
    pba = rows[0].get("policy_bundle_artifact")
    assert isinstance(pba, dict)
    assert pba.get("rollout_stage") == "enforce"
    assert pba.get("revision_id") == "rev-a"
    assert pba.get("rel_path") == ".akc/control/policy_bundle.json"
    assert len(str(pba.get("fingerprint_sha256"))) == 64

    full = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-pb")
    assert full is not None
    assert full.get("policy_bundle_artifact") == pba


def test_operations_index_includes_operational_predicate_summary(tmp_path: Path) -> None:
    report_rel = ".akc/runtime/run-op/operational_validity_report.json"
    m = RunManifest(
        run_id="run-op",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "operational_validity_passed": False,
            "operational_validity_report_ref": {"path": report_rel, "sha256": _hex64("d")},
        },
    )
    mp = _write_manifest(root=tmp_path, manifest=m)
    scope = tmp_path / "t1" / "repo1"
    report_path = scope / report_rel
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(
        json.dumps(
            {
                "schema_id": "akc:operational_validity_report:v1",
                "schema_version": 1,
                "tenant_id": "t1",
                "repo_id": "repo1",
                "run_id": "run-op",
                "evaluated_at_ms": 1,
                "passed": False,
                "operational_spec_version": 1,
                "predicate_results": [
                    {
                        "success_criterion_id": "sc-1",
                        "predicate_kind": "presence",
                        "signal_key": "terminal_health",
                        "passed": False,
                        "message": "health status missing",
                        "details": {"payload_path": "health_status", "actual": "missing"},
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    idx = OperationsIndex(operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    row = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-op")
    assert row is not None
    summary = row.get("operational_predicate_summary")
    assert isinstance(summary, dict)
    assert summary["failed_count"] == 1
    failing = summary.get("failing")
    assert isinstance(failing, list)
    assert failing[0]["success_criterion_id"] == "sc-1"
    assert failing[0]["signal_key"] == "terminal_health"


def test_operations_index_operational_predicate_summary_rejects_cross_repo_ref(tmp_path: Path) -> None:
    leaked_rel = "../repo2/.akc/runtime/run-z/operational_validity_report.json"
    m = RunManifest(
        run_id="run-op",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "operational_validity_passed": False,
            "operational_validity_report_ref": {"path": leaked_rel, "sha256": _hex64("d")},
        },
    )
    mp = _write_manifest(root=tmp_path, manifest=m)
    leak_path = tmp_path / "t1" / "repo2" / ".akc" / "runtime" / "run-z" / "operational_validity_report.json"
    leak_path.parent.mkdir(parents=True, exist_ok=True)
    leak_path.write_text(
        json.dumps(
            {
                "tenant_id": "t1",
                "repo_id": "repo2",
                "run_id": "run-z",
                "evaluated_at_ms": 1,
                "passed": False,
                "operational_spec_version": 1,
                "predicate_results": [{"signal_key": "secret", "passed": False}],
            }
        ),
        encoding="utf-8",
    )

    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    idx = OperationsIndex(operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    row = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-op")
    assert row is not None
    assert row.get("operational_predicate_summary") is None


def test_operations_index_run_labels_from_manifest_and_clear(tmp_path: Path) -> None:
    m = RunManifest(
        run_id="run-lab",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("b"),
            "run_labels": {"tier": "prod", "team": "platform"},
        },
    )
    mp = _write_manifest(root=tmp_path, manifest=m)
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    db = operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1")
    idx = OperationsIndex(sqlite_path=db)
    row = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-lab")
    assert row is not None
    assert row["labels"] == {"team": "platform", "tier": "prod"}

    m2 = RunManifest(
        run_id="run-lab",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("b"),
            "run_labels": {},
        },
    )
    mp.write_text(json.dumps(m2.to_json_obj()), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    row2 = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-lab")
    assert row2 is not None
    assert row2["labels"] == {}


def test_operations_index_run_labels_preserved_when_absent_on_manifest(tmp_path: Path) -> None:
    m = RunManifest(
        run_id="run-x",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        stable_intent_sha256=_hex64("b"),
    )
    mp = _write_manifest(root=tmp_path, manifest=m)
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    db = operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1")
    idx = OperationsIndex(sqlite_path=db)
    idx.upsert_label(
        tenant_id="t1",
        repo_id="repo1",
        run_id="run-x",
        label_key="fleet",
        label_value="east",
    )
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    row = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="run-x")
    assert row is not None
    assert row["labels"] == {"fleet": "east"}


def test_list_runs_filters(tmp_path: Path) -> None:
    m1 = RunManifest(
        run_id="run-a",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="live",
        stable_intent_sha256=_hex64("f"),
        passes=(),
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("f"),
            "recompile_triggers_ref": {"path": "", "sha256": _hex64("e")},
        },
    )
    # Invalid ref shape — trigger count stays 0
    mp1 = _write_manifest(root=tmp_path, manifest=m1)
    m2 = RunManifest(
        run_id="run-b",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("2"),
        replay_mode="live",
        stable_intent_sha256=_hex64("f"),
        passes=(),
        runtime_evidence=(
            RuntimeEvidenceRecord(
                evidence_type="terminal_health",
                timestamp=1,
                runtime_run_id="rt",
                payload={"health_status": "healthy", "aggregate": True},
            ),
        ),
    )
    mp2 = _write_manifest(root=tmp_path, manifest=m2)
    OperationsIndex.upsert_from_manifest_path(mp1, outputs_root=tmp_path)
    OperationsIndex.upsert_from_manifest_path(mp2, outputs_root=tmp_path)
    idx = OperationsIndex(operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    intent = _hex64("f")
    hits = idx.list_runs(tenant_id="t1", stable_intent_sha256=intent)
    assert len(hits) == 2
    no_trig = idx.list_runs(tenant_id="t1", has_recompile_triggers=False)
    assert {r["run_id"] for r in no_trig} == {"run-a", "run-b"}
    ev = idx.list_runs(tenant_id="t1", runtime_evidence_present=True)
    assert len(ev) == 1
    assert ev[0]["run_id"] == "run-b"


def test_upsert_rejects_tenant_path_mismatch(tmp_path: Path) -> None:
    m = RunManifest(
        run_id="run-z",
        tenant_id="t2",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
    )
    mp = _write_manifest(root=tmp_path, manifest=m, tenant="t1", repo="repo1")
    with pytest.raises(ValueError, match="tenant_id"):
        OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)


def test_rebuild_for_tenant(tmp_path: Path) -> None:
    m = RunManifest(run_id="r9", tenant_id="t1", repo_id="x", ir_sha256=_hex64(), replay_mode="live")
    _write_manifest(root=tmp_path, manifest=m, tenant="t1", repo="x")
    n = OperationsIndex.rebuild_for_tenant(outputs_root=tmp_path, tenant_id="t1")
    assert n == 1
    idx = OperationsIndex(operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    assert len(idx.list_runs(tenant_id="t1")) == 1


def test_list_runs_offset(tmp_path: Path) -> None:
    for rid in ("r0", "r1", "r2"):
        m = RunManifest(
            run_id=rid,
            tenant_id="t1",
            repo_id="repo1",
            ir_sha256=_hex64(),
            replay_mode="live",
        )
        _write_manifest(root=tmp_path, manifest=m)
        mp = tmp_path / "t1" / "repo1" / ".akc" / "run" / f"{rid}.manifest.json"
        OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)
    idx = OperationsIndex(operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1"))
    first = idx.list_runs(tenant_id="t1", limit=1, offset=0)
    second = idx.list_runs(tenant_id="t1", limit=1, offset=1)
    assert len(first) == 1 and len(second) == 1
    assert first[0]["run_id"] == "r2"
    assert second[0]["run_id"] == "r1"


def test_try_upsert_swallows_bad_file(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    bad = tmp_path / "not-a-manifest.json"
    bad.write_text("{}", encoding="utf-8")
    with caplog.at_level("DEBUG"):
        try_upsert_operations_index_from_manifest(bad)
    assert "operations index upsert failed" in caplog.text


def test_automation_checkpoint_roundtrip(tmp_path: Path) -> None:
    db = operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1")
    idx = OperationsIndex(sqlite_path=db)
    idx.upsert_automation_checkpoint(
        dedupe_key="k1",
        tenant_id="t1",
        repo_id="repo1",
        run_id="r1",
        action="metadata_tag_write",
        policy_version="p1",
        shard_id="s1",
        status="pending",
        attempts=1,
        next_attempt_at_ms=123,
        last_error="boom",
        last_result={"x": "y"},
        updated_at_ms=111,
    )
    row = idx.get_automation_checkpoint(dedupe_key="k1")
    assert row is not None
    assert row["status"] == "pending"
    assert row["attempts"] == 1
    assert row["next_attempt_at_ms"] == 123
    assert row["last_error"] == "boom"
