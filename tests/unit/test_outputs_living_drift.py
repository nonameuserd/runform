from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.validate import validate_obj
from akc.compile.controller_config import Budget
from akc.compile.interfaces import TenantRepoScope
from akc.intent import compile_intent_spec, compute_intent_fingerprint
from akc.outputs.drift import drift_report, write_baseline, write_drift_artifacts
from akc.outputs.emitters import JsonManifestEmitter
from akc.outputs.fingerprints import fingerprint_ingestion_state
from akc.outputs.models import OutputArtifact, OutputBundle


def test_fingerprint_ingestion_state_filters_to_tenant(tmp_path: Path) -> None:
    state_path = tmp_path / "docs.state.json"
    state_path.write_text(
        json.dumps(
            {
                "t1::docs::a.md": {"kind": "docs", "path": "/x/a.md", "mtime_ns": 1, "size": 10},
                "t2::docs::b.md": {"kind": "docs", "path": "/x/b.md", "mtime_ns": 2, "size": 20},
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    fp = fingerprint_ingestion_state(tenant_id="t1", state_path=state_path)
    assert fp.tenant_id == "t1"
    assert fp.keys_included == 1


def test_drift_report_detects_missing_manifest(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    r = drift_report(scope=scope, outputs_root=tmp_path)
    assert r.has_drift()
    assert any(f.kind == "missing_manifest" for f in r.findings)


def test_drift_report_detects_changed_outputs(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=tmp_path)

    # Mutate output file after emission.
    out = tmp_path / "t1" / "repo1" / "out.txt"
    out.write_text("mutated", encoding="utf-8")

    r = drift_report(scope=scope, outputs_root=tmp_path)
    assert r.has_drift()
    assert any(f.kind == "changed_outputs" for f in r.findings)


def test_drift_report_detects_changed_sources_when_baseline_present(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    # Emit a minimal output + manifest.
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=tmp_path)

    # Create ingestion state and baseline.
    state_path = tmp_path / "docs.state.json"
    state_path.write_text(
        json.dumps(
            {
                "t1::docs::a.md": {
                    "kind": "docs",
                    "path": "/x/a.md",
                    "mtime_ns": 1,
                    "size": 10,
                }
            }
        ),
        encoding="utf-8",
    )
    fp1 = fingerprint_ingestion_state(tenant_id="t1", state_path=state_path)
    baseline_path = tmp_path / "t1" / "repo1" / ".akc" / "living" / "baseline.json"
    write_baseline(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=fp1,
        baseline_path=baseline_path,
    )

    # Change the ingestion state.
    state_path.write_text(
        json.dumps(
            {
                "t1::docs::a.md": {
                    "kind": "docs",
                    "path": "/x/a.md",
                    "mtime_ns": 2,
                    "size": 10,
                }
            }
        ),
        encoding="utf-8",
    )
    fp2 = fingerprint_ingestion_state(tenant_id="t1", state_path=state_path)
    r = drift_report(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=fp2,
        baseline_path=baseline_path,
    )
    assert r.has_drift()
    assert any(f.kind == "changed_sources" for f in r.findings)


def test_write_baseline_requires_scope_ids(tmp_path: Path) -> None:
    # TenantRepoScope validates IDs at construction time.
    with pytest.raises(ValueError, match="tenant_id must be a non-empty string"):
        write_baseline(
            scope=TenantRepoScope(tenant_id="", repo_id="r1"),
            outputs_root=tmp_path,
            ingest_fingerprint=None,
            baseline_path=tmp_path / "baseline.json",
        )


def test_drift_report_detects_changed_intent_when_baseline_present(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    # Emit a minimal output + manifest.
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=tmp_path)

    baseline_path = tmp_path / "t1" / "repo1" / ".akc" / "living" / "baseline.json"

    # Baseline from goal A.
    intent_a = compile_intent_spec(
        tenant_id=scope.tenant_id,
        repo_id=scope.repo_id,
        goal_statement="Goal A",
        controller_budget=Budget(),
    )
    fp_a = compute_intent_fingerprint(intent=intent_a)
    write_baseline(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=None,
        baseline_path=baseline_path,
        intent_semantic_fingerprint=fp_a.semantic,
        intent_goal_text_fingerprint=fp_a.goal_text,
    )

    # Current state corresponds to goal B.
    intent_b = compile_intent_spec(
        tenant_id=scope.tenant_id,
        repo_id=scope.repo_id,
        goal_statement="Goal B",
        controller_budget=Budget(),
    )
    fp_b = compute_intent_fingerprint(intent=intent_b)
    r = drift_report(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=None,
        baseline_path=baseline_path,
        intent_semantic_fingerprint=fp_b.semantic,
        intent_goal_text_fingerprint=fp_b.goal_text,
    )
    assert r.has_drift()
    assert any(f.kind == "changed_intent" for f in r.findings)


def test_drift_report_detects_changed_knowledge_semantic_when_baseline_present(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    # Emit a minimal output + manifest.
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=tmp_path)

    baseline_path = tmp_path / "t1" / "repo1" / ".akc" / "living" / "baseline.json"
    write_baseline(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=None,
        baseline_path=baseline_path,
        knowledge_semantic_fingerprint="aaaaaaaaaaaaaaaa",
    )

    r = drift_report(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=None,
        baseline_path=baseline_path,
        knowledge_semantic_fingerprint="bbbbbbbbbbbbbbbb",
    )
    assert r.has_drift()
    assert any(f.kind == "changed_knowledge_semantic" for f in r.findings)


def test_drift_report_detects_changed_knowledge_provenance_when_baseline_present(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")

    # Emit a minimal output + manifest.
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=tmp_path)

    baseline_path = tmp_path / "t1" / "repo1" / ".akc" / "living" / "baseline.json"
    write_baseline(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=None,
        baseline_path=baseline_path,
        knowledge_provenance_fingerprint="1111111111111111",
    )

    r = drift_report(
        scope=scope,
        outputs_root=tmp_path,
        ingest_fingerprint=None,
        baseline_path=baseline_path,
        knowledge_provenance_fingerprint="2222222222222222",
    )
    assert r.has_drift()
    assert any(f.kind == "changed_knowledge_provenance" for f in r.findings)


def test_write_drift_artifacts_emits_schema_versioned_sidecars(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    bundle = OutputBundle(
        scope=scope,
        name="demo",
        artifacts=(OutputArtifact.from_text(path="out.txt", text="ok"),),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=tmp_path)

    report = drift_report(scope=scope, outputs_root=tmp_path)
    drift_path, triggers_path = write_drift_artifacts(
        scope=scope,
        outputs_root=tmp_path,
        report=report,
        check_id="run-123",
        triggers=[{"kind": "intent_semantic_changed", "details": {"before": "a", "after": "b"}}],
        source="manual",
    )

    drift_payload = json.loads(drift_path.read_text(encoding="utf-8"))
    triggers_payload = json.loads(triggers_path.read_text(encoding="utf-8"))
    assert validate_obj(obj=drift_payload, kind="living_drift_report", version=1) == []
    assert validate_obj(obj=triggers_payload, kind="recompile_triggers", version=1) == []
    assert drift_path.name == "run-123.drift.json"
    assert triggers_path.name == "run-123.triggers.json"
    assert triggers_payload["run_id"] == "run-123"
    assert triggers_payload["source"] == "manual"
    assert triggers_payload["triggers"][0]["source"] == "manual"
    assert "checked_at_ms" in triggers_payload["triggers"][0]

    manifest_payload = json.loads((tmp_path / "t1" / "repo1" / "manifest.json").read_text(encoding="utf-8"))
    living_md = manifest_payload.get("metadata", {}).get("living_artifacts", {})
    assert living_md.get("latest_check_id") == "run-123"
    assert living_md.get("groups", {}).get("living") == ["drift_report", "recompile_triggers"]
    assert living_md.get("artifacts", {}).get("drift_report") == ".akc/living/run-123.drift.json"
    manifest_artifacts = {str(item.get("path")) for item in manifest_payload.get("artifacts", [])}
    assert ".akc/living/run-123.drift.json" in manifest_artifacts
    assert ".akc/living/run-123.triggers.json" in manifest_artifacts
