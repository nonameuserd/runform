from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.compile.interfaces import TenantRepoScope
from akc.outputs.drift import drift_report, write_baseline
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

