"""Integration: partial replay with missing on-disk artifact files forces regeneration."""

from __future__ import annotations

from pathlib import Path

import pytest

from akc.cli import main
from akc.pass_registry import ARTIFACT_PASS_ORDER
from akc.run import RunManifest
from tests.unit.test_cli_compile import (
    _executor_cwd,
    _seed_plan_with_one_step,
    _write_minimal_repo,
)


def test_partial_replay_missing_artifact_files_runs_fresh_artifact_passes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    outputs_root = tmp_path / "out"
    base = outputs_root / tenant_id / repo_id

    _write_minimal_repo(_executor_cwd(outputs_root, tenant_id, repo_id))
    _seed_plan_with_one_step(tenant_id=tenant_id, repo_id=repo_id, outputs_root=outputs_root)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
            ]
        )
    assert excinfo.value.code == 0
    capsys.readouterr()

    first_manifest_path = next(base.joinpath(".akc", "run").glob("*.manifest.json"))
    first_manifest = RunManifest.from_json_file(first_manifest_path)
    for rec in first_manifest.passes:
        if rec.name not in ARTIFACT_PASS_ORDER:
            continue
        md = rec.metadata or {}
        paths = md.get("artifact_paths")
        assert isinstance(paths, list)
        for rel in paths:
            if not isinstance(rel, str) or not rel.strip():
                continue
            p = base / rel
            if p.is_file():
                p.unlink()

    with pytest.raises(SystemExit) as excinfo2:
        main(
            [
                "compile",
                "--tenant-id",
                tenant_id,
                "--repo-id",
                repo_id,
                "--outputs-root",
                str(outputs_root),
                "--mode",
                "quick",
                "--replay-mode",
                "partial_replay",
                # Keep the ARCS loop live enough to succeed; artifact lowering passes stay
                # off this list so they still attempt manifest cache reuse (then regenerate).
                "--partial-replay-passes",
                "generate,execute,verify,intent_acceptance",
                "--replay-manifest-path",
                str(first_manifest_path),
            ]
        )
    assert excinfo2.value.code == 0
    capsys.readouterr()

    run_manifests = sorted(
        base.joinpath(".akc", "run").glob("*.manifest.json"),
        key=lambda p: p.stat().st_mtime,
    )
    assert len(run_manifests) >= 2
    latest = RunManifest.from_json_file(run_manifests[-1])
    span_names = {str(s.get("name", "")) for s in latest.trace_spans if isinstance(s, dict)}
    for name in ARTIFACT_PASS_ORDER:
        assert f"compile.artifact.{name}" in span_names

    for span in latest.trace_spans:
        if not isinstance(span, dict):
            continue
        name = str(span.get("name", ""))
        if not name.startswith("compile.artifact."):
            continue
        attrs = span.get("attributes")
        if not isinstance(attrs, dict):
            continue
        assert attrs.get("reused_from_replay_manifest") is False
