from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.artifacts.schemas import RUNTIME_BUNDLE_SCHEMA_VERSION
from akc.cli import main
from akc.run import RunManifest
from tests.unit.test_cli_compile import (
    _executor_cwd,
    _seed_plan_with_one_step,
    _write_minimal_repo,
)


def _write_runtime_bundle(path: Path, *, tenant_id: str = "tenant-a", repo_id: str = "repo-a") -> None:
    payload = {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": "compile-1",
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
        "spec_hashes": {
            "orchestration_spec_sha256": "a" * 64,
            "coordination_spec_sha256": "b" * 64,
        },
        "deployment_intents": [],
        "runtime_policy_envelope": {},
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _runtime_record(outputs_root: Path) -> dict[str, object]:
    record_path = next(outputs_root.rglob("runtime_run.json"))
    return json.loads(record_path.read_text(encoding="utf-8"))


def test_compile_runtime_handoff_persists_scoped_runtime_record(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "compile-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )

    assert excinfo.value.code == 0
    capsys.readouterr()
    record = _runtime_record(outputs_root)
    scope_dir = Path(str(record["scope_dir"]))

    assert record["tenant_id"] == "tenant-a"
    assert record["repo_id"] == "repo-a"
    assert record["run_id"] == "compile-1"
    assert (scope_dir / "checkpoint.json").exists()
    assert (scope_dir / "events.json").exists()
    assert (scope_dir / "queue_snapshot.json").exists()


def test_cross_tenant_checkpoint_read_is_denied(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outputs_root = tmp_path / "out"
    bundle_path = outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "compile-1.runtime_bundle.json"
    _write_runtime_bundle(bundle_path)

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo.value.code == 0
    capsys.readouterr()
    runtime_run_id = str(_runtime_record(outputs_root)["runtime_run_id"])

    with pytest.raises(SystemExit) as excinfo:
        main(
            [
                "runtime",
                "checkpoint",
                "--runtime-run-id",
                runtime_run_id,
                "--outputs-root",
                str(outputs_root),
                "--tenant-id",
                "tenant-b",
                "--repo-id",
                "repo-a",
            ]
        )

    assert excinfo.value.code == 2
    captured = capsys.readouterr()
    assert "runtime scope mismatch" in (captured.out + captured.err)


def test_compile_then_runtime_start_uses_emitted_bundle(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """End-to-end: compile emits a bundle, then ``runtime start --bundle`` accepts it."""
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

    bundle_path = next(base.joinpath(".akc", "runtime").glob("*.runtime_bundle.json"))

    with pytest.raises(SystemExit) as excinfo2:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(bundle_path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert excinfo2.value.code == 0
    capsys.readouterr()
    record_path = next(outputs_root.rglob("runtime_run.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["tenant_id"] == tenant_id
    assert record["repo_id"] == repo_id


def test_compile_emits_runtime_bundle_with_correlated_intent_ref(
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

    manifest_path = next(base.joinpath(".akc", "run").glob("*.manifest.json"))
    manifest = RunManifest.from_json_file(manifest_path)
    bundle_path = next(base.joinpath(".akc", "runtime").glob("*.runtime_bundle.json"))
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))

    assert manifest.stable_intent_sha256 is not None
    assert bundle["intent_ref"]["stable_intent_sha256"] == manifest.stable_intent_sha256
    assert bundle["intent_policy_projection"]["stable_intent_sha256"] == manifest.stable_intent_sha256
    assert manifest.control_plane is not None
    assert manifest.control_plane["stable_intent_sha256"] == manifest.stable_intent_sha256

    assert manifest.ir_document is not None
    assert manifest.ir_format_version is not None
    assert manifest.ir_document.path == f".akc/ir/{manifest.run_id}.json"
    assert manifest.ir_document.sha256 == manifest.ir_sha256
    assert bundle["schema_version"] == RUNTIME_BUNDLE_SCHEMA_VERSION
    assert bundle["schema_id"] == f"akc:runtime_bundle:v{RUNTIME_BUNDLE_SCHEMA_VERSION}"
    assert bundle.get("embed_system_ir") is False
    assert bundle["system_ir_ref"]["path"] == manifest.ir_document.path
    assert bundle["system_ir_ref"]["fingerprint"] == manifest.ir_sha256
    assert bundle["system_ir_ref"]["format_version"] == manifest.ir_format_version
    assert bundle["coordination_ref"]["path"] == f".akc/agents/{manifest.run_id}.coordination.json"
    assert bundle["coordination_ref"]["fingerprint"] == bundle["spec_hashes"]["coordination_spec_sha256"]
    coord_path = base / bundle["coordination_ref"]["path"]
    assert coord_path.exists()
    ir_path = base / manifest.ir_document.path
    assert ir_path.exists()
    ir_obj = json.loads(ir_path.read_text(encoding="utf-8"))
    assert ir_obj["format_version"] == manifest.ir_format_version
