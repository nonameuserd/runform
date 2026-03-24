"""Phase 1: end-to-end artifact chain for the runtime CLI (bundle → kernel → files → evidence → manifest)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.artifacts.validate import validate_artifact_json
from akc.cli import main
from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.run.manifest import RunManifest
from akc.utils.fingerprint import stable_json_fingerprint


def _workflow_runtime_bundle_payload() -> dict[str, object]:
    """Same shape as ``tests/integration/test_runtime_e2e_native._bundle`` (one operational node)."""
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    run_id = "compile-1"
    contract = OperationalContract(
        contract_id="contract-1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="kernel_started",
                source="runtime.kernel.started",
                details={"event_type": "runtime.kernel.started"},
            ),
        ),
        io_contract=IOContract(
            input_keys=("runtime_run_id",),
            output_keys=("action_id", "action_type", "adapter_id"),
        ),
    )
    node = {
        "id": "node-1",
        "tenant_id": tenant_id,
        "kind": "workflow",
        "name": "Workflow 1",
        "properties": {"order_idx": 0},
        "depends_on": [],
        "contract": contract.to_json_obj(),
    }
    return {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "referenced_ir_nodes": [node],
        "referenced_contracts": [contract.to_json_obj()],
        "spec_hashes": {
            "orchestration_spec_sha256": "a" * 64,
            "coordination_spec_sha256": "b" * 64,
        },
        "deployment_intents": [
            {
                "node_id": "node-1",
                "kind": "workflow",
                "name": "Workflow 1",
                "depends_on": [],
                "effects": None,
                "contract_id": "contract-1",
            }
        ],
        "runtime_policy_envelope": {},
    }


def test_runtime_cli_bundle_to_manifest_pointer_chain(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    outputs_root = tmp_path / "out"
    tenant_id = "tenant-a"
    repo_id = "repo-a"
    run_id = "compile-1"
    base = outputs_root / tenant_id / repo_id
    bundle_path = base / ".akc" / "runtime" / f"{run_id}.runtime_bundle.json"
    bundle_path.parent.mkdir(parents=True, exist_ok=True)
    bundle_obj = _workflow_runtime_bundle_payload()
    bundle_path.write_text(json.dumps(bundle_obj), encoding="utf-8")

    manifest_path = base / ".akc" / "run" / f"{run_id}.manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    seed_manifest = RunManifest(
        run_id=run_id,
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="a" * 64,
        replay_mode="live",
    )
    manifest_path.write_text(json.dumps(seed_manifest.to_json_obj(), indent=2), encoding="utf-8")

    pre_manifest_fp = stable_json_fingerprint(json.loads(manifest_path.read_text(encoding="utf-8")))

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

    record_path = next(outputs_root.rglob("runtime_run.json"))
    record = json.loads(record_path.read_text(encoding="utf-8"))
    scope_dir = Path(str(record["scope_dir"]))

    assert record["tenant_id"] == tenant_id
    assert record["repo_id"] == repo_id
    assert record["run_id"] == run_id
    assert (scope_dir / "runtime_run.json").exists()
    assert (scope_dir / "checkpoint.json").exists()
    assert (scope_dir / "events.json").exists()
    assert (scope_dir / "runtime_evidence.json").exists()

    events_raw = json.loads((scope_dir / "events.json").read_text(encoding="utf-8"))
    assert isinstance(events_raw, list)
    assert any(item.get("event_type") == "runtime.kernel.started" for item in events_raw if isinstance(item, dict))
    assert any(item.get("event_type") == "runtime.action.completed" for item in events_raw if isinstance(item, dict))

    evidence_raw = json.loads((scope_dir / "runtime_evidence.json").read_text(encoding="utf-8"))
    validate_artifact_json(obj=evidence_raw, kind="runtime_evidence_stream")
    evidence_types = {item.get("evidence_type") for item in evidence_raw if isinstance(item, dict)}
    assert "reconcile_outcome" in evidence_types
    assert "reconcile_resource_status" in evidence_types
    assert "terminal_health" in evidence_types
    assert "transition_application" in evidence_types

    updated_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert stable_json_fingerprint(updated_manifest) != pre_manifest_fp
    cp = updated_manifest.get("control_plane")
    assert isinstance(cp, dict)
    assert str(cp.get("runtime_run_id", "")).strip() == str(record["runtime_run_id"]).strip()
    ev_ref = cp.get("runtime_evidence_ref")
    assert isinstance(ev_ref, dict)
    digest = str(ev_ref.get("sha256", "")).strip().lower()
    assert len(digest) == 64
    assert all(c in "0123456789abcdef" for c in digest)
