"""Integration: opt-in exit code 3 when reconcile does not converge (simulate + in-memory provider)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.artifacts.contracts import schema_id_for
from akc.cli import main


def _bundle_path(outputs_root: Path) -> Path:
    return outputs_root / "tenant-a" / "repo-a" / ".akc" / "runtime" / "run-1.runtime_bundle.json"


def _base_bundle_payload(*, nonzero_exit: bool) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "schema_id": schema_id_for(kind="runtime_bundle"),
        "tenant_id": "tenant-a",
        "repo_id": "repo-a",
        "run_id": "run-1",
        "referenced_ir_nodes": [],
        "referenced_contracts": [],
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
    if nonzero_exit:
        payload["runtime_nonzero_exit_on_reconcile_divergence"] = True
    return payload


def _runtime_record(outputs_root: Path) -> dict[str, object]:
    record_path = next(outputs_root.rglob("runtime_run.json"))
    return json.loads(record_path.read_text(encoding="utf-8"))


def test_runtime_start_simulate_divergence_exits_0_without_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Simulate mode does not apply changes; reconcile stays divergent but default CLI exits 0."""

    outputs_root = tmp_path / "out"
    path = _bundle_path(outputs_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_base_bundle_payload(nonzero_exit=False)), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert exc.value.code == 0
    capsys.readouterr()
    record = _runtime_record(outputs_root)
    assert record.get("reconcile_all_converged") is False


def test_runtime_start_simulate_divergence_exits_3_with_flag(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outputs_root = tmp_path / "out"
    path = _bundle_path(outputs_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_base_bundle_payload(nonzero_exit=True)), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        main(
            [
                "runtime",
                "start",
                "--bundle",
                str(path),
                "--mode",
                "simulate",
                "--outputs-root",
                str(outputs_root),
            ]
        )
    assert exc.value.code == 3
    err = capsys.readouterr().err
    assert "convergence contract" in err
    record = _runtime_record(outputs_root)
    assert record.get("status") == "failed"
    assert record.get("reconcile_all_converged") is False
