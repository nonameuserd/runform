"""Integration checks for runtime bundle IR projection (Phase 3)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.cli import main
from tests.unit.test_cli_compile import (
    _executor_cwd,
    _seed_plan_with_one_step,
    _write_minimal_repo,
)


def test_runtime_bundle_deployment_intents_match_referenced_ir(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Each deployment intent must reference a deployable node present in referenced_ir_nodes."""
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
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))

    referenced = {str(n.get("id")) for n in bundle["referenced_ir_nodes"] if isinstance(n, dict)}
    deploy_kinds = {"service", "integration", "infrastructure", "agent"}
    referenced_deployable = {
        str(n.get("id"))
        for n in bundle["referenced_ir_nodes"]
        if isinstance(n, dict) and str(n.get("kind", "")) in deploy_kinds
    }
    for row in bundle["deployment_intents"]:
        assert isinstance(row, dict)
        nid = str(row.get("node_id", ""))
        assert nid in referenced
        assert nid in referenced_deployable
        assert str(row.get("kind", "")) in deploy_kinds
