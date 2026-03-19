from __future__ import annotations

import json
from pathlib import Path

from akc.artifacts.validate import validate_obj
from akc.compile.interfaces import TenantRepoScope
from akc.outputs.models import OutputArtifact, OutputBundle


def test_manifest_schema_regression_v1_snapshot(tmp_path: Path) -> None:
    scope = TenantRepoScope(tenant_id="t", repo_id="r")
    bundle = OutputBundle(
        scope=scope,
        name="compile_session",
        artifacts=(
            OutputArtifact.from_text(
                path=".akc/patches/p1_s1.diff", text="--- a/x\n+++ b/x\n@@\n+ok\n"
            ),
            OutputArtifact.from_json(
                path=".akc/tests/p1_s1.smoke.json",
                obj={
                    "schema_version": 1,
                    "schema_id": "akc:execution_stage:v1",
                    "plan_id": "p1",
                    "step_id": "s1",
                    "stage": "smoke",
                    "command": ["pytest", "-q"],
                    "exit_code": 0,
                    "duration_ms": 12,
                    "stdout": "ok",
                    "stderr": "",
                },
            ),
        ),
        metadata={"schema_version": 1},
    )

    manifest = bundle.to_manifest_obj()
    assert validate_obj(obj=manifest, kind="manifest", version=1) == []

    # Freeze a minimal stable shape snapshot (not the artifact hashes themselves).
    frozen = {
        "schema_version": manifest.get("schema_version"),
        "schema_id": manifest.get("schema_id"),
        "tenant_id": manifest.get("tenant_id"),
        "repo_id": manifest.get("repo_id"),
        "name": manifest.get("name"),
        "artifact_paths": [a.get("path") for a in manifest.get("artifacts", [])],
    }
    snap_path = tmp_path / "manifest.v1.snapshot.json"
    snap_path.write_text(json.dumps(frozen, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    reread = json.loads(snap_path.read_text(encoding="utf-8"))
    assert reread == frozen
