from __future__ import annotations

import json
import zipfile
from pathlib import Path

from akc.compile.interfaces import TenantRepoScope
from akc.memory.plan_state import JsonFilePlanStateStore
from akc.outputs.emitters import JsonManifestEmitter
from akc.outputs.models import OutputArtifact, OutputBundle
from akc.viewer import ViewerInputs, load_viewer_snapshot
from akc.viewer.export import export_bundle
from akc.viewer.web import build_static_viewer


def test_viewer_export_and_web_bundle(tmp_path: Path) -> None:
    tenant_id = "t1"
    repo_id = "r1"

    # Plan state under tmp/.akc/plan
    plan_store = JsonFilePlanStateStore(base_dir=tmp_path)
    plan = plan_store.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="ship viewer",
        initial_steps=["step a", "step b"],
    )
    step_id = plan.steps[0].id

    # Outputs under outputs_root/<tenant>/<repo>/...
    outputs_root = tmp_path / "out"
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    bundle = OutputBundle(
        scope=scope,
        name="compile_session",
        artifacts=(
            OutputArtifact.from_text(
                path=".akc/tests/demo.json",
                text=json.dumps({"plan_id": plan.id, "step_id": step_id, "command": []}),
                media_type="application/json; charset=utf-8",
                metadata={"plan_id": plan.id, "step_id": step_id},
            ),
            OutputArtifact.from_text(
                path=".akc/tests/demo.stdout.txt",
                text="ok\n",
                media_type="text/plain; charset=utf-8",
                metadata={"plan_id": plan.id, "step_id": step_id, "stream": "stdout"},
            ),
        ),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=outputs_root)

    snap = load_viewer_snapshot(
        ViewerInputs(
            tenant_id=tenant_id,
            repo_id=repo_id,
            outputs_root=outputs_root,
            plan_base_dir=tmp_path,
        )
    )
    assert snap.plan.id == plan.id
    assert snap.manifest is not None
    assert any(r.relpath == ".akc/tests/demo.json" for r in snap.evidence.all)

    export_dir = tmp_path / "export"
    res = export_bundle(snapshot=snap, out_dir=export_dir, make_zip=True)
    assert (export_dir / "data" / "plan.json").exists()
    assert (export_dir / "data" / "manifest.json").exists()
    assert (export_dir / "files" / ".akc" / "tests" / "demo.json").exists()
    assert res.zip_path is not None and res.zip_path.exists()

    with zipfile.ZipFile(res.zip_path, "r") as zf:
        names = set(zf.namelist())
    assert "data/plan.json" in names
    assert "data/manifest.json" in names
    assert "files/.akc/tests/demo.json" in names

    web_dir = tmp_path / "web"
    web = build_static_viewer(snapshot=snap, out_dir=web_dir)
    assert web.index_html.exists()
    assert (web_dir / "data" / "plan.json").exists()
    assert (web_dir / "files" / ".akc" / "tests" / "demo.stdout.txt").exists()
