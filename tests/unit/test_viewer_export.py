from __future__ import annotations

import json
import zipfile
from pathlib import Path

from akc.compile.interfaces import TenantRepoScope
from akc.knowledge.persistence import write_knowledge_mediation_report_artifact
from akc.memory.plan_state import JsonFilePlanStateStore
from akc.outputs.emitters import JsonManifestEmitter
from akc.outputs.models import OutputArtifact, OutputBundle
from akc.viewer import ViewerInputs, load_viewer_snapshot
from akc.viewer.export import export_bundle
from akc.viewer.web import VIEWER_UI_VERSION, build_static_viewer


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

    scope_dir = outputs_root / tenant_id / repo_id
    decisions_path = scope_dir / ".akc" / "run" / f"{plan.id}.developer_profile_decisions.json"
    decisions_path.parent.mkdir(parents=True, exist_ok=True)
    decisions_path.write_text(
        json.dumps(
            {"developer_role_profile": "emerging", "resolved": {"sandbox": {"value": "dev", "source": "test"}}},
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    man_path = scope_dir / "manifest.json"
    if man_path.is_file():
        man = json.loads(man_path.read_text(encoding="utf-8"))
        man["control_plane"] = {
            "schema_id": "akc:control_plane_envelope:v1",
            "developer_role_profile": "emerging",
            "developer_profile_decisions_ref": {
                "path": f".akc/run/{plan.id}.developer_profile_decisions.json",
                "sha256": "0" * 64,
            },
        }
        man_path.write_text(json.dumps(man, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    forensics_dir = scope_dir / ".akc" / "viewer" / "forensics" / "20990101T000000Z"
    forensics_dir.mkdir(parents=True, exist_ok=True)
    (forensics_dir / "FORENSICS.json").write_text(
        json.dumps(
            {
                "schema_kind": "akc_forensics_bundle",
                "version": 1,
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "run_id": "run-x",
                "scope_root": str(scope_dir),
                "outputs_root": str(outputs_root),
                "operations_index": {"sqlite_path": "/tmp/x.sqlite", "row_found": True, "run": {"run_id": "run-x"}},
                "replay": {
                    "included": True,
                    "forensics_summary": {"schema_kind": "akc_replay_forensics", "triggers": []},
                },
                "coordination_audit": {"included": False, "tail_line_count": None},
                "otel": {"exports": [{"source_relpath": ".akc/run/run-x.otel.jsonl"}]},
                "knowledge_snapshot": {"included": False},
                "omitted": [],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    play_dir = outputs_root / tenant_id / ".akc" / "control" / "playbooks"
    play_dir.mkdir(parents=True, exist_ok=True)
    (play_dir / "20990101T000001Z.json").write_text(
        json.dumps(
            {
                "schema_kind": "akc_operator_playbook_report",
                "version": 1,
                "generated_at_ms": 1,
                "inputs": {
                    "tenant_id": tenant_id,
                    "repo_id": repo_id,
                    "outputs_root": str(outputs_root),
                    "run_ids": {"a": "run-a", "b": "run-b"},
                },
                "steps": [{"name": "manifest_diff", "status": "ok", "duration_ms": 0}],
                "manifest_diff": {"ok": True},
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    write_knowledge_mediation_report_artifact(
        scope_dir,
        tenant_id=tenant_id,
        repo_id=repo_id,
        mediation_report={
            "policy": "warn_and_continue",
            "events": [
                {
                    "kind": "supersedes",
                    "conflict_group_id": "cg-test",
                    "winner_assertion_id": "a1",
                    "loser_assertion_id": "a2",
                }
            ],
        },
    )

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
    kobs_raw = json.loads((export_dir / "data" / "knowledge_obs.json").read_text(encoding="utf-8"))
    assert "mediation_events" in kobs_raw
    assert "conflict_groups" in kobs_raw
    assert "supersession_hints" in kobs_raw
    assert kobs_raw["supersession_hints"][0]["conflict_group_id"] == "cg-test"
    assert (export_dir / "data" / "knowledge_obs.json").exists()
    op = json.loads((export_dir / "data" / "operator_panels.json").read_text(encoding="utf-8"))
    assert op["forensics"] is not None
    assert op["forensics"]["summary"]["run_id"] == "run-x"
    assert op["playbook"] is not None
    assert op["playbook"]["summary"]["manifest_diff_present"] is True
    assert op.get("profile_panel", {}).get("available") is True
    assert op["profile_panel"]["developer_role_profile"] == "emerging"
    assert op["profile_panel"]["developer_profile_decisions"] is not None
    sc = op["profile_panel"].get("scope_context") or {}
    assert sc.get("tenant_id") == tenant_id
    assert sc.get("repo_id") == repo_id
    assert sc.get("run_id") == plan.id
    assert sc.get("outputs_root") == str(outputs_root.resolve())
    assert sc.get("control_followup_cli") and tenant_id in sc["control_followup_cli"]
    assert "docs/getting-started.md#" in (sc.get("doc_anchors") or {}).get("emerging_role_golden_path", "")
    assert (export_dir / "files" / ".akc" / "knowledge" / "mediation.json").exists()
    assert (export_dir / "files" / ".akc" / "tests" / "demo.json").exists()
    assert res.zip_path is not None and res.zip_path.exists()

    with zipfile.ZipFile(res.zip_path, "r") as zf:
        names = set(zf.namelist())
    assert "data/plan.json" in names
    assert "data/manifest.json" in names
    assert "data/knowledge_obs.json" in names
    assert "data/operator_panels.json" in names
    assert "files/.akc/tests/demo.json" in names

    web_dir = tmp_path / "web"
    web = build_static_viewer(snapshot=snap, out_dir=web_dir)
    assert web.index_html.exists()
    index_html = web.index_html.read_text(encoding="utf-8")
    assert 'role="tablist"' in index_html
    assert "step_subheader" in index_html
    assert 'href="./static/viewer.css"' in index_html
    assert 'src="./static/viewer.js"' in index_html
    assert 'name="akc-viewer-ui-version"' in index_html
    assert VIEWER_UI_VERSION in index_html
    assert "__VIEWER_UI_VERSION__" not in index_html

    static_css = web_dir / "static" / "viewer.css"
    static_js = web_dir / "static" / "viewer.js"
    assert static_css.is_file()
    assert static_js.is_file()
    assert "prefers-color-scheme: light" in static_css.read_text(encoding="utf-8")
    viewer_js = static_js.read_text(encoding="utf-8")
    assert "JSON_TREE_CHUNK" in viewer_js
    assert 'params.get("step")' in viewer_js

    assert (web_dir / "data" / "plan.json").exists()
    assert (web_dir / "data" / "knowledge_obs.json").exists()
    assert (web_dir / "data" / "operator_panels.json").exists()
    assert (web_dir / "files" / ".akc" / "tests" / "demo.stdout.txt").exists()


def test_load_profile_decisions_panel_scope_context_without_manifest(tmp_path: Path) -> None:
    from akc.viewer.control_panels import load_profile_decisions_panel

    outputs_root = tmp_path / "out"
    scoped = outputs_root / "ta" / "ra"
    scoped.mkdir(parents=True)
    panel = load_profile_decisions_panel(
        manifest=None,
        scoped_outputs_dir=scoped,
        tenant_id="ta",
        repo_id="ra",
        outputs_root=outputs_root,
        plan_run_id="run-scope-test",
    )
    assert panel["available"] is False
    sc = panel["scope_context"]
    assert sc["run_id"] == "run-scope-test"
    assert sc["tenant_id"] == "ta"
    assert sc["repo_id"] == "ra"
    assert sc["outputs_root"] == str(outputs_root.resolve())
    assert "akc control runs show" in (sc.get("control_followup_cli") or "")
