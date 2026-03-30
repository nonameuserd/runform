from __future__ import annotations

from pathlib import Path

from akc.viewer import ViewerInputs, load_viewer_snapshot


def test_golden_viewer_demo_fixture_loads() -> None:
    """Regression: checked-in examples/golden-viewer-demo must load in the viewer."""

    repo_root = Path(__file__).resolve().parents[2]
    demo_root = repo_root / "examples" / "golden-viewer-demo"
    snap = load_viewer_snapshot(
        ViewerInputs(
            tenant_id="demo",
            repo_id="golden-repo",
            outputs_root=demo_root / "out",
            plan_base_dir=demo_root,
            schema_version=1,
        )
    )
    assert snap.plan.tenant_id == "demo"
    assert snap.plan.goal
    assert snap.manifest is not None
    assert len(snap.evidence.all) >= 1
