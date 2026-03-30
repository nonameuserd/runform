from __future__ import annotations

from pathlib import Path

from akc.viewer import ViewerInputs, load_viewer_snapshot


def test_golden_viewer_sqlite_demo_fixture_loads() -> None:
    """Regression: sqlite-only golden viewer demo must load without .akc/plan."""

    repo_root = Path(__file__).resolve().parents[2]
    demo_root = repo_root / "examples" / "golden-viewer-sqlite-demo"
    snap = load_viewer_snapshot(
        ViewerInputs(
            tenant_id="demo",
            repo_id="sqlite-repo",
            outputs_root=demo_root / "out",
            plan_base_dir=demo_root,  # no .akc/plan expected; should fall back to sqlite
            schema_version=1,
        )
    )
    assert snap.plan.tenant_id == "demo"
    assert snap.manifest is not None
    assert snap.evidence.all
