from __future__ import annotations

import json
from pathlib import Path

from akc.artifacts.validate import validate_obj
from akc.viewer import ViewerInputs, load_viewer_snapshot


def test_golden_delivery_demo_fixture_loads_in_viewer() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    demo_root = repo_root / "examples" / "golden-delivery-demo"

    snap = load_viewer_snapshot(
        ViewerInputs(
            tenant_id="demo",
            repo_id="delivery-repo",
            outputs_root=demo_root / "out",
            plan_base_dir=demo_root,
            schema_version=1,
        )
    )
    assert snap.manifest is not None
    assert any("delivery" in ref.relpath for ref in snap.evidence.all)


def test_golden_delivery_demo_sidecars_validate() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    base = (
        repo_root
        / "examples"
        / "golden-delivery-demo"
        / "out"
        / "demo"
        / "delivery-repo"
        / ".akc"
        / "delivery"
        / "deliv-demo-1"
    )
    assert (
        validate_obj(
            obj=json.loads((base / "request.json").read_text(encoding="utf-8")),
            kind="delivery_request",
            version=1,
        )
        == []
    )
    assert (
        validate_obj(
            obj=json.loads((base / "session.json").read_text(encoding="utf-8")),
            kind="delivery_session",
            version=1,
        )
        == []
    )
    assert (
        validate_obj(
            obj=json.loads((base / "recipients.json").read_text(encoding="utf-8")),
            kind="delivery_recipients",
            version=1,
        )
        == []
    )
    assert (
        validate_obj(
            obj=json.loads((base / "events.json").read_text(encoding="utf-8")),
            kind="delivery_events",
            version=1,
        )
        == []
    )
    assert (
        validate_obj(
            obj=json.loads((base / "provider_state.json").read_text(encoding="utf-8")),
            kind="delivery_provider_state",
            version=1,
        )
        == []
    )
    assert (
        validate_obj(
            obj=json.loads((base / "activation_evidence.json").read_text(encoding="utf-8")),
            kind="delivery_activation_evidence",
            version=1,
        )
        == []
    )
