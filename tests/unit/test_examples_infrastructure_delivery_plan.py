from __future__ import annotations

import json
from pathlib import Path

from akc.artifacts.validate import validate_obj


def test_fullstack_delivery_plan_example_validates() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    path = repo_root / "examples" / "infrastructure" / "delivery_plan.fullstack.example.json"
    obj = json.loads(path.read_text(encoding="utf-8"))
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []
    classes = {str(t.get("target_class")) for t in obj.get("targets", []) if isinstance(t, dict)}
    assert classes == {"backend_service", "infrastructure_component", "mobile_client", "web_app"}
