from __future__ import annotations

import json
from pathlib import Path

from akc.delivery import ingest


def test_load_recipients_from_file_json_and_lines(tmp_path: Path) -> None:
    j = tmp_path / "r.json"
    j.write_text(json.dumps({"recipients": ["  A@Example.COM ", "b@example.com"]}), encoding="utf-8")
    assert ingest.load_recipients_from_file(j) == ["A@Example.COM", "b@example.com"]

    txt = tmp_path / "r.txt"
    txt.write_text("x@example.com\n# ignored\ny@example.com\n", encoding="utf-8")
    assert ingest.load_recipients_from_file(txt) == ["x@example.com", "y@example.com"]


def test_extract_app_goal_strips_send_tail() -> None:
    assert ingest.extract_app_goal("build a dog walking app and send it to these 3 users") == "build a dog walking app"


def test_build_parsed_warns_when_text_mentions_platforms_not_on_cli() -> None:
    p = ingest.build_parsed_delivery_fields(
        request_text="ship ios app to testers",
        cli_platforms=["web"],
        release_mode="beta",
        authoritative_recipients=["a@example.com"],
    )
    assert p["requested_platforms"] == ["web"]
    assert "ios" in p["request_mentions_platforms"]
    assert any("authoritative" in w for w in p["warnings"])


def test_collect_prerequisite_human_inputs_web_only_beta_emits_hosting(tmp_path: Path) -> None:
    rows = ingest.collect_prerequisite_human_inputs(
        project_dir=tmp_path,
        platforms=["web"],
        release_mode="beta",
    )
    ids = [r["id"] for r in rows]
    assert "web_hosting_endpoint" in ids


def test_infer_required_accounts_from_human_inputs_maps_ids() -> None:
    rows = [
        {"id": "ios_bundle_id"},
        {"id": "firebase_android_app_registration"},
        {"id": "web_hosting_endpoint"},
    ]
    assert ingest.infer_required_accounts_from_human_inputs(rows) == [
        "apple_developer",
        "firebase",
        "web_hosting",
    ]


def test_operator_prereqs_satisfy_web_hosting(tmp_path: Path) -> None:
    manifest = tmp_path / ".akc" / "delivery" / "operator_prereqs.json"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(json.dumps({"web": {"hosting_endpoint": "https://example.com"}}), encoding="utf-8")
    rows = ingest.collect_prerequisite_human_inputs(
        project_dir=tmp_path,
        platforms=["web"],
        release_mode="beta",
    )
    assert "web_hosting_endpoint" not in {r["id"] for r in rows}
