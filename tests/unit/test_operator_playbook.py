from __future__ import annotations

import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

from akc.control.fleet_config import load_fleet_config
from akc.control.fleet_webhooks import deliver_operator_playbook_completed_webhooks, post_signed_fleet_webhook
from akc.control.operator_playbook import run_operator_playbook, validate_operator_playbook_report
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _write_two_manifests(scope: Path) -> None:
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    m1 = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("1"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("a"),
        intent_semantic_fingerprint="a" * 16,
        knowledge_semantic_fingerprint="b" * 16,
        knowledge_provenance_fingerprint="c" * 16,
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("a"),
            "policy_decisions_ref": {
                "path": ".akc/run/r1.policy_decisions.json",
                "sha256": _hex64("p"),
            },
        },
    )
    m2 = RunManifest(
        run_id="r2",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64("2"),
        replay_mode="partial_replay",
        stable_intent_sha256=_hex64("b"),
        intent_semantic_fingerprint="d" * 16,
        knowledge_semantic_fingerprint="e" * 16,
        knowledge_provenance_fingerprint="f" * 16,
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("b"),
        },
    )
    (rd / "r1.manifest.json").write_text(json.dumps(m1.to_json_obj()), encoding="utf-8")
    (rd / "r2.manifest.json").write_text(json.dumps(m2.to_json_obj()), encoding="utf-8")
    pol = [{"action": "tool", "allowed": True, "reason": "ok", "source": "t", "mode": "m"}]
    (rd / "r1.policy_decisions.json").write_text(json.dumps(pol), encoding="utf-8")
    replay = {
        "run_id": "r2",
        "tenant_id": "t1",
        "repo_id": "repo1",
        "replay_mode": "partial_replay",
        "decisions": [
            {
                "pass_name": "generate",
                "replay_mode": "partial_replay",
                "should_call_model": False,
                "should_call_tools": False,
                "trigger_reason": "intent_semantic_changed",
                "inputs_snapshot": {"k": "v"},
            }
        ],
    }
    (rd / "r2.replay_decisions.json").write_text(json.dumps(replay), encoding="utf-8")


def test_run_operator_playbook_composes_and_validates(tmp_path: Path) -> None:
    scope = tmp_path / "t1" / "repo1"
    _write_two_manifests(scope)
    report, path = run_operator_playbook(
        outputs_root=tmp_path,
        tenant_id="t1",
        repo_id="repo1",
        run_id_a="r1",
        run_id_b="r2",
        focus="b",
        include_policy_explain=True,
        timestamp_utc="20990101T000000Z",
    )
    assert path.is_file()
    assert report["schema_kind"] == "akc_operator_playbook_report"
    assert report["manifest_diff"]["left"]["run_id"] == "r1"
    assert report["replay_forensics_summary"] is not None
    assert report["replay_forensics_summary"]["run_id"] == "r2"
    assert report["incident_bundle"] is not None
    assert Path(report["incident_bundle"]["out_dir"]).is_dir()
    assert report["policy_explain_summary"] is not None
    assert report["policy_explain_summary"]["policy_decision_count"] == 0
    assert report["replay_plan_artifact"] is not None
    assert report["replay_plan_artifact"]["effective_partial_replay_passes"] is not None
    assert report["policy_decision_refs"]["a"]["policy_decisions_ref"]["path"].endswith("r1.policy_decisions.json")
    issues = validate_operator_playbook_report(report)
    assert issues == []


def test_post_signed_fleet_webhook_operator_playbook(tmp_path: Path) -> None:
    received: list[bytes] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            n = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(n)
            received.append(body)
            self.send_response(200)
            self.end_headers()

        def log_message(self, _fmt: str, *_args: object) -> None:  # noqa: D102
            return

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    th = Thread(target=srv.serve_forever, daemon=True)
    th.start()
    host, port = srv.server_address
    url = f"http://{host}:{port}/pb"
    secret = "whsec-" + ("y" * 20)
    item = {"tenant_id": "t1", "report_relpath": ".akc/control/playbooks/x.json"}
    try:
        res = post_signed_fleet_webhook(
            url=url,
            secret=secret,
            webhook_id="w-pb",
            event="operator_playbook_completed",
            items=[item],
            dry_run=False,
        )
        assert res.http_status == 200
        assert len(received) == 1
        body = received[0]
        payload = json.loads(body.decode("utf-8"))
        assert payload["event"] == "operator_playbook_completed"
        assert payload["items"] == [item]
    finally:
        srv.shutdown()
        srv.server_close()


def test_deliver_operator_playbook_completed_from_fleet_config(tmp_path: Path) -> None:
    received: list[bytes] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            n = int(self.headers.get("Content-Length", "0"))
            received.append(self.rfile.read(n))
            self.send_response(200)
            self.end_headers()

        def log_message(self, _fmt: str, *_args: object) -> None:  # noqa: D102
            return

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    th = Thread(target=srv.serve_forever, daemon=True)
    th.start()
    host, port = srv.server_address
    url = f"http://{host}:{port}/fleet-pb"
    secret = "whsec-" + ("z" * 20)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "webhooks": [
                    {
                        "id": "wpb",
                        "url": url,
                        "secret": secret,
                        "events": ["operator_playbook_completed"],
                        "tenant_allowlist": ["*"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    cfg = load_fleet_config(cfg_path)
    item = {"tenant_id": "t1", "repo_id": "repo1"}
    try:
        out = deliver_operator_playbook_completed_webhooks(cfg, tenant_id="t1", item=item, dry_run=False)
        assert len(out) == 1
        assert out[0].http_status == 200
        payload = json.loads(received[0].decode("utf-8"))
        assert payload["items"] == [item]
    finally:
        srv.shutdown()
        srv.server_close()


def test_load_fleet_config_accepts_operator_playbook_event(tmp_path: Path) -> None:
    p = tmp_path / "fc.json"
    p.write_text(
        json.dumps(
            {
                "version": 1,
                "shards": [{"id": "s", "outputs_root": str(tmp_path)}],
                "webhooks": [
                    {
                        "id": "w",
                        "url": "https://example.invalid/hook",
                        "secret": "whsec-12345678901234567890",
                        "events": ["operator_playbook_completed"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    cfg = load_fleet_config(p)
    assert cfg.webhooks[0].events == ("operator_playbook_completed",)
