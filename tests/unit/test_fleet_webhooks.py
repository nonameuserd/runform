from __future__ import annotations

import hashlib
import hmac
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

from akc.control.fleet_config import FleetShardConfig, load_fleet_config
from akc.control.fleet_webhooks import deliver_fleet_webhooks, list_living_json_touchpoints, sign_webhook_body
from akc.control.operations_index import OperationsIndex
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def test_sign_webhook_body_roundtrip() -> None:
    body = b'{"a":1}'
    sig = sign_webhook_body(secret="s3cret-s3cret-s3cret", body=body)
    expect = hmac.new(b"s3cret-s3cret-s3cret", body, hashlib.sha256).hexdigest()
    assert sig == expect


def test_list_living_json_touchpoints(tmp_path: Path) -> None:
    living = tmp_path / "t1" / "repo1" / ".akc" / "living"
    living.mkdir(parents=True, exist_ok=True)
    (living / "drift.json").write_text("{}", encoding="utf-8")
    shard = FleetShardConfig(id="s", outputs_root=tmp_path, tenant_allowlist=("*",))
    rows = list_living_json_touchpoints(shard=shard, tenant_id="t1")
    assert len(rows) == 1
    assert rows[0]["rel_path"].replace("\\", "/") == ".akc/living/drift.json"


def test_deliver_webhook_posts_and_advances_watermark(tmp_path: Path) -> None:
    trig_path = ".akc/run/r1.recompile_triggers.json"
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
        control_plane={
            "schema_version": 1,
            "schema_id": "akc:control_plane_envelope:v1",
            "stable_intent_sha256": _hex64("b"),
            "recompile_triggers_ref": {"path": trig_path, "sha256": _hex64("c")},
        },
    )
    scope = tmp_path / "t1" / "repo1"
    rd = scope / ".akc" / "run"
    rd.mkdir(parents=True, exist_ok=True)
    mp = rd / "r1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    triggers_obj = {"triggers": [{"kind": "x"}], "tenant_id": "t1", "repo_id": "repo1", "checked_at_ms": 1}
    (rd / "r1.recompile_triggers.json").write_text(json.dumps(triggers_obj), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=tmp_path)

    received: list[bytes] = []
    captured_sigs: list[str] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            n = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(n)
            received.append(body)
            sig = self.headers.get("X-AKC-Signature")
            if sig is None:
                for hk, hv in self.headers.items():
                    if hk.lower() == "x-akc-signature":
                        sig = hv
                        break
            captured_sigs.append(str(sig or ""))
            self.send_response(200)
            self.end_headers()

        def log_message(self, _fmt: str, *_args: object) -> None:  # noqa: D102
            return

    srv = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    th = Thread(target=srv.serve_forever, daemon=True)
    th.start()
    host, port = srv.server_address
    url = f"http://{host}:{port}/hook"
    secret = "whsec-" + ("x" * 20)
    state_path = tmp_path / "wh_state.json"
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "webhooks": [
                    {
                        "id": "w1",
                        "url": url,
                        "secret": secret,
                        "events": ["recompile_triggers"],
                        "tenant_allowlist": ["*"],
                        "page_size": 10,
                    }
                ],
                "webhook_state_path": str(state_path),
            }
        ),
        encoding="utf-8",
    )
    cfg = load_fleet_config(cfg_path)
    try:
        out = deliver_fleet_webhooks(cfg, tenants=["t1"], dry_run=False)
        assert len(out) == 1
        assert out[0].item_count == 1
        assert out[0].http_status == 200
        assert len(received) == 1
        payload = json.loads(received[0].decode("utf-8"))
        assert payload["event"] == "recompile_triggers"
        assert len(payload["items"]) == 1
        body = received[0]
        sig_line = captured_sigs[0]
        assert sig_line.startswith("v1=")
        assert sig_line[3:] == sign_webhook_body(secret=secret, body=body)
        again = deliver_fleet_webhooks(cfg, tenants=["t1"], dry_run=False)
        assert again[0].item_count == 0
    finally:
        srv.shutdown()
        srv.server_close()
