from __future__ import annotations

import json
import threading
from pathlib import Path
from urllib.request import Request, urlopen

from akc.control.control_audit import control_audit_jsonl_path
from akc.control.fleet_config import load_fleet_config
from akc.control.fleet_http import serve_fleet_http
from akc.control.operations_index import OperationsIndex, operations_sqlite_path
from akc.delivery import store as delivery_store
from akc.delivery.event_types import DELIVERY_INVITE_SENT
from akc.delivery.metrics import compute_delivery_metrics


def _scoped_repo(tmp_path: Path) -> Path:
    scope = tmp_path / "t1" / "repo1"
    akc = scope / ".akc"
    akc.mkdir(parents=True)
    (akc / "project.json").write_text(
        json.dumps(
            {
                "tenant_id": "t1",
                "repo_id": "repo1",
                "outputs_root": str(tmp_path),
            }
        ),
        encoding="utf-8",
    )
    return scope


def test_delivery_session_indexed_in_operations_sqlite(tmp_path: Path) -> None:
    scope = _scoped_repo(tmp_path)
    summary = delivery_store.create_delivery_session(
        project_dir=scope,
        request_text="ship it",
        recipients=["alice@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    sqlite_p = operations_sqlite_path(outputs_root=tmp_path, tenant_id="t1")
    assert sqlite_p.is_file()
    idx = OperationsIndex(sqlite_path=sqlite_p)
    row = idx.get_delivery(tenant_id="t1", repo_id="repo1", delivery_id=did)
    assert row is not None
    assert row["tenant_id"] == "t1"
    assert row["repo_id"] == "repo1"
    assert row["delivery_id"] == did
    assert row["session_phase"] == "accepted"
    assert str(row["session_rel_path"]).endswith(f"{did}/session.json")
    metrics = row.get("metrics")
    assert isinstance(metrics, dict)


def test_delivery_metrics_request_to_invite_ms(tmp_path: Path) -> None:
    scope = _scoped_repo(tmp_path)
    summary = delivery_store.create_delivery_session(
        project_dir=scope,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
        tenant_id="t1",
        repo_id="repo1",
    )
    did = str(summary["delivery_id"])
    delivery_store.append_event(
        project_dir=scope,
        delivery_id=did,
        event_type=DELIVERY_INVITE_SENT,
        payload={"recipient_count": 1},
    )
    req = delivery_store.load_request(scope, did)
    sess = delivery_store.load_session(scope, did)
    ev = delivery_store.load_events(scope, did)
    m = compute_delivery_metrics(request=req, session=sess, events=ev)
    assert m["request_to_invite_sent_ms"] is not None
    assert m["request_to_invite_sent_ms"] >= 0.0


def test_amend_recipients_control_audit_when_scoped(tmp_path: Path) -> None:
    scope = _scoped_repo(tmp_path)
    summary = delivery_store.create_delivery_session(
        project_dir=scope,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
        tenant_id="t1",
        repo_id="repo1",
    )
    did = str(summary["delivery_id"])
    delivery_store.amend_delivery_recipients(
        project_dir=scope,
        delivery_id=did,
        additional_recipients=["b@example.com"],
    )
    audit_p = control_audit_jsonl_path(outputs_root=tmp_path, tenant_id="t1")
    assert audit_p.is_file()
    lines = audit_p.read_text(encoding="utf-8").strip().splitlines()
    assert any(json.loads(x).get("action") == "delivery.recipients.changed" for x in lines)


def test_fleet_http_v1_deliveries(tmp_path: Path) -> None:
    scope = _scoped_repo(tmp_path)
    summary = delivery_store.create_delivery_session(
        project_dir=scope,
        request_text="fleet",
        recipients=["z@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
        tenant_id="t1",
        repo_id="repo1",
    )
    did = str(summary["delivery_id"])

    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": True,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
            }
        ),
        encoding="utf-8",
    )
    cfg = load_fleet_config(cfg_path)
    httpd = serve_fleet_http(cfg, host="127.0.0.1", port=0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        host, port = httpd.server_address
        base = f"http://{host}:{port}"
        with urlopen(f"{base}/v1/deliveries?tenant_id=t1", timeout=3.0) as r:
            body = json.loads(r.read().decode("utf-8"))
        assert body["tenant_id"] == "t1"
        assert len(body["deliveries"]) >= 1
        assert any(d.get("delivery_id") == did for d in body["deliveries"])
        req = Request(f"{base}/v1/deliveries/t1/repo1/{did}", method="GET")
        with urlopen(req, timeout=3.0) as r2:
            one = json.loads(r2.read().decode("utf-8"))
        assert one["delivery"]["delivery_id"] == did
    finally:
        httpd.shutdown()
        httpd.server_close()
