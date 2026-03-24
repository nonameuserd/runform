from __future__ import annotations

import importlib.resources
import json
import threading
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from akc.control.control_audit import control_audit_jsonl_path
from akc.control.fleet_config import load_fleet_config
from akc.control.fleet_http import serve_fleet_http
from akc.control.operations_index import OperationsIndex
from akc.run.manifest import RunManifest


def _hex64(c: str = "a") -> str:
    return (c * 64)[:64]


def _seed_index(root: Path) -> None:
    m = RunManifest(
        run_id="r1",
        tenant_id="t1",
        repo_id="repo1",
        ir_sha256=_hex64(),
        replay_mode="live",
    )
    scope = root / "t1" / "repo1" / ".akc" / "run"
    scope.mkdir(parents=True, exist_ok=True)
    mp = scope / "r1.manifest.json"
    mp.write_text(json.dumps(m.to_json_obj()), encoding="utf-8")
    OperationsIndex.upsert_from_manifest_path(mp, outputs_root=root)


def _write_fleet_config(path: Path, *, root: Path, token: str | None, anonymous: bool) -> None:
    cfg: dict = {
        "version": 1,
        "allow_anonymous_read": anonymous,
        "shards": [{"id": "s0", "outputs_root": str(root)}],
    }
    if token is not None:
        cfg["api_tokens"] = [{"id": "tok1", "token": token, "role": "viewer", "tenant_allowlist": ["*"]}]
    path.write_text(json.dumps(cfg), encoding="utf-8")


def _run_etag(*, base: str, token: str, tenant_id: str = "t1", repo_id: str = "repo1", run_id: str = "r1") -> str:
    req = Request(
        f"{base}/v1/runs/{tenant_id}/{repo_id}/{run_id}",
        headers={"Authorization": f"Bearer {token}"},
        method="GET",
    )
    with urlopen(req, timeout=3.0) as r:
        return str(r.headers.get("ETag") or "")


def test_operator_dashboard_assets_present() -> None:
    root = importlib.resources.files("akc.control.operator_dashboard")
    assert root.joinpath("index.html").is_file()
    assert root.joinpath("app.js").is_file()


def test_cors_headers_on_get_when_env_set(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AKC_FLEET_CORS_ALLOW_ORIGIN", "http://dashboard.example:9090")
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    _write_fleet_config(cfg_path, root=tmp_path, token=None, anonymous=True)
    cfg = load_fleet_config(cfg_path)
    httpd = serve_fleet_http(cfg, host="127.0.0.1", port=0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        host, port = httpd.server_address
        base = f"http://{host}:{port}"
        req = Request(f"{base}/health", method="GET")
        with urlopen(req, timeout=3.0) as r:
            assert r.headers.get("Access-Control-Allow-Origin") == "http://dashboard.example:9090"
            assert "GET" in (r.headers.get("Access-Control-Allow-Methods") or "")
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_options_preflight_for_runs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AKC_FLEET_CORS_ALLOW_ORIGIN", "http://dashboard.example:9090")
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    _write_fleet_config(cfg_path, root=tmp_path, token=None, anonymous=True)
    cfg = load_fleet_config(cfg_path)
    httpd = serve_fleet_http(cfg, host="127.0.0.1", port=0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        host, port = httpd.server_address
        base = f"http://{host}:{port}"
        req = Request(f"{base}/v1/runs", method="OPTIONS")
        with urlopen(req, timeout=3.0) as r:
            assert r.status == 204
            assert r.headers.get("Access-Control-Allow-Origin") == "http://dashboard.example:9090"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_options_unknown_path_is_404(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AKC_FLEET_CORS_ALLOW_ORIGIN", "http://dashboard.example:9090")
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    _write_fleet_config(cfg_path, root=tmp_path, token=None, anonymous=True)
    cfg = load_fleet_config(cfg_path)
    httpd = serve_fleet_http(cfg, host="127.0.0.1", port=0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        host, port = httpd.server_address
        base = f"http://{host}:{port}"
        req = Request(f"{base}/not-a-route", method="OPTIONS")
        with pytest.raises(HTTPError) as ei:
            urlopen(req, timeout=3.0)
        assert ei.value.code == 404
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_health_and_runs_anonymous(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    _write_fleet_config(cfg_path, root=tmp_path, token=None, anonymous=True)
    cfg = load_fleet_config(cfg_path)
    httpd = serve_fleet_http(cfg, host="127.0.0.1", port=0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        host, port = httpd.server_address
        base = f"http://{host}:{port}"
        with urlopen(f"{base}/health", timeout=3.0) as r:
            h = json.loads(r.read().decode("utf-8"))
        assert h["status"] == "ok"
        with urlopen(f"{base}/v1/runs?tenant_id=t1", timeout=3.0) as r2:
            body = json.loads(r2.read().decode("utf-8"))
        assert len(body["runs"]) == 1
        assert body["runs"][0]["run_id"] == "r1"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_runs_require_bearer_when_configured(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    _write_fleet_config(cfg_path, root=tmp_path, token="test-token-9chars-min", anonymous=False)
    cfg = load_fleet_config(cfg_path)
    httpd = serve_fleet_http(cfg, host="127.0.0.1", port=0)
    th = threading.Thread(target=httpd.serve_forever, daemon=True)
    th.start()
    try:
        host, port = httpd.server_address
        base = f"http://{host}:{port}"
        with pytest.raises(HTTPError) as ei:
            urlopen(f"{base}/v1/runs?tenant_id=t1", timeout=3.0)
        assert ei.value.code == 401
        req = Request(
            f"{base}/v1/runs?tenant_id=t1",
            headers={"Authorization": "Bearer test-token-9chars-min"},
            method="GET",
        )
        with urlopen(req, timeout=3.0) as r:
            body = json.loads(r.read().decode("utf-8"))
        assert body["runs"][0]["run_id"] == "r1"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_tenant_forbidden_for_token(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [{"token": "x" * 16, "role": "viewer", "tenant_allowlist": ["other"]}],
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
        req = Request(
            f"http://{host}:{port}/v1/runs?tenant_id=t1",
            headers={"Authorization": f"Bearer {'x' * 16}"},
            method="GET",
        )
        with pytest.raises(HTTPError) as ei:
            urlopen(req, timeout=3.0)
        assert ei.value.code == 403
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_post_run_labels_operator_and_audit(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [
                    {
                        "id": "op1",
                        "token": "op-token-9chars-minxx",
                        "role": "operator",
                        "tenant_allowlist": ["*"],
                    }
                ],
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
        etag = _run_etag(base=base, token="op-token-9chars-minxx")
        body = json.dumps({"key": "env", "value": "prod"}).encode("utf-8")
        req = Request(
            f"{base}/v1/runs/t1/repo1/r1/labels",
            data=body,
            headers={
                "Authorization": "Bearer op-token-9chars-minxx",
                "Content-Type": "application/json",
                "X-Request-ID": "req-test-1",
                "If-Match": etag,
            },
            method="POST",
        )
        with urlopen(req, timeout=3.0) as r:
            resp = json.loads(r.read().decode("utf-8"))
        assert resp["label_key"] == "env"
        assert resp["label_value"] == "prod"
        assert resp["request_id"] == "req-test-1"
        assert r.headers.get("ETag")
        idx = OperationsIndex(sqlite_path=tmp_path / "t1" / ".akc" / "control" / "operations.sqlite")
        row = idx.get_run(tenant_id="t1", repo_id="repo1", run_id="r1")
        assert row is not None
        assert row["labels"] == {"env": "prod"}
        audit_p = control_audit_jsonl_path(outputs_root=tmp_path, tenant_id="t1")
        lines = audit_p.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        rec = json.loads(lines[0])
        assert rec["action"] == "runs.label.set"
        assert rec["actor"] == "op1"
        assert rec["request_id"] == "req-test-1"
        assert rec["details"]["before"] == {"label_value": None}
        assert rec["details"]["after"] == {"label_value": "prod"}
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_post_run_labels_viewer_forbidden(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [
                    {
                        "id": "v1",
                        "token": "view-token-9chars-minx",
                        "role": "viewer",
                        "tenant_allowlist": ["*"],
                    }
                ],
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
        body = json.dumps({"key": "env", "value": "prod"}).encode("utf-8")
        req = Request(
            f"{base}/v1/runs/t1/repo1/r1/labels",
            data=body,
            headers={"Authorization": "Bearer view-token-9chars-minx", "Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as ei:
            urlopen(req, timeout=3.0)
        assert ei.value.code == 403
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_label_only_scope_can_write_not_read(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [
                    {
                        "id": "lab",
                        "token": "lab-token-9chars-minxx",
                        "role": "viewer",
                        "tenant_allowlist": ["*"],
                        "scopes": ["runs:label"],
                    }
                ],
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
        get_req = Request(
            f"{base}/v1/runs?tenant_id=t1",
            headers={"Authorization": "Bearer lab-token-9chars-minxx"},
            method="GET",
        )
        with pytest.raises(HTTPError) as ei:
            urlopen(get_req, timeout=3.0)
        assert ei.value.code == 403
        body = json.dumps({"label_key": "k", "label_value": "v"}).encode("utf-8")
        post_req = Request(
            f"{base}/v1/runs/t1/repo1/r1/labels",
            data=body,
            headers={
                "Authorization": "Bearer lab-token-9chars-minxx",
                "Content-Type": "application/json",
                "If-Match": "*",
            },
            method="POST",
        )
        with urlopen(post_req, timeout=3.0) as r:
            assert r.status == 200
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_post_labels_enforces_tenant_allowlist_even_when_anonymous_read(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": True,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [
                    {
                        "id": "op2",
                        "token": "op2-token-9chars-minx",
                        "role": "operator",
                        "tenant_allowlist": ["other"],
                    }
                ],
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
        body = json.dumps({"key": "x", "value": "y"}).encode("utf-8")
        req = Request(
            f"{base}/v1/runs/t1/repo1/r1/labels",
            data=body,
            headers={
                "Authorization": "Bearer op2-token-9chars-minx",
                "Content-Type": "application/json",
                "If-Match": '"ignored"',
            },
            method="POST",
        )
        with pytest.raises(HTTPError) as ei:
            urlopen(req, timeout=3.0)
        assert ei.value.code == 403
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_post_labels_requires_if_match(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [{"id": "op3", "token": "op3-token-9chars-minx", "role": "operator"}],
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
        body = json.dumps({"key": "env", "value": "prod"}).encode("utf-8")
        req = Request(
            f"{base}/v1/runs/t1/repo1/r1/labels",
            data=body,
            headers={"Authorization": "Bearer op3-token-9chars-minx", "Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as ei:
            urlopen(req, timeout=3.0)
        assert ei.value.code == 428
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_post_labels_stale_if_match_returns_412(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [{"id": "op4", "token": "op4-token-9chars-minx", "role": "operator"}],
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
        body = json.dumps({"key": "env", "value": "prod"}).encode("utf-8")
        req = Request(
            f"{base}/v1/runs/t1/repo1/r1/labels",
            data=body,
            headers={
                "Authorization": "Bearer op4-token-9chars-minx",
                "Content-Type": "application/json",
                "If-Match": '"stale-etag"',
            },
            method="POST",
        )
        with pytest.raises(HTTPError) as ei:
            urlopen(req, timeout=3.0)
        assert ei.value.code == 412
        payload = json.loads(ei.value.read().decode("utf-8"))
        assert payload["error"] == "precondition_failed"
        assert payload["resource"] == "run_labels"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_post_labels_idempotency_replay_avoids_duplicate_audit(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [{"id": "op5", "token": "op5-token-9chars-minx", "role": "operator"}],
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
        etag = _run_etag(base=base, token="op5-token-9chars-minx")
        body = json.dumps({"key": "env", "value": "prod"}).encode("utf-8")
        headers = {
            "Authorization": "Bearer op5-token-9chars-minx",
            "Content-Type": "application/json",
            "If-Match": etag,
            "Idempotency-Key": "idem-1",
        }
        req1 = Request(f"{base}/v1/runs/t1/repo1/r1/labels", data=body, headers=headers, method="POST")
        with urlopen(req1, timeout=3.0) as r1:
            assert r1.status == 200
            assert r1.headers.get("Idempotent-Replay") is None
        req2 = Request(f"{base}/v1/runs/t1/repo1/r1/labels", data=body, headers=headers, method="POST")
        with urlopen(req2, timeout=3.0) as r2:
            assert r2.status == 200
            assert r2.headers.get("Idempotent-Replay") == "true"

        audit_p = control_audit_jsonl_path(outputs_root=tmp_path, tenant_id="t1")
        lines = audit_p.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_post_labels_rejects_non_json_content_type(tmp_path: Path) -> None:
    _seed_index(tmp_path)
    cfg_path = tmp_path / "fleet.json"
    cfg_path.write_text(
        json.dumps(
            {
                "version": 1,
                "allow_anonymous_read": False,
                "shards": [{"id": "s0", "outputs_root": str(tmp_path)}],
                "api_tokens": [{"id": "op6", "token": "op6-token-9chars-minx", "role": "operator"}],
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
        etag = _run_etag(base=base, token="op6-token-9chars-minx")
        req = Request(
            f"{base}/v1/runs/t1/repo1/r1/labels",
            data=b"key=env&value=prod",
            headers={
                "Authorization": "Bearer op6-token-9chars-minx",
                "Content-Type": "application/x-www-form-urlencoded",
                "If-Match": etag,
            },
            method="POST",
        )
        with pytest.raises(HTTPError) as ei:
            urlopen(req, timeout=3.0)
        assert ei.value.code == 415
    finally:
        httpd.shutdown()
        httpd.server_close()
