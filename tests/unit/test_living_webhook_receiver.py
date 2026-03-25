from __future__ import annotations

import json
import urllib.error
import urllib.request
from pathlib import Path

import pytest

from akc.control.fleet_webhooks import sign_webhook_body
from akc.living.automation_profile import PROFILE_OFF, living_automation_profile_from_id
from akc.living.webhook_receiver import (
    LivingWebhookServerConfig,
    _verify_signature,
    process_fleet_webhook_payload,
    run_living_webhook_server_thread,
)


def test_verify_signature_accepts_fleet_style_header() -> None:
    body = b'{"a":1}'
    secret = "test-secret"
    sig = sign_webhook_body(secret=secret, body=body)
    assert _verify_signature(body=body, secret=secret, header_val=f"v1={sig}")


def test_verify_signature_rejects_wrong_secret() -> None:
    body = b'{"a":1}'
    sig = sign_webhook_body(secret="good", body=body)
    assert not _verify_signature(body=body, secret="bad", header_val=f"v1={sig}")


def test_process_fleet_webhook_runs_per_scope(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    calls: list[tuple[str, str, Path]] = []

    def _fake_execute(**kwargs: object) -> int:
        calls.append((str(kwargs["tenant_id"]), str(kwargs["repo_id"]), Path(kwargs["outputs_root"])))
        return 0

    monkeypatch.setattr("akc.living.webhook_receiver.living_recompile_execute", _fake_execute)

    out_root = tmp_path / "out"
    out_root.mkdir()
    ingest = tmp_path / "ingest.json"
    ingest.write_text("{}", encoding="utf-8")

    cfg = LivingWebhookServerConfig(
        bind_host="127.0.0.1",
        port=0,
        secret="s",
        ingest_state_path=ingest,
        tenant_allowlist=frozenset({"*"}),
        outputs_root_allowlist=frozenset({out_root}),
        living_automation_profile=living_automation_profile_from_id(PROFILE_OFF),
        opa_policy_path=None,
        opa_decision_path="data.akc.allow",
        llm_backend=None,
        eval_suite_path=tmp_path / "eval.json",
        goal="g",
        policy_mode="enforce",
        canary_mode="quick",
        accept_mode="thorough",
        canary_test_mode="smoke",
        allow_network=False,
        update_baseline_on_accept=True,
        skip_other_pending=True,
    )

    payload = {
        "schema": "akc.fleet.webhook_delivery.v1",
        "event": "recompile_triggers",
        "items": [
            {"tenant_id": "t1", "repo_id": "r1", "outputs_root": str(out_root)},
            {"tenant_id": "t1", "repo_id": "r1", "outputs_root": str(out_root)},
            {"tenant_id": "t2", "repo_id": "r2", "outputs_root": str(out_root)},
        ],
    }
    status, body = process_fleet_webhook_payload(payload, cfg=cfg)
    assert status == 200
    assert body.get("any_failure") is False
    assert len(calls) == 2
    assert ("t1", "r1", out_root.resolve()) in calls
    assert ("t2", "r2", out_root.resolve()) in calls


def test_process_fleet_webhook_skips_outputs_root_outside_allowlist(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _fake_execute(**kwargs: object) -> int:
        raise AssertionError("living_recompile_execute should not run for disallowed outputs_root")

    monkeypatch.setattr("akc.living.webhook_receiver.living_recompile_execute", _fake_execute)

    allowed = tmp_path / "allowed"
    allowed.mkdir()
    other = tmp_path / "other"
    other.mkdir()
    ingest = tmp_path / "ingest.json"
    ingest.write_text("{}", encoding="utf-8")

    cfg = LivingWebhookServerConfig(
        bind_host="127.0.0.1",
        port=0,
        secret="s",
        ingest_state_path=ingest,
        tenant_allowlist=frozenset({"*"}),
        outputs_root_allowlist=frozenset({allowed}),
        living_automation_profile=living_automation_profile_from_id(PROFILE_OFF),
        opa_policy_path=None,
        opa_decision_path="data.akc.allow",
        llm_backend=None,
        eval_suite_path=tmp_path / "eval.json",
        goal="g",
        policy_mode="enforce",
        canary_mode="quick",
        accept_mode="thorough",
        canary_test_mode="smoke",
        allow_network=False,
        update_baseline_on_accept=True,
        skip_other_pending=True,
    )

    payload = {
        "schema": "akc.fleet.webhook_delivery.v1",
        "event": "recompile_triggers",
        "items": [{"tenant_id": "t1", "repo_id": "r1", "outputs_root": str(other)}],
    }
    status, body = process_fleet_webhook_payload(payload, cfg=cfg)
    assert status == 200
    assert body.get("message") == "no_eligible_items"


def test_process_fleet_webhook_tenant_allowlist(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    def _fake_execute(**kwargs: object) -> int:
        return 0

    monkeypatch.setattr("akc.living.webhook_receiver.living_recompile_execute", _fake_execute)

    out_root = tmp_path / "out"
    out_root.mkdir()
    ingest = tmp_path / "ingest.json"
    ingest.write_text("{}", encoding="utf-8")

    cfg = LivingWebhookServerConfig(
        bind_host="127.0.0.1",
        port=0,
        secret="s",
        ingest_state_path=ingest,
        tenant_allowlist=frozenset({"t1"}),
        outputs_root_allowlist=frozenset({out_root}),
        living_automation_profile=living_automation_profile_from_id(PROFILE_OFF),
        opa_policy_path=None,
        opa_decision_path="data.akc.allow",
        llm_backend=None,
        eval_suite_path=tmp_path / "eval.json",
        goal="g",
        policy_mode="enforce",
        canary_mode="quick",
        accept_mode="thorough",
        canary_test_mode="smoke",
        allow_network=False,
        update_baseline_on_accept=True,
        skip_other_pending=True,
    )

    payload = {
        "schema": "akc.fleet.webhook_delivery.v1",
        "event": "living_drift",
        "items": [{"tenant_id": "t2", "repo_id": "r2", "outputs_root": str(out_root)}],
    }
    status, body = process_fleet_webhook_payload(payload, cfg=cfg)
    assert status == 200
    proc = body["processed"]
    assert len(proc) == 1
    assert proc[0].get("skipped") is True


def test_http_post_triggers_handler(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "akc.living.webhook_receiver.living_recompile_execute",
        lambda **_k: 0,
    )

    out_root = tmp_path / "out"
    out_root.mkdir()
    ingest = tmp_path / "ingest.json"
    ingest.write_text("{}", encoding="utf-8")

    cfg = LivingWebhookServerConfig(
        bind_host="127.0.0.1",
        port=0,
        secret="whsec",
        ingest_state_path=ingest,
        tenant_allowlist=frozenset({"*"}),
        outputs_root_allowlist=frozenset({out_root}),
        living_automation_profile=living_automation_profile_from_id(PROFILE_OFF),
        opa_policy_path=None,
        opa_decision_path="data.akc.allow",
        llm_backend=None,
        eval_suite_path=tmp_path / "eval.json",
        goal="g",
        policy_mode="enforce",
        canary_mode="quick",
        accept_mode="thorough",
        canary_test_mode="smoke",
        allow_network=False,
        update_baseline_on_accept=True,
        skip_other_pending=True,
    )

    _th, httpd = run_living_webhook_server_thread(cfg)
    try:
        host, port = httpd.server_address
        payload = {
            "schema": "akc.fleet.webhook_delivery.v1",
            "event": "recompile_triggers",
            "items": [{"tenant_id": "t1", "repo_id": "r1", "outputs_root": str(out_root)}],
        }
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        sig = sign_webhook_body(secret="whsec", body=body)
        req = urllib.request.Request(
            f"http://{host}:{port}/v1/trigger",
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "X-AKC-Signature": f"v1={sig}",
            },
        )
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            assert resp.status == 200
            data = json.loads(resp.read().decode("utf-8"))
        assert data.get("schema") == "akc.living.webhook_result.v1"
        assert data.get("any_failure") is False
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_http_rejects_bad_signature(tmp_path: Path) -> None:
    out_root = tmp_path / "out"
    out_root.mkdir()
    ingest = tmp_path / "ingest.json"
    ingest.write_text("{}", encoding="utf-8")

    cfg = LivingWebhookServerConfig(
        bind_host="127.0.0.1",
        port=0,
        secret="whsec",
        ingest_state_path=ingest,
        tenant_allowlist=frozenset({"*"}),
        outputs_root_allowlist=frozenset({out_root}),
        living_automation_profile=living_automation_profile_from_id(PROFILE_OFF),
        opa_policy_path=None,
        opa_decision_path="data.akc.allow",
        llm_backend=None,
        eval_suite_path=tmp_path / "eval.json",
        goal="g",
        policy_mode="enforce",
        canary_mode="quick",
        accept_mode="thorough",
        canary_test_mode="smoke",
        allow_network=False,
        update_baseline_on_accept=True,
        skip_other_pending=True,
    )

    _th, httpd = run_living_webhook_server_thread(cfg)
    try:
        host, port = httpd.server_address
        body = b"{}"
        req = urllib.request.Request(
            f"http://{host}:{port}/v1/trigger",
            data=body,
            method="POST",
            headers={"Content-Type": "application/json; charset=utf-8", "X-AKC-Signature": "v1=deadbeef"},
        )
        with pytest.raises(urllib.error.HTTPError) as ei:
            urllib.request.urlopen(req, timeout=5.0)
        assert ei.value.code == 401
    finally:
        httpd.shutdown()
        httpd.server_close()
