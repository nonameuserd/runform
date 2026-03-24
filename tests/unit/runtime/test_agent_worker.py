from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from unittest.mock import patch

from akc.runtime.coordination.worker import (
    AgentWorkerTurnResult,
    CoordinationHttpWorkerPolicy,
    DeterministicAgentWorker,
    HttpAgentWorker,
    HttpAgentWorkerConfig,
    RoleWorkerContext,
    TimeoutEnforcingAgentWorker,
    agent_worker_inner_from_env,
    build_role_worker_context,
    coordination_http_worker_policy_from_bundle,
    coordination_step_runtime_result,
    intent_worker_bounds_from_bundle,
    redact_for_logs,
    resolve_coordination_http_post_url,
)
from akc.runtime.models import RuntimeAction, RuntimeBundle, RuntimeBundleRef, RuntimeContext, RuntimeNodeRef


def test_redact_for_logs_strips_secrets_and_long_hex() -> None:
    s = "Authorization: Bearer secret123 token abcdef0123456789abcdef0123456789"
    out = redact_for_logs(s)
    assert "secret123" not in out
    assert "REDACTED" in out


def test_intent_worker_bounds_from_bundle() -> None:
    ctx = RuntimeContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        policy_mode="enforce",
        adapter_id="native",
    )
    ref = RuntimeBundleRef(
        bundle_path="/x/bundle.json",
        manifest_hash="a" * 64,
        created_at=1,
        source_compile_run_id="run",
    )
    bundle = RuntimeBundle(
        context=ctx,
        ref=ref,
        nodes=(),
        contract_ids=("c",),
        metadata={
            "intent_policy_projection": {
                "operating_bounds_effective": {
                    "max_seconds": 12.5,
                    "max_input_tokens": 100,
                    "max_output_tokens": 200,
                }
            }
        },
    )
    t, mi, mo = intent_worker_bounds_from_bundle(bundle=bundle)
    assert t == 12.5
    assert mi == 100
    assert mo == 200


def test_deterministic_worker_is_stable() -> None:
    w = DeterministicAgentWorker()
    ctx = RoleWorkerContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        coordination_step_id="s1",
        coordination_role_id="writer",
        coordination_spec_sha256="a" * 64,
        action_id="a1",
        idempotency_key="idem-1",
        inputs_fingerprint="fp",
        timeout_s=30.0,
        max_input_tokens=None,
        max_output_tokens=None,
        policy_context_summary={},
    )
    a = w.execute_role_turn(context=ctx)
    b = w.execute_role_turn(context=ctx)
    assert a.output_text_sha256 == b.output_text_sha256
    assert a.status == "succeeded"


def test_timeout_enforcing_worker_times_out() -> None:
    class Slow:
        def execute_role_turn(self, *, context: RoleWorkerContext) -> AgentWorkerTurnResult:
            _ = context
            time.sleep(2.0)
            return AgentWorkerTurnResult(
                status="succeeded",
                output_text_sha256="x",
                output_text_len=1,
                duration_ms=0,
            )

    ctx = RoleWorkerContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        coordination_step_id="s1",
        coordination_role_id="writer",
        coordination_spec_sha256="a" * 64,
        action_id="a1",
        idempotency_key="idem-1",
        inputs_fingerprint="fp",
        timeout_s=0.2,
        max_input_tokens=None,
        max_output_tokens=None,
        policy_context_summary={},
    )
    tw = TimeoutEnforcingAgentWorker(inner=Slow())
    out = tw.execute_role_turn(context=ctx)
    assert out.status == "timeout"
    assert out.error == "agent_worker_deadline_exceeded"


def test_http_agent_worker_round_trip() -> None:
    received: list[bytes] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            ln = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(ln)
            received.append(body)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(
                json.dumps(
                    {"text": "ok", "usage": {"input_tokens": 1, "output_tokens": 2}},
                    separators=(",", ":"),
                ).encode("utf-8")
            )

        def log_message(self, _format: str, *_args: object) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address
        url = f"http://{host}:{port}/v1"
        w = HttpAgentWorker(
            cfg=HttpAgentWorkerConfig(url=url, api_key=None, client_timeout_cap_s=5.0),
            policy=CoordinationHttpWorkerPolicy.for_integration_tests(bundle_host_patterns=("127.0.0.1",)),
        )
        ctx = RoleWorkerContext(
            tenant_id="t",
            repo_id="r",
            run_id="run",
            runtime_run_id="rt",
            coordination_step_id="s1",
            coordination_role_id="writer",
            coordination_spec_sha256="a" * 64,
            action_id="a1",
            idempotency_key="idem-1",
            inputs_fingerprint="fp",
            timeout_s=5.0,
            max_input_tokens=10,
            max_output_tokens=20,
            policy_context_summary={},
        )
        out = w.execute_role_turn(context=ctx)
        assert out.status == "succeeded"
        assert out.usage_input_tokens == 1
        assert out.usage_output_tokens == 2
        assert received
        payload = json.loads(received[0].decode("utf-8"))
        assert payload["tenant_id"] == "t"
        assert payload["max_output_tokens"] == 20
    finally:
        server.shutdown()
        server.server_close()


def test_agent_worker_inner_from_env_http_flag() -> None:
    with patch.dict(
        "os.environ",
        {"AKC_AGENT_WORKER_HTTP": "1", "AKC_HTTP_AGENT_WORKER_URL": "http://example.invalid/x"},
        clear=False,
    ):
        inner = agent_worker_inner_from_env()
        assert isinstance(inner, HttpAgentWorker)


def test_build_role_worker_and_runtime_result() -> None:
    ctx = RuntimeContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        policy_mode="enforce",
        adapter_id="native",
    )
    action = RuntimeAction(
        action_id="coordination:x:0",
        action_type="coordination.step",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="ifp",
        idempotency_key="idem",
        policy_context={
            "coordination_step_id": "workflow_000",
            "coordination_role_id": "writer",
            "coordination_spec_sha256": "b" * 64,
            "run_stage": "coordination.step",
        },
    )
    rwc = build_role_worker_context(context=ctx, action=action, bundle=None)
    assert rwc.coordination_step_id == "workflow_000"
    res = coordination_step_runtime_result(
        adapter_id="native",
        action=action,
        turn=AgentWorkerTurnResult(
            status="succeeded",
            output_text_sha256="h" * 64,
            output_text_len=3,
            duration_ms=1,
            usage_input_tokens=0,
            usage_output_tokens=0,
        ),
    )
    assert res.status == "succeeded"
    assert res.outputs.get("agent_worker_output_sha256") == "h" * 64
    assert "action_id" in res.outputs


def test_http_worker_rejects_usage_over_budget() -> None:
    policy = CoordinationHttpWorkerPolicy(
        bundle_patterns=("127.0.0.1",),
        envelope_patterns=(),
        max_body_bytes=1_048_576,
        max_response_bytes=262_144,
        allow_ambient_proxy_env=False,
        http_execution_enabled=True,
        post_method_allowed=True,
    )
    w = HttpAgentWorker(
        cfg=HttpAgentWorkerConfig(
            url="http://127.0.0.1:9/usage",
            api_key=None,
            client_timeout_cap_s=5.0,
        ),
        policy=policy,
    )
    raw = json.dumps(
        {"text": "x", "usage": {"input_tokens": 1, "output_tokens": 999}},
        separators=(",", ":"),
    ).encode("utf-8")
    ctx = RoleWorkerContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        coordination_step_id="s1",
        coordination_role_id="writer",
        coordination_spec_sha256="a" * 64,
        action_id="a1",
        idempotency_key="idem-1",
        inputs_fingerprint="fp",
        timeout_s=5.0,
        max_input_tokens=None,
        max_output_tokens=10,
        policy_context_summary={},
    )
    with patch(
        "akc.runtime.coordination.worker._coordination_worker_http_post",
        return_value=(raw, None),
    ):
        out = w.execute_role_turn(context=ctx)
    assert out.status == "failed"
    assert out.error == "usage_output_tokens_exceeds_budget"


def _sample_role_worker_context() -> RoleWorkerContext:
    return RoleWorkerContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        coordination_step_id="s1",
        coordination_role_id="writer",
        coordination_spec_sha256="a" * 64,
        action_id="a1",
        idempotency_key="idem-1",
        inputs_fingerprint="fp",
        timeout_s=5.0,
        max_input_tokens=None,
        max_output_tokens=None,
        policy_context_summary={},
    )


def test_http_agent_worker_denies_url_before_http_call() -> None:
    policy = CoordinationHttpWorkerPolicy(
        bundle_patterns=("only.other.example",),
        envelope_patterns=(),
        max_body_bytes=1_048_576,
        max_response_bytes=262_144,
        allow_ambient_proxy_env=False,
        http_execution_enabled=True,
        post_method_allowed=True,
    )
    w = HttpAgentWorker(
        cfg=HttpAgentWorkerConfig(url="http://127.0.0.1:9/nope", api_key=None, client_timeout_cap_s=5.0),
        policy=policy,
    )
    with patch("akc.runtime.coordination.worker._coordination_worker_http_post") as post:
        out = w.execute_role_turn(context=_sample_role_worker_context())
        post.assert_not_called()
    assert out.status == "failed"
    assert out.error == "coordination_http_worker_url_not_allowlisted"


def test_http_agent_worker_allows_url_with_mocked_http() -> None:
    policy = CoordinationHttpWorkerPolicy(
        bundle_patterns=("127.0.0.1",),
        envelope_patterns=(),
        max_body_bytes=1_048_576,
        max_response_bytes=262_144,
        allow_ambient_proxy_env=False,
        http_execution_enabled=True,
        post_method_allowed=True,
    )
    w = HttpAgentWorker(
        cfg=HttpAgentWorkerConfig(url="http://127.0.0.1:9/ok", api_key=None, client_timeout_cap_s=5.0),
        policy=policy,
    )
    raw = json.dumps(
        {"text": "hello", "usage": {"input_tokens": 0, "output_tokens": 1}},
        separators=(",", ":"),
    ).encode("utf-8")
    with patch(
        "akc.runtime.coordination.worker._coordination_worker_http_post",
        return_value=(raw, None),
    ) as post:
        out = w.execute_role_turn(context=_sample_role_worker_context())
        assert post.call_count == 1
    assert out.status == "succeeded"
    assert out.output_text_len == 5


def test_coordination_http_worker_policy_inherits_runtime_allowlist() -> None:
    ctx = RuntimeContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        policy_mode="enforce",
        adapter_id="native",
    )
    ref = RuntimeBundleRef(
        bundle_path="/x/bundle.json",
        manifest_hash="a" * 64,
        created_at=1,
        source_compile_run_id="run",
    )
    bundle = RuntimeBundle(
        context=ctx,
        ref=ref,
        nodes=(),
        contract_ids=("c",),
        policy_envelope={"runtime_allow_http": True},
        metadata={
            "coordination_inherit_http_allowlist": True,
            "runtime_execution": {
                "allow_http": True,
                "http_allowlist": ["127.0.0.1"],
                "http_method_allowlist": ["POST", "GET"],
            },
        },
    )
    pol = coordination_http_worker_policy_from_bundle(bundle)
    assert pol.bundle_patterns == ("127.0.0.1",)
    assert pol.http_execution_enabled
    assert pol.post_method_allowed


def test_resolve_coordination_http_post_url_prefers_delegate_over_env() -> None:
    u, err = resolve_coordination_http_post_url(
        env_url="http://env.example/x",
        policy_context={
            "coordination_delegate_edges": [
                {"edge_id": "e1", "from_step_id": "a", "delegate_target": "http://delegate.example/y"},
            ],
        },
    )
    assert err is None
    assert u == "http://delegate.example/y"


def test_resolve_coordination_http_post_url_multiple_http_delegates_errors() -> None:
    u, err = resolve_coordination_http_post_url(
        env_url="http://env.example/x",
        policy_context={
            "coordination_delegate_edges": [
                {"delegate_target": "http://a.example/1"},
                {"delegate_target": "https://b.example/2"},
            ],
        },
    )
    assert u is None
    assert err == "coordination_http_delegate_multiple_urls"


def test_resolve_coordination_http_post_url_duplicate_delegate_urls_ok() -> None:
    u, err = resolve_coordination_http_post_url(
        env_url="",
        policy_context={
            "coordination_delegate_edges": [
                {"delegate_target": "http://127.0.0.1/same"},
                {"delegate_target": "http://127.0.0.1/same"},
            ],
        },
    )
    assert err is None
    assert u == "http://127.0.0.1/same"


def _coordination_step_action(*, policy_context: dict) -> RuntimeAction:
    return RuntimeAction(
        action_id="coordination:s1:0",
        action_type="coordination.step",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="f" * 64,
        idempotency_key="idem",
        policy_context=policy_context,
    )


def test_agent_worker_inner_from_env_uses_delegate_url_when_env_url_empty() -> None:
    ctx = RuntimeContext(
        tenant_id="t",
        repo_id="r",
        run_id="run",
        runtime_run_id="rt",
        policy_mode="enforce",
        adapter_id="native",
    )
    ref = RuntimeBundleRef(
        bundle_path="/x/bundle.json",
        manifest_hash="a" * 64,
        created_at=1,
        source_compile_run_id="run",
    )
    bundle = RuntimeBundle(
        context=ctx,
        ref=ref,
        nodes=(),
        contract_ids=("c",),
        policy_envelope={"runtime_allow_http": True},
        metadata={
            "coordination_agent_worker_http_allowlist": ["127.0.0.1"],
            "runtime_execution": {
                "allow_http": True,
                "http_method_allowlist": ["POST", "GET"],
            },
        },
    )
    action = _coordination_step_action(
        policy_context={
            "coordination_step_id": "s1",
            "coordination_role_id": "writer",
            "coordination_spec_sha256": "a" * 64,
            "run_stage": "coordination.step",
            "coordination_delegate_edges": [
                {
                    "edge_id": "e1",
                    "from_step_id": "prev",
                    "delegate_target": "http://127.0.0.1:59999/delegate",
                },
            ],
        },
    )
    raw = json.dumps(
        {"text": "via-delegate", "usage": {"input_tokens": 0, "output_tokens": 0}},
        separators=(",", ":"),
    ).encode("utf-8")
    with patch.dict(
        "os.environ",
        {"AKC_AGENT_WORKER_HTTP": "1", "AKC_HTTP_AGENT_WORKER_URL": ""},
        clear=False,
    ):
        inner = agent_worker_inner_from_env(bundle=bundle, action=action)
    assert isinstance(inner, HttpAgentWorker)
    with patch(
        "akc.runtime.coordination.worker._coordination_worker_http_post",
        return_value=(raw, None),
    ) as post:
        out = inner.execute_role_turn(context=_sample_role_worker_context())
        assert post.call_count == 1
        called_url = post.call_args.kwargs["url"]
        assert called_url == "http://127.0.0.1:59999/delegate"
    assert out.status == "succeeded"


def test_agent_worker_inner_from_env_multiple_delegate_http_fails_closed() -> None:
    action = _coordination_step_action(
        policy_context={
            "coordination_step_id": "s1",
            "coordination_role_id": "writer",
            "coordination_spec_sha256": "a" * 64,
            "coordination_delegate_edges": [
                {"delegate_target": "http://127.0.0.1/a"},
                {"delegate_target": "http://127.0.0.1/b"},
            ],
        },
    )
    with patch.dict("os.environ", {"AKC_AGENT_WORKER_HTTP": "1"}, clear=False):
        inner = agent_worker_inner_from_env(bundle=None, action=action)
    out = inner.execute_role_turn(context=_sample_role_worker_context())
    assert out.status == "failed"
    assert out.error == "coordination_http_delegate_multiple_urls"
