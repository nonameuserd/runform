"""Optional external agent / LLM workers for coordination steps (bounded, auditable).

The default worker is deterministic and does not call the network. Remote workers are
opt-in via environment (see :func:`agent_worker_from_env`) so the same coordinator and
audit model apply whether workers are local or remote.

For v2 specs, HTTP POST targets may come from ``coordination_delegate_edges[].delegate_target``
when it is an ``http(s)`` URL (allowlist-checked), overriding ``AKC_HTTP_AGENT_WORKER_URL``
for that step; see :func:`resolve_coordination_http_post_url`.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from concurrent.futures import Future, ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from dataclasses import dataclass
from typing import Any, Final, Literal, Protocol, runtime_checkable

from akc.memory.models import JSONValue
from akc.runtime.action_routing import (
    http_allow_ambient_proxy_env,
    http_envelope_allowlist,
    http_max_body_bytes,
    http_max_response_bytes,
    http_method_allowlist,
    http_policy_enabled,
    resolve_coordination_http_worker_bundle_allowlist,
)
from akc.runtime.http_execute import AllowlistRedirectHandler, redact_url_for_evidence, url_allowed_by_lists
from akc.runtime.models import RuntimeAction, RuntimeActionResult, RuntimeBundle, RuntimeContext

_worker_timeout_executor: ThreadPoolExecutor | None = None


def _timeout_executor() -> ThreadPoolExecutor:
    global _worker_timeout_executor
    if _worker_timeout_executor is None:
        _worker_timeout_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="akc_agent_worker")
    return _worker_timeout_executor


logger = logging.getLogger(__name__)

_BEARER_LIKE: Final[re.Pattern[str]] = re.compile(
    r"(?i)(authorization|bearer|api[_-]?key|token|secret)\s*[:=]\s*[^\s,]+"
)
_LONG_HEX: Final[re.Pattern[str]] = re.compile(r"\b[0-9a-f]{32,}\b", re.IGNORECASE)


def redact_for_logs(text: str, *, max_len: int = 256) -> str:
    """Best-effort redaction for log lines (never log raw secrets or full digests)."""

    s = str(text)
    s = re.sub(r"(?i)bearer\s+\S+", "Bearer [REDACTED]", s)
    s = _BEARER_LIKE.sub("[REDACTED_AUTH]", s)
    s = _LONG_HEX.sub("[REDACTED_HEX]", s)
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


WorkerTurnStatus = Literal["succeeded", "failed", "timeout"]


@dataclass(frozen=True, slots=True)
class CoordinationHttpWorkerPolicy:
    """Bundle/envelope-aligned HTTP policy for :class:`HttpAgentWorker` (tenant-scoped via bundle metadata)."""

    bundle_patterns: tuple[str, ...]
    envelope_patterns: tuple[str, ...]
    max_body_bytes: int
    max_response_bytes: int
    allow_ambient_proxy_env: bool
    http_execution_enabled: bool
    post_method_allowed: bool

    @staticmethod
    def fail_closed() -> CoordinationHttpWorkerPolicy:
        return CoordinationHttpWorkerPolicy(
            bundle_patterns=(),
            envelope_patterns=(),
            max_body_bytes=0,
            max_response_bytes=256,
            allow_ambient_proxy_env=False,
            http_execution_enabled=False,
            post_method_allowed=False,
        )

    @staticmethod
    def for_integration_tests(*, bundle_host_patterns: tuple[str, ...]) -> CoordinationHttpWorkerPolicy:
        """Explicit allowlist for tests that spin a local ``HTTPServer`` (never use in production bundles)."""

        return CoordinationHttpWorkerPolicy(
            bundle_patterns=bundle_host_patterns,
            envelope_patterns=(),
            max_body_bytes=1_048_576,
            max_response_bytes=8_388_608,
            allow_ambient_proxy_env=False,
            http_execution_enabled=True,
            post_method_allowed=True,
        )


def coordination_http_worker_policy_from_bundle(bundle: RuntimeBundle | None) -> CoordinationHttpWorkerPolicy:
    """Derive coordination worker HTTP policy from bundle metadata and ``policy_envelope`` (fail closed)."""

    if bundle is None:
        return CoordinationHttpWorkerPolicy.fail_closed()
    meta = bundle.metadata
    env_pe = bundle.policy_envelope
    bundle_patterns = resolve_coordination_http_worker_bundle_allowlist(meta)
    envelope_patterns = http_envelope_allowlist(env_pe)
    methods = http_method_allowlist(meta)
    return CoordinationHttpWorkerPolicy(
        bundle_patterns=bundle_patterns,
        envelope_patterns=envelope_patterns,
        max_body_bytes=http_max_body_bytes(meta),
        max_response_bytes=http_max_response_bytes(meta),
        allow_ambient_proxy_env=http_allow_ambient_proxy_env(env_pe),
        http_execution_enabled=http_policy_enabled(bundle_metadata=meta, policy_envelope=env_pe),
        post_method_allowed="POST" in methods,
    )


@dataclass(frozen=True, slots=True)
class RoleWorkerContext:
    """Inputs for one coordination role turn (tenant-scoped, hash-friendly)."""

    tenant_id: str
    repo_id: str
    run_id: str
    runtime_run_id: str
    coordination_step_id: str
    coordination_role_id: str
    coordination_spec_sha256: str
    action_id: str
    idempotency_key: str
    inputs_fingerprint: str
    timeout_s: float
    max_input_tokens: int | None
    max_output_tokens: int | None
    policy_context_summary: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class AgentWorkerTurnResult:
    """Outcome of :meth:`AgentWorkerAdapter.execute_role_turn` (no raw model text here)."""

    status: WorkerTurnStatus
    output_text_sha256: str
    output_text_len: int
    duration_ms: int
    error: str | None = None
    usage_input_tokens: int | None = None
    usage_output_tokens: int | None = None


@runtime_checkable
class AgentWorkerAdapter(Protocol):
    """Execute one bounded coordination role turn (remote agent, HTTP backend, or local stub)."""

    def execute_role_turn(self, *, context: RoleWorkerContext) -> AgentWorkerTurnResult: ...


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def intent_worker_bounds_from_bundle(*, bundle: RuntimeBundle | None) -> tuple[float, int | None, int | None]:
    """Derive timeout and token caps from ``intent_policy_projection.operating_bounds_effective``."""

    default_timeout = 60.0
    if bundle is None:
        return default_timeout, None, None
    meta = bundle.metadata.get("intent_policy_projection")
    if not isinstance(meta, Mapping):
        return default_timeout, None, None
    bounds = meta.get("operating_bounds_effective")
    if not isinstance(bounds, Mapping):
        return default_timeout, None, None
    max_seconds_raw = bounds.get("max_seconds")
    timeout_s = default_timeout
    if isinstance(max_seconds_raw, (int, float)) and float(max_seconds_raw) > 0:
        timeout_s = float(max_seconds_raw)
    max_in = bounds.get("max_input_tokens")
    max_out = bounds.get("max_output_tokens")
    mi = int(max_in) if isinstance(max_in, (int, float)) and int(max_in) > 0 else None
    mo = int(max_out) if isinstance(max_out, (int, float)) and int(max_out) > 0 else None
    return timeout_s, mi, mo


def build_role_worker_context(
    *,
    context: RuntimeContext,
    action: RuntimeAction,
    bundle: RuntimeBundle | None,
) -> RoleWorkerContext:
    pc = action.policy_context or {}
    step = str(pc.get("coordination_step_id", "")).strip()
    role = str(pc.get("coordination_role_id", "")).strip()
    spec = str(pc.get("coordination_spec_sha256", "")).strip().lower()
    timeout_s, max_in, max_out = intent_worker_bounds_from_bundle(bundle=bundle)
    summary: dict[str, JSONValue] = {
        "run_stage": pc.get("run_stage"),
        "coordination_step_id": step,
        "coordination_role_id": role,
        "coordination_spec_sha256": spec,
    }
    ext = pc.get("external_identity_metadata")
    if isinstance(ext, Mapping):
        summary["external_identity_metadata"] = dict(ext)
    pred = pc.get("coordination_handoff_predecessor_output_sha256s")
    if pred is not None:
        summary["coordination_handoff_predecessor_output_sha256s"] = pred
    return RoleWorkerContext(
        tenant_id=context.tenant_id,
        repo_id=context.repo_id,
        run_id=context.run_id,
        runtime_run_id=context.runtime_run_id,
        coordination_step_id=step,
        coordination_role_id=role,
        coordination_spec_sha256=spec,
        action_id=action.action_id,
        idempotency_key=action.idempotency_key,
        inputs_fingerprint=action.inputs_fingerprint,
        timeout_s=timeout_s,
        max_input_tokens=max_in,
        max_output_tokens=max_out,
        policy_context_summary=summary,
    )


def _run_turn_with_timeout(
    *,
    inner: AgentWorkerAdapter,
    context: RoleWorkerContext,
) -> AgentWorkerTurnResult:
    """Enforce ``context.timeout_s`` even if the inner worker blocks."""

    timeout_s = float(context.timeout_s) if context.timeout_s > 0 else 60.0

    def _call() -> AgentWorkerTurnResult:
        return inner.execute_role_turn(context=context)

    fut: Future[AgentWorkerTurnResult] = _timeout_executor().submit(_call)
    try:
        return fut.result(timeout=timeout_s)
    except FuturesTimeoutError:
        fut.cancel()
        return AgentWorkerTurnResult(
            status="timeout",
            output_text_sha256=_sha256_text(""),
            output_text_len=0,
            duration_ms=int(timeout_s * 1000),
            error="agent_worker_deadline_exceeded",
        )


class TimeoutEnforcingAgentWorker:
    """Wraps an :class:`AgentWorkerAdapter` with mandatory wall-clock timeout."""

    def __init__(self, inner: AgentWorkerAdapter) -> None:
        self._inner = inner

    def execute_role_turn(self, *, context: RoleWorkerContext) -> AgentWorkerTurnResult:
        return _run_turn_with_timeout(inner=self._inner, context=context)


def resolve_coordination_http_post_url(
    *,
    env_url: str,
    policy_context: Mapping[str, Any] | None,
) -> tuple[str | None, str | None]:
    """Pick the POST URL for :class:`HttpAgentWorker` (fail-closed).

    When ``policy_context`` contains v2 ``coordination_delegate_edges`` with HTTP(S)
    ``delegate_target`` values, those override ``AKC_HTTP_AGENT_WORKER_URL`` for the
    request URL. Multiple distinct HTTP delegate targets return ``(None, error)``.

    Returns:
        ``(url, None)`` when a single URL should be used, or ``(None, err)`` on
        ambiguous delegate configuration. Empty env URL with no HTTP delegate targets
        yields ``(None, None)`` (caller should fall back to deterministic worker).
    """

    env_u = str(env_url).strip()
    if policy_context is None:
        return (env_u or None, None)
    raw = policy_context.get("coordination_delegate_edges")
    if not isinstance(raw, list):
        return (env_u or None, None)
    found: list[str] = []
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        t = item.get("delegate_target")
        if not isinstance(t, str):
            continue
        tl = t.strip()
        if not tl:
            continue
        low = tl.lower()
        if low.startswith("http://") or low.startswith("https://"):
            found.append(tl)
    uniq = sorted(frozenset(found))
    if len(uniq) > 1:
        return (None, "coordination_http_delegate_multiple_urls")
    if len(uniq) == 1:
        return (uniq[0], None)
    return (env_u or None, None)


class DeterministicAgentWorker:
    """Default worker: deterministic, no network, stable fingerprints."""

    def execute_role_turn(self, *, context: RoleWorkerContext) -> AgentWorkerTurnResult:
        started = time.perf_counter()
        # Stable placeholder body derived from idempotency (replay-stable).
        body = f"noop:{context.idempotency_key}"
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return AgentWorkerTurnResult(
            status="succeeded",
            output_text_sha256=_sha256_text(body),
            output_text_len=len(body),
            duration_ms=max(elapsed_ms, 0),
            usage_input_tokens=0,
            usage_output_tokens=0,
        )


class FailedConfigAgentWorker:
    """Inner worker that fails every turn (deterministic HTTP delegate misconfiguration)."""

    def __init__(self, *, error: str) -> None:
        self._error = str(error).strip() or "coordination_agent_worker_config_error"

    def execute_role_turn(self, *, context: RoleWorkerContext) -> AgentWorkerTurnResult:
        _ = context
        started = time.perf_counter()
        elapsed_ms = int((time.perf_counter() - started) * 1000)
        return AgentWorkerTurnResult(
            status="failed",
            output_text_sha256=_sha256_text(""),
            output_text_len=0,
            duration_ms=max(elapsed_ms, 0),
            error=self._error,
        )


@dataclass(frozen=True, slots=True)
class HttpAgentWorkerConfig:
    """Environment for :class:`HttpAgentWorker` (stdlib HTTP, mirrors ``examples/llm_backends/http_llm_backend``).

    - ``AKC_HTTP_AGENT_WORKER_URL``: POST endpoint (JSON in/out).
    - ``AKC_HTTP_AGENT_WORKER_API_KEY``: optional ``Authorization: Bearer`` header.
    - ``AKC_HTTP_AGENT_WORKER_TIMEOUT_S``: optional upper bound for HTTP client (defaults to context timeout).
    """

    url: str
    api_key: str | None
    client_timeout_cap_s: float | None

    @staticmethod
    def from_env() -> HttpAgentWorkerConfig:
        url = os.environ.get("AKC_HTTP_AGENT_WORKER_URL", "").strip()
        api_key_raw = os.environ.get("AKC_HTTP_AGENT_WORKER_API_KEY")
        cap_raw = os.environ.get("AKC_HTTP_AGENT_WORKER_TIMEOUT_S", "").strip()
        api_key = api_key_raw.strip() if isinstance(api_key_raw, str) and api_key_raw.strip() else None
        cap: float | None = None
        if cap_raw:
            try:
                c = float(cap_raw)
                if c > 0:
                    cap = c
            except ValueError:
                cap = None
        return HttpAgentWorkerConfig(url=url, api_key=api_key, client_timeout_cap_s=cap)


def _coordination_worker_http_post(
    *,
    url: str,
    body: bytes,
    headers: Mapping[str, str],
    timeout_s: float,
    max_response_bytes: int,
    allow_ambient_proxy_env: bool,
    url_allowed: Callable[[str], bool],
    max_redirects: int = 5,
) -> tuple[bytes | None, str | None]:
    """POST with redirect re-checks and a hard response size cap (aligned with :func:`execute_bounded_http`)."""

    handlers: list[urllib.request.BaseHandler] = [
        AllowlistRedirectHandler(max_redirects=max_redirects, url_allowed=url_allowed),
    ]
    if not allow_ambient_proxy_env:
        handlers.insert(0, urllib.request.ProxyHandler({}))
    opener = urllib.request.build_opener(*handlers)
    req = urllib.request.Request(url, data=body, method="POST")
    for hk, hv in headers.items():
        req.add_header(hk, hv)
    try:
        timeout = max(timeout_s, 0.001)
        with opener.open(req, timeout=timeout) as resp:
            code = getattr(resp, "status", None)
            if code is None:
                code = resp.getcode()
            chunk = resp.read(max_response_bytes + 1)
    except urllib.error.HTTPError as exc:
        code = int(exc.code)
        chunk = exc.read(max_response_bytes + 1) if exc.fp is not None else b""
        if not (200 <= code < 400):
            return None, f"http_error status={code}"
    except Exception as exc:
        return None, str(exc)
    else:
        code_int = int(code) if code is not None else None
        if code_int is None or not (200 <= code_int < 400):
            return None, f"unexpected status={code_int}"

    if len(chunk) > max_response_bytes:
        return None, "response exceeded max_response_bytes"
    return chunk, None


class HttpAgentWorker:
    """HTTP JSON worker; enable with ``AKC_AGENT_WORKER_HTTP=1`` and a URL (fail-closed)."""

    def __init__(
        self,
        cfg: HttpAgentWorkerConfig | None = None,
        *,
        policy: CoordinationHttpWorkerPolicy | None = None,
        post_url: str | None = None,
    ) -> None:
        self._cfg = cfg or HttpAgentWorkerConfig.from_env()
        self._policy = policy if policy is not None else CoordinationHttpWorkerPolicy.fail_closed()
        pu = str(post_url).strip() if post_url is not None else ""
        self._post_url: str | None = pu or None

    def _effective_post_url(self) -> str:
        return self._post_url or self._cfg.url

    def execute_role_turn(self, *, context: RoleWorkerContext) -> AgentWorkerTurnResult:
        post_u = self._effective_post_url().strip()
        if not post_u:
            raise ValueError("HttpAgentWorker requires a POST URL (env or coordination delegate_target)")
        started = time.perf_counter()
        url_redacted = redact_url_for_evidence(post_u)
        pol = self._policy

        def _fail(error: str) -> AgentWorkerTurnResult:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return AgentWorkerTurnResult(
                status="failed",
                output_text_sha256=_sha256_text(""),
                output_text_len=0,
                duration_ms=max(elapsed_ms, 0),
                error=error,
            )

        if not pol.http_execution_enabled:
            logger.info("agent_worker.http denied url_redacted=%s reason=http_execution_disabled", url_redacted)
            return _fail("coordination_http_execution_disabled")
        if not pol.bundle_patterns:
            logger.info("agent_worker.http denied url_redacted=%s reason=allowlist_missing", url_redacted)
            return _fail("coordination_http_worker_allowlist_missing")
        if not pol.post_method_allowed:
            logger.info("agent_worker.http denied url_redacted=%s reason=post_not_allowlisted", url_redacted)
            return _fail("coordination_http_post_not_allowlisted")
        if not url_allowed_by_lists(
            post_u,
            bundle_patterns=pol.bundle_patterns,
            envelope_patterns=pol.envelope_patterns,
        ):
            logger.info("agent_worker.http denied url_redacted=%s reason=url_not_allowlisted", url_redacted)
            return _fail("coordination_http_worker_url_not_allowlisted")

        prompt = self._render_prompt(context=context)
        payload: dict[str, Any] = {
            "tenant_id": context.tenant_id,
            "repo_id": context.repo_id,
            "run_id": context.run_id,
            "runtime_run_id": context.runtime_run_id,
            "coordination_step_id": context.coordination_step_id,
            "coordination_role_id": context.coordination_role_id,
            "coordination_spec_sha256": context.coordination_spec_sha256,
            "timeout_s": context.timeout_s,
            "max_input_tokens": context.max_input_tokens,
            "max_output_tokens": context.max_output_tokens,
            "prompt": prompt,
            "idempotency_key": context.idempotency_key,
            "inputs_fingerprint": context.inputs_fingerprint,
        }
        line = redact_for_logs(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        logger.info("agent_worker.http request url_redacted=%s payload=%s", url_redacted, line)

        headers: dict[str, str] = {"Content-Type": "application/json"}
        if self._cfg.api_key:
            headers["Authorization"] = f"Bearer {self._cfg.api_key}"

        http_timeout = float(context.timeout_s)
        if self._cfg.client_timeout_cap_s is not None:
            http_timeout = min(http_timeout, float(self._cfg.client_timeout_cap_s))
        if http_timeout <= 0:
            http_timeout = 30.0

        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if pol.max_body_bytes > 0 and len(body) > pol.max_body_bytes:
            return _fail("coordination_http_request_body_exceeds_cap")

        def _allowed(u: str) -> bool:
            return url_allowed_by_lists(u, bundle_patterns=pol.bundle_patterns, envelope_patterns=pol.envelope_patterns)

        raw_bytes, post_err = _coordination_worker_http_post(
            url=post_u,
            body=body,
            headers=headers,
            timeout_s=http_timeout,
            max_response_bytes=pol.max_response_bytes,
            allow_ambient_proxy_env=pol.allow_ambient_proxy_env,
            url_allowed=_allowed,
        )
        if post_err is not None or raw_bytes is None:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            err = post_err or "http_request_failed"
            logger.info(
                "agent_worker.http error url_redacted=%s err=%s",
                url_redacted,
                redact_for_logs(err, max_len=120),
            )
            return AgentWorkerTurnResult(
                status="failed",
                output_text_sha256=_sha256_text(""),
                output_text_len=0,
                duration_ms=max(elapsed_ms, 0),
                error=f"http_error:{err}",
            )

        try:
            resp_obj = json.loads(raw_bytes.decode("utf-8"))
        except Exception:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return AgentWorkerTurnResult(
                status="failed",
                output_text_sha256=_sha256_text(""),
                output_text_len=0,
                duration_ms=max(elapsed_ms, 0),
                error="invalid_json_response",
            )

        if not isinstance(resp_obj, dict):
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return AgentWorkerTurnResult(
                status="failed",
                output_text_sha256=_sha256_text(""),
                output_text_len=0,
                duration_ms=max(elapsed_ms, 0),
                error="response_not_object",
            )

        text_raw = resp_obj.get("text")
        if not isinstance(text_raw, str):
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return AgentWorkerTurnResult(
                status="failed",
                output_text_sha256=_sha256_text(""),
                output_text_len=0,
                duration_ms=max(elapsed_ms, 0),
                error="missing_text_field",
            )
        text = text_raw
        if context.max_output_tokens is not None and len(text) > int(context.max_output_tokens) * 4:
            # Heuristic guard: refuse obviously oversized payloads vs declared budget.
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return AgentWorkerTurnResult(
                status="failed",
                output_text_sha256=_sha256_text(""),
                output_text_len=0,
                duration_ms=max(elapsed_ms, 0),
                error="output_exceeds_token_budget_heuristic",
            )

        usage_in: int | None = None
        usage_out: int | None = None
        usage_raw = resp_obj.get("usage")
        if isinstance(usage_raw, Mapping):
            maybe_in = usage_raw.get("input_tokens")
            maybe_out = usage_raw.get("output_tokens")
            if isinstance(maybe_in, (int, float)):
                usage_in = int(maybe_in)
            if isinstance(maybe_out, (int, float)):
                usage_out = int(maybe_out)
        if (
            context.max_output_tokens is not None
            and usage_out is not None
            and usage_out > int(context.max_output_tokens)
        ):
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            return AgentWorkerTurnResult(
                status="failed",
                output_text_sha256=_sha256_text(""),
                output_text_len=0,
                duration_ms=max(elapsed_ms, 0),
                error="usage_output_tokens_exceeds_budget",
            )

        elapsed_ms = int((time.perf_counter() - started) * 1000)
        logger.info(
            "agent_worker.http ok output_len=%s sha256=%s",
            len(text),
            _sha256_text(text)[:16] + "...",
        )
        return AgentWorkerTurnResult(
            status="succeeded",
            output_text_sha256=_sha256_text(text),
            output_text_len=len(text),
            duration_ms=max(elapsed_ms, 0),
            usage_input_tokens=usage_in,
            usage_output_tokens=usage_out,
        )

    @staticmethod
    def _render_prompt(*, context: RoleWorkerContext) -> str:
        parts = [
            f"[coordination_step_id] {context.coordination_step_id}",
            f"[coordination_role_id] {context.coordination_role_id}",
            f"[inputs_fingerprint] {context.inputs_fingerprint}",
        ]
        return "\n".join(parts)


def agent_worker_inner_from_env(
    *,
    bundle: RuntimeBundle | None = None,
    action: RuntimeAction | None = None,
) -> AgentWorkerAdapter:
    """Select inner worker: HTTP when explicitly enabled, otherwise deterministic."""

    flag = os.environ.get("AKC_AGENT_WORKER_HTTP", "").strip().lower()
    if flag not in {"1", "true", "yes"}:
        return DeterministicAgentWorker()
    cfg = HttpAgentWorkerConfig.from_env()
    pc: Mapping[str, Any] | None = None
    if action is not None and action.action_type == "coordination.step":
        raw_pc = action.policy_context
        pc = raw_pc if isinstance(raw_pc, dict) else None
    post_url, cfg_err = resolve_coordination_http_post_url(env_url=cfg.url, policy_context=pc)
    if cfg_err is not None:
        return FailedConfigAgentWorker(error=cfg_err)
    if not post_url:
        return DeterministicAgentWorker()
    policy = coordination_http_worker_policy_from_bundle(bundle)
    return HttpAgentWorker(cfg=cfg, policy=policy, post_url=post_url)


def agent_worker_from_env(
    *,
    bundle: RuntimeBundle | None = None,
    action: RuntimeAction | None = None,
) -> AgentWorkerAdapter:
    """Worker used by :class:`~akc.runtime.adapters.native.NativeRuntimeAdapter` by default (timeout-wrapped)."""

    return TimeoutEnforcingAgentWorker(inner=agent_worker_inner_from_env(bundle=bundle, action=action))


def coordination_step_runtime_result(
    *,
    adapter_id: str,
    action: RuntimeAction,
    turn: AgentWorkerTurnResult,
) -> RuntimeActionResult:
    """Map a worker turn into :class:`RuntimeActionResult` without embedding raw model text."""

    status: Literal["succeeded", "failed", "cancelled"] = (
        "succeeded" if turn.status == "succeeded" else ("failed" if turn.status == "failed" else "failed")
    )
    if turn.status == "timeout":
        status = "failed"
    outputs: dict[str, JSONValue] = {
        "action_id": action.action_id,
        "action_type": action.action_type,
        "adapter_id": adapter_id,
        "agent_worker_status": turn.status,
        "agent_worker_output_sha256": turn.output_text_sha256,
        "agent_worker_output_len": int(turn.output_text_len),
    }
    cost: dict[str, JSONValue] | None = None
    if turn.usage_input_tokens is not None or turn.usage_output_tokens is not None:
        cost = {}
        if turn.usage_input_tokens is not None:
            cost["input_tokens"] = int(turn.usage_input_tokens)
        if turn.usage_output_tokens is not None:
            cost["output_tokens"] = int(turn.usage_output_tokens)
    return RuntimeActionResult(
        status=status,
        outputs=outputs,
        error=turn.error,
        duration_ms=turn.duration_ms,
        cost=cost,
    )
