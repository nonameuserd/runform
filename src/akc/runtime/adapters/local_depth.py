from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from akc.memory.models import JSONValue
from akc.runtime.action_routing import (
    HttpRouteSpec,
    ResolvedActionRoute,
    argv0_allowed,
    http_allow_ambient_proxy_env,
    http_bundle_allowlist,
    http_envelope_allowlist,
    http_max_body_bytes,
    http_max_response_bytes,
    http_method_allowlist,
    http_policy_enabled,
    resolve_action_route,
    run_subprocess_route,
    subprocess_argv_allowlist,
    subprocess_policy_enabled,
)
from akc.runtime.adapters.base import RuntimeAdapter, RuntimeAdapterCapabilities
from akc.runtime.adapters.native import NativeRuntimeAdapter
from akc.runtime.coordination.isolation import subprocess_cwd_for_runtime_action, tenant_repo_root
from akc.runtime.http_execute import execute_bounded_http, url_allowed_by_lists
from akc.runtime.models import (
    RuntimeAction,
    RuntimeActionResult,
    RuntimeActionStatus,
    RuntimeBundle,
    RuntimeContext,
)

if TYPE_CHECKING:
    from akc.runtime.kernel import RuntimeGraphNode


@dataclass(slots=True)
class LocalDepthRuntimeAdapter(RuntimeAdapter):
    """Local execution adapter with explicit action routing (opt-in subprocess).

    Use with :func:`akc.runtime.init.create_local_depth_runtime` or as the primary
    adapter in :class:`HybridRuntimeAdapter`. Honors ``resolve_action_route``;
    :class:`NativeRuntimeAdapter` ignores routing hints.
    """

    outputs_root: Path
    delegate: RuntimeAdapter = field(default_factory=NativeRuntimeAdapter)
    adapter_id: str = "local_depth"
    respects_runtime_action_routing: bool = True
    _bundle: RuntimeBundle | None = field(default=None, init=False, repr=False)

    def capabilities(self) -> RuntimeAdapterCapabilities:
        return RuntimeAdapterCapabilities()

    def prepare(self, *, context: RuntimeContext, bundle: RuntimeBundle) -> None:
        self._bundle = bundle
        self.delegate.prepare(context=context, bundle=bundle)

    def execute_action(self, *, context: RuntimeContext, action: RuntimeAction) -> RuntimeActionResult:
        bundle = self._bundle
        if bundle is None:
            raise RuntimeError("LocalDepthRuntimeAdapter.prepare must run before execute_action")
        return self.execute_action_with_graph_node(context=context, action=action, bundle=bundle, graph_node=None)

    def execute_action_with_graph_node(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        bundle: RuntimeBundle,
        graph_node: RuntimeGraphNode | None,
    ) -> RuntimeActionResult:
        """Like ``execute_action`` but supplies the graph node for IR/contract routing hints."""
        route = resolve_action_route(
            action=action,
            graph_node=graph_node,
            bundle_metadata=bundle.metadata,
        )
        return self._execute_routed(context=context, action=action, bundle=bundle, route=route)

    def _execute_routed(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        bundle: RuntimeBundle,
        route: ResolvedActionRoute,
    ) -> RuntimeActionResult:
        started = time.time_ns()
        if route.kind == "noop":
            return self._noop_result(action=action, started_ns=started)
        if route.kind == "http":
            return self._run_http(
                context=context,
                action=action,
                bundle=bundle,
                spec=route.http,
                started_ns=started,
            )
        if route.kind == "subprocess":
            return self._run_subprocess(
                context=context,
                action=action,
                bundle=bundle,
                route=route,
                started_ns=started,
            )
        return self.delegate.execute_action(context=context, action=action)

    def _noop_result(self, *, action: RuntimeAction, started_ns: int) -> RuntimeActionResult:
        return RuntimeActionResult(
            status="succeeded",
            outputs={
                "action_id": action.action_id,
                "action_type": action.action_type,
                "adapter_id": self.adapter_id,
                "route": "noop",
            },
            duration_ms=self._duration_ms(started_ns),
        )

    def _run_http(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        bundle: RuntimeBundle,
        spec: HttpRouteSpec | None,
        started_ns: int,
    ) -> RuntimeActionResult:
        _ = context
        if spec is None:
            return RuntimeActionResult(
                status="failed",
                error="http route missing runtime_execution.http spec (url required)",
                duration_ms=self._duration_ms(started_ns),
            )
        if not http_policy_enabled(bundle_metadata=bundle.metadata, policy_envelope=bundle.policy_envelope):
            return RuntimeActionResult(
                status="failed",
                error="http execution disabled (set runtime_execution.allow_http or runtime_allow_http)",
                duration_ms=self._duration_ms(started_ns),
            )
        bundle_list = http_bundle_allowlist(bundle.metadata)
        if not bundle_list:
            return RuntimeActionResult(
                status="failed",
                error="http_allowlist is empty or missing (fail closed)",
                duration_ms=self._duration_ms(started_ns),
            )
        methods = http_method_allowlist(bundle.metadata)
        if spec.method not in methods:
            return RuntimeActionResult(
                status="failed",
                error=f"http method not allowlisted: {spec.method!r}",
                duration_ms=self._duration_ms(started_ns),
            )
        env_list = http_envelope_allowlist(bundle.policy_envelope)
        if not url_allowed_by_lists(spec.url, bundle_patterns=bundle_list, envelope_patterns=env_list):
            return RuntimeActionResult(
                status="failed",
                error="http url not allowed by bundle/envelope http_allowlist",
                duration_ms=self._duration_ms(started_ns),
            )
        max_body = http_max_body_bytes(bundle.metadata)
        body_bytes: bytes | None = None
        if spec.body is not None:
            body_bytes = spec.body.encode("utf-8")
            if max_body > 0 and len(body_bytes) > max_body:
                return RuntimeActionResult(
                    status="failed",
                    error=f"http body exceeds http_max_body_bytes ({max_body})",
                    duration_ms=self._duration_ms(started_ns),
                )
        allow_proxy = http_allow_ambient_proxy_env(bundle.policy_envelope)
        max_resp = http_max_response_bytes(bundle.metadata)

        def _allowed(u: str) -> bool:
            return url_allowed_by_lists(u, bundle_patterns=bundle_list, envelope_patterns=env_list)

        bounded = execute_bounded_http(
            url=spec.url,
            method=spec.method,
            headers=spec.headers,
            body=body_bytes,
            timeout_ms=spec.timeout_ms,
            max_response_bytes=max_resp,
            allow_ambient_proxy_env=allow_proxy,
            url_allowed=_allowed,
        )
        outputs: dict[str, JSONValue] = {
            "action_id": action.action_id,
            "action_type": action.action_type,
            "adapter_id": self.adapter_id,
            "route": "http",
            "http_status_code": bounded.status_code,
            "http_latency_ms": bounded.latency_ms,
            "http_url_redacted": bounded.url_redacted,
            "http_response_snippet": bounded.response_body_snippet[:16_384],
        }
        if bounded.ok:
            return RuntimeActionResult(
                status="succeeded",
                outputs=outputs,
                duration_ms=self._duration_ms(started_ns),
            )
        err = bounded.error or "http request failed"
        return RuntimeActionResult(
            status="failed",
            outputs=outputs,
            error=err,
            duration_ms=self._duration_ms(started_ns),
        )

    def _run_subprocess(
        self,
        *,
        context: RuntimeContext,
        action: RuntimeAction,
        bundle: RuntimeBundle,
        route: ResolvedActionRoute,
        started_ns: int,
    ) -> RuntimeActionResult:
        spec = route.subprocess
        if spec is None:
            return RuntimeActionResult(
                status="failed",
                error="subprocess route missing spec",
                duration_ms=self._duration_ms(started_ns),
            )
        if not subprocess_policy_enabled(
            bundle_metadata=bundle.metadata,
            policy_envelope=bundle.policy_envelope,
        ):
            return RuntimeActionResult(
                status="failed",
                error=(
                    "subprocess execution disabled (set runtime_execution.allow_subprocess or runtime_allow_subprocess)"
                ),
                duration_ms=self._duration_ms(started_ns),
            )
        allow = subprocess_argv_allowlist(bundle.metadata)
        if not allow:
            return RuntimeActionResult(
                status="failed",
                error="subprocess_allowlist is empty or missing (fail closed)",
                duration_ms=self._duration_ms(started_ns),
            )
        if not argv0_allowed(argv0=spec.argv[0], allowlist=allow):
            return RuntimeActionResult(
                status="failed",
                error=f"subprocess argv[0] not allowlisted: {spec.argv[0]!r}",
                duration_ms=self._duration_ms(started_ns),
            )
        cwd = subprocess_cwd_for_runtime_action(
            context=context,
            outputs_root=self.outputs_root,
            action_policy_context=dict(action.policy_context) if action.policy_context else None,
        )
        env_minimal: dict[str, str] = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
        pc = action.policy_context
        if isinstance(pc, Mapping):
            if "coordination_execution_allow_network_effective" in pc:
                env_minimal["AKC_COORDINATION_NETWORK_ALLOWED"] = (
                    "1" if bool(pc.get("coordination_execution_allow_network_effective")) else "0"
                )
            fs = pc.get("coordination_filesystem_scope")
            if isinstance(fs, Mapping):
                rr = fs.get("read_only_roots")
                roots: list[str] = []
                if isinstance(rr, list):
                    roots = [str(x) for x in rr if str(x).strip()]
                try:
                    repo = tenant_repo_root(context=context, outputs_root=self.outputs_root)
                    resolved = [str(repo / r.replace("\\", "/").lstrip("/")) for r in roots]
                    env_minimal["AKC_COORDINATION_READ_ONLY_ROOTS_JSON"] = json.dumps(resolved)
                except Exception:
                    env_minimal["AKC_COORDINATION_READ_ONLY_ROOTS_JSON"] = json.dumps(roots)
        try:
            code, out, err = run_subprocess_route(spec=spec, cwd=cwd, env_minimal=env_minimal)
        except Exception as exc:
            return RuntimeActionResult(
                status="failed",
                error=str(exc),
                duration_ms=self._duration_ms(started_ns),
            )
        outputs: dict[str, JSONValue] = {
            "action_id": action.action_id,
            "action_type": action.action_type,
            "adapter_id": self.adapter_id,
            "exit_code": int(code),
            "stdout": out[:16_384],
            "stderr": err[:16_384],
        }
        status: RuntimeActionStatus = "succeeded" if code == 0 else "failed"
        return RuntimeActionResult(
            status=status,
            outputs=outputs,
            error=None if code == 0 else f"subprocess exited with code {code}",
            duration_ms=self._duration_ms(started_ns),
        )

    @staticmethod
    def _duration_ms(started_ns: int) -> int:
        return max(0, int((time.time_ns() - started_ns) / 1_000_000))

    def wait_signal(self, *, context: RuntimeContext, signal_spec: Mapping[str, object]) -> object:
        return self.delegate.wait_signal(context=context, signal_spec=signal_spec)

    def checkpoint(self, *, context: RuntimeContext) -> str | None:
        return self.delegate.checkpoint(context=context)

    def restore(self, *, context: RuntimeContext, checkpoint_token: str) -> None:
        self.delegate.restore(context=context, checkpoint_token=checkpoint_token)

    def cancel(self, *, context: RuntimeContext, action_id: str) -> None:
        self.delegate.cancel(context=context, action_id=action_id)
