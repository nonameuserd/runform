"""Map runtime actions to execution lanes (noop / subprocess / http / delegate).

Routing inputs (in precedence order):
1. Bundle metadata ``runtime_action_routes`` keyed by ``action.action_type``.
2. IR node ``properties.runtime_execution`` (overrides contract acceptance).
3. Contract ``acceptance.runtime_execution`` on the node's operational contract.
4. Default: ``delegate_adapter`` (backward-compatible stub/delegate execution).

Subprocess specs are only *honored* when :class:`LocalDepthRuntimeAdapter` is used,
``runtime_execution.allow_subprocess`` is true, policy allows
``runtime.action.execute.subprocess``, and ``subprocess_allowlist`` contains the
basename of ``argv[0]`` (see ``docs/runtime-execution.md``).

HTTP specs require ``runtime_execution.allow_http`` or envelope ``runtime_allow_http``,
policy action ``runtime.action.execute.http``, non-empty ``http_allowlist`` (optionally
narrowed by envelope ``http_allowlist``), method allowlist, and body/response caps.
"""

from __future__ import annotations

import os
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal

from akc.memory.models import normalize_repo_id
from akc.runtime.models import RuntimeAction, RuntimeContext

if TYPE_CHECKING:
    from akc.runtime.kernel import RuntimeGraphNode

ActionRouteKind = Literal["noop", "subprocess", "http", "delegate_adapter"]


@dataclass(frozen=True, slots=True)
class SubprocessRouteSpec:
    argv: tuple[str, ...]
    timeout_ms: int


@dataclass(frozen=True, slots=True)
class HttpRouteSpec:
    """Outbound HTTP spec from IR/bundle ``runtime_execution.http`` (policy + allowlists enforced in adapter)."""

    url: str
    method: str
    headers: tuple[tuple[str, str], ...]
    body: str | None
    timeout_ms: int


@dataclass(frozen=True, slots=True)
class ResolvedActionRoute:
    kind: ActionRouteKind
    subprocess: SubprocessRouteSpec | None = None
    http: HttpRouteSpec | None = None


def tenant_scoped_runtime_cwd(*, context: RuntimeContext, outputs_root: str | Path) -> Path:
    """Absolute cwd under ``outputs_root``, matching :class:`FileSystemRuntimeStateStore` layout."""
    base = Path(outputs_root).expanduser().resolve()
    return (
        base
        / context.tenant_id.strip()
        / normalize_repo_id(context.repo_id)
        / ".akc"
        / "runtime"
        / context.run_id.strip()
        / context.runtime_run_id.strip()
    )


def _coerce_route_kind(raw: object) -> ActionRouteKind | None:
    if not isinstance(raw, str):
        return None
    s = raw.strip().lower()
    if s in ("noop", "subprocess", "http", "delegate_adapter"):
        return s  # type: ignore[return-value]
    return None


def _merge_runtime_execution_hints(
    *,
    graph_node: RuntimeGraphNode | None,
) -> dict[str, Any]:
    from_contract: dict[str, Any] = {}
    if graph_node is not None and graph_node.contract_mapping is not None:
        acc = graph_node.contract_mapping.source_contract.acceptance
        if acc is not None:
            raw = acc.get("runtime_execution")
            if isinstance(raw, Mapping):
                from_contract = dict(raw)
    from_node: dict[str, Any] = {}
    if graph_node is not None:
        raw = graph_node.ir_node.properties.get("runtime_execution")
        if isinstance(raw, Mapping):
            from_node = dict(raw)
    merged = {**from_contract, **from_node}
    return merged


def resolve_action_route(
    *,
    action: RuntimeAction,
    graph_node: RuntimeGraphNode | None,
    bundle_metadata: Mapping[str, Any],
) -> ResolvedActionRoute:
    """Resolve the execution lane for ``action`` (configuration only; enforcement is adapter + policy)."""
    workflow_contract_route = _resolve_workflow_contract_route(
        action=action,
        bundle_metadata=bundle_metadata,
    )
    if workflow_contract_route is not None:
        return _finalize_route(
            kind=workflow_contract_route,
            graph_node=graph_node,
            bundle_metadata=bundle_metadata,
        )

    routes = bundle_metadata.get("runtime_action_routes")
    if isinstance(routes, Mapping):
        override = _coerce_route_kind(routes.get(action.action_type))
        if override is not None:
            return _finalize_route(kind=override, graph_node=graph_node, bundle_metadata=bundle_metadata)

    hints = _merge_runtime_execution_hints(graph_node=graph_node)
    kind = _coerce_route_kind(hints.get("route"))
    if kind is None:
        kind = _default_route_for_action(action=action, bundle_metadata=bundle_metadata)
    kind = _enforce_workflow_contract_route_allowlist(
        action=action,
        bundle_metadata=bundle_metadata,
        candidate=kind,
    )
    return _finalize_route(kind=kind, graph_node=graph_node, hints=hints, bundle_metadata=bundle_metadata)


def _full_layer_replacement_enabled(bundle_metadata: Mapping[str, Any]) -> bool:
    mode = bundle_metadata.get("layer_replacement_mode")
    if isinstance(mode, str) and mode.strip().lower() == "full":
        return True
    contract = _workflow_execution_contract(bundle_metadata)
    return contract is not None and contract.get("full_layer_replacement") is True


def _default_route_for_action(*, action: RuntimeAction, bundle_metadata: Mapping[str, Any]) -> ActionRouteKind:
    if str(action.action_type).startswith("workflow.") and _full_layer_replacement_enabled(bundle_metadata):
        return "subprocess"
    return "delegate_adapter"


def _workflow_execution_contract(bundle_metadata: Mapping[str, Any]) -> Mapping[str, Any] | None:
    raw = bundle_metadata.get("workflow_execution_contract")
    return raw if isinstance(raw, Mapping) else None


def _resolve_workflow_contract_route(
    *,
    action: RuntimeAction,
    bundle_metadata: Mapping[str, Any],
) -> ActionRouteKind | None:
    if not str(action.action_type).startswith("workflow."):
        return None
    contract = _workflow_execution_contract(bundle_metadata)
    if contract is None:
        return None
    raw_overrides = contract.get("route_overrides")
    if not isinstance(raw_overrides, Mapping):
        return None
    route = _coerce_route_kind(raw_overrides.get(action.action_type))
    if route is None:
        return None
    return _enforce_workflow_contract_route_allowlist(
        action=action,
        bundle_metadata=bundle_metadata,
        candidate=route,
    )


def _enforce_workflow_contract_route_allowlist(
    *,
    action: RuntimeAction,
    bundle_metadata: Mapping[str, Any],
    candidate: ActionRouteKind,
) -> ActionRouteKind:
    if not str(action.action_type).startswith("workflow."):
        return candidate
    contract = _workflow_execution_contract(bundle_metadata)
    if contract is None:
        return candidate
    allowed_raw = contract.get("allowed_routes")
    if not isinstance(allowed_raw, Sequence) or isinstance(allowed_raw, (str, bytes)):
        return candidate
    allowed: set[ActionRouteKind] = set()
    for item in allowed_raw:
        rk = _coerce_route_kind(item)
        if rk is not None:
            allowed.add(rk)
    if not allowed:
        return candidate
    if candidate in allowed:
        return candidate
    return "delegate_adapter"


def _finalize_route(
    *,
    kind: ActionRouteKind,
    graph_node: RuntimeGraphNode | None,
    hints: Mapping[str, Any] | None = None,
    bundle_metadata: Mapping[str, Any] | None = None,
) -> ResolvedActionRoute:
    hints = dict(hints or _merge_runtime_execution_hints(graph_node=graph_node))
    if kind == "subprocess":
        sp_raw = hints.get("subprocess")
        if not isinstance(sp_raw, Mapping):
            wf_contract = _workflow_execution_contract(bundle_metadata) if bundle_metadata is not None else None
            if isinstance(wf_contract, Mapping):
                maybe_default = wf_contract.get("default_subprocess")
                if isinstance(maybe_default, Mapping):
                    sp_raw = maybe_default
        if not isinstance(sp_raw, Mapping):
            return ResolvedActionRoute(kind="delegate_adapter")
        argv_raw = sp_raw.get("argv")
        if not isinstance(argv_raw, Sequence) or isinstance(argv_raw, (str, bytes)):
            return ResolvedActionRoute(kind="delegate_adapter")
        argv = tuple(str(x) for x in argv_raw if str(x).strip())
        if not argv:
            return ResolvedActionRoute(kind="delegate_adapter")
        timeout_ms = 30_000
        if sp_raw.get("timeout_ms") is not None:
            try:
                timeout_ms = int(sp_raw["timeout_ms"])
            except (TypeError, ValueError):
                timeout_ms = 30_000
        if timeout_ms < 1:
            timeout_ms = 1
        return ResolvedActionRoute(
            kind="subprocess",
            subprocess=SubprocessRouteSpec(argv=argv, timeout_ms=timeout_ms),
        )
    if kind == "http":
        http_spec = _parse_http_route_spec(hints.get("http"))
        return ResolvedActionRoute(kind="http", http=http_spec)
    if kind == "noop":
        return ResolvedActionRoute(kind="noop")
    return ResolvedActionRoute(kind="delegate_adapter")


def subprocess_policy_enabled(
    *,
    bundle_metadata: Mapping[str, Any],
    policy_envelope: Mapping[str, Any],
) -> bool:
    exec_meta = bundle_metadata.get("runtime_execution")
    return bool(policy_envelope.get("runtime_allow_subprocess")) or (
        isinstance(exec_meta, Mapping) and bool(exec_meta.get("allow_subprocess"))
    )


def subprocess_argv_allowlist(bundle_metadata: Mapping[str, Any]) -> frozenset[str]:
    exec_meta = bundle_metadata.get("runtime_execution")
    if not isinstance(exec_meta, Mapping):
        return frozenset()
    raw = exec_meta.get("subprocess_allowlist")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return frozenset()
    return frozenset(str(x).strip() for x in raw if str(x).strip())


def argv0_allowed(*, argv0: str, allowlist: frozenset[str]) -> bool:
    base = os.path.basename(argv0.strip())
    return base in allowlist


def _parse_http_route_spec(raw: object) -> HttpRouteSpec | None:
    if not isinstance(raw, Mapping):
        return None
    url = str(raw.get("url", "")).strip()
    if not url:
        return None
    method = str(raw.get("method", "GET")).strip().upper() or "GET"
    headers = _coerce_http_headers(raw.get("headers"))
    raw_body = raw.get("body")
    body: str | None = None if raw_body is None else str(raw_body)
    timeout_ms = 30_000
    if raw.get("timeout_ms") is not None:
        try:
            timeout_ms = int(raw["timeout_ms"])
        except (TypeError, ValueError):
            timeout_ms = 30_000
    if timeout_ms < 1:
        timeout_ms = 1
    return HttpRouteSpec(url=url, method=method, headers=headers, body=body, timeout_ms=timeout_ms)


def _coerce_http_headers(raw: object) -> tuple[tuple[str, str], ...]:
    if not isinstance(raw, Mapping):
        return ()
    out: list[tuple[str, str]] = []
    for k, v in raw.items():
        name = str(k).strip()
        val = str(v).strip()
        if not name or any(ch in name for ch in ("\r", "\n", " ")) or any(ch in val for ch in ("\r", "\n")):
            continue
        out.append((name, val))
    return tuple(out)


def http_policy_enabled(
    *,
    bundle_metadata: Mapping[str, Any],
    policy_envelope: Mapping[str, Any],
) -> bool:
    exec_meta = bundle_metadata.get("runtime_execution")
    return bool(policy_envelope.get("runtime_allow_http")) or (
        isinstance(exec_meta, Mapping) and bool(exec_meta.get("allow_http"))
    )


def http_bundle_allowlist(bundle_metadata: Mapping[str, Any]) -> tuple[str, ...]:
    exec_meta = bundle_metadata.get("runtime_execution")
    if not isinstance(exec_meta, Mapping):
        return ()
    raw = exec_meta.get("http_allowlist")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    return tuple(str(x).strip() for x in raw if str(x).strip())


def coordination_agent_worker_http_allowlist(bundle_metadata: Mapping[str, Any]) -> tuple[str, ...]:
    """Dedicated allowlist for :class:`~akc.runtime.coordination.worker.HttpAgentWorker` (optional)."""
    raw = bundle_metadata.get("coordination_agent_worker_http_allowlist")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    return tuple(str(x).strip() for x in raw if str(x).strip())


def coordination_inherit_runtime_http_allowlist(bundle_metadata: Mapping[str, Any]) -> bool:
    """When true, coordination HTTP worker may reuse ``runtime_execution.http_allowlist`` (explicit opt-in)."""

    return bundle_metadata.get("coordination_inherit_http_allowlist") is True


def resolve_coordination_http_worker_bundle_allowlist(bundle_metadata: Mapping[str, Any]) -> tuple[str, ...]:
    """Resolve bundle-side HTTP patterns for the coordination agent worker (fail closed if unset)."""

    dedicated = coordination_agent_worker_http_allowlist(bundle_metadata)
    if dedicated:
        return dedicated
    if coordination_inherit_runtime_http_allowlist(bundle_metadata):
        return http_bundle_allowlist(bundle_metadata)
    return ()


def http_envelope_allowlist(policy_envelope: Mapping[str, Any]) -> tuple[str, ...]:
    raw = policy_envelope.get("http_allowlist")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    return tuple(str(x).strip() for x in raw if str(x).strip())


def http_method_allowlist(bundle_metadata: Mapping[str, Any]) -> frozenset[str]:
    exec_meta = bundle_metadata.get("runtime_execution")
    if not isinstance(exec_meta, Mapping):
        return frozenset({"GET"})
    raw = exec_meta.get("http_method_allowlist")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return frozenset({"GET"})
    methods = frozenset(str(x).strip().upper() for x in raw if str(x).strip())
    return methods or frozenset({"GET"})


def http_max_body_bytes(bundle_metadata: Mapping[str, Any]) -> int:
    exec_meta = bundle_metadata.get("runtime_execution")
    if not isinstance(exec_meta, Mapping):
        return 4096
    try:
        n = int(exec_meta.get("http_max_body_bytes", 4096))
    except (TypeError, ValueError):
        return 4096
    return max(0, n)


def http_max_response_bytes(bundle_metadata: Mapping[str, Any]) -> int:
    exec_meta = bundle_metadata.get("runtime_execution")
    if not isinstance(exec_meta, Mapping):
        return 262_144
    try:
        n = int(exec_meta.get("http_max_response_bytes", 262_144))
    except (TypeError, ValueError):
        return 262_144
    return max(256, min(n, 8_388_608))


def http_allow_ambient_proxy_env(policy_envelope: Mapping[str, Any]) -> bool:
    return bool(policy_envelope.get("runtime_http_allow_ambient_proxy_env"))


def run_subprocess_route(
    *,
    spec: SubprocessRouteSpec,
    cwd: Path,
    env_minimal: Mapping[str, str] | None = None,
) -> tuple[int, str, str]:
    """Run ``spec.argv`` with timeout; returns ``(returncode, stdout, stderr)`` text (utf-8, replace errors)."""
    cwd.mkdir(parents=True, exist_ok=True)
    env = dict(env_minimal) if env_minimal is not None else {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
    timeout_s = max(spec.timeout_ms / 1000.0, 0.001)
    proc = subprocess.run(
        list(spec.argv),
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout_s,
        env=env,
        check=False,
    )
    out = proc.stdout or ""
    err = proc.stderr or ""
    return int(proc.returncode), out, err
