"""Optional compile-time MCP: live resource reads merged after index retrieval.

Requires the ``ingest-mcp`` extra (``mcp`` + ``httpx``). Policy actions
``mcp.resource.read`` / ``mcp.tool.call`` must be allowlisted (and optionally
approved by OPA) alongside ``llm.complete`` / ``executor.run``.

Tenant isolation: only the active :class:`~akc.compile.interfaces.TenantRepoScope`
is threaded through policy context and document metadata; MCP servers are
user-configured out-of-process trust boundaries (see ingest MCP docs).
"""

from __future__ import annotations

import base64
import json
import logging
from pathlib import Path
from typing import Any

from pydantic import AnyUrl

from akc.compile.controller_config import Budget, CompileMcpToolSpec, ControllerConfig
from akc.compile.controller_patch_utils import (
    combined_tool_like_calls,
    refresh_controller_estimated_cost_usd,
)
from akc.compile.interfaces import TenantRepoScope
from akc.control.policy import (
    MCP_RESOURCE_READ_ACTION,
    MCP_TOOL_CALL_ACTION,
    CapabilityToken,
    PolicyDecision,
    PolicyEngine,
    ToolAuthorizationRequest,
)
from akc.ingest.connectors.mcp.client import read_resource_contents_to_parts, run_mcp_session_sync
from akc.ingest.connectors.mcp.config import load_mcp_ingest_config
from akc.ingest.connectors.mcp.connector import mcp_placeholder_text, mcp_source_id_for_uri
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import content_hash
from akc.memory.models import JSONValue
from akc.run.manifest import McpReplayEvent
from akc.utils.fingerprint import stable_json_fingerprint

logger = logging.getLogger(__name__)

_MCP_HEADER = "[Compile-time MCP supplemental context — lower precedence than vector index hits]\n"
_MCP_TOOL_HEADER = "[Compile-time MCP tool output — lower precedence than vector index hits]\n"


def resolved_compile_mcp_server(config: ControllerConfig) -> Any:
    """Return :class:`~akc.ingest.connectors.mcp.config.ResolvedMcpServer` for compile MCP."""

    cfg_path = Path(str(config.compile_mcp_config_path)).expanduser()
    mcp_cfg = load_mcp_ingest_config(cfg_path)
    server_name = (
        str(config.compile_mcp_server).strip()
        if config.compile_mcp_server is not None and str(config.compile_mcp_server).strip()
        else mcp_cfg.default_server
    )
    if not server_name:
        raise ValueError("compile_mcp_server or mcp config default_server is required for compile-time MCP")
    if server_name not in mcp_cfg.servers:
        raise ValueError(f"unknown MCP server {server_name!r} in {cfg_path}")
    return mcp_cfg.servers[server_name]


def _call_tool_result_to_text(result: Any) -> str:
    lines: list[str] = []
    if bool(getattr(result, "isError", False)):
        lines.append("[mcp tool returned isError=true]")
    for block in getattr(result, "content", None) or []:
        txt = getattr(block, "text", None)
        if isinstance(txt, str) and txt.strip():
            lines.append(txt.strip())
    sc = getattr(result, "structuredContent", None)
    if sc is not None:
        try:
            lines.append(json.dumps(sc, sort_keys=True, ensure_ascii=False))
        except (TypeError, ValueError):
            lines.append(str(sc))
    if not lines:
        return str(result)
    return "\n".join(lines)


def _invoke_mcp_tool_sync(*, server: Any, tool_name: str, arguments: dict[str, Any], timeout_s: float | None) -> Any:
    async def _op(session: Any) -> Any:
        return await session.call_tool(tool_name, arguments)

    return run_mcp_session_sync(server, _op, timeout_s=timeout_s)


def run_compile_mcp_tools_into_ctx(
    *,
    ctx: dict[str, Any],
    config: ControllerConfig,
    scope: TenantRepoScope,
    policy_engine: PolicyEngine,
    accounting: dict[str, Any],
    budget: Budget,
    stage: str,
) -> list[McpReplayEvent]:
    """Run configured MCP tools before an LLM generate/repair call; append synthetic docs + audit rows."""

    if not config.compile_mcp_enabled or not config.compile_mcp_tools:
        return []

    server = resolved_compile_mcp_server(config)
    documents: list[dict[str, Any]] = list(ctx.get("documents") or ())
    events: list[McpReplayEvent] = []
    timeout_s = config.compile_mcp_session_timeout_s

    for spec in config.compile_mcp_tools:
        if not isinstance(spec, CompileMcpToolSpec):
            continue
        tool_name = str(spec.tool_name).strip()
        args: dict[str, Any] = dict(spec.arguments or {})
        args_digest = mcp_arguments_digest(args)

        if budget.max_tool_calls is not None and combined_tool_like_calls(accounting) >= int(budget.max_tool_calls):
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_TOOL_CALL_ACTION,
                    tool_name=tool_name,
                    arguments_sha256=args_digest,
                    reason="budget.max_tool_calls_exceeded",
                )
            )
            continue
        if budget.max_mcp_calls is not None and int(accounting.get("mcp_calls", 0)) >= int(budget.max_mcp_calls):
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_TOOL_CALL_ACTION,
                    tool_name=tool_name,
                    arguments_sha256=args_digest,
                    reason="budget.max_mcp_calls_exceeded",
                )
            )
            continue

        pol_ctx: dict[str, JSONValue] = {
            "stage": str(stage),
            "mcp_server": server.name,
            "mcp_tool_name": tool_name,
            "mcp_arguments_sha256": args_digest,
        }
        decision, _tok = _authorize_mcp(
            policy_engine=policy_engine,
            scope=scope,
            action=MCP_TOOL_CALL_ACTION,
            context=pol_ctx,
            accounting=accounting,
        )
        if bool(decision.block):
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_TOOL_CALL_ACTION,
                    tool_name=tool_name,
                    arguments_sha256=args_digest,
                    reason=str(decision.reason or "policy.blocked"),
                )
            )
            continue

        try:
            raw = _invoke_mcp_tool_sync(server=server, tool_name=tool_name, arguments=args, timeout_s=timeout_s)
            text = _call_tool_result_to_text(raw)
        except (ConnectorError, OSError, RuntimeError, ValueError) as exc:
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_TOOL_CALL_ACTION,
                    tool_name=tool_name,
                    arguments_sha256=args_digest,
                    reason=f"mcp.tool_failed:{type(exc).__name__}",
                )
            )
            continue

        payload_digest = content_hash(text)
        accounting["mcp_calls"] = int(accounting.get("mcp_calls", 0)) + 1
        seq = int(accounting["mcp_calls"])
        doc_id = f"mcp-tool:{server.name}:{tool_name}:{args_digest[:12]}:{seq}"
        documents.append(
            {
                "doc_id": doc_id,
                "title": f"mcp-tool:{server.name}:{tool_name}",
                "content": f"{_MCP_TOOL_HEADER}{text}",
                "score": 0.04,
                "metadata": {
                    "tenant_id": scope.tenant_id,
                    "repo_id": scope.repo_id,
                    "source_type": "mcp_compile_tool",
                    "mcp_server_id": server.name,
                    "mcp_tool_name": tool_name,
                    "mcp_arguments_sha256": args_digest,
                    "mcp_payload_sha256": payload_digest,
                    "mcp_compile_stage": str(stage),
                    "mcp_compile_precedence": "supplemental_after_index",
                },
            }
        )
        events.append(
            McpReplayEvent(
                kind="tool.call",
                server=server.name,
                action=MCP_TOOL_CALL_ACTION,
                tool_name=tool_name,
                arguments_sha256=args_digest,
                payload_sha256=payload_digest,
            )
        )
        refresh_controller_estimated_cost_usd(accounting=accounting, config=config)

    ctx["documents"] = documents
    return events


def _decode_blob_if_text(*, blob_b64: str, mime: str | None) -> tuple[str, bool]:
    try:
        raw = base64.b64decode(blob_b64, validate=True)
    except Exception:
        return "", False
    if mime is None or not str(mime).strip():
        mime_ok = True
    else:
        base = str(mime).lower().split(";", 1)[0].strip()
        mime_ok = base.startswith("text/") or base in {
            "application/json",
            "application/javascript",
            "application/xml",
            "application/xhtml+xml",
            "image/svg+xml",
        }
    if not mime_ok:
        return "", False
    try:
        return raw.decode("utf-8"), True
    except UnicodeDecodeError:
        try:
            return raw.decode("utf-8", errors="replace"), True
        except Exception:
            return "", False


def _append_policy_audit(
    *,
    accounting: dict[str, Any],
    scope: TenantRepoScope,
    action: str,
    token: CapabilityToken,
    decision: PolicyDecision,
    context: dict[str, JSONValue],
) -> None:
    accounting["policy_decisions"].append(
        {
            "action": action,
            "scope": {"tenant_id": scope.tenant_id, "repo_id": scope.repo_id},
            "token_id": str(getattr(token, "token_id", "")),
            "constraints": dict(getattr(token, "constraints", {}) or {}),
            "context": dict(context),
            "allowed": bool(decision.allowed),
            "reason": str(decision.reason),
            "source": str(decision.source),
            "mode": str(decision.mode),
            "block": bool(decision.block),
        }
    )


def _authorize_mcp(
    *,
    policy_engine: PolicyEngine,
    scope: TenantRepoScope,
    action: str,
    context: dict[str, JSONValue],
    accounting: dict[str, Any],
) -> tuple[PolicyDecision, CapabilityToken]:
    constraints: dict[str, JSONValue] = {str(k): v for k, v in context.items()}
    token = policy_engine.issuer.issue(scope=scope, action=action, constraints=constraints)
    decision = policy_engine.authorize(
        req=ToolAuthorizationRequest(
            scope=scope,
            action=action,
            capability=token,
            context=context,
        )
    )
    _append_policy_audit(
        accounting=accounting,
        scope=scope,
        action=action,
        token=token,
        decision=decision,
        context=context,
    )
    return decision, token


def _read_mcp_resource_text(*, server: Any, uri: str, timeout_s: float | None) -> str:
    async def _op(session: Any) -> object:
        return await session.read_resource(AnyUrl(uri))

    read_result = run_mcp_session_sync(server, _op, timeout_s=timeout_s)
    parts = read_resource_contents_to_parts(read_result)
    try:
        from mcp.types import BlobResourceContents, TextResourceContents
    except ImportError as e:  # pragma: no cover
        raise ConnectorError("The 'mcp' package is not installed. Install akc with the ingest-mcp extra.") from e

    texts: list[str] = []
    used_mime: str | None = None
    for part in parts:
        if isinstance(part, TextResourceContents):
            t = part.text
            if isinstance(t, str) and t.strip():
                texts.append(t)
            pm = getattr(part, "mimeType", None)
            if isinstance(pm, str) and pm.strip():
                used_mime = used_mime or pm
        elif isinstance(part, BlobResourceContents):
            b64 = part.blob
            pm = getattr(part, "mimeType", None)
            mime_part = pm if isinstance(pm, str) and pm.strip() else used_mime
            decoded, ok = _decode_blob_if_text(blob_b64=b64, mime=mime_part)
            if ok and decoded.strip():
                texts.append(decoded)
            elif not ok:
                logger.warning("Skipping non-text MCP blob resource part uri=%s mime=%s", uri, mime_part)
                texts.append(
                    mcp_placeholder_text(
                        uri=uri,
                        mime=mime_part,
                        note="binary or non-text blob part (compile-time MCP)",
                    )
                )

    if not texts:
        return mcp_placeholder_text(uri=uri, mime=used_mime, note="empty or unreadable resource")
    body = "\n\n".join(texts).strip()
    return body if body else mcp_placeholder_text(uri=uri, mime=used_mime, note="empty decoded content")


def merge_compile_time_mcp_into_ctx(
    *,
    ctx: dict[str, Any],
    config: ControllerConfig,
    scope: TenantRepoScope,
    policy_engine: PolicyEngine,
    accounting: dict[str, Any],
    budget: Budget,
) -> list[McpReplayEvent]:
    """Append MCP resource documents after structured index results; return replay audit rows."""

    uris = tuple(str(u).strip() for u in config.compile_mcp_resource_uris if str(u).strip())
    if not uris:
        return []

    server = resolved_compile_mcp_server(config)

    documents: list[dict[str, Any]] = list(ctx.get("documents") or ())
    events: list[McpReplayEvent] = []
    timeout_s = config.compile_mcp_session_timeout_s

    for uri in uris:
        if budget.max_tool_calls is not None and combined_tool_like_calls(accounting) >= int(budget.max_tool_calls):
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_RESOURCE_READ_ACTION,
                    uri=uri,
                    reason="budget.max_tool_calls_exceeded",
                )
            )
            continue
        if budget.max_mcp_calls is not None and int(accounting.get("mcp_calls", 0)) >= int(budget.max_mcp_calls):
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_RESOURCE_READ_ACTION,
                    uri=uri,
                    reason="budget.max_mcp_calls_exceeded",
                )
            )
            continue

        pol_ctx: dict[str, JSONValue] = {
            "stage": "retrieve",
            "mcp_server": server.name,
            "mcp_uri": uri,
        }
        decision, _tok = _authorize_mcp(
            policy_engine=policy_engine,
            scope=scope,
            action=MCP_RESOURCE_READ_ACTION,
            context=pol_ctx,
            accounting=accounting,
        )
        if bool(decision.block):
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_RESOURCE_READ_ACTION,
                    uri=uri,
                    reason=str(decision.reason or "policy.blocked"),
                )
            )
            continue

        try:
            text = _read_mcp_resource_text(server=server, uri=uri, timeout_s=timeout_s)
        except (ConnectorError, OSError, RuntimeError, ValueError) as exc:
            events.append(
                McpReplayEvent(
                    kind="refused_live",
                    server=server.name,
                    action=MCP_RESOURCE_READ_ACTION,
                    uri=uri,
                    reason=f"mcp.read_failed:{type(exc).__name__}",
                )
            )
            continue

        digest = content_hash(text)
        accounting["mcp_calls"] = int(accounting.get("mcp_calls", 0)) + 1
        doc_id = mcp_source_id_for_uri(server_name=server.name, uri=uri)
        documents.append(
            {
                "doc_id": doc_id,
                "title": f"mcp:{server.name}:{uri}",
                "content": f"{_MCP_HEADER}{text}",
                "score": 0.05,
                "metadata": {
                    "tenant_id": scope.tenant_id,
                    "repo_id": scope.repo_id,
                    "source_type": "mcp_compile_resource",
                    "mcp_server_id": server.name,
                    "mcp_uri": uri,
                    "mcp_payload_sha256": digest,
                    "mcp_compile_precedence": "supplemental_after_index",
                },
            }
        )
        events.append(
            McpReplayEvent(
                kind="resource.read",
                server=server.name,
                action=MCP_RESOURCE_READ_ACTION,
                uri=uri,
                payload_sha256=digest,
            )
        )
        refresh_controller_estimated_cost_usd(accounting=accounting, config=config)

    ctx["documents"] = documents
    return events


def mcp_arguments_digest(arguments: dict[str, Any] | None) -> str:
    """Stable SHA256 for MCP tool arguments (audit/replay metadata)."""

    return stable_json_fingerprint(dict(arguments or {}))
