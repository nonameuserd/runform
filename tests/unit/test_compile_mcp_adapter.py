"""Unit tests for compile-time MCP merge (policy, budgets, replay metadata)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from akc.compile.controller_config import Budget, CompileMcpToolSpec, ControllerConfig, CostRates, TierConfig
from akc.compile.interfaces import TenantRepoScope
from akc.compile.mcp_adapter import (
    mcp_arguments_digest,
    merge_compile_time_mcp_into_ctx,
    run_compile_mcp_tools_into_ctx,
)
from akc.control.policy import (
    MCP_RESOURCE_READ_ACTION,
    MCP_TOOL_CALL_ACTION,
    CapabilityIssuer,
    DefaultDenyPolicyEngine,
    ToolAuthorizationPolicy,
)
from akc.run.manifest import McpReplayEvent


def _minimal_config(
    *,
    tmp_mcp_json: Path,
    uris: tuple[str, ...],
    tool_allowlist: tuple[str, ...],
    max_tool_calls: int | None = 10,
    max_mcp_calls: int | None = None,
) -> ControllerConfig:
    return ControllerConfig(
        tiers={
            "medium": TierConfig(name="medium", llm_model="stub"),
        },
        budget=Budget(max_tool_calls=max_tool_calls, max_mcp_calls=max_mcp_calls),
        tool_allowlist=tool_allowlist,
        cost_rates=CostRates(mcp_call_usd=0.01),
        compile_mcp_enabled=True,
        compile_mcp_config_path=str(tmp_mcp_json),
        compile_mcp_server="s1",
        compile_mcp_resource_uris=uris,
        compile_mcp_session_timeout_s=5.0,
    )


def test_mcp_arguments_digest_stable() -> None:
    assert mcp_arguments_digest({"b": 1, "a": 2}) == mcp_arguments_digest({"a": 2, "b": 1})


def test_merge_blocked_by_policy_records_refused_live(
    tmp_path: Path,
) -> None:
    cfg_file = tmp_path / "mcp.json"
    cfg_file.write_text(
        '{"servers":{"s1":{"transport":"stdio","command":"false","args":[]}},"default_server":"s1"}',
        encoding="utf-8",
    )
    scope = TenantRepoScope(tenant_id="t1", repo_id="r1")
    policy_engine = DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(
            mode="enforce",
            allow_actions=("llm.complete", "executor.run"),
        ),
    )
    accounting: dict = {
        "policy_decisions": [],
        "input_tokens": 0,
        "output_tokens": 0,
        "tool_calls": 0,
        "mcp_calls": 0,
    }
    config = _minimal_config(
        tmp_mcp_json=cfg_file,
        uris=("file:///x",),
        tool_allowlist=("llm.complete", "executor.run"),
    )
    ctx: dict = {"documents": [{"doc_id": "idx1", "title": "t", "content": "c", "score": 1.0, "metadata": {}}]}
    events = merge_compile_time_mcp_into_ctx(
        ctx=ctx,
        config=config,
        scope=scope,
        policy_engine=policy_engine,
        accounting=accounting,
        budget=config.budget,
    )
    assert len(events) == 1
    assert events[0].kind == "refused_live"
    assert events[0].reason == "policy.default_deny.action_not_allowlisted"
    assert len(ctx["documents"]) == 1


def test_tool_merge_blocked_by_policy(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mcp.json"
    cfg_file.write_text(
        '{"servers":{"s1":{"transport":"stdio","command":"false","args":[]}},"default_server":"s1"}',
        encoding="utf-8",
    )
    scope = TenantRepoScope(tenant_id="t1", repo_id="r1")
    policy_engine = DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(
            mode="enforce",
            allow_actions=("llm.complete", "executor.run", MCP_RESOURCE_READ_ACTION),
        ),
    )
    accounting: dict = {
        "policy_decisions": [],
        "input_tokens": 0,
        "output_tokens": 0,
        "tool_calls": 0,
        "mcp_calls": 0,
        "mcp_manifest_events": [],
    }
    base = _minimal_config(
        tmp_mcp_json=cfg_file,
        uris=(),
        tool_allowlist=("llm.complete", "executor.run", MCP_RESOURCE_READ_ACTION),
        max_tool_calls=50,
    )
    config = replace(
        base,
        compile_mcp_tools=(CompileMcpToolSpec(tool_name="noop", arguments={}),),
    )
    ctx: dict = {"documents": []}
    events = run_compile_mcp_tools_into_ctx(
        ctx=ctx,
        config=config,
        scope=scope,
        policy_engine=policy_engine,
        accounting=accounting,
        budget=config.budget,
        stage="generate",
    )
    assert len(events) == 1
    assert events[0].kind == "refused_live"
    assert events[0].action == MCP_TOOL_CALL_ACTION
    assert "default_deny" in (events[0].reason or "")


def test_merge_respects_max_mcp_calls(tmp_path: Path) -> None:
    cfg_file = tmp_path / "mcp.json"
    cfg_file.write_text(
        '{"servers":{"s1":{"transport":"stdio","command":"false","args":[]}},"default_server":"s1"}',
        encoding="utf-8",
    )
    scope = TenantRepoScope(tenant_id="t1", repo_id="r1")
    allow = ("llm.complete", "executor.run", MCP_RESOURCE_READ_ACTION)
    policy_engine = DefaultDenyPolicyEngine(
        issuer=CapabilityIssuer(),
        policy=ToolAuthorizationPolicy(mode="enforce", allow_actions=allow),
    )
    accounting: dict = {
        "policy_decisions": [],
        "input_tokens": 0,
        "output_tokens": 0,
        "tool_calls": 0,
        "mcp_calls": 0,
    }
    config = _minimal_config(
        tmp_mcp_json=cfg_file,
        uris=("file:///a", "file:///b"),
        tool_allowlist=allow,
        max_mcp_calls=1,
        max_tool_calls=100,
    )
    ctx: dict = {"documents": []}

    def _fake_read(**_kwargs: object) -> str:
        return "hello from mcp"

    with patch("akc.compile.mcp_adapter._read_mcp_resource_text", side_effect=_fake_read):
        events = merge_compile_time_mcp_into_ctx(
            ctx=ctx,
            config=config,
            scope=scope,
            policy_engine=policy_engine,
            accounting=accounting,
            budget=config.budget,
        )
    assert len(events) == 2
    assert events[0].kind == "resource.read"
    assert events[1].kind == "refused_live"
    assert events[1].reason == "budget.max_mcp_calls_exceeded"
    assert accounting["mcp_calls"] == 1


def test_mcp_replay_event_roundtrip() -> None:
    ev = McpReplayEvent(
        kind="resource.read",
        server="s1",
        action=MCP_RESOURCE_READ_ACTION,
        uri="file:///z",
        payload_sha256="a" * 64,
    )
    ev2 = McpReplayEvent.from_json_obj(ev.to_json_obj())
    assert ev2 == ev
