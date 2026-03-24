from __future__ import annotations

import sys
from pathlib import Path

from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.ir.schema import IRNode
from akc.runtime.action_routing import (
    argv0_allowed,
    resolve_action_route,
    resolve_coordination_http_worker_bundle_allowlist,
    subprocess_argv_allowlist,
    subprocess_policy_enabled,
    tenant_scoped_runtime_cwd,
)
from akc.runtime.contracts import map_operational_contract
from akc.runtime.kernel import RuntimeGraphNode
from akc.runtime.models import RuntimeAction, RuntimeContext, RuntimeNodeRef


def _node(contract_acceptance: dict | None = None) -> RuntimeGraphNode:
    contract = OperationalContract(
        contract_id="c1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="t1",
                source="runtime.kernel.started",
                details={"event_type": "runtime.kernel.started"},
            ),
        ),
        io_contract=IOContract(input_keys=("x",), output_keys=("y",)),
        acceptance=contract_acceptance,
    )
    ir_node = IRNode(
        id="n1",
        tenant_id="t",
        kind="workflow",
        name="w",
        properties={
            "runtime_execution": {
                "route": "subprocess",
                "subprocess": {"argv": [sys.executable, "-c", "print(1)"], "timeout_ms": 1000},
            }
        },
        contract=contract,
    )
    mapping = map_operational_contract(contract)
    return RuntimeGraphNode(
        ir_node=ir_node,
        contract_mapping=mapping,
        depends_on=(),
        initial_state="ready",
        terminal_states=("completed",),
    )


def test_resolve_action_route_metadata_override_wins() -> None:
    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    meta = {"runtime_action_routes": {"workflow.execute": "noop"}}
    r = resolve_action_route(action=action, graph_node=_node(), bundle_metadata=meta)
    assert r.kind == "noop"


def test_resolve_action_route_subprocess_from_hints() -> None:
    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    r = resolve_action_route(action=action, graph_node=_node(), bundle_metadata={})
    assert r.kind == "subprocess"
    assert r.subprocess is not None
    assert r.subprocess.argv[0] == sys.executable


def test_subprocess_policy_enabled_metadata_or_envelope() -> None:
    assert not subprocess_policy_enabled(bundle_metadata={}, policy_envelope={})
    assert subprocess_policy_enabled(
        bundle_metadata={"runtime_execution": {"allow_subprocess": True}},
        policy_envelope={},
    )
    assert subprocess_policy_enabled(
        bundle_metadata={},
        policy_envelope={"runtime_allow_subprocess": True},
    )


def test_tenant_scoped_runtime_cwd_matches_state_store_layout(tmp_path: Path) -> None:
    ctx = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-b",
        run_id="run-1",
        runtime_run_id="rr-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    cwd = tenant_scoped_runtime_cwd(context=ctx, outputs_root=tmp_path)
    assert "tenant-a" in cwd.parts
    assert ".akc" in cwd.parts
    assert "run-1" in cwd.parts
    assert "rr-1" in cwd.parts


def test_argv0_allowed_basename_only() -> None:
    allow = frozenset({"python3"})
    assert argv0_allowed(argv0="/usr/bin/python3", allowlist=allow)
    assert not argv0_allowed(argv0="/evil/python3", allowlist=frozenset({"other"}))


def test_subprocess_argv_allowlist_parses_metadata() -> None:
    meta = {"runtime_execution": {"subprocess_allowlist": ["python3", ""]}}
    assert "python3" in subprocess_argv_allowlist(meta)


def test_resolve_http_route() -> None:
    contract = OperationalContract(
        contract_id="c1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="t1",
                source="e",
                details={"event_type": "e"},
            ),
        ),
        io_contract=IOContract(input_keys=("x",), output_keys=("y",)),
    )
    ir_node = IRNode(
        id="n1",
        tenant_id="t",
        kind="workflow",
        name="w",
        properties={"runtime_execution": {"route": "http"}},
        contract=contract,
    )
    mapping = map_operational_contract(contract)
    gn = RuntimeGraphNode(
        ir_node=ir_node,
        contract_mapping=mapping,
        depends_on=(),
        initial_state="ready",
        terminal_states=("completed",),
    )
    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    assert resolve_action_route(action=action, graph_node=gn, bundle_metadata={}).kind == "http"


def test_contract_acceptance_runtime_execution_merged() -> None:
    contract = OperationalContract(
        contract_id="c1",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="t1",
                source="runtime.kernel.started",
                details={"event_type": "runtime.kernel.started"},
            ),
        ),
        io_contract=IOContract(input_keys=("x",), output_keys=("y",)),
        acceptance={
            "runtime_execution": {
                "route": "subprocess",
                "subprocess": {"argv": [sys.executable, "-c", "0"], "timeout_ms": 500},
            }
        },
    )
    ir_node = IRNode(
        id="n1",
        tenant_id="t",
        kind="workflow",
        name="w",
        properties={},
        contract=contract,
    )
    mapping = map_operational_contract(contract)
    gn = RuntimeGraphNode(
        ir_node=ir_node,
        contract_mapping=mapping,
        depends_on=(),
        initial_state="ready",
        terminal_states=("completed",),
    )
    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    r = resolve_action_route(action=action, graph_node=gn, bundle_metadata={})
    assert r.kind == "subprocess"


def test_resolve_coordination_http_worker_bundle_allowlist_dedicated_wins() -> None:
    meta = {
        "coordination_agent_worker_http_allowlist": ["api.example.com"],
        "coordination_inherit_http_allowlist": True,
        "runtime_execution": {"http_allowlist": ["legacy.example.com"]},
    }
    assert resolve_coordination_http_worker_bundle_allowlist(meta) == ("api.example.com",)


def test_resolve_coordination_http_worker_bundle_allowlist_inherit_runtime() -> None:
    meta = {
        "coordination_inherit_http_allowlist": True,
        "runtime_execution": {"http_allowlist": ["127.0.0.1"]},
    }
    assert resolve_coordination_http_worker_bundle_allowlist(meta) == ("127.0.0.1",)


def test_resolve_coordination_http_worker_bundle_allowlist_empty_without_opt_in() -> None:
    meta = {"runtime_execution": {"http_allowlist": ["127.0.0.1"]}}
    assert resolve_coordination_http_worker_bundle_allowlist(meta) == ()


def test_workflow_execution_contract_route_override_applies() -> None:
    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    meta = {
        "workflow_execution_contract": {
            "allowed_routes": ["delegate_adapter", "noop"],
            "route_overrides": {"workflow.execute": "noop"},
        }
    }
    r = resolve_action_route(action=action, graph_node=_node(), bundle_metadata=meta)
    assert r.kind == "noop"


def test_workflow_execution_contract_disallows_candidate_route() -> None:
    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    meta = {"workflow_execution_contract": {"allowed_routes": ["delegate_adapter"]}}
    r = resolve_action_route(action=action, graph_node=_node(), bundle_metadata=meta)
    assert r.kind == "delegate_adapter"


def test_full_layer_replacement_defaults_workflow_to_subprocess() -> None:
    action = RuntimeAction(
        action_id="a1",
        action_type="workflow.execute",
        node_ref=RuntimeNodeRef(node_id="n1", kind="workflow", contract_id="c1"),
        inputs_fingerprint="fp",
        idempotency_key="k1",
    )
    meta = {
        "layer_replacement_mode": "full",
        "workflow_execution_contract": {
            "allowed_routes": ["delegate_adapter", "subprocess"],
            "default_subprocess": {"argv": [sys.executable, "-c", "print(1)"], "timeout_ms": 1000},
        },
    }
    r = resolve_action_route(action=action, graph_node=None, bundle_metadata=meta)
    assert r.kind == "subprocess"
    assert r.subprocess is not None
