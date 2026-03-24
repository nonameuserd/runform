from __future__ import annotations

import pytest

from akc.compile.interfaces import TenantRepoScope
from akc.outputs import (
    AgentRoleSpec,
    AgentSpec,
    CoordinationEdgeSpec,
    CoordinationSpec,
    LlmBackendSpec,
    OrchestrationSpec,
    OrchestrationStepSpec,
    SystemDesignSpec,
)


def _mk_agent(*, scope: TenantRepoScope, name: str, roles: list[AgentRoleSpec], params=None) -> AgentSpec:
    llm = LlmBackendSpec(backend="mock", model="mock-model", params=params)
    return AgentSpec(scope=scope, name=name, llm=llm, roles=roles, spec_version=1)


def test_system_design_spec_render_is_deterministic() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="r1")

    # Same semantic spec, different input order for agents/roles/steps/edges.
    agent_b = _mk_agent(
        scope=scope,
        name="agent_b",
        roles=[
            AgentRoleSpec(name="planner", tools=("executor.run",)),
            AgentRoleSpec(name="retriever", tools=("llm.complete",)),
        ],
        params={"temperature": 0.2},
    )
    agent_a = _mk_agent(
        scope=scope,
        name="agent_a",
        roles=[
            AgentRoleSpec(name="retriever", tools=("llm.complete",)),
            AgentRoleSpec(name="writer", tools=("llm.complete", "executor.run")),
        ],
        params={"top_p": 0.9},
    )

    orch1 = OrchestrationSpec(
        steps=[
            OrchestrationStepSpec(step_id="s1", order_idx=1, agent_name="agent_b", role="planner", inputs={"x": 1}),
            OrchestrationStepSpec(step_id="s0", order_idx=0, agent_name="agent_a", role="writer", inputs={"y": "z"}),
        ],
        execution_allow_network=False,
    )
    coord1 = CoordinationSpec(
        edges=[
            CoordinationEdgeSpec(edge_id="e1", kind="depends_on", src_step_id="s0", dst_step_id="s1"),
            CoordinationEdgeSpec(edge_id="e0", kind="shares_state", src_step_id="s1", dst_step_id="s0"),
        ]
    )
    spec1 = SystemDesignSpec(
        scope=scope,
        system_id="sys",
        agents=[agent_b, agent_a],
        orchestration=orch1,
        coordination=coord1,
    )

    # Swap ordering in inputs.
    agent_b_swapped = _mk_agent(
        scope=scope,
        name="agent_b",
        roles=[
            AgentRoleSpec(name="retriever", tools=("llm.complete",)),
            AgentRoleSpec(name="planner", tools=("executor.run",)),
        ],
        params={"temperature": 0.2},
    )
    agent_a_swapped = _mk_agent(
        scope=scope,
        name="agent_a",
        roles=[
            AgentRoleSpec(name="writer", tools=("executor.run", "llm.complete")),
            AgentRoleSpec(name="retriever", tools=("llm.complete",)),
        ],
        params={"top_p": 0.9},
    )
    orch2 = OrchestrationSpec(
        steps=list(reversed(orch1.steps)),
        execution_allow_network=False,
    )
    coord2 = CoordinationSpec(edges=list(reversed(coord1.edges)))
    spec2 = SystemDesignSpec(
        scope=scope,
        system_id="sys",
        agents=[agent_a_swapped, agent_b_swapped],
        orchestration=orch2,
        coordination=coord2,
    )

    assert spec1.render_json() == spec2.render_json()


def test_system_design_spec_rejects_tenant_scope_mismatch() -> None:
    scope_ok = TenantRepoScope(tenant_id="t1", repo_id="r1")
    scope_bad = TenantRepoScope(tenant_id="t2", repo_id="r1")

    agent_ok = _mk_agent(
        scope=scope_ok,
        name="agent_a",
        roles=[AgentRoleSpec(name="planner", tools=("executor.run",))],
    )
    agent_bad = _mk_agent(
        scope=scope_bad,
        name="agent_b",
        roles=[AgentRoleSpec(name="planner", tools=("executor.run",))],
    )

    orch = OrchestrationSpec(
        steps=[OrchestrationStepSpec(step_id="s0", order_idx=0, agent_name="agent_a", role="planner")],
        execution_allow_network=False,
    )
    coord = CoordinationSpec(
        edges=[CoordinationEdgeSpec(edge_id="e0", kind="depends_on", src_step_id="s0", dst_step_id="s0")]
    )

    with pytest.raises(ValueError):
        SystemDesignSpec(
            scope=scope_ok,
            system_id="sys",
            agents=[agent_ok, agent_bad],
            orchestration=orch,
            coordination=coord,
        )


def test_system_design_spec_rejects_disallowed_tools() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="r1")
    agent = _mk_agent(
        scope=scope,
        name="agent_a",
        roles=[AgentRoleSpec(name="planner", tools=("executor.run", "evil.tool"))],
    )

    orch = OrchestrationSpec(
        steps=[OrchestrationStepSpec(step_id="s0", order_idx=0, agent_name="agent_a", role="planner")],
        execution_allow_network=False,
    )
    coord = CoordinationSpec(
        edges=[CoordinationEdgeSpec(edge_id="e0", kind="depends_on", src_step_id="s0", dst_step_id="s0")]
    )

    with pytest.raises(ValueError):
        SystemDesignSpec(
            scope=scope,
            system_id="sys",
            agents=[agent],
            orchestration=orch,
            coordination=coord,
        )


def test_system_design_spec_rejects_secret_like_llm_params() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="r1")
    agent = _mk_agent(
        scope=scope,
        name="agent_a",
        roles=[AgentRoleSpec(name="planner", tools=("executor.run",))],
        params={"api_key": "shh"},
    )

    orch = OrchestrationSpec(
        steps=[OrchestrationStepSpec(step_id="s0", order_idx=0, agent_name="agent_a", role="planner")],
        execution_allow_network=False,
    )
    coord = CoordinationSpec(
        edges=[CoordinationEdgeSpec(edge_id="e0", kind="depends_on", src_step_id="s0", dst_step_id="s0")]
    )

    with pytest.raises(ValueError):
        SystemDesignSpec(
            scope=scope,
            system_id="sys",
            agents=[agent],
            orchestration=orch,
            coordination=coord,
        )


def test_system_design_spec_rejects_unknown_coordination_step() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="r1")
    agent = _mk_agent(
        scope=scope,
        name="agent_a",
        roles=[AgentRoleSpec(name="planner", tools=("executor.run",))],
    )

    orch = OrchestrationSpec(
        steps=[OrchestrationStepSpec(step_id="s0", order_idx=0, agent_name="agent_a", role="planner")],
        execution_allow_network=False,
    )
    coord = CoordinationSpec(
        edges=[CoordinationEdgeSpec(edge_id="e0", kind="depends_on", src_step_id="s0", dst_step_id="missing")]
    )

    with pytest.raises(ValueError):
        SystemDesignSpec(
            scope=scope,
            system_id="sys",
            agents=[agent],
            orchestration=orch,
            coordination=coord,
        )
