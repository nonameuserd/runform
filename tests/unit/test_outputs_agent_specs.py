from __future__ import annotations

import json

import pytest

from akc.compile.interfaces import TenantRepoScope
from akc.outputs.models import AgentBudget, AgentRoleSpec, AgentSpec, LlmBackendSpec


def test_agent_spec_requires_scope_and_non_empty_roles() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    llm = LlmBackendSpec(backend="openai", model="gpt-5")

    with pytest.raises(ValueError, match="roles must be non-empty"):
        AgentSpec(scope=scope, name="demo", llm=llm, roles=[])


def test_agent_spec_rejects_duplicate_role_names() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    llm = LlmBackendSpec(backend="openai", model="gpt-5")
    with pytest.raises(ValueError, match="duplicate agent role"):
        AgentSpec(
            scope=scope,
            name="demo",
            llm=llm,
            roles=[AgentRoleSpec(name="planner"), AgentRoleSpec(name="planner")],
        )


def test_agent_spec_renders_deterministic_json_and_binds_scope() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    spec = AgentSpec(
        scope=scope,
        name="compiler-agent",
        llm=LlmBackendSpec(backend="openai", model="gpt-5", params={"temperature": 0.2}),
        roles=[
            AgentRoleSpec(name="planner", tools=["index.query"], budget=AgentBudget(max_steps=10)),
            AgentRoleSpec(
                name="writer",
                tools=["fs.write", "python"],
                instructions="Prefer small diffs.",
            ),
        ],
        metadata={"purpose": "unit-test"},
    )

    j1 = spec.render_json()
    j2 = spec.render_json()
    assert j1 == j2

    obj = json.loads(j1)
    assert obj["scope"]["tenant_id"] == "t1"
    assert obj["scope"]["repo_id"] == "repo1"
    assert obj["name"] == "compiler-agent"
    assert obj["llm"]["backend"] == "openai"
    assert obj["llm"]["model"] == "gpt-5"
    assert obj["roles"][0]["name"] == "planner"


def test_agent_spec_to_artifacts_default_to_akc_agents_dir() -> None:
    scope = TenantRepoScope(tenant_id="t1", repo_id="repo1")
    spec = AgentSpec(
        scope=scope,
        name="demo",
        llm=LlmBackendSpec(backend="openai", model="gpt-5"),
        roles=[AgentRoleSpec(name="planner")],
    )
    a_json = spec.to_artifact_json()
    a_yml = spec.to_artifact_yaml()
    assert a_json.path == ".akc/agents/demo.json"
    assert a_yml.path == ".akc/agents/demo.yml"
    assert a_json.media_type.startswith("application/json")
    assert a_yml.media_type.startswith("application/yaml")

