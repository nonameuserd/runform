from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.compile.interfaces import TenantRepoScope
from akc.memory.models import JSONValue, json_dumps, require_non_empty
from akc.outputs.models import AgentBudget, AgentRoleName, AgentSpec
from akc.outputs.models import OutputArtifact as _OutputArtifact

CoordinationEdgeKind = Literal[
    "depends_on",
    "notifies",
    "shares_state",
    "parallel",
    "barrier",
    "delegate",
    "handoff",
]


_SYSTEM_DESIGN_SPEC_KIND = "system_design_spec"
_SYSTEM_DESIGN_SPEC_DEFAULT_VERSION = 1

_DEFAULT_SAFE_TOOL_ALLOWLIST: tuple[str, ...] = ("llm.complete", "executor.run")

# Conservative: reject any param key that looks secret-like.
_DISALLOWED_LLM_PARAM_KEY_SUBSTRINGS: tuple[str, ...] = (
    "api_key",
    "apikey",
    "secret",
    "token",
    "password",
    "access_key",
    "private_key",
    "credential",
)


def _validate_json_value(v: JSONValue) -> None:
    # json_dumps is the project-standard “serializable or fail” validator.
    json_dumps(v)


def _validate_llm_params_safety(*, params: Mapping[str, JSONValue]) -> None:
    # Safety guardrail: do not allow secrets to be embedded in emitted configs.
    for k in params:
        ks = str(k).strip().lower()
        if not ks:
            raise ValueError("llm.params keys must be non-empty strings")
        if any(sub in ks for sub in _DISALLOWED_LLM_PARAM_KEY_SUBSTRINGS):
            raise ValueError(f"llm.params contains disallowed secret-like key: {k}")
    # Also validate JSON-serializability / JSONValue discipline.
    for _k, v in params.items():
        _validate_json_value(v)


def _sorted_list_of_dicts_by_key(items: Sequence[Mapping[str, Any]], *, key: str) -> list[Any]:
    return sorted(items, key=lambda d: str(d.get(key, "")))


@dataclass(frozen=True, slots=True)
class OrchestrationStepSpec:
    """A single orchestration action within a system run.

    Each step selects an `AgentSpec` and one of its roles.
    """

    step_id: str
    order_idx: int
    agent_name: str
    role: AgentRoleName
    # Best-effort JSON-serializable inputs to be interpreted by runtime glue.
    inputs: Mapping[str, JSONValue] | None = None
    # Optional per-step runtime budget.
    budget: AgentBudget | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.step_id, name="orchestration.step_id")
        require_non_empty(self.agent_name, name="orchestration.agent_name")
        require_non_empty(self.role, name="orchestration.role")
        if int(self.order_idx) < 0:
            raise ValueError("orchestration.order_idx must be >= 0")
        if self.inputs is not None:
            if not isinstance(self.inputs, Mapping):
                raise ValueError("orchestration.inputs must be an object")
            # Validate JSONValue-ness and serializability.
            for k, v in self.inputs.items():
                require_non_empty(str(k), name="orchestration.inputs[] key")
                _validate_json_value(v)

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "step_id": self.step_id,
            "order_idx": int(self.order_idx),
            "agent_name": self.agent_name,
            "role": self.role,
            "inputs": dict(self.inputs) if self.inputs is not None else None,
            "budget": self.budget.to_json_obj() if self.budget is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}


@dataclass(frozen=True, slots=True)
class OrchestrationSpec:
    """System-level orchestration config with strict safety constraints."""

    spec_version: int = _SYSTEM_DESIGN_SPEC_DEFAULT_VERSION
    max_parallel_steps: int | None = None
    # Network egress is deny-by-default at the spec layer as a guardrail.
    execution_allow_network: bool = False
    # If execution_allow_network is true, require an explicit reason.
    network_allow_reason: str | None = None
    roles: Sequence[str] = ()
    trigger_sources: Sequence[str] = ()
    io_contract: Mapping[str, Mapping[str, Sequence[str]]] | None = None
    policies: Mapping[str, JSONValue] | None = None
    state_machine: Mapping[str, JSONValue] | None = None
    steps: Sequence[OrchestrationStepSpec] = ()

    def __post_init__(self) -> None:
        if int(self.spec_version) <= 0:
            raise ValueError("orchestration.spec_version must be > 0")
        if self.max_parallel_steps is not None and int(self.max_parallel_steps) <= 0:
            raise ValueError("orchestration.max_parallel_steps must be > 0 when set")
        if bool(self.execution_allow_network):
            if self.network_allow_reason is None or not str(self.network_allow_reason).strip():
                raise ValueError("orchestration.network_allow_reason is required when execution_allow_network=true")
        else:
            # Fail closed: do not allow reason without network.
            if self.network_allow_reason is not None and str(self.network_allow_reason).strip():
                raise ValueError("orchestration.network_allow_reason must be empty when execution_allow_network=false")
        cleaned_roles = [str(role).strip() for role in self.roles]
        if any(not role for role in cleaned_roles):
            raise ValueError("orchestration.roles must contain non-empty strings")
        cleaned_trigger_sources = [str(source).strip() for source in self.trigger_sources]
        if any(not source for source in cleaned_trigger_sources):
            raise ValueError("orchestration.trigger_sources must contain non-empty strings")
        if self.io_contract is not None:
            if not isinstance(self.io_contract, Mapping):
                raise ValueError("orchestration.io_contract must be an object")
            for state_id, contract in self.io_contract.items():
                require_non_empty(str(state_id), name="orchestration.io_contract state_id")
                if not isinstance(contract, Mapping):
                    raise ValueError("orchestration.io_contract[] must be an object")
                for contract_key in ("inputs", "outputs"):
                    raw_values = contract.get(contract_key)
                    if raw_values is None:
                        continue
                    if not isinstance(raw_values, Sequence) or isinstance(raw_values, (str, bytes)):
                        raise ValueError(f"orchestration.io_contract[].{contract_key} must be an array")
                    for io_item in raw_values:
                        require_non_empty(
                            str(io_item),
                            name=f"orchestration.io_contract[].{contract_key}[]",
                        )
        if self.policies is not None:
            if not isinstance(self.policies, Mapping):
                raise ValueError("orchestration.policies must be an object")
            for policy_key, policy_value in self.policies.items():
                require_non_empty(str(policy_key), name="orchestration.policies key")
                _validate_json_value(policy_value)
        if self.state_machine is not None:
            if not isinstance(self.state_machine, Mapping):
                raise ValueError("orchestration.state_machine must be an object")
            for key, value in self.state_machine.items():
                require_non_empty(str(key), name="orchestration.state_machine key")
                _validate_json_value(value)
        if not self.steps:
            raise ValueError("orchestration.steps must be non-empty")
        seen: set[str] = set()
        for s in self.steps:
            if s.step_id in seen:
                raise ValueError(f"duplicate orchestration step_id: {s.step_id}")
            seen.add(s.step_id)

    def to_json_obj(self) -> dict[str, JSONValue]:
        steps_sorted = sorted(self.steps, key=lambda x: (int(x.order_idx), str(x.step_id)))
        obj: dict[str, JSONValue] = {
            "spec_version": int(self.spec_version),
            "max_parallel_steps": int(self.max_parallel_steps) if self.max_parallel_steps is not None else None,
            "execution_allow_network": bool(self.execution_allow_network),
            "network_allow_reason": self.network_allow_reason,
            "roles": cast(JSONValue, sorted(str(role) for role in self.roles)),
            "trigger_sources": cast(JSONValue, sorted(str(source) for source in self.trigger_sources)),
            "io_contract": (
                {
                    str(state_id): {
                        str(contract_key): [str(value) for value in values]
                        for contract_key, values in sorted(contract.items())
                    }
                    for state_id, contract in sorted(self.io_contract.items())
                }
                if self.io_contract is not None
                else None
            ),
            "policies": (
                {str(key): value for key, value in sorted(self.policies.items())} if self.policies is not None else None
            ),
            "state_machine": (
                {str(key): value for key, value in sorted(self.state_machine.items())}
                if self.state_machine is not None
                else None
            ),
            "steps": [s.to_json_obj() for s in steps_sorted],
        }
        return {k: v for k, v in obj.items() if v is not None}


@dataclass(frozen=True, slots=True)
class CoordinationEdgeSpec:
    edge_id: str
    kind: CoordinationEdgeKind
    src_step_id: str
    dst_step_id: str
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.edge_id, name="coordination.edge_id")
        require_non_empty(self.kind, name="coordination.kind")
        require_non_empty(self.src_step_id, name="coordination.src_step_id")
        require_non_empty(self.dst_step_id, name="coordination.dst_step_id")
        if self.metadata is not None:
            for k, v in self.metadata.items():
                require_non_empty(str(k), name="coordination.metadata key")
                _validate_json_value(v)

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "edge_id": self.edge_id,
            "kind": self.kind,
            "src_step_id": self.src_step_id,
            "dst_step_id": self.dst_step_id,
            "metadata": dict(self.metadata) if self.metadata is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}


@dataclass(frozen=True, slots=True)
class CoordinationSpec:
    """Coordination edges between orchestration steps."""

    spec_version: int = _SYSTEM_DESIGN_SPEC_DEFAULT_VERSION
    edges: Sequence[CoordinationEdgeSpec] = ()

    def __post_init__(self) -> None:
        if int(self.spec_version) <= 0:
            raise ValueError("coordination.spec_version must be > 0")
        if not self.edges:
            raise ValueError("coordination.edges must be non-empty")
        seen: set[str] = set()
        for e in self.edges:
            if e.edge_id in seen:
                raise ValueError(f"duplicate coordination edge_id: {e.edge_id}")
            seen.add(e.edge_id)

    def to_json_obj(self) -> dict[str, JSONValue]:
        edges_sorted = sorted(self.edges, key=lambda x: str(x.edge_id))
        return {
            "spec_version": int(self.spec_version),
            "edges": [e.to_json_obj() for e in edges_sorted],
        }


@dataclass(frozen=True, slots=True)
class SystemDesignSpec:
    """Internal system design JSON for orchestration + coordination.

    Tenant isolation:
    - The spec is bound to an explicit `scope`.
    - All embedded `AgentSpec`s must share the same tenant+repo scope.
    """

    scope: TenantRepoScope
    system_id: str
    spec_version: int = _SYSTEM_DESIGN_SPEC_DEFAULT_VERSION
    agents: Sequence[AgentSpec] = ()
    orchestration: OrchestrationSpec | None = None
    coordination: CoordinationSpec | None = None

    # Safe config constraints:
    # - Only allow tool actions from this allowlist in emitted agent roles.
    # - By default, we use a conservative allowlist that matches current controller
    #   tool-authorizations.
    allowed_tool_actions: Sequence[str] = _DEFAULT_SAFE_TOOL_ALLOWLIST

    def __post_init__(self) -> None:
        require_non_empty(self.system_id, name="system_id")
        if int(self.spec_version) <= 0:
            raise ValueError("system.spec_version must be > 0")
        if not self.agents:
            raise ValueError("system.agents must be non-empty")
        if self.orchestration is None:
            raise ValueError("system.orchestration is required")
        if self.coordination is None:
            raise ValueError("system.coordination is required")

        # Validate allowlist.
        allowlist = [str(a).strip() for a in self.allowed_tool_actions]
        if any(not a for a in allowlist):
            raise ValueError("system.allowed_tool_actions must contain non-empty strings")
        allowset = set(allowlist)

        # Tenant scope enforcement:
        seen_agents: set[str] = set()
        for agent in self.agents:
            if agent.scope.tenant_id.strip() != self.scope.tenant_id.strip():
                raise ValueError("agent scope tenant_id mismatch in SystemDesignSpec")
            if agent.scope.repo_id.strip() != self.scope.repo_id.strip():
                raise ValueError("agent scope repo_id mismatch in SystemDesignSpec")
            if agent.name in seen_agents:
                raise ValueError(f"duplicate agent name in system.agents: {agent.name}")
            seen_agents.add(agent.name)

            # Safe config constraint: reject secret-like llm params.
            if agent.llm.params:
                _validate_llm_params_safety(params=agent.llm.params)

            # Safe config constraint: validate role tools subset of allowlist.
            for role in agent.roles:
                for tool in role.tools:
                    t = str(tool).strip()
                    if t not in allowset:
                        raise ValueError(f"disallowed tool action in agent role: {t} (allowlist={sorted(allowset)})")
                # Deterministic: do not allow duplicates within a role tool list.
                if len({str(t) for t in role.tools}) != len(role.tools):
                    raise ValueError(f"duplicate tools in agent role: {role.name}")

        # Cross reference validation (orchestration <-> agents):
        agent_name_set = seen_agents
        step_ids = {s.step_id for s in self.orchestration.steps}
        if len(step_ids) != len(tuple(self.orchestration.steps)):
            # OrchestrationSpec already checks uniqueness; keep this as defense-in-depth.
            raise ValueError("system orchestration step_id collision")

        for s in self.orchestration.steps:
            if s.agent_name not in agent_name_set:
                raise ValueError(f"orchestration step references unknown agent_name: {s.agent_name}")

        # Cross reference validation (coordination edges refer to orchestration steps).
        for e in self.coordination.edges:
            if e.src_step_id not in step_ids or e.dst_step_id not in step_ids:
                raise ValueError(f"coordination edge {e.edge_id} references unknown step_id(s)")

    def _agents_json_canonical(self) -> list[dict[str, JSONValue]]:
        # Canonicalize agent role list ordering for deterministic renders.
        agents_sorted = sorted(self.agents, key=lambda a: str(a.name))
        out: list[dict[str, JSONValue]] = []
        for a in agents_sorted:
            aj = a.to_json_obj()
            # roles: stable sort by role.name to avoid input-order nondeterminism.
            roles = aj.get("roles")
            if isinstance(roles, list):
                roles_sorted = _sorted_list_of_dicts_by_key(
                    [r for r in roles if isinstance(r, Mapping)],
                    key="name",
                )
                # tools: stable sort by tool action name.
                for r in roles_sorted:
                    if isinstance(r, Mapping) and isinstance(r.get("tools"), list):
                        tools = r.get("tools")
                        if isinstance(tools, list):
                            rd = cast(dict[str, JSONValue], r)
                            rd["tools"] = cast(JSONValue, sorted(str(t) for t in tools))
                aj["roles"] = cast(JSONValue, roles_sorted)
            out.append(aj)
        return out

    def to_json_obj(self) -> dict[str, JSONValue]:
        orch = self.orchestration
        coord = self.coordination
        if orch is None or coord is None:
            raise ValueError("system.orchestration and system.coordination are required")
        obj: dict[str, JSONValue] = {
            "spec_version": int(self.spec_version),
            "scope": {"tenant_id": self.scope.tenant_id, "repo_id": self.scope.repo_id},
            "system_id": self.system_id,
            "agents": cast(JSONValue, self._agents_json_canonical()),
            "allowed_tool_actions": cast(JSONValue, list(self.allowed_tool_actions)),
            "orchestration": orch.to_json_obj(),
            "coordination": coord.to_json_obj(),
        }
        apply_schema_envelope(obj=cast(dict[str, Any], obj), kind=_SYSTEM_DESIGN_SPEC_KIND)
        # Validate serializability.
        json_dumps(cast(JSONValue, obj))
        return obj

    def render_json(self) -> str:
        return json.dumps(self.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"

    def to_artifact_json(
        self,
        *,
        directory: str = ".akc/system",
        filename: str | None = None,
        media_type: str = "application/json; charset=utf-8",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> _OutputArtifact:
        require_non_empty(directory, name="directory")
        fn = (filename.strip() if isinstance(filename, str) else self.system_id).strip()
        require_non_empty(fn, name="filename")
        if not fn.endswith(".json"):
            fn = f"{fn}.json"
        path = f"{directory.rstrip('/')}/{fn}"
        return _OutputArtifact.from_text(
            path=path,
            text=self.render_json(),
            media_type=media_type,
            metadata=metadata,
        )


__all__ = [
    "CoordinationEdgeKind",
    "CoordinationEdgeSpec",
    "CoordinationSpec",
    "OrchestrationSpec",
    "OrchestrationStepSpec",
    "SystemDesignSpec",
]
