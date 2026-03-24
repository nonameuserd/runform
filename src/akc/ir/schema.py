from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast

from akc.ir.provenance import ProvenancePointer
from akc.ir.versioning import (
    IR_FORMAT_VERSION,
    IR_SCHEMA_KIND,
    IR_SCHEMA_VERSION,
    require_supported_ir_version,
)
from akc.memory.models import JSONValue, require_non_empty
from akc.utils.fingerprint import stable_json_fingerprint

NodeKind = Literal[
    "service",
    "workflow",
    "intent",
    "entity",
    "knowledge",
    "integration",
    "policy",
    "agent",
    "infrastructure",
    "other",
]
ALLOWED_NODE_KINDS: tuple[str, ...] = (
    "service",
    "workflow",
    "intent",
    "entity",
    "knowledge",
    "integration",
    "policy",
    "agent",
    "infrastructure",
    "other",
)
ContractCategory = Literal["runtime", "deployment", "authorization", "acceptance"]
ALLOWED_CONTRACT_CATEGORIES: tuple[ContractCategory, ...] = (
    "runtime",
    "deployment",
    "authorization",
    "acceptance",
)


def stable_node_id(*, kind: str, name: str) -> str:
    require_non_empty(kind, name="kind")
    require_non_empty(name, name="name")
    raw = f"{kind.strip()}::{name.strip()}".encode()
    return f"irn_{sha256(raw).hexdigest()[:16]}"


@dataclass(frozen=True, slots=True)
class EffectAnnotation:
    """Effect declaration for a node, used by policy/runtime gates."""

    network: bool = False
    fs_read: tuple[str, ...] = ()
    fs_write: tuple[str, ...] = ()
    secrets: tuple[str, ...] = ()
    tools: tuple[str, ...] = ()

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "network": bool(self.network),
            "fs_read": list(self.fs_read),
            "fs_write": list(self.fs_write),
            "secrets": list(self.secrets),
            "tools": list(self.tools),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> EffectAnnotation:
        return EffectAnnotation(
            network=bool(obj.get("network", False)),
            fs_read=tuple(str(x) for x in (obj.get("fs_read") or [])),
            fs_write=tuple(str(x) for x in (obj.get("fs_write") or [])),
            secrets=tuple(str(x) for x in (obj.get("secrets") or [])),
            tools=tuple(str(x) for x in (obj.get("tools") or [])),
        )


def _require_mapping(value: Any, *, what: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{what} must be an object")
    return value


def _require_non_empty_int(value: Any, *, name: str) -> int:
    # Note: bool is a subclass of int in Python; reject it explicitly.
    if isinstance(value, bool):
        raise ValueError(f"{name} must be an integer")
    if not isinstance(value, int):
        raise ValueError(f"{name} must be an integer")
    if value < 0:
        raise ValueError(f"{name} must be >= 0")
    return value


def _validate_json_value(value: Any, *, what: str) -> None:
    """Runtime validator for the `JSONValue` recursive shape."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return
    if isinstance(value, list):
        for i, v in enumerate(value):
            _validate_json_value(v, what=f"{what}[{i}]")
        return
    if isinstance(value, dict):
        for k, v in value.items():
            if not isinstance(k, str):
                raise ValueError(f"{what} must have string keys (got key type {type(k).__name__})")
            _validate_json_value(v, what=f"{what}.{k}")
        return
    raise ValueError(f"{what} must be JSONValue-compatible")


def _require_no_unknown_keys(obj: Mapping[str, Any], *, allowed: set[str], what: str) -> None:
    unknown = set(obj.keys()) - allowed
    if unknown:
        sorted_unknown = ", ".join(sorted(unknown))
        raise ValueError(f"{what} contains unknown keys: {sorted_unknown}")


@dataclass(frozen=True, slots=True)
class IRNode:
    """Typed node inside the compiler IR graph."""

    id: str
    tenant_id: str
    kind: NodeKind
    name: str
    properties: Mapping[str, JSONValue]
    depends_on: tuple[str, ...] = ()
    effects: EffectAnnotation | None = None
    provenance: tuple[ProvenancePointer, ...] = ()
    contract: OperationalContract | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="ir_node.id")
        require_non_empty(self.tenant_id, name="ir_node.tenant_id")
        require_non_empty(self.kind, name="ir_node.kind")
        require_non_empty(self.name, name="ir_node.name")
        if self.kind not in ALLOWED_NODE_KINDS:
            raise ValueError(f"ir_node.kind must be one of {ALLOWED_NODE_KINDS}; got {self.kind!r}")
        for pointer in self.provenance:
            if pointer.tenant_id.strip() != self.tenant_id.strip():
                raise ValueError("all provenance pointers must match ir_node.tenant_id")

        if self.contract is not None:
            # Contract/state-machine coupling invariant.
            # - `state_machine` (when present) is a runtime-level concern.
            # - Non-runtime contract categories must not specify a state machine.
            category = self.contract.contract_category.strip().lower()
            if category != "runtime" and self.contract.state_machine is not None:
                raise ValueError("ir_node.contract.state_machine must be None unless contract_category == 'runtime'")

    def to_json_obj(self) -> dict[str, JSONValue]:
        deps_str = sorted({str(d).strip() for d in self.depends_on if str(d).strip()})
        # NOTE: JSONValue uses `list[JSONValue]`; `list[str]` is not compatible due to invariance.
        deps: list[JSONValue] = [cast(JSONValue, s) for s in deps_str]

        provenance_sorted_raw = sorted(
            (p.to_json_obj() for p in self.provenance),
            key=lambda x: (
                str(x.get("kind", "")),
                str(x.get("source_id", "")),
                str(x.get("locator", "")),
            ),
        )
        # NOTE: JSONValue uses `list[JSONValue]`; `list[dict[str, JSONValue]]`
        # is not compatible due to invariance.
        provenance_sorted: list[JSONValue] = [cast(JSONValue, x) for x in provenance_sorted_raw]
        out: dict[str, JSONValue] = {
            "id": self.id.strip(),
            "tenant_id": self.tenant_id.strip(),
            "kind": self.kind,
            "name": self.name.strip(),
            "properties": dict(self.properties),
            "depends_on": deps,
            "effects": self.effects.to_json_obj() if self.effects is not None else None,
            "provenance": provenance_sorted,
            "contract": self.contract.to_json_obj() if self.contract is not None else None,
        }
        return {k: v for k, v in out.items() if v is not None}

    def fingerprint(self) -> str:
        return stable_json_fingerprint(self.to_json_obj())

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> IRNode:
        props = obj.get("properties")
        if not isinstance(props, dict):
            raise ValueError("ir_node.properties must be an object")

        depends_raw = obj.get("depends_on") or []
        if not isinstance(depends_raw, Sequence) or isinstance(depends_raw, (str, bytes)):
            raise ValueError("ir_node.depends_on must be an array")

        effects_raw = obj.get("effects")
        if effects_raw is not None and not isinstance(effects_raw, dict):
            raise ValueError("ir_node.effects must be an object when set")

        prov_raw = obj.get("provenance") or []
        if not isinstance(prov_raw, Sequence) or isinstance(prov_raw, (str, bytes)):
            raise ValueError("ir_node.provenance must be an array")

        provenance: list[ProvenancePointer] = []
        for p in prov_raw:
            if not isinstance(p, dict):
                raise ValueError("ir_node.provenance[] must be an object")
            provenance.append(ProvenancePointer.from_json_obj(dict(p)))

        contract: OperationalContract | None = None
        contract_raw = obj.get("contract", None)
        if "contract" in obj and contract_raw is not None:
            if not isinstance(contract_raw, dict):
                raise ValueError("ir_node.contract must be an object when set")
            contract = OperationalContract.from_json_obj(cast(Mapping[str, Any], contract_raw))

        return IRNode(
            id=str(obj.get("id", "")),
            tenant_id=str(obj.get("tenant_id", "")),
            kind=str(obj.get("kind", "other")),  # type: ignore[arg-type]
            name=str(obj.get("name", "")),
            properties=props,
            depends_on=tuple(str(x) for x in depends_raw),
            effects=(EffectAnnotation.from_json_obj(effects_raw) if isinstance(effects_raw, dict) else None),
            provenance=tuple(provenance),
            contract=contract,
        )


@dataclass(frozen=True, slots=True)
class OperationalBudget:
    """Runtime budget limits for a contract (all optional, but at least one must be set)."""

    max_seconds: int | None = None
    max_steps: int | None = None
    max_tokens: int | None = None

    def __post_init__(self) -> None:
        if self.max_seconds is None and self.max_steps is None and self.max_tokens is None:
            raise ValueError("OperationalBudget must set at least one budget field")
        for v, name in (
            (self.max_seconds, "max_seconds"),
            (self.max_steps, "max_steps"),
            (self.max_tokens, "max_tokens"),
        ):
            if v is not None:
                _require_non_empty_int(v, name=name)

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "max_seconds": self.max_seconds,
            "max_steps": self.max_steps,
            "max_tokens": self.max_tokens,
        }
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> OperationalBudget:
        allowed = {"max_seconds", "max_steps", "max_tokens"}
        _require_mapping(obj, what="OperationalBudget")
        _require_no_unknown_keys(obj, allowed=allowed, what="OperationalBudget")

        ms = obj.get("max_seconds", None)
        steps = obj.get("max_steps", None)
        tokens = obj.get("max_tokens", None)

        if ms is not None:
            ms = _require_non_empty_int(ms, name="OperationalBudget.max_seconds")
        if steps is not None:
            steps = _require_non_empty_int(steps, name="OperationalBudget.max_steps")
        if tokens is not None:
            tokens = _require_non_empty_int(tokens, name="OperationalBudget.max_tokens")

        # __post_init__ enforces "at least one set".
        return OperationalBudget(
            max_seconds=cast(int | None, ms),
            max_steps=cast(int | None, steps),
            max_tokens=cast(int | None, tokens),
        )


@dataclass(frozen=True, slots=True)
class ContractTrigger:
    """Typed event trigger used by `OperationalContract`."""

    trigger_id: str
    source: str
    details: Mapping[str, JSONValue] = field(default_factory=dict)

    def __post_init__(self) -> None:
        require_non_empty(self.trigger_id, name="trigger_id")
        require_non_empty(self.source, name="source")
        _require_mapping(self.details, what="ContractTrigger.details")
        for k, v in self.details.items():
            if not isinstance(k, str):
                raise ValueError("ContractTrigger.details keys must be strings")
            _validate_json_value(v, what=f"ContractTrigger.details.{k}")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "trigger_id": self.trigger_id.strip(),
            "source": self.source.strip(),
            "details": dict(self.details),
        }
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> ContractTrigger:
        allowed = {"trigger_id", "source", "details"}
        _require_mapping(obj, what="ContractTrigger")
        _require_no_unknown_keys(obj, allowed=allowed, what="ContractTrigger")

        trigger_id_raw = obj.get("trigger_id")
        source_raw = obj.get("source")
        details_raw = obj.get("details", {})
        if "details" in obj and details_raw is None:
            raise ValueError("ContractTrigger.details must be an object when set")
        details = _require_mapping(details_raw, what="ContractTrigger.details")
        validated_details: dict[str, JSONValue] = {}
        for k, v in details.items():
            if not isinstance(k, str):
                raise ValueError("ContractTrigger.details keys must be strings")
            _validate_json_value(v, what=f"ContractTrigger.details.{k}")
            validated_details[k] = cast(JSONValue, v)

        if not isinstance(trigger_id_raw, str):
            raise ValueError("ContractTrigger.trigger_id must be a string")
        if not isinstance(source_raw, str):
            raise ValueError("ContractTrigger.source must be a string")
        require_non_empty(trigger_id_raw, name="ContractTrigger.trigger_id")
        require_non_empty(source_raw, name="ContractTrigger.source")
        return ContractTrigger(
            trigger_id=trigger_id_raw.strip(),
            source=source_raw.strip(),
            details=validated_details,
        )


@dataclass(frozen=True, slots=True)
class IOContract:
    """Declared input/output keys for a contract."""

    input_keys: tuple[str, ...]
    output_keys: tuple[str, ...]
    schema: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        if len(self.input_keys) == 0:
            raise ValueError("IOContract.input_keys must be non-empty")
        if len(self.output_keys) == 0:
            raise ValueError("IOContract.output_keys must be non-empty")
        for k in self.input_keys:
            require_non_empty(k, name="IOContract.input_keys[]")
        for k in self.output_keys:
            require_non_empty(k, name="IOContract.output_keys[]")
        if self.schema is not None:
            _require_mapping(self.schema, what="IOContract.schema")
            for key, v in self.schema.items():
                if not isinstance(key, str):
                    raise ValueError("IOContract.schema keys must be strings")
                _validate_json_value(v, what=f"IOContract.schema.{key}")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "input_keys": [k.strip() for k in self.input_keys],
            "output_keys": [k.strip() for k in self.output_keys],
        }
        if self.schema is not None:
            obj["schema"] = dict(self.schema)
        return obj

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> IOContract:
        allowed = {"input_keys", "output_keys", "schema"}
        _require_mapping(obj, what="IOContract")
        _require_no_unknown_keys(obj, allowed=allowed, what="IOContract")

        in_raw = obj.get("input_keys")
        out_raw = obj.get("output_keys")
        schema_raw = obj.get("schema", None)

        if not isinstance(in_raw, Sequence) or isinstance(in_raw, (str, bytes)):
            raise ValueError("IOContract.input_keys must be an array")
        if not isinstance(out_raw, Sequence) or isinstance(out_raw, (str, bytes)):
            raise ValueError("IOContract.output_keys must be an array")

        input_keys: list[str] = []
        for i, k in enumerate(in_raw):
            if not isinstance(k, str):
                raise ValueError(f"IOContract.input_keys[{i}] must be a string")
            require_non_empty(k, name=f"IOContract.input_keys[{i}]")
            input_keys.append(k.strip())

        output_keys: list[str] = []
        for i, k in enumerate(out_raw):
            if not isinstance(k, str):
                raise ValueError(f"IOContract.output_keys[{i}] must be a string")
            require_non_empty(k, name=f"IOContract.output_keys[{i}]")
            output_keys.append(k.strip())

        schema: Mapping[str, JSONValue] | None = None
        if "schema" in obj:
            if schema_raw is None:
                raise ValueError("IOContract.schema must be an object when set")
            schema_obj = _require_mapping(schema_raw, what="IOContract.schema")
            validated_schema: dict[str, JSONValue] = {}
            for key, v in schema_obj.items():
                if not isinstance(key, str):
                    raise ValueError("IOContract.schema keys must be strings")
                _validate_json_value(v, what=f"IOContract.schema.{key}")
                validated_schema[key] = cast(JSONValue, v)
            schema = validated_schema

        return IOContract(
            input_keys=tuple(input_keys),
            output_keys=tuple(output_keys),
            schema=schema,
        )


@dataclass(frozen=True, slots=True)
class StateTransition:
    """Transition between contract states, optionally guarded by a trigger id."""

    transition_id: str
    from_state: str
    to_state: str
    trigger_id: str | None = None
    guard: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.transition_id, name="transition_id")
        require_non_empty(self.from_state, name="from_state")
        require_non_empty(self.to_state, name="to_state")
        if self.trigger_id is not None:
            require_non_empty(self.trigger_id, name="trigger_id")
        if self.guard is not None:
            _require_mapping(self.guard, what="StateTransition.guard")
            for key, v in self.guard.items():
                if not isinstance(key, str):
                    raise ValueError("StateTransition.guard keys must be strings")
                _validate_json_value(v, what=f"StateTransition.guard.{key}")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "transition_id": self.transition_id.strip(),
            "from_state": self.from_state.strip(),
            "to_state": self.to_state.strip(),
            "trigger_id": self.trigger_id.strip() if self.trigger_id is not None else None,
            "guard": dict(self.guard) if self.guard is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> StateTransition:
        allowed = {"transition_id", "from_state", "to_state", "trigger_id", "guard"}
        _require_mapping(obj, what="StateTransition")
        _require_no_unknown_keys(obj, allowed=allowed, what="StateTransition")

        transition_id_raw = obj.get("transition_id")
        from_state_raw = obj.get("from_state")
        to_state_raw = obj.get("to_state")
        trigger_id_raw = obj.get("trigger_id", None)
        guard_raw = obj.get("guard", None)

        if not isinstance(transition_id_raw, str):
            raise ValueError("StateTransition.transition_id must be a string")
        if not isinstance(from_state_raw, str):
            raise ValueError("StateTransition.from_state must be a string")
        if not isinstance(to_state_raw, str):
            raise ValueError("StateTransition.to_state must be a string")
        require_non_empty(transition_id_raw, name="StateTransition.transition_id")
        require_non_empty(from_state_raw, name="StateTransition.from_state")
        require_non_empty(to_state_raw, name="StateTransition.to_state")
        if trigger_id_raw is not None and not isinstance(trigger_id_raw, str):
            raise ValueError("StateTransition.trigger_id must be a string when set")

        guard: Mapping[str, JSONValue] | None = None
        if "guard" in obj:
            if guard_raw is None:
                raise ValueError("StateTransition.guard must be an object when set")
            guard_obj = _require_mapping(guard_raw, what="StateTransition.guard")
            validated_guard: dict[str, JSONValue] = {}
            for key, v in guard_obj.items():
                if not isinstance(key, str):
                    raise ValueError("StateTransition.guard keys must be strings")
                _validate_json_value(v, what=f"StateTransition.guard.{key}")
                validated_guard[key] = cast(JSONValue, v)
            guard = validated_guard

        return StateTransition(
            transition_id=transition_id_raw.strip(),
            from_state=from_state_raw.strip(),
            to_state=to_state_raw.strip(),
            trigger_id=trigger_id_raw.strip() if trigger_id_raw is not None else None,
            guard=guard,
        )


@dataclass(frozen=True, slots=True)
class StateMachineContract:
    """A simple state machine: initial state + transitions."""

    initial_state: str
    transitions: tuple[StateTransition, ...] = ()

    def __post_init__(self) -> None:
        require_non_empty(self.initial_state, name="initial_state")
        if len(self.transitions) == 0:
            raise ValueError("StateMachineContract.transitions must be non-empty")
        # Enforce uniqueness of transition ids (helps stable ordering/diffing).
        seen: set[str] = set()
        for t in self.transitions:
            if t.transition_id in seen:
                raise ValueError(f"duplicate transition_id in StateMachineContract: {t.transition_id}")
            seen.add(t.transition_id)

    def to_json_obj(self) -> dict[str, JSONValue]:
        transitions_sorted = sorted(self.transitions, key=lambda t: t.transition_id)
        return {
            "initial_state": self.initial_state.strip(),
            "transitions": cast(
                JSONValue,
                [t.to_json_obj() for t in transitions_sorted],
            ),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> StateMachineContract:
        allowed = {"initial_state", "transitions"}
        _require_mapping(obj, what="StateMachineContract")
        _require_no_unknown_keys(obj, allowed=allowed, what="StateMachineContract")

        initial_state_raw = obj.get("initial_state")
        transitions_raw = obj.get("transitions")

        if not isinstance(transitions_raw, Sequence) or isinstance(transitions_raw, (str, bytes)):
            raise ValueError("StateMachineContract.transitions must be an array")
        transitions: list[StateTransition] = []
        for i, t in enumerate(transitions_raw):
            if not isinstance(t, dict):
                raise ValueError(f"StateMachineContract.transitions[{i}] must be an object")
            transitions.append(StateTransition.from_json_obj(cast(Mapping[str, Any], t)))

        if not isinstance(initial_state_raw, str):
            raise ValueError("StateMachineContract.initial_state must be a string")
        require_non_empty(initial_state_raw, name="StateMachineContract.initial_state")
        return StateMachineContract(
            initial_state=initial_state_raw.strip(),
            transitions=tuple(transitions),
        )


@dataclass(frozen=True, slots=True)
class OperationalContract:
    """Typed operational semantics for a node (triggers, IO, optional budget/state machine)."""

    contract_id: str
    contract_category: ContractCategory
    triggers: tuple[ContractTrigger, ...] = ()
    io_contract: IOContract | None = None
    state_machine: StateMachineContract | None = None
    runtime_budget: OperationalBudget | None = None
    acceptance: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.contract_id, name="contract_id")
        require_non_empty(self.contract_category, name="contract_category")
        if self.contract_category not in ALLOWED_CONTRACT_CATEGORIES:
            raise ValueError(
                "OperationalContract.contract_category must be one of "
                f"{ALLOWED_CONTRACT_CATEGORIES}; got {self.contract_category!r}"
            )
        if len(self.triggers) == 0:
            raise ValueError("OperationalContract.triggers must be non-empty")
        if self.io_contract is None:
            raise ValueError("OperationalContract.io_contract must be set")
        # Enforce uniqueness of stable ids for deterministic ordering and diffs.
        trigger_ids: set[str] = set()
        for t in self.triggers:
            if t.trigger_id in trigger_ids:
                raise ValueError(f"duplicate trigger_id in OperationalContract: {t.trigger_id}")
            trigger_ids.add(t.trigger_id)
        if self.state_machine is not None:
            for transition in self.state_machine.transitions:
                if transition.trigger_id is not None and transition.trigger_id not in trigger_ids:
                    raise ValueError(
                        "StateMachineContract transition trigger_id must reference an OperationalContract trigger_id"
                    )

        if self.acceptance is not None:
            _require_mapping(self.acceptance, what="OperationalContract.acceptance")
            for key, v in self.acceptance.items():
                if not isinstance(key, str):
                    raise ValueError("OperationalContract.acceptance keys must be strings")
                _validate_json_value(v, what=f"OperationalContract.acceptance.{key}")

    def to_json_obj(self) -> dict[str, JSONValue]:
        triggers_sorted = sorted(self.triggers, key=lambda t: t.trigger_id)
        io_contract = self.io_contract
        if io_contract is None:
            raise ValueError("OperationalContract.io_contract must be set")
        obj: dict[str, JSONValue] = {
            "contract_id": self.contract_id.strip(),
            "contract_category": self.contract_category.strip(),
            "triggers": cast(JSONValue, [t.to_json_obj() for t in triggers_sorted]),
            "io_contract": io_contract.to_json_obj(),
            "state_machine": self.state_machine.to_json_obj() if self.state_machine is not None else None,
            "runtime_budget": (self.runtime_budget.to_json_obj() if self.runtime_budget is not None else None),
            "acceptance": dict(self.acceptance) if self.acceptance is not None else None,
        }
        return {k: v for k, v in obj.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> OperationalContract:
        allowed = {
            "contract_id",
            "contract_category",
            "triggers",
            "io_contract",
            "state_machine",
            "runtime_budget",
            "acceptance",
        }
        _require_mapping(obj, what="OperationalContract")
        _require_no_unknown_keys(obj, allowed=allowed, what="OperationalContract")

        contract_id_raw = obj.get("contract_id")
        contract_category_raw = obj.get("contract_category")
        triggers_raw = obj.get("triggers", None)
        io_raw = obj.get("io_contract", None)
        state_machine_raw = obj.get("state_machine", None)
        runtime_budget_raw = obj.get("runtime_budget", None)
        acceptance_raw = obj.get("acceptance", None)

        if not isinstance(triggers_raw, Sequence) or isinstance(triggers_raw, (str, bytes)):
            raise ValueError("OperationalContract.triggers must be an array")
        if not isinstance(io_raw, dict):
            raise ValueError("OperationalContract.io_contract must be an object")

        triggers: list[ContractTrigger] = []
        for i, t in enumerate(triggers_raw):
            if not isinstance(t, dict):
                raise ValueError(f"OperationalContract.triggers[{i}] must be an object")
            triggers.append(ContractTrigger.from_json_obj(cast(Mapping[str, Any], t)))

        io_contract = IOContract.from_json_obj(cast(Mapping[str, Any], io_raw))

        state_machine: StateMachineContract | None = None
        if "state_machine" in obj:
            if state_machine_raw is None:
                raise ValueError("OperationalContract.state_machine must be an object when set")
            if not isinstance(state_machine_raw, dict):
                raise ValueError("OperationalContract.state_machine must be an object")
            state_machine = StateMachineContract.from_json_obj(cast(Mapping[str, Any], state_machine_raw))

        runtime_budget: OperationalBudget | None = None
        if "runtime_budget" in obj:
            if runtime_budget_raw is None:
                raise ValueError("OperationalContract.runtime_budget must be an object when set")
            if not isinstance(runtime_budget_raw, dict):
                raise ValueError("OperationalContract.runtime_budget must be an object")
            runtime_budget = OperationalBudget.from_json_obj(cast(Mapping[str, Any], runtime_budget_raw))

        acceptance: Mapping[str, JSONValue] | None = None
        if "acceptance" in obj:
            if acceptance_raw is None:
                raise ValueError("OperationalContract.acceptance must be an object when set")
            if not isinstance(acceptance_raw, dict):
                raise ValueError("OperationalContract.acceptance must be an object")
            acc_obj = _require_mapping(acceptance_raw, what="OperationalContract.acceptance")
            validated_acc: dict[str, JSONValue] = {}
            for key, v in acc_obj.items():
                if not isinstance(key, str):
                    raise ValueError("OperationalContract.acceptance keys must be strings")
                _validate_json_value(v, what=f"OperationalContract.acceptance.{key}")
                validated_acc[key] = cast(JSONValue, v)
            acceptance = validated_acc

        if not isinstance(contract_id_raw, str):
            raise ValueError("OperationalContract.contract_id must be a string")
        if not isinstance(contract_category_raw, str):
            raise ValueError("OperationalContract.contract_category must be a string")
        require_non_empty(contract_id_raw, name="OperationalContract.contract_id")
        require_non_empty(contract_category_raw, name="OperationalContract.contract_category")

        return OperationalContract(
            contract_id=contract_id_raw.strip(),
            contract_category=cast(ContractCategory, contract_category_raw.strip().lower()),
            triggers=tuple(triggers),
            io_contract=io_contract,
            state_machine=state_machine,
            runtime_budget=runtime_budget,
            acceptance=acceptance,
        )


@dataclass(frozen=True, slots=True)
class IRDocument:
    """Versioned intermediate representation for one tenant+repo scope."""

    tenant_id: str
    repo_id: str
    nodes: tuple[IRNode, ...]
    schema_version: int = IR_SCHEMA_VERSION
    format_version: str = IR_FORMAT_VERSION

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="ir.tenant_id")
        require_non_empty(self.repo_id, name="ir.repo_id")
        require_supported_ir_version(
            schema_version=int(self.schema_version),
            format_version=self.format_version,
        )
        seen: set[str] = set()
        for n in self.nodes:
            if n.id in seen:
                raise ValueError(f"duplicate ir node id: {n.id}")
            if n.tenant_id.strip() != self.tenant_id.strip():
                raise ValueError("all ir nodes must match ir.tenant_id")
            seen.add(n.id)

    def to_json_obj(self) -> dict[str, JSONValue]:
        nodes = sorted((n.to_json_obj() for n in self.nodes), key=lambda x: str(x["id"]))
        nodes_value = cast(JSONValue, nodes)
        return {
            "schema_kind": IR_SCHEMA_KIND,
            "schema_version": int(self.schema_version),
            "format_version": self.format_version,
            "tenant_id": self.tenant_id.strip(),
            "repo_id": self.repo_id.strip(),
            "nodes": nodes_value,
        }

    def to_json_file(self, path: str | Path) -> None:
        """Write this IRDocument to a JSON file (deterministic key order)."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = self.to_json_obj()
        p.write_text(
            json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )

    def fingerprint(self) -> str:
        return stable_json_fingerprint(self.to_json_obj())

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> IRDocument:
        if str(obj.get("schema_kind", "")) != IR_SCHEMA_KIND:
            raise ValueError(f"ir.schema_kind must be {IR_SCHEMA_KIND}")
        nodes_raw = obj.get("nodes")
        if not isinstance(nodes_raw, Sequence) or isinstance(nodes_raw, (str, bytes)):
            raise ValueError("ir.nodes must be an array")
        nodes: list[IRNode] = []
        for n in nodes_raw:
            if not isinstance(n, dict):
                raise ValueError("ir.nodes[] must be objects")
            nodes.append(IRNode.from_json_obj(n))
        return IRDocument(
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            nodes=tuple(nodes),
            schema_version=int(obj.get("schema_version", IR_SCHEMA_VERSION)),
            format_version=str(obj.get("format_version", IR_FORMAT_VERSION)),
        )

    @staticmethod
    def from_json_file(path: str | Path) -> IRDocument:
        """Load an IRDocument from a JSON file."""
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise ValueError("ir file must contain a JSON object")
        return IRDocument.from_json_obj(raw)
