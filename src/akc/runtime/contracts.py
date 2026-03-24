from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass

from akc.ir import (
    ContractTrigger,
    IOContract,
    OperationalBudget,
    OperationalContract,
    StateTransition,
)
from akc.runtime.models import RuntimeActionResult, RuntimeEvent

EventMatchPredicate = Callable[[RuntimeEvent], bool]


@dataclass(frozen=True, slots=True)
class RuntimeContractMapping:
    contract_id: str
    event_match_predicates: Mapping[str, EventMatchPredicate]
    io_contract: IOContract
    allowed_transitions: Mapping[str, tuple[StateTransition, ...]]
    runtime_budget: OperationalBudget | None
    source_contract: OperationalContract


def _trigger_predicate(trigger: ContractTrigger) -> EventMatchPredicate:
    details = dict(trigger.details)
    expected_type = str(details.get("event_type") or trigger.source).strip()
    expected_source = str(trigger.source).strip()

    def _matches(event: RuntimeEvent) -> bool:
        payload_source = str(event.payload.get("source", "")).strip()
        return event.event_type.strip() == expected_type and (not payload_source or payload_source == expected_source)

    return _matches


def map_operational_contract(contract: OperationalContract) -> RuntimeContractMapping:
    predicates = {trigger.trigger_id: _trigger_predicate(trigger) for trigger in contract.triggers}
    transitions: dict[str, list[StateTransition]] = {}
    if contract.contract_category == "runtime" and contract.state_machine is not None:
        for transition in contract.state_machine.transitions:
            transitions.setdefault(transition.from_state, []).append(transition)
    if contract.io_contract is None:
        raise ValueError("OperationalContract.io_contract must be set for runtime mapping")
    return RuntimeContractMapping(
        contract_id=contract.contract_id,
        event_match_predicates=predicates,
        io_contract=contract.io_contract,
        allowed_transitions={key: tuple(value) for key, value in transitions.items()},
        runtime_budget=contract.runtime_budget,
        source_contract=contract,
    )


def validate_action_inputs(*, mapping: RuntimeContractMapping, payload: Mapping[str, object]) -> None:
    expected = set(mapping.io_contract.input_keys)
    actual = set(payload.keys())
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(f"runtime action inputs missing keys: {missing}")


def validate_action_outputs(*, mapping: RuntimeContractMapping, payload: Mapping[str, object]) -> None:
    expected = set(mapping.io_contract.output_keys)
    actual = set(payload.keys())
    missing = sorted(expected - actual)
    if missing:
        raise ValueError(f"runtime action outputs missing keys: {missing}")


def enforce_action_budget(*, mapping: RuntimeContractMapping, result: RuntimeActionResult) -> None:
    budget = mapping.runtime_budget
    if budget is None:
        return
    if (
        budget.max_seconds is not None
        and result.duration_ms is not None
        and result.duration_ms > budget.max_seconds * 1000
    ):
        raise ValueError("runtime action exceeded max_seconds budget")


def is_allowed_transition(
    *,
    mapping: RuntimeContractMapping,
    from_state: str,
    to_state: str,
    trigger_id: str,
) -> bool:
    for transition in mapping.allowed_transitions.get(from_state, ()):
        if transition.to_state != to_state:
            continue
        if transition.trigger_id is None:
            return True
        if transition.trigger_id == trigger_id:
            return True
    return False
