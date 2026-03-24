from __future__ import annotations

from akc.ir import (
    ContractTrigger,
    IOContract,
    OperationalContract,
    StateMachineContract,
    StateTransition,
)
from akc.runtime.contracts import is_allowed_transition, map_operational_contract
from akc.runtime.models import RuntimeContext, RuntimeEvent


def _context() -> RuntimeContext:
    return RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )


def test_contract_mapping_defaults_event_type_to_trigger_source() -> None:
    contract = OperationalContract(
        contract_id="contract-1",
        contract_category="runtime",
        triggers=(ContractTrigger(trigger_id="wake", source="clock.tick"),),
        io_contract=IOContract(input_keys=("input",), output_keys=("output",)),
        state_machine=StateMachineContract(
            initial_state="queued",
            transitions=(
                StateTransition(
                    transition_id="t-1",
                    from_state="queued",
                    to_state="running",
                    trigger_id="wake",
                ),
            ),
        ),
    )

    mapping = map_operational_contract(contract)
    event = RuntimeEvent(
        event_id="evt-1",
        event_type="clock.tick",
        timestamp=1,
        context=_context(),
        payload={},
    )

    assert mapping.event_match_predicates["wake"](event) is True
    assert is_allowed_transition(
        mapping=mapping,
        from_state="queued",
        to_state="running",
        trigger_id="wake",
    )


def test_contract_mapping_rejects_mismatched_payload_source() -> None:
    contract = OperationalContract(
        contract_id="contract-2",
        contract_category="runtime",
        triggers=(
            ContractTrigger(
                trigger_id="dispatch",
                source="scheduler",
                details={"event_type": "runtime.action.dispatch"},
            ),
        ),
        io_contract=IOContract(input_keys=("input",), output_keys=("output",)),
    )

    mapping = map_operational_contract(contract)
    event = RuntimeEvent(
        event_id="evt-2",
        event_type="runtime.action.dispatch",
        timestamp=1,
        context=_context(),
        payload={"source": "external"},
    )

    assert mapping.event_match_predicates["dispatch"](event) is False


def test_non_runtime_contract_does_not_expose_state_machine_transitions() -> None:
    contract = OperationalContract(
        contract_id="contract-3",
        contract_category="deployment",
        triggers=(ContractTrigger(trigger_id="ignored", source="builder"),),
        io_contract=IOContract(input_keys=("input",), output_keys=("output",)),
        state_machine=StateMachineContract(
            initial_state="queued",
            transitions=(
                StateTransition(
                    transition_id="t-2",
                    from_state="queued",
                    to_state="running",
                    trigger_id="ignored",
                ),
            ),
        ),
    )

    mapping = map_operational_contract(contract)

    assert mapping.allowed_transitions == {}
