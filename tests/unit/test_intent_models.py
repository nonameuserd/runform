import json
from pathlib import Path

import pytest

from akc.compile.controller_config import Budget
from akc.intent import (
    Constraint,
    IntentSpecV1,
    JsonFileIntentStore,
    Objective,
    OperatingBound,
    OperationalValidityParams,
    OperationalValidityParamsError,
    PolicyRef,
    SQLiteIntentStore,
    SuccessCriterion,
    compile_intent_spec,
    compute_intent_fingerprint,
    parse_operational_validity_params,
    stable_intent_sha256,
)


def _make_intent(*, goal_statement: str) -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_123",
        tenant_id="tenant_a",
        repo_id="repo_1",
        spec_version=1,
        status="active",
        title="t",
        goal_statement=goal_statement,
        summary="s",
        derived_from_goal_text=True,
        objectives=(Objective(id="obj1", priority=1, statement="do the thing", target="ship"),),
        constraints=(Constraint(id="c1", kind="hard", statement="must be safe"),),
        policies=(PolicyRef(id="p1", source="cfg", requirement="net=false"),),
        success_criteria=(
            SuccessCriterion(
                id="sc1",
                evaluation_mode="human_gate",
                description="approved by human",
                params={"threshold": 1},
            ),
        ),
        operating_bounds=OperatingBound(max_seconds=30.0, max_steps=5, allow_network=False),
        assumptions=(),
        risk_notes=(),
        tags=("tag1",),
        metadata={"extra": "value"},
        created_at_ms=1,
        updated_at_ms=2,
    )


def test_intent_json_roundtrip() -> None:
    intent = _make_intent(goal_statement="goal v1")
    payload = intent.to_json_obj()
    again = IntentSpecV1.from_json_obj(json.loads(json.dumps(payload)))
    assert again.to_json_obj() == payload


def test_intent_fingerprints_distinguish_semantic_vs_goal_text() -> None:
    old = _make_intent(goal_statement="goal v1")
    new = _make_intent(goal_statement="goal v2")

    old_fp = compute_intent_fingerprint(intent=old)
    new_fp = compute_intent_fingerprint(intent=new)

    assert old_fp.semantic == new_fp.semantic
    assert old_fp.goal_text != new_fp.goal_text


def test_intent_spec_rejects_duplicate_objective_ids() -> None:
    with pytest.raises(ValueError, match="intent.objectives must have unique ids"):
        IntentSpecV1(
            intent_id="intent_dup_obj",
            tenant_id="tenant_a",
            repo_id="repo_1",
            spec_version=1,
            status="active",
            goal_statement="Goal",
            derived_from_goal_text=True,
            objectives=(
                Objective(id="obj1", priority=1, statement="do x", target=None),
                Objective(id="obj1", priority=2, statement="do y", target=None),
            ),
            constraints=(),
            policies=(),
            success_criteria=(),
            operating_bounds=OperatingBound(max_seconds=10.0, max_steps=1, allow_network=False),
            assumptions=(),
            risk_notes=(),
            tags=(),
            metadata=None,
            created_at_ms=1,
            updated_at_ms=2,
        )


def test_intent_spec_requires_goal_or_objectives_or_constraints() -> None:
    with pytest.raises(ValueError, match="intent must define at least one"):
        IntentSpecV1(
            intent_id="intent_empty",
            tenant_id="tenant_a",
            repo_id="repo_1",
            spec_version=1,
            status="draft",
            title=None,
            goal_statement=None,
            summary=None,
            derived_from_goal_text=False,
            objectives=(),
            constraints=(),
            policies=(),
            success_criteria=(),
            operating_bounds=None,
            assumptions=(),
            risk_notes=(),
            tags=(),
            metadata=None,
            created_at_ms=1,
            updated_at_ms=2,
        )


def test_intent_stable_sha256_is_deterministic_for_nested_dict_key_order() -> None:
    # stable_intent_sha256 uses json.dumps(sort_keys=True) so nested dict order
    # should not affect the output.
    md_ab = {"a": 1, "b": 2}
    md_ba = {"b": 2, "a": 1}

    base_kwargs = dict(  # noqa: C408  # kwargs preserve precise types for IntentSpecV1(**...)
        intent_id="intent_sha_md",
        tenant_id="tenant_a",
        repo_id="repo_1",
        spec_version=1,
        status="active",
        title="t",
        goal_statement="goal v1",
        summary="s",
        derived_from_goal_text=True,
        objectives=(Objective(id="obj1", priority=1, statement="do the thing", target="ship"),),
        constraints=(Constraint(id="c1", kind="hard", statement="must be safe"),),
        policies=(PolicyRef(id="p1", source="cfg", requirement="net=false"),),
        success_criteria=(
            SuccessCriterion(
                id="sc1",
                evaluation_mode="human_gate",
                description="approved by human",
                params={"threshold": 1},
            ),
        ),
        operating_bounds=OperatingBound(max_seconds=30.0, max_steps=5, allow_network=False),
        assumptions=(),
        risk_notes=(),
        tags=("tag1",),
        created_at_ms=1,
        updated_at_ms=2,
    )

    intent_ab = IntentSpecV1(metadata=md_ab, **base_kwargs)
    intent_ba = IntentSpecV1(metadata=md_ba, **base_kwargs)

    assert stable_intent_sha256(intent=intent_ab) == stable_intent_sha256(intent=intent_ba)


def test_goal_only_compile_intent_is_deterministic_for_same_goal_and_budget() -> None:
    # Goal-only compatibility should derive a stable intent_id so replays/caches
    # remain stable across identical runs.
    budget = Budget(max_llm_calls=4, max_repairs_per_step=2, max_iterations_total=4)
    i1 = compile_intent_spec(
        tenant_id="tenant_a",
        repo_id="repo_1",
        goal_statement="Do the thing",
        controller_budget=budget,
    )
    i2 = compile_intent_spec(
        tenant_id="tenant_a",
        repo_id="repo_1",
        goal_statement="Do the thing",
        controller_budget=budget,
    )
    assert i1.intent_id == i2.intent_id


def test_json_file_intent_store(tmp_path: Path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = _make_intent(goal_statement="goal v1")

    created = store.create_intent(intent=intent)
    assert created.intent_id == "intent_123"
    assert store.get_active_intent_id(tenant_id="tenant_a", repo_id="repo_1") == "intent_123"

    loaded = store.load_intent(tenant_id="tenant_a", repo_id="repo_1", intent_id="intent_123")
    assert loaded is not None
    assert loaded.to_json_obj() == intent.to_json_obj()

    # No cross-tenant contamination: active pointer should be scoped.
    assert store.get_active_intent_id(tenant_id="tenant_b", repo_id="repo_1") is None


def test_json_file_intent_store_rejects_cross_tenant_load(tmp_path: Path) -> None:
    store = JsonFileIntentStore(base_dir=tmp_path)
    intent = _make_intent(goal_statement="goal v1")
    _ = store.create_intent(intent=intent)

    # Intent files are scoped under <base>/<tenant>/<repo>/ so cross-tenant load
    # returns None (no leakage).
    loaded = store.load_intent(tenant_id="tenant_b", repo_id="repo_1", intent_id=intent.intent_id)
    assert loaded is None


def test_sqlite_intent_store(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "intent.sqlite"
    store = SQLiteIntentStore(path=str(sqlite_path))
    intent = _make_intent(goal_statement="goal v1")

    created = store.create_intent(intent=intent)
    assert created.intent_id == "intent_123"
    assert store.get_active_intent_id(tenant_id="tenant_a", repo_id="repo_1") == "intent_123"

    loaded = store.load_intent(tenant_id="tenant_a", repo_id="repo_1", intent_id="intent_123")
    assert loaded is not None
    assert loaded.to_json_obj() == intent.to_json_obj()

    assert store.get_active_intent_id(tenant_id="tenant_b", repo_id="repo_1") is None


def test_operational_validity_params_roundtrip() -> None:
    raw = {
        "spec_version": 1,
        "window": "single_run",
        "predicate_kind": "threshold",
        "threshold": 0.99,
        "threshold_comparator": "gte",
        "signals": [{"evidence_type": "reconcile_outcome", "payload_path": "payload.health_status"}],
        "bundle_schema_version": 4,
        "expected_evidence_types": ["terminal_health"],
    }
    p = OperationalValidityParams.from_mapping(raw)
    assert p.to_json_obj()["spec_version"] == 1
    again = OperationalValidityParams.from_mapping(p.to_json_obj())
    assert again == p


def test_operational_validity_params_rejects_rolling_without_ms() -> None:
    with pytest.raises(OperationalValidityParamsError, match="rolling_ms"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "rolling_ms",
                "predicate_kind": "presence",
                "expected_evidence_types": ["x"],
            }
        )


def test_operational_validity_params_rejects_rolling_without_rollup_path() -> None:
    with pytest.raises(OperationalValidityParamsError, match="evidence_rollup_rel_path"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "rolling_ms",
                "rolling_window_ms": 60_000,
                "predicate_kind": "presence",
                "expected_evidence_types": ["terminal_health"],
            }
        )


def test_operational_validity_params_rejects_rollup_path_when_single_run() -> None:
    with pytest.raises(OperationalValidityParamsError, match="evidence_rollup_rel_path"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "single_run",
                "predicate_kind": "presence",
                "expected_evidence_types": ["x"],
                "evidence_rollup_rel_path": ".akc/verification/w.json",
            }
        )


def test_operational_validity_params_rolling_ms_roundtrip() -> None:
    raw = {
        "spec_version": 1,
        "window": "rolling_ms",
        "rolling_window_ms": 86_400_000,
        "predicate_kind": "presence",
        "signals": [{"evidence_type": "terminal_health", "payload_path": "health_status"}],
        "expected_evidence_types": ["terminal_health"],
        "evidence_rollup_rel_path": ".akc/verification/last_day.json",
    }
    p = OperationalValidityParams.from_mapping(raw)
    again = OperationalValidityParams.from_mapping(p.to_json_obj())
    assert again == p


def test_operational_validity_params_rejects_unknown_keys() -> None:
    with pytest.raises(OperationalValidityParamsError, match="unknown key"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "single_run",
                "predicate_kind": "presence",
                "expected_evidence_types": ["x"],
                "prometheus_query": "up",
            }
        )


def test_operational_validity_params_rejects_unknown_signal_keys() -> None:
    with pytest.raises(OperationalValidityParamsError, match="unknown key"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "single_run",
                "predicate_kind": "presence",
                "expected_evidence_types": ["x"],
                "signals": [{"evidence_type": "terminal_health", "headers": {"Authorization": "x"}}],
            }
        )


def test_operational_validity_params_rejects_non_opaque_otel_query_stub() -> None:
    with pytest.raises(OperationalValidityParamsError, match="otel_query_stub"):
        OperationalValidityParams.from_mapping(
            {
                "spec_version": 1,
                "window": "single_run",
                "predicate_kind": "presence",
                "expected_evidence_types": ["x"],
                "signals": [{"evidence_type": "terminal_health", "otel_query_stub": "https://metrics/query"}],
            }
        )


def test_parse_operational_validity_params_empty_is_none() -> None:
    assert parse_operational_validity_params({}) is None
    assert parse_operational_validity_params(None) is None


def test_sqlite_intent_store_rejects_cross_tenant_load(tmp_path: Path) -> None:
    sqlite_path = tmp_path / "intent.sqlite"
    store = SQLiteIntentStore(path=str(sqlite_path))
    intent = _make_intent(goal_statement="goal v1")
    _ = store.create_intent(intent=intent)

    loaded = store.load_intent(tenant_id="tenant_b", repo_id="repo_1", intent_id=intent.intent_id)
    assert loaded is None
