from akc.compile.controller_config import Budget
from akc.intent import (
    Constraint,
    IntentSpecV1,
    OperatingBound,
    PolicyRef,
    SuccessCriterion,
    build_handoff_intent_ref,
    project_deployment_intent_projection,
    project_intent_operating_bounds_to_policy_context,
    project_runtime_intent_projection,
    project_stage_timeout_s,
)


def _intent_spec() -> IntentSpecV1:
    return IntentSpecV1(
        intent_id="intent_123",
        tenant_id="tenant_a",
        repo_id="repo_a",
        goal_statement="Ship a constrained runtime bundle",
        constraints=(
            Constraint(id="constraint_b", kind="soft", statement="Prefer small blast radius"),
            Constraint(id="constraint_a", kind="hard", statement="Do not widen network access"),
        ),
        policies=(
            PolicyRef(id="policy_b", source="soc2", requirement="Log deployment access"),
            PolicyRef(id="policy_a", source="security", requirement="Enforce tenant isolation"),
        ),
        success_criteria=(
            SuccessCriterion(
                id="success_b",
                evaluation_mode="tests",
                description="Unit tests must pass",
            ),
            SuccessCriterion(
                id="success_a",
                evaluation_mode="metric_threshold",
                description="Error rate stays below threshold",
            ),
        ),
        operating_bounds=OperatingBound(
            max_seconds=20.0,
            max_steps=5,
            max_input_tokens=50,
            max_output_tokens=150,
            allow_network=True,
        ),
    )


def test_project_intent_operating_bounds_intersects_numeric_fields() -> None:
    controller_budget = Budget(
        max_wall_time_s=30.0,
        max_iterations_total=10,
        max_input_tokens=100,
        max_output_tokens=200,
    )
    intent_bounds = OperatingBound(
        max_seconds=20.0,
        max_steps=5,
        max_input_tokens=50,
        max_output_tokens=150,
        allow_network=True,
    )

    proj = project_intent_operating_bounds_to_policy_context(
        intent_bounds=intent_bounds,
        controller_budget=controller_budget,
        hard_allow_network=True,
    )

    assert proj.effective == {
        "max_seconds": 20.0,
        "max_steps": 5,
        "max_input_tokens": 50,
        "max_output_tokens": 150,
        "allow_network": True,
    }

    decision_fields = {str(d.get("field")) for d in proj.narrowing_decisions}
    assert decision_fields == {
        "max_seconds",
        "max_steps",
        "max_input_tokens",
        "max_output_tokens",
    }


def test_project_intent_operating_bounds_hard_denies_allow_network() -> None:
    controller_budget = Budget(
        max_wall_time_s=30.0,
        max_iterations_total=10,
        max_input_tokens=100,
        max_output_tokens=200,
    )
    intent_bounds = OperatingBound(
        max_seconds=20.0,
        max_steps=5,
        max_input_tokens=50,
        max_output_tokens=150,
        allow_network=True,
    )

    proj = project_intent_operating_bounds_to_policy_context(
        intent_bounds=intent_bounds,
        controller_budget=controller_budget,
        hard_allow_network=False,
    )

    assert proj.effective["allow_network"] is False
    decision_fields = {str(d.get("field")) for d in proj.narrowing_decisions}
    assert "allow_network" in decision_fields


def test_project_runtime_intent_projection_serializes_authoritative_runtime_contract() -> None:
    projection = project_runtime_intent_projection(
        intent=_intent_spec(),
        operating_bounds_effective={
            "max_seconds": 15.0,
            "max_steps": 4,
            "max_input_tokens": 40,
            "max_output_tokens": 120,
            "allow_network": False,
        },
    )

    assert projection.intent_id == "intent_123"
    assert projection.spec_version == 1
    assert projection.operating_bounds_effective["allow_network"] is False
    assert [policy["id"] for policy in projection.policies] == ["policy_a", "policy_b"]
    assert projection.success_criteria_summary is not None
    assert projection.success_criteria_summary["evaluation_modes"] == ["metric_threshold", "tests"]
    criteria = projection.success_criteria_summary["criteria"]
    assert isinstance(criteria, list)
    assert [criterion["id"] for criterion in criteria] == ["success_a", "success_b"]


def test_project_deployment_intent_projection_emits_traceable_tags() -> None:
    projection = project_deployment_intent_projection(intent=_intent_spec())

    assert projection.intent_id == "intent_123"
    assert projection.constraint_ids == ("constraint_a", "constraint_b")
    assert projection.policy_ids == ("policy_a", "policy_b")
    assert projection.success_criteria_modes == ("metric_threshold", "tests")
    assert projection.trace_tags == (
        "constraint:constraint_a",
        "constraint:constraint_b",
        "policy:policy_a",
        "policy:policy_b",
        "success_mode:metric_threshold",
        "success_mode:tests",
    )


def test_project_intent_operating_bounds_applies_intent_when_controller_budget_missing() -> None:
    controller_budget = Budget(
        max_wall_time_s=None,
        max_iterations_total=10,
        max_input_tokens=None,
        max_output_tokens=None,
    )
    intent_bounds = OperatingBound(
        max_seconds=15.0,
        max_steps=7,
        max_input_tokens=20,
        max_output_tokens=25,
        allow_network=False,
    )

    proj = project_intent_operating_bounds_to_policy_context(
        intent_bounds=intent_bounds,
        controller_budget=controller_budget,
        hard_allow_network=True,
    )

    assert proj.effective["max_seconds"] == 15.0
    assert proj.effective["max_steps"] == 7
    assert proj.effective["max_input_tokens"] == 20
    assert proj.effective["max_output_tokens"] == 25
    assert proj.effective["allow_network"] is False

    decision_fields = {str(d.get("field")) for d in proj.narrowing_decisions}
    assert decision_fields == {
        "max_seconds",
        "max_steps",
        "max_input_tokens",
        "max_output_tokens",
        "allow_network",
    }


def test_project_stage_timeout_s_min_with_intent_max_seconds() -> None:
    effective, decision = project_stage_timeout_s(stage_timeout_s=10.0, intent_max_seconds=8.0)
    assert effective == 8.0
    assert decision is not None
    assert decision["field"] == "stage_timeout_s"


def test_project_stage_timeout_s_applies_intent_when_stage_timeout_missing() -> None:
    effective, decision = project_stage_timeout_s(stage_timeout_s=None, intent_max_seconds=8.0)
    assert effective == 8.0
    assert decision is not None
    assert decision["reason_code"] == "intent.applied_stage_timeout"


def test_project_stage_timeout_s_noop_when_intent_missing() -> None:
    effective, decision = project_stage_timeout_s(stage_timeout_s=10.0, intent_max_seconds=None)
    assert effective == 10.0
    assert decision is None


def test_project_runtime_intent_projection_embeds_observability_stubs() -> None:
    op_params = {
        "spec_version": 1,
        "window": "single_run",
        "predicate_kind": "presence",
        "signals": [{"evidence_type": "reconcile_outcome", "otel_query_stub": "otel_bind_a"}],
    }
    intent = IntentSpecV1(
        intent_id="intent_obs",
        tenant_id="tenant_a",
        repo_id="repo_a",
        goal_statement="Observe via OTLP binding stubs",
        success_criteria=(
            SuccessCriterion(
                id="sc_op",
                evaluation_mode="operational_spec",
                description="Operational gate",
                params=op_params,
            ),
            SuccessCriterion(
                id="sc_metric",
                evaluation_mode="metric_threshold",
                description="Metric gate",
                params={"otel_query_stub": "metric_bind_b"},
            ),
        ),
    )
    projection = project_runtime_intent_projection(intent=intent)
    summary = projection.success_criteria_summary
    assert summary is not None
    obs = summary.get("observability")
    assert isinstance(obs, dict)
    assert obs["otel_query_stubs"] == ["metric_bind_b", "otel_bind_a"]
    assert "intent.metric_oteld_stub:metric_bind_b" in obs["intent_trace_tags"]
    assert "intent.oteld_stub:otel_bind_a" in obs["intent_trace_tags"]


def test_build_handoff_intent_ref_matches_runtime_and_deployment_projections() -> None:
    intent = _intent_spec()
    handoff = build_handoff_intent_ref(intent=intent)
    runtime_p = project_runtime_intent_projection(intent=intent)
    deploy_p = project_deployment_intent_projection(intent=intent)
    assert handoff["intent_id"] == runtime_p.intent_id == deploy_p.intent_id
    assert handoff["stable_intent_sha256"] == runtime_p.stable_intent_sha256 == deploy_p.stable_intent_sha256
    assert (
        handoff["semantic_fingerprint"] == runtime_p.intent_semantic_fingerprint == deploy_p.intent_semantic_fingerprint
    )
    assert (
        handoff["goal_text_fingerprint"]
        == runtime_p.intent_goal_text_fingerprint
        == deploy_p.intent_goal_text_fingerprint
    )
