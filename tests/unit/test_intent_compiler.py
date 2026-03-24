import json
from pathlib import Path

import pytest

from akc.compile.controller_config import Budget
from akc.intent import IntentCompilerError, compile_intent_spec


def test_compile_intent_from_goal_compat_transform() -> None:
    budget = Budget(
        max_llm_calls=4,
        max_repairs_per_step=2,
        max_iterations_total=8,
        max_wall_time_s=30.5,
        max_input_tokens=100,
        max_output_tokens=200,
    )
    intent = compile_intent_spec(
        tenant_id="tenant_a",
        repo_id="repo_1",
        goal_statement="Do the thing",
        controller_budget=budget,
    )

    assert intent.goal_statement == "Do the thing"
    assert intent.derived_from_goal_text is True
    assert intent.tenant_id == "tenant_a"
    assert intent.repo_id == "repo_1"
    assert intent.spec_version == 1
    assert len(intent.objectives) == 1
    obj = intent.objectives[0]
    assert obj.id == "objective_default"
    assert obj.statement == "Do the thing"

    assert intent.operating_bounds is not None
    b = intent.operating_bounds
    assert b.max_seconds == 30.5
    assert b.max_steps == 8
    assert b.max_input_tokens == 100
    assert b.max_output_tokens == 200
    assert b.allow_network is False


def test_compile_intent_from_file_validates_scope_version_and_bounds(tmp_path: Path) -> None:
    intent_file = tmp_path / "intent.json"
    intent_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "spec_version": 1,
                "intent_id": "intent_file_1",
                "tenant_id": "tenant_a",
                "repo_id": "repo_1",
                "status": "draft",
                "goal_statement": "File-goal",
                "derived_from_goal_text": False,
                "operating_bounds": {
                    "max_seconds": 10.0,
                    "max_steps": 2,
                    "max_input_tokens": 50,
                    "max_output_tokens": 60,
                    "allow_network": False,
                },
                "objectives": [],
                "constraints": [],
                "policies": [],
                "success_criteria": [],
                "assumptions": [],
                "risk_notes": [],
                "tags": [],
                "metadata": None,
                "created_at_ms": 1,
                "updated_at_ms": 2,
            }
        ),
        encoding="utf-8",
    )

    budget = Budget(
        max_llm_calls=4,
        max_repairs_per_step=2,
        max_iterations_total=4,
        max_wall_time_s=20.0,
        max_input_tokens=100,
        max_output_tokens=200,
    )

    intent = compile_intent_spec(
        tenant_id="tenant_a",
        repo_id="repo_1",
        intent_file=intent_file,
        controller_budget=budget,
    )
    assert intent.goal_statement == "File-goal"
    assert intent.operating_bounds is not None
    assert intent.operating_bounds.max_seconds == 10.0
    assert intent.operating_bounds.max_steps == 2

    with pytest.raises(IntentCompilerError):
        compile_intent_spec(
            tenant_id="tenant_wrong",
            repo_id="repo_1",
            intent_file=intent_file,
            controller_budget=budget,
        )

    with pytest.raises(IntentCompilerError):
        compile_intent_spec(
            tenant_id="tenant_a",
            repo_id="repo_1",
            intent_file=tmp_path / "missing.json",
            controller_budget=budget,
        )

    bad_version_file = tmp_path / "intent_bad_version.json"
    bad_version_file.write_text(
        json.dumps(
            {
                "spec_version": 2,
                "intent_id": "x",
                "tenant_id": "tenant_a",
                "repo_id": "repo_1",
                "status": "draft",
                "goal_statement": "x",
                "operating_bounds": {"max_seconds": 1.0, "max_steps": 1, "allow_network": False},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(IntentCompilerError):
        compile_intent_spec(
            tenant_id="tenant_a",
            repo_id="repo_1",
            intent_file=bad_version_file,
            controller_budget=budget,
        )

    bad_bounds_file = tmp_path / "intent_bad_bounds.json"
    # `max_steps` must be an integer, not a string.
    bad_bounds_file.write_text(
        json.dumps(
            {
                "spec_version": 1,
                "schema_version": 1,
                "intent_id": "x",
                "tenant_id": "tenant_a",
                "repo_id": "repo_1",
                "status": "draft",
                "goal_statement": "x",
                "operating_bounds": {"max_seconds": 1.0, "max_steps": "3", "allow_network": False},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(IntentCompilerError):
        compile_intent_spec(
            tenant_id="tenant_a",
            repo_id="repo_1",
            intent_file=bad_bounds_file,
            controller_budget=budget,
        )

    # Exceed controller budget: intent max_steps=5 > budget.max_iterations_total=4.
    exceed_bounds_file = tmp_path / "intent_exceed_bounds.json"
    exceed_bounds_file.write_text(
        json.dumps(
            {
                "spec_version": 1,
                "schema_version": 1,
                "intent_id": "x",
                "tenant_id": "tenant_a",
                "repo_id": "repo_1",
                "status": "draft",
                "goal_statement": "x",
                "operating_bounds": {"max_seconds": 1.0, "max_steps": 5, "allow_network": False},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(IntentCompilerError):
        compile_intent_spec(
            tenant_id="tenant_a",
            repo_id="repo_1",
            intent_file=exceed_bounds_file,
            controller_budget=budget,
        )
