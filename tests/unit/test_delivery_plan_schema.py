from __future__ import annotations

from akc.artifacts.validate import validate_obj


def _minimal_delivery_plan(*, status: str = "ready") -> dict:
    return {
        "run_id": "run_x",
        "tenant_id": "tenant_x",
        "repo_id": "repo_x",
        "targets": [],
        "environments": ["local", "staging", "production"],
        "delivery_paths": {"local": ["direct_apply"]},
        "required_human_inputs": [],
        "promotion_readiness": {"status": status},
    }


def test_delivery_plan_schema_accepted_when_minimal_valid() -> None:
    assert validate_obj(obj=_minimal_delivery_plan(), kind="delivery_plan", version=1) == []


def test_delivery_plan_schema_accepts_legacy_unknown_top_level_fields() -> None:
    obj = _minimal_delivery_plan()
    obj["future_compiler_field"] = {"nested": True}
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []


def test_delivery_plan_schema_rejects_non_enum_promotion_status_fail_closed() -> None:
    issues = validate_obj(
        obj=_minimal_delivery_plan(status="maybe"),
        kind="delivery_plan",
        version=1,
    )
    assert issues
    assert any("promotion_readiness" in i.path and "status" in i.message.lower() for i in issues) or any(
        "maybe" in i.message for i in issues
    )


def test_delivery_plan_schema_requires_promotion_readiness_status() -> None:
    obj = _minimal_delivery_plan()
    obj["promotion_readiness"] = {"is_promotion_ready": True}
    issues = validate_obj(obj=obj, kind="delivery_plan", version=1)
    assert issues
    assert any("status" in i.message.lower() for i in issues)


def test_delivery_plan_schema_human_input_status_enum() -> None:
    obj = _minimal_delivery_plan()
    obj["required_human_inputs"] = [
        {
            "id": "domain_name",
            "status": "pending",
            "ui_prompt": {
                "audience": "operator",
                "title": "t",
                "question": "q",
                "value_kind": "hostname",
                "sensitive": False,
            },
            "answer_binding": {"kind": "ir_node_property", "property": "domain", "scope": "listed_targets"},
        }
    ]
    issues = validate_obj(obj=obj, kind="delivery_plan", version=1)
    assert issues
    assert any("pending" in i.message or "enum" in i.message.lower() for i in issues)


def test_delivery_plan_schema_accepts_full_promotion_readiness_shape() -> None:
    obj = _minimal_delivery_plan()
    obj["promotion_readiness"] = {
        "status": "blocked",
        "blocking_inputs": ["cloud_credentials"],
        "promotion_blockers": ["cloud_credentials", "production_manual_approval_gate"],
        "production_manual_approval_required": True,
        "is_promotion_ready": False,
        "default_promotion_environment": "production",
    }
    assert validate_obj(obj=obj, kind="delivery_plan", version=1) == []
