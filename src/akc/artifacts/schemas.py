from __future__ import annotations

from typing import Any, Final, Literal

from akc.artifacts.contracts import ARTIFACT_SCHEMA_VERSION, schema_id_for

SchemaKind = Literal[
    "manifest",
    "plan_state",
    "execution_stage",
    "verifier_result",
    "operational_validity_report",
    "operational_assurance_result",
    "operational_evidence_window",
    "runtime_bundle",
    "delivery_plan",
    "runtime_evidence_stream",
    "run_trace_spans",
    "run_cost_attribution",
    "replay_decisions",
    "living_drift_report",
    "recompile_triggers",
    "control_plane_envelope",
    "promotion_packet",
    "reliability_scoreboard",
    "autopilot_decision",
    "autopilot_human_escalation",
]


def _base_envelope(*, kind: SchemaKind) -> dict[str, Any]:
    # Envelope fields are optional for backward compatibility (older artifacts
    # may not include them). When present, they must match.
    return {
        "schema_version": {
            "type": "integer",
            "minimum": 1,
            "default": ARTIFACT_SCHEMA_VERSION,
        },
        "schema_id": {
            "type": "string",
            "default": schema_id_for(kind=kind, version=ARTIFACT_SCHEMA_VERSION),
        },
    }


MANIFEST_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="manifest", version=1),
    "title": "AKC run manifest",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="manifest"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "name": {"type": "string", "minLength": 1},
        "artifacts": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "path": {"type": "string", "minLength": 1},
                    "media_type": {"type": "string", "minLength": 1},
                    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                    "size_bytes": {"type": "integer", "minimum": 0},
                    "metadata": {"type": ["object", "null"]},
                },
                "required": ["path", "media_type", "sha256", "size_bytes"],
            },
        },
        "metadata": {"type": ["object", "null"]},
    },
    "required": ["tenant_id", "repo_id", "name", "artifacts"],
}


PLAN_STATE_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="plan_state", version=1),
    "title": "AKC plan state",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="plan_state"),
        "id": {"type": "string", "minLength": 1},
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "goal": {"type": "string", "minLength": 1},
        "status": {"type": "string", "enum": ["active", "completed", "abandoned"]},
        "created_at_ms": {"type": "integer", "minimum": 0},
        "updated_at_ms": {"type": "integer", "minimum": 0},
        "steps": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "id": {"type": "string", "minLength": 1},
                    "title": {"type": "string", "minLength": 1},
                    "status": {
                        "type": "string",
                        "enum": ["pending", "in_progress", "done", "failed", "skipped"],
                    },
                    "order_idx": {"type": "integer", "minimum": 0},
                    "started_at_ms": {"type": ["integer", "null"], "minimum": 0},
                    "finished_at_ms": {"type": ["integer", "null"], "minimum": 0},
                    "notes": {"type": ["string", "null"]},
                    "inputs": {"type": "object"},
                    "outputs": {"type": "object"},
                },
                "required": ["id", "title", "status", "order_idx", "inputs", "outputs"],
            },
        },
        "next_step_id": {"type": ["string", "null"]},
        "budgets": {"type": "object"},
        "last_feedback": {"type": "object"},
    },
    "required": [
        "id",
        "tenant_id",
        "repo_id",
        "goal",
        "status",
        "created_at_ms",
        "updated_at_ms",
        "steps",
        "budgets",
        "last_feedback",
    ],
}


EXECUTION_STAGE_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="execution_stage", version=1),
    "title": "AKC execution stage evidence",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="execution_stage"),
        "plan_id": {"type": "string", "minLength": 1},
        "step_id": {"type": "string", "minLength": 1},
        "stage": {"type": ["string", "null"]},
        "command": {"type": "array", "items": {"type": "string"}},
        "exit_code": {"type": ["integer", "null"]},
        "duration_ms": {"type": ["integer", "null"], "minimum": 0},
        "stdout": {"type": ["string", "null"]},
        "stderr": {"type": ["string", "null"]},
    },
    "required": ["plan_id", "step_id", "command"],
}


VERIFIER_RESULT_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="verifier_result", version=1),
    "title": "AKC verifier result",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="verifier_result"),
        "scope": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "tenant_id": {"type": "string", "minLength": 1},
                "repo_id": {"type": "string", "minLength": 1},
            },
            "required": ["tenant_id", "repo_id"],
        },
        "plan_id": {"type": "string", "minLength": 1},
        "step_id": {"type": "string", "minLength": 1},
        "passed": {"type": "boolean"},
        "checked_at_ms": {"type": "integer", "minimum": 0},
        "policy": {"type": "object"},
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "code": {"type": "string", "minLength": 1},
                    "message": {"type": "string", "minLength": 1},
                    "severity": {"type": "string", "enum": ["error", "warning"]},
                    "evidence": {"type": ["object", "null"]},
                },
                "required": ["code", "message", "severity"],
            },
        },
    },
    "required": ["scope", "plan_id", "step_id", "passed", "checked_at_ms", "policy", "findings"],
}


_INTENT_REF_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "intent_id": {"type": "string", "minLength": 1},
        "stable_intent_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        "semantic_fingerprint": {"type": "string", "pattern": "^[0-9a-f]{16}$"},
        "goal_text_fingerprint": {"type": "string", "pattern": "^[0-9a-f]{16}$"},
    },
    "required": [
        "intent_id",
        "stable_intent_sha256",
        "semantic_fingerprint",
        "goal_text_fingerprint",
    ],
}

_ARTIFACT_REF_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "path": {"type": "string", "minLength": 1},
        "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    },
    "required": ["path", "sha256"],
}

_OPERATIONAL_PREDICATE_RESULT_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "predicate_kind": {"type": "string", "enum": ["threshold", "presence"]},
        "signal_key": {"type": "string", "minLength": 1},
        "passed": {"type": "boolean"},
        "observed_value": {"type": ["number", "null"]},
        "details": {"type": ["object", "null"], "additionalProperties": True},
    },
    "required": ["predicate_kind", "passed"],
}


OPERATIONAL_VALIDITY_REPORT_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="operational_validity_report", version=1),
    "title": "AKC operational validity report",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="operational_validity_report"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "run_id": {"type": "string", "minLength": 1},
        "runtime_run_id": {"type": ["string", "null"]},
        "evaluated_at_ms": {"type": "integer", "minimum": 0},
        "passed": {"type": "boolean"},
        "operational_spec_version": {"type": "integer", "minimum": 1},
        "success_criterion_id": {"type": ["string", "null"]},
        "intent_ref": _INTENT_REF_SCHEMA,
        "runtime_bundle_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                **dict(_ARTIFACT_REF_SCHEMA["properties"]),
                "schema_version": {"type": "integer", "minimum": 1},
            },
            "required": ["path", "sha256"],
        },
        "runtime_evidence_ref": _ARTIFACT_REF_SCHEMA,
        "predicate_results": {
            "type": "array",
            "items": _OPERATIONAL_PREDICATE_RESULT_SCHEMA,
        },
        "expected_evidence_types": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "notes": {"type": ["string", "null"]},
    },
    "required": [
        "tenant_id",
        "repo_id",
        "run_id",
        "evaluated_at_ms",
        "passed",
        "operational_spec_version",
    ],
}

OPERATIONAL_ASSURANCE_RESULT_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="operational_assurance_result", version=1),
    "title": "AKC operational assurance result",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="operational_assurance_result"),
        "run_id": {"type": "string", "minLength": 1},
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "checked_at_ms": {"type": "integer", "minimum": 0},
        "enforcement_mode": {"type": "string", "enum": ["advisory", "blocking"]},
        "passed": {"type": "boolean"},
        "advisory_only": {"type": "boolean"},
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "code": {"type": "string", "minLength": 1},
                    "message": {"type": "string", "minLength": 1},
                    "severity": {"type": "string", "enum": ["error", "warning"]},
                    "evidence": {"type": ["object", "null"]},
                },
                "required": ["code", "message", "severity"],
            },
        },
        "provider_results": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "stub_id": {"type": "string", "minLength": 1},
                    "provider": {"type": "string", "minLength": 1},
                    "status": {"type": "string", "enum": ["ok", "error", "missing"]},
                    "details": {"type": ["object", "null"]},
                },
                "required": ["stub_id", "provider", "status"],
            },
        },
        "attestation_fingerprint_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    },
    "required": [
        "run_id",
        "tenant_id",
        "repo_id",
        "checked_at_ms",
        "enforcement_mode",
        "passed",
        "advisory_only",
        "findings",
        "provider_results",
        "attestation_fingerprint_sha256",
    ],
}


OPERATIONAL_EVIDENCE_WINDOW_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="operational_evidence_window", version=1),
    "title": "AKC operational evidence window (rollup)",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="operational_evidence_window"),
        "window_start_ms": {"type": "integer", "minimum": 0},
        "window_end_ms": {"type": "integer", "minimum": 0},
        "runtime_evidence_exports": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "path": {"type": "string", "minLength": 1},
                    "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                },
                "required": ["path", "sha256"],
            },
        },
        "notes": {"type": ["string", "null"]},
    },
    "required": ["window_start_ms", "window_end_ms", "runtime_evidence_exports"],
}


RUNTIME_BUNDLE_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="runtime_bundle", version=1),
    "title": "AKC runtime bundle",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="runtime_bundle"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "run_id": {"type": "string", "minLength": 1},
        "intent_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "intent_id": {"type": "string", "minLength": 1},
                "stable_intent_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "semantic_fingerprint": {"type": "string", "pattern": "^[0-9a-f]{16}$"},
                "goal_text_fingerprint": {"type": "string", "pattern": "^[0-9a-f]{16}$"},
            },
            "required": [
                "intent_id",
                "stable_intent_sha256",
                "semantic_fingerprint",
                "goal_text_fingerprint",
            ],
        },
        "intent_policy_projection": {"type": "object"},
        "system_ir_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "fingerprint": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "format_version": {"type": "string", "minLength": 1},
                "schema_version": {"type": "integer", "minimum": 1},
            },
        },
        "system_ir": {"type": "object"},
        "referenced_ir_nodes": {"type": "array", "items": {"type": "object"}},
        "referenced_contracts": {"type": "array", "items": {"type": "object"}},
        "spec_hashes": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "orchestration_spec_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "coordination_spec_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": [
                "orchestration_spec_sha256",
                "coordination_spec_sha256",
            ],
        },
        "deployment_intents": {"type": "array", "items": {"type": "object"}},
        "runtime_policy_envelope": {"type": "object"},
        "knowledge_layer_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "knowledge_hub_node_id": {"type": "string", "minLength": 1},
                "knowledge_semantic_fingerprint_16": {"type": ["string", "null"]},
                "knowledge_provenance_fingerprint_16": {"type": ["string", "null"]},
                "persisted_snapshot_relpath": {"type": ["string", "null"]},
                "knowledge_assertion_ids": {"type": "array", "items": {"type": "string"}},
            },
        },
    },
    "required": [
        "tenant_id",
        "repo_id",
        "run_id",
        "referenced_ir_nodes",
        "referenced_contracts",
        "spec_hashes",
        "deployment_intents",
        "runtime_policy_envelope",
    ],
}


# v2: same JSON shape as v1; version bump marks bundles emitted with IR spine handoff
# semantics (system_ir_ref + reconciler hash expectations). Consumers must accept v1 and v2.
RUNTIME_BUNDLE_V2: Final[dict[str, Any]] = {
    **RUNTIME_BUNDLE_V1,
    "$id": schema_id_for(kind="runtime_bundle", version=2),
    "title": "AKC runtime bundle (v2)",
}

# v3: additive — explicit embed_system_ir when the bundle may carry inline system_ir
# (air-gapped / debugging); envelope + apply_schema_envelope remain the version gate.
RUNTIME_BUNDLE_V3: Final[dict[str, Any]] = {
    **RUNTIME_BUNDLE_V2,
    "$id": schema_id_for(kind="runtime_bundle", version=3),
    "title": "AKC runtime bundle (v3)",
    "properties": {
        **dict(RUNTIME_BUNDLE_V2["properties"]),
        "embed_system_ir": {"type": "boolean"},
    },
}

_COORDINATION_REF_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "path": {"type": "string", "minLength": 1},
        "fingerprint": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    },
    "required": ["path", "fingerprint"],
}

# v4: additive — coordination handoff refs and optional inline coordination (multi-agent substrate).
RUNTIME_BUNDLE_V4: Final[dict[str, Any]] = {
    **RUNTIME_BUNDLE_V3,
    "$id": schema_id_for(kind="runtime_bundle", version=4),
    "title": "AKC runtime bundle (v4)",
    "properties": {
        **dict(RUNTIME_BUNDLE_V3["properties"]),
        "coordination_ref": _COORDINATION_REF_SCHEMA,
        "coordination_spec": {"type": "object"},
        "reconcile_desired_state_source": {"type": "string", "enum": ["ir", "deployment_intents"]},
        "reconcile_deploy_targets_from_ir_only": {"type": "boolean"},
        "deployment_intents_ir_alignment": {"type": "string", "enum": ["off", "strict"]},
        "deployment_provider_contract": {"type": "object"},
        "workflow_execution_contract": {"type": "object"},
        "coordination_execution_contract": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "parallel_dispatch_enabled": {"type": "boolean"},
                "max_in_flight_steps": {"type": "integer", "minimum": 1},
                "max_in_flight_per_role": {"type": "integer", "minimum": 1},
                "completion_fold_order": {"type": "string", "enum": ["coordination_step_id"]},
            },
        },
    },
}

# Default schema_version for newly emitted runtime bundles (compile/runtime handoff).
RUNTIME_BUNDLE_SCHEMA_VERSION: Final[int] = 4

# Structured questions for non-technical / guided UIs: only `status: "missing"` rows are emitted
# today; bindings point at IR properties so re-compile stays authoritative (no parallel plan model).
_DELIVERY_PLAN_UI_PROMPT_V1: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "audience": {
            "type": "string",
            "description": "Who the copy is written for (UI may vary tone).",
            "enum": ["non_technical", "operator", "developer"],
        },
        "title": {"type": "string", "minLength": 1},
        "question": {"type": "string", "minLength": 1},
        "help_text": {"type": "string"},
        "value_kind": {
            "type": "string",
            "minLength": 1,
            "description": "Opaque hint for input widgets (e.g. hostname, url_path).",
        },
        "sensitive": {"type": "boolean"},
    },
    "required": ["audience", "title", "question", "value_kind", "sensitive"],
}

_DELIVERY_PLAN_ANSWER_BINDING_V1: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "kind": {
            "type": "string",
            "description": "Where answers are written on re-ingest / re-compile.",
            "enum": ["ir_node_property"],
        },
        "property": {"type": "string", "minLength": 1},
        "scope": {
            "type": "string",
            "enum": ["listed_targets", "repo", "tenant"],
        },
        "target_ids": {"type": "array", "items": {"type": "string", "minLength": 1}},
    },
    "required": ["kind", "property", "scope"],
}

_DELIVERY_PLAN_REQUIRED_HUMAN_INPUT_V1: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "id": {"type": "string", "minLength": 1},
        "status": {
            "type": "string",
            "description": "Emit step lists only missing items; satisfied rows are omitted.",
            "enum": ["missing", "satisfied"],
        },
        "blocking_for": {
            "type": "array",
            "items": {"type": "string", "minLength": 1},
            "description": "Environments where this gap blocks promotion-style rollout.",
        },
        "reason": {"type": "string"},
        "ask_order": {
            "type": "integer",
            "minimum": 0,
            "description": "Stable ordering for question wizards (lower first).",
        },
        "context": {
            "type": "object",
            "additionalProperties": True,
            "description": "UI-only detail (e.g. secret name lists); compile remains driven by IR + plan JSON.",
        },
        "ui_prompt": _DELIVERY_PLAN_UI_PROMPT_V1,
        "answer_binding": _DELIVERY_PLAN_ANSWER_BINDING_V1,
    },
    "required": ["id", "status", "ui_prompt", "answer_binding"],
}

DELIVERY_PLAN_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="delivery_plan", version=1),
    "title": "AKC delivery plan",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="delivery_plan"),
        "run_id": {"type": "string", "minLength": 1},
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "inputs_fingerprint": {"type": "string", "minLength": 1},
        "targets": {"type": "array", "items": {"type": "object"}},
        "environments": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "environment_model": {"type": "array", "items": {"type": "object"}},
        "delivery_paths": {"type": "object"},
        "operational_profiles": {"type": "object"},
        "required_human_inputs": {
            "type": "array",
            "items": _DELIVERY_PLAN_REQUIRED_HUMAN_INPUT_V1,
        },
        "promotion_readiness": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "status": {"type": "string", "enum": ["ready", "blocked"]},
                "blocking_inputs": {"type": "array", "items": {"type": "string"}},
                "promotion_blockers": {
                    "type": "array",
                    "items": {"type": "string", "minLength": 1},
                    "description": "Human input ids plus compile-time gates (e.g. production_manual_approval_gate).",
                },
                "production_manual_approval_required": {
                    "type": "boolean",
                    "description": "True when the production environment model requires manual approval before deploy.",
                },
                "is_promotion_ready": {"type": "boolean"},
                "default_promotion_environment": {"type": "string", "minLength": 1},
            },
            "required": ["status"],
        },
    },
    "required": [
        "run_id",
        "tenant_id",
        "repo_id",
        "targets",
        "environments",
        "delivery_paths",
        "required_human_inputs",
        "promotion_readiness",
    ],
}


_TRACE_SPAN_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "trace_id": {"type": "string", "minLength": 1},
        "span_id": {"type": "string", "minLength": 1},
        "parent_span_id": {"type": ["string", "null"]},
        "name": {"type": "string", "minLength": 1},
        "kind": {"type": "string", "minLength": 1},
        "start_time_unix_nano": {"type": "integer", "minimum": 1},
        "end_time_unix_nano": {"type": "integer", "minimum": 1},
        "attributes": {"type": ["object", "null"]},
        "status": {"type": "string", "minLength": 1},
    },
    "required": [
        "trace_id",
        "span_id",
        "name",
        "kind",
        "start_time_unix_nano",
        "end_time_unix_nano",
        "status",
    ],
}


_RECOMPILE_TRIGGER_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "kind": {
            "type": "string",
            "enum": [
                "intent_semantic_changed",
                "intent_stable_changed",
                "knowledge_semantic_changed",
                "knowledge_provenance_changed",
                "operational_validity_failed",
                "acceptance_criterion_failed",
            ],
        },
        "details": {"type": "object", "additionalProperties": True},
    },
    "required": ["kind", "details"],
}

CONVERGENCE_CERTIFICATE_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": "akc:convergence_certificate:v1",
    "title": "AKC convergence certificate (runtime evidence payload)",
    "description": (
        "Versioned payload carried under runtime_evidence_stream items with "
        "evidence_type=convergence_certificate. Per-resource rows include resource_id; "
        "aggregate rows use resource_id=__runtime_aggregate__ and aggregate=true."
    ),
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "certificate_schema_version": {"type": "integer", "const": 1},
        "resource_id": {"type": "string", "minLength": 1},
        "aggregate": {"type": "boolean"},
        "desired_hash": {"type": "string"},
        "observed_hash": {"type": "string"},
        "health": {"type": "string", "enum": ["healthy", "degraded", "unknown", "failed"]},
        "attempts": {"type": "integer", "minimum": 1},
        "window_ms": {"type": "integer", "minimum": 0},
        "provider_id": {"type": "string", "minLength": 1},
        "policy_mode": {"type": "string", "minLength": 1},
        "converged": {"type": "boolean"},
    },
    "required": [
        "certificate_schema_version",
        "resource_id",
        "desired_hash",
        "observed_hash",
        "health",
        "attempts",
        "window_ms",
        "provider_id",
        "policy_mode",
        "converged",
    ],
}


def _runtime_evidence_item_schema(*, evidence_type: str, required_payload_keys: list[str]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "evidence_type": {"const": evidence_type},
            "timestamp": {"type": "integer", "minimum": 0},
            "runtime_run_id": {"type": "string", "minLength": 1},
            "payload": {
                "type": "object",
                "additionalProperties": True,
                "required": required_payload_keys,
            },
        },
        "required": ["evidence_type", "timestamp", "runtime_run_id", "payload"],
    }


RUNTIME_EVIDENCE_STREAM_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="runtime_evidence_stream", version=1),
    "title": "AKC runtime evidence stream",
    "type": "array",
    "items": {
        "oneOf": [
            _runtime_evidence_item_schema(
                evidence_type="action_decision",
                required_payload_keys=["action_id", "decision"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="transition_application",
                required_payload_keys=["action_id", "transition"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="retry_budget",
                required_payload_keys=["action_id", "retry_count", "budget_burn"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="reconcile_outcome",
                required_payload_keys=["resource_id", "operation_type", "applied", "health_status"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="rollback_chain",
                required_payload_keys=["resource_id", "chain"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="provider_capability_snapshot",
                required_payload_keys=["provider_id", "mutation_mode", "rollback_mode", "rollback_determinism"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="rollback_attempt",
                required_payload_keys=["resource_id", "rollback_target_hash"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="rollback_result",
                required_payload_keys=["resource_id", "rollback_outcome"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="terminal_health",
                required_payload_keys=["resource_id", "health_status"],
            ),
            _runtime_evidence_item_schema(
                evidence_type="reconcile_resource_status",
                required_payload_keys=[
                    "resource_id",
                    "converged",
                    "conditions",
                    "observed_hash",
                    "health_status",
                ],
            ),
            _runtime_evidence_item_schema(
                evidence_type="convergence_certificate",
                required_payload_keys=[
                    "resource_id",
                    "certificate_schema_version",
                    "desired_hash",
                    "observed_hash",
                    "health",
                    "attempts",
                    "window_ms",
                    "provider_id",
                    "policy_mode",
                    "converged",
                ],
            ),
            _runtime_evidence_item_schema(
                evidence_type="delivery_lifecycle",
                required_payload_keys=["event"],
            ),
        ]
    },
}


RUN_TRACE_SPANS_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="run_trace_spans", version=1),
    "title": "AKC run trace spans",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="run_trace_spans"),
        "run_id": {"type": "string", "minLength": 1},
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "spans": {"type": "array", "items": _TRACE_SPAN_SCHEMA},
    },
    "required": ["run_id", "tenant_id", "repo_id", "spans"],
}


RUN_COST_ATTRIBUTION_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="run_cost_attribution", version=1),
    "title": "AKC run cost attribution",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="run_cost_attribution"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "run_id": {"type": "string", "minLength": 1},
        "currency": {"type": "string", "minLength": 1},
        "pricing_version": {"type": "string", "minLength": 1},
        "llm_calls": {"type": "integer", "minimum": 0},
        "tool_calls": {"type": "integer", "minimum": 0},
        "input_tokens": {"type": "integer", "minimum": 0},
        "output_tokens": {"type": "integer", "minimum": 0},
        "total_tokens": {"type": "integer", "minimum": 0},
        "estimated_cost_usd": {"type": "number", "minimum": 0},
        "repair_iterations": {"type": "integer", "minimum": 0},
        "wall_time_ms": {"type": "integer", "minimum": 0},
        "budget": {"type": ["object", "null"]},
        "cost_rates": {"type": ["object", "null"]},
        "by_pass": {"type": ["object", "null"], "additionalProperties": True},
        "by_component": {"type": ["object", "null"], "additionalProperties": True},
        "tenant_totals": {"type": ["object", "null"]},
    },
    "required": [
        "tenant_id",
        "repo_id",
        "run_id",
        "currency",
        "pricing_version",
        "llm_calls",
        "tool_calls",
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "estimated_cost_usd",
        "repair_iterations",
        "wall_time_ms",
    ],
}


REPLAY_DECISIONS_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="replay_decisions", version=1),
    "title": "AKC replay decisions",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="replay_decisions"),
        "run_id": {"type": "string", "minLength": 1},
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "replay_source_run_id": {"type": ["string", "null"]},
        "replay_mode": {"type": "string", "minLength": 1},
        "decisions": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "pass_name": {"type": "string", "minLength": 1},
                    "replay_mode": {"type": "string", "minLength": 1},
                    "should_call_model": {"type": "boolean"},
                    "should_call_tools": {"type": "boolean"},
                    "trigger_reason": {"type": "string", "minLength": 1},
                    "trigger": {"anyOf": [{"type": "null"}, _RECOMPILE_TRIGGER_SCHEMA]},
                    "inputs_snapshot": {"type": "object", "additionalProperties": True},
                },
                "required": [
                    "pass_name",
                    "replay_mode",
                    "should_call_model",
                    "should_call_tools",
                    "trigger_reason",
                    "inputs_snapshot",
                ],
            },
        },
    },
    "required": ["run_id", "tenant_id", "repo_id", "replay_mode", "decisions"],
}


LIVING_DRIFT_REPORT_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="living_drift_report", version=1),
    "title": "AKC living drift report",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="living_drift_report"),
        "checked_at_ms": {"type": "integer", "minimum": 0},
        "baseline_path": {"type": ["string", "null"]},
        "baseline_manifest_sha256": {"type": ["string", "null"], "pattern": "^[0-9a-f]{64}$"},
        "scope": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "tenant_id": {"type": "string", "minLength": 1},
                "repo_id": {"type": "string", "minLength": 1},
            },
            "required": ["tenant_id", "repo_id"],
        },
        "findings": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "kind": {
                        "type": "string",
                        "enum": [
                            "changed_sources",
                            "changed_outputs",
                            "missing_manifest",
                            "changed_intent",
                            "changed_knowledge_semantic",
                            "changed_knowledge_provenance",
                            "operational_validity_failed",
                        ],
                    },
                    "severity": {"type": "string", "enum": ["low", "med", "high"]},
                    "details": {"type": "object", "additionalProperties": True},
                },
                "required": ["kind", "severity", "details"],
            },
        },
    },
    "required": ["scope", "findings"],
}


RECOMPILE_TRIGGERS_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="recompile_triggers", version=1),
    "title": "AKC recompile triggers",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="recompile_triggers"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "run_id": {"type": ["string", "null"]},
        "check_id": {"type": ["string", "null"]},
        "checked_at_ms": {"type": "integer", "minimum": 0},
        "source": {"type": ["string", "null"]},
        "triggers": {"type": "array", "items": _RECOMPILE_TRIGGER_SCHEMA},
    },
    "required": ["tenant_id", "repo_id", "checked_at_ms", "triggers"],
}


CONTROL_PLANE_ENVELOPE_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="control_plane_envelope", version=1),
    "title": "AKC run manifest control plane envelope",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="control_plane_envelope"),
        "stable_intent_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        "runtime_run_id": {"type": ["string", "null"]},
        "runtime_events": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
        "policy_decisions": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
        "runtime_evidence_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["path", "sha256"],
        },
        "policy_decisions_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["path", "sha256"],
        },
        "coordination_audit_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["path", "sha256"],
        },
        "replay_decisions_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["path", "sha256"],
        },
        "recompile_triggers_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["path", "sha256"],
        },
        "operational_assurance_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["path", "sha256"],
        },
        "governance_profile_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "path": {"type": "string", "minLength": 1},
                "sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            },
            "required": ["path", "sha256"],
        },
        # Policy-as-code provenance (additive; optional on older manifests).
        "policy_bundle_id": {"type": "string", "minLength": 1},
        "policy_git_sha": {"type": "string", "minLength": 1},
        "rego_pack_version": {"type": "string", "minLength": 1},
        # Operations index tags (additive); when present on upsert, replaces DB labels for the run.
        "run_labels": {
            "type": "object",
            "maxProperties": 32,
            "additionalProperties": {"type": "string", "minLength": 1},
        },
    },
}


PROMOTION_PACKET_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="promotion_packet", version=1),
    "title": "AKC promotion packet",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="promotion_packet"),
        "packet_version": {"type": "integer", "minimum": 1},
        "run_ref": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "tenant_id": {"type": "string", "minLength": 1},
                "repo_id": {"type": "string", "minLength": 1},
                "run_id": {"type": "string", "minLength": 1},
                "step_id": {"type": "string", "minLength": 1},
            },
            "required": ["tenant_id", "repo_id", "run_id", "step_id"],
        },
        "intent_ref": _INTENT_REF_SCHEMA,
        "promotion_mode": {"type": "string", "enum": ["artifact_only", "staged_apply", "live_apply"]},
        "promotion_state": {"type": "string", "enum": ["artifact_only", "staged_apply", "live_apply"]},
        "patch_hash_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
        "touched_paths": {"type": "array", "items": {"type": "string", "minLength": 1}},
        "required_tests": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "properties": {
                    "stage": {"type": "string"},
                    "command": {"type": "array", "items": {"type": "string"}},
                    "exit_code": {"type": ["integer", "null"]},
                    "passed": {"type": ["boolean", "null"]},
                },
                "required": ["stage", "command"],
            },
        },
        "verifier_result": {"type": ["object", "null"]},
        "policy_decision_trace": {"type": "array", "items": {"type": "object", "additionalProperties": True}},
        "policy_allow_decision": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "allowed": {"type": "boolean"},
                "action": {"type": "string"},
                "reason": {"type": "string"},
                "token_id": {"type": "string"},
            },
            "required": ["allowed"],
        },
        "compile_apply_attestation": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "compile_realization_mode": {"type": "string", "enum": ["artifact_only", "scoped_apply"]},
                "applied": {"type": "boolean"},
                "apply_decision_token_id": {"type": "string"},
                "policy_allow_decision": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": {
                        "allowed": {"type": "boolean"},
                        "action": {"type": "string"},
                        "reason": {"type": "string"},
                        "token_id": {"type": "string"},
                    },
                    "required": ["allowed"],
                },
                "patch_fingerprint_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                "scope_root": {"type": ["string", "null"]},
                "touched_paths": {"type": "array", "items": {"type": "string", "minLength": 1}},
            },
        },
        "apply_target_metadata": {"type": "object", "additionalProperties": True},
        "issued_at_ms": {"type": "integer", "minimum": 0},
        "packet_signature_sha256": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
    },
    "required": [
        "packet_version",
        "run_ref",
        "intent_ref",
        "promotion_mode",
        "promotion_state",
        "patch_hash_sha256",
        "touched_paths",
        "required_tests",
        "policy_decision_trace",
        "policy_allow_decision",
        "apply_target_metadata",
        "issued_at_ms",
        "packet_signature_sha256",
    ],
}


RELIABILITY_SCOREBOARD_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="reliability_scoreboard", version=1),
    "title": "AKC reliability scoreboard (controller KPIs)",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="reliability_scoreboard"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "window_start_ms": {"type": "integer", "minimum": 0},
        "window_end_ms": {"type": "integer", "minimum": 0},
        "kpi": {
            "type": "object",
            "additionalProperties": True,
            "properties": {
                "policy_compliance_rate": {"type": "number", "minimum": 0, "maximum": 1},
                "rollouts_total": {"type": "integer", "minimum": 0},
                "rollouts_with_rollback": {"type": "integer", "minimum": 0},
                "rollbacks_total": {"type": "integer", "minimum": 0},
                "convergence_latency_ms_avg": {"type": "number", "minimum": 0},
                "mttr_like_repair_latency_ms_avg": {"type": "number", "minimum": 0},
                "failed_promotions_prevented": {"type": "integer", "minimum": 0},
            },
        },
        "notes": {"type": ["string", "null"]},
    },
    "required": ["tenant_id", "repo_id", "window_start_ms", "window_end_ms", "kpi"],
}


BUDGET_STATE_JSON_SCHEMA: Final[dict[str, Any]] = {
    "type": "object",
    "additionalProperties": True,
    "required": [
        "window_start_ms",
        "mutations_count",
        "rollbacks_count",
        "consecutive_failures",
        "active_rollouts",
        "human_escalation_required",
        "cooldown_until_ms",
    ],
    "properties": {
        "window_start_ms": {"type": "integer", "minimum": 0},
        "mutations_count": {"type": "integer", "minimum": 0},
        "rollbacks_count": {"type": "integer", "minimum": 0},
        "consecutive_failures": {"type": "integer", "minimum": 0},
        "active_rollouts": {"type": "integer", "minimum": 0},
        "human_escalation_required": {"type": "boolean"},
        "cooldown_until_ms": {"type": "integer", "minimum": 0},
    },
}


AUTOPILOT_DECISION_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="autopilot_decision", version=1),
    "title": "AKC runtime autopilot decision (control-plane friendly)",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="autopilot_decision"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "controller_id": {"type": "string", "minLength": 1},
        "env_profile": {"type": "string", "minLength": 1},
        "decision_at_ms": {"type": "integer", "minimum": 0},
        "attempt_id": {"type": "string", "minLength": 1},
        "decision": {
            "type": "string",
            "minLength": 1,
            "description": "e.g. skip_not_lease_holder, compile_failed, escalation_hold, runtime_rollout",
        },
        "budget_state": BUDGET_STATE_JSON_SCHEMA,
        "lease_denied_streak": {"type": "integer", "minimum": 0},
        "lease": {"type": "object", "additionalProperties": True},
        "lease_backend": {"type": "string"},
        "lease_namespace": {"type": ["string", "null"]},
        "scope_name": {"type": "string"},
        "reason": {"type": "string"},
        "compile_inputs": {"type": "object", "additionalProperties": True},
        "compile_apply": {"type": "object", "additionalProperties": True},
        "runtime_outcome": {"type": "object", "additionalProperties": True},
    },
    "required": [
        "tenant_id",
        "repo_id",
        "controller_id",
        "decision_at_ms",
        "attempt_id",
        "decision",
        "budget_state",
    ],
}


AUTOPILOT_HUMAN_ESCALATION_V1: Final[dict[str, Any]] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "$id": schema_id_for(kind="autopilot_human_escalation", version=1),
    "title": "AKC runtime autopilot human escalation marker",
    "type": "object",
    "additionalProperties": True,
    "properties": {
        **_base_envelope(kind="autopilot_human_escalation"),
        "tenant_id": {"type": "string", "minLength": 1},
        "repo_id": {"type": "string", "minLength": 1},
        "generated_at_ms": {"type": "integer", "minimum": 0},
        "reason": {"type": "string", "minLength": 1},
        "budget_state": BUDGET_STATE_JSON_SCHEMA,
    },
    "required": ["tenant_id", "repo_id", "generated_at_ms", "reason", "budget_state"],
}


def get_schema(*, kind: SchemaKind, version: int = ARTIFACT_SCHEMA_VERSION) -> dict[str, Any]:
    if kind == "runtime_bundle":
        if int(version) == 1:
            return RUNTIME_BUNDLE_V1
        if int(version) == 2:
            return RUNTIME_BUNDLE_V2
        if int(version) == 3:
            return RUNTIME_BUNDLE_V3
        if int(version) == 4:
            return RUNTIME_BUNDLE_V4
        raise ValueError(f"unsupported runtime_bundle schema version: {version}")
    if kind == "delivery_plan":
        if int(version) == 1:
            return DELIVERY_PLAN_V1
        raise ValueError(f"unsupported delivery_plan schema version: {version}")
    if int(version) != 1:
        raise ValueError(f"unsupported schema version: {version}")
    if kind == "manifest":
        return MANIFEST_V1
    if kind == "plan_state":
        return PLAN_STATE_V1
    if kind == "execution_stage":
        return EXECUTION_STAGE_V1
    if kind == "verifier_result":
        return VERIFIER_RESULT_V1
    if kind == "operational_validity_report":
        return OPERATIONAL_VALIDITY_REPORT_V1
    if kind == "operational_assurance_result":
        return OPERATIONAL_ASSURANCE_RESULT_V1
    if kind == "operational_evidence_window":
        return OPERATIONAL_EVIDENCE_WINDOW_V1
    if kind == "runtime_evidence_stream":
        return RUNTIME_EVIDENCE_STREAM_V1
    if kind == "run_trace_spans":
        return RUN_TRACE_SPANS_V1
    if kind == "run_cost_attribution":
        return RUN_COST_ATTRIBUTION_V1
    if kind == "replay_decisions":
        return REPLAY_DECISIONS_V1
    if kind == "living_drift_report":
        return LIVING_DRIFT_REPORT_V1
    if kind == "recompile_triggers":
        return RECOMPILE_TRIGGERS_V1
    if kind == "control_plane_envelope":
        return CONTROL_PLANE_ENVELOPE_V1
    if kind == "promotion_packet":
        return PROMOTION_PACKET_V1
    if kind == "reliability_scoreboard":
        return RELIABILITY_SCOREBOARD_V1
    if kind == "autopilot_decision":
        return AUTOPILOT_DECISION_V1
    if kind == "autopilot_human_escalation":
        return AUTOPILOT_HUMAN_ESCALATION_V1
    raise ValueError(f"unknown schema kind: {kind}")
