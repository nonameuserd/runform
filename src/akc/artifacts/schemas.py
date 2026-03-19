from __future__ import annotations

from typing import Any, Final, Literal

from akc.artifacts.contracts import ARTIFACT_SCHEMA_VERSION, schema_id_for

SchemaKind = Literal["manifest", "plan_state", "execution_stage", "verifier_result"]


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


def get_schema(*, kind: SchemaKind, version: int = ARTIFACT_SCHEMA_VERSION) -> dict[str, Any]:
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
    raise ValueError(f"unknown schema kind: {kind}")
