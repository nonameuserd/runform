from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from akc.artifacts.contracts import ARTIFACT_SCHEMA_VERSION
from akc.compile.controller_config import Budget
from akc.intent.models import (
    IntentModelError,
    IntentSpec,
    IntentSpecV1,
    Objective,
    OperatingBound,
    compute_intent_fingerprint,
    deterministic_intent_id_from_semantic_fingerprint,
)
from akc.memory.models import normalize_repo_id, require_non_empty


class IntentCompilerError(Exception):
    """Raised when an input goal/intent-file cannot be normalized/validated."""


_EXPECTED_INTENT_SPEC_VERSION = 1


def _operating_bounds_from_budget(*, budget: Budget) -> OperatingBound:
    # Compatibility transform described in Phase 2:
    # controller-budget-derived operating bounds.
    return OperatingBound(
        max_seconds=float(budget.max_wall_time_s) if budget.max_wall_time_s is not None else None,
        max_steps=int(budget.max_iterations_total),
        max_input_tokens=int(budget.max_input_tokens) if budget.max_input_tokens is not None else None,
        max_output_tokens=int(budget.max_output_tokens) if budget.max_output_tokens is not None else None,
        allow_network=False,
    )


def _default_objective_from_goal(*, goal_statement: str) -> Objective:
    return Objective(
        id="objective_default",
        priority=1,
        statement=goal_statement,
        target="achieve_goal",
        metadata=None,
    )


def _parse_intent_file_to_obj(*, path: Path) -> dict[str, Any]:
    require_non_empty(str(path), name="intent_file_path")
    if not path.exists():
        raise IntentCompilerError(f"intent file does not exist: {path}")
    if not path.is_file():
        raise IntentCompilerError(f"intent file is not a regular file: {path}")

    raw = path.read_text(encoding="utf-8")
    suffix = path.suffix.lower()

    if suffix == ".json":
        loaded = json.loads(raw)
    elif suffix in {".yaml", ".yml"}:
        try:
            import yaml
        except Exception as e:  # pragma: no cover
            raise IntentCompilerError(
                "YAML intent files require PyYAML (install `akc[dev]` or `akc[ingest-openapi]`)."
            ) from e
        loaded = yaml.safe_load(raw)
    else:
        # Best-effort: try JSON first, then YAML if PyYAML is available.
        try:
            loaded = json.loads(raw)
        except Exception:
            try:
                import yaml
            except Exception as e:  # pragma: no cover
                raise IntentCompilerError("Unsupported intent file extension; expected .json or .yaml/.yml") from e
            loaded = yaml.safe_load(raw)

    if not isinstance(loaded, dict):
        raise IntentCompilerError("intent file root must be a JSON/YAML object")
    return loaded


def _require_non_empty_str(value: Any, *, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise IntentCompilerError(f"{name} must be a non-empty string")
    return value


def _validate_operating_bounds_raw(
    *,
    raw: Any,
    context: str,
) -> None:
    if raw is None:
        return
    if not isinstance(raw, Mapping):
        raise IntentCompilerError(f"{context}.operating_bounds must be an object")

    allowed_keys = {
        "max_seconds",
        "max_steps",
        "max_input_tokens",
        "max_output_tokens",
        "allow_network",
    }
    for k in raw:
        if not isinstance(k, str):
            raise IntentCompilerError(f"{context}.operating_bounds keys must be strings")
        if k not in allowed_keys:
            raise IntentCompilerError(f"{context}.operating_bounds has unsupported key: {k}")

    def _validate_pos_float(name: str) -> None:
        v = raw.get(name)
        if v is None:
            return
        if isinstance(v, bool) or not isinstance(v, (int, float)):
            raise IntentCompilerError(f"{context}.operating_bounds.{name} must be a number")
        if float(v) <= 0:
            raise IntentCompilerError(f"{context}.operating_bounds.{name} must be > 0 when set")

    def _validate_pos_int(name: str) -> None:
        v = raw.get(name)
        if v is None:
            return
        if isinstance(v, bool) or not isinstance(v, int):
            raise IntentCompilerError(f"{context}.operating_bounds.{name} must be an integer")
        if int(v) <= 0:
            raise IntentCompilerError(f"{context}.operating_bounds.{name} must be > 0 when set")

    _validate_pos_float("max_seconds")
    _validate_pos_int("max_steps")
    _validate_pos_int("max_input_tokens")
    _validate_pos_int("max_output_tokens")

    if "allow_network" in raw:
        v = raw.get("allow_network")
        if not isinstance(v, bool):
            raise IntentCompilerError(f"{context}.operating_bounds.allow_network must be a boolean")


def _validate_explicit_intent_scope_and_versions(
    *,
    raw: Mapping[str, Any],
    expected_tenant_id: str,
    expected_repo_id: str,
    expected_spec_version: int,
) -> None:
    context = "explicit_intent"

    tenant_id_raw = raw.get("tenant_id")
    require_non_empty(str(expected_tenant_id), name="tenant_id")
    tenant_id = _require_non_empty_str(tenant_id_raw, name=f"{context}.tenant_id")
    if tenant_id.strip() != expected_tenant_id.strip():
        raise IntentCompilerError(f"intent tenant_id mismatch: expected={expected_tenant_id} got={tenant_id}")

    repo_id_raw = raw.get("repo_id")
    repo_id = _require_non_empty_str(repo_id_raw, name=f"{context}.repo_id")
    if normalize_repo_id(repo_id) != normalize_repo_id(expected_repo_id):
        expected_norm = normalize_repo_id(expected_repo_id)
        got_norm = normalize_repo_id(repo_id)
        raise IntentCompilerError(f"intent repo_id mismatch: expected={expected_norm} got={got_norm}")

    if "spec_version" not in raw:
        raise IntentCompilerError(f"{context}.spec_version is required for explicit intent files")

    spec_version_raw = raw.get("spec_version")
    if isinstance(spec_version_raw, bool) or not isinstance(spec_version_raw, (int, float, str)):
        raise IntentCompilerError(f"{context}.spec_version must be a number")
    try:
        spec_version = int(spec_version_raw)
    except Exception as e:
        raise IntentCompilerError(f"{context}.spec_version must be an integer") from e

    if spec_version != expected_spec_version:
        raise IntentCompilerError(
            f"unsupported intent spec_version: got={spec_version} expected={expected_spec_version}"
        )

    if "schema_version" in raw:
        schema_version_raw = raw.get("schema_version")
        try:
            schema_version = int(schema_version_raw)  # type: ignore[arg-type]
        except Exception as e:
            raise IntentCompilerError(f"{context}.schema_version must be an integer") from e
        if schema_version != ARTIFACT_SCHEMA_VERSION:
            expected_schema = ARTIFACT_SCHEMA_VERSION
            raise IntentCompilerError(
                f"unsupported intent artifact schema_version: got={schema_version} expected={expected_schema}"
            )


def _validate_operating_bounds_monotonic_to_budget(
    *,
    requested: OperatingBound,
    budget: Budget,
    context: str,
) -> None:
    # Ensure intent does not widen controller authority.
    # Mapping:
    # - intent.max_seconds <-> budget.max_wall_time_s
    # - intent.max_steps <-> budget.max_iterations_total
    # - intent.max_input_tokens <-> budget.max_input_tokens
    # - intent.max_output_tokens <-> budget.max_output_tokens
    if (
        budget.max_wall_time_s is not None
        and requested.max_seconds is not None
        and float(requested.max_seconds) > float(budget.max_wall_time_s)
    ):
        raise IntentCompilerError(
            f"{context}.operating_bounds.max_seconds exceeds controller budget "
            f"({requested.max_seconds} > {budget.max_wall_time_s})"
        )
    if requested.max_steps is not None and int(requested.max_steps) > int(budget.max_iterations_total):
        raise IntentCompilerError(
            f"{context}.operating_bounds.max_steps exceeds controller budget "
            f"({requested.max_steps} > {budget.max_iterations_total})"
        )
    if (
        budget.max_input_tokens is not None
        and requested.max_input_tokens is not None
        and int(requested.max_input_tokens) > int(budget.max_input_tokens)
    ):
        raise IntentCompilerError(
            f"{context}.operating_bounds.max_input_tokens exceeds controller budget "
            f"({requested.max_input_tokens} > {budget.max_input_tokens})"
        )
    if (
        budget.max_output_tokens is not None
        and requested.max_output_tokens is not None
        and int(requested.max_output_tokens) > int(budget.max_output_tokens)
    ):
        raise IntentCompilerError(
            f"{context}.operating_bounds.max_output_tokens exceeds controller budget "
            f"({requested.max_output_tokens} > {budget.max_output_tokens})"
        )


def compile_intent_spec(
    *,
    tenant_id: str,
    repo_id: str,
    goal_statement: str | None = None,
    intent_file: str | Path | None = None,
    controller_budget: Budget | None = None,
    expected_intent_spec_version: int = _EXPECTED_INTENT_SPEC_VERSION,
) -> IntentSpec:
    """Build a tenant+repo-scoped `IntentSpec` from goal text or an intent file.

    Phase 2 compatibility transform:
    - `goal_statement` becomes `IntentSpec.goal_statement`
    - plus a single default objective
    - plus operating bounds derived from the controller budget
    """

    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    tenant_id_norm = tenant_id.strip()
    repo_id_norm = normalize_repo_id(repo_id)

    if goal_statement is not None and intent_file is not None:
        raise IntentCompilerError("provide only one of goal_statement or intent_file")
    if goal_statement is None and intent_file is None:
        raise IntentCompilerError("must provide either goal_statement or intent_file")

    budget = controller_budget or Budget()

    if intent_file is not None:
        path = Path(intent_file).expanduser()
        return compile_intent_spec_from_file(
            tenant_id=tenant_id_norm,
            repo_id=repo_id_norm,
            intent_file=path,
            controller_budget=budget,
            expected_intent_spec_version=expected_intent_spec_version,
        )

    assert goal_statement is not None  # for type checkers
    require_non_empty(goal_statement, name="goal_statement")
    goal = goal_statement.strip()
    # Create a compatibility intent with a default objective and controller-derived
    # operating bounds. Use a deterministic `intent_id` derived from semantic
    # fingerprint so prompt/cache keys remain stable across runs.
    base_spec = IntentSpecV1(
        tenant_id=tenant_id_norm,
        repo_id=repo_id_norm,
        status="draft",
        title=None,
        goal_statement=goal,
        derived_from_goal_text=True,
        objectives=(_default_objective_from_goal(goal_statement=goal),),
        operating_bounds=_operating_bounds_from_budget(budget=budget),
    ).normalized()
    fp = compute_intent_fingerprint(intent=base_spec)
    stable_intent_id = deterministic_intent_id_from_semantic_fingerprint(semantic_fingerprint=fp.semantic)
    return IntentSpecV1(
        intent_id=stable_intent_id,
        tenant_id=base_spec.tenant_id,
        repo_id=base_spec.repo_id,
        spec_version=base_spec.spec_version,
        status=base_spec.status,
        title=base_spec.title,
        goal_statement=base_spec.goal_statement,
        summary=base_spec.summary,
        derived_from_goal_text=base_spec.derived_from_goal_text,
        objectives=base_spec.objectives,
        constraints=base_spec.constraints,
        policies=base_spec.policies,
        success_criteria=base_spec.success_criteria,
        operating_bounds=base_spec.operating_bounds,
        assumptions=base_spec.assumptions,
        risk_notes=base_spec.risk_notes,
        tags=base_spec.tags,
        metadata=base_spec.metadata,
        created_at_ms=base_spec.created_at_ms,
        updated_at_ms=base_spec.updated_at_ms,
    ).normalized()


def compile_intent_spec_from_file(
    *,
    tenant_id: str,
    repo_id: str,
    intent_file: Path,
    controller_budget: Budget,
    expected_intent_spec_version: int = _EXPECTED_INTENT_SPEC_VERSION,
) -> IntentSpecV1:
    """Load and normalize an explicit intent file, enforcing scope + schema safety."""

    raw = _parse_intent_file_to_obj(path=intent_file)
    _validate_explicit_intent_scope_and_versions(
        raw=raw,
        expected_tenant_id=tenant_id,
        expected_repo_id=repo_id,
        expected_spec_version=expected_intent_spec_version,
    )
    _validate_operating_bounds_raw(raw=raw.get("operating_bounds"), context="explicit_intent")

    # If operating bounds are missing, fall back to controller budget.
    if raw.get("operating_bounds") is None:
        raw["operating_bounds"] = _operating_bounds_from_budget(budget=controller_budget).to_json_obj()

    try:
        intent = IntentSpecV1.from_json_obj(raw)
    except (IntentModelError, ValueError) as e:
        raise IntentCompilerError(f"invalid intent file: {intent_file}") from e

    # Re-validate monotonicity once typed.
    if intent.operating_bounds is not None:
        _validate_operating_bounds_monotonic_to_budget(
            requested=intent.operating_bounds,
            budget=controller_budget,
            context="explicit_intent",
        )

    # Enforce final scope match after model normalization.
    normalized = intent.normalized()
    if normalized.tenant_id != tenant_id or normalize_repo_id(normalized.repo_id) != repo_id:
        raise IntentCompilerError("tenant_id/repo_id mismatch after normalization")
    return normalized
