from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator

from akc.artifacts.schemas import SchemaKind, get_schema


@dataclass(frozen=True, slots=True)
class SchemaValidationIssue:
    path: str
    message: str


def should_validate_artifact_json(*, enabled: bool | None = None) -> bool:
    """Return whether schema validation should fail writes in this process.

    Production remains tolerant by default. Validation is enabled explicitly via
    `enabled=True` or implicitly in CI / pytest / `AKC_VALIDATE_ARTIFACT_JSON=1`.
    """

    if enabled is not None:
        return bool(enabled)
    env_flag = str(os.environ.get("AKC_VALIDATE_ARTIFACT_JSON", "")).strip().lower()
    if env_flag in {"1", "true", "yes", "on"}:
        return True
    if env_flag in {"0", "false", "no", "off"}:
        return False
    return bool(os.environ.get("CI") or os.environ.get("PYTEST_CURRENT_TEST"))


def validate_obj(*, obj: Any, kind: SchemaKind, version: int = 1) -> list[SchemaValidationIssue]:
    """Validate an object against the frozen artifact schema.

    Returns a list of issues (empty means valid).
    """

    schema = get_schema(kind=kind, version=version)
    v = Draft202012Validator(schema)
    issues: list[SchemaValidationIssue] = []
    for err in sorted(v.iter_errors(obj), key=lambda e: (list(e.path), e.message)):
        loc = "/" + "/".join(str(p) for p in err.path) if err.path else "/"
        issues.append(SchemaValidationIssue(path=loc, message=str(err.message)))
    return issues


def validate_artifact_json(
    *,
    obj: Any,
    kind: SchemaKind,
    version: int = 1,
    enabled: bool | None = None,
) -> list[SchemaValidationIssue]:
    """Validate an artifact object and optionally raise in dev/CI contexts."""

    issues = validate_obj(obj=obj, kind=kind, version=version)
    if issues and should_validate_artifact_json(enabled=enabled):
        rendered = "; ".join(f"{issue.path}: {issue.message}" for issue in issues[:8])
        if len(issues) > 8:
            rendered += f"; ... ({len(issues)} issues total)"
        raise ValueError(f"artifact JSON failed schema validation for kind={kind!r} version={int(version)}: {rendered}")
    return issues
