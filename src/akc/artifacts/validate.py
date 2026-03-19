from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from jsonschema import Draft202012Validator

from akc.artifacts.schemas import SchemaKind, get_schema


@dataclass(frozen=True, slots=True)
class SchemaValidationIssue:
    path: str
    message: str


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
