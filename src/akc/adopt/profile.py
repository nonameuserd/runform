from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal, TypeAlias

BuildCommandKind: TypeAlias = Literal["build", "test", "lint", "typecheck", "format", "ci", "other"]


@dataclass(frozen=True, slots=True)
class LanguageEntry:
    """Detected language evidence inside a project directory."""

    language: str
    percent: float
    bytes: int
    files: int
    evidence: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class BuildCommand:
    """A detected native command (build/test/lint/format) used by the project."""

    command: tuple[str, ...]
    kind: BuildCommandKind
    source: str | None = None


@dataclass(frozen=True, slots=True)
class CISystem:
    """CI system detection (e.g. GitHub Actions)."""

    name: str
    evidence: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConventionSnapshot:
    """Extracted high-signal conventions (naming, layout, import style)."""

    layout: dict[str, str] = field(default_factory=dict)
    naming: dict[str, str] = field(default_factory=dict)
    imports: dict[str, str] = field(default_factory=dict)
    tests: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ProjectProfile:
    """Project analysis profile (emitted by `akc init --detect`)."""

    root: Path
    languages: list[LanguageEntry]
    package_managers: list[str]
    build_commands: list[BuildCommand]
    ci_systems: list[CISystem]
    conventions: ConventionSnapshot
    entry_points: list[str]
    architecture_hints: dict[str, Any]

    def to_json_dict(self) -> dict[str, Any]:
        """Serialize into JSON-friendly primitives."""

        def _path_to_str(value: Any) -> Any:
            if isinstance(value, Path):
                return str(value)
            return value

        raw = asdict(self)

        # asdict converts nested dataclasses, but keeps Path objects for `root`.
        raw["root"] = _path_to_str(self.root)
        return raw

    def to_json_str(self, *, indent: int = 2) -> str:
        """Serialize to deterministic JSON."""

        return json.dumps(self.to_json_dict(), indent=indent, sort_keys=True) + "\n"
