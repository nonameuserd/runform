from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Literal, Mapping, Sequence, cast

from akc.compile.interfaces import TenantRepoScope
from akc.memory.models import JSONValue, require_non_empty
from akc.outputs.yaml import dump_yaml


def _normalize_relpath(p: str) -> str:
    # Store relpaths in a portable form.
    p2 = str(PurePosixPath(str(p).strip()))
    require_non_empty(p2, name="artifact.path")
    if p2.startswith("/"):
        raise ValueError("artifact.path must be relative")
    # Reject escape (../) and current-dir segments to keep deterministic bundles.
    parts = [seg for seg in PurePosixPath(p2).parts if seg not in ("", ".")]
    if any(seg == ".." for seg in parts):
        raise ValueError("artifact.path must not contain '..'")
    if not parts:
        raise ValueError("artifact.path must be a non-empty relative path")
    return str(PurePosixPath(*parts))


@dataclass(frozen=True, slots=True)
class OutputArtifact:
    """A single output artifact (file-like) produced by compilation."""

    path: str
    content: bytes
    media_type: str = "application/octet-stream"
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", _normalize_relpath(self.path))
        require_non_empty(self.media_type, name="artifact.media_type")

    @classmethod
    def from_text(
        cls,
        *,
        path: str,
        text: str,
        media_type: str = "text/plain; charset=utf-8",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> OutputArtifact:
        require_non_empty(text, name="text")
        return cls(path=path, content=text.encode("utf-8"), media_type=media_type, metadata=metadata)

    @classmethod
    def from_json(
        cls,
        *,
        path: str,
        obj: Mapping[str, Any],
        media_type: str = "application/json; charset=utf-8",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> OutputArtifact:
        text = json.dumps(dict(obj), indent=2, sort_keys=True, ensure_ascii=False) + "\n"
        return cls.from_text(path=path, text=text, media_type=media_type, metadata=metadata)

    def sha256_hex(self) -> str:
        return hashlib.sha256(self.content).hexdigest()

    def size_bytes(self) -> int:
        return len(self.content)

    def text(self, *, encoding: str = "utf-8") -> str:
        return self.content.decode(encoding, errors="replace")

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "media_type": self.media_type,
            "sha256": self.sha256_hex(),
            "size_bytes": self.size_bytes(),
            "metadata": dict(cast(Mapping[str, JSONValue], self.metadata)) if self.metadata else None,
        }


@dataclass(frozen=True, slots=True)
class OutputBundle:
    """A named bundle of artifacts emitted under a single tenant+repo scope."""

    scope: TenantRepoScope
    name: str
    artifacts: tuple[OutputArtifact, ...]
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.name, name="bundle.name")
        # Enforce uniqueness on artifact paths within a bundle (deterministic emission).
        seen: set[str] = set()
        for a in self.artifacts:
            if a.path in seen:
                raise ValueError(f"duplicate artifact path: {a.path}")
            seen.add(a.path)

    def to_manifest_obj(self) -> dict[str, Any]:
        return {
            "tenant_id": self.scope.tenant_id,
            "repo_id": self.scope.repo_id,
            "name": self.name,
            "artifacts": [a.to_json_obj() for a in self.artifacts],
            "metadata": dict(cast(Mapping[str, JSONValue], self.metadata)) if self.metadata else None,
        }


AgentRoleName = Literal["planner", "retriever", "writer", "verifier"]


@dataclass(frozen=True, slots=True)
class AgentBudget:
    """Budget constraints for an agent role.

    These fields are intentionally minimal and backend-agnostic.
    """

    max_steps: int | None = None
    max_input_tokens: int | None = None
    max_output_tokens: int | None = None
    max_seconds: float | None = None

    def __post_init__(self) -> None:
        if self.max_steps is not None and int(self.max_steps) <= 0:
            raise ValueError("budget.max_steps must be > 0 when set")
        if self.max_input_tokens is not None and int(self.max_input_tokens) <= 0:
            raise ValueError("budget.max_input_tokens must be > 0 when set")
        if self.max_output_tokens is not None and int(self.max_output_tokens) <= 0:
            raise ValueError("budget.max_output_tokens must be > 0 when set")
        if self.max_seconds is not None and float(self.max_seconds) <= 0:
            raise ValueError("budget.max_seconds must be > 0 when set")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "max_steps": int(self.max_steps) if self.max_steps is not None else None,
            "max_input_tokens": int(self.max_input_tokens) if self.max_input_tokens is not None else None,
            "max_output_tokens": int(self.max_output_tokens) if self.max_output_tokens is not None else None,
            "max_seconds": float(self.max_seconds) if self.max_seconds is not None else None,
        }
        # Drop nulls for compactness / readability.
        return {k: v for k, v in obj.items() if v is not None}


@dataclass(frozen=True, slots=True)
class AgentRoleSpec:
    """Config for a single agent role (planner/retriever/writer/verifier)."""

    name: AgentRoleName
    tools: Sequence[str] = ()
    budget: AgentBudget | None = None
    instructions: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.name, name="role.name")
        for t in self.tools:
            require_non_empty(str(t), name="role.tools[]")
        if self.instructions is not None:
            require_non_empty(self.instructions, name="role.instructions")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "name": self.name,
            "tools": [str(t) for t in self.tools],
            "budget": self.budget.to_json_obj() if self.budget is not None else None,
            "instructions": self.instructions,
        }
        # Drop nulls for stable/diff-friendly specs.
        return {k: v for k, v in obj.items() if v is not None}


@dataclass(frozen=True, slots=True)
class LlmBackendSpec:
    """Backend selector + parameters for running an agent."""

    backend: str
    model: str
    params: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.backend, name="llm.backend")
        require_non_empty(self.model, name="llm.model")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {"backend": self.backend, "model": self.model}
        if self.params:
            obj["params"] = dict(self.params)
        return obj


@dataclass(frozen=True, slots=True)
class AgentSpec:
    """Agent runtime spec emitted as JSON and/or YAML.

    Tenant isolation:
    - Each spec is explicitly bound to a `TenantRepoScope`.
    - Consumers must not run a spec under a different scope without an explicit override.
    """

    scope: TenantRepoScope
    name: str
    llm: LlmBackendSpec
    roles: Sequence[AgentRoleSpec]
    spec_version: int = 1
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.name, name="agent.name")
        if int(self.spec_version) <= 0:
            raise ValueError("agent.spec_version must be > 0")
        if not self.roles:
            raise ValueError("agent.roles must be non-empty")
        # Enforce unique role names for deterministic, non-ambiguous configs.
        seen: set[str] = set()
        for r in self.roles:
            if r.name in seen:
                raise ValueError(f"duplicate agent role: {r.name}")
            seen.add(r.name)

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "spec_version": int(self.spec_version),
            "scope": {"tenant_id": self.scope.tenant_id, "repo_id": self.scope.repo_id},
            "name": self.name,
            "llm": self.llm.to_json_obj(),
            "roles": [r.to_json_obj() for r in self.roles],
            "metadata": dict(cast(Mapping[str, JSONValue], self.metadata)) if self.metadata else None,
        }
        return {k: v for k, v in obj.items() if v is not None}

    def render_json(self) -> str:
        return json.dumps(self.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False) + "\n"

    def render_yaml(self) -> str:
        return dump_yaml(self.to_json_obj())

    def to_artifact_json(
        self,
        *,
        directory: str = ".akc/agents",
        filename: str | None = None,
        media_type: str = "application/json; charset=utf-8",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> OutputArtifact:
        require_non_empty(directory, name="directory")
        fn = filename.strip() if isinstance(filename, str) else self.name
        require_non_empty(fn, name="filename")
        if not fn.endswith(".json"):
            fn = f"{fn}.json"
        path = f"{directory.rstrip('/')}/{fn}"
        return OutputArtifact.from_text(path=path, text=self.render_json(), media_type=media_type, metadata=metadata)

    def to_artifact_yaml(
        self,
        *,
        directory: str = ".akc/agents",
        filename: str | None = None,
        media_type: str = "application/yaml; charset=utf-8",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> OutputArtifact:
        require_non_empty(directory, name="directory")
        fn = filename.strip() if isinstance(filename, str) else self.name
        require_non_empty(fn, name="filename")
        if not (fn.endswith(".yml") or fn.endswith(".yaml")):
            fn = f"{fn}.yml"
        path = f"{directory.rstrip('/')}/{fn}"
        return OutputArtifact.from_text(path=path, text=self.render_yaml(), media_type=media_type, metadata=metadata)

