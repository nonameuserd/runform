from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from akc.memory.models import JSONValue, require_non_empty
from akc.outputs.models import OutputArtifact
from akc.outputs.yaml import dump_yaml


@dataclass(frozen=True, slots=True)
class WorkflowStep:
    """A single job step (GitHub Actions style)."""

    name: str | None = None
    uses: str | None = None
    run: str | None = None
    with_: Mapping[str, JSONValue] | None = None
    env: Mapping[str, str] | None = None
    shell: str | None = None
    id: str | None = None
    working_directory: str | None = None

    def __post_init__(self) -> None:
        if self.name is not None:
            require_non_empty(self.name, name="step.name")
        if self.uses is not None:
            require_non_empty(self.uses, name="step.uses")
        if self.run is not None:
            require_non_empty(self.run, name="step.run")
        if self.uses is None and self.run is None:
            raise ValueError("step must set either uses or run")
        if self.shell is not None:
            require_non_empty(self.shell, name="step.shell")
        if self.id is not None:
            require_non_empty(self.id, name="step.id")
        if self.working_directory is not None:
            require_non_empty(self.working_directory, name="step.working_directory")

    def to_obj(self) -> dict[str, Any]:
        obj: dict[str, Any] = {}
        if self.name is not None:
            obj["name"] = self.name
        if self.id is not None:
            obj["id"] = self.id
        if self.uses is not None:
            obj["uses"] = self.uses
        if self.run is not None:
            obj["run"] = self.run
        if self.with_:
            obj["with"] = dict(self.with_)
        if self.env:
            obj["env"] = dict(self.env)
        if self.shell is not None:
            obj["shell"] = self.shell
        if self.working_directory is not None:
            obj["working-directory"] = self.working_directory
        return obj


@dataclass(frozen=True, slots=True)
class WorkflowJob:
    """A single job (GitHub Actions style)."""

    runs_on: str
    steps: Sequence[WorkflowStep]
    name: str | None = None
    needs: Sequence[str] | None = None
    env: Mapping[str, str] | None = None
    permissions: Mapping[str, str] | None = None
    if_: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.runs_on, name="job.runs_on")
        if self.name is not None:
            require_non_empty(self.name, name="job.name")
        if not self.steps:
            raise ValueError("job.steps must be non-empty")
        if self.if_ is not None:
            require_non_empty(self.if_, name="job.if")
        if self.needs is not None:
            if any(not isinstance(n, str) or n.strip() == "" for n in self.needs):
                raise ValueError("job.needs must be a sequence of non-empty strings")

    def to_obj(self) -> dict[str, Any]:
        obj: dict[str, Any] = {"runs-on": self.runs_on, "steps": [s.to_obj() for s in self.steps]}
        if self.name is not None:
            obj["name"] = self.name
        if self.needs:
            obj["needs"] = list(self.needs)
        if self.env:
            obj["env"] = dict(self.env)
        if self.permissions:
            obj["permissions"] = dict(self.permissions)
        if self.if_ is not None:
            obj["if"] = self.if_
        return obj


@dataclass(frozen=True, slots=True)
class GithubActionsWorkflow:
    """A GitHub Actions workflow DSL that can be rendered to YAML."""

    name: str
    on: Mapping[str, Any] | Sequence[str]
    jobs: Mapping[str, WorkflowJob]

    def __post_init__(self) -> None:
        require_non_empty(self.name, name="workflow.name")
        if not self.jobs:
            raise ValueError("workflow.jobs must be non-empty")
        for job_id in self.jobs.keys():
            require_non_empty(str(job_id), name="workflow.job_id")

    def to_obj(self) -> dict[str, Any]:
        on_obj: Any
        if isinstance(self.on, (list, tuple)):
            on_obj = list(self.on)
        else:
            on_obj = dict(self.on)
        return {
            "name": self.name,
            "on": on_obj,
            "jobs": {str(k): v.to_obj() for k, v in self.jobs.items()},
        }

    def render_yaml(self) -> str:
        return dump_yaml(self.to_obj())

    def to_artifact(
        self,
        *,
        filename: str,
        directory: str = ".github/workflows",
        media_type: str = "application/yaml; charset=utf-8",
        metadata: Mapping[str, JSONValue] | None = None,
    ) -> OutputArtifact:
        require_non_empty(filename, name="filename")
        require_non_empty(directory, name="directory")
        fn = filename
        if not (fn.endswith(".yml") or fn.endswith(".yaml")):
            fn = f"{fn}.yml"
        path = f"{directory.rstrip('/')}/{fn}"
        return OutputArtifact.from_text(path=path, text=self.render_yaml(), media_type=media_type, metadata=metadata)

