"""Outputs: code, workflow definitions, agent specs.

Phase 4 introduces a small, explicit output model plus emitters that can persist
bundled artifacts while enforcing tenant+repo isolation.
"""

from akc.outputs.emitters import Emitter, FileSystemEmitter, JsonManifestEmitter
from akc.outputs.models import (
    AgentBudget,
    AgentRoleSpec,
    AgentSpec,
    LlmBackendSpec,
    OutputArtifact,
    OutputBundle,
)
from akc.outputs.system_specs import (
    CoordinationEdgeKind,
    CoordinationEdgeSpec,
    CoordinationSpec,
    OrchestrationSpec,
    OrchestrationStepSpec,
    SystemDesignSpec,
)
from akc.outputs.workflows import GithubActionsWorkflow, WorkflowJob, WorkflowStep
from akc.outputs.yaml import dump_yaml

__all__ = [
    "AgentBudget",
    "AgentRoleSpec",
    "AgentSpec",
    "Emitter",
    "FileSystemEmitter",
    "JsonManifestEmitter",
    "GithubActionsWorkflow",
    "LlmBackendSpec",
    "OutputArtifact",
    "OutputBundle",
    "WorkflowJob",
    "WorkflowStep",
    "dump_yaml",
    "CoordinationEdgeKind",
    "CoordinationEdgeSpec",
    "CoordinationSpec",
    "OrchestrationSpec",
    "OrchestrationStepSpec",
    "SystemDesignSpec",
]
