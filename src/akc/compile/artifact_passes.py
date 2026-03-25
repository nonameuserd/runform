"""Deterministic artifact lowering passes (post-controller / post-ARCS loop).

These passes form a **linear DAG**: each stage depends on the previous lowering
outputs where noted. Order is `ARTIFACT_PASS_ORDER` in
`akc.pass_registry.ARTIFACT_PASS_ORDER` (single source of truth); `REPLAYABLE_PASSES`
in `akc.run.manifest` appends that tuple after controller-loop pass names.

Per-pass contracts (compile session registers `PassRecord.output_sha256` from
sorted artifact path + sha256; see `CompileSession._register_pass`):

- **system_design**
  - Inputs: `IRDocument`, `IntentSpec`, optional `knowledge_snapshot` mapping.
  - Outputs: `.akc/design/<run_id>.system_design.json` (+ optional `.md`).
  - Hash inputs: pass name + emitted artifact bytes (manifest aggregates paths
    and per-file sha256).

- **orchestration_spec**
  - Inputs: `IRDocument`, `IntentSpec`.
  - Outputs: `.akc/orchestration/<run_id>.orchestration.json`, `.orchestrator.py`,
    `.orchestrator.ts`.
  - Hash inputs: same registration rule; primary json drives `output_sha256` on
    the pass result type.

- **agent_coordination**
  - Inputs: `IRDocument`, `IntentSpec`.
  - Outputs: `.akc/agents/<run_id>.coordination.json`, protocol stubs `.py`/`.ts`.
  - Hash inputs: same registration rule.

- **delivery_plan**
  - Inputs: `IRDocument`, `IntentSpec`, orchestration JSON text, coordination JSON text.
  - Outputs: `.akc/deployment/<run_id>.delivery_plan.json` and companion
    `.akc/design/<run_id>.delivery_summary.md` (non-technical narrative; JSON remains authoritative).
  - Hash inputs: same registration rule.

- **runtime_bundle**
  - Inputs: `IRDocument`, `IntentSpec`, orchestration JSON text, coordination
    JSON text, optional delivery-plan JSON text (from prior passes; may be replay-cloned).
  - Outputs: `.akc/runtime/<run_id>.runtime_bundle.json` (schema envelope);
    bundle embeds `spec_hashes` (fingerprints of orchestration/coordination JSON
    objects) and IR/intent references.
  - Hash inputs: manifest pass hash + bundle file sha256; bundle content hashes
    upstream specs for runtime handoff.

- **deployment_config**
  - Inputs: `IRDocument`, `IntentSpec`, same orchestration/coordination texts as
    `runtime_bundle`, optional delivery-plan JSON text.
  - Outputs: Docker Compose + K8s manifests under `.akc/deployment/`, GitHub
    Actions workflow under `.github/workflows/`.
  - Hash inputs: same registration rule as other passes.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.schemas import RUNTIME_BUNDLE_SCHEMA_VERSION
from akc.compile.artifact_consistency import effective_allow_network_for_handoff
from akc.compile.delivery_projection import (
    build_delivery_plan,
    parse_json_artifact_text,
    render_delivery_summary_markdown,
)
from akc.compile.interfaces import LLMMessage, LLMRequest, TenantRepoScope
from akc.compile.patch_utils import extract_touched_paths
from akc.intent.policy_projection import (
    build_handoff_intent_ref,
    project_deployment_intent_projection,
    project_runtime_intent_projection,
)
from akc.ir import IRDocument, IRNode
from akc.ir.workflow_order import sorted_workflow_nodes_for_coordination_emit, workflow_coordination_layer_key
from akc.memory.models import JSONValue
from akc.outputs import (
    AgentRoleSpec,
    AgentSpec,
    CoordinationEdgeSpec,
    CoordinationSpec,
    LlmBackendSpec,
    OrchestrationSpec,
    OrchestrationStepSpec,
    OutputArtifact,
    SystemDesignSpec,
    dump_yaml,
)
from akc.outputs.models import AgentRoleName
from akc.run.vcr import llm_vcr_prompt_key
from akc.runtime.coordination.isolation import validate_role_profiles_network_vs_bundle
from akc.runtime.policy import derive_runtime_evidence_expectations
from akc.utils.fingerprint import stable_json_fingerprint

if TYPE_CHECKING:
    from akc.intent.models import IntentSpec


@dataclass(frozen=True, slots=True)
class ArtifactPromptEnvelope:
    llm_request: LLMRequest
    prompt_key: str
    user_prompt: str


@dataclass(frozen=True, slots=True)
class ParsedPatchArtifact:
    patch_text: str
    touched_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SystemDesignPassResult:
    artifact_json: OutputArtifact
    artifact_md: OutputArtifact | None
    output_sha256: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class OrchestrationSpecPassResult:
    artifact_json: OutputArtifact
    artifact_python_stub: OutputArtifact
    artifact_typescript_stub: OutputArtifact
    output_sha256: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class AgentCoordinationPassResult:
    artifact_json: OutputArtifact
    artifact_python_stub: OutputArtifact
    artifact_typescript_stub: OutputArtifact
    output_sha256: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class DeploymentConfigPassResult:
    artifact_docker_compose: OutputArtifact
    artifact_k8s_deployment: OutputArtifact
    artifact_k8s_service: OutputArtifact
    artifact_k8s_configmap: OutputArtifact
    artifact_github_actions: OutputArtifact
    additional_artifacts: tuple[OutputArtifact, ...]
    output_sha256: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class RuntimeBundlePassResult:
    artifact_json: OutputArtifact
    output_sha256: str
    metadata: dict[str, JSONValue]


@dataclass(frozen=True, slots=True)
class DeliveryPlanPassResult:
    artifact_json: OutputArtifact
    artifact_summary_md: OutputArtifact
    output_sha256: str
    metadata: dict[str, JSONValue]


def _patch_envelope_system_skill_section(
    *,
    skill_blocks: Sequence[str] | None,
    system_preamble: str | None,
    skill_system_append: str | None,
) -> str:
    """Build the optional suffix appended to the base AKC system string.

    If ``skill_blocks`` is not ``None``, it wins and non-empty stripped blocks are
    joined with blank lines. Otherwise ``system_preamble`` and
    ``skill_system_append`` are combined (both optional; legacy callers use only
    ``skill_system_append``).
    """

    if skill_blocks is not None:
        parts = [b.strip() for b in skill_blocks if isinstance(b, str) and b.strip()]
        return "\n\n".join(parts)
    p1 = (system_preamble or "").strip()
    p2 = (skill_system_append or "").strip()
    if p1 and p2:
        return f"{p1}\n\n{p2}"
    return p1 or p2


def build_patch_artifact_prompt_envelope(
    *,
    user_prompt: str,
    tier_name: str,
    tier_model: str,
    plan_id: str,
    step_id: str,
    replay_mode: str,
    temperature: float,
    max_output_tokens: int | None,
    system_preamble: str | None = None,
    skill_blocks: Sequence[str] | None = None,
    skill_system_append: str | None = None,
    compile_skills_active: Sequence[Mapping[str, Any]] | None = None,
    compile_skills_mode: str | None = None,
) -> ArtifactPromptEnvelope:
    base_system = "You are an AKC compile loop assistant."
    extra = _patch_envelope_system_skill_section(
        skill_blocks=skill_blocks,
        system_preamble=system_preamble,
        skill_system_append=skill_system_append,
    )
    system_content = f"{base_system}\n\n{extra}" if extra else base_system
    llm_messages = [
        LLMMessage(role="system", content=system_content),
        LLMMessage(role="user", content=user_prompt),
    ]
    meta: dict[str, JSONValue] = {
        "tier": tier_name,
        "tier_model": tier_model,
        "plan_id": plan_id,
        "step_id": step_id,
        "replay_mode": replay_mode,
    }
    if compile_skills_active is not None:
        meta["compile_skills_active"] = cast(
            JSONValue,
            [dict(m) for m in compile_skills_active],
        )
    if compile_skills_mode is not None:
        meta["compile_skills_mode"] = compile_skills_mode
    llm_request = LLMRequest(
        messages=llm_messages,
        temperature=float(temperature),
        max_output_tokens=int(max_output_tokens) if max_output_tokens is not None else None,
        metadata=meta,
    )
    prompt_key = llm_vcr_prompt_key(
        messages=llm_messages,
        temperature=float(temperature),
        max_output_tokens=int(max_output_tokens) if max_output_tokens is not None else None,
        metadata=llm_request.metadata,
    )
    return ArtifactPromptEnvelope(
        llm_request=llm_request,
        prompt_key=prompt_key,
        user_prompt=user_prompt,
    )


def parse_patch_artifact_strict(
    *,
    text: str,
) -> ParsedPatchArtifact | None:
    raw_text = str(text)
    if not raw_text.strip():
        return None
    if "```" in raw_text:
        return None

    lines = raw_text.splitlines()
    minus_headers = [ln for ln in lines if ln.startswith("--- ")]
    plus_headers = [ln for ln in lines if ln.startswith("+++ ")]
    if not minus_headers or not plus_headers:
        return None
    if len(minus_headers) != len(plus_headers):
        return None

    touched_paths = tuple(extract_touched_paths(raw_text))
    if not touched_paths:
        return None

    return ParsedPatchArtifact(
        patch_text=raw_text,
        touched_paths=touched_paths,
    )


def parse_patch_artifact_metadata(*, metadata: Mapping[str, Any] | None) -> ParsedPatchArtifact | None:
    if metadata is None:
        return None
    raw = metadata.get("llm_text")
    if not isinstance(raw, str):
        return None
    return parse_patch_artifact_strict(text=raw)


def parse_patch_artifact_vcr_cache(*, llm_vcr: Mapping[str, str] | None, prompt_key: str) -> ParsedPatchArtifact | None:
    if llm_vcr is None:
        return None
    cached = llm_vcr.get(prompt_key)
    if not isinstance(cached, str):
        return None
    return parse_patch_artifact_strict(text=cached)


def run_system_design_pass(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_spec: IntentSpec,
    knowledge_snapshot: Mapping[str, Any] | None,
    emit_markdown_companion: bool = True,
) -> SystemDesignPassResult:
    scope = TenantRepoScope(tenant_id=ir_document.tenant_id, repo_id=ir_document.repo_id)
    llm_params: dict[str, JSONValue] = {}
    if intent_spec.operating_bounds is not None and intent_spec.operating_bounds.max_output_tokens:
        llm_params["max_output_limit"] = int(intent_spec.operating_bounds.max_output_tokens)

    agent = AgentSpec(
        scope=scope,
        name="compile_controller",
        llm=LlmBackendSpec(backend="akc", model="deterministic_system_design", params=llm_params),
        roles=(
            AgentRoleSpec(name="planner", tools=("llm.complete",)),
            AgentRoleSpec(name="retriever", tools=("llm.complete",)),
            AgentRoleSpec(name="writer", tools=("executor.run",)),
            AgentRoleSpec(name="verifier", tools=("executor.run",)),
        ),
    )

    workflow_nodes = list(sorted_workflow_nodes_for_coordination_emit(ir_document.nodes))
    steps: list[OrchestrationStepSpec] = []
    for idx, node in enumerate(workflow_nodes):
        step_id = f"workflow_{idx:03d}"
        role = _coordination_role_for_workflow_index(idx)
        steps.append(
            OrchestrationStepSpec(
                step_id=step_id,
                order_idx=idx,
                agent_name=agent.name,
                role=role,
                inputs={
                    "ir_node_id": node.id,
                    "ir_node_name": node.name,
                    "step_status": str(node.properties.get("status", "pending")),
                },
            )
        )
    if not steps:
        steps = [
            OrchestrationStepSpec(
                step_id="workflow_000",
                order_idx=0,
                agent_name=agent.name,
                role="writer",
                inputs={"reason": "fallback_when_ir_has_no_workflow_nodes"},
            )
        ]

    edges: list[CoordinationEdgeSpec] = []
    if len(steps) == 1:
        edges.append(
            CoordinationEdgeSpec(
                edge_id="edge_000",
                kind="depends_on",
                src_step_id=steps[0].step_id,
                dst_step_id=steps[0].step_id,
            )
        )
    else:
        for idx in range(1, len(steps)):
            edges.append(
                CoordinationEdgeSpec(
                    edge_id=f"edge_{idx:03d}",
                    kind="depends_on",
                    src_step_id=steps[idx - 1].step_id,
                    dst_step_id=steps[idx].step_id,
                )
            )

    exec_allow_net = bool(
        intent_spec.operating_bounds.allow_network if intent_spec.operating_bounds is not None else False
    )
    design = SystemDesignSpec(
        scope=scope,
        system_id=run_id,
        agents=(agent,),
        orchestration=OrchestrationSpec(
            steps=tuple(steps),
            execution_allow_network=exec_allow_net,
            network_allow_reason=("intent_operating_bounds_allow_network" if exec_allow_net else None),
        ),
        coordination=CoordinationSpec(edges=tuple(edges)),
    )
    json_artifact = design.to_artifact_json(
        directory=".akc/design",
        filename=f"{run_id}.system_design.json",
        metadata={"run_id": run_id, "kind": "system_design"},
    )

    md_artifact: OutputArtifact | None = None
    if emit_markdown_companion:
        intent_constraints = len(intent_spec.constraints)
        intent_success_criteria = len(intent_spec.success_criteria)
        knowledge_assertions = 0
        if isinstance(knowledge_snapshot, Mapping):
            raw_constraints = knowledge_snapshot.get("canonical_constraints")
            if isinstance(raw_constraints, Sequence):
                knowledge_assertions = len(list(raw_constraints))
        md_text = "\n".join(
            [
                "# System Design",
                "",
                f"- run_id: `{run_id}`",
                f"- tenant_id: `{scope.tenant_id}`",
                f"- repo_id: `{scope.repo_id}`",
                f"- ir_nodes: `{len(ir_document.nodes)}`",
                f"- orchestration_steps: `{len(steps)}`",
                f"- intent_constraints: `{intent_constraints}`",
                f"- intent_success_criteria: `{intent_success_criteria}`",
                f"- knowledge_canonical_constraints: `{knowledge_assertions}`",
                "",
            ]
        )
        md_artifact = OutputArtifact.from_text(
            path=f".akc/design/{run_id}.system_design.md",
            text=md_text,
            media_type="text/markdown; charset=utf-8",
            metadata={"run_id": run_id, "kind": "system_design_markdown"},
        )

    return SystemDesignPassResult(
        artifact_json=json_artifact,
        artifact_md=md_artifact,
        output_sha256=json_artifact.sha256_hex(),
        metadata={
            "run_id": run_id,
            "ir_node_count": len(ir_document.nodes),
            "orchestration_step_count": len(steps),
            "includes_markdown": md_artifact is not None,
        },
    )


def _render_python_orchestrator_stub(*, spec_path: str) -> str:
    return "\n".join(
        [
            "from __future__ import annotations",
            "",
            "import json",
            "from pathlib import Path",
            "from typing import Any",
            "",
            "",
            "class OrchestratorSpecError(RuntimeError):",
            '    """Raised when orchestration spec validation fails."""',
            "",
            "",
            "def _load_spec(*, path: str | Path, tenant_id: str, repo_id: str) -> dict[str, Any]:",
            '    """Load orchestration spec and enforce tenant/repo isolation."""',
            '    payload = json.loads(Path(path).read_text(encoding="utf-8"))',
            "    if not isinstance(payload, dict):",
            '        raise OrchestratorSpecError("orchestration spec must be a JSON object")',
            '    if payload.get("tenant_id") != tenant_id or payload.get("repo_id") != repo_id:',
            "        raise OrchestratorSpecError(",
            '            "tenant/repo scope mismatch for orchestration spec"',
            "        )",
            '    steps_raw = payload.get("steps")',
            "    if not isinstance(steps_raw, list) or not steps_raw:",
            '        raise OrchestratorSpecError("orchestration.steps must be a non-empty list")',
            "    return payload",
            "",
            "",
            'def run_orchestrator(*, tenant_id: str, repo_id: str, spec_path: str = "' + spec_path + '") -> list[str]:',
            '    """Run orchestration steps deterministically and return ordered step IDs."""',
            "    spec = _load_spec(path=spec_path, tenant_id=tenant_id, repo_id=repo_id)",
            "    steps = sorted(",
            '        [x for x in spec.get("steps", []) if isinstance(x, dict)],',
            '        key=lambda x: (int(x.get("order_idx", 0)), str(x.get("step_id", ""))),',
            "    )",
            "    ordered: list[str] = []",
            "    for step in steps:",
            '        step_id = str(step.get("step_id", "")).strip()',
            "        if not step_id:",
            '            raise OrchestratorSpecError("each orchestration step needs step_id")',
            "        ordered.append(step_id)",
            "    return ordered",
            "",
            "",
            'if __name__ == "__main__":',
            "    raise SystemExit(",
            '        "Use run_orchestrator(tenant_id=..., repo_id=...) from your runtime entrypoint."',
            "    )",
            "",
        ]
    )


def _render_typescript_orchestrator_stub(*, spec_path: str) -> str:
    return "\n".join(
        [
            'import { readFileSync } from "node:fs";',
            "",
            "export interface OrchestrationStep {",
            "  step_id: string;",
            "  order_idx: number;",
            "  role: string;",
            "  agent_name: string;",
            "}",
            "",
            "export interface OrchestrationSpec {",
            "  spec_version: number;",
            "  tenant_id: string;",
            "  repo_id: string;",
            "  run_id: string;",
            "  steps: OrchestrationStep[];",
            "}",
            "",
            "export class OrchestratorSpecError extends Error {}",
            "",
            "/** Load orchestration spec and enforce tenant/repo isolation. */",
            "export function loadOrchestrationSpec(",
            "  tenantId: string,",
            "  repoId: string,",
            '  specPath: string = "' + spec_path + '"',
            "): OrchestrationSpec {",
            '  const payload = JSON.parse(readFileSync(specPath, "utf-8")) as unknown;',
            '  if (typeof payload !== "object" || payload === null) {',
            '    throw new OrchestratorSpecError("orchestration spec must be a JSON object");',
            "  }",
            "  const spec = payload as Partial<OrchestrationSpec>;",
            "  if (spec.tenant_id !== tenantId || spec.repo_id !== repoId) {",
            '    throw new OrchestratorSpecError("tenant/repo scope mismatch for orchestration spec");',
            "  }",
            "  if (!Array.isArray(spec.steps) || spec.steps.length === 0) {",
            '    throw new OrchestratorSpecError("orchestration.steps must be a non-empty list");',
            "  }",
            "  return spec as OrchestrationSpec;",
            "}",
            "",
            "/** Execute steps in deterministic order and return ordered step IDs. */",
            "export function runOrchestrator(",
            "  tenantId: string,",
            "  repoId: string,",
            '  specPath: string = "' + spec_path + '"',
            "): string[] {",
            "  const spec = loadOrchestrationSpec(tenantId, repoId, specPath);",
            "  return [...spec.steps]",
            "    .sort((a, b) => (a.order_idx - b.order_idx) || a.step_id.localeCompare(b.step_id))",
            "    .map((s) => {",
            '      if (!s.step_id || typeof s.step_id !== "string") {',
            '        throw new OrchestratorSpecError("each orchestration step needs step_id");',
            "      }",
            "      return s.step_id;",
            "    });",
            "}",
            "",
        ]
    )


_COORDINATION_ROLE_CYCLE: tuple[AgentRoleName, AgentRoleName, AgentRoleName] = (
    "planner",
    "retriever",
    "writer",
)


def _coordination_role_for_workflow_index(idx: int) -> AgentRoleName:
    return _COORDINATION_ROLE_CYCLE[idx % 3]


def _build_coordination_edges_from_parallel_groups(
    *,
    step_ids: tuple[str, ...],
    parallel_groups: tuple[tuple[str, ...], ...],
) -> tuple[list[CoordinationEdgeSpec], int]:
    """Emit fork (parallel), join (barrier), and sequential role handoffs between layers."""

    idx_by_step: dict[str, int] = {sid: i for i, sid in enumerate(step_ids)}
    edges: list[CoordinationEdgeSpec] = []
    edge_counter = 0

    def _next_edge_id() -> str:
        nonlocal edge_counter
        edge_counter += 1
        return f"edge_{edge_counter:04d}"

    uses_reserved_kinds = False

    if len(step_ids) == 1:
        edges.append(
            CoordinationEdgeSpec(
                edge_id=_next_edge_id(),
                kind="depends_on",
                src_step_id=step_ids[0],
                dst_step_id=step_ids[0],
            )
        )
        return edges, 1

    groups = list(parallel_groups)
    for gi in range(1, len(groups)):
        prev = tuple(sorted(groups[gi - 1]))
        curr = tuple(sorted(groups[gi]))
        lp, lc = len(prev), len(curr)
        if lp == 1 and lc == 1:
            s0, d0 = prev[0], curr[0]
            fr = _coordination_role_for_workflow_index(idx_by_step[s0])
            tr = _coordination_role_for_workflow_index(idx_by_step[d0])
            hid = f"handoff_{s0}_to_{d0}"
            edges.append(
                CoordinationEdgeSpec(
                    edge_id=_next_edge_id(),
                    kind="handoff",
                    src_step_id=s0,
                    dst_step_id=d0,
                    metadata={
                        "handoff_id": hid,
                        "from_role": fr,
                        "to_role": tr,
                    },
                )
            )
            uses_reserved_kinds = True
        elif lp == 1 and lc > 1:
            s0 = prev[0]
            for d in curr:
                edges.append(
                    CoordinationEdgeSpec(
                        edge_id=_next_edge_id(),
                        kind="parallel",
                        src_step_id=s0,
                        dst_step_id=d,
                    )
                )
            uses_reserved_kinds = True
        elif lp > 1:
            for s in prev:
                for d in curr:
                    edges.append(
                        CoordinationEdgeSpec(
                            edge_id=_next_edge_id(),
                            kind="barrier",
                            src_step_id=s,
                            dst_step_id=d,
                        )
                    )
            uses_reserved_kinds = True

    spec_version = 2 if uses_reserved_kinds else 1
    return edges, spec_version


def run_orchestration_spec_pass(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_spec: IntentSpec,
) -> OrchestrationSpecPassResult:
    scope = TenantRepoScope(tenant_id=ir_document.tenant_id, repo_id=ir_document.repo_id)
    workflow_nodes = list(sorted_workflow_nodes_for_coordination_emit(ir_document.nodes))
    steps: list[dict[str, JSONValue]] = []
    for idx, node in enumerate(workflow_nodes):
        steps.append(
            {
                "step_id": f"workflow_{idx:03d}",
                "order_idx": idx,
                "agent_name": "compile_controller",
                "role": cast(JSONValue, _coordination_role_for_workflow_index(idx)),
                "ir_node_id": node.id,
                "ir_node_name": node.name,
                "status": str(node.properties.get("status", "pending")),
            }
        )
    if not steps:
        steps.append(
            {
                "step_id": "workflow_000",
                "order_idx": 0,
                "agent_name": "compile_controller",
                "role": "writer",
                "status": "pending",
            }
        )

    execution_allow_network = bool(
        intent_spec.operating_bounds.allow_network if intent_spec.operating_bounds is not None else False
    )
    orchestration_spec = OrchestrationSpec(
        spec_version=1,
        max_parallel_steps=1,
        execution_allow_network=execution_allow_network,
        network_allow_reason=("intent_operating_bounds_allow_network" if execution_allow_network else None),
        roles=("planner", "retriever", "writer"),
        trigger_sources=("compile_session", "intent_spec", "ir_document"),
        io_contract={
            str(step["step_id"]): {
                "inputs": sorted(str(key) for key in step),
                "outputs": ["execution_status", "artifact_paths"],
            }
            for step in steps
        },
        policies={
            "replay_budget_mode": "deterministic_artifact_pass",
            "max_parallel_steps": 1,
        },
        state_machine={
            "initial_state": str(steps[0]["step_id"]),
            "states": [str(step["step_id"]) for step in steps],
            "transitions": [
                {
                    "from": str(steps[idx]["step_id"]),
                    "to": str(steps[idx + 1]["step_id"]),
                    "event": "step_completed",
                }
                for idx in range(len(steps) - 1)
            ],
        },
        steps=tuple(
            OrchestrationStepSpec(
                step_id=str(step["step_id"]),
                order_idx=int(cast(int, step["order_idx"])),
                agent_name=str(step["agent_name"]),
                role=cast(AgentRoleName, step["role"]),
                inputs={
                    "ir_node_id": step.get("ir_node_id"),
                    "ir_node_name": step.get("ir_node_name"),
                    "status": step.get("status"),
                },
            )
            for step in steps
        ),
    )
    orchestration_obj: dict[str, JSONValue] = {
        "run_id": run_id,
        "tenant_id": scope.tenant_id,
        "repo_id": scope.repo_id,
        **orchestration_spec.to_json_obj(),
    }

    json_artifact = OutputArtifact.from_json(
        path=f".akc/orchestration/{run_id}.orchestration.json",
        obj=orchestration_obj,
        metadata={"run_id": run_id, "kind": "orchestration_spec"},
    )
    python_artifact = OutputArtifact.from_text(
        path=f".akc/orchestration/{run_id}.orchestrator.py",
        text=_render_python_orchestrator_stub(spec_path=f".akc/orchestration/{run_id}.orchestration.json"),
        media_type="text/x-python; charset=utf-8",
        metadata={"run_id": run_id, "kind": "orchestrator_stub_python"},
    )
    typescript_artifact = OutputArtifact.from_text(
        path=f".akc/orchestration/{run_id}.orchestrator.ts",
        text=_render_typescript_orchestrator_stub(spec_path=f".akc/orchestration/{run_id}.orchestration.json"),
        media_type="application/typescript; charset=utf-8",
        metadata={"run_id": run_id, "kind": "orchestrator_stub_typescript"},
    )

    return OrchestrationSpecPassResult(
        artifact_json=json_artifact,
        artifact_python_stub=python_artifact,
        artifact_typescript_stub=typescript_artifact,
        output_sha256=json_artifact.sha256_hex(),
        metadata={
            "run_id": run_id,
            "ir_node_count": len(ir_document.nodes),
            "orchestration_step_count": len(steps),
            "python_stub_path": python_artifact.path,
            "typescript_stub_path": typescript_artifact.path,
        },
    )


def _coordination_typescript_sdk_template() -> str:
    """Load the canonical TS SDK bundled with ``akc.coordination`` (embedded into emitted artifacts)."""

    import akc.coordination as coordination_pkg

    root = Path(coordination_pkg.__file__).resolve().parent
    path = root / "static" / "coordination_sdk.ts"
    if not path.is_file():
        msg = f"coordination TypeScript SDK template missing: {path}"
        raise FileNotFoundError(msg)
    return path.read_text(encoding="utf-8")


def _render_python_coordination_protocol_stub(*, spec_path: str) -> str:
    default_path_literal = json.dumps(spec_path)
    return "\n".join(
        [
            "from __future__ import annotations",
            "",
            "import argparse",
            "import json",
            "import sys",
            "",
            "from akc.coordination.models import CoordinationSchedule",
            "from akc.coordination.protocol import (",
            "    coordination_schedule_to_jsonable,",
            "    load_coordination_spec_file,",
            "    schedule_coordination,",
            ")",
            "",
            f"DEFAULT_COORDINATION_SPEC_PATH = {default_path_literal}",
            "",
            "",
            "def run_coordination_protocol(",
            "    *,",
            "    tenant_id: str,",
            "    repo_id: str,",
            "    spec_path: str = DEFAULT_COORDINATION_SPEC_PATH,",
            ") -> CoordinationSchedule:",
            '    """Deterministic coordination step schedule (same graph walk as the AKC runtime kernel)."""',
            "    spec = load_coordination_spec_file(path=spec_path, tenant_id=tenant_id, repo_id=repo_id)",
            "    return schedule_coordination(spec)",
            "",
            "",
            "def main() -> None:",
            '    p = argparse.ArgumentParser(description="AKC coordination protocol — print schedule JSON.")',
            '    p.add_argument("--tenant-id", required=True)',
            '    p.add_argument("--repo-id", required=True)',
            '    p.add_argument("--spec-path", default=DEFAULT_COORDINATION_SPEC_PATH)',
            "    args = p.parse_args()",
            "    sched = run_coordination_protocol(",
            "        tenant_id=args.tenant_id, repo_id=args.repo_id, spec_path=args.spec_path",
            "    )",
            "    json.dump(coordination_schedule_to_jsonable(sched), sys.stdout, indent=2, sort_keys=True)",
            '    sys.stdout.write("\\n")',
            "",
            "",
            'if __name__ == "__main__":',
            "    main()",
            "",
        ]
    )


def _render_typescript_coordination_protocol_stub(*, spec_path: str) -> str:
    sdk = _coordination_typescript_sdk_template()
    default_ts = json.dumps(spec_path)
    suffix = f"""
// --- generated default path (this compile emit) ---
export const DEFAULT_COORDINATION_SPEC_PATH: string = {default_ts};

export function runCoordinationProtocol(
  tenantId: string,
  repoId: string,
  specPath: string = DEFAULT_COORDINATION_SPEC_PATH,
): CoordinationSchedule {{
  const spec = loadCoordinationSpec(tenantId, repoId, specPath);
  return scheduleCoordination(spec);
}}

if (coordinationIsMainModule(process.argv, import.meta.url)) {{
  coordinationRunCli(process.argv, DEFAULT_COORDINATION_SPEC_PATH);
}}
"""
    return f"{sdk.rstrip()}\n{suffix}"


def run_agent_coordination_pass(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_spec: IntentSpec,
) -> AgentCoordinationPassResult:
    scope = TenantRepoScope(tenant_id=ir_document.tenant_id, repo_id=ir_document.repo_id)
    llm_params: dict[str, JSONValue] = {}
    if intent_spec.operating_bounds is not None and intent_spec.operating_bounds.max_output_tokens:
        llm_params["max_output_limit"] = int(intent_spec.operating_bounds.max_output_tokens)
    base_agent = AgentSpec(
        scope=scope,
        name="compile_controller",
        llm=LlmBackendSpec(backend="akc", model="deterministic_coordination", params=llm_params),
        roles=(
            AgentRoleSpec(name="planner", tools=("llm.complete",)),
            AgentRoleSpec(name="retriever", tools=("llm.complete",)),
            AgentRoleSpec(name="writer", tools=("executor.run",)),
            AgentRoleSpec(name="verifier", tools=("executor.run",)),
        ),
        metadata={"run_id": run_id, "kind": "coordination_base_agent"},
    )
    workflow_nodes = list(sorted_workflow_nodes_for_coordination_emit(ir_document.nodes))
    step_ids = (
        tuple(f"workflow_{idx:03d}" for idx in range(len(workflow_nodes))) if workflow_nodes else ("workflow_000",)
    )
    parallel_groups: tuple[tuple[str, ...], ...]
    if workflow_nodes:
        pg_list: list[tuple[str, ...]] = []
        for _lk, grp in groupby(
            enumerate(workflow_nodes),
            key=lambda ix_n: workflow_coordination_layer_key(ix_n[1].properties),
        ):
            pg_list.append(tuple(step_ids[i] for i, _n in grp))
        parallel_groups = tuple(pg_list)
    else:
        parallel_groups = (("workflow_000",),)
    coordination_edges, coord_spec_version = _build_coordination_edges_from_parallel_groups(
        step_ids=step_ids,
        parallel_groups=parallel_groups,
    )
    role_specs = sorted(base_agent.roles, key=lambda role: str(role.name))
    role_nodes: list[dict[str, JSONValue]] = []
    for role in role_specs:
        role_nodes.append(
            {
                "node_id": f"role_{role.name}",
                "kind": "role",
                "label": role.name,
                "tools": [str(t) for t in role.tools],
            }
        )
    step_nodes: list[dict[str, JSONValue]] = []
    for idx, step_id in enumerate(step_ids):
        step_nodes.append(
            {
                "node_id": step_id,
                "kind": "step",
                "label": f"workflow_step_{idx}",
            }
        )
    intent_allow_net = bool(
        intent_spec.operating_bounds.allow_network if intent_spec.operating_bounds is not None else False
    )
    role_profiles: dict[str, JSONValue] = {}
    for role in role_specs:
        tools = sorted({str(t) for t in role.tools})
        role_net = intent_allow_net
        if intent_allow_net and str(role.name) in ("planner", "retriever"):
            role_net = False
        role_profiles[str(role.name)] = cast(
            JSONValue,
            {
                "filesystem_scope": {
                    "read_only_roots": ["."],
                    "scratch_subdir": "scratch",
                },
                "allowed_tools": tools,
                "execution_allow_network": role_net,
            },
        )
    coordination_obj: dict[str, JSONValue] = {
        "spec_version": coord_spec_version,
        "run_id": run_id,
        "tenant_id": scope.tenant_id,
        "repo_id": scope.repo_id,
        "agent_spec": base_agent.to_json_obj(),
        "agent_roles": [role.to_json_obj() for role in role_specs],
        "coordination_graph": cast(
            JSONValue,
            {
                "nodes": cast(JSONValue, role_nodes + step_nodes),
                "edges": cast(JSONValue, [edge.to_json_obj() for edge in coordination_edges]),
            },
        ),
        "orchestration_bindings": cast(
            JSONValue,
            [
                {
                    "role_name": role.name,
                    "agent_name": base_agent.name,
                    "orchestration_step_ids": cast(JSONValue, list(step_ids)),
                }
                for role in role_specs
            ],
        ),
        "governance": cast(
            JSONValue,
            {
                "max_steps": len(step_ids),
                "allowed_capabilities": cast(
                    JSONValue,
                    sorted({str(tool) for role in role_specs for tool in role.tools}),
                ),
                "execution_allow_network": intent_allow_net,
                "role_profiles": cast(JSONValue, role_profiles),
            },
        ),
        "tenant_isolation_contract": cast(
            JSONValue,
            {
                "require_scope_match": True,
                "scope_fields": cast(JSONValue, ["tenant_id", "repo_id"]),
            },
        ),
    }
    json_artifact = OutputArtifact.from_json(
        path=f".akc/agents/{run_id}.coordination.json",
        obj=coordination_obj,
        metadata={"run_id": run_id, "kind": "agent_coordination_spec"},
    )
    python_artifact = OutputArtifact.from_text(
        path=f".akc/agents/{run_id}.coordination_protocol.py",
        text=_render_python_coordination_protocol_stub(spec_path=f".akc/agents/{run_id}.coordination.json"),
        media_type="text/x-python; charset=utf-8",
        metadata={"run_id": run_id, "kind": "coordination_protocol_python"},
    )
    typescript_artifact = OutputArtifact.from_text(
        path=f".akc/agents/{run_id}.coordination_protocol.ts",
        text=_render_typescript_coordination_protocol_stub(spec_path=f".akc/agents/{run_id}.coordination.json"),
        media_type="application/typescript; charset=utf-8",
        metadata={"run_id": run_id, "kind": "coordination_protocol_typescript"},
    )
    return AgentCoordinationPassResult(
        artifact_json=json_artifact,
        artifact_python_stub=python_artifact,
        artifact_typescript_stub=typescript_artifact,
        output_sha256=json_artifact.sha256_hex(),
        metadata={
            "run_id": run_id,
            "workflow_node_count": len(workflow_nodes),
            "coordination_edge_count": len(coordination_edges),
            "role_count": len(role_specs),
            "python_stub_path": python_artifact.path,
            "typescript_stub_path": typescript_artifact.path,
        },
    )


def _step_ir_node_id(step: Mapping[str, Any]) -> str | None:
    """Extract `ir_node_id` from orchestration step JSON (`inputs` or legacy top-level)."""
    inputs = step.get("inputs")
    if isinstance(inputs, Mapping):
        v = inputs.get("ir_node_id")
        if isinstance(v, str) and v.strip():
            return v.strip()
    v = step.get("ir_node_id")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _transitive_depends_on_closure(*, seed_ids: set[str], nodes: tuple[IRNode, ...]) -> set[str]:
    by_id = {n.id: n for n in nodes}
    closure: set[str] = {nid for nid in seed_ids if nid in by_id}
    changed = True
    while changed:
        changed = False
        for nid in list(closure):
            node = by_id.get(nid)
            if node is None:
                continue
            for dep in node.depends_on:
                if dep in by_id and dep not in closure:
                    closure.add(dep)
                    changed = True
    return closure


def _coordination_seed_ir_nodes(
    *,
    orchestration_obj: Mapping[str, Any],
    coordination_obj: Mapping[str, Any],
) -> set[str]:
    """Resolve coordination bindings to IR node ids via orchestration step `ir_node_id` fields."""
    step_map: dict[str, str] = {}
    steps = orchestration_obj.get("steps")
    if isinstance(steps, Sequence) and not isinstance(steps, (str, bytes)):
        for step in steps:
            if not isinstance(step, Mapping):
                continue
            sid = step.get("step_id")
            if not isinstance(sid, str) or not sid.strip():
                continue
            irn = _step_ir_node_id(step)
            if irn is not None:
                step_map[sid.strip()] = irn

    seeds: set[str] = set()
    bindings = coordination_obj.get("orchestration_bindings")
    if not isinstance(bindings, Sequence) or isinstance(bindings, (str, bytes)):
        return seeds
    for binding in bindings:
        if not isinstance(binding, Mapping):
            continue
        step_ids = binding.get("orchestration_step_ids")
        if not isinstance(step_ids, Sequence) or isinstance(step_ids, (str, bytes)):
            continue
        for raw_sid in step_ids:
            sid = str(raw_sid).strip()
            irn = step_map.get(sid)
            if irn is not None:
                seeds.add(irn)
    return seeds


def _select_runtime_bundle_ir_node_ids(
    *,
    ir_document: IRDocument,
    orchestration_obj: Mapping[str, Any],
    coordination_obj: Mapping[str, Any],
) -> set[str]:
    """
    Deterministic seed → subgraph closure over `depends_on`, plus all `knowledge` hubs.

    Prefer orchestration/coordination `ir_node_id` wiring; otherwise infer seeds from
    nodes that carry contracts, then workflow nodes, then the full graph (legacy fallback).
    """
    nodes = ir_document.nodes
    seeds: set[str] = set()
    orchestration_steps = orchestration_obj.get("steps")
    if isinstance(orchestration_steps, Sequence) and not isinstance(orchestration_steps, (str, bytes)):
        for step in orchestration_steps:
            if not isinstance(step, Mapping):
                continue
            irn = _step_ir_node_id(step)
            if irn is not None:
                seeds.add(irn)
    seeds |= _coordination_seed_ir_nodes(
        orchestration_obj=orchestration_obj,
        coordination_obj=coordination_obj,
    )

    if not seeds:
        seeds = {n.id for n in nodes if n.contract is not None}
    if not seeds:
        seeds = {n.id for n in nodes if n.kind == "workflow"}
    if not seeds:
        seeds = {n.id for n in nodes}

    closure = _transitive_depends_on_closure(seed_ids=seeds, nodes=nodes)
    closure |= {n.id for n in nodes if n.kind == "knowledge"}
    return closure


def _default_deployment_provider_contract() -> dict[str, JSONValue]:
    return {
        "kind": "in_memory",
        "mutation_mode": "observe_only",
        "rollback_mode": "none",
        "rollback_determinism": "deterministic",
        "required_env_flags": cast(JSONValue, []),
        "required_policy_actions": cast(JSONValue, []),
    }


def _default_workflow_execution_contract() -> dict[str, JSONValue]:
    return {
        "allowed_routes": cast(JSONValue, ["delegate_adapter", "noop", "subprocess", "http"]),
        "required_policy_actions_by_route": cast(
            JSONValue,
            {
                "subprocess": ["runtime.action.execute.subprocess"],
                "http": ["runtime.action.execute.http"],
            },
        ),
    }


def _default_coordination_execution_contract() -> dict[str, JSONValue]:
    # Stage B default flip: coordination dispatch is parallel-by-default with conservative caps.
    return {
        "parallel_dispatch_enabled": True,
        "max_in_flight_steps": 4,
        "max_in_flight_per_role": 2,
        "completion_fold_order": "coordination_step_id",
    }


def run_delivery_plan_pass(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_spec: IntentSpec,
    orchestration_spec_text: str,
    coordination_spec_text: str,
) -> DeliveryPlanPassResult:
    orchestration_obj = parse_json_artifact_text(orchestration_spec_text)
    coordination_obj = parse_json_artifact_text(coordination_spec_text)
    delivery_plan_obj = build_delivery_plan(
        run_id=run_id,
        ir_document=ir_document,
        intent_obj=intent_spec.to_json_obj(),
        orchestration_obj=orchestration_obj,
        coordination_obj=coordination_obj,
    )
    artifact = OutputArtifact.from_json(
        path=f".akc/deployment/{run_id}.delivery_plan.json",
        obj=delivery_plan_obj,
        metadata={
            "run_id": run_id,
            "kind": "delivery_plan",
            "target_count": len(cast(list[Any], delivery_plan_obj.get("targets", []))),
            "required_human_inputs_count": len(cast(list[Any], delivery_plan_obj.get("required_human_inputs", []))),
        },
    )
    summary_md = render_delivery_summary_markdown(run_id=run_id, delivery_plan=delivery_plan_obj)
    summary_artifact = OutputArtifact.from_text(
        path=f".akc/design/{run_id}.delivery_summary.md",
        text=summary_md,
        media_type="text/markdown; charset=utf-8",
        metadata={"run_id": run_id, "kind": "delivery_summary_markdown", "companion_of": artifact.path},
    )
    return DeliveryPlanPassResult(
        artifact_json=artifact,
        artifact_summary_md=summary_artifact,
        output_sha256=artifact.sha256_hex(),
        metadata={
            "run_id": run_id,
            "delivery_plan_path": artifact.path,
            "delivery_summary_path": summary_artifact.path,
            "target_count": len(cast(list[Any], delivery_plan_obj.get("targets", []))),
            "required_human_inputs_count": len(cast(list[Any], delivery_plan_obj.get("required_human_inputs", []))),
            "promotion_readiness_status": str(
                cast(dict[str, Any], delivery_plan_obj.get("promotion_readiness", {})).get("status", "unknown")
            ),
            "includes_delivery_summary": True,
        },
    )


def run_runtime_bundle_pass(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_spec: IntentSpec,
    orchestration_spec_text: str,
    coordination_spec_text: str,
    delivery_plan_text: str | None = None,
    embed_system_ir: bool = False,
    runtime_bundle_schema_version: int = RUNTIME_BUNDLE_SCHEMA_VERSION,
    reconcile_deploy_targets_from_ir_only: bool = False,
    deployment_intents_ir_alignment: Literal["off", "strict"] = "off",
) -> RuntimeBundlePassResult:
    scope = TenantRepoScope(tenant_id=ir_document.tenant_id, repo_id=ir_document.repo_id)
    orchestration_obj = parse_json_artifact_text(orchestration_spec_text)
    coordination_obj = parse_json_artifact_text(coordination_spec_text)
    delivery_plan_obj = (
        parse_json_artifact_text(delivery_plan_text)
        if isinstance(delivery_plan_text, str) and delivery_plan_text
        else None
    )

    selected_ids = _select_runtime_bundle_ir_node_ids(
        ir_document=ir_document,
        orchestration_obj=orchestration_obj,
        coordination_obj=coordination_obj,
    )

    knowledge_hubs = [n for n in ir_document.nodes if n.kind == "knowledge"]
    knowledge_layer_ref: dict[str, JSONValue] | None = None
    if knowledge_hubs:
        hub = sorted(knowledge_hubs, key=lambda h: h.id)[0]
        props = dict(hub.properties)
        knowledge_layer_ref = {
            "knowledge_hub_node_id": hub.id,
            "knowledge_semantic_fingerprint_16": props.get("knowledge_semantic_fingerprint_16"),
            "knowledge_provenance_fingerprint_16": props.get("knowledge_provenance_fingerprint_16"),
            "persisted_snapshot_relpath": props.get("persisted_snapshot_relpath"),
            "knowledge_assertion_ids": props.get("knowledge_assertion_ids"),
        }

    referenced_nodes = sorted(
        (node for node in ir_document.nodes if node.id in selected_ids),
        key=lambda node: node.id,
    )
    referenced_contracts = sorted(
        (node.contract for node in referenced_nodes if node.contract is not None),
        key=lambda contract: contract.contract_id,
    )
    deployable_kinds = frozenset({"service", "integration", "infrastructure", "agent"})
    delivery_targets_by_id: dict[str, Mapping[str, Any]] = {}
    if isinstance(delivery_plan_obj, Mapping):
        for raw_target in cast(list[Any], delivery_plan_obj.get("targets", [])):
            if not isinstance(raw_target, Mapping):
                continue
            tid = raw_target.get("target_id")
            if isinstance(tid, str) and tid.strip():
                delivery_targets_by_id[tid.strip()] = raw_target
    deployment_intents: list[dict[str, JSONValue]] = []
    for node in sorted(
        (n for n in referenced_nodes if n.kind in deployable_kinds),
        key=lambda n: n.id,
    ):
        projected = delivery_targets_by_id.get(node.id)
        deployment_intents.append(
            {
                "node_id": node.id,
                "kind": node.kind,
                "name": node.name,
                "depends_on": list(node.depends_on),
                "effects": node.effects.to_json_obj() if node.effects is not None else None,
                "contract_id": node.contract.contract_id if node.contract is not None else None,
                "target_class": (str(projected.get("target_class")) if isinstance(projected, Mapping) else "unknown"),
                "environment_support": (
                    sorted(str(k) for k in cast(dict[str, Any], projected.get("supported_delivery_paths", {})))
                    if isinstance(projected, Mapping)
                    else ["local", "staging", "production"]
                ),
                "delivery_paths": (
                    projected.get("supported_delivery_paths")
                    if isinstance(projected, Mapping)
                    else {"local": ["direct_apply"]}
                ),
                "operational_profile_fingerprint": (
                    stable_json_fingerprint(dict(projected)) if isinstance(projected, Mapping) else None
                ),
            }
        )

    allow_network, renv_knowledge = effective_allow_network_for_handoff(
        ir_document=ir_document,
        intent_spec=intent_spec,
    )
    runtime_intent_projection = project_runtime_intent_projection(intent=intent_spec)
    intent_ref = build_handoff_intent_ref(intent=intent_spec)
    runtime_policy_envelope: dict[str, Any] = {
        "tenant_id": scope.tenant_id,
        "repo_id": scope.repo_id,
        "run_id": run_id,
        "allow_network": allow_network,
        "network_allow_reason": ("intent_operating_bounds_allow_network" if allow_network else None),
        "tenant_isolation_required": True,
        "adapter_fallback_mode": "native",
        **renv_knowledge,
    }
    if intent_spec.success_criteria:
        runtime_policy_envelope["require_reconcile_evidence"] = True

    _gov_chk = coordination_obj.get("governance")
    iso_issues = validate_role_profiles_network_vs_bundle(
        bundle_allow_network=allow_network,
        governance=_gov_chk if isinstance(_gov_chk, Mapping) else None,
    )
    if iso_issues:
        raise ValueError("; ".join(iso_issues))
    gov_raw = coordination_obj.get("governance")
    role_iso_snapshot: dict[str, JSONValue] = {}
    if isinstance(gov_raw, Mapping):
        rp = gov_raw.get("role_profiles")
        if isinstance(rp, Mapping):
            role_iso_snapshot = dict(rp)
    runtime_policy_envelope["coordination_role_isolation_profiles"] = role_iso_snapshot

    ir_relpath = f".akc/ir/{run_id}.json"
    system_ir_ref: dict[str, JSONValue] = {
        "path": ir_relpath,
        "fingerprint": ir_document.fingerprint(),
        "format_version": ir_document.format_version,
        "schema_version": int(ir_document.schema_version),
    }

    coordination_ref: dict[str, JSONValue] = {
        "path": f".akc/agents/{run_id}.coordination.json",
        "fingerprint": stable_json_fingerprint(coordination_obj),
    }
    bundle_payload: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": scope.tenant_id,
        "repo_id": scope.repo_id,
        "system_ir_ref": system_ir_ref,
        "coordination_ref": coordination_ref,
        "intent_ref": dict(intent_ref),
        "intent_policy_projection": runtime_intent_projection.to_json_obj(),
        "referenced_ir_nodes": [node.to_json_obj() for node in referenced_nodes],
        "referenced_contracts": [contract.to_json_obj() for contract in referenced_contracts],
        "spec_hashes": {
            "orchestration_spec_sha256": stable_json_fingerprint(orchestration_obj),
            "coordination_spec_sha256": stable_json_fingerprint(coordination_obj),
        },
        "deployment_intents": deployment_intents,
        "delivery_plan_ref": (
            {
                "path": f".akc/deployment/{run_id}.delivery_plan.json",
                "fingerprint": stable_json_fingerprint(delivery_plan_obj),
            }
            if isinstance(delivery_plan_obj, Mapping)
            else None
        ),
        "promotion_readiness": (
            cast(dict[str, Any], delivery_plan_obj.get("promotion_readiness"))
            if isinstance(delivery_plan_obj, Mapping)
            else {"status": "unknown"}
        ),
        "runtime_policy_envelope": runtime_policy_envelope,
        "deployment_provider_contract": _default_deployment_provider_contract(),
        "workflow_execution_contract": _default_workflow_execution_contract(),
        "coordination_execution_contract": _default_coordination_execution_contract(),
        # Compile always emits system_ir_ref (or embedded IR): IR fingerprints are the
        # authoritative desired-state lane unless a hand-authored bundle overrides.
        "reconcile_desired_state_source": "ir",
    }
    if int(runtime_bundle_schema_version) >= 4:
        if deployment_intents_ir_alignment not in {"off", "strict"}:
            raise ValueError("deployment_intents_ir_alignment must be off or strict")
        if deployment_intents_ir_alignment == "strict":
            bundle_payload["deployment_intents_ir_alignment"] = "strict"
        if reconcile_deploy_targets_from_ir_only:
            bundle_payload["reconcile_deploy_targets_from_ir_only"] = True
    evidence_expectations = derive_runtime_evidence_expectations(
        projection=runtime_intent_projection.to_json_obj(),
        policy_envelope=runtime_policy_envelope,
    )
    if evidence_expectations:
        bundle_payload["runtime_evidence_expectations"] = list(evidence_expectations)
    if int(runtime_bundle_schema_version) >= 3:
        bundle_payload["embed_system_ir"] = bool(embed_system_ir)
    if embed_system_ir:
        bundle_payload["system_ir"] = ir_document.to_json_obj()
    if knowledge_layer_ref is not None:
        bundle_payload["knowledge_layer_ref"] = knowledge_layer_ref
    bundle_obj = apply_schema_envelope(
        obj=bundle_payload,
        kind="runtime_bundle",
        version=int(runtime_bundle_schema_version),
    )
    json_artifact = OutputArtifact.from_json(
        path=f".akc/runtime/{run_id}.runtime_bundle.json",
        obj=bundle_obj,
        metadata={"run_id": run_id, "kind": "runtime_bundle"},
    )
    return RuntimeBundlePassResult(
        artifact_json=json_artifact,
        output_sha256=json_artifact.sha256_hex(),
        metadata={
            "run_id": run_id,
            "runtime_bundle_path": json_artifact.path,
            "intent_id": runtime_intent_projection.intent_id,
            "stable_intent_sha256": runtime_intent_projection.stable_intent_sha256,
            "referenced_node_count": len(referenced_nodes),
            "referenced_contract_count": len(referenced_contracts),
            "deployment_intent_count": len(deployment_intents),
            "system_ir_path": ir_relpath,
            "system_ir_fingerprint": ir_document.fingerprint(),
            "ir_format_version": ir_document.format_version,
            "embed_system_ir": bool(embed_system_ir),
            "runtime_bundle_schema_version": int(runtime_bundle_schema_version),
            "orchestration_spec_sha256": stable_json_fingerprint(orchestration_obj),
            "coordination_spec_sha256": stable_json_fingerprint(coordination_obj),
        },
    )


def _validate_docker_compose_hardening(*, compose_obj: Mapping[str, Any]) -> None:
    services = compose_obj.get("services")
    if not isinstance(services, Mapping) or not services:
        raise ValueError("docker-compose services must be a non-empty mapping")
    for service_name, raw_service in services.items():
        if not isinstance(raw_service, Mapping):
            raise ValueError(f"docker-compose service `{service_name}` must be an object")
        if raw_service.get("read_only") is not True:
            raise ValueError(f"docker-compose service `{service_name}` must set read_only=true")
        security_opt = raw_service.get("security_opt")
        if not isinstance(security_opt, Sequence) or "no-new-privileges:true" not in security_opt:
            raise ValueError(f"docker-compose service `{service_name}` must include no-new-privileges")
        cap_drop = raw_service.get("cap_drop")
        if not isinstance(cap_drop, Sequence) or "ALL" not in cap_drop:
            raise ValueError(f"docker-compose service `{service_name}` must drop ALL capabilities")
        if "cap_add" in raw_service:
            cap_add = raw_service.get("cap_add")
            if not isinstance(cap_add, Sequence) or len(cap_add) > 0:
                raise ValueError(f"docker-compose service `{service_name}` cannot request cap_add by default")
        tmpfs = raw_service.get("tmpfs")
        if not isinstance(tmpfs, Sequence) or "/tmp" not in tmpfs:
            raise ValueError(f"docker-compose service `{service_name}` must mount tmpfs /tmp")
        user_val = raw_service.get("user")
        if not isinstance(user_val, str) or user_val.strip() in {"", "0", "root"}:
            raise ValueError(f"docker-compose service `{service_name}` must run as non-root user")


def _validate_k8s_restricted_security_context(*, deployment_obj: Mapping[str, Any]) -> None:
    spec = deployment_obj.get("spec")
    if not isinstance(spec, Mapping):
        raise ValueError("k8s deployment spec must be an object")
    template = spec.get("template")
    if not isinstance(template, Mapping):
        raise ValueError("k8s deployment template must be an object")
    template_spec = template.get("spec")
    if not isinstance(template_spec, Mapping):
        raise ValueError("k8s deployment template.spec must be an object")
    pod_security_context = template_spec.get("securityContext")
    if not isinstance(pod_security_context, Mapping):
        raise ValueError("k8s pod securityContext must be set")
    if pod_security_context.get("runAsNonRoot") is not True:
        raise ValueError("k8s pod securityContext.runAsNonRoot must be true")
    containers = template_spec.get("containers")
    if not isinstance(containers, Sequence) or not containers:
        raise ValueError("k8s deployment template.spec.containers must be non-empty")
    volumes = template_spec.get("volumes")
    if isinstance(volumes, Sequence):
        for vol in volumes:
            if isinstance(vol, Mapping) and "hostPath" in vol:
                raise ValueError("k8s hostPath volumes are prohibited")
    for container in containers:
        if not isinstance(container, Mapping):
            raise ValueError("k8s container entries must be objects")
        sc = container.get("securityContext")
        if not isinstance(sc, Mapping):
            raise ValueError("k8s container securityContext must be set")
        if sc.get("allowPrivilegeEscalation") is not False:
            raise ValueError("k8s allowPrivilegeEscalation must be false")
        if sc.get("readOnlyRootFilesystem") is not True:
            raise ValueError("k8s readOnlyRootFilesystem must be true")
        if sc.get("privileged") is True:
            raise ValueError("k8s privileged containers are prohibited")
        caps = sc.get("capabilities")
        if not isinstance(caps, Mapping):
            raise ValueError("k8s capabilities must be set")
        dropped = caps.get("drop")
        if not isinstance(dropped, Sequence) or "ALL" not in dropped:
            raise ValueError("k8s capabilities.drop must include ALL")


def _deployment_k8s_intent_annotations(*, intent_ref: Mapping[str, JSONValue]) -> dict[str, str]:
    """Kubernetes annotations derived only from the handoff ``intent_ref`` block."""

    return {
        "akc.run/intent-id": str(intent_ref["intent_id"]),
        "akc.run/stable-intent-sha256": str(intent_ref["stable_intent_sha256"]),
    }


def _deployment_compose_intent_labels(*, intent_ref: Mapping[str, JSONValue]) -> dict[str, str]:
    """Docker Compose service labels mirroring the same ``intent_ref`` identity."""

    return {
        "com.akc.run.intent-id": str(intent_ref["intent_id"]),
        "com.akc.run.stable-intent-sha256": str(intent_ref["stable_intent_sha256"]),
    }


def _deployment_output_artifact_metadata(
    *,
    run_id: str,
    kind: str,
    intent_ref: Mapping[str, JSONValue],
    intent_projection: Mapping[str, JSONValue],
) -> dict[str, JSONValue]:
    return {
        "run_id": run_id,
        "kind": kind,
        "intent_ref": dict(intent_ref),
        "intent_projection": dict(intent_projection),
    }


def _validate_workflow_policy(*, workflow_obj: Mapping[str, Any]) -> None:
    trigger = workflow_obj.get("on")
    trigger_keys: set[str] = set()
    if isinstance(trigger, Mapping):
        trigger_keys = {str(k) for k in trigger}
    elif isinstance(trigger, Sequence):
        trigger_keys = {str(item) for item in trigger}
    if "pull_request_target" in trigger_keys:
        raise ValueError("workflow cannot use pull_request_target")
    jobs = workflow_obj.get("jobs")
    if not isinstance(jobs, Mapping) or not jobs:
        raise ValueError("workflow jobs must be a non-empty mapping")
    is_pr_workflow = "pull_request" in trigger_keys
    for job_id, raw_job in jobs.items():
        if not isinstance(raw_job, Mapping):
            raise ValueError(f"workflow job `{job_id}` must be an object")
        permissions = raw_job.get("permissions")
        if isinstance(permissions, Mapping):
            for permission_name, permission_value in permissions.items():
                if (
                    is_pr_workflow
                    and str(permission_value).lower() == "write"
                    and str(permission_name)
                    in {
                        "contents",
                        "id-token",
                        "actions",
                        "security-events",
                        "packages",
                        "pull-requests",
                    }
                ):
                    raise ValueError(f"workflow job `{job_id}` has prohibited write permission in PR context")
        steps = raw_job.get("steps")
        if not isinstance(steps, Sequence) or not steps:
            raise ValueError(f"workflow job `{job_id}` must define steps")
        if is_pr_workflow:
            for step in steps:
                if not isinstance(step, Mapping):
                    continue
                run_cmd = step.get("run")
                if isinstance(run_cmd, str) and "secrets." in run_cmd:
                    raise ValueError(f"workflow job `{job_id}` references secrets.* in PR workflow")
                env_map = step.get("env")
                if isinstance(env_map, Mapping):
                    for env_value in env_map.values():
                        if isinstance(env_value, str) and "secrets." in env_value:
                            raise ValueError(f"workflow job `{job_id}` references secrets.* in PR workflow")


def _service_key(raw: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "-" for ch in raw.lower()).strip("-")
    return cleaned or "akc-app"


def _deploy_target_runtime_slice(target: Mapping[str, Any], *, default_image: str) -> dict[str, Any]:
    """Per-target ports, health, resources, and image for Compose/Kubernetes emission."""

    hc = cast(dict[str, Any], target.get("health_contract", {}))
    rc = cast(dict[str, Any], target.get("runtime_contract", {}))
    sr = cast(dict[str, Any], target.get("scaling_resources", {}))
    cfg = cast(dict[str, Any], target.get("config_secrets_contract", {}))
    op = cast(dict[str, Any], target.get("operational_config", {}))
    raw_port = rc.get("port", 8080)
    port = int(raw_port) if isinstance(raw_port, int) else 8080
    health_path = str(hc.get("readiness_path", "/healthz"))
    health_known = bool(hc.get("health_endpoint_known", True))
    raw_rep = sr.get("replicas", 1)
    replicas = int(raw_rep) if isinstance(raw_rep, int) else 1
    resource_requests = cast(dict[str, JSONValue], sr.get("requests", {"cpu": "250m", "memory": "256Mi"}))
    resource_limits = cast(dict[str, JSONValue], sr.get("limits", {"cpu": "1000m", "memory": "1Gi"}))
    required_env = cast(list[str], cfg.get("required_env", []))
    required_secrets = cast(list[str], cfg.get("required_secrets", []))
    env_defaults = {key: f"<set-{key.lower()}>" for key in required_env}
    bc = cast(dict[str, Any], target.get("build_contract", {}))
    raw_img = bc.get("image")
    image = str(raw_img).strip() if isinstance(raw_img, str) and str(raw_img).strip() else default_image
    restart_policy = str(cast(dict[str, Any], op.get("restart", {})).get("compose_policy", "unless-stopped"))
    return {
        "port": port,
        "health_path": health_path,
        "health_known": health_known,
        "replicas": replicas,
        "resource_requests": resource_requests,
        "resource_limits": resource_limits,
        "required_env": required_env,
        "required_secrets": required_secrets,
        "env_defaults": env_defaults,
        "image": image,
        "restart_policy": restart_policy,
    }


def _dump_yaml_documents(docs: Sequence[Mapping[str, Any]]) -> str:
    parts = [dump_yaml(dict(d)).rstrip() for d in docs]
    return "\n---\n".join(parts) + "\n"


def _k8s_target_secret_name(*, run_id: str, target_key: str) -> str:
    """Per-workload Secret so disjoint key sets stay isolated per tenant workload."""

    return f"akc-secrets-{run_id}-{target_key}"


def _recommended_k8s_labels(*, app_name: str, run_id: str, repo_id: str, component: str = "service") -> dict[str, str]:
    return {
        "app.kubernetes.io/name": app_name,
        "app.kubernetes.io/instance": run_id,
        "app.kubernetes.io/component": component,
        "app.kubernetes.io/part-of": repo_id,
        "app.kubernetes.io/managed-by": "akc",
        "app.kubernetes.io/version": run_id,
    }


def run_deployment_config_pass(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_spec: IntentSpec,
    orchestration_spec_text: str,
    coordination_spec_text: str,
    delivery_plan_text: str | None = None,
) -> DeploymentConfigPassResult:
    image_name = f"ghcr.io/{ir_document.repo_id}/akc:{run_id}"
    intent_ref = build_handoff_intent_ref(intent=intent_spec)
    deployment_intent_projection = project_deployment_intent_projection(intent=intent_spec)
    intent_metadata = deployment_intent_projection.to_json_obj()
    delivery_plan_obj = (
        parse_json_artifact_text(delivery_plan_text)
        if isinstance(delivery_plan_text, str) and delivery_plan_text
        else None
    )
    delivery_targets = (
        cast(list[dict[str, Any]], delivery_plan_obj.get("targets", []))
        if isinstance(delivery_plan_obj, Mapping)
        else []
    )
    non_mobile = [t for t in delivery_targets if str(t.get("target_class")) != "mobile_client"]
    deploy_targets = non_mobile if non_mobile else list(delivery_targets)
    primary_target = deploy_targets[0] if deploy_targets else (delivery_targets[0] if delivery_targets else {})
    service_name = _service_key(str(primary_target.get("name", "akc-app")))
    rollout_policy = cast(dict[str, Any], primary_target.get("rollout_recovery_policy", {}))
    rollout_strategy = str(rollout_policy.get("strategy", "rolling"))
    canary_direct_apply_supported = rollout_strategy == "canary"
    if canary_direct_apply_supported and isinstance(primary_target, Mapping):
        supported_paths = cast(dict[str, Any], primary_target.get("supported_delivery_paths", {}))
        local_paths = cast(list[str], supported_paths.get("local", []))
        staging_paths = cast(list[str], supported_paths.get("staging", []))
        canary_direct_apply_supported = "direct_apply" in local_paths or "direct_apply" in staging_paths
    intent_k8s_annotations = _deployment_k8s_intent_annotations(intent_ref=intent_ref)
    allow_network, _renv = effective_allow_network_for_handoff(
        ir_document=ir_document,
        intent_spec=intent_spec,
    )
    operational_cfg = cast(dict[str, Any], primary_target.get("operational_config", {}))
    obs_toggles = cast(dict[str, Any], operational_cfg.get("observability_toggles", {}))
    configmap_labels = _recommended_k8s_labels(
        app_name=service_name,
        run_id=run_id,
        repo_id=ir_document.repo_id,
    )

    union_env_keys: list[str] = []
    secrets_placeholders: dict[str, str] = {}
    for target in deploy_targets or [primary_target]:
        cfg = cast(dict[str, Any], target.get("config_secrets_contract", {}))
        for key in cast(list[str], cfg.get("required_env", [])):
            if key not in union_env_keys:
                union_env_keys.append(key)
        for key in cast(list[str], cfg.get("required_secrets", [])):
            secrets_placeholders[key] = "<set-in-secret-store>"

    compose_services: dict[str, JSONValue] = {}
    compose_target_by_service: dict[str, dict[str, Any]] = {}
    k8s_deployments: list[dict[str, JSONValue]] = []
    k8s_services: list[dict[str, JSONValue]] = []
    k8s_secrets: list[dict[str, JSONValue]] = []
    target_to_service_key: dict[str, str] = {}
    for target in deploy_targets:
        target_id = str(target.get("target_id", "")).strip()
        if not target_id:
            continue
        target_to_service_key[target_id] = _service_key(str(target.get("name", target_id)))
    if not target_to_service_key:
        target_to_service_key["primary"] = service_name
    for target in deploy_targets or [primary_target]:
        raw_name = str(target.get("name", "akc-app"))
        target_key = _service_key(raw_name)
        prof = _deploy_target_runtime_slice(target, default_image=image_name)
        port = int(prof["port"])
        health_path = str(prof["health_path"])
        health_known = bool(prof["health_known"])
        restart_policy = str(prof["restart_policy"])
        env_defaults = cast(dict[str, str], prof["env_defaults"])
        required_secrets = cast(list[str], prof["required_secrets"])
        resource_requests = cast(dict[str, JSONValue], prof["resource_requests"])
        resource_limits = cast(dict[str, JSONValue], prof["resource_limits"])
        tgt_image = str(prof["image"])
        replicas = max(1, int(prof["replicas"]))
        deps = cast(list[str], target.get("depends_on", []))
        depends_on: dict[str, JSONValue] = {}
        for dep in deps:
            dep_key = target_to_service_key.get(dep)
            if dep_key is not None and dep_key != target_key:
                depends_on[dep_key] = {"condition": "service_healthy"}
        compose_service: dict[str, JSONValue] = {
            "image": tgt_image,
            "read_only": True,
            "security_opt": ["no-new-privileges:true"],
            "cap_drop": ["ALL"],
            "tmpfs": ["/tmp"],
            "user": "10001:10001",
            "labels": dict(_deployment_compose_intent_labels(intent_ref=intent_ref)),
            "env_file": [".akc/deployment/compose/app.env"],
            "environment": {
                "AKC_TENANT_ID": ir_document.tenant_id,
                "AKC_REPO_ID": ir_document.repo_id,
                "AKC_ALLOW_NETWORK": str(allow_network).lower(),
                "AKC_ROLLOUT_STRATEGY": rollout_strategy,
                "AKC_CANARY_DIRECT_APPLY_SUPPORTED": str(canary_direct_apply_supported).lower(),
                "AKC_CONFIG_PATH": "/run/config/app-config.json",
            },
            "ports": [f"{port}:{port}"],
            "restart": restart_policy,
        }
        if health_known:
            compose_service["healthcheck"] = {
                "test": ["CMD", "curl", "-fsS", f"http://localhost:{port}{health_path}"],
                "interval": "15s",
                "timeout": "5s",
                "retries": 5,
            }
        if depends_on:
            compose_service["depends_on"] = depends_on
        compose_services[target_key] = compose_service
        compose_target_by_service[target_key] = target

        component = str(target.get("target_class", "backend_service")).replace("_", "-")
        app_l = _recommended_k8s_labels(
            app_name=target_key,
            run_id=run_id,
            repo_id=ir_document.repo_id,
            component=component,
        )
        dep_obj: dict[str, JSONValue] = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": f"{target_key}-{run_id}",
                "labels": dict(app_l),
                "annotations": dict(intent_k8s_annotations),
            },
            "spec": {
                "replicas": replicas,
                "selector": {
                    "matchLabels": {"app.kubernetes.io/name": target_key, "app.kubernetes.io/instance": run_id},
                },
                "template": {
                    "metadata": {
                        "labels": dict(app_l),
                        "annotations": dict(intent_k8s_annotations),
                    },
                    "spec": {
                        "securityContext": {"runAsNonRoot": True, "seccompProfile": {"type": "RuntimeDefault"}},
                        "containers": [
                            {
                                "name": target_key,
                                "image": tgt_image,
                                "ports": [{"containerPort": port}],
                                "envFrom": [{"configMapRef": {"name": f"akc-runtime-{run_id}"}}],
                                "env": [{"name": k, "value": v} for k, v in sorted(env_defaults.items())]
                                + [
                                    {
                                        "name": key,
                                        "valueFrom": {
                                            "secretKeyRef": {
                                                "name": _k8s_target_secret_name(run_id=run_id, target_key=target_key),
                                                "key": key,
                                            },
                                        },
                                    }
                                    for key in sorted(required_secrets)
                                ],
                                "securityContext": {
                                    "allowPrivilegeEscalation": False,
                                    "readOnlyRootFilesystem": True,
                                    "capabilities": {"drop": ["ALL"]},
                                },
                                "readinessProbe": (
                                    {
                                        "httpGet": {"path": health_path, "port": port},
                                        "initialDelaySeconds": 5,
                                        "periodSeconds": 10,
                                    }
                                    if health_known
                                    else None
                                ),
                                "livenessProbe": (
                                    {
                                        "httpGet": {"path": health_path, "port": port},
                                        "initialDelaySeconds": 10,
                                        "periodSeconds": 15,
                                    }
                                    if health_known
                                    else None
                                ),
                                "startupProbe": (
                                    {
                                        "httpGet": {"path": health_path, "port": port},
                                        "failureThreshold": 30,
                                        "periodSeconds": 5,
                                    }
                                    if health_known
                                    else None
                                ),
                                "resources": {
                                    "requests": dict(resource_requests),
                                    "limits": dict(resource_limits),
                                },
                            }
                        ],
                    },
                },
            },
        }
        ctrs = cast(
            list[dict[str, Any]],
            cast(dict[str, Any], cast(dict[str, Any], dep_obj["spec"])["template"])["spec"]["containers"],
        )
        for container in ctrs:
            for probe_key in ("readinessProbe", "livenessProbe", "startupProbe"):
                if container.get(probe_key) is None:
                    container.pop(probe_key, None)
        _validate_k8s_restricted_security_context(deployment_obj=dep_obj)
        k8s_deployments.append(dep_obj)
        if required_secrets:
            k8s_secrets.append(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {
                        "name": _k8s_target_secret_name(run_id=run_id, target_key=target_key),
                        "labels": dict(app_l),
                        "annotations": dict(intent_k8s_annotations),
                    },
                    "type": "Opaque",
                    "stringData": dict.fromkeys(sorted(required_secrets), "<set-in-secret-store>"),
                }
            )

        svc_obj: dict[str, JSONValue] = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": f"{target_key}-{run_id}",
                "labels": dict(app_l),
                "annotations": dict(intent_k8s_annotations),
            },
            "spec": {
                "selector": {"app.kubernetes.io/name": target_key, "app.kubernetes.io/instance": run_id},
                "ports": [{"port": 80, "targetPort": port}],
                "type": "ClusterIP",
            },
        }
        k8s_services.append(svc_obj)
    compose_obj: dict[str, JSONValue] = {
        "version": "3.9",
        "services": compose_services or {service_name: {}},
        "x-akc-rollout": {
            "strategy": rollout_strategy,
            "direct_apply_canary_supported": canary_direct_apply_supported,
            "gitops_rollout_crd_deferred": True,
        },
        "x-akc-config": {
            "env_file": ".akc/deployment/compose/app.env",
            "config_template": ".akc/deployment/compose/app-config.json",
            "secrets_placeholders": secrets_placeholders,
        },
    }
    _validate_docker_compose_hardening(compose_obj=compose_obj)
    staging_overlay_services: dict[str, JSONValue] = {}
    production_overlay_services: dict[str, JSONValue] = {}
    for svc_name in compose_services:
        tgt = compose_target_by_service.get(svc_name, primary_target)
        sr = cast(dict[str, Any], tgt.get("scaling_resources", {}))
        rep = int(sr.get("replicas", 1)) if isinstance(sr.get("replicas"), int) else 1
        staging_overlay_services[svc_name] = {
            "environment": {"AKC_ENVIRONMENT": "staging"},
            "deploy": {"replicas": max(1, rep)},
        }
        production_overlay_services[svc_name] = {
            "environment": {"AKC_ENVIRONMENT": "production"},
            "deploy": {"replicas": max(2, rep)},
        }
    compose_staging_overlay_obj: dict[str, JSONValue] = {"services": staging_overlay_services}
    compose_production_overlay_obj: dict[str, JSONValue] = {"services": production_overlay_services}

    config_map_obj: dict[str, JSONValue] = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"akc-runtime-{run_id}",
            "labels": dict(configmap_labels),
            "annotations": dict(intent_k8s_annotations),
        },
        "data": {
            "TENANT_ID": ir_document.tenant_id,
            "REPO_ID": ir_document.repo_id,
            "RUN_ID": run_id,
            "LOGGING_ENABLED": str(bool(obs_toggles.get("logging_enabled", True))).lower(),
            "TRACING_ENABLED": str(bool(obs_toggles.get("tracing_enabled", True))).lower(),
            "METRICS_ENABLED": str(bool(obs_toggles.get("metrics_enabled", True))).lower(),
            "ORCHESTRATION_SPEC_JSON": orchestration_spec_text,
            "COORDINATION_SPEC_JSON": coordination_spec_text,
        },
    }
    k8s_deployments_documents = _dump_yaml_documents(cast(Sequence[Mapping[str, Any]], k8s_deployments))
    k8s_services_documents = _dump_yaml_documents(cast(Sequence[Mapping[str, Any]], k8s_services))
    k8s_staging_patch_docs: list[dict[str, Any]] = []
    k8s_production_patch_docs: list[dict[str, Any]] = []
    for target in deploy_targets or [primary_target]:
        raw_name = str(target.get("name", "akc-app"))
        patch_key = _service_key(raw_name)
        sr = cast(dict[str, Any], target.get("scaling_resources", {}))
        rep = int(sr.get("replicas", 1)) if isinstance(sr.get("replicas"), int) else 1
        k8s_staging_patch_docs.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": f"{patch_key}-{run_id}"},
                "spec": {"replicas": max(1, rep)},
            }
        )
        k8s_production_patch_docs.append(
            {
                "apiVersion": "apps/v1",
                "kind": "Deployment",
                "metadata": {"name": f"{patch_key}-{run_id}"},
                "spec": {"replicas": max(2, rep)},
            }
        )
    k8s_staging_patch_yaml = _dump_yaml_documents(cast(Sequence[Mapping[str, Any]], k8s_staging_patch_docs))
    k8s_production_patch_yaml = _dump_yaml_documents(cast(Sequence[Mapping[str, Any]], k8s_production_patch_docs))

    k8s_resource_files: list[str] = ["deployment.yml", "service.yml", "configmap.yml"]
    if k8s_secrets:
        k8s_resource_files.append("secrets.yml")
    k8s_base_kustomization_obj: dict[str, JSONValue] = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "resources": k8s_resource_files,
    }
    k8s_staging_overlay_obj: dict[str, JSONValue] = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "resources": ["../../base"],
        "patchesStrategicMerge": ["deployment-patch.yml"],
        "commonLabels": {"app.kubernetes.io/environment": "staging"},
    }
    k8s_production_overlay_obj: dict[str, JSONValue] = {
        "apiVersion": "kustomize.config.k8s.io/v1beta1",
        "kind": "Kustomization",
        "resources": ["../../base"],
        "patchesStrategicMerge": ["deployment-patch.yml"],
        "commonLabels": {"app.kubernetes.io/environment": "production"},
    }
    workflow_obj: dict[str, JSONValue] = {
        "name": f"AKC Delivery Pipeline {run_id}",
        "on": {"workflow_dispatch": {}, "push": {"branches": ["main"]}},
        "permissions": {"contents": "read"},
        "jobs": {
            "build": {
                "name": "Build",
                "runs-on": "ubuntu-latest",
                "permissions": {"contents": "read"},
                "steps": [
                    {"name": "Checkout", "uses": "actions/checkout@v4"},
                    {"name": "Build image", "run": "docker build -t ${IMAGE} .", "env": {"IMAGE": image_name}},
                ],
            },
            "verify": {
                "name": "Verify",
                "runs-on": "ubuntu-latest",
                "needs": ["build"],
                "permissions": {"contents": "read"},
                "steps": [{"name": "Run tests", "run": "pytest -q"}],
            },
            "publish": {
                "name": "Publish",
                "runs-on": "ubuntu-latest",
                "needs": ["verify"],
                "permissions": {"contents": "read", "packages": "write", "id-token": "write"},
                "steps": [
                    {
                        "name": "Configure OIDC auth placeholder",
                        "run": "echo 'Configure cloud/registry federation with OIDC; no long-lived secrets required'",
                    },
                    {
                        "name": "Publish image",
                        "run": "echo 'Publish ${IMAGE} using short-lived OIDC credentials'",
                        "env": {"IMAGE": image_name},
                    },
                ],
            },
            "deploy_staging": {
                "name": "Deploy Staging",
                "runs-on": "ubuntu-latest",
                "environment": {"name": "staging"},
                "needs": ["publish"],
                "permissions": {"contents": "read", "id-token": "write"},
                "steps": [{"name": "Deploy overlay", "run": "kubectl apply -k .akc/deployment/k8s/overlays/staging"}],
            },
            "health_check": {
                "name": "Health Check",
                "runs-on": "ubuntu-latest",
                "needs": ["deploy_staging"],
                "permissions": {"contents": "read"},
                "steps": [
                    {
                        "name": "Probe health",
                        "run": "echo 'Run staging health checks against /healthz before promotion'",
                    }
                ],
            },
            "approval": {
                "name": "Approval Gate",
                "runs-on": "ubuntu-latest",
                "needs": ["health_check"],
                "environment": {"name": "production"},
                "permissions": {"contents": "read"},
                "steps": [
                    {
                        "name": "Require production approval",
                        "run": "echo 'Production deployment requires environment reviewer approval'",
                    }
                ],
            },
            "deploy_prod": {
                "name": "Deploy Production",
                "runs-on": "ubuntu-latest",
                "needs": ["approval"],
                "environment": {"name": "production"},
                "permissions": {"contents": "read", "id-token": "write"},
                "steps": [
                    {"name": "Deploy overlay", "run": "kubectl apply -k .akc/deployment/k8s/overlays/production"}
                ],
            },
            "rollback": {
                "name": "Rollback Hook",
                "runs-on": "ubuntu-latest",
                "needs": ["deploy_prod"],
                "if": "${{ always() && needs.deploy_prod.result == 'failure' }}",
                "permissions": {"contents": "read"},
                "steps": [
                    {"name": "Rollback", "run": "echo 'Implement rollback command for target platform/provider'"}
                ],
            },
        },
    }
    _validate_workflow_policy(workflow_obj=workflow_obj)

    compose_artifact = OutputArtifact.from_text(
        path=".akc/deployment/docker-compose.yml",
        text=dump_yaml(compose_obj),
        media_type="application/yaml; charset=utf-8",
        metadata=_deployment_output_artifact_metadata(
            run_id=run_id,
            kind="deployment_docker_compose",
            intent_ref=intent_ref,
            intent_projection=intent_metadata,
        ),
    )
    deployment_artifact = OutputArtifact.from_text(
        path=".akc/deployment/k8s/deployment.yml",
        text=k8s_deployments_documents,
        media_type="application/yaml; charset=utf-8",
        metadata=_deployment_output_artifact_metadata(
            run_id=run_id,
            kind="deployment_k8s_deployment",
            intent_ref=intent_ref,
            intent_projection=intent_metadata,
        ),
    )
    service_artifact = OutputArtifact.from_text(
        path=".akc/deployment/k8s/service.yml",
        text=k8s_services_documents,
        media_type="application/yaml; charset=utf-8",
        metadata=_deployment_output_artifact_metadata(
            run_id=run_id,
            kind="deployment_k8s_service",
            intent_ref=intent_ref,
            intent_projection=intent_metadata,
        ),
    )
    configmap_artifact = OutputArtifact.from_text(
        path=".akc/deployment/k8s/configmap.yml",
        text=dump_yaml(config_map_obj),
        media_type="application/yaml; charset=utf-8",
        metadata=_deployment_output_artifact_metadata(
            run_id=run_id,
            kind="deployment_k8s_configmap",
            intent_ref=intent_ref,
            intent_projection=intent_metadata,
        ),
    )
    k8s_secrets_documents: str | None = (
        _dump_yaml_documents(cast(Sequence[Mapping[str, Any]], k8s_secrets)) if k8s_secrets else None
    )
    k8s_secrets_artifacts: tuple[OutputArtifact, ...] = ()
    if k8s_secrets_documents is not None:
        sec_meta = _deployment_output_artifact_metadata(
            run_id=run_id,
            kind="deployment_k8s_secrets",
            intent_ref=intent_ref,
            intent_projection=intent_metadata,
        )
        k8s_secrets_artifacts = (
            OutputArtifact.from_text(
                path=".akc/deployment/k8s/secrets.yml",
                text=k8s_secrets_documents,
                media_type="application/yaml; charset=utf-8",
                metadata=sec_meta,
            ),
        )
    workflow_artifact = OutputArtifact.from_text(
        path=f".github/workflows/akc_deploy_{run_id}.yml",
        text=dump_yaml(workflow_obj),
        media_type="application/yaml; charset=utf-8",
        metadata=_deployment_output_artifact_metadata(
            run_id=run_id,
            kind="deployment_github_actions",
            intent_ref=intent_ref,
            intent_projection=intent_metadata,
        ),
    )
    additional_artifacts = (
        OutputArtifact.from_text(
            path=".akc/deployment/compose/app.env",
            text=(
                "\n".join([f"{key}=<set-{key.lower()}>" for key in sorted(union_env_keys)]) + "\n"
                if union_env_keys
                else "# No required environment variables inferred.\n"
            ),
            media_type="text/plain; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_compose_env"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/compose/app-config.json",
            text=json.dumps(
                {
                    "run_id": run_id,
                    "tenant_id": ir_document.tenant_id,
                    "repo_id": ir_document.repo_id,
                    "logging_tracing_toggles": cast(dict[str, Any], operational_cfg.get("observability_toggles", {})),
                    "alert_health_expectations": cast(
                        dict[str, Any], operational_cfg.get("alert_health_expectations", {})
                    ),
                    "secret_placeholders": secrets_placeholders,
                },
                indent=2,
                sort_keys=True,
            )
            + "\n",
            media_type="application/json; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_compose_config_template"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/compose/docker-compose.staging.yml",
            text=dump_yaml(compose_staging_overlay_obj),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_compose_staging_overlay"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/compose/docker-compose.production.yml",
            text=dump_yaml(compose_production_overlay_obj),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_compose_production_overlay"},
        ),
        *k8s_secrets_artifacts,
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/base/deployment.yml",
            text=k8s_deployments_documents,
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_base_deployment"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/base/service.yml",
            text=k8s_services_documents,
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_base_service"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/base/configmap.yml",
            text=dump_yaml(config_map_obj),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_base_configmap"},
        ),
        *(
            (
                OutputArtifact.from_text(
                    path=".akc/deployment/k8s/base/secrets.yml",
                    text=k8s_secrets_documents,
                    media_type="application/yaml; charset=utf-8",
                    metadata={"run_id": run_id, "kind": "deployment_k8s_base_secrets"},
                ),
            )
            if k8s_secrets_documents is not None
            else ()
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/base/kustomization.yml",
            text=dump_yaml(k8s_base_kustomization_obj),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_base_kustomization"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/overlays/staging/kustomization.yml",
            text=dump_yaml(k8s_staging_overlay_obj),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_staging_overlay"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/overlays/staging/deployment-patch.yml",
            text=k8s_staging_patch_yaml,
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_staging_patch"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/overlays/production/kustomization.yml",
            text=dump_yaml(k8s_production_overlay_obj),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_production_overlay"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/k8s/overlays/production/deployment-patch.yml",
            text=k8s_production_patch_yaml,
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_k8s_production_patch"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/gitops/flux-kustomization.yml",
            text=dump_yaml(
                {
                    "apiVersion": "kustomize.toolkit.fluxcd.io/v1",
                    "kind": "Kustomization",
                    "metadata": {"name": f"akc-{run_id}-production"},
                    "spec": {
                        "interval": "5m",
                        "path": ".akc/deployment/k8s/overlays/production",
                        "prune": True,
                        "sourceRef": {"kind": "GitRepository", "name": "akc-repo"},
                    },
                }
            ),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_gitops_flux_template"},
        ),
        OutputArtifact.from_text(
            path=".akc/deployment/gitops/argo-application.yml",
            text=dump_yaml(
                {
                    "apiVersion": "argoproj.io/v1alpha1",
                    "kind": "Application",
                    "metadata": {"name": f"akc-{run_id}-production"},
                    "spec": {
                        "project": "default",
                        "source": {
                            "repoURL": "https://github.com/example/repo.git",
                            "targetRevision": "main",
                            "path": ".akc/deployment/k8s/overlays/production",
                        },
                        "destination": {"server": "https://kubernetes.default.svc", "namespace": "default"},
                        "syncPolicy": {"automated": {"prune": True, "selfHeal": True}},
                    },
                }
            ),
            media_type="application/yaml; charset=utf-8",
            metadata={"run_id": run_id, "kind": "deployment_gitops_argo_template"},
        ),
    )

    return DeploymentConfigPassResult(
        artifact_docker_compose=compose_artifact,
        artifact_k8s_deployment=deployment_artifact,
        artifact_k8s_service=service_artifact,
        artifact_k8s_configmap=configmap_artifact,
        artifact_github_actions=workflow_artifact,
        additional_artifacts=additional_artifacts,
        output_sha256=compose_artifact.sha256_hex(),
        metadata={
            "run_id": run_id,
            "tenant_id": ir_document.tenant_id,
            "repo_id": ir_document.repo_id,
            "intent_id": deployment_intent_projection.intent_id,
            "stable_intent_sha256": deployment_intent_projection.stable_intent_sha256,
            "intent_ref": intent_ref,
            "intent_projection": intent_metadata,
            "delivery_plan_ref": (
                {
                    "path": f".akc/deployment/{run_id}.delivery_plan.json",
                    "fingerprint": stable_json_fingerprint(delivery_plan_obj),
                }
                if isinstance(delivery_plan_obj, Mapping)
                else None
            ),
            "workflow_path": workflow_artifact.path,
            "k8s_deployment_path": deployment_artifact.path,
            "rollout_strategy": rollout_strategy,
            "canary_direct_apply_supported": canary_direct_apply_supported,
            "k8s_secret_manifest_count": len(k8s_secrets),
            "additional_artifact_count": len(additional_artifacts),
        },
    )
