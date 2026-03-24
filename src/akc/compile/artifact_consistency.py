"""Deterministic cross-artifact consistency checks (orchestration, coordination, bundle).

Pure functions over JSON mappings and text; safe to call from tests without LLM or I/O.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence, Set
from typing import Any

from akc.intent.models import IntentSpecV1
from akc.intent.policy_projection import project_runtime_intent_projection
from akc.ir import IRDocument
from akc.knowledge.runtime_projection import knowledge_runtime_envelope_from_ir
from akc.utils.fingerprint import stable_json_fingerprint


def effective_allow_network_for_handoff(
    *, ir_document: IRDocument, intent_spec: IntentSpecV1
) -> tuple[bool, dict[str, Any]]:
    """Match ``run_runtime_bundle_pass`` / deployment: intent projection + knowledge tightening."""

    runtime_intent_projection = project_runtime_intent_projection(intent=intent_spec)
    allow_network = bool(runtime_intent_projection.operating_bounds_effective.get("allow_network", False))
    k_env = knowledge_runtime_envelope_from_ir(ir_document)
    k_hard = k_env.get("knowledge_hard_constraints") or []
    k_network_forbidden = bool(k_env.get("knowledge_network_egress_forbidden"))
    renv_knowledge: dict[str, Any] = {}
    if k_hard:
        renv_knowledge["knowledge_hard_constraints"] = list(k_hard)
        renv_knowledge["knowledge_network_egress_forbidden"] = k_network_forbidden
        if k_network_forbidden:
            allow_network = False
            renv_knowledge["knowledge_network_tightening"] = True
    k_rules = k_env.get("knowledge_policy_rules")
    if isinstance(k_rules, list) and k_rules:
        renv_knowledge["knowledge_policy_rules"] = list(k_rules)
    k_expl = k_env.get("knowledge_explanations")
    if isinstance(k_expl, dict) and k_expl:
        renv_knowledge["knowledge_explanations"] = dict(k_expl)
    k_deny = k_env.get("knowledge_derived_deny_actions")
    if isinstance(k_deny, list) and k_deny:
        renv_knowledge["knowledge_derived_deny_actions"] = list(k_deny)
    return allow_network, renv_knowledge


_COMPOSE_AKC_ALLOW_NETWORK_RE = re.compile(
    r"^\s*AKC_ALLOW_NETWORK:\s*(?:(true|false)|\"(true|false)\")\s*$",
    re.MULTILINE,
)


def _step_ir_node_id(step: Mapping[str, Any]) -> str | None:
    """Extract ``ir_node_id`` from an orchestration step (matches artifact_passes)."""
    inputs = step.get("inputs")
    if isinstance(inputs, Mapping):
        v = inputs.get("ir_node_id")
        if isinstance(v, str) and v.strip():
            return v.strip()
    v = step.get("ir_node_id")
    if isinstance(v, str) and v.strip():
        return v.strip()
    return None


def _orchestration_step_ids(orchestration_obj: Mapping[str, Any]) -> set[str]:
    steps = orchestration_obj.get("steps")
    if not isinstance(steps, Sequence) or isinstance(steps, (str, bytes)):
        return set()
    out: set[str] = set()
    for step in steps:
        if not isinstance(step, Mapping):
            continue
        sid = step.get("step_id")
        if isinstance(sid, str) and sid.strip():
            out.add(sid.strip())
    return out


def validate_orchestration_ir_references(
    *,
    orchestration_obj: Mapping[str, Any],
    ir_node_ids: Set[str],
) -> tuple[str, ...]:
    """Each orchestration step ``ir_node_id`` (when set) must exist in ``ir_node_ids``."""

    issues: list[str] = []
    steps = orchestration_obj.get("steps")
    if not isinstance(steps, Sequence) or isinstance(steps, (str, bytes)):
        return ()
    for idx, step in enumerate(steps):
        if not isinstance(step, Mapping):
            continue
        irn = _step_ir_node_id(step)
        if irn is not None and irn not in ir_node_ids:
            issues.append(
                f"orchestration.steps[{idx}] ir_node_id {irn!r} is not present in the IR document node id set"
            )
    return tuple(issues)


def validate_coordination_orchestration_consistency(
    *,
    orchestration_obj: Mapping[str, Any],
    coordination_obj: Mapping[str, Any],
) -> tuple[str, ...]:
    """Coordination bindings and graph edges must reference orchestration ``step_id`` values."""

    step_ids = _orchestration_step_ids(orchestration_obj)
    issues: list[str] = []

    bindings = coordination_obj.get("orchestration_bindings")
    if isinstance(bindings, Sequence) and not isinstance(bindings, (str, bytes)):
        for bi, binding in enumerate(bindings):
            if not isinstance(binding, Mapping):
                continue
            oids = binding.get("orchestration_step_ids")
            if not isinstance(oids, Sequence) or isinstance(oids, (str, bytes)):
                continue
            for j, raw_sid in enumerate(oids):
                sid = str(raw_sid).strip()
                if sid and sid not in step_ids:
                    issues.append(
                        "coordination.orchestration_bindings["
                        f"{bi}].orchestration_step_ids[{j}] references unknown step_id {sid!r}"
                    )

    cg = coordination_obj.get("coordination_graph")
    if isinstance(cg, Mapping):
        edges = cg.get("edges")
        if isinstance(edges, Sequence) and not isinstance(edges, (str, bytes)):
            for ei, edge in enumerate(edges):
                if not isinstance(edge, Mapping):
                    continue
                for key in ("src_step_id", "dst_step_id"):
                    v = edge.get(key)
                    if isinstance(v, str) and v.strip():
                        sv = v.strip()
                        if sv not in step_ids:
                            issues.append(
                                f"coordination.coordination_graph.edges[{ei}].{key} references unknown step_id {sv!r}"
                            )
    return tuple(issues)


def validate_runtime_bundle_intent_blocks_consistency(bundle_obj: Mapping[str, Any]) -> tuple[str, ...]:
    """``intent_ref`` must agree with ``intent_policy_projection`` on identity and fingerprints."""

    issues: list[str] = []
    intent_ref = bundle_obj.get("intent_ref")
    proj = bundle_obj.get("intent_policy_projection")
    if not isinstance(intent_ref, Mapping) or not isinstance(proj, Mapping):
        return ("runtime_bundle.intent_ref and intent_policy_projection must be objects",)

    for label, rk, pk in (
        ("intent_id", "intent_id", "intent_id"),
        ("stable_intent_sha256", "stable_intent_sha256", "stable_intent_sha256"),
        ("semantic fingerprint", "semantic_fingerprint", "intent_semantic_fingerprint"),
        ("goal text fingerprint", "goal_text_fingerprint", "intent_goal_text_fingerprint"),
    ):
        rv, pv = intent_ref.get(rk), proj.get(pk)
        if rv != pv:
            issues.append(
                f"runtime_bundle intent_ref.{rk} ({rv!r}) does not match "
                f"intent_policy_projection.{pk!r} ({pv!r}) for {label}"
            )
    if issues:
        return tuple(issues)
    return ()


def validate_runtime_bundle_system_ir_ref(
    *,
    bundle_obj: Mapping[str, Any],
    ir_document: IRDocument,
) -> tuple[str, ...]:
    """``system_ir_ref`` must match the compile-time ``IRDocument`` fingerprint and path."""

    run_id = bundle_obj.get("run_id")
    if not isinstance(run_id, str) or not run_id.strip():
        return ("runtime_bundle.run_id must be a non-empty string",)

    ref = bundle_obj.get("system_ir_ref")
    if not isinstance(ref, Mapping):
        return ("runtime_bundle.system_ir_ref must be an object",)

    want_path = f".akc/ir/{run_id.strip()}.json"
    got_path = ref.get("path")
    if got_path != want_path:
        issues: list[str] = [
            f"runtime_bundle.system_ir_ref.path expected {want_path!r}, got {got_path!r}",
        ]
    else:
        issues = []

    want_fp = ir_document.fingerprint()
    got_fp = ref.get("fingerprint")
    if want_fp != got_fp:
        issues.append("runtime_bundle.system_ir_ref.fingerprint does not match compile-time IRDocument.fingerprint()")

    want_fmt = ir_document.format_version
    got_fmt = ref.get("format_version")
    if want_fmt != got_fmt:
        issues.append(f"runtime_bundle.system_ir_ref.format_version expected {want_fmt!r}, got {got_fmt!r}")

    want_sv = int(ir_document.schema_version)
    got_sv = ref.get("schema_version")
    if isinstance(got_sv, bool) or not isinstance(got_sv, (int, float)):
        issues.append("runtime_bundle.system_ir_ref.schema_version must be an integer when set")
    elif int(got_sv) != want_sv:
        issues.append(f"runtime_bundle.system_ir_ref.schema_version expected {want_sv}, got {int(got_sv)}")

    return tuple(issues)


def validate_deployment_intents_align_with_ir(bundle_obj: Mapping[str, Any]) -> tuple[str, ...]:
    """``deployment_intents`` must match deployable nodes in ``referenced_ir_nodes`` (same shape as IR projection).

    Deployable kinds are ``service``, ``integration``, ``infrastructure``, ``agent`` — matching
    ``run_runtime_bundle_pass`` and the integration checks in ``test_runtime_ir_bundle_projection``.
    """

    deploy_kinds = frozenset({"service", "integration", "infrastructure", "agent"})
    ref_raw = bundle_obj.get("referenced_ir_nodes")
    if ref_raw is None:
        ref_nodes: list[Any] = []
    elif not isinstance(ref_raw, list):
        return ("runtime_bundle.referenced_ir_nodes must be a list for deployment intent alignment",)
    else:
        ref_nodes = ref_raw

    dep_raw = bundle_obj.get("deployment_intents")
    if dep_raw is None:
        dep_intents: list[Any] = []
    elif not isinstance(dep_raw, list):
        return ("runtime_bundle.deployment_intents must be a list for deployment intent alignment",)
    else:
        dep_intents = dep_raw

    id_to_kind: dict[str, str] = {}
    for n in ref_nodes:
        if not isinstance(n, dict):
            continue
        raw_id = n.get("id")
        if not isinstance(raw_id, str) or not raw_id.strip():
            continue
        nid = str(raw_id).strip()
        kind = str(n.get("kind", "")).strip()
        id_to_kind[nid] = kind

    referenced_ids = set(id_to_kind)
    referenced_deployable = {nid for nid, k in id_to_kind.items() if k in deploy_kinds}

    issues: list[str] = []
    seen_intent_ids: set[str] = set()
    for idx, row in enumerate(dep_intents):
        if not isinstance(row, dict):
            issues.append(f"runtime_bundle.deployment_intents[{idx}] must be an object")
            continue
        nid_raw = row.get("node_id")
        nid = str(nid_raw).strip() if isinstance(nid_raw, str) else ""
        if not nid:
            issues.append(f"runtime_bundle.deployment_intents[{idx}].node_id must be a non-empty string")
            continue
        row_kind = str(row.get("kind", "")).strip()
        if row_kind not in deploy_kinds:
            issues.append(
                f"runtime_bundle.deployment_intents[{idx}] kind {row_kind!r} must be one of: "
                f"{', '.join(sorted(deploy_kinds))}"
            )
        if nid not in referenced_ids:
            issues.append(
                f"runtime_bundle.deployment_intents[{idx}] node_id {nid!r} is not present in referenced_ir_nodes"
            )
            continue
        ir_kind = id_to_kind.get(nid, "")
        if ir_kind not in deploy_kinds:
            issues.append(
                f"runtime_bundle.deployment_intents[{idx}] node_id {nid!r} is not a deployable IR node "
                f"(referenced kind {ir_kind!r})"
            )
        elif row_kind and row_kind != ir_kind:
            issues.append(
                f"runtime_bundle.deployment_intents[{idx}] kind {row_kind!r} does not match "
                f"referenced_ir_nodes[{nid!r}].kind {ir_kind!r}"
            )
        if nid in seen_intent_ids:
            issues.append(f"runtime_bundle.deployment_intents[{idx}] duplicates node_id {nid!r}")
        seen_intent_ids.add(nid)

    missing = sorted(referenced_deployable - seen_intent_ids)
    if missing:
        issues.append(
            "runtime_bundle.deployment_intents is missing deployment rows for deployable referenced_ir_nodes: "
            + ", ".join(missing)
        )

    return tuple(issues)


def validate_runtime_policy_envelope_scope(bundle_obj: Mapping[str, Any]) -> tuple[str, ...]:
    """``runtime_policy_envelope`` tenant/repo/run must match bundle top-level fields."""

    env = bundle_obj.get("runtime_policy_envelope")
    if not isinstance(env, Mapping):
        return ("runtime_bundle.runtime_policy_envelope must be an object",)

    issues: list[str] = []
    for key in ("tenant_id", "repo_id", "run_id"):
        top = bundle_obj.get(key)
        inner = env.get(key)
        if top != inner:
            issues.append(f"runtime_policy_envelope.{key} ({inner!r}) must match bundle.{key} ({top!r})")
    return tuple(issues)


def validate_runtime_policy_envelope_network_derived(
    *,
    bundle_obj: Mapping[str, Any],
    ir_document: IRDocument,
    intent_spec: IntentSpecV1,
) -> tuple[str, ...]:
    """``runtime_policy_envelope.allow_network`` must match ``run_runtime_bundle_pass`` derivation."""

    env = bundle_obj.get("runtime_policy_envelope")
    if not isinstance(env, Mapping):
        return ("runtime_bundle.runtime_policy_envelope must be an object",)

    allow_network, _renv = effective_allow_network_for_handoff(
        ir_document=ir_document,
        intent_spec=intent_spec,
    )
    k_env = knowledge_runtime_envelope_from_ir(ir_document)
    k_hard = k_env.get("knowledge_hard_constraints") or []

    got = env.get("allow_network")
    if not isinstance(got, bool):
        return ("runtime_bundle.runtime_policy_envelope.allow_network must be a bool",)
    if got != allow_network:
        return (
            "runtime_bundle.runtime_policy_envelope.allow_network does not match "
            "intent projection + knowledge tightening (expected "
            f"{allow_network!r}, got {got!r})",
        )
    if k_hard and not isinstance(env.get("knowledge_hard_constraints"), list):
        return (
            "runtime_bundle.runtime_policy_envelope.knowledge_hard_constraints must be a list "
            "when knowledge constraints are present on the IR",
        )
    return ()


def _parse_compose_allow_network(compose_yaml_text: str) -> bool | None:
    """Return whether ``AKC_ALLOW_NETWORK`` is true/false, or None if unset."""

    m = _COMPOSE_AKC_ALLOW_NETWORK_RE.search(compose_yaml_text)
    if not m:
        return None
    raw = m.group(1) or m.group(2)
    return raw == "true"


def validate_bundle_vs_deployment_compose_network(
    *,
    bundle_obj: Mapping[str, Any],
    compose_yaml_text: str,
) -> tuple[str, ...]:
    """Deployment compose must not advertise network access when the runtime bundle forbids it."""

    env = bundle_obj.get("runtime_policy_envelope")
    if not isinstance(env, Mapping):
        return ("runtime_bundle.runtime_policy_envelope must be an object",)

    bundle_allow = env.get("allow_network")
    if not isinstance(bundle_allow, bool):
        return ("runtime_bundle.runtime_policy_envelope.allow_network must be a bool",)

    compose_allow = _parse_compose_allow_network(compose_yaml_text)
    if compose_allow is None:
        return ("deployment docker-compose.yml must set AKC_ALLOW_NETWORK",)

    # Dangerous mismatch: runtime handoff forbids network but deployment env enables it.
    if not bundle_allow and compose_allow:
        return (
            "deployment docker-compose AKC_ALLOW_NETWORK is true but "
            "runtime_bundle.runtime_policy_envelope.allow_network is false",
        )
    return ()


def validate_runtime_bundle_coordination_ref(bundle_obj: Mapping[str, Any]) -> tuple[str, ...]:
    """When ``coordination_ref`` is present, its fingerprint must match ``spec_hashes``."""

    ref = bundle_obj.get("coordination_ref")
    if not isinstance(ref, Mapping) or not ref:
        return ()
    path = ref.get("path")
    fp = ref.get("fingerprint")
    if not isinstance(path, str) or not str(path).strip():
        return ("runtime_bundle.coordination_ref.path must be a non-empty string when coordination_ref is set",)
    if not isinstance(fp, str) or len(fp.strip()) != 64:
        return (
            "runtime_bundle.coordination_ref.fingerprint must be a 64-character lowercase hex sha256 "
            "when coordination_ref is set",
        )
    run_id = bundle_obj.get("run_id")
    if isinstance(run_id, str) and run_id.strip():
        want = f".akc/agents/{run_id.strip()}.coordination.json"
        if str(path).strip() != want:
            return (
                f"runtime_bundle.coordination_ref.path expected {want!r} for this run_id, got {str(path).strip()!r}",
            )
    specs = bundle_obj.get("spec_hashes")
    if not isinstance(specs, Mapping):
        return ("runtime_bundle.spec_hashes must be an object when coordination_ref is present",)
    want_coord = specs.get("coordination_spec_sha256")
    if not isinstance(want_coord, str) or len(want_coord.strip()) != 64:
        return (
            "runtime_bundle.spec_hashes.coordination_spec_sha256 must be a 64-char hex sha256 "
            "when coordination_ref is present",
        )
    if fp.strip().lower() != want_coord.strip().lower():
        return ("runtime_bundle.coordination_ref.fingerprint does not match spec_hashes.coordination_spec_sha256",)
    return ()


def validate_runtime_bundle_coordination_inline(bundle_obj: Mapping[str, Any]) -> tuple[str, ...]:
    """When ``coordination_spec`` is embedded, its fingerprint must match ``spec_hashes``."""

    inline = bundle_obj.get("coordination_spec")
    if not isinstance(inline, Mapping):
        return ()
    specs = bundle_obj.get("spec_hashes")
    if not isinstance(specs, Mapping):
        return ("runtime_bundle.spec_hashes must be an object when coordination_spec is embedded",)
    want = specs.get("coordination_spec_sha256")
    if not isinstance(want, str) or len(want.strip()) != 64:
        return (
            "runtime_bundle.spec_hashes.coordination_spec_sha256 must be a 64-char hex sha256 "
            "when coordination_spec is embedded",
        )
    got = stable_json_fingerprint(dict(inline))
    if got != want.strip().lower():
        return ("runtime_bundle.coordination_spec does not match spec_hashes.coordination_spec_sha256",)
    return ()


def validate_runtime_bundle_spec_hashes(
    *,
    bundle_obj: Mapping[str, Any],
    orchestration_json_text: str,
    coordination_json_text: str,
) -> tuple[str, ...]:
    """``spec_hashes`` in a runtime bundle must match fingerprints of orchestration/coordination JSON."""

    try:
        orch = json.loads(orchestration_json_text)
        coord = json.loads(coordination_json_text)
    except json.JSONDecodeError as exc:
        return (f"orchestration/coordination JSON decode error: {exc}",)
    if not isinstance(orch, dict) or not isinstance(coord, dict):
        return ("orchestration and coordination specs must decode to JSON objects",)

    specs = bundle_obj.get("spec_hashes")
    if not isinstance(specs, Mapping):
        return ("runtime_bundle.spec_hashes must be an object when present",)

    want_orch = stable_json_fingerprint(orch)
    want_coord = stable_json_fingerprint(coord)
    got_orch = specs.get("orchestration_spec_sha256")
    got_coord = specs.get("coordination_spec_sha256")
    issues: list[str] = []
    if want_orch != got_orch:
        issues.append(
            "runtime_bundle.spec_hashes.orchestration_spec_sha256 does not match "
            "stable_json_fingerprint(orchestration JSON)"
        )
    if want_coord != got_coord:
        issues.append(
            "runtime_bundle.spec_hashes.coordination_spec_sha256 does not match "
            "stable_json_fingerprint(coordination JSON)"
        )
    return tuple(issues)


def collect_cross_artifact_consistency_issues(
    *,
    ir_document: IRDocument,
    orchestration_json_text: str,
    coordination_json_text: str,
    runtime_bundle_obj: Mapping[str, Any],
    intent_spec: IntentSpecV1 | None = None,
    deployment_docker_compose_yaml: str | None = None,
) -> tuple[str, ...]:
    """Run orchestration↔IR, coordination↔orchestration, bundle spec hashes, and handoff closure."""

    try:
        orch = json.loads(orchestration_json_text)
        coord = json.loads(coordination_json_text)
    except json.JSONDecodeError as exc:
        return (f"orchestration/coordination JSON decode error: {exc}",)
    if not isinstance(orch, dict) or not isinstance(coord, dict):
        return ("orchestration and coordination specs must decode to JSON objects",)

    ir_node_ids = {n.id for n in ir_document.nodes}
    issues: list[str] = []
    issues.extend(validate_orchestration_ir_references(orchestration_obj=orch, ir_node_ids=ir_node_ids))
    issues.extend(validate_coordination_orchestration_consistency(orchestration_obj=orch, coordination_obj=coord))
    issues.extend(
        validate_runtime_bundle_spec_hashes(
            bundle_obj=runtime_bundle_obj,
            orchestration_json_text=orchestration_json_text,
            coordination_json_text=coordination_json_text,
        )
    )
    issues.extend(validate_runtime_bundle_intent_blocks_consistency(runtime_bundle_obj))
    issues.extend(validate_runtime_bundle_coordination_ref(runtime_bundle_obj))
    issues.extend(validate_runtime_bundle_coordination_inline(runtime_bundle_obj))
    issues.extend(validate_runtime_bundle_system_ir_ref(bundle_obj=runtime_bundle_obj, ir_document=ir_document))
    issues.extend(validate_runtime_policy_envelope_scope(runtime_bundle_obj))
    if intent_spec is not None:
        issues.extend(
            validate_runtime_policy_envelope_network_derived(
                bundle_obj=runtime_bundle_obj,
                ir_document=ir_document,
                intent_spec=intent_spec,
            )
        )
    if deployment_docker_compose_yaml is not None and deployment_docker_compose_yaml.strip():
        issues.extend(
            validate_bundle_vs_deployment_compose_network(
                bundle_obj=runtime_bundle_obj,
                compose_yaml_text=deployment_docker_compose_yaml,
            )
        )
    return tuple(issues)
