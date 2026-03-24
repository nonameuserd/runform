"""Compile-time checks for operational structure on :class:`~akc.ir.schema.IRDocument`.

Workflow nodes produced for runtime orchestration should carry a runtime
:class:`~akc.ir.schema.OperationalContract`. Intent nodes should carry an
acceptance contract when present in the graph.

Graph integrity checks (referential ``depends_on``, acyclicity, knowledge hub
payloads, deployable-node minimal metadata) align with
:data:`akc.pass_registry.ARTIFACT_PASS_ORDER` lowering and
:func:`akc.compile.artifact_passes.run_runtime_bundle_pass` expectations for
``knowledge_layer_ref`` and ``deployment_intents``.
"""

from __future__ import annotations

from collections import defaultdict, deque
from collections.abc import Mapping
from typing import Any

from akc.ir import IRDocument
from akc.knowledge.persistence import KNOWLEDGE_SNAPSHOT_RELPATH

_DEPLOYABLE_KINDS: frozenset[str] = frozenset({"service", "integration", "infrastructure", "agent"})

# Keys consumed by :func:`akc.runtime.policy._apply_ir_policy_nodes_to_runtime_actions` (metadata or top-level).
_POLICY_RUNTIME_ACTION_KEYS: frozenset[str] = frozenset(
    {
        "runtime_allow_actions",
        "runtime_deny_actions",
        "allow_runtime_actions",
        "deny_runtime_actions",
    }
)


def _policy_runtime_action_value_ok(value: Any) -> bool:
    if value is None:
        return True
    if not isinstance(value, list):
        return False
    return all(isinstance(x, str) for x in value)


def _ir_policy_runtime_metadata_issues(ir: IRDocument) -> list[str]:
    """Reject unknown ``metadata`` keys and non-list action values on ``policy`` nodes."""

    issues: list[str] = []
    for node in ir.nodes:
        if node.kind != "policy":
            continue
        props = dict(node.properties)
        raw_meta = props.get("metadata")
        if raw_meta is not None:
            if not isinstance(raw_meta, Mapping):
                issues.append(f"policy node {node.id!r} metadata must be an object when set")
            else:
                meta = dict(raw_meta)
                for key, val in meta.items():
                    ks = str(key)
                    if ks not in _POLICY_RUNTIME_ACTION_KEYS:
                        issues.append(
                            f"policy node {node.id!r} metadata has unknown key {ks!r} "
                            f"(allowed: {', '.join(sorted(_POLICY_RUNTIME_ACTION_KEYS))})"
                        )
                    elif not _policy_runtime_action_value_ok(val):
                        issues.append(f"policy node {node.id!r} metadata[{ks!r}] must be a list of strings or null")
        for dk in _POLICY_RUNTIME_ACTION_KEYS:
            if dk not in props:
                continue
            val = props[dk]
            if not _policy_runtime_action_value_ok(val):
                issues.append(f"policy node {node.id!r} property {dk!r} must be a list of strings or null")
    return issues


def validate_ir_operational_contracts(ir: IRDocument) -> tuple[str, ...]:
    """Return human-readable issues for workflow/intent contracts; empty means valid."""

    issues: list[str] = []
    for node in ir.nodes:
        if node.kind == "workflow":
            c = node.contract
            if c is None:
                issues.append(f"workflow node {node.id!r} missing OperationalContract")
            elif c.contract_category != "runtime":
                issues.append(
                    f"workflow node {node.id!r} expected contract_category 'runtime', got {c.contract_category!r}"
                )
        elif node.kind == "intent":
            c = node.contract
            if c is None:
                issues.append(f"intent node {node.id!r} missing OperationalContract")
            elif c.contract_category != "acceptance":
                issues.append(
                    f"intent node {node.id!r} expected contract_category 'acceptance', got {c.contract_category!r}"
                )
    return tuple(issues)


def validate_ir_graph_integrity(ir: IRDocument) -> tuple[str, ...]:
    """Referential integrity, ``depends_on`` DAG, knowledge hub payloads, deployable nodes.

    Mirrors fields consumed by :func:`~akc.compile.artifact_passes.run_runtime_bundle_pass`
    for ``knowledge_layer_ref`` (first ``kind=knowledge`` node by id) and
    ``deployment_intents`` (``service`` / ``integration`` / ``infrastructure`` / ``agent``).
    """

    issues: list[str] = []
    issues.extend(_depends_on_referential_and_dag_issues(ir))
    issues.extend(_knowledge_hub_property_issues(ir))
    issues.extend(_deployable_node_minimal_issues(ir))
    issues.extend(_ir_policy_runtime_metadata_issues(ir))
    return tuple(issues)


def validate_ir_operational_structure(ir: IRDocument) -> tuple[str, ...]:
    """Return human-readable issues; empty tuple means valid.

    Combines :func:`validate_ir_operational_contracts` and
    :func:`validate_ir_graph_integrity` for callers that apply a single policy knob.
    """

    return validate_ir_operational_contracts(ir) + validate_ir_graph_integrity(ir)


def _depends_on_referential_and_dag_issues(ir: IRDocument) -> list[str]:
    issues: list[str] = []
    by_id = {n.id: n for n in ir.nodes}

    for node in ir.nodes:
        for dep in node.depends_on:
            ds = str(dep).strip()
            if not ds:
                issues.append(f"ir node {node.id!r} has an empty depends_on entry")
                continue
            if ds == node.id:
                issues.append(f"ir node {node.id!r} depends_on must not include self ({ds!r})")
            elif ds not in by_id:
                issues.append(f"ir node {node.id!r} depends_on references unknown node id {ds!r}")

    # Acyclicity on valid internal edges (unknown targets excluded; self-loops already flagged).
    indeg: dict[str, int] = {n.id: 0 for n in ir.nodes}
    dependents: dict[str, list[str]] = defaultdict(list)
    for n in ir.nodes:
        for d in set(n.depends_on):
            ds = str(d).strip()
            if ds in by_id and ds != n.id:
                dependents[ds].append(n.id)
                indeg[n.id] += 1

    queue = deque([nid for nid, k in indeg.items() if k == 0])
    processed = 0
    while queue:
        u = queue.popleft()
        processed += 1
        for v in dependents[u]:
            indeg[v] -= 1
            if indeg[v] == 0:
                queue.append(v)

    if processed != len(by_id):
        issues.append(
            "depends_on graph contains a cycle (dependencies among ir nodes must be acyclic for bundle closure)"
        )
    return issues


def _knowledge_hub_property_issues(ir: IRDocument) -> list[str]:
    hubs = [n for n in ir.nodes if n.kind == "knowledge"]
    if not hubs:
        return []

    issues: list[str] = []
    if len(hubs) > 1:
        ids = ", ".join(sorted(h.id for h in hubs))
        issues.append(
            f"multiple knowledge nodes ({len(hubs)}); runtime_bundle uses lexicographically first id only ({ids})"
        )

    hub = sorted(hubs, key=lambda h: h.id)[0]
    props = dict(hub.properties)

    sem = props.get("knowledge_semantic_fingerprint_16")
    prov = props.get("knowledge_provenance_fingerprint_16")
    aids_raw = props.get("knowledge_assertion_ids")

    sem_ok = isinstance(sem, str) and bool(sem.strip())
    prov_ok = isinstance(prov, str) and bool(prov.strip())
    aids_ok = isinstance(aids_raw, list) and any(isinstance(x, str) and bool(str(x).strip()) for x in aids_raw)

    if not sem_ok and not prov_ok and not aids_ok:
        issues.append(
            f"knowledge node {hub.id!r} must set at least one of knowledge_semantic_fingerprint_16, "
            f"knowledge_provenance_fingerprint_16, or a non-empty knowledge_assertion_ids list "
            f"(matches hub emission rules in ir_builder)"
        )

    rel = props.get("persisted_snapshot_relpath")
    if rel is None:
        issues.append(
            f"knowledge node {hub.id!r} missing persisted_snapshot_relpath "
            f"(expected e.g. {KNOWLEDGE_SNAPSHOT_RELPATH!r} for knowledge_layer_ref)"
        )
    elif not isinstance(rel, str):
        issues.append(f"knowledge node {hub.id!r} persisted_snapshot_relpath must be a string when set")
    elif not rel.strip():
        issues.append(f"knowledge node {hub.id!r} persisted_snapshot_relpath must be non-empty")

    if aids_raw is not None:
        if not isinstance(aids_raw, list):
            issues.append(f"knowledge node {hub.id!r} knowledge_assertion_ids must be a list when set")
        else:
            for i, x in enumerate(aids_raw):
                if not isinstance(x, str):
                    issues.append(f"knowledge node {hub.id!r} knowledge_assertion_ids[{i}] must be a string")

    for key in ("knowledge_semantic_fingerprint_16", "knowledge_provenance_fingerprint_16"):
        v = props.get(key)
        if v is not None and not isinstance(v, str):
            issues.append(f"knowledge node {hub.id!r} {key} must be a string or null")

    return issues


def _deployable_node_minimal_issues(ir: IRDocument) -> list[str]:
    """Deployable kinds used in runtime_bundle deployment_intents should expose capability metadata."""

    issues: list[str] = []
    for node in ir.nodes:
        if node.kind not in _DEPLOYABLE_KINDS:
            continue
        if node.effects is None and node.contract is None:
            issues.append(
                f"deployable node {node.id!r} (kind={node.kind!r}) has neither effects nor OperationalContract; "
                f"runtime_bundle deployment_intents may omit capability metadata"
            )
    return issues
