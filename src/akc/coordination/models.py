"""Coordination graph models and deterministic layer scheduling.

**Spec v1** schedules only ``depends_on`` precedence edges.

**Spec v2** (``spec_version >= 2`` or ``coordination_spec_version >= 2``) additionally
accepts reserved kinds ``parallel``, ``barrier``, ``delegate``, and ``handoff``. For
scheduling they are **lowered** to the same precedence semantics as ``depends_on`` (see
``docs/coordination-semantics.md``). Runtime handoff/delegate metadata is preserved in
the parsed graph for later phases. This module validates required v2 handoff/delegate
``metadata`` fields when scheduling and computes layers.

This module lives under ``akc.coordination`` so compile-time emitters and the runtime
share one implementation without circular imports (runtime I/O helpers stay under
``akc.runtime.coordination``).
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any, NamedTuple

from akc.memory.models import require_non_empty


class CoordinationParseError(ValueError):
    """Coordination JSON is structurally invalid."""


class CoordinationUnsupportedEdgeKind(CoordinationParseError):
    """Edge kind is not supported for the effective coordination spec version."""


class CoordinationReservedEdgeRequiresSpecV2(CoordinationUnsupportedEdgeKind):
    """Reserved edge kinds require ``spec_version`` / ``coordination_spec_version`` >= 2."""


class CoordinationCycleError(CoordinationParseError):
    """The dependency graph contains a cycle (for ``depends_on`` edges)."""


@dataclass(frozen=True, slots=True)
class CoordinationGraphNode:
    node_id: str
    kind: str
    label: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.node_id, name="coordination.node_id")
        require_non_empty(self.kind, name="coordination.kind")


@dataclass(frozen=True, slots=True)
class CoordinationGraphEdge:
    edge_id: str
    kind: str
    src_step_id: str
    dst_step_id: str
    metadata: Mapping[str, Any] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.edge_id, name="coordination.edge_id")
        require_non_empty(self.kind, name="coordination.edge.kind")
        require_non_empty(self.src_step_id, name="coordination.src_step_id")
        require_non_empty(self.dst_step_id, name="coordination.dst_step_id")


@dataclass(frozen=True, slots=True)
class CoordinationGraph:
    nodes: tuple[CoordinationGraphNode, ...]
    edges: tuple[CoordinationGraphEdge, ...]


@dataclass(frozen=True, slots=True)
class OrchestrationBinding:
    role_name: str
    agent_name: str
    orchestration_step_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        require_non_empty(self.role_name, name="coordination.binding.role_name")
        require_non_empty(self.agent_name, name="coordination.binding.agent_name")


@dataclass(frozen=True, slots=True)
class FilesystemScopeSpec:
    """Per-role filesystem hints relative to the tenant/repo outputs root."""

    read_only_roots: tuple[str, ...]
    scratch_subdir: str | None

    def __post_init__(self) -> None:
        for r in self.read_only_roots:
            require_non_empty(str(r).strip(), name="filesystem_scope.read_only_roots[]")
        if self.scratch_subdir is not None:
            s = str(self.scratch_subdir).strip()
            if not s or s.startswith("/") or ".." in Path(s).parts:
                raise CoordinationParseError("filesystem_scope.scratch_subdir must be a safe relative path")


@dataclass(frozen=True, slots=True)
class RoleIsolationProfile:
    """Per-role isolation profile (Phase 2 — enforceable separation)."""

    filesystem_scope: FilesystemScopeSpec
    allowed_tools: tuple[str, ...]
    execution_allow_network: bool | None


@dataclass(frozen=True, slots=True)
class CoordinationGovernance:
    max_steps: int | None
    allowed_capabilities: tuple[str, ...]
    execution_allow_network: bool | None
    role_profiles: Mapping[str, RoleIsolationProfile]


@dataclass(frozen=True, slots=True)
class ParsedCoordinationSpec:
    spec_version: int
    run_id: str
    tenant_id: str
    repo_id: str
    graph: CoordinationGraph
    orchestration_bindings: tuple[OrchestrationBinding, ...]
    governance: CoordinationGovernance | None
    raw: Mapping[str, Any]

    def __post_init__(self) -> None:
        require_non_empty(self.run_id, name="coordination.run_id")
        require_non_empty(self.tenant_id, name="coordination.tenant_id")
        require_non_empty(self.repo_id, name="coordination.repo_id")


@dataclass(frozen=True, slots=True)
class CoordinationScheduleLayer:
    layer_index: int
    step_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CoordinationSchedule:
    layers: tuple[CoordinationScheduleLayer, ...]
    step_order: tuple[str, ...]
    #: One stable reason string per layer (Kahn topological layering); empty when ``layers`` is empty.
    layer_reason: tuple[str, ...] = ()
    #: JSON-serializable records describing unique precedence arcs after lowering (deduped ``src→dst``).
    lowered_precedence_edges: tuple[dict[str, Any], ...] = ()


_V1_SCHEDULABLE_KINDS = frozenset({"depends_on"})
_V2_LOWERED_PRECEDENCE_KINDS = frozenset({"parallel", "barrier", "delegate", "handoff"})
_V2_SCHEDULABLE_KINDS = _V1_SCHEDULABLE_KINDS | _V2_LOWERED_PRECEDENCE_KINDS


class _LoweredPrecedence(NamedTuple):
    src_step_id: str
    dst_step_id: str
    from_edge_id: str
    original_kind: str


def _validate_v2_handoff_edge_metadata(edge: CoordinationGraphEdge) -> None:
    if edge.metadata is None:
        raise CoordinationParseError(f"coordination edge {edge.edge_id!r} (handoff): metadata object is required")
    hid = edge.metadata.get("handoff_id")
    if not isinstance(hid, str) or not hid.strip():
        raise CoordinationParseError(
            f"coordination edge {edge.edge_id!r} (handoff): metadata.handoff_id must be a non-empty string"
        )


def _validate_v2_delegate_edge_metadata(edge: CoordinationGraphEdge) -> None:
    if edge.metadata is None:
        raise CoordinationParseError(f"coordination edge {edge.edge_id!r} (delegate): metadata object is required")
    target = edge.metadata.get("delegate_target")
    if not isinstance(target, str) or not target.strip():
        raise CoordinationParseError(
            f"coordination edge {edge.edge_id!r} (delegate): metadata.delegate_target must be a non-empty string"
        )


def _lower_edges_for_scheduling(*, graph: CoordinationGraph, spec_version: int) -> tuple[_LoweredPrecedence, ...]:
    """Map schedulable edges to precedence tuples; deterministic sort by (src, dst, edge_id).

    Reserved v2 kinds lower to the same precedence as ``depends_on``. Self-edges are omitted.
    """

    is_v2 = spec_version >= 2
    sched_kinds = _V2_SCHEDULABLE_KINDS if is_v2 else _V1_SCHEDULABLE_KINDS
    raw: list[_LoweredPrecedence] = []
    for e in graph.edges:
        if e.kind not in sched_kinds:
            continue
        if is_v2 and e.kind == "handoff":
            _validate_v2_handoff_edge_metadata(e)
        if is_v2 and e.kind == "delegate":
            _validate_v2_delegate_edge_metadata(e)
        if e.src_step_id == e.dst_step_id:
            continue
        raw.append(
            _LoweredPrecedence(
                src_step_id=e.src_step_id,
                dst_step_id=e.dst_step_id,
                from_edge_id=e.edge_id,
                original_kind=e.kind,
            )
        )
    return tuple(sorted(raw, key=lambda x: (x.src_step_id, x.dst_step_id, x.from_edge_id)))


def _dedupe_lowered_precedence(
    lowered: tuple[_LoweredPrecedence, ...],
) -> tuple[tuple[tuple[str, str], ...], tuple[dict[str, Any], ...]]:
    """One precedence arc per (src, dst); JSON-serializable provenance per arc."""

    if not lowered:
        return (), ()
    arcs: list[tuple[str, str]] = []
    dbg: list[dict[str, Any]] = []
    for (src, dst), grp in groupby(lowered, key=lambda x: (x.src_step_id, x.dst_step_id)):
        items = list(grp)
        arcs.append((src, dst))
        dbg.append(
            {
                "src_step_id": src,
                "dst_step_id": dst,
                "lowered_from_edge_ids": sorted({x.from_edge_id for x in items}),
                "original_kinds": sorted({x.original_kind for x in items}),
            }
        )
    return tuple(arcs), tuple(dbg)


def _parse_node(raw: Mapping[str, Any]) -> CoordinationGraphNode:
    node_id = str(raw.get("node_id", "")).strip()
    kind = str(raw.get("kind", "")).strip()
    label = raw.get("label")
    lab = str(label).strip() if isinstance(label, str) else None
    return CoordinationGraphNode(node_id=node_id, kind=kind, label=lab)


def _parse_edge(raw: Mapping[str, Any]) -> CoordinationGraphEdge:
    edge_id = str(raw.get("edge_id", "")).strip()
    kind = str(raw.get("kind", "")).strip()
    src = str(raw.get("src_step_id", "")).strip()
    dst = str(raw.get("dst_step_id", "")).strip()
    meta_raw = raw.get("metadata")
    meta: Mapping[str, Any] | None = None
    if isinstance(meta_raw, Mapping):
        meta = dict(meta_raw)
    elif meta_raw is not None:
        raise CoordinationParseError(f"coordination edge {edge_id!r}: metadata must be an object when present")
    return CoordinationGraphEdge(edge_id=edge_id, kind=kind, src_step_id=src, dst_step_id=dst, metadata=meta)


def parse_coordination_obj(obj: Mapping[str, Any]) -> ParsedCoordinationSpec:
    """Parse a coordination JSON object into strict dataclasses."""

    if not isinstance(obj, Mapping):
        raise CoordinationParseError("coordination spec must be a JSON object")
    run_id = str(obj.get("run_id", "")).strip()
    tenant_id = str(obj.get("tenant_id", "")).strip()
    repo_id = str(obj.get("repo_id", "")).strip()
    cv_raw = obj.get("coordination_spec_version")
    sv_raw = obj.get("spec_version")
    cv = int(cv_raw) if isinstance(cv_raw, (int, float)) else None
    sv = int(sv_raw) if isinstance(sv_raw, (int, float)) else None
    if cv is not None and sv is not None and cv != sv:
        raise CoordinationParseError(
            "coordination_spec_version and spec_version must match when both are present "
            f"(got coordination_spec_version={cv}, spec_version={sv})"
        )
    if cv is not None:
        spec_version = cv
    elif sv is not None:
        spec_version = sv
    else:
        spec_version = 1

    cg_raw = obj.get("coordination_graph")
    if not isinstance(cg_raw, Mapping):
        raise CoordinationParseError("coordination_graph must be an object")

    nodes_raw = cg_raw.get("nodes")
    if not isinstance(nodes_raw, Sequence) or isinstance(nodes_raw, (str, bytes)):
        raise CoordinationParseError("coordination_graph.nodes must be an array")
    nodes: list[CoordinationGraphNode] = []
    for item in nodes_raw:
        if isinstance(item, Mapping):
            nodes.append(_parse_node(item))

    edges_raw = cg_raw.get("edges")
    if not isinstance(edges_raw, Sequence) or isinstance(edges_raw, (str, bytes)):
        raise CoordinationParseError("coordination_graph.edges must be an array")
    edges: list[CoordinationGraphEdge] = []
    for item in edges_raw:
        if isinstance(item, Mapping):
            edges.append(_parse_edge(item))

    bindings_out: list[OrchestrationBinding] = []
    bindings_raw = obj.get("orchestration_bindings")
    if isinstance(bindings_raw, Sequence) and not isinstance(bindings_raw, (str, bytes)):
        for b in bindings_raw:
            if not isinstance(b, Mapping):
                continue
            role_name = str(b.get("role_name", "")).strip()
            agent_name = str(b.get("agent_name", "")).strip()
            oids = b.get("orchestration_step_ids")
            step_ids: list[str] = []
            if isinstance(oids, Sequence) and not isinstance(oids, (str, bytes)):
                for raw_sid in oids:
                    sid = str(raw_sid).strip()
                    if sid:
                        step_ids.append(sid)
            bindings_out.append(
                OrchestrationBinding(
                    role_name=role_name,
                    agent_name=agent_name,
                    orchestration_step_ids=tuple(step_ids),
                )
            )

    gov_raw = obj.get("governance")
    governance: CoordinationGovernance | None = None
    if isinstance(gov_raw, Mapping):
        max_steps = int(gov_raw["max_steps"]) if isinstance(gov_raw.get("max_steps"), (int, float)) else None
        caps_raw = gov_raw.get("allowed_capabilities")
        caps: tuple[str, ...] = ()
        if isinstance(caps_raw, Sequence) and not isinstance(caps_raw, (str, bytes)):
            caps = tuple(sorted({str(x).strip() for x in caps_raw if str(x).strip()}))
        allow_net = gov_raw.get("execution_allow_network")
        enet: bool | None = bool(allow_net) if isinstance(allow_net, bool) else None
        role_profiles: dict[str, RoleIsolationProfile] = {}
        rp_raw = gov_raw.get("role_profiles")
        if isinstance(rp_raw, Mapping):
            for role_key, prof_raw in rp_raw.items():
                rk = str(role_key).strip()
                if not rk or not isinstance(prof_raw, Mapping):
                    continue
                fs_raw = prof_raw.get("filesystem_scope")
                ro_list: tuple[str, ...] = ()
                scratch_sub: str | None = None
                if isinstance(fs_raw, Mapping):
                    ror = fs_raw.get("read_only_roots")
                    if isinstance(ror, Sequence) and not isinstance(ror, (str, bytes)):
                        ro_list = tuple(sorted({str(x).strip() for x in ror if str(x).strip()}))
                    ss = fs_raw.get("scratch_subdir")
                    if isinstance(ss, str) and ss.strip():
                        scratch_sub = ss.strip()
                fs_spec = FilesystemScopeSpec(read_only_roots=ro_list, scratch_subdir=scratch_sub)
                tools_raw = prof_raw.get("allowed_tools")
                tools: tuple[str, ...] = ()
                if isinstance(tools_raw, Sequence) and not isinstance(tools_raw, (str, bytes)):
                    tools = tuple(sorted({str(x).strip() for x in tools_raw if str(x).strip()}))
                rnet = prof_raw.get("execution_allow_network")
                r_enet: bool | None = bool(rnet) if isinstance(rnet, bool) else None
                role_profiles[rk] = RoleIsolationProfile(
                    filesystem_scope=fs_spec,
                    allowed_tools=tools,
                    execution_allow_network=r_enet,
                )
        governance = CoordinationGovernance(
            max_steps=max_steps,
            allowed_capabilities=caps,
            execution_allow_network=enet,
            role_profiles=role_profiles,
        )

    graph = CoordinationGraph(nodes=tuple(nodes), edges=tuple(edges))
    return ParsedCoordinationSpec(
        spec_version=spec_version,
        run_id=run_id,
        tenant_id=tenant_id,
        repo_id=repo_id,
        graph=graph,
        orchestration_bindings=tuple(bindings_out),
        governance=governance,
        raw=dict(obj),
    )


def step_ids_for_scheduling(graph: CoordinationGraph) -> tuple[str, ...]:
    """Stable ids participating in coordination: ``kind`` = ``step`` plus any edge endpoints."""

    out: set[str] = set()
    for n in graph.nodes:
        if n.kind == "step":
            out.add(n.node_id)
    for e in graph.edges:
        out.add(e.src_step_id)
        out.add(e.dst_step_id)
    return tuple(sorted(out))


class CoordinationScheduler:
    """Deterministic topological layers: within each layer, step ids are sorted lexicographically."""

    __slots__ = ("_spec",)

    def __init__(self, spec: ParsedCoordinationSpec) -> None:
        self._spec = spec

    def schedule(self) -> CoordinationSchedule:
        graph = self._spec.graph
        steps = step_ids_for_scheduling(graph)
        if not steps:
            return CoordinationSchedule(
                layers=(),
                step_order=(),
                layer_reason=(),
                lowered_precedence_edges=(),
            )

        is_v2 = self._spec.spec_version >= 2
        allowed = _V2_SCHEDULABLE_KINDS if is_v2 else _V1_SCHEDULABLE_KINDS
        for e in graph.edges:
            if e.kind in _V2_LOWERED_PRECEDENCE_KINDS and not is_v2:
                raise CoordinationReservedEdgeRequiresSpecV2(
                    f"edge kind {e.kind!r} requires spec_version or coordination_spec_version >= 2"
                )
            if e.kind not in allowed:
                raise CoordinationUnsupportedEdgeKind(
                    f"unsupported coordination edge kind {e.kind!r}; "
                    f"spec_version {self._spec.spec_version} allows {sorted(allowed)} only"
                )

        lowered = _lower_edges_for_scheduling(graph=graph, spec_version=self._spec.spec_version)
        steps_set = set(steps)
        for row in lowered:
            if row.src_step_id not in steps_set or row.dst_step_id not in steps_set:
                raise CoordinationParseError(
                    f"coordination edge {row.from_edge_id!r} references unknown step ids "
                    f"{row.src_step_id!r} -> {row.dst_step_id!r}"
                )

        arcs, lowered_dbg = _dedupe_lowered_precedence(lowered)

        adj: dict[str, list[str]] = {s: [] for s in steps}
        indeg: Counter[str] = Counter(dict.fromkeys(steps, 0))
        for src, dst in arcs:
            adj[src].append(dst)
            indeg[dst] += 1

        layers: list[CoordinationScheduleLayer] = []
        layer_reason: list[str] = []
        order: list[str] = []
        remaining = set(steps)
        layer_idx = 0
        while remaining:
            layer = sorted([s for s in remaining if indeg[s] == 0])
            if not layer:
                raise CoordinationCycleError("coordination precedence graph has a cycle")
            layers.append(CoordinationScheduleLayer(layer_index=layer_idx, step_ids=tuple(layer)))
            layer_reason.append(f"kahn_layer:{layer_idx}")
            order.extend(layer)
            for s in layer:
                remaining.remove(s)
                for nxt in adj[s]:
                    indeg[nxt] -= 1
            layer_idx += 1

        return CoordinationSchedule(
            layers=tuple(layers),
            step_order=tuple(order),
            layer_reason=tuple(layer_reason),
            lowered_precedence_edges=lowered_dbg,
        )


def schedule_coordination_layers(spec: ParsedCoordinationSpec) -> CoordinationSchedule:
    """Convenience helper — same as :class:`CoordinationScheduler`(...).schedule()."""

    return CoordinationScheduler(spec).schedule()
