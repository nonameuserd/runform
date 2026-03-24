/**
 * AKC coordination SDK (TypeScript) — mirrors ``akc.coordination.models`` scheduling semantics.
 * Types align with ``schemas/agent_coordination_spec.v1.schema.json`` (spec_version 1–2).
 */
/// <reference types="node" />
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";

const V1_SCHEDULABLE_KINDS = new Set<string>(["depends_on"]);
const V2_LOWERED_PRECEDENCE_KINDS = new Set<string>(["parallel", "barrier", "delegate", "handoff"]);
const V2_SCHEDULABLE_KINDS = new Set<string>([...V1_SCHEDULABLE_KINDS, ...V2_LOWERED_PRECEDENCE_KINDS]);

/** Raised when coordination JSON fails structural or scope validation. */
export class CoordinationParseError extends Error {
  override readonly name: string = "CoordinationParseError";
}

/** Raised when an edge kind is not schedulable in the current spec version. */
export class CoordinationUnsupportedEdgeKind extends CoordinationParseError {
  override readonly name: string = "CoordinationUnsupportedEdgeKind";
}

/**
 * Reserved edge kinds (`parallel`, `barrier`, `delegate`, `handoff`) on a v1 effective spec.
 * Mirrors Python `CoordinationReservedEdgeRequiresSpecV2`.
 */
export class CoordinationReservedEdgeRequiresSpecV2 extends CoordinationUnsupportedEdgeKind {
  override readonly name: string = "CoordinationReservedEdgeRequiresSpecV2";
}

/** Raised when precedence edges form a cycle. */
export class CoordinationCycleError extends CoordinationParseError {
  override readonly name: string = "CoordinationCycleError";
}

/** Graph node in ``coordination_graph.nodes`` (role, step, …). */
export interface CoordinationGraphNodeJson {
  node_id: string;
  kind: string;
  label?: string;
  tools?: string[];
  [key: string]: unknown;
}

export interface CoordinationGraphEdgeJson {
  edge_id: string;
  kind: string;
  src_step_id: string;
  dst_step_id: string;
  metadata?: Record<string, unknown>;
  [key: string]: unknown;
}

export interface CoordinationGraphJson {
  nodes: CoordinationGraphNodeJson[];
  edges: CoordinationGraphEdgeJson[];
}

export interface OrchestrationBindingJson {
  role_name: string;
  agent_name: string;
  orchestration_step_ids: string[];
}

/** Parsed coordination document (scheduling-relevant fields). */
export interface ParsedCoordinationSpec {
  spec_version: number;
  run_id: string;
  tenant_id: string;
  repo_id: string;
  graph: CoordinationGraphJson;
  orchestration_bindings: OrchestrationBindingJson[];
  raw: Record<string, unknown>;
}

export interface CoordinationScheduleLayer {
  layer_index: number;
  step_ids: string[];
}

export interface CoordinationSchedule {
  layers: CoordinationScheduleLayer[];
  step_order: string[];
  /** Stable Kahn layer labels; omitted when empty (no layers). */
  layer_reason?: string[];
  /** Deduped precedence arcs after lowering (debug / CLI). */
  lowered_precedence_edges?: Record<string, unknown>[];
}

function requireNonEmpty(value: string, name: string): void {
  if (!String(value).trim()) {
    throw new CoordinationParseError(`${name} must be non-empty`);
  }
}

function parseNode(raw: Record<string, unknown>): CoordinationGraphNodeJson {
  const node_id = String(raw.node_id ?? "").trim();
  const kind = String(raw.kind ?? "").trim();
  requireNonEmpty(node_id, "coordination.node_id");
  requireNonEmpty(kind, "coordination.kind");
  const labelRaw = raw.label;
  const label = typeof labelRaw === "string" && labelRaw.trim() ? labelRaw.trim() : undefined;
  const out: CoordinationGraphNodeJson = { node_id, kind };
  if (label !== undefined) {
    out.label = label;
  }
  if (Array.isArray(raw.tools)) {
    out.tools = raw.tools.map((t) => String(t));
  }
  return out;
}

function parseEdge(raw: Record<string, unknown>): CoordinationGraphEdgeJson {
  const edge_id = String(raw.edge_id ?? "").trim();
  const kind = String(raw.kind ?? "").trim();
  const src_step_id = String(raw.src_step_id ?? "").trim();
  const dst_step_id = String(raw.dst_step_id ?? "").trim();
  requireNonEmpty(edge_id, "coordination.edge_id");
  requireNonEmpty(kind, "coordination.edge.kind");
  requireNonEmpty(src_step_id, "coordination.src_step_id");
  requireNonEmpty(dst_step_id, "coordination.dst_step_id");
  const out: CoordinationGraphEdgeJson = { edge_id, kind, src_step_id, dst_step_id };
  if (raw.metadata !== undefined) {
    if (typeof raw.metadata !== "object" || raw.metadata === null || Array.isArray(raw.metadata)) {
      throw new CoordinationParseError(
        `coordination edge '${edge_id}': metadata must be an object when present`,
      );
    }
    out.metadata = { ...(raw.metadata as Record<string, unknown>) };
  }
  return out;
}

function parseCoordinationObj(obj: Record<string, unknown>): ParsedCoordinationSpec {
  const run_id = String(obj.run_id ?? "").trim();
  const tenant_id = String(obj.tenant_id ?? "").trim();
  const repo_id = String(obj.repo_id ?? "").trim();
  const cvRaw = obj.coordination_spec_version;
  const svRaw = obj.spec_version;
  const cv = typeof cvRaw === "number" && Number.isFinite(cvRaw) ? Math.trunc(cvRaw) : undefined;
  const sv = typeof svRaw === "number" && Number.isFinite(svRaw) ? Math.trunc(svRaw) : undefined;
  if (cv !== undefined && sv !== undefined && cv !== sv) {
    throw new CoordinationParseError(
      `coordination_spec_version and spec_version must match when both are present (got coordination_spec_version=${cv}, spec_version=${sv})`,
    );
  }
  const spec_version = cv !== undefined ? cv : sv !== undefined ? sv : 1;

  requireNonEmpty(run_id, "coordination.run_id");
  requireNonEmpty(tenant_id, "coordination.tenant_id");
  requireNonEmpty(repo_id, "coordination.repo_id");

  const cgRaw = obj.coordination_graph;
  if (typeof cgRaw !== "object" || cgRaw === null || Array.isArray(cgRaw)) {
    throw new CoordinationParseError("coordination_graph must be an object");
  }
  const cg = cgRaw as Record<string, unknown>;
  const nodesRaw = cg.nodes;
  if (!Array.isArray(nodesRaw)) {
    throw new CoordinationParseError("coordination_graph.nodes must be an array");
  }
  const nodes: CoordinationGraphNodeJson[] = [];
  for (const item of nodesRaw) {
    if (typeof item === "object" && item !== null && !Array.isArray(item)) {
      nodes.push(parseNode(item as Record<string, unknown>));
    }
  }
  const edgesRaw = cg.edges;
  if (!Array.isArray(edgesRaw)) {
    throw new CoordinationParseError("coordination_graph.edges must be an array");
  }
  const edges: CoordinationGraphEdgeJson[] = [];
  for (const item of edgesRaw) {
    if (typeof item === "object" && item !== null && !Array.isArray(item)) {
      edges.push(parseEdge(item as Record<string, unknown>));
    }
  }

  const bindingsOut: OrchestrationBindingJson[] = [];
  const bindingsRaw = obj.orchestration_bindings;
  if (Array.isArray(bindingsRaw)) {
    for (const b of bindingsRaw) {
      if (typeof b !== "object" || b === null || Array.isArray(b)) {
        continue;
      }
      const bm = b as Record<string, unknown>;
      const role_name = String(bm.role_name ?? "").trim();
      const agent_name = String(bm.agent_name ?? "").trim();
      requireNonEmpty(role_name, "coordination.binding.role_name");
      requireNonEmpty(agent_name, "coordination.binding.agent_name");
      const oids = bm.orchestration_step_ids;
      const step_ids: string[] = [];
      if (Array.isArray(oids)) {
        for (const rawSid of oids) {
          const sid = String(rawSid).trim();
          if (sid) {
            step_ids.push(sid);
          }
        }
      }
      bindingsOut.push({ role_name, agent_name, orchestration_step_ids: step_ids });
    }
  }

  return {
    spec_version,
    run_id,
    tenant_id,
    repo_id,
    graph: { nodes, edges },
    orchestration_bindings: bindingsOut,
    raw: { ...obj },
  };
}

/**
 * Collect step ids that participate in scheduling: nodes with ``kind === "step"`` plus edge endpoints.
 * Ids are sorted lexicographically (matches Python ``step_ids_for_scheduling``).
 */
export function stepIdsForScheduling(graph: CoordinationGraphJson): string[] {
  const out = new Set<string>();
  for (const n of graph.nodes) {
    if (n.kind === "step") {
      out.add(n.node_id);
    }
  }
  for (const e of graph.edges) {
    out.add(e.src_step_id);
    out.add(e.dst_step_id);
  }
  return [...out].sort((a, b) => a.localeCompare(b));
}

interface LoweredPrecedence {
  src_step_id: string;
  dst_step_id: string;
  from_edge_id: string;
  original_kind: string;
}

function validateV2HandoffEdgeMetadata(e: CoordinationGraphEdgeJson): void {
  if (e.metadata === undefined) {
    throw new CoordinationParseError(
      `coordination edge '${e.edge_id}' (handoff): metadata object is required`,
    );
  }
  const hid = e.metadata.handoff_id;
  if (typeof hid !== "string" || !hid.trim()) {
    throw new CoordinationParseError(
      `coordination edge '${e.edge_id}' (handoff): metadata.handoff_id must be a non-empty string`,
    );
  }
}

function validateV2DelegateEdgeMetadata(e: CoordinationGraphEdgeJson): void {
  if (e.metadata === undefined) {
    throw new CoordinationParseError(
      `coordination edge '${e.edge_id}' (delegate): metadata object is required`,
    );
  }
  const target = e.metadata.delegate_target;
  if (typeof target !== "string" || !target.trim()) {
    throw new CoordinationParseError(
      `coordination edge '${e.edge_id}' (delegate): metadata.delegate_target must be a non-empty string`,
    );
  }
}

/** Lower schedulable edges to precedence tuples; deterministic order (matches Python). */
export function lowerEdgesForScheduling(
  graph: CoordinationGraphJson,
  specVersion: number,
): LoweredPrecedence[] {
  const isV2 = specVersion >= 2;
  const schedKinds = isV2 ? V2_SCHEDULABLE_KINDS : V1_SCHEDULABLE_KINDS;
  const raw: LoweredPrecedence[] = [];
  for (const e of graph.edges) {
    if (!schedKinds.has(e.kind)) {
      continue;
    }
    if (isV2 && e.kind === "handoff") {
      validateV2HandoffEdgeMetadata(e);
    }
    if (isV2 && e.kind === "delegate") {
      validateV2DelegateEdgeMetadata(e);
    }
    if (e.src_step_id === e.dst_step_id) {
      continue;
    }
    raw.push({
      src_step_id: e.src_step_id,
      dst_step_id: e.dst_step_id,
      from_edge_id: e.edge_id,
      original_kind: e.kind,
    });
  }
  raw.sort((a, b) => {
    const c1 = a.src_step_id.localeCompare(b.src_step_id);
    if (c1 !== 0) {
      return c1;
    }
    const c2 = a.dst_step_id.localeCompare(b.dst_step_id);
    if (c2 !== 0) {
      return c2;
    }
    return a.from_edge_id.localeCompare(b.from_edge_id);
  });
  return raw;
}

function dedupeLoweredPrecedence(lowered: LoweredPrecedence[]): {
  arcs: [string, string][];
  debug: Record<string, unknown>[];
} {
  if (lowered.length === 0) {
    return { arcs: [], debug: [] };
  }
  const arcs: [string, string][] = [];
  const debug: Record<string, unknown>[] = [];
  let i = 0;
  while (i < lowered.length) {
    const src = lowered[i]!.src_step_id;
    const dst = lowered[i]!.dst_step_id;
    const group: LoweredPrecedence[] = [];
    while (i < lowered.length && lowered[i]!.src_step_id === src && lowered[i]!.dst_step_id === dst) {
      group.push(lowered[i]!);
      i++;
    }
    arcs.push([src, dst]);
    const eids = [...new Set(group.map((x) => x.from_edge_id))].sort((a, b) => a.localeCompare(b));
    const kinds = [...new Set(group.map((x) => x.original_kind))].sort((a, b) => a.localeCompare(b));
    debug.push({
      src_step_id: src,
      dst_step_id: dst,
      lowered_from_edge_ids: eids,
      original_kinds: kinds,
    });
  }
  return { arcs, debug };
}

/**
 * Deterministic topological layers, matching Python ``CoordinationScheduler`` in ``akc.coordination.models``.
 * v2 lowers parallel/barrier/delegate/handoff to the same precedence as ``depends_on``.
 */
export function scheduleCoordination(spec: ParsedCoordinationSpec): CoordinationSchedule {
  const graph = spec.graph;
  const steps = stepIdsForScheduling(graph);
  if (steps.length === 0) {
    return { layers: [], step_order: [] };
  }

  const isV2 = spec.spec_version >= 2;
  const allowed = isV2 ? V2_SCHEDULABLE_KINDS : V1_SCHEDULABLE_KINDS;
  for (const e of graph.edges) {
    if (V2_LOWERED_PRECEDENCE_KINDS.has(e.kind) && !isV2) {
      throw new CoordinationReservedEdgeRequiresSpecV2(
        `edge kind '${e.kind}' requires spec_version or coordination_spec_version >= 2`,
      );
    }
    if (!allowed.has(e.kind)) {
      throw new CoordinationUnsupportedEdgeKind(
        `unsupported coordination edge kind '${e.kind}'; spec_version ${spec.spec_version} allows ${[...allowed].sort().join(", ")} only`,
      );
    }
  }

  const lowered = lowerEdgesForScheduling(graph, spec.spec_version);
  const stepsSet = new Set(steps);
  for (const row of lowered) {
    if (!stepsSet.has(row.src_step_id) || !stepsSet.has(row.dst_step_id)) {
      throw new CoordinationParseError(
        `coordination edge '${row.from_edge_id}' references unknown step ids '${row.src_step_id}' -> '${row.dst_step_id}'`,
      );
    }
  }

  const { arcs, debug } = dedupeLoweredPrecedence(lowered);

  const adj = new Map<string, string[]>();
  const indeg = new Map<string, number>();
  for (const s of steps) {
    adj.set(s, []);
    indeg.set(s, 0);
  }
  for (const [src, dst] of arcs) {
    adj.get(src)!.push(dst);
    indeg.set(dst, (indeg.get(dst) ?? 0) + 1);
  }

  const layers: CoordinationScheduleLayer[] = [];
  const layerReason: string[] = [];
  const order: string[] = [];
  const remaining = new Set(steps);
  let layerIdx = 0;
  while (remaining.size > 0) {
    const layer = [...remaining]
      .filter((s) => (indeg.get(s) ?? 0) === 0)
      .sort((a, b) => a.localeCompare(b));
    if (layer.length === 0) {
      throw new CoordinationCycleError("coordination precedence graph has a cycle");
    }
    layers.push({ layer_index: layerIdx, step_ids: layer });
    layerReason.push(`kahn_layer:${layerIdx}`);
    order.push(...layer);
    for (const s of layer) {
      remaining.delete(s);
      for (const nxt of adj.get(s) ?? []) {
        indeg.set(nxt, (indeg.get(nxt) ?? 0) - 1);
      }
    }
    layerIdx += 1;
  }

  const out: CoordinationSchedule = { layers, step_order: order };
  if (layerReason.length > 0) {
    out.layer_reason = layerReason;
  }
  if (debug.length > 0) {
    out.lowered_precedence_edges = debug;
  }
  return out;
}

/**
 * Read a coordination JSON file from disk, enforce tenant/repo isolation, and parse
 * the graph (same preconditions as ``akc.coordination.protocol.load_coordination_spec_file``).
 */
export function loadCoordinationSpec(
  tenantId: string,
  repoId: string,
  specPath: string,
): ParsedCoordinationSpec {
  const raw = readFileSync(specPath, "utf-8");
  const payload = JSON.parse(raw) as unknown;
  if (typeof payload !== "object" || payload === null || Array.isArray(payload)) {
    throw new CoordinationParseError("coordination spec must be a JSON object");
  }
  const obj = payload as Record<string, unknown>;
  const roles = obj.agent_roles;
  if (!Array.isArray(roles) || roles.length === 0) {
    throw new CoordinationParseError("agent_roles must be a non-empty list");
  }
  const bindingsRaw = obj.orchestration_bindings;
  if (!Array.isArray(bindingsRaw) || bindingsRaw.length === 0) {
    throw new CoordinationParseError("orchestration_bindings must be a non-empty list");
  }
  const spec = parseCoordinationObj(obj);
  if (spec.tenant_id !== tenantId || spec.repo_id !== repoId) {
    throw new CoordinationParseError("tenant/repo scope mismatch for coordination spec");
  }
  if (spec.orchestration_bindings.length === 0) {
    throw new CoordinationParseError("orchestration_bindings must be a non-empty list");
  }
  return spec;
}

/** JSON-serialize a schedule for CLIs and logging. */
export function coordinationScheduleToJson(schedule: CoordinationSchedule): Record<string, unknown> {
  const out: Record<string, unknown> = {
    layers: schedule.layers.map((layer) => ({
      layer_index: layer.layer_index,
      step_ids: [...layer.step_ids],
    })),
    step_order: [...schedule.step_order],
  };
  if (schedule.layer_reason !== undefined && schedule.layer_reason.length > 0) {
    out.layer_reason = [...schedule.layer_reason];
  }
  if (schedule.lowered_precedence_edges !== undefined && schedule.lowered_precedence_edges.length > 0) {
    out.lowered_precedence_edges = schedule.lowered_precedence_edges.map((x) => ({ ...x }));
  }
  return out;
}

/** Parse argv for ``coordinationRunCli`` / generated protocol CLIs. */
export function parseCoordinationCliArgs(argv: string[]): {
  tenantId: string;
  repoId: string;
  specPath: string;
} {
  const tenantIdx = argv.indexOf("--tenant-id");
  const repoIdx = argv.indexOf("--repo-id");
  const pathIdx = argv.indexOf("--spec-path");
  if (tenantIdx < 0 || tenantIdx + 1 >= argv.length) {
    throw new CoordinationParseError("missing --tenant-id");
  }
  if (repoIdx < 0 || repoIdx + 1 >= argv.length) {
    throw new CoordinationParseError("missing --repo-id");
  }
  const tenantId = argv[tenantIdx + 1]!;
  const repoId = argv[repoIdx + 1]!;
  let specPath = "";
  if (pathIdx >= 0 && pathIdx + 1 < argv.length) {
    specPath = argv[pathIdx + 1]!;
  }
  return { tenantId, repoId, specPath };
}

/** Load, schedule, and print JSON to stdout (used by generated protocol files). */
export function coordinationRunCli(argv: string[], defaultSpecPath: string): void {
  const { tenantId, repoId, specPath } = parseCoordinationCliArgs(argv);
  const path = specPath || defaultSpecPath;
  const spec = loadCoordinationSpec(tenantId, repoId, path);
  const sched = scheduleCoordination(spec);
  console.log(JSON.stringify(coordinationScheduleToJson(sched), null, 2));
}

/** Whether this module is the Node entrypoint (for optional CLI behavior). */
export function coordinationIsMainModule(argv: string[], importMetaUrl: string): boolean {
  if (argv.length < 2) {
    return false;
  }
  try {
    return importMetaUrl === pathToFileURL(argv[1]!).href;
  } catch {
    return false;
  }
}
