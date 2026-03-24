# Coordination spec: lowering, determinism, and errors

This document is **normative** for how AKC turns a coordination JSON graph into **execution layers** (Kahn topological levels). It is the contract for **parsing + scheduling** shared by compile, runtime, and tooling.

## Where this is implemented

| Surface | Location |
|--------|----------|
| **Canonical scheduler and parser** | `akc.coordination.models` — `parse_coordination_obj`, `step_ids_for_scheduling`, `CoordinationScheduler`, `schedule_coordination_layers`, exception types |
| **Protocol helper** | `akc.coordination.protocol.schedule_coordination` — same semantics as `schedule_coordination_layers` |
| **Runtime import alias** | `akc.runtime.coordination.models` re-exports `akc.coordination.models` (backward-compatible path; kernel/CLI may import either) |
| **CLI** | `akc runtime coordination-plan` → `akc.cli.runtime.cmd_runtime_coordination_plan` |
| **TypeScript SDK** | `src/akc/coordination/static/coordination_sdk.ts` — `parseCoordinationObj`, `lowerEdgesForScheduling`, `scheduleCoordination`, `coordinationScheduleToJson`, etc. |
| **JSON Schema (artifact shape)** | `src/akc/coordination/schemas/agent_coordination_spec.v1.schema.json` (schema id stays v1; `spec_version` / `coordination_spec_version` may be 1 or 2) |

The parser used for scheduling does not require every field the schema marks required (for example it does not validate `agent_roles`); **schema validation** and **bundle checks** add further constraints. Scheduling behavior below applies once a document is accepted by `parse_coordination_obj` / `parseCoordinationObj`.

## Spec version

- **`spec_version`** (integer in schema; Python also accepts JSON numbers coerced with `int()`, e.g. `2.0`) and optional **`coordination_spec_version`** describe the same logical version when present.
- Parsers (Python `parse_coordination_obj`, TS `parseCoordinationObj`) resolve the effective version as: use **`coordination_spec_version`** if it is a number; else **`spec_version`** if it is a number; else **1**.
- If **both** are numbers and they **differ**, loading raises **`CoordinationParseError`** with message **`coordination_spec_version and spec_version must match when both are present (got coordination_spec_version=…, spec_version=…)`** (exact numeric values included).

**v1:** only **`depends_on`** edges participate in scheduling.

**v2:** effective version ≥ **2**. Edge kinds **`parallel`**, **`barrier`**, **`delegate`**, and **`handoff`** are **schedulable** and are **lowered** to precedence constraints (below). Unknown kinds still raise **`CoordinationUnsupportedEdgeKind`**.

## Lowering (v2 reserved kinds → precedence)

For layer scheduling, each edge of kind **`depends_on`**, **`parallel`**, **`barrier`**, **`delegate`**, or **`handoff`** contributes one **precedence tuple** `(src_step_id, dst_step_id, edge_id, original_kind)` before deduplication. Semantically each tuple is **`src_step_id` must complete before `dst_step_id`**, identical to a **`depends_on`** edge between the same endpoints.

- **`parallel`:** fork sugar — same precedence as `depends_on` (branches run in the same layer after the source once the source has no other predecessors).
- **`barrier`:** join sugar — N edges into `dst` imply an AND-join (each source is a predecessor); lowering still emits one tuple per barrier edge before deduplication.
- **`handoff` / `delegate`:** control-flow order matches **`depends_on`** for v2 scheduling. For **validation**, v2 **`handoff`** edges require a **`metadata`** object with **`handoff_id`** (non-empty string) and v2 **`delegate`** edges require **`metadata.delegate_target`** (non-empty string). Additional fields remain for kernel/worker phases.

Self-edges (`src_step_id == dst_step_id`) are ignored for precedence (no cycle, no indegree change).

### Duplicate `(src_step_id, dst_step_id)` arcs

After lowering and sorting, **multiple tuples with the same `(src, dst)` are merged into a single precedence arc** for indegree and adjacency. Scheduling therefore matches a simple directed graph with **at most one edge per ordered pair**. Provenance is preserved in **`lowered_precedence_edges`** on **`CoordinationSchedule`**: each record has `src_step_id`, `dst_step_id`, `lowered_from_edge_ids`, and `original_kinds` (sorted, deduplicated). This matters when several parallel or barrier edges collapse to the same pair.

## Determinism

- **Step set:** step ids are the union of nodes with `kind: "step"` and all edge endpoints, **deduplicated**, then sorted **lexicographically** (`step_ids_for_scheduling` / `stepIdsForScheduling`).
- **Layers:** classic Kahn topological layering on **deduped** precedence arcs. Within each layer, ready steps (indegree zero) are emitted in **lexicographic ascending** order of `step_id`.
- **Edge iteration order** when building indegrees does **not** change layer membership beyond deduplication; **graph structure**, **deduped arcs**, and **lexicographic tie-break** determine layers.
- **Layer labels:** for each layer index *i*, **`layer_reason`** contains **`kahn_layer:`** *i* (Python returns a tuple of the same length as **`layers`**; TS includes **`layer_reason`** only when non-empty). When there are no layers (empty step set), **`layer_reason`** is empty.

## Validation errors (Python)

All are subclasses of **`CoordinationParseError`** unless noted.

| Exception | When |
|-----------|------|
| **`CoordinationParseError`** | Malformed JSON shape, missing `coordination_graph`, version mismatch between `spec_version` and `coordination_spec_version`, invalid governance paths, unknown **`src_step_id` / `dst_step_id`** on a lowered edge (not in the scheduling step set), invalid `metadata` shape, etc. v2 **`handoff`** / **`delegate`** edges with missing or invalid required `metadata` — messages include edge id and field name. |
| **`CoordinationReservedEdgeRequiresSpecV2`** (subtype of **`CoordinationUnsupportedEdgeKind`**) | Edge kind is `parallel`, `barrier`, `delegate`, or `handoff` while effective spec version is **1**. |
| **`CoordinationUnsupportedEdgeKind`** | Unknown `kind`, or kind not allowed for the effective spec version. |
| **`CoordinationCycleError`** | After lowering and deduplication, the precedence graph has a **cycle** (indegree never reaches zero). |

Runtime bundle fingerprint and ref checks are unchanged; see [runtime-execution.md](runtime-execution.md).
