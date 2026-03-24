# Coordination spec: lowering, determinism, and errors

This document is **normative** for how AKC turns a coordination JSON graph into **execution layers** (topological levels). It applies to `akc.coordination.models.CoordinationScheduler`, `akc runtime coordination-plan`, and the TypeScript SDK in `src/akc/coordination/static/coordination_sdk.ts`.

## Spec version

- **`spec_version`** (integer, required in schema) and optional **`coordination_spec_version`** (integer) both describe the same logical version when present.
- Parsers (Python `parse_coordination_obj`, TS `parseCoordinationObj`) resolve the effective version as: use **`coordination_spec_version`** if it is a number; else **`spec_version`** if it is a number; else **1**.
- If **both** are numbers and they **differ**, loading raises **`CoordinationParseError`** with a stable message (`coordination_spec_version and spec_version must match when both are present`).

**v1:** only **`depends_on`** edges participate in scheduling.

**v2:** effective version ≥ **2**. Edge kinds **`parallel`**, **`barrier`**, **`delegate`**, and **`handoff`** are **schedulable** and are **lowered** to precedence constraints (below). Unknown kinds still raise **`CoordinationUnsupportedEdgeKind`**.

## Lowering (v2 reserved kinds → precedence)

For layer scheduling, each edge of kind **`depends_on`**, **`parallel`**, **`barrier`**, **`delegate`**, or **`handoff`** contributes one **precedence** constraint: **`src_step_id` must complete before `dst_step_id`**, identical to a **`depends_on`** edge between the same endpoints.

- **`parallel`:** fork sugar — same precedence as `depends_on` (branches run in the same layer after the source once the source has no other predecessors).
- **`barrier`:** join sugar — N edges into `dst` imply an AND-join (each source is a predecessor); lowering is still one precedence edge per barrier edge, same as N `depends_on` edges.
- **`handoff` / `delegate`:** control-flow order matches **`depends_on`** for v2 scheduling. For **validation**, v2 **`handoff`** edges require `metadata.handoff_id` (non-empty string) and v2 **`delegate`** edges require `metadata.delegate_target` (non-empty string). Additional fields remain for kernel/worker phases.

Self-edges (`src_step_id == dst_step_id`) are ignored for precedence (no cycle, no indegree change).

## Determinism

- **Step set:** step ids are the union of nodes with `kind: "step"` and all edge endpoints, **deduplicated**, then sorted **lexicographically** (`step_ids_for_scheduling`).
- **Layers:** classic Kahn topological layering. Within each layer, ready steps (indegree zero) are emitted in **lexicographic ascending** order of `step_id`.
- **Edge iteration order** when building indegrees does **not** change layer membership; only the **graph structure** and **tie-break above** matter.

## Validation errors (Python)

All are subclasses of **`CoordinationParseError`** unless noted.

| Exception | When |
|-----------|------|
| **`CoordinationParseError`** | Malformed JSON shape, missing `coordination_graph`, version mismatch between `spec_version` and `coordination_spec_version`, invalid governance paths, etc. Also v2 **`handoff`** / **`delegate`** edges with missing or invalid required `metadata` (`handoff_id`, `delegate_target`) — stable messages include edge id and field name. |
| **`CoordinationReservedEdgeRequiresSpecV2`** (subtype of **`CoordinationUnsupportedEdgeKind`**) | Edge kind is `parallel`, `barrier`, `delegate`, or `handoff` while effective spec version is **1**. |
| **`CoordinationUnsupportedEdgeKind`** | Unknown `kind`, or kind not allowed for the effective spec version. |
| **`CoordinationCycleError`** | After lowering, the precedence graph has a **cycle** (indegree never reaches zero). |

Runtime bundle fingerprint and ref checks are unchanged; see [runtime-execution.md](runtime-execution.md).
