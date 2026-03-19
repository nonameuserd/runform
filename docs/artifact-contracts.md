## Artifact contracts (Phase 3)

This document freezes the **viewer-facing** artifact formats so we can evolve internals without changing the viewer trust boundary.

### Stability goals

- **Backwards compatibility**: additive changes must not break existing viewers.
- **Deterministic output**: artifacts must be emitted under a deterministic, tenant-scoped output directory.
- **Tenant isolation**: no cross-tenant reads/writes; emitted artifacts must not escape `<output_dir>/<tenant_id>/<repo_id>/...`.
- **Evidence completeness**: evidence artifacts must represent what executed and what verification/test gates decided.

### Tenant-scoped path rules

All compile outputs are scoped under:

- `<output_dir>/<tenant_id>/<repo_id>/manifest.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/tests/*.json` and companion `.txt` streams
- `<output_dir>/<tenant_id>/<repo_id>/.akc/verification/*.json`

Enforcement:

- Emission uses path normalization and rejects traversal in `artifact.path`.
- The filesystem emitter rejects writes outside the scoped directory.
- Executors enforce `cwd` to remain under a per-scope work root.

### Schema versioning

Artifacts may include an envelope:

- `schema_version` (integer)
- `schema_id` (string, e.g. `akc:manifest:v1`)

These fields are **optional** for backward compatibility, but when present they must be valid for the schema version.

### Frozen schemas (v1)

Machine-checkable schemas live in `src/akc/artifacts/schemas.py`:

- **manifest**: emitted `manifest.json`
- **plan_state**: serialized `PlanState` JSON objects
- **execution_stage**: `.akc/tests/*.json` stage evidence records
- **verifier_result**: `.akc/verification/*.json` verifier decisions/findings

### `manifest.json` stability rules

- Must remain a JSON object with required keys: `tenant_id`, `repo_id`, `name`, `artifacts`.
- Artifact entries must include: `path`, `media_type`, `sha256`, `size_bytes`.
- Additive fields are allowed (top-level and per-artifact) to avoid breaking older viewers.

### `.akc/tests/*.json` evidence stability rules

Each record represents a stage execution:

- Must include: `plan_id`, `step_id`, `command`.
- Should include: `stage`, `exit_code`, `duration_ms`, `stdout`, `stderr` (can be `null`).

Companion text artifacts may be emitted for viewer convenience, but JSON records are the stable source of truth for what executed.

### Security/correctness tie-in

The stable evidence artifacts are designed to answer:

- **What was executed**: `command`, `stdout`, `stderr`, `exit_code`, `duration_ms`.
- **What gates decided**: verifier result includes `passed`, `findings[]`, `policy`.

