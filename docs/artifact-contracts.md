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
- `<output_dir>/<tenant_id>/<repo_id>/.akc/verification/<run_id>.operational_validity_report.json` (optional; links intent/runtime evidence without embedding secrets)
- `<output_dir>/<tenant_id>/<repo_id>/.akc/verification/<run_id>.operational_assurance.json` (optional; verifier coupling result with provider outcomes + enforcement mode)
- `<output_dir>/<tenant_id>/<repo_id>/.akc/intent/*.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/design/*.system_design.json` (and optional `.md`)
- `<output_dir>/<tenant_id>/<repo_id>/.akc/orchestration/*.orchestration.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/orchestration/*.orchestrator.py|*.orchestrator.ts`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/agents/*.coordination.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/agents/*.coordination_protocol.py|*.coordination_protocol.ts`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/deployment/docker-compose.yml`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/deployment/k8s/{deployment,service,configmap}.yml`
- `<output_dir>/<tenant_id>/<repo_id>/.github/workflows/akc_deploy_<run_id>.yml`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/runtime/<run_id>.runtime_bundle.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/run/<run_id>.spans.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/run/<run_id>.otel.jsonl` (NDJSON trace export for external telemetry; runtime may append after compile)
- `<output_dir>/<tenant_id>/<repo_id>/.akc/run/<run_id>.otel_metrics.jsonl` (optional NDJSON metric export for offline operational checks)
- `<output_dir>/<tenant_id>/<repo_id>/.akc/run/<run_id>.costs.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/run/<run_id>.replay_decisions.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/run/<run_id>.recompile_triggers.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/runtime/<run_id>/<runtime_run_id>/runtime_run.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/runtime/<run_id>/<runtime_run_id>/{checkpoint,events,queue_snapshot,runtime_evidence,policy_decisions}.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/runtime/<run_id>/<runtime_run_id>/evidence/coordination_audit.jsonl` (append-only JSON lines when multi-agent coordination is active)
- `<output_dir>/<tenant_id>/<repo_id>/.akc/living/baseline.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/living/<check_id>.drift.json`
- `<output_dir>/<tenant_id>/<repo_id>/.akc/living/<check_id>.triggers.json`

Enforcement:

- Emission uses path normalization and rejects traversal in `artifact.path`.
- The filesystem emitter rejects writes outside the scoped directory.
- Executors enforce `cwd` to remain under a per-scope work root.

### Control-plane indexes (operator catalog)

Cross-run discovery uses **SQLite indexes** next to per-tenant outputs (same layout family as the cost/metrics index):

- `<output_dir>/<tenant_id>/.akc/control/metrics.sqlite` — cost rollups (`akc metrics`)
- `<output_dir>/<tenant_id>/.akc/control/operations.sqlite` — run catalog: identity, pass summary, `stable_intent_sha256`, recompile-trigger counts, runtime-evidence presence, aggregate health, optional `run_labels` synced from manifest `control_plane.run_labels` when that key is present, operator tags via `akc control runs label set`, and indexed sidecar pointers (`akc control runs …`)
- `<output_dir>/<tenant_id>/.akc/control/control_audit.jsonl` — optional append-only JSON lines for operator accountability (e.g. `akc policy explain --record-audit`); one object per line with `ts_ms`, `actor`, `action`, `tenant_id`, `details`

**Freshness:** The operations index is updated on a **best-effort** basis when compile emits a run manifest and when the runtime CLI refreshes a compile manifest with runtime control-plane links. It is **eventually consistent** with the JSON artifacts: if a manifest is copied in without going through those writers, run `akc control index rebuild --tenant-id … --outputs-root …` to rescan `<tenant_id>/*/.akc/run/*.manifest.json`. All queries are scoped by `tenant_id` (one database file per tenant under that tree); optional `repo_id` filters apply within the tenant. There are no cross-tenant reads. When `control_plane.run_labels` is **omitted** on a manifest upsert, existing index labels for that run are left unchanged; when `run_labels` is present (including `{}`), the index replaces labels for that run from the manifest.

### Schema versioning

Artifacts may include an envelope:

- `schema_version` (integer)
- `schema_id` (string, e.g. `akc:manifest:v1`)

These fields are **optional** for backward compatibility, but when present they must be valid for the schema version.

New control-plane sidecars introduced in v1 should include both fields on write paths. Evolution remains additive-only: viewers/operators must ignore unknown keys.

### Frozen schemas (v1)

Machine-checkable schemas live in `src/akc/artifacts/schemas.py`:

- **AKC trace export (NDJSON line)**: `src/akc/control/schemas/akc_trace_export.v1.schema.json` (not an artifact envelope; one object per line in `.otel.jsonl`)
- **AKC metric export (NDJSON line)**: `src/akc/control/schemas/akc_metric_export.v1.schema.json` (one object per line in `.otel_metrics.jsonl`; used for offline `operational_spec` metric signals and composite ratio predicates — see `docs/runtime-execution.md`)
- **Convergence certificate payload (runtime evidence item)**: `src/akc/artifacts/schemas/convergence_certificate.v1.schema.json` (versioned payload for `runtime_evidence_stream` rows where `evidence_type=convergence_certificate`)

- **manifest**: emitted `manifest.json`
- **plan_state**: serialized `PlanState` JSON objects
- **execution_stage**: `.akc/tests/*.json` stage evidence records
- **verifier_result**: `.akc/verification/*.json` verifier decisions/findings
- **operational_validity_report**: `.akc/verification/<run_id>.operational_validity_report.json` attestation of operational success-criterion predicates (machine schema: `src/akc/artifacts/schemas.py`; mirrored JSON Schema: `src/akc/artifacts/schemas/operational_validity_report.v1.schema.json`)
- **operational_assurance_result**: `.akc/verification/<run_id>.operational_assurance.json` coupled operational verifier outcome (`advisory`/`blocking`) plus telemetry-provider result rows for `otel_query_stub` bindings
- **operational_evidence_window**: operator-authored rollup under `.akc/verification/*.json` (recommended naming: `<window_id>.operational_evidence_window.json`) listing `window_start_ms` / `window_end_ms` and `runtime_evidence_exports[]` of `{path, sha256}` pointers to exported `runtime_evidence_stream` JSON arrays (paths relative to the tenant/repo outputs root). Used when intent `operational_spec.params.window` is **`rolling_ms`** together with `rolling_window_ms` and `evidence_rollup_rel_path` (see `docs/runtime-execution.md`). Schema: `src/akc/artifacts/schemas/operational_evidence_window.v1.schema.json` (also in `schemas.py`).
- **runtime_bundle**: `.akc/runtime/*.runtime_bundle.json` compile-to-runtime handoff contract (current default envelope is **v4**; v1–v3 remain accepted for older bundles)
- **runtime_evidence_stream**: `.akc/runtime/<run_id>/<runtime_run_id>/runtime_evidence.json` runtime control-plane evidence stream
- **run_trace_spans**: `.akc/run/*.spans.json` compile/run trace sidecar
- **run_cost_attribution**: `.akc/run/*.costs.json` immutable per-run cost rollup
- **replay_decisions**: `.akc/run/*.replay_decisions.json` per-pass replay/model/tool decisions
- **recompile_triggers**: `.akc/run/*.recompile_triggers.json` and `.akc/living/*.triggers.json` trigger snapshots
- **living_drift_report**: `.akc/living/*.drift.json` structured drift findings
- **control_plane_envelope**: `RunManifest.control_plane` committed keys (`stable_intent_sha256`, `policy_decisions`, `runtime_evidence_ref`, `policy_decisions_ref`, `replay_decisions_ref`, `recompile_triggers_ref`, runtime replay hints)
- Additional optional refs include `operational_assurance_ref` and `governance_profile_ref` (tenant-scoped pointer+sha entries, additive-only).

### `manifest.json` stability rules

- Must remain a JSON object with required keys: `tenant_id`, `repo_id`, `name`, `artifacts`.
- Artifact entries must include: `path`, `media_type`, `sha256`, `size_bytes`.
- Additive fields are allowed (top-level and per-artifact) to avoid breaking older viewers.
- `metadata.artifact_passes` is stable when present and may include:
  - ordered pass names (`order`)
  - grouped pass names (`groups`)
  - per-artifact digest map (`output_hashes`)
  Viewers/auditors must treat this as optional metadata and ignore unknown keys.

### `.akc/tests/*.json` evidence stability rules

Each record represents a stage execution:

- Must include: `plan_id`, `step_id`, `command`.
- Should include: `stage`, `exit_code`, `duration_ms`, `stdout`, `stderr` (can be `null`).

Companion text artifacts may be emitted for viewer convenience, but JSON records are the stable source of truth for what executed.

### Artifact-pass output stability rules

The artifact-pass surface is now part of the viewer contract:

- **Path conventions are stable**:
  - run-scoped specs/stubs use `<run_id>` in filename
  - deployment hardening files use stable fixed paths under `.akc/deployment/`
  - generated GitHub workflow uses `akc_deploy_<run_id>.yml`
  - run control-plane sidecars live under `.akc/run/<run_id>.*.json`
  - living drift artifacts live under `.akc/living/<check_id>.*.json` and the accepted baseline remains `.akc/living/baseline.json`
- **JSON schema evolution is additive-only** for:
  - system design (`.akc/design/*.system_design.json`)
  - orchestration (`.akc/orchestration/*.orchestration.json`)
  - coordination (`.akc/agents/*.coordination.json`)
- **Tenant isolation fields are required** in JSON specs:
  - `tenant_id`
  - `repo_id`
- **Runtime bundle invariants are required**:
  - When present, `schema_version` / `schema_id` must agree (for example `schema_version: 4` with `schema_id: akc:runtime_bundle:v4`). Older bundles may still use v1–v3 identifiers.
  - `run_id`, `tenant_id`, and `repo_id` must be present
  - `intent_ref` may be present and, when emitted, must carry `intent_id`, `stable_intent_sha256`, `semantic_fingerprint`, and `goal_text_fingerprint`
  - `intent_policy_projection` may be present and, when emitted, must remain additive-only
  - `spec_hashes.orchestration_spec_sha256` and `spec_hashes.coordination_spec_sha256` must be 64-character lowercase hex SHA-256 digests of the **canonical coordination/orchestration JSON objects** (stable JSON serialization used by `stable_json_fingerprint` in code)
  - **`coordination_ref`** (recommended when a coordination spec exists): `{ "path": ".akc/agents/<run_id>.coordination.json", "fingerprint": "<sha256>" }`. The `fingerprint` must equal `spec_hashes.coordination_spec_sha256` and must match the hashed bytes of the file at `path` (compile-time consistency checks enforce ref ↔ `spec_hashes`; runtime load fails closed on mismatch)
  - **`coordination_spec`** (optional inline embed for air-gapped or debugging): when present, must be the same JSON object whose fingerprint is `spec_hashes.coordination_spec_sha256`
  - `system_ir_ref` and optional `embed_system_ir` follow the IR spine rules already used by the reconciler
  - `runtime_policy_envelope` must exist, remain additive-only, and represent the runtime baseline intersected with the intent-derived policy projection

See also [runtime-execution.md](runtime-execution.md) for how the runtime loads coordination, schedules steps, and writes audit evidence.

### Runtime evidence stability rules

The runtime layer adds tenant-scoped operational evidence under `.akc/runtime/`:

- `runtime_run.json` is the operator record for one runtime execution, including scope, mode, bundle path, checkpoint/events/evidence paths, and terminal status.
- `checkpoint.json` is the persisted kernel checkpoint and queue cursor for restart/replay.
- `events.json` is the ordered runtime event transcript used by `akc runtime events` and runtime replay.
- `queue_snapshot.json` is the persisted scheduler state for at-least-once restart behavior.
- `runtime_evidence.json` is the structured control-plane evidence stream (`action_decision`, `transition_application`, `retry_budget`, `reconcile_outcome`, `rollback_chain`, `terminal_health`).
- `policy_decisions.json` is the runtime policy decision log, scoped to the same tenant/repo/run/runtime-run root.
- `evidence/coordination_audit.jsonl` (under the same scoped runtime directory) is an append-only **coordination audit trail**: one JSON object per line for coordination-scoped action completions, including spec fingerprints, role/step identifiers, idempotency keys, and content hashes (see `CoordinationAuditRecord` in code). Linked from manifest metadata when the runtime CLI records evidence paths.

These runtime artifacts are additive and read-only from the viewer/operator perspective. Consumers must ignore unknown fields and must not assume execution authority from artifact presence alone.

On successful `akc runtime start`, the runtime layer updates the existing compile-time `RunManifest` at `.akc/run/<run_id>.manifest.json` only when that file already exists. That additive update preserves compile-time fields, including `stable_intent_sha256`, refreshes `runtime_bundle` and `runtime_event_transcript`, records the current `runtime_evidence`, and writes `control_plane.runtime_evidence_ref` plus `control_plane.policy_decisions_ref` as relative `.akc/...` pointers for the concrete runtime run.

If no compile-time `RunManifest` exists, runtime commands do not create one retroactively. In that case the runtime sidecars remain the source of truth, and `akc runtime replay` constructs an in-memory **synthetic** manifest with the same pointer/linkage shape for replay only. That synthetic manifest copies **`stable_intent_sha256`** from the on-disk **`intent_ref`** on the runtime bundle (when present) into both `RunManifest.stable_intent_sha256` and `control_plane.stable_intent_sha256`, alongside `control_plane.runtime_run_id` and evidence pointers, so replay and living drift correlation stay aligned with compile-time manifests even when `.akc/run/<run_id>.manifest.json` was never emitted.

### Run and living control-plane sidecars

- `.akc/run/<run_id>.spans.json` is the schema-versioned **manifest sidecar** for compile trace spans (`run_trace_spans` envelope).
- `.akc/run/<run_id>.otel.jsonl` is the **canonical NDJSON sink** for observability export (omitted at compile time when there are zero compile `TraceSpan` rows, since text artifacts must be non-empty): one **AKC trace export** object per line (versioned JSON Schema: `src/akc/control/schemas/akc_trace_export.v1.schema.json`). Compile emits an initial file from controller `TraceSpan` records; the local runtime **appends** additional lines for kernel `TraceSpan` rows and coordination audit spans mapped from `otel_trace_json_from_akc_event`. Use this path for log shippers and OTLP-adjacent pipelines—do not duplicate the same spans into another parallel export file. `akc runtime` also mirrors each appended line to optional sinks when set: `AKC_OTEL_EXPORT_STDOUT=1`, `AKC_OTEL_EXPORT_HTTP_URL`, `AKC_OTEL_EXPORT_FILE`, and optional `AKC_OTEL_EXPORT_HTTP_TIMEOUT_SEC` (see `otel_export_extra_callbacks_from_env` in `akc.control.otel_export`). Programmatic use of `StdoutOtelExportSink`, `HttpPostOtelExportSink`, `FileAppendOtelExportSink`, and `MultiOtelExportSink` remains available for custom hosts. Each record carries **`tenant_id`, `repo_id`, `run_id`**, and **`akc.stable_intent_sha256`** on both `resource.attributes` and `span.attributes` when known (aligned with deployment annotations in [akc-alignment.md](akc-alignment.md)).
- `.akc/run/<run_id>.otel_metrics.jsonl` is the optional **metric export** sidecar: one **AKC metric export** object per line (`src/akc/control/schemas/akc_metric_export.v1.schema.json`). Runtime operational validity evaluation reads it when present (tenant/repo outputs path-scoped). Operators may append lines from their own exporters as long as records satisfy the schema; there is no implicit OTLP/Prometheus translation in core AKC.
- `.akc/run/<run_id>.costs.json` is the schema-versioned source of truth for immutable per-run cost attribution.
- `.akc/run/<run_id>.replay_decisions.json` records evaluated per-pass replay decisions and rationale.
- `.akc/run/<run_id>.recompile_triggers.json` snapshots semantic invalidation triggers evaluated during compile/replay.
- `.akc/living/<check_id>.drift.json` records drift findings from `drift_report`.
- `.akc/living/<check_id>.triggers.json` records the trigger snapshot paired with a living drift check.

For all of the above, v1 evolution is additive-only. Validation may run in dev/CI write paths, but production readers/writers must stay tolerant of unknown fields.
- Tenant `manifest.json` may additionally expose a `metadata.living_artifacts` rollup with `order`, `groups`, `artifacts`, `output_hashes`, `latest_check_id`, and `source` for the most recent living drift emission. This metadata is additive-only and may be refreshed by `akc drift` or `safe_recompile`.
- **Hardened deployment invariants are required**:
  - Docker Compose services must include `read_only: true`, `no-new-privileges`, `cap_drop: [ALL]`, `tmpfs`, and non-root `user`
  - Kubernetes deployment manifests must satisfy restricted security context (`runAsNonRoot: true`, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, dropped capabilities)
  - Generated GitHub workflow must avoid `pull_request_target`, avoid prohibited `secrets.*` in PR contexts, and remain least-privilege for permissions

### CI evidence and tests for hardened outputs

The hardened artifact-pass outputs are covered by tests and CI policy checks:

- `tests/unit/test_artifact_passes.py` validates emitted deployment and workflow hardening rules.
- `tests/unit/test_compile_session_end_to_end_light.py` validates tenant-scoped emission paths and run-manifest pass metadata.
- `scripts/ci_policy_test.py` enforces workflow safety patterns over `.github/workflows/*.yml`.

### Security/correctness tie-in

The stable evidence artifacts are designed to answer:

- **What was executed**: `command`, `stdout`, `stderr`, `exit_code`, `duration_ms`.
- **What gates decided**: verifier result includes `passed`, `findings[]`, `policy`.
