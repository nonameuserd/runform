# Runtime execution: routing, adapters, checkpoints

This document covers local/native execution depth: how actions map to execution lanes, how to extend the runtime via adapters, and what `RuntimeCheckpoint.replay_token` means relative to adapter checkpoint APIs. It also documents **multi-agent coordination** handoff from the compile-time runtime bundle (schema v4) through deterministic scheduling and audit trails.

## Multi-agent coordination (runtime bundle)

When a runtime bundle includes coordination (via **`coordination_ref`** and/or optional inline **`coordination_spec`**), the kernel loads the coordination JSON, validates fingerprints against **`spec_hashes.coordination_spec_sha256`**, and may enqueue **`coordination.step`** actions before normal workflow scheduling. Implementation: `akc.runtime.coordination.load`, `akc.coordination.models` (graph/schedule types; `akc.runtime.coordination.models` re-exports the same symbols for kernel imports), `akc.runtime.coordination.worker` (agent turns for `coordination.step`), `akc.runtime.kernel`.

### Fingerprints and fail-closed loading

- **`spec_hashes.coordination_spec_sha256`** is the SHA-256 (hex) of the coordination JSON object using the project’s **stable JSON fingerprint** (sorted keys, compact separators), same as compile emission.
- If **`coordination_ref`** is used, the file at `path` (resolved relative to the repo root for `.akc/...` paths) is read and must match both `spec_hashes.coordination_spec_sha256` and **`coordination_ref.fingerprint`** (the ref must describe the same bytes as on disk).
- Optional **`coordination_spec`** embeds the object directly (air-gapped/debug); it must still agree with `spec_hashes` when consistency checks run at compile time.

### Scheduler behavior and spec versions

Normative rules for **v1 vs v2**, **lowering** of reserved edge kinds, **determinism**, and **parse/schedule errors** are in [coordination-semantics.md](coordination-semantics.md).

- **v1 (default):** layers are built from **`depends_on`** edges only. Reserved kinds **`parallel`**, **`barrier`**, **`delegate`**, **`handoff`** raise **`CoordinationReservedEdgeRequiresSpecV2`** (a **`CoordinationUnsupportedEdgeKind`** subtype).
- **v2:** set **`spec_version: 2`** and/or **`coordination_spec_version: 2`** (they must agree if both are set). Reserved kinds are **lowered** to the same precedence as **`depends_on`** for scheduling, so a v2 graph can match an equivalent explicit **`depends_on`** graph from v1.
- Within each layer, **step ids are sorted lexicographically**. Coordination steps map to IR / orchestration **step ids** for policy context and routing; **`governance.role_profiles`** feed **`policy_context`** (see `akc.runtime.coordination.isolation`).

### Coordination execution contract (parallel dispatch)

Runtime bundle v4 may include `coordination_execution_contract`:

- `parallel_dispatch_enabled` (bool)
- `max_in_flight_steps` (int, >= 1)
- `max_in_flight_per_role` (int, >= 1)
- `completion_fold_order` (`"coordination_step_id"`)

When enabled, the kernel runs `coordination.step` actions in a bounded worker pool and then folds outcomes in deterministic `coordination_step_id` order before checkpoint/event updates. Non-coordination actions remain sequential.

Compile emit now defaults this contract to enabled with conservative caps (`4` steps, `2` per role), while runtime CLI can override per run:

- `akc runtime start --coordination-parallel-dispatch {inherit|enabled|disabled}`
- `--coordination-max-in-flight-steps N`
- `--coordination-max-in-flight-per-role N`

### Audit path and events

- **Append-only audit file**: `<outputs_root>/<tenant>/<repo>/.akc/runtime/<run_id>/<runtime_run_id>/evidence/coordination_audit.jsonl` — one JSON object per line for relevant coordination action outcomes (`CoordinationAuditRecord`).
- **Runtime events** may include optional coordination telemetry keys on applicable payloads (for example spec fingerprints, `role_id`, `graph_step_id`, parent event linkage); see `akc.runtime.coordination.audit` for the canonical key set merged into `RuntimeEvent.payload`.

Manifest emission can record a relative pointer to this evidence path when the runtime CLI updates compile-time manifests (see `artifact-contracts.md`).

## Action routing

Module: `src/akc/runtime/action_routing.py`

Each `RuntimeAction` resolves to one of:

| Route | Behavior |
|-------|----------|
| `delegate_adapter` | Default. Executes the configured adapter’s `execute_action` / `execute_action_with_graph_node` (kernel prefers the graph-node path when present). With **`NativeRuntimeAdapter`**, ordinary actions succeed immediately with stub outputs; **`coordination.step`** is **not** a stub—it runs the agent-worker pipeline (`coordination_step_runtime_result`, see `akc.runtime.adapters.native`). |
| `noop` | Succeeds without calling the delegate; outputs include `route: noop`. |
| `subprocess` | Opt-in. Runs `argv` under a tenant-scoped working directory (see below), with timeout and scrubbed env. Only honored when using `LocalDepthRuntimeAdapter`. |
| `http` | When using **`LocalDepthRuntimeAdapter`**, performs **policy-bounded** HTTP via `akc.runtime.http_execute` (allowlists, method/body/response caps). Otherwise not implemented on the active adapter. |

**Precedence**

1. Bundle metadata `runtime_action_routes` map: `action_type` → route name.
2. IR node `properties.runtime_execution` (overrides contract acceptance).
3. `OperationalContract.acceptance.runtime_execution` for that node’s contract.
4. Otherwise `delegate_adapter`.

### Full layer replacement mode

When runtime bundle metadata sets `layer_replacement_mode: "full"` (or `workflow_execution_contract.full_layer_replacement: true`), workflow actions default to `subprocess` route instead of `delegate_adapter`.

To keep this deterministic and non-stub:

- Set `workflow_execution_contract.default_subprocess` with `argv` and optional `timeout_ms`.
- Keep `runtime_execution.allow_subprocess` (or envelope `runtime_allow_subprocess`) enabled.
- Keep `runtime_execution.subprocess_allowlist` non-empty.

For mutating deployment providers in this mode, live apply requires deterministic rollback snapshots:

- `deployment_provider.rollback_apply_manifest_by_desired_hash` must be present and non-empty, or runtime start fails closed before reconcile.

**Subprocess configuration** (node properties or contract acceptance):

```json
"runtime_execution": {
  "route": "subprocess",
  "subprocess": {
    "argv": ["python3", "-c", "print(1)"],
    "timeout_ms": 5000
  }
}
```

**Gates** (defense in depth; align with [oss-security-requirements.md](oss-security-requirements.md) section 4 — allowlisted commands, scrubbed environment, confined filesystem):

1. `runtime_execution.allow_subprocess: true` in bundle metadata **or** `runtime_allow_subprocess: true` in `runtime_policy_envelope`.
2. Non-empty `runtime_execution.subprocess_allowlist`: basenames of permitted `argv[0]` (fail closed if missing).
3. Runtime policy must allow `runtime.action.execute.subprocess` (included in the default allow set; intent/IR projections may narrow it).
4. Working directory defaults to `outputs_root / <tenant> / <repo> / .akc/runtime / <run_id> / <runtime_run_id> /` (same scope as `FileSystemRuntimeStateStore`); when the action carries coordination **`policy_context`**, `LocalDepthRuntimeAdapter` may use `subprocess_cwd_for_runtime_action` to place work under `.../roles/<role_id>/` (and optional scratch subdirs per `coordination_filesystem_scope`).
5. Subprocess environment is minimal (`PATH` only by default).

`IOContract.output_keys` for nodes that use subprocess should include the fields the adapter emits: `action_id`, `action_type`, `adapter_id`, `exit_code`, `stdout`, `stderr` (strings; `exit_code` is a JSON number).

## Extension points: registry and `create_hybrid_runtime`

- **`RuntimeAdapterRegistry`** (`src/akc/runtime/adapters/registry.py`): register `adapter_id` → factory. **`register_default_runtime_adapters`** registers only `native`. Adapters that need constructor arguments (for example `LocalDepthRuntimeAdapter(outputs_root=...)`) should be registered with a closure factory at integration sites.
- **`create_hybrid_runtime`** (`src/akc/runtime/init.py`): supported way to plug a custom primary adapter. The kernel uses `HybridRuntimeAdapter`, which forwards **`execute_action`** to the primary only (no native fallback for execution). For **`execute_action_with_graph_node`**, it calls the primary’s method when implemented; otherwise it uses **`execute_action`**. It falls back to `NativeRuntimeAdapter` for `wait_signal`, `checkpoint`, `restore`, and `cancel` when the primary does not advertise the corresponding capability. **`HybridRuntimeAdapter.capabilities()` reports the primary only**; the fallback does not upgrade advertised durability.
- **`create_local_depth_runtime`**: kernel wired with `LocalDepthRuntimeAdapter` for routing + opt-in subprocess (still uses in-memory scheduler/state unless you replace those).

### Adapter capabilities: honored vs advertised

| Capability | `NativeRuntimeAdapter` (default) | `NativeRuntimeAdapter(honest_capabilities=False)` | `LocalDepthRuntimeAdapter` | Notes |
|------------|----------------------------------|---------------------------------------------------|----------------------------|-------|
| `execute_action` | Stub success for generic actions; **`coordination.step`** via agent worker (`agent_worker_from_env` / injected worker) | Same | Routing + optional subprocess + same native delegate for coordination | User-visible behavior depends on action type and routing. |
| `durable_waits` / `external_signals` / `compensation_hooks` / `external_checkpointing` | Advertised **`False`** | All **`True`** (legacy stub) | All **`False`** (delegate may differ) | Default native is **honest**: no durable external wait, real signal I/O, compensation, or process checkpoint restore. Use `honest_capabilities=False` only for tests that relied on the old broad advertisement. |
| `checkpoint` / `restore` | Symbolic token `{runtime_run_id}:native`, no-op restore | Same | Delegates to inner adapter (default native) | Not workload-durable (see below). |

**Hybrid:** `HybridRuntimeAdapter.capabilities()` returns **only** the primary adapter’s flags. The native fallback may still run for `wait_signal` / `checkpoint` / `restore` / `cancel` when the primary lacks a capability, but operators and telemetry must not infer production-grade durability from the fallback path.

## Checkpoints and `replay_token`

**`RuntimeCheckpoint` (filesystem / in-memory store)**

- Persisted by `FileSystemRuntimeStateStore` as `checkpoint.json`: `cursor` (event stream position), `pending_queue`, `node_states`, optional `replay_token`. `save_checkpoint`, `save_queue_snapshot`, `append_event` (`events.json`), and `append_trace_span` (`runtime_trace_spans.json`) write via a same-directory temp file plus `os.replace` so crashes mid-write are less likely to leave torn JSON at the final path (best-effort atomicity on POSIX).
- **`recover_or_init_checkpoint` ordering** (`RuntimeKernel`):
  1. Load **`queue_snapshot.json`** if present and **`restore_snapshot`** into the scheduler.
  2. Load **`checkpoint.json`** if present.
  3. If a checkpoint exists and **no** queue snapshot file existed in step 1, re-enqueue every action in `checkpoint.pending_queue`, then persist a fresh queue snapshot.
  4. **If a checkpoint exists:** run **`_sync_coordination_layer_enqueue`**: when coordination metadata in `checkpoint.node_states["__coordination__"]` indicates a plan that is only **partially** enqueued (`plan_enqueued` and a non-null `coordination_next_layer_to_enqueue`), enqueue the next coordination layer(s) whose dependencies are satisfied (advances `coordination_next_layer_to_enqueue`, updates `pending_queue` from the scheduler). Then **`save_checkpoint`** and **`_persist_queue_snapshot`**, and **return** (no cold init).
  5. **If no checkpoint exists:** build the runtime graph if needed, build/initialize checkpoint (including initial coordination plan enqueue via **`_enqueue_coordination_plan_if_needed`**), **`save_checkpoint`**, then **`_persist_queue_snapshot`**.
- **Implications**
  - If **`queue_snapshot.json` is present**, its queued / in-flight / dead-letter state is **authoritative** for the scheduler; `checkpoint.pending_queue` is **not** merged in on that path (avoid assuming both files always agree).
  - **Crash between `save_checkpoint` and `save_queue_snapshot` on first init:** on restart, the queue snapshot is absent, so step 3 rehydrates the scheduler from `checkpoint.pending_queue` and writes a new snapshot—work is not silently dropped.
  - **Crash after queue snapshot but before checkpoint update** (later in the loop): recovery behavior depends on which files exist; operators should treat **pairing** of checkpoint + queue snapshot as the intended steady state after successful `_persist_queue_snapshot` calls in the kernel.

**`replay_token` vs adapter checkpoint**

- **`replay_token`**: kernel-level idempotency marker. After a successful action, the checkpoint’s `replay_token` is set to that action’s `idempotency_key`. If the same action is dispatched again with the same key while `replay_token` still matches, the kernel emits `runtime.action.replayed` and **does not** call `execute_action` again (see `RuntimeKernel.run_action`).
- **`NativeRuntimeAdapter.checkpoint` / `restore`**: symbolic token and no-op restore only; they do **not** capture OS process state or subprocess handles. Do not rely on them for durable workload checkpointing.
- **Cross-layer story**: durable progress for the control plane is `checkpoint.json` + queue snapshots + events; adapter tokens are not a source of truth for replay.
- **SQLite option:** :class:`~akc.runtime.state_store.SqliteRuntimeStateStore` implements the same :class:`~akc.runtime.state_store.RuntimeStateStore` protocol under the same scope directory, using `runtime_state.sqlite3` (WAL) so events and trace spans append as rows instead of rewriting JSON arrays. It does not write run-level OTEL JSONL or coordination audit files (the kernel skips those when the methods are absent).

### Operator-facing guarantees (alignment with §4)

- **Real (artifact / kernel):** `replay_token` idempotency for re-dispatched actions, filesystem or in-memory `RuntimeCheckpoint` + queue snapshots + event streams as implemented by `RuntimeStateStore`, and audited coordination evidence where enabled.
- **Symbolic / stub:** `NativeRuntimeAdapter.checkpoint` / `restore` tokens, and any hybrid **fallback** invocation of native for waits or cancellation when the primary does not advertise the capability—these do not add external durability.
- **Hybrid:** Telemetry and `adapter.capabilities()` reflect the **primary** adapter only, so a `LocalDepthRuntimeAdapter` (or similar) primary does not appear “fully durable” merely because native fallback stubs return a checkpoint string.

## Reconciliation convergence (operational semantics)

Module: `src/akc/runtime/reconciler.py` (`DeploymentReconciler._evaluate_convergence`)

For each **resource** in the reconcile plan, **converged** means all of the following:

1. **Hash match:** observed provider state matches the **desired hash** for that resource (or the resource is correctly **absent** when desired is delete/absent).
2. **Health gate** (bundle metadata `reconcile_health_gate`):
   - **`permissive`** (default): aggregate `health_status` is `healthy` or `unknown` (stub-friendly).
   - **`strict`**: requires `healthy`, **or** an observed condition with `type` **Ready** and `status: true` (Kubernetes-style), **or** `unknown` while cumulative resync wait is still below `reconcile_health_unknown_grace_ms` (milliseconds). Otherwise the gate fails even when the hash matches; `last_error` explains strict readiness vs unknown.
3. **Bounded resync:** the runtime CLI may run multiple reconcile passes when `reconcile_resync_iterations` / `reconcile_resync_interval_ms` are set on the bundle. Each pass records **retry/backoff metadata** on evidence (`resync_attempt`, `resync_max_attempts`, `resync_interval_ms` on `reconcile_outcome`, `rollback_chain`, and per-resource `terminal_health`; plus rollup fields on `reconcile_resource_status`). Elapsed wait is the **cumulative scheduled sleep** between attempts (fixed interval, or exponential backoff when enabled — see below) and feeds strict unknown-grace evaluation via `resync_elapsed_wait_ms`.

**Exponential backoff + jitter (optional):** set `reconcile_resync_exponential_backoff: true` on the bundle. Optional metadata: `reconcile_resync_base_interval_ms` (defaults to `reconcile_resync_interval_ms` when that is greater than 0, else `1000`), `reconcile_resync_max_interval_ms`, `reconcile_resync_jitter_ratio` (0–1, default `0.2`), and `reconcile_resync_jitter_seed` (integer). When `reconcile_resync_jitter_seed` is set, delays are **deterministic** for tests and replays; when omitted, jitter is derived from a hash of `runtime_run_id` and the attempt index so one run is stable across attempts but different runs are decorrelated.

**Convergence certificate (v1):** for each reconcile pass that produced `reconcile_resource_status` rows, the CLI also appends `convergence_certificate` evidence (and an aggregate row with `resource_id` `__runtime_aggregate__`, `aggregate: true`). Payload shape is versioned (`certificate_schema_version: 1`) with `desired_hash`, `observed_hash`, `health`, `attempts` (completed resync attempts), `window_ms` (cumulative scheduled wait between attempts in that CLI reconcile loop), `provider_id` (from `deployment_provider.kind` or `in_memory`), `policy_mode`, and `converged`. The JSON Schema file `src/akc/artifacts/schemas/convergence_certificate.v1.schema.json` documents the payload; `operational_validity_report` and intent `operational_spec` can require `convergence_certificate` via `expected_evidence_types` and use `max_resync_attempts_bound` / `reject_failed_aggregate_terminal_health` in params (see `akc.intent.operational_eval`).

**CLI exit on divergence (opt-in):** when `runtime_nonzero_exit_on_reconcile_divergence: true` is set on the bundle, `akc runtime start` / `akc runtime reconcile` exit with code **3** if any resource is not converged or aggregate health (reconcile + kernel terminal status, same worst-of as `terminal_health`) is `failed`. Default remains **0** when the flag is absent so existing bundles stay compatible.

**Bundle JSON → metadata:** `RuntimeKernel.load_bundle` copies top-level reconcile/deployment keys (including `deployment_provider`, `runtime_nonzero_exit_on_reconcile_divergence`, `reconcile_resync_*`, `reconcile_health_gate`, `reconcile_health_unknown_grace_ms`) from the bundle file into `RuntimeBundle.metadata` so the CLI and reconciler observe the same fields as in-memory bundle fixtures.

**Observed health conditions:** providers attach `ObservedResource.health_conditions` (and JSON under `payload.health_conditions`) — rows with `type`, `status`, optional `reason`, `message`, `last_transition_time` — for artifact replay. Docker Compose and Kubernetes observers document their mapping in `src/akc/runtime/providers/compose.py` and `kubernetes.py`. Optional **`deployment_provider.observe_probes`** adds read-only **HTTP** / **TCP** checks (`src/akc/runtime/observe_probes.py`); failing probes downgrade aggregate health where applicable.

Structured evidence also includes a final **`terminal_health`** row with `resource_id` `__runtime_aggregate__` and `aggregate: true`, combining reconcile resource health with **kernel loop terminal status** (`kernel_terminal_status` / `runtime_status`) using worst-of ordering (`failed` → `degraded` → `unknown` → `healthy`). Replay and `akc runtime replay` resolve **terminal health** from that aggregate row when present.

**Living feedback:** `src/akc/runtime/living_bridge.py` maps kernel terminal events (`runtime.kernel.loop_finished` with `max_iterations_exceeded`), non-successful `runtime.action.completed`, `runtime.action.dead_lettered`, `runtime.adapter.fallback`, adapter `outputs.health_status` degradation, legacy `runtime.reconcile.failed` / `runtime.action.failed`, and optional `runtime.reconcile.resource_status` (hash matched but not converged — e.g. strict health gate) to `RuntimeHealthSignal`.

**Living automation profile (Phase E):** one named profile wires recompile trigger defaults, optional time-compression baselines on compile manifests (`baseline_duration_hours` + `compression_factor_vs_baseline`), and whether `akc runtime autopilot` passes **bridge-gated** runtime transcript events into `safe_recompile_on_drift`. Resolution order: CLI (`--living-automation-profile`) → env `AKC_LIVING_AUTOMATION_PROFILE` → `.akc/project.json` `living_automation_profile`.

| Profile | Autopilot runtime→recompile | `living_loop_v1` manifest defaults |
|---------|----------------------------|------------------------------------|
| `off` (default) | No — ingest drift checks still run | No extra baseline metadata |
| `living_loop_v1` | Yes — only when `DefaultLivingRuntimeBridge` maps an event to a health signal | Default 8h baseline for compression factor; granular acceptance triggers unless the eval suite’s `living_recompile_policy` overrides |

Implementation: `src/akc/living/automation_profile.py`, `src/akc/living/runtime_bridge.py`, `src/akc/run/recompile_triggers.py`, `src/akc/run/time_compression.py`.

### Autopilot deployment (single-writer)

`akc runtime autopilot` is a **single-writer per tenant/repo scope** at the mutating boundary: it acquires a lease before doing work (`src/akc/runtime/autopilot.py`).

| Lease backend | Coordination |
|---------------|----------------|
| `filesystem` (default) | `fcntl` lock + JSON state under `<outputs_root>/<tenant>/<repo>/.akc/autopilot/leases/`. Safe across processes on **one machine** (or multiple processes sharing a mount where flock semantics coordinate). |
| `k8s` | `coordination.k8s.io/v1` **Lease** in `--lease-namespace`, via `kubectl` in the pod. Use for leadership across nodes when outputs are not on shared POSIX storage. |

**Operational rules:**

- Prefer **one replica** per autopilot deployment unless you use `k8s` leases with a distinct namespace and RBAC, or you know flock covers your storage layout.
- Run `akc living doctor` with `--lease-backend`, `--lease-namespace`, and `--expect-replicas` (or `AKC_AUTOPILOT_LEASE_BACKEND`, `AKC_AUTOPILOT_LEASE_NAMESPACE`, `AKC_AUTOPILOT_EXPECT_REPLICAS`) to surface warnings when profile is `living_loop_unattended_v1`. The same three flags work with `akc verify --living-unattended` (CLI overrides env for those checks).

Reference manifests: `deploy/README.md` (systemd unit, Docker Compose, Kubernetes Deployment, optional `Dockerfile.autopilot`).

**Health and restarts:** treat **process liveness** as the primary signal (systemd `Restart=`, Docker `restart:`, Kubernetes `livenessProbe`). Optional log or metrics scraping can watch `journalctl`, container logs, or future OTEL lines; reliability KPIs live under `.akc/autopilot/scoreboards/` (below).

## Reliability SLO gate (autopilot scoreboards)

For Plan 3 staging/soak acceptance, AKC can gate on `reliability_scoreboard` artifacts emitted by the autopilot loop:

- Scoreboards are written under `<outputs_root>/<tenant>/<repo>/.akc/autopilot/scoreboards/`.
- File pattern: `*.reliability_scoreboard.v1.json`.
- Each artifact includes window bounds (`window_start_ms`, `window_end_ms`) and KPI fields such as:
  - `policy_compliance_rate`
  - `rollbacks_total`
  - `convergence_latency_ms_avg`
  - `mttr_like_repair_latency_ms_avg`
  - `failed_promotions_prevented`

The repository includes:

- Target config: `configs/slo/reliability_scoreboard_targets.json`
- Checker: `scripts/check_reliability_slo_gate.py`

The checker enforces KPI comparators (`gte` / `lte`) for the last `required_consecutive_windows` per scope (default `2`), matching the "two consecutive weeks/windows" acceptance rule.

## Time Compression Evidence

AKC tracks lifecycle timestamps and derived duration metrics as evidence artifacts rather than narrative claims.

- Canonical lifecycle timestamps (manifest control plane):
  - `intent_received_at`
  - `compile_started_at`
  - `compile_completed_at`
  - `runtime_healthy_at`
- Canonical duration metrics (derived in shared runtime/eval utility):
  - `intent_to_healthy_runtime_ms` (primary)
  - `compile_to_healthy_runtime_ms` (secondary)
  - `compression_factor_vs_baseline` (`baseline_duration_hours / observed_hours`)

`compression_factor_vs_baseline` is a claim metric, not a correctness metric. It should only be interpreted together with reliability evidence (pass rate, rollback and convergence KPIs, and consecutive-window SLO gates) to avoid gaming a single speed number.

### Example: staging soak gate

```bash
uv run python scripts/check_reliability_slo_gate.py \
  --outputs-root /var/lib/akc/staging-soak-outputs \
  --targets-path configs/slo/reliability_scoreboard_targets.json \
  --format json
```

Optional scope filtering:

```bash
uv run python scripts/check_reliability_slo_gate.py \
  --outputs-root /var/lib/akc/staging-soak-outputs \
  --tenant-id tenant-a \
  --repo-id repo-a
```

### Suggested KPI target profiles

Use one targets file per environment (or generate from templates) so the same checker runs with stricter policy as you move from CI to prod.

- **CI (fast signal, low blast radius)**
  - `required_consecutive_windows`: `2`
  - `policy_compliance_rate.gte`: `0.90`
  - `rollbacks_total.lte`: `1`
  - `convergence_latency_ms_avg.lte`: `120000`
  - `mttr_like_repair_latency_ms_avg.lte`: `120000`
- **Staging soak (pre-prod acceptance gate)**
  - `required_consecutive_windows`: `2`
  - `policy_compliance_rate.gte`: `0.95`
  - `rollbacks_total.lte`: `0`
  - `convergence_latency_ms_avg.lte`: `60000`
  - `mttr_like_repair_latency_ms_avg.lte`: `60000`
- **Prod (strictest)**
  - `required_consecutive_windows`: `2` (or higher, based on release cadence)
  - `policy_compliance_rate.gte`: `0.99`
  - `rollbacks_total.lte`: `0`
  - `convergence_latency_ms_avg.lte`: `30000`
  - `mttr_like_repair_latency_ms_avg.lte`: `30000`

## Operational validity: `rolling_ms` and cross-run evidence rollups

Intent success criteria with `evaluation_mode: operational_spec` may set `params.window` to **`rolling_ms`** (with `rolling_window_ms` and **`evidence_rollup_rel_path`**, a path under `.akc/verification/` relative to the tenant/repo outputs root). That mode does **not** ask the runtime CLI to observe production for an arbitrary calendar window by itself; operators supply a versioned **`operational_evidence_window.v1`** rollup JSON (for example `.akc/verification/window-7d.operational_evidence_window.json`) that declares a time span and hashed pointers to one or more on-disk **`runtime_evidence_stream`** exports (same array shape as per-run `runtime_evidence.json`).

When the runtime CLI emits `operational_validity_report` for `post_runtime` criteria in **`rolling_ms`**, it loads the rollup from `evidence_rollup_rel_path`, validates the rollup envelope shape (`operational_evidence_window.v1`), verifies each export’s **SHA-256** against the rollup, merges records into one logical sequence, and evaluates predicates offline (`akc.intent.operational_eval`). Checks include: **(1)** the rollup’s declared span `(window_end_ms - window_start_ms)` must be less than or equal to intent `rolling_window_ms`, and **(2)** every merged evidence row’s `timestamp` must fall between `window_start_ms` and `window_end_ms` (inclusive). OTel NDJSON sidecars for the **current** runtime run are still merged only for that run’s attestation pass (v1); multi-export OTel rollups are not in scope here.

Compile-time acceptance (`evaluation_phase: compile`) rejects **`rolling_ms`**; use **`single_run`** with `operational_compile_bundle` for compile-time operational checks, or defer to `post_runtime` with a rollup.

### Operational SLI-shaped predicates over exported telemetry (offline)

AKC borrows **SLO vocabulary** (target, comparator, good vs bad events, error budget as a ratio) but evaluates only **artifact-local exported bundles**, not a hosted time-series database or PromQL. Intent `operational_spec` may include:

- **`otel_metric_signals`**: match **AKC metric export** NDJSON points by `metric.name` plus optional string attribute equality; `predicate_kind` **`presence`** requires at least one matching point, **`threshold`** compares the **sum** of numeric `metric.as_double` / `metric.as_int` across matches to the global `threshold` / `threshold_comparator`.
- **`composite_predicates`**: bounded checks such as **`span_status_fraction`** (fraction of trace-export spans whose `span.status` equals `status_good_value`, with mandatory `max_spans`) and **`metric_counter_ratio`** (ratio of summed counter values for two named metrics, with mandatory `max_metric_points` and `max_metric_series` cardinality caps). Oversized NDJSON sidecars fail closed via parser line/point caps (`parse_otel_metric_ndjson_slice` in `akc.intent.operational_eval`).
- **`expected_evidence_types`**: the reserved type **`akc_otel_metrics_export`** means “non-empty `{run_id}.otel_metrics.jsonl` for this run” (path-scoped like `.otel.jsonl`).

`otel_query_stub` bindings are resolved from optional tenant-scoped operator config at
`.akc/control/telemetry_bindings.json` during operational coupling verification. Intent remains
secret-free; provider details stay outside intent JSON.

Schema for one metric line: `src/akc/control/schemas/akc_metric_export.v1.schema.json`. Golden fixtures: `tests/fixtures/operational_eval/`. **PromQL, ad-hoc query strings, and live TSDB pulls remain out of scope** for core eval; bindings belong in operator config outside intent JSON.

## Related code

- `src/akc/runtime/kernel.py` — graph, dispatch, coordination plan enqueue, `_sync_coordination_layer_enqueue`, `replay_token` handling, optional `runtime.action.execute.subprocess` / `runtime.action.execute.http` authorization, `runtime.kernel.loop_finished` after each terminal loop outcome.
- `src/akc/runtime/coordination/` — load and fingerprint checks, isolation projection, audit record shaping, agent worker helpers (`worker.py`).
- `src/akc/runtime/coordination/models.py` — re-exports `akc.coordination.models` (shared `CoordinationScheduler` and graph types).
- `src/akc/coordination/` — shared coordination graph parsing and scheduling (compile + runtime + generated protocol stubs).
- `src/akc/runtime/adapters/base.py` — `HybridRuntimeAdapter` (primary-only execution, capability-gated fallbacks).
- `src/akc/runtime/adapters/local_depth.py` — routed execution.
- `src/akc/runtime/adapters/native.py` — stub workflow actions + `coordination.step` agent-worker execution.
- `src/akc/runtime/policy.py` — `RUNTIME_POLICY_ACTIONS` includes `runtime.action.execute.subprocess` and `runtime.action.execute.http`.
- `src/akc/cli/runtime.py` — evidence projection, aggregate terminal health, synthetic replay manifest linkage (see `docs/artifact-contracts.md`).

## Fleet-plane trust contract (cross-shard automation)

Runtime execution remains outside the fleet plane. Cross-shard automation in `akc.control.automation_coordinator` is constrained to control-plane classes only:

- metadata/tag writes in tenant-scoped operations index rows
- incident workflow orchestration using existing read-only playbook/export surfaces
- webhook signaling

Fleet automation must not call compile/runtime execution paths. Specifically prohibited:

- triggering compile loop passes or any generated patch/tool execution
- invoking runtime adapters, reconcile apply, or deployment mutation APIs
- bypassing tenant-scoped artifact boundaries with service-memory-only state

Exactly-once-ish boundaries are implemented with durable action checkpoints keyed by `(shard, tenant, repo, run, action, policy_version)`, bounded retries with backoff, and dead-letter artifacts written under tenant control paths.
