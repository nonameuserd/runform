# Viewer Trust Boundary Contract

## Summary

This document defines the trust boundary for any “viewer” that presents AKC plan state and evidence artifacts: the **filesystem-backed viewer** invoked by `akc view` (`src/akc/viewer/`, static HTML, TUI, export bundle), and the same contract applies to **any UI that only reads local scope data** the same way.

The viewer is a read-only consumer. It must never take over execution, must never run tools or commands implied by artifact contents, and must not access secrets or tool credentials.

The **fleet operator dashboard** (`akc fleet dashboard-serve`, `akc.control.operator_dashboard`) is a separate surface: it uses **GET-only** HTTP against `akc fleet serve` for merged index rows and paths. It still must not execute compile/runtime or run artifact-derived commands; operators use local trees and `akc view export` for bytes. See [Fleet control-plane HTTP](#fleet-control-plane-http-write-plane-narrow) below.

## Threat model (what the viewer must assume)

For the viewer, the following are treated as **untrusted input** (display or copy only):

- **Plan state** — JSON under `<plan_base_dir>/.akc/plan/<tenant>/<repo>/*.json` (active pointer + plan bodies), or the active plan in scoped **`memory.sqlite`** when the JSON store is absent (see `load_viewer_snapshot`).
- **`manifest.json`** at `<outputs_root>/<tenant>/<repo>/manifest.json` and every path listed in `artifacts[]` (tests, verification, design, orchestration, coordination, deployment, generated workflows, control-plane refs, delivery artifacts, and any future relative paths).
- **Knowledge envelopes** — `.akc/knowledge/snapshot.json`, `.akc/knowledge/snapshot.fingerprint.json`, `.akc/knowledge/mediation.json` when present under the scoped outputs directory.
- **Code-memory conflict reports** — records read from `.akc/memory.sqlite` (`conflict_report` items) for debugging displays.
- **Operator panel sources (summaries only in-bundle)** — e.g. `FORENSICS.json` under `.akc/viewer/forensics/*/` and playbook JSON under `<outputs_root>/<tenant>/.akc/control/playbooks/`; manifest-linked JSON such as `developer_profile_decisions.json` when resolved **strictly under** the scoped outputs directory.
- **Any patch text, stdout/stderr, or nested JSON** inside those files.

The viewer must treat all of the above as **data to render or download**, not as executable instructions.

## Allowed actions (in scope)

The viewer may:

- Read plan state from the JSON plan store **or** fall back to `SQLitePlanStateStore` in scoped `.akc/memory.sqlite` (`src/akc/viewer/snapshot.py`).
- Read `manifest.json` and evidence files under the tenant/repo **scoped outputs directory** (`<outputs_root>/<tenant>/<repo>/`), with path confinement enforced when copying (`src/akc/viewer/export.py`, `src/akc/viewer/web.py`).
- Validate loaded objects against frozen JSON Schemas **tolerantly**: violations are surfaced in synthetic fields (e.g. `metadata.viewer_schema_issues`, `last_feedback.viewer_schema_issues`) rather than failing closed (`src/akc/artifacts/validate.py`, `load_viewer_snapshot`).
- Display:
  - step status and progress from plan state;
  - links to evidence artifacts and human-readable stdout/stderr / structured findings;
  - optional knowledge, mediation, and conflict-report summaries;
  - read-only operator panel summaries (forensics, playbook, autopilot, profile / developer decisions) derived from JSON already on disk.
- Provide **download / open** UX for evidence files (copy into `files/**`, open in browser, zip export).

The viewer must remain **local-first** for artifact bytes:

- no hosted execution mode by default;
- no remote execution integration inside the OSS filesystem viewer.

### Local HTTP serve (`akc view … web --serve`) — out-of-band helper

`akc view … web --serve` is an **optional developer convenience**: after generating a static bundle, the CLI can start a **stdlib** HTTP server that serves **only** that bundle directory.

- **Not a product surface for remote access.** The server binds **`127.0.0.1` only** (no LAN/WAN exposure from this mode). Do not use it as a substitute for the fleet operator dashboard or any authenticated service.
- **Path confinement:** Requests are resolved under the bundle root; paths that escape the root (including via normalization) are not served.
- **Response headers:** Responses include `X-Content-Type-Options: nosniff` to reduce MIME-sniffing risk for local browsing.
- **Why it exists:** Some browsers restrict `fetch()` and related APIs on `file://` pages; serving the same files over `http://127.0.0.1:…` avoids that limitation when debugging the static viewer.

Trust boundary unchanged: the helper **only serves files already written** to the bundle; it does not widen read scope beyond that directory or execute artifact content.

## Prohibited actions (out of scope)

The viewer must not:

- execute generated code, apply patches to a working tree, or run tests;
- call into the compile/execute pipeline or invoke any executor or tooling that would change repository state;
- access secrets or tool credentials from the environment or from the host;
- follow attacker-controlled URLs from artifact content in ways that fetch or execute untrusted code (no “click-to-open external exploit” patterns);
- load dynamic code modules supplied by artifact content.

## Trust boundary statement (non-negotiable)

The compile loop and executor remain the only components that may **execute or validate** generated artifacts in the engineering sense.

The viewer is strictly a read-only renderer:

- viewer reads data;
- renderer displays evidence;
- the viewer never moves the trust boundary or expands capability beyond **read-only local inspection**.

If a future viewer needs more capability, treat that as a **security boundary change** and as an OSS contract update.

## Evidence in the current repo (what this contract matches)

| Concern | Location |
|--------|----------|
| Sandbox and tenant isolation (execution vs UI) | `docs/security.md`; `src/akc/execute/factory.py`, `dev.py`, `strong.py` |
| Plan persistence (JSON + SQLite backends) | `src/akc/memory/plan_state.py` (`JsonFilePlanStateStore`, `SQLitePlanStateStore`) |
| Viewer snapshot assembly | `src/akc/viewer/snapshot.py` (`load_viewer_snapshot`), `models.py` |
| Operator panel discovery (read-only summaries) | `src/akc/viewer/control_panels.py` |
| Static web / export bundles | `src/akc/viewer/web.py`, `export.py`, `tui.py`, `serve.py` (optional local HTTP helper for bundles) |
| CLI entrypoint | `src/akc/cli/view.py` (`akc view`; optional `--plan-base-dir` for `.akc/plan` root) |
| Output emission and `manifest.json` | `src/akc/outputs/emitters.py` (`JsonManifestEmitter`, under-root writes); compile path binds session + emitter in `src/akc/compile/session.py` |
| Schema kinds used by the viewer | `manifest`, `plan_state` via `validate_obj` in `src/akc/artifacts/validate.py`; contract narrative in `docs/artifact-contracts.md` |
| Tests | `tests/unit/test_viewer_export.py` and related viewer tests |

**Intent and replay metadata** in manifests (e.g. `stable_intent_sha256`, replay sidecars) are still **untrusted display input**. How they affect invalidation belongs to the compile/delivery contracts, not the viewer. See `docs/viewer.md` and `docs/akc-alignment.md` (*Intent authority and replay*).

## Fleet control-plane HTTP (write plane, narrow)

The fleet HTTP server (`akc fleet serve`, `akc.control.fleet_http`) stays a **trust-boundary** surface: it does **not** serve raw artifact trees or arbitrary files. Read routes expose merged rows from per-tenant `operations.sqlite` indexes only (runs, run detail, delivery sessions).

Additional read API (still index metadata only, no artifact bytes):

- `GET /v1/runs`, `GET /v1/runs/<tenant>/<repo>/<run_id>`
- `GET /v1/deliveries`, `GET /v1/deliveries/<tenant>/<repo>/<delivery_id>`

**Writes** are intentionally minimal and map 1:1 to safe control-plane mutations. For example, `POST /v1/runs/<tenant>/<repo>/<run_id>/labels` updates run labels via the same path as control-plane / `OperationsIndex` label upserts. Writes do **not** edit run manifests or trigger compile/runtime execution. Callers must send **`If-Match`** with the labels ETag; optional **`Idempotency-Key`** is supported for safe retries.

### Threat model (Bearer API)

- **CSRF:** Not applicable to typical Bearer-token API usage (no browser cookie session). Clients send `Authorization: Bearer …` explicitly.
- **Replay:** Tokens are static secrets; replay of a captured request is possible until rotation. `X-Request-ID` is for correlation/audit only.
- **Tenant isolation:** Every mutating request is checked against the token’s `tenant_allowlist` (anonymous read bypass does **not** apply to writes).
- **Capability scopes:** When tokens are required, authenticated **reads** need **`runs:read`**. **`POST …/labels`** requires **`runs:metadata:write`** or **`runs:label`** (`auth_has_scope` accepts `runs:label` as satisfying the metadata-write check for that route). See `src/akc/control/fleet_auth.py`.
- **Audit:** Accepted mutations append to `<outputs_root>/<tenant>/.akc/control/control_audit.jsonl` (`append_control_audit_event`).

For CORS when hosting the operator dashboard on another origin, set `AKC_FLEET_CORS_ALLOW_ORIGIN` to the dashboard origin so browsers may send `Authorization` on GET/POST (see module docstring in `fleet_http.py`).

## Cross-shard automation contract (bounded, non-execution)

The fleet automation coordinator (`src/akc/control/automation_coordinator.py`) is a **control-plane-only** worker over read-indexed run rows and tenant-scoped control artifacts. It is bounded (`max_candidates`, `max_actions`, retry caps) and replay-safe via durable dedupe/checkpoint metadata in each tenant `operations.sqlite`.

Allowed operation classes (see `ALLOWED_AUTOMATION_ACTIONS`):

- **`metadata_tag_write`** — label / metadata updates consistent with operations index mutations
- **`incident_workflow_orchestration`** — read-only playbook reporting plus incident artifact generation (no compile execution)
- **`webhook_signal`** — signed notifications to configured subscribers

Explicitly prohibited on the fleet automation plane:

- compile loop invocation (`akc compile`, pass execution, patch generation/application);
- runtime/deployment execution (`akc runtime start/reconcile`, adapter action execution);
- arbitrary tool or command execution derived from artifact content.

## Compatibility expectations

To keep viewer evolution safe:

- The viewer must be **schema-driven** against the artifact contract (`docs/artifact-contracts.md`); unknown additive fields should be ignored safely.
- The viewer must **never** depend on artifact content to decide whether to execute something.
- The viewer must render “verification failed” and schema-warning states **without** changing execution behavior.
- The viewer must tolerate additive manifest metadata (for example `metadata.artifact_passes` or `control_plane` blocks) without assuming fixed key sets.
