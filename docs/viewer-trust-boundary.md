# Viewer Trust Boundary Contract (Phase 0)

## Summary

This document defines the trust boundary for any “viewer” (local web UI, TUI, or static HTML viewer) that presents AKC plans and evidence artifacts.

The viewer is a read-only consumer. It must never take over execution, must never run tools/commands implied by artifacts, and must not access secrets/tool credentials.

## Threat model (what the viewer must assume)

For the viewer, the following are treated as untrusted input:

- `.akc/plan` JSON plan state (plan steps, notes, outputs, and feedback fields)
- emitted evidence artifacts (e.g. `.akc/tests/*.{json,txt}`, `.akc/verification/*.json`, `.akc/design/*.system_design.json`, `.akc/orchestration/*.orchestration.json`, `.akc/agents/*.coordination.json`, `.akc/deployment/**`, generated `.github/workflows/akc_deploy_*.yml`, and the bundle `manifest.json`)
- any patch text or stdout/stderr stored inside artifacts

The viewer must treat artifact contents as data to render or download, not as executable instructions.

## Allowed actions (in scope)

The viewer may:

- read plan state from the persisted plan store (e.g. `.akc/plan/.../*.json` and an active plan pointer)
- read emitted artifacts from the output emitter root (e.g. a tenant-scoped `manifest.json` and evidence under `.akc/tests/*`)
- display:
  - step status/progress from plan state
  - links to evidence artifacts
  - human-readable stdout/stderr and structured JSON findings
- render artifact-pass outputs (design/orchestration/coordination/deployment) as read-only content
- provide “download this evidence file” UX (copy files / open in a browser / offer downloads)

The viewer must remain local-first:

- no hosted execution mode by default
- no remote execution integration inside the OSS viewer

## Prohibited actions (out of scope)

The viewer must not:

- execute generated code, apply patches to a working tree, or run tests
- call into the compile/execute pipeline or invoke any executor/tooling that would change repository state
- access secrets/tool credentials from the environment or from the host
- reach out to attacker-controlled URLs derived from artifact content (no “click-to-open external code” patterns that fetch/execute)
- load dynamic code modules provided by artifact content

## Trust boundary statement (non-negotiable)

The compile loop and executor remain the only components that can execute or validate generated artifacts.

The viewer is strictly a read-only renderer for artifacts:

- viewer reads data
- renderer displays evidence
- viewer never changes the trust boundary or expands capabilities beyond “read-only”

If a future viewer requires more capabilities, that capability change must be treated as a security boundary change and discussed as an OSS contract update.

## Evidence in the current repo (what this contract matches)

- Execution sandbox and tenant isolation are documented in `docs/security.md` (enforced vs best-effort, untrusted inputs, sandbox lanes).
- Plans are persisted and tenant/repo scoped via `src/akc/memory/plan_state.py` under a `.akc/plan` directory.
- Output emission is filesystem-scoped and path-confined via `src/akc/outputs/emitters.py` (under-root enforcement) and tied together via `manifest.json`.
- Compile session emission writes patch/evidence files (e.g. `.akc/tests/*` and optional verification JSON) under tenant-scoped output paths via `src/akc/compile/session.py`.

## Fleet control-plane HTTP (write plane, narrow)

The fleet HTTP server (`akc fleet serve`, `akc.control.fleet_http`) stays a **trust-boundary** surface: it does **not** serve raw artifact trees or arbitrary files. Read routes expose merged rows from per-tenant `operations.sqlite` indexes only.

**Writes** are intentionally minimal and map 1:1 to existing safe control-plane mutations (for example, `POST /v1/runs/{tenant}/{repo}/{run_id}/labels` matches `OperationsIndex.upsert_label` / `akc control runs label-set`). They do **not** edit run manifests or trigger compile/runtime execution.

### Threat model (Bearer API)

- **CSRF:** Not applicable to typical Bearer-token API usage (no browser cookie session). Clients must send `Authorization: Bearer …` explicitly.
- **Replay:** Tokens are static secrets; replay of a captured request is possible until rotation. Optional **nonce / request signing** may be added later; `X-Request-ID` is echoed for correlation and audit only.
- **Tenant isolation:** Every mutating request is checked against the token’s `tenant_allowlist` (anonymous read bypass does **not** apply to writes).
- **Capability scopes:** Tokens carry scopes such as `runs:read`, `runs:label`, and (future) `audit:read`, orthogonal to the tenant allowlist. A read-only token cannot apply labels over HTTP.
- **Audit:** Each accepted mutation appends one line to `<outputs_root>/<tenant>/.akc/control/control_audit.jsonl` (actor from token metadata, request id, before/after snapshot for the mutation).

## Cross-shard automation contract (bounded, non-execution)

The fleet automation coordinator is a **control-plane only** worker that operates over read-indexed run rows and tenant-scoped control artifacts. It is intentionally bounded (`max_candidates`, `max_actions`, retry caps) and replay-safe through durable dedupe/checkpoint metadata in each tenant `operations.sqlite`.

Allowed cross-shard operation classes are explicitly limited to:

- metadata/tag writes (for example `run_labels` in `operations.sqlite`)
- incident workflow orchestration (read-only playbook report + incident artifact generation)
- webhook signaling (signed notifications to configured subscribers)

Explicitly prohibited from the fleet plane:

- compile loop invocation (`akc compile`, pass execution, patch generation/application)
- runtime/deployment execution (`akc runtime start/reconcile`, adapter action execution)
- arbitrary tool or command execution derived from artifact content

## Compatibility expectations

To keep viewer evolution safe:

- the viewer must be schema-driven against the artifact contract (unknown fields should be ignored safely)
- the viewer must never depend on artifact content to decide whether to execute something
- the viewer must be able to render “verification failed” states without changing any execution behavior
- the viewer must tolerate additive artifact groups in `manifest.json` metadata (for example, `metadata.artifact_passes`) without assuming fixed key sets

