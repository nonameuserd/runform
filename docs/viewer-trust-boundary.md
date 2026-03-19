# Viewer Trust Boundary Contract (Phase 0)

## Summary

This document defines the trust boundary for any “viewer” (local web UI, TUI, or static HTML viewer) that presents AKC plans and evidence artifacts.

The viewer is a read-only consumer. It must never take over execution, must never run tools/commands implied by artifacts, and must not access secrets/tool credentials.

## Threat model (what the viewer must assume)

For the viewer, the following are treated as untrusted input:

- `.akc/plan` JSON plan state (plan steps, notes, outputs, and feedback fields)
- emitted evidence artifacts (e.g. `.akc/tests/*.{json,txt}`, `.akc/verification/*.json`, and the bundle `manifest.json`)
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

## Compatibility expectations

To keep viewer evolution safe:

- the viewer must be schema-driven against the artifact contract (unknown fields should be ignored safely)
- the viewer must never depend on artifact content to decide whether to execute something
- the viewer must be able to render “verification failed” states without changing any execution behavior

