# OSS Direction Memo (Phase 0)

## Purpose

This memo defines the recommended open-source (OSS) direction for the Agentic Knowledge Compiler (AKC) with an explicit goal: ship a CLI-first, safety/correctness builder that compiles evidence-backed artifacts while keeping execution trust boundaries local and well-defined.

It is written to be actionable for maintainers and contributors, and to document what AKC already does well (and what it intentionally does not do yet).

## MVP (what we should ship first)

The OSS MVP should be a CLI-first workflow for safety-focused builders:

1. `akc ingest` to normalize sources into a structured (and tenant-scoped) index.
2. `akc compile` to run the correctness-aware compile loop:
   - Plan -> Retrieve -> Generate -> Execute -> Repair
   - with tests and (optionally) deterministic verification gates
3. `akc verify` to validate the emitted evidence artifacts produced by `compile`.
4. `akc drift` / `akc watch` to help teams detect when inputs no longer match previously emitted outputs.

The MVP is “artifact-first”: every compile run produces auditable evidence artifacts (e.g. a `manifest.json` plus structured test/verification records) that can be inspected offline.

## Recommendation (what we will conclude)

### Primary OSS product direction (MVP)

- **CLI-first OSS** for safety/correctness builders.
- Emphasize **reproducible, tenant-scoped compilation** that produces auditable artifacts and test/verification evidence.
- Keep any WebUI/service capability **optional and local-first**, reading existing artifact/state contracts instead of introducing a new hosted execution trust boundary.

### Secondary direction (what to add after core adoption)

- A **thin “viewer”** (local web or TUI) that:
  - lists runs/plans for a tenant+repo,
  - renders step status from plan state,
  - links/downloads `manifest.json` and `.akc/tests/*.json` findings.
- Only later consider any hosted service mode, and explicitly keep it out of “OSS core” scope unless the sandbox/security story is fully formalized.

## Primary differentiators (already present in-repo)

### 1. CLI-first UX and explicit entrypoints

The CLI exposes the primary user experience as first-class commands (no required web server, API keys, or hosted UI).

### 2. Correctness-aware compile loop (with gates + repair)

AKC’s compile controller is built around a bounded loop that:

- requires tests-by-default (with a policy heuristic for when tests must be included),
- runs a smoke/full test gating scheme,
- budget-limits iterations/repairs,
- and optionally runs a deterministic verifier gate that can veto promotion even when tests pass.

### 3. Defense-in-depth execution + tenant isolation (security model)

AKC treats generated and ingested payloads as untrusted and constrains execution using defense-in-depth:

- tenant-scoped artifacts (no cross-tenant reads/writes),
- capabilities + policy evaluation before execution,
- sandbox lane selection and resource limits,
- and correlation-safe observability (tenant/run IDs without leaking sensitive payloads).

## Out of scope (trust boundary + hosted modes)

AKC’s OSS direction keeps the execution trust boundary small and stable. The OSS project should not grow a general “agent execution service” inside a UI or viewer.

Non-goals:

- Hosted execution / SaaS viewer as the default OSS mode.
- A viewer that can execute or otherwise take over the compilation/execution process.
- A viewer that can access secrets or tool credentials.
- Expanding the trust boundary to include “remote UI” execution paths.

What we do allow (later feature posture):

- An optional local viewer (TUI or local web static) that reads:
  - plan state (e.g. `.akc/plan`),
  - emitted artifacts and evidence (e.g. `manifest.json`, `.akc/tests/*`, and verification records),
  - and renders progress + evidence links without changing execution semantics.

The viewer is schema-driven and local-first by design: it should be a read-only consumer of artifact contracts, not a new execution surface.

## Roadmap (priorities, not a re-architecture)

This memo is Phase 0 and is not asking for behavior changes. It documents alignment and establishes a shared contract mindset.

Suggested sequencing:

1. Phase 0 (this memo): publish direction + evidence mapping; document the viewer non-goals.
2. Phase 1: align repository hygiene with OpenSSF OSPS baseline concepts (security reporting, release hygiene, access control).
3. Phase 2: supply-chain hardening with SLSA provenance and keyless release signing.
4. Phase 3: freeze and machine-validate artifact schemas (manifest + evidence JSONs) so viewers and auditors stay compatible.
5. Phase 4: strengthen tenant isolation and correctness gates with adversarial test cases.
6. Phase 5: build a thin local viewer only after contracts are stable, so the viewer never expands the trust boundary.

## “What evidence proves” (positioning for OSS users)

AKC should be positioned as a tool for safety/correctness builders that produces:

- deterministic, tenant-scoped plan state,
- evidence-backed compile outcomes,
- structured test/verification outputs,
- and a security posture that is explicit about enforced vs best-effort controls.

The OSS community can extend connectors, models, and output types, but should keep the core principles stable:

- keep execution trust local and constrained,
- make evidence artifacts the primary audit surface,
- and treat tenant identity as a first-class security input.
