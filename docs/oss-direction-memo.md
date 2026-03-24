# OSS Direction Memo

## Purpose

This memo defines the recommended open-source (OSS) direction for the Agentic Knowledge Compiler (AKC) with an explicit goal: ship a CLI-first, safety/correctness builder that compiles evidence-backed artifacts while keeping execution trust boundaries local and well-defined.

It is written to be actionable for maintainers and contributors, and to document what AKC already does well (and what it intentionally does not do yet). It should stay aligned with the **actual** CLI and packages under `src/akc/` (see [README](../README.md) and [artifact contracts](artifact-contracts.md)).

## MVP (what we should ship first)

The OSS MVP remains a CLI-first workflow for safety-focused builders. In the current tree, the corresponding commands are implemented as follows:

1. **`akc init`** — bootstrap `.akc/project.json` and local policy stubs so tenant/repo scope and policy paths are explicit.
2. **`akc ingest`** — normalize sources into a structured, tenant-scoped index (connectors: docs, OpenAPI, Slack; pluggable embedders and vector backends: memory, SQLite, pgvector).
3. **`akc compile`** — run the correctness-aware compile loop **Plan → Retrieve → Generate → Execute → Repair**, with tests and optional deterministic verification gates (see `src/akc/compile/`).
4. **`akc verify`** — validate emitted evidence artifacts from compile outputs.
5. **`akc drift`** / **`akc watch`** — detect when inputs no longer match previously emitted outputs (`src/akc/outputs/drift.py` and CLI wiring).

The MVP is **artifact-first**: compile runs produce auditable evidence under a tenant-scoped output tree (for example `manifest.json`, `.akc/tests/*.json`, `.akc/verification/*`, and related envelopes described in [artifact contracts](artifact-contracts.md)).

### Beyond the core loop (optional OSS surfaces)

The repository intentionally ships additional **local, opt-in** commands that do not replace the core story but support operators and advanced workflows. Treat these as extensions, not prerequisites for “hello compile”:

- **`akc view`** — read-only TUI, static HTML bundle, or portable export over existing plan/evidence (`src/akc/viewer/`); does not execute compiles or hold secrets.
- **`akc runtime`** — operate runtime bundles, scheduler/reconciler evidence, replay (`src/akc/runtime/`).
- **`akc control`** — query operations indexes, manifest diff, forensics/replay, policy bundles (`src/akc/control/`).
- **`akc deliver`** — named-recipient delivery control plane when compile emits a `delivery_plan` (`src/akc/delivery/`).
- **`akc living`** — safe recompile on drift, webhook helpers (`src/akc/living/`).
- **`akc fleet`** — aggregate read-only views across shards (HTTP query API and CLI helpers).
- **`akc eval`**, **`akc metrics`**, **`akc policy explain`** — harnesses and operator UX.

Optional **Rust** acceleration lives under `rust/crates/` (e.g. experimental docs ingest); Python remains the primary OSS surface.

## Recommendation (what we will conclude)

### Primary OSS product direction (MVP)

- **CLI-first OSS** for safety/correctness builders.
- Emphasize **reproducible, tenant-scoped compilation** that produces auditable artifacts and test/verification evidence (paths and schema rules in [artifact contracts](artifact-contracts.md)).
- Keep any WebUI/service capability **optional and local-first**, reading existing artifact/state contracts instead of introducing a new hosted execution trust boundary.

### Secondary direction (what to add after core adoption)

- A **thin “viewer”** (already present as `akc view`): lists plans/runs for a tenant+repo, renders step status from plan state, links or copies `manifest.json` and evidence files into a static or TUI bundle — **read-only**, schema-driven.
- Only later consider any hosted service mode, and explicitly keep it out of “OSS core” scope unless the sandbox/security story is fully formalized.

## Primary differentiators (already present in-repo)

### 1. CLI-first UX and explicit entrypoints

The CLI exposes the primary user experience as first-class commands (no required web server, API keys, or hosted UI). `akc --help` lists `init`, `ingest`, `compile`, `verify`, `drift`, `watch`, and the extension commands above.

### 2. Correctness-aware compile loop (with gates + repair)

The compile controller is built around a bounded loop that:

- requires tests-by-default (with policy heuristics for when tests must be included),
- runs smoke/full test gating,
- budget-limits iterations/repairs,
- and optionally runs a deterministic verifier gate that can veto promotion even when tests pass.

### 3. Defense-in-depth execution + tenant isolation (security model)

AKC treats generated and ingested payloads as untrusted and constrains execution using defense-in-depth:

- tenant-scoped artifacts (no cross-tenant reads/writes),
- capabilities + policy evaluation before execution,
- sandbox lane selection and resource limits,
- correlation-safe observability (tenant/run IDs without leaking sensitive payloads).

### 4. Versioned artifacts and contracts

Emitted JSON uses explicit **schema versioning** and stable `schema_id` patterns where applicable (`src/akc/artifacts/contracts.py`, `SchemaKind` in `src/akc/artifacts/schemas.py`). [Artifact contracts](artifact-contracts.md) documents viewer-facing layouts under `<output_dir>/<tenant_id>/<repo_id>/...`. Machine-checkable JSON Schemas live alongside those definitions.

## Out of scope (trust boundary + hosted modes)

AKC’s OSS direction keeps the execution trust boundary small and stable. The OSS project should not grow a general “agent execution service” inside a UI or viewer.

Non-goals:

- Hosted execution / SaaS viewer as the default OSS mode.
- A viewer that can execute or otherwise take over the compilation/execution process (the in-tree viewer is **read-only**).
- A viewer that can access secrets or tool credentials.
- Expanding the trust boundary to include “remote UI” execution paths.

What we do allow (feature posture):

- The **local** viewer (`akc view`) reads plan state and emitted artifacts and renders progress + evidence links without changing execution semantics.

The viewer is schema-driven and local-first by design: a read-only consumer of artifact contracts, not a new execution surface.

## Roadmap (priorities, not a re-architecture)

This memo documents alignment and a shared contract mindset; it is not a mandate to stop shipping features. Suggested sequencing:

1. **Phase 0 (direction):** publish direction + evidence mapping; document viewer non-goals — **this memo** (living document).
2. **Phase 1:** align repository hygiene with OpenSSF OSPS baseline concepts (security reporting, release hygiene, access control). **Partial:** private disclosure is documented in [SECURITY.md](../SECURITY.md); continue hardening releases and governance.
3. **Phase 2:** supply-chain hardening with SLSA provenance and keyless release signing (CI/dev extras include dependency audit hooks; Rust supply-chain config under `rust/supply-chain/`).
4. **Phase 3:** deepen machine validation of artifact schemas (manifest + evidence JSONs) so viewers and auditors stay compatible — **in progress** (`SchemaKind`, coordination JSON Schema under `src/akc/coordination/schemas/`, validation in `src/akc/artifacts/validate.py`).
5. **Phase 4:** strengthen tenant isolation and correctness gates with adversarial test cases (ongoing via `tests/unit/` and `tests/integration/`).
6. **Phase 5 (viewer):** the **first** thin local viewer is **in tree** (`akc view`); remaining work is UX polish, portability, and **contract stability** so the viewer never expands the trust boundary.

## “What evidence proves” (positioning for OSS users)

AKC should be positioned as a tool for safety/correctness builders that produces:

- deterministic, tenant-scoped plan state and run manifests,
- evidence-backed compile outcomes,
- structured test/verification outputs,
- and a security posture that is explicit about enforced vs best-effort controls.

The OSS community can extend connectors, models, and output types, but should keep the core principles stable:

- keep execution trust local and constrained,
- make evidence artifacts the primary audit surface (see [artifact contracts](artifact-contracts.md)),
- and treat tenant identity as a first-class security input.
