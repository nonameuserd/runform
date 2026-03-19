# OSS Security & Correctness Requirements (Phase 0)

This document is a checklist of security/correctness requirements that OSS CI and release processes must enforce.

It is written as “requirements to implement/maintain”, not as a claim about future features. Where the repo already enforces something, the evidence points to the corresponding workflow/tests.

## 1) CI correctness gates (baseline)

Every PR and mainline change must run the baseline quality gates:

- Python lint: `uv run ruff check .`
- Python formatting check: `uv run ruff format --check .`
- Python typecheck: `uv run mypy src/akc`
- Python tests: `uv run pytest`
- Rust checks:
  - `cargo fmt --check`
  - `cargo clippy -- -D warnings`
  - `cargo test --all`

Evidence: `.github/workflows/ci.yml`

## 2) Untrusted input handling in CI (least privilege)

CI must assume PR code is untrusted. Requirements:

- PR workflows run with least privilege (avoid secrets access; no privileged tokens in the PR job unless explicitly needed).
- Use GitHub Actions `pull_request` contexts with restrictive permissions.
- Avoid patterns that route secrets to untrusted steps.

Evidence: `.github/workflows/ci.yml` (CI permissions default to `contents: read`)

## 3) Tenant isolation invariants (security correctness)

AKC’s security model requires tenant isolation:

- Every request must carry `tenant_id` and `run_id`.
- Artifacts must be namespaced by tenant and stored under tenant-scoped directories.
- No cross-tenant caching or cross-tenant reads/writes.

CI/release requirements:

- Run sandbox isolation tests, including hardened `bwrap` lane tests (Linux) and Windows tenant isolation tests.
- Treat any failure in tenant isolation tests as a release-blocking correctness/safety failure.

Evidence:
- Security model: `docs/security.md`
- CI tenant sandbox tests: `.github/workflows/ci.yml` (`rust_bwrap`, `rust_windows`)
- Tenant isolation tests in Rust: `rust/crates/akc_executor/tests/tenant_isolation.rs`

## 4) Execution sandbox posture and enforcement checks

Execution must be defense-in-depth and policy-gated:

- Network must be denied unless explicitly allowed by policy and test configuration.
- Command execution must be allowlisted.
- Environment must be scrubbed.
- Filesystem access must be validated and confined by backend (strong on Linux `bwrap`, best-effort on native/macOS).

CI/release requirements:

- Ensure sandbox posture tests cover the critical lanes.
- Ensure executor changes include or update the relevant tenant isolation / sandbox regression tests.

Evidence: `docs/security.md`, `.github/workflows/ci.yml`

## 5) Patch safety and verifier gate requirements

Correctness and security require a deterministic verifier gate that can veto promotion:

- The verifier must validate patch format (unified diff headers) and reject suspicious paths:
  - absolute paths, traversal (`..`), drive prefixes, NUL bytes, and empty/undefined paths.
  - modifications to internal emitted artifacts (e.g. `.akc/`).
- When strict, any verifier finding must veto promotion (after tests pass).

CI/release requirements:

- The verifier’s unit tests must be run on every PR.
- Any change to verifier behavior must include updated tests and evidence artifacts documenting the change.

Evidence:
- Verifier implementation: `src/akc/compile/verifier.py`
- Verifier unit tests: `tests/unit/test_compile_verifier_unit.py`

## 6) Artifact confinement and “under root” safety

Evidence artifacts must be safe to consume and safe to write:

- Artifact write paths must stay under the configured emitter root.
- Tenant scoping must be respected for all artifacts.
- Path traversal/symlink-like escaping must be rejected.

CI/release requirements:

- Run emitter and output contract tests every PR.
- Keep the artifact contract stable and treat changes as schema/boundary updates requiring explicit review.

Evidence:
- Emitter implementation: `src/akc/outputs/emitters.py`
- Emitter tests: `tests/unit/test_outputs_emitters.py`

## 7) Evidence completeness requirements

For each successful compile run, evidence artifacts must be emitted so that a later viewer/auditor can reconstruct what happened:

- A bundle manifest (`manifest.json`) that ties together artifacts.
- Structured evidence under `.akc/tests/*` including stdout/stderr and exit codes where available.
- Optional structured verification records under `.akc/verification/*.json` when verifier feedback exists.

CI/release requirements:

- Tests must validate that emission occurs with correct scoping and stable bundle semantics.

Evidence:
- Compile session emission: `src/akc/compile/session.py`
- Manifest emitter contract: `src/akc/outputs/emitters.py`

## 8) Supply-chain hardening and provenance (release processes)

Release must produce verifiable provenance evidence:

- Rust integrity checks must run before provenance generation (including `cargo-vet`).
- SLSA provenance must be generated for release artifacts via the `slsa-github-generator` workflow.
- Release jobs must use keyless signing via OIDC where supported.

CI/release requirements:

- Any release workflow modifications must preserve provenance generation.
- Provenance generation must not be skipped without an explicit documented exception.

Evidence:
- Rust supply chain checks: `.github/workflows/ci.yml` (`cargo vet`)
- Release provenance: `.github/workflows/release.yml` (`slsa-github-generator`)

## 9) Viewer contract alignment (security boundary)

Although a viewer may be implemented later, CI/release requirements must preserve the boundary:

- Viewer must be read-only: it must not execute patches, run code, call the executor, or access secrets.
- Viewer must treat all evidence as untrusted data and render without interpretation-as-instructions.

CI/release requirements:

- Any addition of viewer code must include tests asserting read-only behavior and no execution side effects.

Evidence:
- Viewer non-execution threat model: `docs/security.md`
- Viewer contract: `docs/viewer-trust-boundary.md`

## 10) Phase 1 OSS hygiene (OSPS Level 1: security reporting + release hygiene)
This phase aligns the repo with OpenSSF OSPS concepts for security reporting and release hygiene:

### 10.1) Security reporting docs
- `SECURITY.md` defines the private vulnerability disclosure process.
- `security-insights.yml` provides a machine-readable, OSPS-aligned summary of security reporting and release hygiene (aligned to the OpenSSF Security Insights schema).
- `CONTRIBUTING.md` points contributors to the security reporting process for security-sensitive bugs.

### 10.2) CI guardrails for untrusted PRs (least privilege)
- All untrusted PR code must run under low-privilege defaults:
  - GitHub Actions `pull_request` context (not `pull_request_target`).
  - Workflow-level permissions remain restrictive (`contents: read`).
- The CI workflow includes a `policy-test` stage that asserts:
  - no `pull_request_target` / privileged patterns are introduced,
  - no `secrets.*` references appear in `pull_request` workflows,
  - no artifact uploads occur in PR contexts without provenance/attestation signals.

### 10.3) Branch protection expectations (required checks)
Branch protection for default and release branches should require the CI checks that implement the baseline hygiene gates:
- `CI / ci`
- `CI / Rust checks`
- `CI / Linux bwrap hardened isolation`
- `CI / Windows sandbox`
- `CI / Policy test (OSPS untrusted PR guardrails)`

