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
- Python dependency vulnerability scanning must be enforced in CI and the release publishing path.
  - The gate uses `pip-audit` to generate a CycloneDX SBOM annotated with known Python dependency vulnerabilities.
- All Python installs in CI/release must be lockfile-only (`uv sync --frozen`) to prevent dependency drift.

Evidence:
- Rust supply chain checks: `.github/workflows/ci.yml` (`cargo vet`)
- Python vulnerability scanning gate + SBOM output: `.github/workflows/ci.yml` and `.github/workflows/release.yml` (`pip-audit` + `uv export`)
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

## 11) WASM policy quick reference (deterministic limits + stable failures)

When `lane=wasm`, policy and regression checks should treat the following as stable contract points:

- Resource limit semantics:
  - `wall_time_ms` is a real elapsed-time deadline on supported platforms via Wasmtime epoch interruption.
  - `cpu_fuel` is a first-class deterministic CPU budget:
    - `cpu_fuel_budget = clamp(cpu_fuel, 1, 2_000_000_000)`
    - when both are set, timeout and CPU exhaustion remain distinct failure classes.
  - `memory_bytes` applies an explicit Wasmtime linear-memory cap when set.
  - `stdout_max_bytes` / `stderr_max_bytes` are deterministic in-memory capture caps (default `1 MiB` each).

- Machine-readable error envelope:
  - first stderr line is:
    - `AKC_WASM_ERROR code=<CODE> exit_code=<N> message=<TEXT>`

- Stable WASM failure classes (recommended policy keys):
  - timeout: `code=WASM_TIMEOUT`, `exit_code=124`
  - cpu/fuel exhaustion: `code=WASM_CPU_FUEL_EXHAUSTED`, `exit_code=137`
  - memory limit exceeded: `code=WASM_MEMORY_LIMIT_EXCEEDED`, `exit_code=138`
  - unsupported platform capability: `code=WASM_UNSUPPORTED_PLATFORM_CAPABILITY`, `exit_code=78`

- Platform fail-closed behavior:
  - Windows + requested WASM wall-time limit is currently unsupported and must fail with:
    - `WASM_UNSUPPORTED_PLATFORM_CAPABILITY` / `78`
  - do not silently downgrade unsupported strict guarantees in policy-enforced runs.

CI/release requirements:

- WASM regression tests must validate deterministic classification for timeout/fuel/memory/capability failures.
- WASM regression tests must validate filesystem contract enforcement, including:
  - no implicit filesystem access without `preopen_dirs`
  - write denial on read-only preopens
  - write allowance only when `allowed_write_paths` is a subset of `preopen_dirs`
- Policy tests should assert code/exit pairs above remain stable across executor changes.
- Policy tests should assert WASM execution context includes backend, network flag, preopen list, limits tuple, and platform capability profile for `executor.run`.
- Policy tests should assert WASM execution context separately exposes writable preopens and read-only preopens.
- Prod policy tests should assert deny behavior for:
  - unsupported strict platform controls
  - disallowed preopen path patterns
  - disallowed writable preopen path patterns
  - network-enabled WASM runs without an explicit exception
- CLI and bridge regression tests should validate that `--sandbox-cpu-fuel` and
  protocol `limits.cpu_fuel` map to stable failure classification.

Evidence:
- WASM lane implementation: `rust/crates/akc_executor/src/backend/wasm.rs`
- Exit code constants: `rust/crates/akc_executor/src/lib.rs`
- Bridge parsing/logging surface: `src/akc/compile/rust_bridge.py`
- WASM regression tests: `rust/crates/akc_executor/tests/wasm_lane.rs`

## 12) Docker strong hardening requirements and rollout

When the CLI uses `--sandbox strong`, Docker is the default strong lane. OSS security requirements must treat the Docker hardening contract as an explicit, test-backed surface rather than implicit runtime behavior.

Required documented defaults:

- network denied by default
- read-only root filesystem enabled by default
- `no-new-privileges` enabled by default
- `cap-drop ALL` enabled by default
- non-root user `65532:65532` by default
- `/tmp` mounted as tmpfs by default
- memory cap `1024 MiB` by default
- PID cap `256` by default
- stdout/stderr capture cap `2048 KiB` per stream by default

Required optional controls:

- `--docker-user`
- `--docker-tmpfs`
- `--docker-seccomp-profile`
- `--docker-apparmor-profile`
- `--docker-ulimit-nofile`
- `--docker-ulimit-nproc`
- `--docker-cpus`

Required fail-closed behavior:

- Docker-only hardening flags must be rejected outside `--sandbox strong`.
- Docker-only hardening flags must be rejected when the strong lane is fixed to WASM.
- Docker-only hardening flags must be rejected when `auto` fallback would drop them because Docker is unavailable.
- Invalid tmpfs/user/seccomp/AppArmor/ulimit values must fail before container launch.
- Absolute seccomp profile paths must exist and be files.
- AppArmor profiles must fail closed on unsupported hosts.

Required policy surface:

- OPA input for `executor.run` in Docker mode must include:
  - network mode / exception
  - read-only rootfs flag
  - no-new-privileges flag
  - cap-drop-all flag
  - user presence and non-root classification
  - seccomp/AppArmor profile identifiers
  - memory / PID / CPU / ulimit settings
  - tmpfs mounts
- Production policy profiles should deny Docker execution when required hardening controls are absent or weakened, including seccomp/AppArmor posture, `/tmp` tmpfs availability, memory/PID/ulimit ceilings, or network enablement without an explicit exception.

Required rollout stages:

1. Audit policy only.
   Use `--policy-mode audit_only` to verify emitted Docker policy context and collect expected denials without blocking merges or releases.
2. Enforce policy in CI and release branches.
   Protected branches and release workflows must run an actual Docker strong compile in `--policy-mode enforce` so hardening regressions are release-blocking.
3. Enforce as default in the production profile.
   Production-facing guidance and automation should default to `--sandbox strong --strong-lane-preference docker` with the prod Rego policy profile in enforce mode.

CI/release requirements:

- Unit tests must validate Docker command assembly and invalid hardening rejection.
- Integration tests must validate runtime behavior for:
  - non-root execution
  - read-only rootfs denial outside tmpfs
  - writable tmpfs path
  - network isolation
- CI must run a Docker strong compile against the prod Rego profile so executor/policy drift is caught outside of unit tests.
- CI/release branch protection should treat Docker hardening regressions as blocking once the rollout reaches stage 2.
- Documentation in `docs/security.md` and `docs/getting-started.md` must stay aligned with the implemented defaults and tests.

Evidence:
- Docker executor implementation: `src/akc/compile/executors.py`
- CLI Docker preflight/validation: `src/akc/cli/compile.py`
- Docker policy context emission: `src/akc/compile/controller.py`
- Docker unit tests: `tests/unit/test_compile_executors.py`, `tests/unit/test_cli_compile.py`
- Docker integration tests: `tests/integration/test_docker_runtime_hardening.py`
