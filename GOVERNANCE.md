# Governance

This document describes how the Agentic Knowledge Compiler project is governed: who maintains it, how decisions are made, and what is in and out of scope.

## Maintainers

Maintainers are responsible for:

- Reviewing and merging pull requests
- Releasing versions and maintaining the changelog
- Enforcing the [Code of Conduct](CODE_OF_CONDUCT.md) and [Security](SECURITY.md) process
- Triage security reports and coordinate fixes/disclosure per [`SECURITY.md`](SECURITY.md)
- Steering the project within the scope below

The current maintainer set is listed in the repository (e.g. in [CODEOWNERS](.github/CODEOWNERS) or in the README). New maintainers are added by consensus of existing maintainers.

## Decision-making

- **Routine changes** (bug fixes, docs, small features) are decided by review and merge. At least one maintainer approval is required.
- **Larger changes** (new phases, new subsystems, breaking API changes) should be proposed in an **RFC** (e.g. a GitHub Discussion or a doc in `docs/`) so the community and maintainers can comment. Decisions are made by maintainer consensus after a reasonable comment period.
- **Disputes** (e.g. conduct or scope) are resolved by maintainers. If a maintainer is involved, they recuse themselves from that decision.

## Scope

### In scope

- **Ingestion:** Connectors for docs, APIs, and messaging; chunking, normalization, and structured indexing.
- **Memory:** Code memory, plan state, and optional knowledge/causal graphs.
- **Compilation:** Plan → Retrieve → Generate → Execute → Repair loop; retrieval from index and code memory; test-driven repair.
- **Outputs:** Emitting code, workflow definitions, and agent specs as first-class artifacts.
- **Correctness:** Tests by default, optional formal verification or agentic verifiers for critical paths.
- **Documentation and tooling:** Docs (architecture, research, getting started), CI (Ruff, mypy, pytest), and release process.

### Operational validity

Operational validity reports and attestations (whether runtime evidence satisfies intent operational success criteria) are **conditional assurance**, not proof of arbitrary production correctness. In particular:

- **Signal quality:** Verdicts depend on the completeness and fidelity of exported evidence and telemetry (for example `runtime_evidence`, OTel NDJSON). Sparse, misleading, or incomplete traces can produce false passes or false failures.
- **Window length:** Checks run over a **bounded** evidence window (for example a single runtime run or a pre-filtered slice). Short windows may miss intermittent failures; long windows may dilute or conflate unrelated incidents.

Intent JSON must **not** embed credentials or ad-hoc external metric query strings. Bindings for future external metric or OTLP queries belong in **operator-supplied configuration** outside the intent specification, consistent with the artifact path and isolation rules in `docs/artifact-contracts.md`.

### Control plane and replay planning

The `akc control` CLI includes **read-oriented** workflows: manifest diff, replay forensics, incident and forensics bundles, operator playbooks, and **replay plan** emission.

**Replay plan** artifacts (`schema_kind: akc_replay_plan`) list the **effective partial-replay pass set** for a run manifest (declared `partial_replay_passes` unioned with mandatory passes implied by intent success-criterion evaluation modes, same semantics as manifest diff). They also include a **suggested** `akc compile` argv template. That JSON is for operators and CI to **copy or parameterize**; it is **not** auto-executed by the viewer or fleet read APIs. Running compile remains a separate, explicitly scoped action: use the correct `tenant_id` / `repo_id` / `outputs_root`, and supply `--replay-manifest-path` (or equivalent) when replaying against a prior run, as described in [getting-started.md](docs/getting-started.md).

### Out of scope

- **Hosted SaaS:** This project is open source; we do not operate a commercial hosted service.
- **Proprietary connectors or models:** Default experience should work with open or local components; optional cloud backends are fine if documented and optional.
- **Features that undermine security or reproducibility:** e.g. mandatory telemetry, non-auditable execution, or required proprietary APIs.

## License and contributions

- The project is licensed under **Apache-2.0**. Contributions are accepted under the [Developer Certificate of Origin](https://developercertificate.org/) (see [CONTRIBUTING.md](CONTRIBUTING.md)).
- We aim to align with community and OpenSSF best practices where applicable (dependency hygiene, versioning, vulnerability handling, documentation).

## Developer role profile and project bootstrap

- **Global CLI default:** If you omit `--developer-role-profile` on the CLI, leave `AKC_DEVELOPER_ROLE_PROFILE` unset, and there is no `.akc/project.json` (or it does not set `developer_role_profile`), the effective profile is **`classic`**. Precedence is **CLI → `AKC_DEVELOPER_ROLE_PROFILE` → project file → `classic`**. This preserves predictable behavior for existing scripts and CI that do not opt into the newer UX defaults.
- **New repositories:** `akc init` writes `.akc/project.json` with **`developer_role_profile: emerging`** by default (override with `akc init --developer-role-profile classic`). That scopes the “golden path” to projects that explicitly run init, without changing the global default for bare `akc` invocations.
- **Policy stub:** By default, `akc init` also installs a **local** starter Rego file at `.akc/policy/compile_tools.rego` and sets `opa_policy_path` / `opa_decision_path` in `.akc/project.json`. Compile and `akc living-recompile` resolve OPA paths with precedence **CLI → `AKC_OPA_POLICY_PATH` / `AKC_OPA_DECISION_PATH` → project file → compile defaults** (decision path defaults to `data.akc.allow` when unset). Use `akc init --no-policy-stub` to skip the copy and omit those keys.

A future release may switch the **global** default to `emerging` if maintainers document a migration path; until then, the split above is the explicit product decision.

**Alignment vs vision:** Maintainer-facing, evidence-based statements about how the codebase matches `docs/akc-vision.md` (including onboarding, gating, and tier-2 caveats) live in [`docs/akc-alignment.md`](docs/akc-alignment.md). Prefer that document and its checklist when evaluating whether a “strong end-state” claim is supported in OSS.

## Changes to this document

Changes to GOVERNANCE.md (e.g. adding/removing maintainers, changing scope) are made by maintainer consensus and documented in the [CHANGELOG](CHANGELOG.md).
