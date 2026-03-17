# Governance

This document describes how the Agentic Knowledge Compiler project is governed: who maintains it, how decisions are made, and what is in and out of scope.

## Maintainers

Maintainers are responsible for:

- Reviewing and merging pull requests
- Releasing versions and maintaining the changelog
- Enforcing the [Code of Conduct](CODE_OF_CONDUCT.md) and [Security](SECURITY.md) process
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

### Out of scope

- **Hosted SaaS:** This project is open source; we do not operate a commercial hosted service.
- **Proprietary connectors or models:** Default experience should work with open or local components; optional cloud backends are fine if documented and optional.
- **Features that undermine security or reproducibility:** e.g. mandatory telemetry, non-auditable execution, or required proprietary APIs.

## License and contributions

- The project is licensed under **Apache-2.0**. Contributions are accepted under the [Developer Certificate of Origin](https://developercertificate.org/) (see [CONTRIBUTING.md](CONTRIBUTING.md)).
- We aim to align with community and OpenSSF best practices where applicable (dependency hygiene, versioning, vulnerability handling, documentation).

## Changes to this document

Changes to GOVERNANCE.md (e.g. adding/removing maintainers, changing scope) are made by maintainer consensus and documented in the [CHANGELOG](CHANGELOG.md).
