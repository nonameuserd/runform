# Agentic Knowledge Compiler (AKC)

**AKC compiles docs, messaging (Slack, Discord, Telegram, WhatsApp Cloud API), OpenAPI, and MCP sources into runnable artifacts** (code, workflows, agent specs) using a correctness-aware loop:
**Plan → Retrieve → Generate → Execute → Repair**.

Unlike “summarize & hope”, AKC is built around **grounding + replay + safety gates**:

- **Retrieval-first** from a structured index + **code memory**
- **Tenant + repo isolation** threaded through ingest/compile/runtime artifacts
- **Policy gates (OPA/Rego)** for tool use and mutation (default-deny posture)
- **Tests-by-default** with repair loops and evidence artifacts (manifests, spans, decisions)

## 60-second demo (offline)

**Requirements:** Python 3.11+, [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/nonameuserd/runform.git
cd runform
uv sync --extra dev
source .venv/bin/activate   # Windows: .venv\Scripts\activate

# ingest local docs and build an index (no API keys)
akc ingest --tenant-id demo --connector docs --input ./docs --embedder hash --index-backend sqlite

# (sqlite index defaults to ./.akc/ingest/<tenant-id>/index.sqlite3; override with --sqlite-path)

# compile + verify gates (compile uses an offline backend by default)
AKC_DEVELOPER_ROLE_PROFILE=emerging akc compile --tenant-id demo --repo-id runform --outputs-root ./out
akc verify  --tenant-id demo --repo-id runform --outputs-root ./out

# inspect evidence
akc view --tenant-id demo --repo-id runform --outputs-root ./out web
```

## What you get

- **Artifacts**: versioned IR, run manifests, retrieval snapshots, policy decisions, test outputs
- **Runtime bundles**: scheduler/reconciler-ready bundles and replayable evidence
- **Optional delivery**: named-recipient packaging/distribution via `akc deliver`

## Status

**Alpha.** Interfaces are stabilizing; expect breaking changes between minor releases.

## Repository overview

| Area                    | Contents                                                                                                                                                                                                                      |
| ----------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Python package**      | `src/akc/` — ingestion, versioned IR, run manifests, compile loop, runtime kernel, control plane (OPA/Rego), optional app **delivery**, living/drift automation, fleet aggregation, artifact contracts, eval harness, viewers |
| **Rust (optional)**     | `rust/crates/` — `akc_ingest`, `akc_executor`, `akc_protocol`, and shared helpers; optional Rust-backed docs ingestion and executor paths from Python                                                                               |
| **Tests**               | `tests/unit/`, `tests/integration/`                                                                                                                                                                                           |
| **Examples & fixtures** | `examples/`                                                                                                                                                                                                                   |
| **Reference configs**   | `configs/` — eval suites, sample SLO/policy stubs                                                                                                                                                                             |
| **Deploy references**   | `deploy/` — systemd, Compose, Kubernetes, and CI examples for autopilot / living flows                                                                                                                                        |
| **CI helpers**          | `scripts/` — policy, retrieval, benchmark, and reliability SLO gates                                                                                                                                                          |

The compile phase follows **Plan → Retrieve → Generate → Execute → Repair**, with retrieval from structured indexes and **code memory**. **Runtime** applies bundles through a scheduler/reconciler; **delivery** is a separate path for named-recipient packaging and distribution (stores under `.akc/delivery/`) when a compile emits a `delivery_plan`.

### Package map (`src/akc/`)

| Package         | Role                                                                                               |
| --------------- | -------------------------------------------------------------------------------------------------- |
| `ingest/`       | Connectors: docs, OpenAPI, **messaging** (Slack, Discord, Telegram, **WhatsApp** Cloud webhook captures), MCP; chunking, embeddings, vector index (memory / SQLite / pgvector) |
| `ir/`           | Versioned intermediate representation and diffing                                                  |
| `run/`          | Run manifest, replay, VCR helpers, delivery lifecycle hooks                                        |
| `intent/`       | Intent specs, stores, resolution, policy projection                                                |
| `compile/`      | Controller, planner, retriever, verifiers, skills pipeline, MCP adapter, artifact/IR passes, scoped apply |
| `memory/`       | Code memory, plan state, why-graph stores                                                          |
| `outputs/`      | Emitters, drift and fingerprint helpers                                                            |
| `runtime/`      | Kernel, autopilot, leases, providers (local, compose, Kubernetes), bundle handoff                  |
| `delivery/`     | Delivery sessions, packaging and distribution adapters                                             |
| `control/`      | Policy bundles, operations/cost indexes, fleet helpers, OTEL hooks                                 |
| `living/`       | Safe recompile on drift, webhook receiver, automation profiles                                     |
| `coordination/` | Coordination graph semantics shared by compile and runtime                                         |
| `knowledge/`    | Canonical knowledge snapshots and fingerprints                                                     |
| `artifacts/`    | Tenant-scoped JSON envelopes and validation                                                        |
| `execute/`      | Executor factory and sandbox execution surface                                                     |
| `viewer/`       | TUI and static HTML snapshot/export                                                                |
| `evals/`        | Evaluation harness                                                                                 |

## Quick start

**Requirements:** Python 3.11+, [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/nonameuserd/runform.git
cd runform
uv sync
uv sync --extra dev   # pytest, ruff, mypy, pre-commit, …

source .venv/bin/activate   # Windows: .venv\Scripts\activate
akc --help
```

Bootstrap a project (creates `.akc/project.json` and optional local policy stub):

```bash
akc init
```

Then use **[Getting started](docs/getting-started.md)** for ingest → compile → verify, scope resolution (`CLI` → `AKC_*` → `.akc/project.json`), and deployment notes.

### Optional install extras

Core `akc` ships with **`jsonschema`**, **`pydantic`**, and (on Windows) **`windows-curses`** for the TUI. Everything else is optional:

| Extra                | Purpose                                                        |
| -------------------- | -------------------------------------------------------------- |
| `dev`                | Test, lint, type-check, release tooling                        |
| `ingest-docs`        | Richer docs parsing (e.g. markdown-it, BeautifulSoup)          |
| `ingest-openapi`     | YAML OpenAPI specs                                             |
| `ingest-embed`       | HTTP client for remote embedders                               |
| `ingest-vectorstore` | e.g. Chroma persistent backend                                 |
| `ingest-messaging`   | Official **Slack** SDK (stdlib HTTP fallback exists); use **`akc ingest --connector slack`**. **Discord** / **Telegram** use **`--connector discord`** / **`telegram`** with bot tokens. **WhatsApp** uses **`--connector whatsapp`** and **`--input`** pointing at stored **Cloud API** JSON/JSONL webhook payloads (capture on your side; optional dedupe via **`--whatsapp-state-path`**) |
| `ingest-mcp`         | MCP Python SDK + HTTP client for **`akc ingest --connector mcp`** |
| `mcp-serve`          | MCP SDK for **`akc mcp serve`** (read-only tools server)       |
| `ingest-all`         | All connector / ingest / MCP extras above (including `mcp-serve`) |
| `vectorstore-pg`     | Postgres + pgvector index backend                              |
| `delivery-providers` | JWT / Google OAuth for store APIs (`akc deliver` distribution) |

Example:

```bash
uv sync --extra ingest-docs --extra ingest-all
uv sync --extra mcp-serve
uv sync --extra vectorstore-pg --extra delivery-providers
```

## CLI surface (top level)

Run `uv run akc --help` for the full tree. High-level commands:

- **`init`** — project bootstrap
- **`ingest`** — pluggable connectors: **`docs`**, **`openapi`**, **messaging** (**`slack`**, **`discord`**, **`telegram`**, **`whatsapp`**), **`mcp`**; index backends **`memory`**, **`sqlite`**, **`pgvector`**
- **`mcp`** — **`mcp serve`** runs AKC as an MCP server (requires `mcp-serve` extra)
- **`slack list-channels`** — list Slack channels (use with messaging / Slack ingest)
- **`compile`**, **`verify`** — compilation and verification gates
- **`drift`**, **`watch`** — output vs source drift and polling
- **`living-recompile`**, **`living-webhook-serve`**, **`living-doctor`** — drift-driven automation and health checks
- **`eval`** — eval harness
- **`runtime`** — `start`, `stop`, `status`, `events`, `reconcile`, `checkpoint`, `replay`, `coordination-plan`, `autopilot`
- **`metrics`** — metrics helpers
- **`policy explain`** — policy / denial narratives
- **`control`** — runs index, manifest diff, replay forensics, incident/forensics export, playbooks, policy bundle validate/show/write
- **`deliver`** — named-recipient app delivery (see below)
- **`fleet`** — multi-scope HTTP catalog, webhooks, automation coordinator, operator dashboard, policy-bundle distribution
- **`view`** — `tui`, `web`, `export` for evidence and snapshots

### Named-recipient app delivery (`akc deliver`)

Implementation detail: **[Delivery architecture](docs/delivery-architecture.md)** (on-disk layout, modules, CLI, sequencing).

This is the **App Delivery to Named Recipients v1** workflow: a plain-language `--request`, explicit `--recipient` / `--recipients-file` (authoritative list—not parsed from free text), optional `akc compile` via `--compile`, then packaging and distribution adapters. First-wave targets are **web**, **iOS**, and **Android**; release modes are **`beta`**, **`store`**, or **`both`** (beta then human gate then store). Artifacts live under `.akc/delivery/<id>/` (`delivery_request.v1`, `delivery_session.v1`, events, provider state, activation evidence). Store/API signing uses optional **`delivery-providers`** extra; missing prerequisites produce a **blocked** session rather than silent downgrade.

**Default (submit)** — no subcommand, flags on `akc deliver`:

```bash
akc deliver \
  --request "build an app and send it to these users" \
  --recipient alice@example.com --recipient bob@example.com --recipient carol@example.com \
  --platforms web,ios,android \
  --release-mode both
```

**Subcommands:**

| Command                                                                | Role                                                                                                                                                 |
| ---------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| `akc deliver` (default)                                                | Create session from `--request`, `--recipient` / `--recipients-file`, `--platforms`, `--release-mode`; optional `--compile` and `--delivery-version` |
| `akc deliver status --delivery-id <id>`                                | Request + session JSON                                                                                                                               |
| `akc deliver events --delivery-id <id>`                                | Control-plane event list                                                                                                                             |
| `akc deliver resend --delivery-id <id> --recipient <email>`            | Record resend for one recipient                                                                                                                      |
| `akc deliver promote --delivery-id <id> --lane beta` or `--lane store` | Promotion after beta readiness                                                                                                                       |
| `akc deliver gate-pass --delivery-id <id>`                             | Human readiness gate (`release-mode=both` before store)                                                                                              |
| `akc deliver activation-report --delivery-id <id>`                     | Ingest app-side activation JSON (invite-based proof)                                                                                                 |
| `akc deliver web-invite-open …`                                        | Record signed web invite open (web beta provider proof)                                                                                              |

## Documentation

- **[Getting started](docs/getting-started.md)** — install, first run, configuration, embedding providers
- **[Architecture](docs/architecture.md)** — end-to-end flow and components
- **[Delivery architecture](docs/delivery-architecture.md)** — `akc deliver`, `.akc/delivery/` artifacts, packaging and distribution
- **[Artifact contracts](docs/artifact-contracts.md)** — emitted JSON shapes and versioning
- **[IR schema](docs/ir-schema.md)** — intermediate representation
- **[Runtime execution](docs/runtime-execution.md)** — autopilot, leases, SLO gates
- **[Viewer](docs/viewer.md)** — trust boundary and export workflows
- **[Research](docs/research.md)** — alignment (ARCS, DeepCode, DocAgent, etc.)
- **[Governance](GOVERNANCE.md)** — maintainers and decisions
- **[Deploy](deploy/README.md)** — Docker, Compose, Kubernetes examples

## Signed standalone binaries (CI Release)

The GitHub Release workflow builds standalone executables via Nuitka for macOS/Windows/Linux. Extract the archive and run the binary at the **root** of the `akc.dist/` folder: **`akc.bin`** on macOS/Linux, **`akc.exe`** on Windows (Nuitka standalone mode cannot place the POSIX binary under `bin/`; the bundled package data lives alongside it under `akc/`).

- **Unsigned builds still work**: if signing secrets are not configured, CI will still publish binaries, just **unsigned / not notarized**.
- **Signed builds (recommended)**:
  - macOS: **Developer ID codesigning + notarytool notarization**
  - Windows: **Authenticode signing via `signtool.exe`**

### Required GitHub Secrets

Configure these repository secrets for signing in `.github/workflows/release.yml`.

#### macOS (Developer ID + notarytool)

- **`APPLE_SIGNING_CERT_P12_BASE64`**: Base64-encoded `.p12` containing your *Developer ID Application* certificate + private key.
- **`APPLE_SIGNING_CERT_P12_PASSWORD`**: Password for that `.p12`.
- **`APPLE_SIGNING_IDENTITY`**: Codesign identity string, e.g. `Developer ID Application: Your Org (TEAMID)`.
- **`APPLE_NOTARYTOOL_KEY_ID`**: App Store Connect API key id (e.g. `ABC123DEFG`).
- **`APPLE_NOTARYTOOL_ISSUER_ID`**: App Store Connect issuer id (UUID).
- **`APPLE_NOTARYTOOL_PRIVATE_KEY_P8_BASE64`**: Base64-encoded App Store Connect API private key (`AuthKey_XXXXXX.p8`).

#### Windows (signtool)

- **`WINDOWS_SIGNING_CERT_PFX_BASE64`**: Base64-encoded code signing certificate `.pfx` (with private key).
- **`WINDOWS_SIGNING_CERT_PFX_PASSWORD`**: Password for that `.pfx`.

## Contributing

Issues and pull requests are welcome. See **[CONTRIBUTING.md](CONTRIBUTING.md)**, **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)**, and **[SECURITY.md](SECURITY.md)**.

**Local checks:** `uv run ruff check .`, `uv run ruff format .`, `uv run mypy src/akc`, `uv run pytest`.

## License

Licensed under the [Apache-2.0](LICENSE) license.
