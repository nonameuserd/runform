# Agentic Knowledge Compiler (AKC)

AKC is an **AI-native software factory** for turning project knowledge into buildable software.

It grounds software generation in real project context: docs, codebases, OpenAPI specs, messaging exports, and MCP resources. Instead of stopping at a one-shot AI answer, AKC moves work through structured stages that can generate code and other artifacts, verify results, track evidence, and support runtime, delivery, and control-plane workflows.

The core loop is:

**Plan -> Retrieve -> Generate -> Execute -> Repair**

AKC defaults to an offline-friendly path for local demos:

- ingest can use `--embedder hash` for deterministic, key-free indexing
- compile uses an offline backend by default
- verify, view, and most control-plane flows run locally
- validation can stay evidence-first through operator-side validator bindings plus exported artifacts

## Status

**Alpha.** The repository is active and broad in scope, but interfaces are still moving.

## What AKC Does

- **Ground software work in context:** ingest `docs`, `codebase`, `openapi`, `slack`, `discord`, `telegram`, `whatsapp`, and `mcp`
- **Generate more than answers:** produce code and other structured artifacts through compile and verification stages
- **Keep work inspectable:** record replayable evidence, policy decisions, and validation results
- **Support operator workflows:** expose assistant, runtime, living-recompile, control-plane, and fleet surfaces
- **Move toward delivery:** support named-recipient delivery sessions and packaging/distribution workflows for web, iOS, and Android targets
- **Preserve local inspection:** provide TUI, static web, and export viewer modes for emitted artifacts

## Quick Start

### Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)

### Install

```bash
git clone https://github.com/nonameuserd/runform.git
cd runform
uv sync
uv sync --extra dev

source .venv/bin/activate   # Windows: .venv\Scripts\activate
akc --help
```

Useful optional extras:

- `uv sync --extra ingest-all` for docs/OpenAPI/embedding/vector/messaging/MCP ingest extras
- `uv sync --extra mcp-serve` for `akc mcp`
- `uv sync --extra vectorstore-pg` for the pgvector index backend
- `uv sync --extra delivery-providers` for store-signing/provider integrations used by `akc deliver`

### Safe Local Demo

This path stays local and avoids working-tree writes by using `--artifact-only`.

```bash
# Optional: create .akc/project.json and a local policy stub
akc init

# Ingest local docs into a persistent sqlite index
akc ingest \
  --tenant-id demo \
  --connector docs \
  --input ./docs \
  --embedder hash \
  --index-backend sqlite

# Compile into reviewable artifacts only
akc compile \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --artifact-only

# Verify emitted artifacts
akc verify \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out

# Generate a static HTML viewer bundle
akc view \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  web
```

If you want weighted memory during compile or assistant sessions:

```bash
export AKC_WEIGHTED_MEMORY_ENABLED=1
```

## Common Workflows

### Ingest

Index repository or external knowledge sources for later retrieval:

```bash
akc ingest --tenant-id demo --connector codebase --input . --embedder hash --index-backend sqlite
akc ingest --tenant-id demo --connector openapi --input ./examples/openapi/petstore.json
```

### Compile and Verify

Generate artifacts under `<outputs-root>/<tenant>/<repo>/` and verify them:

```bash
akc compile --tenant-id demo --repo-id runform --outputs-root ./out --artifact-only
akc verify --tenant-id demo --repo-id runform --outputs-root ./out
```

`akc compile` defaults to `scoped_apply`, so use `--artifact-only` when you want a non-mutating run.

To execute observability or mobile validators before operational verification:

```bash
akc verify \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --execute-validators
```

### Assistant

Run a single planning turn:

```bash
akc assistant --mode plan -p "show me the next command to verify the latest compile outputs"
```

Run an interactive session:

```bash
akc assistant
```

### View Artifacts

```bash
akc view --tenant-id demo --repo-id runform --outputs-root ./out tui
akc view --tenant-id demo --repo-id runform --outputs-root ./out web
akc view --tenant-id demo --repo-id runform --outputs-root ./out export
```

### Runtime, Control, Delivery, and Fleet

The CLI also exposes:

- `runtime` for start/stop/status, reconcile, replay, checkpoint, coordination planning, and autopilot
- `control` and `policy` for operational indexes, replay forensics, policy bundles, and explainability
- `control-bot` for the standalone multi-channel operator gateway
- `deliver` for named-recipient delivery sessions
- `fleet` for aggregated read-only control-plane views across many outputs roots

## Repository Map

| Path | Purpose |
| --- | --- |
| `src/akc/` | Main Python package: CLI, ingest, compile, runtime, control, delivery, memory, assistant, viewer |
| `tests/` | Unit, integration, and end-to-end coverage |
| `docs/` | Getting started, CLI reference, architecture, runtime, delivery, viewer, ops |
| `configs/` | Example policy, eval, and SLO configuration |
| `deploy/` | Deployment references for systemd, Compose, Kubernetes, and CI |
| `examples/` | Sample OpenAPI, WASM, and backend integration examples |
| `rust/` | Optional Rust crates for ingest, executor, and protocol surfaces |
| `scripts/` | CI and quality gate helpers |
| `packaging/`, `tools/nuitka/` | Packaging and standalone binary build helpers |

## Documentation

- [Docs index](docs/index.md)
- [Getting started](docs/getting-started.md)
- [CLI command reference](docs/cli-commands.md)
- [Architecture](docs/architecture.md)
- [Compile-time skills](docs/compile-skills.md)
- [Validation](docs/validation.md)
- [Runtime execution](docs/runtime-execution.md)
- [Delivery architecture](docs/delivery-architecture.md)
- [Viewer](docs/viewer.md)
- [Ops runbook](docs/ops-runbook.md)
- [Artifact contracts](docs/artifact-contracts.md)

## Development

Local checks:

```bash
uv run ruff check .
uv run ruff format .
uv run mypy src/akc
uv run pytest
```

Contributor docs:

- [CONTRIBUTING.md](CONTRIBUTING.md)
- [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)
- [SECURITY.md](SECURITY.md)
- [GOVERNANCE.md](GOVERNANCE.md)

## License

Licensed under the [Apache-2.0](LICENSE) license.
