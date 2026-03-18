# Getting started

## Prerequisites

- **Python 3.11+**
- **[uv](https://docs.astral.sh/uv/)** (recommended) or pip

## Install

```bash
git clone https://github.com/your-org/agentic-knowledge-compiler.git
cd agentic-knowledge-compiler
uv sync
```

For development (pytest, ruff, mypy, pre-commit):

```bash
uv sync --extra dev
uv run pre-commit install
```

## Run the CLI

The CLI provides `ingest`, `compile`, `verify`, `drift`, and `watch`. List commands:

```bash
uv run akc
```

### Extension points and transparency

- **Connectors and vector stores are pluggable** via CLI flags and ingest modules. The `--connector` choice (docs, openapi, slack) and `--index-backend` (memory, sqlite, pgvector) can be extended by adding new connectors or backends.
- **Embedding defaults to offline:** the default embedder is `none`; use `--embedder hash` for deterministic, key-free indexing. **Cloud providers (OpenAI, Gemini) are opt-in only**—use them only when you explicitly set `--embedder openai` or `--embedder gemini` and provide the corresponding API key.
- **Compile** uses an offline LLM backend by default; no API keys are required for the standard ingest → compile → verify path. Examples in this guide use offline or generic options; cloud-backed options are explicitly marked as optional.

### Embedding providers

- **Offline (recommended for tests/demos):** `--embedder hash` (deterministic, no API keys) or default `none`
- **OpenAI-compatible (optional):** `--embedder openai` with `AKC_OPENAI_API_KEY` (and optional `AKC_OPENAI_BASE_URL`, `AKC_OPENAI_EMBED_MODEL`)
- **Gemini (optional):** `--embedder gemini` with `AKC_GEMINI_API_KEY` (and optional `AKC_GEMINI_BASE_URL`, `AKC_GEMINI_EMBED_MODEL`)

### Ingest local docs

```bash
uv run akc ingest \
  --tenant-id tenant-1 \
  --connector docs \
  --input ./docs \
  --no-index
```

Run a fully-offline ingest + index + query:

```bash
uv run akc ingest \
  --tenant-id tenant-1 \
  --connector docs \
  --input ./docs \
  --embedder hash \
  --index-backend memory \
  --query "getting started" \
  -k 3
```

### Ingest an OpenAPI spec

```bash
uv run akc ingest \
  --tenant-id tenant-1 \
  --connector openapi \
  --input ./examples/openapi/petstore.json \
  --no-index
```

### Ingest Slack threads (Q&A heuristic)

Set a token (bot or user token with access to the channel):

```bash
export AKC_SLACK_TOKEN="xoxb-..."
```

Then run ingestion with `--input` set to the channel id (e.g. `C123...`):

```bash
uv run akc ingest \
  --tenant-id tenant-1 \
  --connector slack \
  --input C12345678 \
  --no-index
```

### Compile (Plan → Retrieve → Generate → Execute → Repair)

Run the compile loop for a tenant/repo. All outputs go under `<outputs-root>/<tenant-id>/<repo-id>/` (manifest, `.akc/tests`, code memory).

```bash
uv run akc compile \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out
```

Optional: `--mode quick` (default) or `--mode thorough`, `--goal "Your goal"`, `--work-root` to override the executor work directory.

### Verify

Check emitted artifacts (tests, verifier results) for a tenant/repo:

```bash
uv run akc verify \
  --tenant-id my-tenant \
  --repo-id my-repo \
  --outputs-root ./out
```

## End-to-end run (ingest → compile → verify)

1. **Ingest** docs (offline, no index):  
   `uv run akc ingest --tenant-id my-tenant --connector docs --input ./docs --embedder hash --no-index`

2. **Compile**:  
   `uv run akc compile --tenant-id my-tenant --repo-id my-repo --outputs-root ./out`

3. **Verify**:  
   `uv run akc verify --tenant-id my-tenant --repo-id my-repo --outputs-root ./out`

Outputs and tests live under `./out/my-tenant/my-repo/` (e.g. `manifest.json`, `.akc/tests/`, `.akc/memory.sqlite`). The CLI uses an offline LLM backend by default so no API keys are required for this path.

## Project structure

- `src/akc/` — Core package (ingest, memory, compile, outputs)
- `docs/` — Architecture and research docs
- `tests/` — Unit and integration tests
- `examples/` — Example inputs and expected outputs

## Next steps

- Read [architecture.md](architecture.md) for the pipeline and components.
- Read [research.md](research.md) for the research behind the design.
- See [CONTRIBUTING.md](../CONTRIBUTING.md) to run tests and open a PR.
