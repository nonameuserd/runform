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

Phase 1 provides `akc ingest` for indexing docs, OpenAPI specs, and (optionally) Slack threads:

```bash
uv run akc
```

### Embedding providers

- **Offline (recommended for tests/demos):** `--embedder hash` (deterministic, no API keys)
- **OpenAI-compatible:** `--embedder openai` with `AKC_OPENAI_API_KEY` (and optional `AKC_OPENAI_BASE_URL`, `AKC_OPENAI_EMBED_MODEL`)
- **Gemini:** `--embedder gemini` with `AKC_GEMINI_API_KEY` (and optional `AKC_GEMINI_BASE_URL`, `AKC_GEMINI_EMBED_MODEL`)

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

When the compile loop is implemented (Phase 3), the goal is:

```bash
uv run akc compile --input ./docs
```

## Project structure

- `src/akc/` — Core package (ingest, memory, compile, outputs)
- `docs/` — Architecture and research docs
- `tests/` — Unit and integration tests
- `examples/` — Example inputs and expected outputs

## Next steps

- Read [architecture.md](architecture.md) for the pipeline and components.
- Read [research.md](research.md) for the research behind the design.
- See [CONTRIBUTING.md](../CONTRIBUTING.md) to run tests and open a PR.
