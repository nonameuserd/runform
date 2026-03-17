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

Phase 0 provides a minimal CLI that prints version and usage:

```bash
uv run akc
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
