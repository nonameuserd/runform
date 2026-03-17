# Agentic Knowledge Compiler (AKC)

**Turn messy real-world information into executable knowledge.**

The Agentic Knowledge Compiler takes heterogeneous inputs—documents, messaging threads, API specs—and compiles them into runnable artifacts: code, workflows, and agent specifications. It emphasizes **compilation** (not just summarization), **living alignment** to sources, and **correctness-aware** design (tests, repair loops, optional verification).

## What’s the problem?

- Real-world knowledge lives in docs, Slack, APIs, and tickets—scattered and unstructured.
- One-shot codegen produces brittle outputs that drift from sources.
- Agent systems often behave as black boxes with no clear correctness story.

## What does AKC do?

| Others do                | AKC does                                                                 |
| ------------------------ | ------------------------------------------------------------------------ |
| Summarization (RAG, Q&A) | **Compilation** → runnable code, workflows, and agent specs                |
| One-shot codegen         | **Living systems** that stay aligned to sources via re-ingestion and repair |
| Black-box agents         | **Correctness-aware** design: tests, synthesize–execute–repair, optional verification |

## Quick start

**Requirements:** Python 3.11+, [uv](https://docs.astral.sh/uv/) (recommended) or pip.

```bash
# Clone and install
git clone https://github.com/your-org/agentic-knowledge-compiler.git
cd agentic-knowledge-compiler
uv sync
uv sync --extra dev   # for development (pytest, ruff, mypy)

# Run the CLI (stub in Phase 0; full compile in Phase 3)
uv run akc
```

**Goal (once the compile loop exists):**  
`uv sync && uv run akc compile --input ./docs`

## Project layout

```
src/akc/           # Core package
├── ingest/        # Connectors, chunking, indexing
├── memory/        # Code memory, plan state, optional KG
├── compile/       # Plan → Retrieve → Generate → Execute → Repair
└── outputs/       # Code, workflows, agent specs
docs/              # Architecture, research, getting started
tests/unit/        # Unit tests
tests/integration/ # Integration tests
examples/          # Example inputs → outputs
```

## Documentation

- [**Architecture**](docs/architecture.md) — High-level flow and component roles
- [**Research**](docs/research.md) — DeepCode, ARCS, DocAgent, ReAct, ActMem, and related work
- [**Getting started**](docs/getting-started.md) — Setup and first steps
- [**Contributing**](CONTRIBUTING.md) — How to contribute; link to [Code of Conduct](CODE_OF_CONDUCT.md) and [Security](SECURITY.md)

## Development and CI

- **Lint:** `uv run ruff check .`
- **Format:** `uv run ruff format .`
- **Types:** `uv run mypy src/akc`
- **Tests:** `uv run pytest`

CI runs these on every push and PR. Pre-commit hooks: `uv run pre-commit install`.

## License and governance

- **License:** [Apache-2.0](LICENSE)
- **Governance:** [GOVERNANCE.md](GOVERNANCE.md) — maintainers, decision process, scope
- **Security:** [SECURITY.md](SECURITY.md) — how to report vulnerabilities

## Roadmap

- **Phase 0** — Project bootstrap ✅ (repo, OSS docs, CI, README, architecture & research docs)
- **Phase 1** — Ingestion (docs + API connectors, chunking, index)
- **Phase 2** — Memory and planning (code memory, plan state)
- **Phase 3** — Compile loop (Plan → Retrieve → Generate → Execute → Repair)
- **Phase 4** — Outputs and “living” (code, workflows, agents; re-ingest and drift checks)
- **Phase 5** — Correctness (tests by default; optional formal verification)

See the [plan](.cursor/plans/agentic_knowledge_compiler_oss_656c38f4.plan.md) and [CHANGELOG](CHANGELOG.md) for details.
