# Agentic Knowledge Compiler (AKC)

**Compile documents, messaging, and APIs into runnable artifacts**—code, workflows, and agent specs—with retrieval, tests, and repair loops instead of one-off summarization.

## This repository

This is the source tree for the AKC Python package (`akc`): ingestion connectors, memory/plan state, the compile loop (plan → retrieve → generate → execute → repair), and artifact outputs. Design goals are **tenant-scoped isolation**, **tests-by-default** policy gates, and optional verification. Deeper architecture, IR, and runtime behavior live in the docs below.

## Quick start

**Requirements:** Python 3.11+, [uv](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/nonameuserd/runform.git
cd runform
uv sync
uv sync --extra dev   # development: pytest, ruff, mypy, …

uv run akc --help
```

Bootstrap a project (creates `.akc/project.json` and optional local policy stub):

```bash
uv run akc init
```

Then follow **[Getting started](docs/getting-started.md)** for ingest → compile → verify, scope resolution (`CLI` → `AKC_*` → `.akc/project.json`), and deployment notes.

## Documentation

* [Getting started](docs/getting-started.md) — install, first run, configuration
* [Architecture](docs/architecture.md) — components and data flow
* [Governance](GOVERNANCE.md) — maintainers and decisions
* [Deploy](deploy/README.md) — Docker, Compose, Kubernetes examples

## Contributing

Issues and pull requests are welcome. See **[CONTRIBUTING.md](CONTRIBUTING.md)** for workflow, **[CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)** for community expectations, and **[SECURITY.md](SECURITY.md)** for reporting vulnerabilities.

**Local checks:** `uv run ruff check .`, `uv run ruff format .`, `uv run mypy src/akc`, `uv run pytest`.

## License

Licensed under the [Apache-2.0](LICENSE) license.
