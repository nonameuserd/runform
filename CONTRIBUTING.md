# Contributing to the Agentic Knowledge Compiler

Thank you for your interest in contributing. This document explains how to get set up, run checks, and submit changes.

## License and legal

- This project is licensed under the **Apache License 2.0**. By contributing, you agree that your contributions will be licensed under the same license.
- We use the **Developer Certificate of Origin (DCO)**. By submitting a pull request, you certify that your contribution is your original work (or that you have the right to submit it under the Apache-2.0 license) and that you agree to the [Developer Certificate of Origin, v1.1](https://developercertificate.org/).
- Please read [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) and [SECURITY.md](SECURITY.md).

## Getting started

1. **Fork and clone** the repository.
2. **Install the project** with [uv](https://docs.astral.sh/uv/):
   ```bash
   uv sync
   uv sync --extra dev   # include dev dependencies (pytest, ruff, mypy, pre-commit)
   ```
3. **Install pre-commit hooks** (recommended):
   ```bash
   uv run pre-commit install
   ```

## Branching and workflow

- Create a **branch** from `main` for your change (e.g. `feature/docs-connector`, `fix/ruff-config`).
- Keep changes focused; prefer several small PRs over one large one.
- Open a **pull request** when ready. Fill in the PR template and link any related issues.

## Running tests and checks

Before opening a PR, ensure:

| Check        | Command |
|-------------|---------|
| Lint        | `uv run ruff check .` |
| Format      | `uv run ruff format --check .` (or `ruff format .` to fix) |
| Type check  | `uv run mypy src/akc` |
| Tests       | `uv run pytest` |

One-shot before pushing:

```bash
uv run ruff check . && uv run ruff format --check . && uv run mypy src/akc && uv run pytest
```

CI runs the same checks on every PR.

### Manual check: `akc view … tui`

Automated tests cover **pure helpers** only (scroll math, filters, preflight). After changing `src/akc/viewer/tui.py`, smoke the real TUI locally:

1. Run `uv run akc view --tenant-id … --repo-id … --outputs-root … tui` in a normal terminal (not piped).
2. Confirm fixed **header** stays visible while scrolling long **knowledge** or **profile** text.
3. In **evidence** mode, confirm **split preview** updates as you move `↑`/`↓`, and `v` still opens fullscreen preview.
4. Press `/`, type a substring, Enter — confirm **n** / **N** jump between matching steps.
5. Resize to a very short window or use `TERM=dumb` and confirm the CLI **skips curses** with a clear message and prints the text fallback.

## Pull request process

1. Update tests and docs as needed for your change.
2. Ensure all CI checks pass.
3. Request review from maintainers (see [GOVERNANCE.md](GOVERNANCE.md)).
4. Address review feedback. Maintainers may squash-merge when the PR is approved.

## Documentation

- User and contributor docs live in `docs/`. See [docs/architecture.md](docs/architecture.md) and [docs/research.md](docs/research.md) for design and research context.
- Keep docstrings and type hints accurate for public APIs under `src/akc/`.

## Questions

- Open a [GitHub Discussion](https://github.com/your-org/agentic-knowledge-compiler/discussions) for questions and ideas.
- For security-sensitive issues, use the process in [SECURITY.md](SECURITY.md) instead of public issues.
