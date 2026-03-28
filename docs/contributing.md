# Contributing (docs)

For the full contribution guide, see **[CONTRIBUTING.md](https://github.com/nonameuserd/runform/blob/main/CONTRIBUTING.md)** in the repository root.

Quick checklist before opening a PR:

- Run `uv run ruff check .` and `uv run ruff format --check .`
- Run `uv run mypy src/akc`
- Run `uv run pytest`
- Install pre-commit: `uv run pre-commit install`

Also see [CODE_OF_CONDUCT.md](https://github.com/nonameuserd/runform/blob/main/CODE_OF_CONDUCT.md) and [SECURITY.md](https://github.com/nonameuserd/runform/blob/main/SECURITY.md).

## Documentation site (local preview)

To build the published docs site locally:

```bash
uv sync --extra docs
uv run mkdocs serve
```

Then open the URL MkDocs prints (usually `http://127.0.0.1:8000/`). A maintainer must set **Settings → Pages → Build and deployment** to **GitHub Actions** so pushes to `main` deploy the site — see `.github/workflows/docs.yml` (deploy) and `.github/workflows/docs-verify.yml` (strict `mkdocs build` on pull requests).
