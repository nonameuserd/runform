# AKC Viewer

## Summary

AKC includes an **optional, local-first, read-only viewer** that renders:

- plan progress from `.akc/plan` (or the scoped sqlite memory store when present), and
- emitted evidence artifacts referenced by `manifest.json` (e.g. `.akc/tests/*`, `.akc/verification/*`).

The viewer is intentionally thin and **does not execute** anything.

## Trust boundary (non-negotiable)

The viewer is a read-only consumer of artifacts:

- It **never** runs commands, applies patches, imports dynamic modules, or invokes the executor/compile loop.
- It treats plan state and artifacts as **untrusted input** and only renders/copies them.

See `docs/viewer-trust-boundary.md`.

## CLI usage

All viewer modes require the tenant/repo scope and an outputs root:

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs tui
```

### TUI (terminal)

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs tui
```

- Uses a curses UI to navigate steps and evidence paths.
- Press `v` to preview a text file (read-only).

### Static web viewer (generated HTML)

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs web --out-dir ./viewer
open ./viewer/index.html
```

This generates a folder containing:

- `index.html`
- `data/plan.json` (snapshot)
- `data/manifest.json` (when present)
- `files/**` (copied evidence files for local open/download UX)

### Export bundle (for later inspection)

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs export --out-dir ./evidence-bundle
```

This generates a portable bundle directory plus a `.zip` alongside it:

- `data/plan.json`
- `data/manifest.json` (when present)
- `files/**` (copied evidence files referenced by the manifest)
