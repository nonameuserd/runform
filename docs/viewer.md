# AKC Viewer

AKC includes a local-first, read-only viewer exposed through `akc view`.

Current subcommands:

- `tui`
- `web`
- `export`

## What the viewer reads

The viewer assembles a snapshot from local scoped data only.

Primary sources:

- plan state under `.akc/plan/...`
- scoped SQLite-backed plan state and memory where available
- `<outputs_root>/<tenant>/<repo>/manifest.json`
- manifest-referenced evidence files
- optional knowledge and operator-summary artifacts already present on disk

Relevant code:

- `src/akc/cli/view.py`
- `src/akc/viewer/snapshot.py`
- `src/akc/viewer/tui.py`
- `src/akc/viewer/web.py`
- `src/akc/viewer/export.py`
- `src/akc/viewer/control_panels.py`

## Trust boundary

The viewer does not execute compile, runtime, or tool actions.

It is a read-only inspection surface:

- no patch apply
- no command execution
- no secret access on behalf of artifacts
- no dynamic module loading from artifact content

See [viewer-trust-boundary.md](viewer-trust-boundary.md) for the full boundary statement.

## CLI usage

All modes require:

- `--tenant-id`
- `--repo-id`
- `--outputs-root`

Optional shared flag:

- `--plan-base-dir`

Base command shape:

```bash
akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/out <subcommand>
```

## TUI

Interactive terminal UI:

```bash
akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/out tui
```

Behavior notes:

- uses curses
- falls back to plain-text summary when the terminal cannot support the TUI

## Static web bundle

Generate a local static bundle:

```bash
akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/out web --out-dir ./viewer
```

Optional local-only serving:

```bash
akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/out web --out-dir ./viewer --serve
```

`--serve` binds to `127.0.0.1` only and is a developer convenience for browsing the generated bundle.

## Export bundle

Export a portable evidence bundle:

```bash
akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/out export --out-dir ./evidence
```

Notes:

- `--include-all-evidence` is on by default
- a `.zip` is created by default
- use `--no-zip` to skip zip creation

## Output layout

The generated web and export bundles include snapshot data plus copied evidence files under the bundle root.

Typical outputs include:

- `data/plan.json`
- `data/manifest.json` when present
- copied evidence files under `files/`

The exact contents depend on what exists under the scoped outputs tree.

## When to use each mode

- `tui` for quick local inspection in a terminal
- `web` for richer local browsing and sharing a generated bundle directory
- `export` for preserving a portable evidence package

## Related docs

- [Viewer trust boundary](viewer-trust-boundary.md)
- [Artifact contracts](artifact-contracts.md)
- [Security](security.md)
