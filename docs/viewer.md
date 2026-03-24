# AKC Viewer

## Summary

AKC includes an **optional, local-first, read-only viewer** that renders:

- plan progress from `.akc/plan` (or the scoped sqlite memory store when present), and
- emitted evidence artifacts referenced by `manifest.json` (e.g. `.akc/tests/*`, `.akc/verification/*`, `.akc/design/*`, `.akc/orchestration/*`, `.akc/agents/*`, `.akc/deployment/*`, and generated `.github/workflows/akc_deploy_*.yml`).

The viewer is intentionally thin and **does not execute** anything.

## Trust boundary (non-negotiable)

The viewer is a read-only consumer of artifacts:

- It **never** runs commands, applies patches, imports dynamic modules, or invokes the executor/compile loop.
- It treats plan state and artifacts as **untrusted input** and only renders/copies them.
- It is schema-driven against `docs/artifact-contracts.md` and must ignore unknown additive fields.

See `docs/viewer-trust-boundary.md`.

### Intent authority in manifests and replay sidecars

When rendering run manifests or `.akc/run/*.replay_decisions.json`, treat **intent** as the contract boundary: **`stable_intent_sha256`** ties the run to the normalized intent artifact, and each replay decision’s **`inputs_snapshot`** may record that hash plus **`intent_mandatory_partial_replay_passes`** (passes required under `partial_replay` because of success-criterion evaluation modes). When the manifest and current run both carry a stable hash, a mismatch forces a full pass invalidation path (see **`intent_stable_changed`** in **`recompile_triggers`**). Behavior is specified in **`docs/akc-alignment.md`** under *Intent authority and replay*.

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
