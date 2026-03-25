# AKC Viewer

## Summary

AKC includes an **optional, local-first, read-only viewer** (`akc view`, `src/akc/viewer/`) that renders:

- **Plan progress** from `<plan_base>/.akc/plan/...` (active pointer + plan JSON), or from the **scoped** `SQLitePlanStateStore` in `<outputs_root>/<tenant>/<repo>/.akc/memory.sqlite` when the JSON store is missing or unusable.
- **Emitted evidence** listed in `<outputs_root>/<tenant>/<repo>/manifest.json` (for example `.akc/tests/*`, `.akc/verification/*`, design/orchestration/coordination/deployment passes, generated workflows, and any other manifest paths).
- **Optional debug surfaces** (when present on disk): knowledge snapshot / mediation under `.akc/knowledge/`, conflict reports from code memory in `memory.sqlite`, and read-only **operator panel summaries** (forensics bundles under `.akc/viewer/forensics/`, playbook JSON under `<outputs_root>/<tenant>/.akc/control/playbooks/`, manifest-linked profile decisions, etc.).

The viewer is intentionally thin and **does not execute** anything. For HTTP-only operator discovery (merged index rows, no artifact bytes), see the fleet API and `docs/viewer-trust-boundary.md`.

## Trust boundary (non-negotiable)

The viewer is a read-only consumer of artifacts:

- It **never** runs commands, applies patches, imports dynamic modules, or invokes the executor or compile loop.
- It treats plan state and artifacts as **untrusted input** and only renders or copies them under path confinement.
- Loaded objects are checked against frozen JSON Schemas in **`src/akc/artifacts/validate.py`** (see **`docs/artifact-contracts.md`**). Validation is **tolerant**: issues are surfaced in synthetic fields (e.g. `metadata.viewer_schema_issues`) instead of failing the whole snapshot.

See **`docs/viewer-trust-boundary.md`** for the full threat model and fleet/control-plane boundaries.

### Intent authority in manifests and replay sidecars

When rendering run manifests or `.akc/run/*.replay_decisions.json`, treat **intent** as the contract boundary: **`stable_intent_sha256`** ties the run to the normalized intent artifact, and each replay decision’s **`inputs_snapshot`** may record that hash plus **`intent_mandatory_partial_replay_passes`** (passes required under `partial_replay` because of success-criterion evaluation modes). When the manifest and current run both carry a stable hash, a mismatch forces a full pass invalidation path (see **`intent_stable_changed`** in **`recompile_triggers`**). Behavior is specified in **`docs/akc-alignment.md`** under *Intent authority and replay*.

## CLI usage

All modes require tenant/repo scope and **`--outputs-root`** (the directory that contains `<tenant>/<repo>/manifest.json` and scoped `.akc/*`).

Optional flags:

- **`--plan-base-dir`** — Directory that contains `.akc/plan` (default: current working directory). The viewer never writes here.
- **`--schema-version`** — Schema version passed to manifest/plan validation (default: `1`).

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs tui
```

### TUI (terminal)

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs tui
```

Uses **stdlib curses** only. If the terminal is too small, `TERM=dumb`, or stdin/stdout is not a TTY, the CLI prints a short message and falls back to a plain-text summary (same as when curses fails to initialize).

| Keys | Action |
|------|--------|
| `↑` / `↓` | Move selection (steps or evidence list) or scroll the body (knowledge / profile) |
| `Enter` | Open **evidence** view for the selected step |
| `b` | Back to **steps** from evidence, knowledge, or profile |
| `h` / `l` or `[` / `]` | Cycle modes: steps → evidence → knowledge → profile |
| `/` | Edit a **substring filter** on step titles (Enter apply, Esc cancel) |
| `n` / `N` | Next / previous step matching the current filter |
| `o` / `p` | Jump to **knowledge** / **profile** |
| `v` | **Fullscreen** text preview of the selected evidence file (then any key except arrows/PgUp/PgDn to close; arrows scroll) |
| `,` / `.` | Scroll the **split** evidence preview pane |
| `PgUp` / `PgDn` | Page scroll (body, preview, or fullscreen preview) |
| `q` / `Esc` | Quit |

In **evidence** mode, the pane is split (~40% list, ~60% live preview) so you can scan files without blocking on a separate preview screen. Status badges use color when the terminal supports it.

### Static web viewer (generated HTML)

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs web --out-dir ./viewer
open ./viewer/index.html
```

To serve the same bundle over HTTP on **127.0.0.1** only (optional; useful when the browser blocks `fetch` on `file://`), add **`--serve`** and optionally **`--port`** (default is an ephemeral port):

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs web --out-dir ./viewer --serve
```

Security and scope are documented in **`docs/viewer-trust-boundary.md`** (*Local HTTP serve*).

If you omit **`--out-dir`**, the CLI writes under  
`<outputs_root>/<tenant>/<repo>/.akc/viewer/web/<timestamp>/`.

The generated folder includes:

- `index.html`
- `data/plan.json` — plan snapshot
- `data/manifest.json` — when a manifest exists in the scoped outputs dir
- `data/knowledge_obs.json` — derived observation payload from knowledge + mediation + conflict reports (empty/minimal when sources are absent)
- `data/operator_panels.json` — read-only summaries for forensics, playbook, autopilot, profile (null sections when absent)
- `files/**` — copied manifest evidence paths plus `.akc/knowledge/snapshot.json`, `snapshot.fingerprint.json`, and `mediation.json` when present (for local open/download UX)

### Export bundle (for later inspection)

```bash
uv run akc view --tenant-id TENANT --repo-id REPO --outputs-root /path/to/outputs export --out-dir ./evidence-bundle
```

Default output directory when **`--out-dir`** is omitted:  
`<outputs_root>/<tenant>/<repo>/.akc/viewer/export/<timestamp>/`.

This writes a portable directory and, by default, a **`.zip`** sibling (disable with **`--no-zip`**). Contents:

- `data/plan.json`, `data/manifest.json` (when present), `data/knowledge_obs.json`, `data/operator_panels.json`
- `files/**` — manifest-referenced evidence (see **`--include-all-evidence`**, default on) and the same optional `.akc/knowledge/*` files as the web bundle

## See also

- **`docs/viewer-trust-boundary.md`** — full trust boundary, fleet HTTP, automation plane
- **`tests/unit/test_viewer_export.py`** — export and path-confinement tests
