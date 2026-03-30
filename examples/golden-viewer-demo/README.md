# Golden viewer demo (inspectable artifacts)

This directory is a **checked-in snapshot** of AKC plan state plus a scoped `manifest.json` and a few evidence files. Use it to try `akc view` without running ingest, compile, or any LLM.

## Try it

From this directory (so `.akc/project.json` is picked up):

```bash
cd examples/golden-viewer-demo
akc view tui
```

Static HTML bundle (opens in a browser if you pass `--serve`, or open `index.html` from the output folder):

```bash
akc view web --out-dir ./.akc/viewer/web-demo
```

Portable zip + dir (for sharing):

```bash
akc view export --out-dir ./.akc/viewer/export-demo
```

## Layout

| Path | Role |
|------|------|
| `.akc/project.json` | `tenant_id`, `repo_id`, and `outputs_root` (`out`) |
| `.akc/plan/...` | Active plan JSON (what the viewer shows as the plan) |
| `out/demo/golden-repo/manifest.json` | Bundle manifest listing artifact paths and hashes |
| `out/demo/golden-repo/.akc/...` | Sample diff + test-stage files linked to plan steps |

Repo-root `out/` is gitignored for normal work; this example keeps its outputs under **`examples/golden-viewer-demo/out/`** so the golden tree can be committed.
