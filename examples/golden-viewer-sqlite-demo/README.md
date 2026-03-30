# Golden viewer demo (sqlite plan-state fallback)

This example is a **checked-in snapshot** that exercises the viewer’s sqlite fallback path:

- There is **no** `.akc/plan/...` directory.
- Plan state lives at `out/demo/sqlite-repo/.akc/memory.sqlite`.
- Artifacts live under `out/demo/sqlite-repo/` and are indexed via `manifest.json`.

Try it:

```bash
cd examples/golden-viewer-sqlite-demo
akc view tui
# or
akc view web --out-dir ./.akc/viewer/web-demo
```

