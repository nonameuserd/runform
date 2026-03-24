# Operator dashboard (read-only)

This bundle is a **static** single-page UI for the fleet operations catalog. It uses **GET** requests only (`/health`, `/v1/runs`, `/v1/runs/{tenant}/{repo}/{run_id}`). It does not run compile, runtime, or `akc view`.

## Run locally

1. Start the fleet API (example):

   ```bash
   akc fleet serve --config ./fleet.json --host 127.0.0.1 --port 8765
   ```

2. Start a static server for this directory (serves `index.html` and `app.js`):

   ```bash
   akc fleet dashboard-serve --host 127.0.0.1 --port 9090
   ```

3. Open `http://127.0.0.1:9090/` in a browser. Enter the fleet base URL (e.g. `http://127.0.0.1:8765`), a **read-scoped** bearer token, and a tenant id, then load runs.

## Cross-origin (different ports or hosts)

Browsers block cross-origin `fetch` unless the fleet API returns CORS headers. Set:

```bash
export AKC_FLEET_CORS_ALLOW_ORIGIN=http://127.0.0.1:9090
```

to the **exact** origin of the dashboard (scheme + host + port). Restart `akc fleet serve` after changing it.

## Local bundles and exports

The API does **not** return artifact bytes. For evidence, operators use paths shown in the UI (`outputs_root` + `manifest_rel_path` and sidecar `rel_path` values) on the workstation, and optional `akc view export` zips. HTTP label writes use `POST …/labels` with a `runs:label` token (out of scope for this UI).
