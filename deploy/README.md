# Autopilot deployment references

These files are **minimal, copy-paste starting points** for running `akc runtime autopilot` as a long-lived process. Adjust paths, images, and secrets for your environment.

| Artifact | Use case |
|----------|----------|
| [`systemd/akc-runtime-autopilot.service`](systemd/akc-runtime-autopilot.service) | Single VM / bare metal; filesystem lease; `systemd` restarts on exit. |
| [`compose/docker-compose.autopilot.yml`](compose/docker-compose.autopilot.yml) | One container per scope (or shared host path); healthcheck = process liveness. Mounts [`compose/ingest_state.example.json`](compose/ingest_state.example.json) as a stand-in ingest file. |
| [`kubernetes/autopilot-deployment.yaml`](kubernetes/autopilot-deployment.yaml) | `replicas: 1` by default; optional `coordination.k8s.io` Lease when using `--lease-backend k8s`. |
| [`Dockerfile.autopilot`](Dockerfile.autopilot) | Optional image that installs the `akc` CLI and `kubectl` (for Kubernetes lease). |
| [`ci/github-actions-living-recompile.example.yml`](ci/github-actions-living-recompile.example.yml) | Example CI job: path-filtered `akc living-recompile` for lower latency than autopilot-only polling. |

## External triggers (optional; lower latency)

Autopilot is interval-driven; fleet **outbound** webhooks (`src/akc/control/fleet_webhooks.py`) notify subscribers but do not run compiles. For faster turnaround:

1. **CI** — Use the example workflow above (or equivalent) so pushes touching `.akc/living/*.json` or ingest/trigger paths run a **one-shot** `akc living-recompile`, which uses the same `safe_recompile_on_drift` path as the autopilot loop.
2. **Inbound webhook** — Run `akc living-webhook-serve` behind TLS and a reverse proxy. Configure the **same** shared secret in fleet webhook config and `AKC_LIVING_WEBHOOK_SECRET`. POST bodies must match `akc.fleet.webhook_delivery.v1` with `X-AKC-Signature: v1=<hex>` over the raw bytes (see `sign_webhook_body` in `fleet_webhooks.py`). Endpoints: `POST /v1/trigger` or `POST /`, `GET /health`.

Tenant isolation: use `--tenant-allowlist` (or `AKC_LIVING_WEBHOOK_TENANT_ALLOWLIST`) so each receiver only runs scopes you expect.

## Single-writer semantics

Autopilot acquires a **per-scope lease** before mutating work (`src/akc/runtime/autopilot.py`):

- **`filesystem`**: `fcntl` locks on `<outputs>/<tenant>/<repo>/.akc/autopilot/leases/`. This coordinates processes on **one host** (or multiple processes sharing the same mount with working flock semantics).
- **`k8s`**: Kubernetes `Lease` objects in `--lease-namespace` coordinate leadership **across pods**.

Running **multiple replicas** against the same tenant/repo without a shared filesystem lock or a Kubernetes Lease namespace is unsafe. Use `akc living doctor` (with `--lease-backend` / `--expect-replicas` or `AKC_AUTOPILOT_*` env vars) to surface warnings.

See **Autopilot deployment (single-writer)** in `docs/runtime-execution.md`.

## Environment variables (typical)

| Variable | Purpose |
|----------|---------|
| `AKC_LIVING_AUTOMATION_PROFILE` | Set to `living_loop_unattended_v1` for unattended defaults path (`--unattended-defaults`). |
| `AKC_OUTPUTS_ROOT` | Root containing `<tenant>/<repo>/` scopes. |
| `AKC_INGEST_STATE_PATH` or `AKC_AUTOPILOT_INGEST_STATE` | Drift checks / ingest state JSON. |
| `AKC_TENANT_ID` / `AKC_REPO_ID` | Optional scope hints. |
| `AKC_AUTOPILOT_LEASE_BACKEND` | `filesystem` (default) or `k8s`. |
| `AKC_AUTOPILOT_LEASE_NAMESPACE` | Required when using `k8s` lease backend. |
| `AKC_AUTOPILOT_EXPECT_REPLICAS` | For validation warnings (default `1`). |

Pinned CLI flags (budgets or `--unattended-defaults`) and resource limits belong in the unit / compose / Deployment spec.
