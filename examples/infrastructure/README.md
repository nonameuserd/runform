# Infrastructure examples (full-stack, not API-only)

AKC **infrastructure synthesis** (`infrastructure_synthesis` artifact pass) walks **every row** in the compile **delivery plan** `targets` array. That includes:

| `target_class` (illustrative) | Role |
| --- | --- |
| `backend_service` | APIs and similar services |
| `web_app` | Browser-facing apps (SPA/SSR hosts, static+CDN workloads in delivery semantics) |
| `mobile_client` | Store-distributed clients (iOS / Android); delivery paths differ from GitOps-style web |
| `worker` | Queue/worker runtimes (same container registry + logging pattern in IaC manifests) |
| `integration` | Integration services |
| `infrastructure_component` | Shared data plane (e.g. Redis, Postgres, S3) when the node name matches supported kinds |

Public targets (`exposure_model.public: true`) with a **`domain`** get Route53 + ACM-style resource rows in the synthesized plan; private targets (typical for workers or mobile build artifacts) omit the public DNS/TLS pair while still receiving per-target registry, logs, and IRSA placeholders.

## Checked-in shape

[`delivery_plan.fullstack.example.json`](./delivery_plan.fullstack.example.json) is a **schema-valid** `delivery_plan` (v1) with:

- **API** — `backend_service`, public, secret contract
- **Web** — `web_app`, public, depends on API
- **Mobile** — `mobile_client`, private exposure (store pipeline handoff)
- **Cache** — `infrastructure_component` named “redis cache” (recognized by synthesis)

## Golden no-run IaC snapshot

[`golden-iac-demo/`](./golden-iac-demo/) is a checked-in snapshot of what infrastructure synthesis emits:

- `.akc/infra/run-demo.infra_plan.json`
- `.akc/infra/run-demo.iac_manifest.json`
- `.akc/infra/run-demo/terraform/*` (Terraform JSON)
- `.akc/infra/run-demo/aws-cdk/*` (AWS CDK TypeScript)

Real runs embed an equivalent `targets` list inside artifacts emitted under `.akc/deployment/` after compile; this file is a standalone reference for operators and docs.

## Related commands

- `akc compile` … produces `.akc/infra/<run_id>.infra_plan.json` and `.akc/infra/<run_id>.iac_manifest.json` when delivery context triggers synthesis (see `docs/cli-commands.md`, **provision**).
- `akc provision plan|apply` consumes those files.

## Tenant isolation

`tenant_id` / `repo_id` in the plan scope all synthesized resources and state keys; do not mix tenants in one provisioning run.
