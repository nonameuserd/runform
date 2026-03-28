# Environment Model

AKC currently uses two closely related environment vocabularies in code:

- an operator-facing **runtime/autopilot profile**: `dev`, `staging`, `prod`
- a user-facing **delivery/deployment model** emitted by compile: `local`, `staging`, `production`

This page documents how those fit together and where they show up.

## Operator-facing profiles

Operator workflows use `dev`, `staging`, and `prod` as the safety profile for runtime and unattended automation.

Primary entry point:

- `akc runtime autopilot --env-profile dev|staging|prod`

Current behavior:


| Profile   | Intended use                         | Default drift/check cadence | Automation budget shape |
| --------- | ------------------------------------ | --------------------------- | ----------------------- |
| `dev`     | Local development and fast iteration | fastest                     | loosest                 |
| `staging` | Pre-production validation            | medium                      | moderate                |
| `prod`    | Production control loops             | slowest                     | strictest               |


The defaults are implemented in `src/akc/living/unattended_defaults.py`:

- `dev`: 5 minute checks, highest mutation and rollback budgets
- `staging`: 10 minute checks, reduced budgets
- `prod`: 60 minute checks, tightest budgets and escalation thresholds

This profile is operator-facing: it changes how aggressively AKC will re-check, mutate, and escalate in unattended runtime flows.

## User-facing delivery environments

Compile emits a delivery/deployment environment model with `local`, `staging`, and `production`.

Primary source:

- `src/akc/compile/delivery_projection.py`

That model is written into `.akc/deployment/<run_id>.delivery_plan.json` under:

- `environments`
- `environment_model`
- `promotion_readiness`
- `required_human_inputs`

Current environment model:


| Environment  | Preferred runtime       | Preferred delivery path | Human approval | Readiness checks |
| ------------ | ----------------------- | ----------------------- | -------------- | ---------------- |
| `local`      | `docker_compose`        | `direct_apply`          | no             | no               |
| `staging`    | `kubernetes_or_compose` | `direct_apply`          | no             | yes              |
| `production` | `kubernetes`            | `workflow_handoff`      | yes            | yes              |


This is the user-facing or deployment-facing view of the system: where software is expected to run, how it should be handed off, and which gates apply before exposing changes more broadly.

## How the two vocabularies relate

They are intentionally similar but not identical:

- `dev` is the operator/autopilot profile
- `local` is the compile-time deployment environment
- `prod` is the operator/autopilot profile
- `production` is the compile-time deployment environment

In practice:

- operator controls use `dev|staging|prod`
- emitted delivery plans use `local|staging|production`

The `staging` label is shared across both because it represents the same intermediate intent: pre-production validation before production promotion.

## Promotion defaults

Promotion defaults are environment-sensitive.

Source:

- `src/akc/promotion.py`

Current default behavior:

- if `AKC_ENV` or `ENVIRONMENT` is `dev`, `local`, or `development`, default promotion mode is `artifact_only`
- if `AKC_ENV` or `ENVIRONMENT` is set to any other non-empty value, default promotion mode is `staged_apply`
- if no env is set, non-`dev` sandbox modes default to `staged_apply`
- otherwise, default promotion mode is `artifact_only`

This is why local/demo flows stay conservative by default while governed non-dev environments move toward staged promotion.

## Runtime delivery lane mapping

Runtime lifecycle tracking uses `staging` and `production` delivery lanes.

Primary entry point:

- `akc runtime start --delivery-target-lane staging|production`

Source:

- `src/akc/run/delivery_lifecycle.py`

Normalization rules:

- `production` and `prod` map to `production`
- `staging`, `local`, `dev`, and `development` map to `staging`

That mapping controls how runtime health timestamps are projected into delivery lifecycle fields such as `staging_healthy_at` and `prod_healthy_at`.

## Where this shows up in docs and artifacts

- onboarding and safe demos usually describe the `local` / `artifact_only` path
- runtime/autopilot docs and flags use `dev|staging|prod`
- delivery plans and promotion readiness artifacts use `local|staging|production`
- operator review and approval flows become stricter as execution moves toward `production`

## Source of truth

The current source-of-truth files are:

- `src/akc/compile/delivery_projection.py`
- `src/akc/living/unattended_defaults.py`
- `src/akc/promotion.py`
- `src/akc/run/delivery_lifecycle.py`
- `src/akc/cli/__init__.py`

