# Delivery Architecture

This document describes the current delivery architecture implemented in this repository.

AKC delivery is a **named-recipient control plane** built around `akc deliver` and tenant-scoped delivery artifacts under `.akc/delivery/<delivery_id>/`.

It sits **after** compile. `akc compile` may emit a `delivery_plan` and related manifest/runtime refs, but delivery orchestration, packaging, recipient tracking, distribution, and activation evidence are separate from the core compile controller loop.

## Overview

Delivery turns a plain-language request plus an explicit recipient list into a tracked delivery session.

Current responsibilities:

- capture a delivery request, explicit recipients, platforms, release mode, and logical delivery version
- persist request/session/events/sidecars under `.akc/delivery/<delivery_id>/`
- optionally run `akc compile`, then bind compile outputs back onto the delivery request/session
- run packaging lanes for `web`, `ios`, and `android`
- run distribution lanes for beta and store delivery, with beta-first sequencing for `release_mode=both`
- collect provider-side and app-side activation evidence and roll it up into session state
- sync delivery summaries into the tenant operations index when project scoping matches the outputs layout

The delivery flow is intentionally **control-plane first**: state is explicit, artifacts are versioned, and every lifecycle transition is reflected in `session.json` and `events.json`.

## Current command surface

Primary CLI entry point:

- `akc deliver` with no subcommand performs the default **submit** action

Current subcommands:

- `status`
- `events`
- `resend`
- `promote`
- `gate-pass`
- `activation-report`
- `web-invite-open`

Important submit flags:

- `--project-dir`
- `--request`
- `--recipient`
- `--recipients-file`
- `--platforms`
- `--release-mode`
- `--delivery-version`
- `--compile`

Important behavior:

- recipients are **authoritative** and must come from `--recipient` and/or `--recipients-file`; they are not inferred from free text
- `--compile` runs `akc compile` from `--project-dir`, then binds compile handoff refs and runs packaging
- after successful packaging, delivery automatically runs the first distribution wave
- for `release_mode=both`, the automatic post-package wave is **beta only**; store promotion requires `gate-pass` and then `promote --lane store`

## Delivery layout

Per delivery session, AKC writes:

- `.akc/delivery/<delivery_id>/request.json`
- `.akc/delivery/<delivery_id>/session.json`
- `.akc/delivery/<delivery_id>/recipients.json`
- `.akc/delivery/<delivery_id>/events.json`
- `.akc/delivery/<delivery_id>/provider_state.json`
- `.akc/delivery/<delivery_id>/activation_evidence.json`

Related local-only operator input:

- `.akc/delivery/operator_prereqs.json`

Related compile handoff artifacts:

- `.akc/deployment/<run_id>.delivery_plan.json`
- `.akc/run/<run_id>.manifest.json`
- `.akc/runtime/<run_id>.runtime_bundle.json`

The request/session documents store only **refs and summaries** of compile outputs. Delivery does not copy the full compile artifacts into its own sidecars.

## Main modules

The delivery package lives under `src/akc/delivery/`.

| Module | Role |
| --- | --- |
| `store.py` | Creates sessions, persists JSON sidecars, updates pipeline/session state, records resend/gate/promotion/activation events |
| `orchestrate.py` | Runs `akc compile`, packaging, activation-client contract emission, and post-package distribution |
| `compile_handoff.py` | Loads compile manifest and `delivery_plan`, derives refs and non-secret metadata for delivery |
| `packaging_adapters.py` | Per-platform packaging lanes and packaging preflight |
| `adapters.py` | Distribution adapter definitions, lane scheduling, and strict prereq checks |
| `distribution_dispatch.py` | Executes beta/store distribution jobs and updates session/provider state |
| `provider_clients.py` | Optional provider API clients used when real provider execution is enabled |
| `activation.py` / `activation_contract.py` | Recipient activation recompute and generated activation client contract |
| `ingest.py` | Request parsing, recipients-file parsing, operator prereq loading, and local project probes |
| `metrics.py` | Derived delivery funnel metrics for `akc deliver status` and control-plane indexing |
| `control_index.py` | Sync helpers for the tenant operations index and control audit |
| `event_types.py` | Canonical event type strings |
| `versioning.py` | Deterministic provider/build version mapping from `delivery_version` |
| `invites.py` | Signed web invite URL construction and verification helpers |

CLI wiring lives in `src/akc/cli/deliver.py`.

## Session model

The delivery session is the operational center of the feature.

`session.json` tracks:

- `session_phase`
- `release_mode`
- selected `platforms`
- `compile_run_id`
- `delivery_version`
- `pipeline` stages: `compile`, `build`, `package`, `distribution`, `release`
- `per_platform` lane state for `beta` and `store`
- `per_recipient` delivery and activation state
- delivery-wide `activation_proof` rollups
- `store_release` state
- `human_readiness_gate`
- optional `distribution_plan` sequencing state for `release_mode=both`
- optional `compile_outputs_ref` after compile handoff is bound

Typical `session_phase` progression:

- `accepted`
- `blocked` when strict prereq preflight fails
- `building`
- `packaging`
- `distributing`
- `releasing`
- `failed`

For `release_mode=both`, the session also carries a distribution sequence plan:

- `beta_delivery`
- `human_readiness_gate`
- `store_promotion`

## Compile handoff

When `akc deliver --compile` succeeds, delivery loads compile outputs from the scoped project artifacts and stores a slim handoff summary on both `request.json` and `session.json`.

Current handoff fields include:

- `compile_run_id`
- manifest presence and relative path
- `delivery_plan` relative path and fingerprint
- promotion readiness from the `delivery_plan` when present
- runtime bundle relative path

Delivery also derives non-secret platform metadata from compile outputs, especially web distribution hints such as suggested base URLs.

This is a one-way handoff:

- compile remains the producer of `delivery_plan`, manifest, and runtime bundle artifacts
- delivery consumes those artifacts for packaging and distribution
- delivery does not extend the compile controller loop itself

## Packaging

Packaging runs after compile handoff is bound.

Current packaging lanes:

- `web_bundle`
- `ios_build`
- `android_build`

Current implementation status:

- packaging adapters are still **v1 stubs**
- they produce structured outputs and provider-version metadata
- strict packaging preflight is enforced by default for `store` and `both`
- `beta` packaging defaults to a more relaxed local-iteration posture unless overridden by `AKC_PACKAGING_ENFORCE_PREFLIGHT`

On successful packaging, AKC also writes:

- `.akc/delivery/<delivery_id>/activation_client_contract.v1.json`

This contract gives generated clients a stable way to report activation/heartbeat evidence back into the delivery control plane.

## Distribution

Distribution runs after packaging succeeds.

Current distribution lanes:

- web beta via signed invite URLs
- iOS beta via TestFlight
- Android beta via Firebase App Distribution
- iOS store via App Store Connect release flow
- Android store via Google Play release flow

Key sequencing rule:

- `release_mode=both` always schedules **beta delivery before store promotion**

That ordering is encoded in `src/akc/delivery/adapters.py` and mirrored into the session `distribution_plan`.

Current execution behavior:

- distribution adapter preflight is fail-closed by default
- prereqs are resolved from environment variables, `.akc/delivery/operator_prereqs.json`, and local repo probes
- provider execution is explicit; when real provider execution is not enabled, dispatch returns stub/dry-run shaped results rather than pretending a release happened
- per-platform per-lane outcomes are recorded in both `session.json` and `provider_state.json`

## Activation and recipient lifecycle

Delivery tracks activation at both the per-recipient and rollup levels.

Evidence sources today:

- app-side activation reports ingested through `akc deliver activation-report`
- signed web invite opens ingested through `akc deliver web-invite-open`
- provider-side install-detected style records stored as activation evidence

The activation subsystem recomputes:

- recipient `status`
- per-recipient `activation_proof`
- session-wide `activation_proof` rollups such as fully satisfied recipient counts

This is what powers delivery funnel metrics such as:

- request to first invite sent
- request to first active recipient
- invite acceptance rate
- install rate
- activation rate
- request to store live

## Events and audit

Delivery keeps an append-only event timeline in `events.json`.

Important current event types include:

- `delivery.request.accepted`
- `delivery.request.parsed`
- `delivery.preflight.completed`
- `delivery.compile.completed`
- `delivery.compile.outputs.bound`
- `delivery.build.packaged`
- `delivery.invite.sent`
- `delivery.invite.resend_requested`
- `delivery.provider.install_detected`
- `delivery.activation.first_run`
- `delivery.recipient.active`
- `delivery.store.promotion_requested`
- `delivery.store.submitted`
- `delivery.store.live`
- `delivery.human_gate.passed`
- `delivery.failed`

When the project is correctly scoped to tenant/repo outputs, delivery also writes control-audit events and syncs summary rows into the tenant `operations.sqlite` index. That is what powers fleet-style `deliveries` reads and aggregate reporting.

## Safety model

Delivery follows the same general AKC posture as the rest of the system:

- tenant/repo scoping is explicit
- `delivery_id` is path-safe and cannot escape `.akc/delivery/`
- recipients must be explicit, not guessed from prose
- adapter and packaging prereq checks fail closed by default
- beta and store lanes are modeled separately
- `release_mode=both` requires a human readiness gate before store promotion
- activation and distribution are recorded as artifacts and events instead of hidden in in-memory state

## Current limitations

The current implementation is intentionally narrower than a full mobile release platform.

Important limits today:

- packaging adapters are still stubs and require operator wiring for real build/export outputs
- web invite generation is built-in, but outbound email sending is still an operator/provider responsibility
- store releases require configured provider credentials and packaged artifacts such as `.ipa` or `.aab`
- enterprise/MDM/ad-hoc/sideload channels are explicitly excluded from the current `both` sequencing model
- there is no separate long-running delivery service; the CLI and on-disk control plane are the source of truth

## Related docs

- [architecture.md](architecture.md)
- [artifact-contracts.md](artifact-contracts.md)
- [getting-started.md](getting-started.md)
- [environment-model.md](environment-model.md)
