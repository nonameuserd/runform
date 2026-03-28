# Runtime Execution

This page describes the runtime surface that AKC exposes today: runtime bundles, routing, adapters, coordination, reconcile, and autopilot.

For exact flags, use `akc runtime --help` and `akc runtime <subcommand> --help`.

## Runtime command tree

Current runtime subcommands:

- `start`
- `coordination-plan`
- `stop`
- `status`
- `events`
- `reconcile`
- `checkpoint`
- `replay`
- `autopilot`

## What runtime operates on

The runtime surface consumes emitted runtime bundles and writes runtime state and evidence under the scoped outputs tree.

Relevant modules:

- `src/akc/cli/runtime.py`
- `src/akc/runtime/kernel.py`
- `src/akc/runtime/models.py`
- `src/akc/runtime/state_store.py`

## Start and inspect a runtime bundle

Two useful entry points:

```bash
akc runtime coordination-plan --bundle /path/to/runtime_bundle.json
akc runtime start --bundle /path/to/runtime_bundle.json
```

`coordination-plan` is the safe read-only way to inspect scheduling before starting a run.

## Adapter model

Runtime behavior is built around adapters.

Primary adapter implementations:

- `src/akc/runtime/adapters/native.py`
- `src/akc/runtime/adapters/local_depth.py`
- `src/akc/runtime/adapters/base.py`
- `src/akc/runtime/adapters/registry.py`

High-level behavior:

- `NativeRuntimeAdapter` is the default adapter
- `LocalDepthRuntimeAdapter` adds runtime action routing and optional local-depth subprocess and HTTP execution
- `HybridRuntimeAdapter` is the supported extension point for custom primary adapters with capability-gated native fallback behavior

Helper constructors:

- `src/akc/runtime/init.py:create_native_runtime`
- `src/akc/runtime/init.py:create_local_depth_runtime`
- `src/akc/runtime/init.py:create_hybrid_runtime`

## Action routing

Runtime action routing lives in `src/akc/runtime/action_routing.py`.

Current route kinds:

- `delegate_adapter`
- `noop`
- `subprocess`
- `http`

Routing is policy-bounded and context-sensitive. The runtime bundle, node-level metadata, and runtime policy all affect what is allowed.

## Coordination

Coordination support is implemented under:

- `src/akc/runtime/coordination/load.py`
- `src/akc/runtime/coordination/models.py`
- `src/akc/runtime/coordination/worker.py`
- `src/akc/runtime/coordination/audit.py`
- `src/akc/runtime/coordination/isolation.py`

`akc runtime coordination-plan` uses the same coordination model the runtime kernel consumes.

Key properties:

- deterministic schedule layers
- explicit handling of coordination spec versions
- coordination audit records emitted alongside runtime evidence
- runtime policy context derived from coordination role/profile metadata

For the schema and normalization rules, see [coordination-semantics.md](coordination-semantics.md).

## Reconcile

Reconcile is the runtime surface that compares desired bundle state with observed state and records evidence.

Relevant modules:

- `src/akc/runtime/reconciler.py`
- `src/akc/runtime/providers/`
- `src/akc/runtime/observe_probes.py`
- `src/akc/runtime/resync_backoff.py`

Typical uses:

- inspect convergence
- collect reconcile evidence
- drive progressive rollout logic
- feed autopilot and living-automation loops

## Runtime validation

Post-runtime `operational_spec` can reference operator-side validators through `validator_stub`.

Current behavior:

- runtime executes validator bindings through `LocalDepthRuntimeAdapter`
- validator outputs are normalized into exported artifacts and runtime evidence
- `operational_validity_report` is computed from the merged exported evidence set
- native adapter execution is blocked and exported as failure-shaped validator evidence instead of silently bypassing the gate

See [validation.md](validation.md) for registry format and examples.

## Runtime providers

Current provider code lives under `src/akc/runtime/providers/`.

Current provider-related modules include:

- `compose.py`
- `kubernetes.py`
- `factory.py`
- `_subprocess.py`

These are the deployment-facing execution providers used by reconcile and related runtime flows.

## Events, checkpoint, and replay

Runtime state and evidence are not treated as opaque internal state.

Important commands:

- `akc runtime events`
- `akc runtime checkpoint`
- `akc runtime replay`

Important modules:

- `src/akc/runtime/events.py`
- `src/akc/runtime/state_store.py`
- `src/akc/run/`

This is how runtime behavior stays inspectable and replayable rather than hidden behind a single controller loop.

## Autopilot

`akc runtime autopilot` is the always-on controller loop for living recompile and reliability workflows.

Relevant code:

- `src/akc/runtime/autopilot.py`
- `src/akc/runtime/living_bridge.py`
- `src/akc/living/automation_profile.py`

Autopilot is where runtime health, drift, and policy-controlled automation come together.

Important themes:

- single-writer behavior per scope
- living recompile triggers
- reliability scoreboard artifacts
- optional SLO gate behavior before new live rollouts

## Security posture

Runtime execution is intentionally distinct from compile-time execution.

The runtime surface relies on:

- policy gating
- adapter capability boundaries
- scoped filesystem paths
- bounded subprocess/HTTP routing when enabled
- replayable evidence

For the detailed threat model and enforcement discussion, see [security.md](security.md).

## Practical reading order

If you are trying to understand runtime from the code:

1. `src/akc/cli/runtime.py`
2. `src/akc/runtime/kernel.py`
3. `src/akc/runtime/action_routing.py`
4. `src/akc/runtime/adapters/`
5. `src/akc/runtime/reconciler.py`
6. `src/akc/runtime/coordination/`
7. `src/akc/runtime/autopilot.py`
