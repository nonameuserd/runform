# Validation

This page describes the validation system AKC exposes today for observability-backed and mobile-backed `operational_spec` checks.

## Model

Validation is evidence-driven.

- `operational_spec` stays the source-of-truth contract
- intent JSON contains opaque `validator_stub` references, not raw LogQL, PromQL, or TraceQL
- operator-side bindings resolve those stubs to concrete validators
- validators export normalized artifacts under `.akc/verification/validators/<run_id>/`
- runtime and verify consume only the exported evidence artifacts

The three validator-produced artifact types are:

- `observability_query_result.v1`
- `mobile_journey_result.v1`
- `device_capture_result.v1`

The matching runtime evidence types are:

- `akc_observability_query_result`
- `akc_mobile_journey_result`
- `akc_device_capture_result`

## Registry

Default registry path:

```text
configs/validation/validator_bindings.v1.yaml
```

You can override it in `.akc/project.json`:

```json
{
  "validation": {
    "bindings_path": "configs/validation/validator_bindings.v1.yaml"
  }
}
```

Or at verify time:

```bash
akc verify \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --execute-validators \
  --validator-bindings ./configs/validation/validator_bindings.v1.yaml
```

## Supported binding kinds

- `logql_query`
- `promql_query`
- `traceql_query`
- `maestro_flow`
- `android_helper`
- `ios_simulator_helper`

Notes:

- `maestro_flow` is the cross-platform journey runner in v1
- `android_helper` is for ADB-backed setup/debug/capture operations
- `ios_simulator_helper` is for `xcrun simctl`-backed setup/debug/capture operations
- validator execution is runtime-enabled only through `LocalDepthRuntimeAdapter`
- after load-time normalization, resolved `flow_path`, `apk_path`, and `app_path` must fall under the tenant/repo scope directory passed to execution (`<outputs_root>/<tenant_id>/<repo_id>`); place flows and app binaries there (or under a subdirectory), not only next to the registry file

## Example registry

See [configs/validation/validator_bindings.example.yaml](https://github.com/nonameuserd/runform/blob/main/configs/validation/validator_bindings.example.yaml) for a ready-to-copy example.

Example:

```yaml
schema_version: 1
schema_kind: validator_bindings
bindings:
  obs.login_5xx:
    kind: logql_query
    url: https://loki.example.internal/loki/api/v1/query
    query: 'sum(count_over_time({app="mobile-api"} |= "POST /login" |= " 5" [5m]))'
    target: loki-main

  mobile.login.android:
    kind: maestro_flow
    platform: android
    journey_id: login
    flow_path: ./flows/android/login.yaml
    device_id: emulator-5554
    app_id: com.example.app

  android.login.failure_screenshot:
    kind: android_helper
    operation: screenshot
    artifact_name: login_failure
    device_id: emulator-5554

  ios.login.simulator:
    kind: maestro_flow
    platform: ios
    journey_id: login
    flow_path: ./flows/ios/login.yaml
    device_id: booted
    app_id: com.example.ios

  ios.login.failure_screenshot:
    kind: ios_simulator_helper
    operation: screenshot
    device_id: booted
    artifact_name: login_failure
```

## `operational_spec` patterns

### Observability presence check

```json
{
  "spec_version": 1,
  "window": "single_run",
  "predicate_kind": "presence",
  "signals": [
    {
      "evidence_type": "akc_observability_query_result",
      "validator_stub": "obs.login_5xx",
      "payload_path": "status"
    }
  ],
  "expected_evidence_types": ["akc_observability_query_result"],
  "evaluation_phase": "post_runtime"
}
```

Use this when you want a query to execute and export normalized evidence. The concrete query stays in the registry.

### Observability threshold check

```json
{
  "spec_version": 1,
  "window": "single_run",
  "predicate_kind": "threshold",
  "signals": [
    {
      "evidence_type": "akc_observability_query_result",
      "validator_stub": "obs.login_5xx",
      "payload_path": "summary.value"
    }
  ],
  "threshold": 0,
  "threshold_comparator": "eq",
  "expected_evidence_types": ["akc_observability_query_result"],
  "evaluation_phase": "post_runtime"
}
```

Use this when your query result normalizes into `summary.value` or a known numeric summary field.

### Mobile journey check

```json
{
  "spec_version": 1,
  "window": "single_run",
  "predicate_kind": "presence",
  "signals": [
    {
      "evidence_type": "akc_mobile_journey_result",
      "validator_stub": "mobile.login.android",
      "payload_path": "status"
    }
  ],
  "expected_evidence_types": ["akc_mobile_journey_result"],
  "evaluation_phase": "post_runtime"
}
```

### Mobile assertion threshold check

```json
{
  "spec_version": 1,
  "window": "single_run",
  "predicate_kind": "threshold",
  "signals": [
    {
      "evidence_type": "akc_mobile_journey_result",
      "validator_stub": "mobile.login.android",
      "payload_path": "assertions_failed"
    }
  ],
  "threshold": 0,
  "threshold_comparator": "eq",
  "expected_evidence_types": ["akc_mobile_journey_result"],
  "evaluation_phase": "post_runtime"
}
```

### Device capture presence check

```json
{
  "spec_version": 1,
  "window": "single_run",
  "predicate_kind": "presence",
  "signals": [
    {
      "evidence_type": "akc_device_capture_result",
      "validator_stub": "android.login.failure_screenshot",
      "payload_path": "artifact_path"
    }
  ],
  "expected_evidence_types": ["akc_device_capture_result"],
  "evaluation_phase": "post_runtime"
}
```

## Execution flow

### `akc verify`

`akc verify --execute-validators`:

1. loads the latest scoped runtime record and bundle
2. resolves post-runtime `operational_spec` criteria
3. loads the validator registry
4. executes referenced bindings
5. writes normalized artifacts and runtime evidence
6. recomputes `operational_validity_report`
7. runs operational coupling verification against exported artifacts

### Runtime

When runtime is using `LocalDepthRuntimeAdapter`, post-runtime validator bindings are executed before runtime evidence is persisted and before `operational_validity_report` is written.

If runtime is on the native adapter, validator execution is blocked and exported as failure-shaped validator evidence instead of silently succeeding.

## Best-practice defaults

- keep queries and device commands in the operator registry, not in intent JSON
- use `maestro_flow` for user journeys and keep ADB/simctl helpers narrow
- keep helper bindings for setup and evidence capture, not as the primary assertion language
- evaluate only exported validator evidence in `operational_spec`
- prefer stable `validator_stub` ids that map one intent-facing contract to one operator-maintained binding
