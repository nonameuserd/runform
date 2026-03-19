# Logging Schema (Executor + Ingest)

All AKC Rust and Python logging uses **newline-delimited JSON** written to `stderr` for machine ingestion.

This document defines a stable field contract so log consumers can rely on exact **field names**.

## Common fields

Every log record MUST include the following fields (Rust and Python emitters):

- `ts_unix_ms`: integer epoch time in milliseconds
- `level`: one of `error`, `warn`, `info`, `debug`, `trace`
- `event`: string event name
- `pid`: integer process id
- `tenant_id`: string or `null`
- `run_id`: string or `null`

## Surface events (recommended for consumers)

These events correspond to *boundary* calls and are emitted by both:
- Rust CLI binaries (`surface="cli"`)
- Rust PyO3 wrapper (`surface="pyo3"`)
- Python wrapper (`src/akc/compile/rust_bridge.py`)

### `exec_surface_start`

Required fields (in addition to common):
- `surface`: `cli` or `pyo3`
- `lane`: `process` or `wasm`
- `program`: compact identifier for the executed artifact (first command/module filename)
- `network_requested`: boolean (network capability requested)
- `wall_time_ms`: integer or `null`
- `stdout_max_bytes`: integer or `null`
- `stderr_max_bytes`: integer or `null`

### `exec_surface_complete`

Required fields (in addition to common):
- `surface`: `cli` or `pyo3`
- `ok`: boolean
- `exit_code`: integer
- `stdout_bytes`: integer
- `stderr_bytes`: integer

### `exec_surface_error`

Required fields (in addition to common):
- `surface`: `cli` or `pyo3`
- `error_kind`: short string (consumer-friendly, non-sensitive)
- `exit_code`: integer (CLI) or `null` (PyO3)

## Executor-only (Rust library) events

These events are emitted by Rust `akc_executor` via `akc_protocol::observability::log_event()`:

### `exec_start`
Required fields (in addition to common):
- `lane`, `program`
- `network_requested`
- `wall_time_ms`, `memory_bytes`
- `stdout_max_bytes`, `stderr_max_bytes`
- `stdin_present`, `env_count`

### `exec_complete`
Required fields (in addition to common):
- `lane`
- `ok`, `exit_code`
- `timeout`
- `stdout_bytes`, `stderr_bytes`
Optional:
- `module` (WASM lane)
- `program` (process lane)
- `trap` (WASM failures)

## Ingest surface events

These events correspond to boundary calls for ingestion:

### `ingest_surface_start`

Required fields (in addition to common):
- `surface`: `cli` or `pyo3`
- `kind`: one of `none`, `docs`, `messaging`, `api`
- `input_paths_count`: integer

### `ingest_surface_complete`

Required fields (in addition to common):
- `surface`: `cli` or `pyo3`
- `ok`: boolean
- `records`: integer

### `ingest_surface_error`

Required fields (in addition to common):
- `surface`: `cli` or `pyo3`
- `error_kind`: short string (consumer-friendly, non-sensitive)
- `exit_code`: integer (CLI) or `null` (PyO3)

## Python-only wrapper error events

The Python wrapper emits these events when it cannot even reach the Rust boundary:

### `exec_cli_binary_not_found`
- `surface` (= `cli`)
- `error_kind` (= `binary_not_found`)
- `exit_code` (= 30)
- `binary`
- `stderr_bytes`

### `exec_cli_invalid_json`
- `surface` (= `cli`)
- `error_kind` (= `invalid_json`)
- `exit_code`
- `stderr_bytes`

### `ingest_cli_binary_not_found`
- `surface` (= `cli`)
- `error_kind` (= `binary_not_found`)
- `exit_code` (= 30)
- `binary`
- `stderr_bytes`

### `ingest_cli_invalid_json`
- `surface` (= `cli`)
- `error_kind` (= `invalid_json`)
- `exit_code`
- `stderr_bytes`

