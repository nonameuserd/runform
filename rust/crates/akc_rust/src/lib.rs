use akc_executor::run_exec;
use akc_ingest::ingest;
use akc_protocol::observability::{log_event, log_event_unscoped, LogLevel};
use akc_protocol::{ExecRequest, ExecResponse, IngestRequest, IngestResponse};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use serde_json::json;

/// Small adapter to let PyO3 convert Rust errors into Python exceptions.
///
/// Using a non-`PyErr` error type here avoids clippy detecting an identity
/// conversion inside PyO3's generated glue code.
#[derive(Debug)]
struct AkcPyError(String);

impl From<AkcPyError> for pyo3::PyErr {
    fn from(err: AkcPyError) -> Self {
        PyValueError::new_err(err.0)
    }
}

fn executor_error_kind(err: &akc_executor::ExecutorError) -> &'static str {
    match err {
        akc_executor::ExecutorError::PolicyDenied => "policy_denied",
        akc_executor::ExecutorError::UnsupportedLane => "unsupported_lane",
        akc_executor::ExecutorError::Timeout => "timeout",
        akc_executor::ExecutorError::EmptyCommand => "empty_command",
        akc_executor::ExecutorError::CommandNotAllowed => "command_not_allowed",
        akc_executor::ExecutorError::Wasm(_) => "wasm_error",
        akc_executor::ExecutorError::Io(_) => "io_error",
    }
}

fn ingest_error_kind(err: &akc_ingest::IngestError) -> &'static str {
    match err {
        akc_ingest::IngestError::Protocol(_) => "protocol_error",
        akc_ingest::IngestError::Io(_) => "io_error",
        akc_ingest::IngestError::UnsupportedKind => "unsupported_kind",
    }
}

fn compact_artifact_id(command0: Option<&str>) -> String {
    let Some(s) = command0 else {
        return "".to_string();
    };
    let p = std::path::Path::new(s);
    p.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| s.to_string())
}

/// Parse a JSON string into an `ExecRequest`, execute it via the Rust executor,
/// and return the `ExecResponse` as a JSON string.
///
/// This provides a low-overhead embedding path while keeping the boundary
/// schema identical to the subprocess JSON interface.
#[pyfunction]
fn run_exec_json(request_json: &str) -> Result<String, AkcPyError> {
    let request: ExecRequest = match serde_json::from_str(request_json) {
        Ok(v) => v,
        Err(_) => {
            log_event_unscoped(LogLevel::Error, "exec_pyo3_invalid_request_json", json!({}));
            return Err(AkcPyError("invalid ExecRequest JSON".to_string()));
        }
    };

    let tenant_id = request.tenant_id.clone();
    let run_id = request.run_id.clone();
    let lane = match &request.lane {
        akc_protocol::ExecLane::Process => "process",
        akc_protocol::ExecLane::Wasm => "wasm",
    };
    let program_id = compact_artifact_id(request.command.first().map(|s| s.as_str()));

    log_event(
        LogLevel::Info,
        "exec_surface_start",
        &tenant_id,
        &run_id,
        json!({
            "surface": "pyo3",
            "lane": lane,
            "program": program_id,
            "network_requested": request.capabilities.network,
            "wall_time_ms": request.limits.wall_time_ms,
            "stdout_max_bytes": request.limits.stdout_max_bytes,
            "stderr_max_bytes": request.limits.stderr_max_bytes,
        }),
    );

    // Ensure the internal executor logs can correlate CLI vs PyO3.
    std::env::set_var("AKC_EXEC_SURFACE", "pyo3");

    let response: ExecResponse = match run_exec(request) {
        Ok(v) => {
            log_event(
                LogLevel::Info,
                "exec_surface_complete",
                &tenant_id,
                &run_id,
                json!({
                    "surface": "pyo3",
                    "ok": v.ok,
                    "exit_code": v.exit_code,
                    "stdout_bytes": v.stdout.len(),
                    "stderr_bytes": v.stderr.len(),
                }),
            );
            v
        }
        Err(err) => {
            log_event(
                LogLevel::Error,
                "exec_surface_error",
                &tenant_id,
                &run_id,
                json!({
                    "surface": "pyo3",
                    "error_kind": executor_error_kind(&err),
                }),
            );
            return Err(AkcPyError(format!("executor error: {}", err)));
        }
    };

    serde_json::to_string(&response)
        .map_err(|err| AkcPyError(format!("failed to serialize ExecResponse: {err}")))
}

/// Parse a JSON string into an `IngestRequest`, execute it via the Rust ingest
/// library, and return the `IngestResponse` as a JSON string.
#[pyfunction]
fn ingest_json(request_json: &str) -> Result<String, AkcPyError> {
    let request: IngestRequest = match serde_json::from_str(request_json) {
        Ok(v) => v,
        Err(_) => {
            log_event_unscoped(
                LogLevel::Error,
                "ingest_pyo3_invalid_request_json",
                json!({}),
            );
            return Err(AkcPyError("invalid IngestRequest JSON".to_string()));
        }
    };

    let tenant_id = request.tenant_id.clone();
    let run_id = request.run_id.clone();
    let kind_label = match &request.kind {
        None => "none",
        Some(akc_protocol::IngestKind::Docs(_)) => "docs",
        Some(akc_protocol::IngestKind::Messaging(_)) => "messaging",
        Some(akc_protocol::IngestKind::Api(_)) => "api",
    };
    let input_paths_count = match &request.kind {
        Some(akc_protocol::IngestKind::Docs(docs)) => docs.input_paths.len(),
        _ => 0_usize,
    };

    log_event(
        LogLevel::Info,
        "ingest_surface_start",
        &tenant_id,
        &run_id,
        json!({
            "surface": "pyo3",
            "kind": kind_label,
            "input_paths_count": input_paths_count,
        }),
    );

    // Ensure internal ingestion logs can correlate CLI vs PyO3.
    std::env::set_var("AKC_INGEST_SURFACE", "pyo3");

    let response: IngestResponse = match ingest(request) {
        Ok(v) => {
            log_event(
                LogLevel::Info,
                "ingest_surface_complete",
                &tenant_id,
                &run_id,
                json!({
                    "surface": "pyo3",
                    "ok": v.ok,
                    "records": v.records.len(),
                }),
            );
            v
        }
        Err(err) => {
            log_event(
                LogLevel::Error,
                "ingest_surface_error",
                &tenant_id,
                &run_id,
                json!({
                    "surface": "pyo3",
                    "error_kind": ingest_error_kind(&err),
                    "exit_code": null,
                }),
            );
            return Err(AkcPyError(format!("ingest error: {}", err)));
        }
    };

    serde_json::to_string(&response)
        .map_err(|err| AkcPyError(format!("failed to serialize IngestResponse: {err}")))
}

/// Python module definition.
#[pymodule]
fn akc_rust(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_exec_json, m)?)?;
    m.add_function(wrap_pyfunction!(ingest_json, m)?)?;
    Ok(())
}
