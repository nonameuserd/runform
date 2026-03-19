use std::io::{self, Read, Write};

use akc_executor::run_exec;
use akc_protocol::observability::{log_event, log_event_unscoped, LogLevel};
use akc_protocol::ExecRequest;
use serde_json::json;

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

fn main() {
    // Read all of stdin into a string.
    let mut input = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut input) {
        let _ = writeln!(io::stderr(), "failed to read stdin: {err}");
        log_event_unscoped(
            LogLevel::Error,
            "exec_request_stdin_read_failed",
            json!({ "exit_code": 30 }),
        );
        std::process::exit(30);
    }

    // Deserialize the request; map JSON/protocol errors to a validation exit code.
    let request: ExecRequest = match serde_json::from_str(&input) {
        Ok(req) => req,
        Err(err) => {
            let _ = writeln!(io::stderr(), "invalid ExecRequest JSON: {err}");
            log_event_unscoped(
                LogLevel::Error,
                "exec_request_invalid_json",
                json!({ "exit_code": 10 }),
            );
            std::process::exit(10);
        }
    };

    // Boundary logs for CLI vs PyO3 correlation.
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
            "surface": "cli",
            "lane": lane,
            "program": program_id,
            "network_requested": request.capabilities.network,
            "wall_time_ms": request.limits.wall_time_ms,
            "stdout_max_bytes": request.limits.stdout_max_bytes,
            "stderr_max_bytes": request.limits.stderr_max_bytes,
        }),
    );

    std::env::set_var("AKC_EXEC_SURFACE", "cli");

    // Execute via the library.
    match run_exec(request) {
        Ok(response) => match serde_json::to_string(&response) {
            Ok(json) => {
                if let Err(err) = writeln!(io::stdout(), "{json}") {
                    let _ = writeln!(io::stderr(), "failed to write response: {err}");
                    std::process::exit(30);
                }
                log_event(
                    LogLevel::Info,
                    "exec_surface_complete",
                    &tenant_id,
                    &run_id,
                    json!({
                        "surface": "cli",
                        "ok": response.ok,
                        "exit_code": response.exit_code,
                        "stdout_bytes": response.stdout.len(),
                        "stderr_bytes": response.stderr.len(),
                    }),
                );
                std::process::exit(0);
            }
            Err(err) => {
                let _ = writeln!(io::stderr(), "failed to serialize ExecResponse: {err}");
                std::process::exit(30);
            }
        },
        Err(err) => {
            // Policy-denied vs validation vs sandbox failure.
            let code: i32 = match err {
                akc_executor::ExecutorError::PolicyDenied => 20,
                akc_executor::ExecutorError::EmptyCommand => 10,
                akc_executor::ExecutorError::CommandNotAllowed => 20,
                akc_executor::ExecutorError::Timeout => 40,
                _ => 30,
            };
            let _ = writeln!(io::stderr(), "{err}");
            log_event(
                LogLevel::Error,
                "exec_surface_error",
                &tenant_id,
                &run_id,
                json!({
                    "surface": "cli",
                    "error_kind": executor_error_kind(&err),
                    "exit_code": code,
                }),
            );
            std::process::exit(code);
        }
    }
}
