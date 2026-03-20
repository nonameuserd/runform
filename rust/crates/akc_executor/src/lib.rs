//! `akc_executor` provides sandboxed execution primitives.
//!
//! The CLI and PyO3 surfaces are layered on top of this library.

mod backend;
mod env_policy;
mod error;
mod fs_policy;
mod util;

#[cfg(windows)]
mod windows_job;

pub use error::ExecutorError;
pub const WASM_EXIT_TIMEOUT: i32 = 124;
pub const WASM_EXIT_CPU_FUEL_EXHAUSTED: i32 = 137;
pub const WASM_EXIT_MEMORY_LIMIT_EXCEEDED: i32 = 138;
pub const WASM_EXIT_UNSUPPORTED_PLATFORM_CAPABILITY: i32 = 78;

use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::{ExecLane, ExecRequest, ExecResponse, RunId, TenantId};
use serde_json::json;

use crate::backend::process::run_process_lane;
use crate::backend::wasm::run_wasm_lane;
use crate::error::executor_error_kind;
use crate::util::compact_artifact_id;

/// Execute a request.
///
/// v1 implements the `process` lane by spawning an OS process in a tenant/run-scoped
/// workspace. The `wasm` lane is reserved for future work.
pub fn run_exec(request: ExecRequest) -> Result<ExecResponse, ExecutorError> {
    let tenant_id: TenantId = request.tenant_id.clone();
    let run_id: RunId = request.run_id.clone();
    let lane = match &request.lane {
        ExecLane::Process => "process",
        ExecLane::Wasm => "wasm",
    };
    let program_id = compact_artifact_id(request.command.first().map(|s| s.as_str()));

    log_event(
        LogLevel::Info,
        "exec_start",
        &tenant_id,
        &run_id,
        json!({
            "lane": lane,
            "program": program_id,
            "network_requested": request.capabilities.network,
            "allowed_read_paths": request.fs_policy.allowed_read_paths,
            "allowed_write_paths": request.fs_policy.allowed_write_paths,
            "preopen_dirs": request.fs_policy.preopen_dirs,
            "wall_time_ms": request.limits.wall_time_ms,
            "cpu_fuel": request.limits.cpu_fuel,
            "memory_bytes": request.limits.memory_bytes,
            "stdout_max_bytes": request.limits.stdout_max_bytes,
            "stderr_max_bytes": request.limits.stderr_max_bytes,
            "stdin_present": request.stdin_text.is_some(),
            "env_count": request.env.len(),
        }),
    );

    if request.capabilities.network {
        log_event(
            LogLevel::Warn,
            "exec_policy_denied",
            &tenant_id,
            &run_id,
            json!({
                "reason": "network_capability_denied",
            }),
        );
        return Err(ExecutorError::PolicyDenied);
    }

    let res = match request.lane {
        ExecLane::Process => run_process_lane(request),
        ExecLane::Wasm => run_wasm_lane(request),
    };

    if let Err(ref err) = res {
        log_event(
            LogLevel::Error,
            "exec_error",
            &tenant_id,
            &run_id,
            json!({
                "error_kind": executor_error_kind(err),
                "surface": std::env::var("AKC_EXEC_SURFACE").unwrap_or_else(|_| "unknown".to_string()),
            }),
        );
    }

    res
}
