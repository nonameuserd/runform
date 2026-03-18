//! `akc_executor` provides sandboxed execution primitives.
//!
//! The CLI and PyO3 surfaces are layered on top of this library.

use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::{ExecLane, ExecRequest, ExecResponse, RunId, TenantId};
use serde_json::json;
use wasmtime::{Config, Engine, Linker, Module, Store, Trap};
use wasmtime_wasi::pipe;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

fn compact_artifact_id(command0: Option<&str>) -> String {
    let Some(s) = command0 else {
        return "".to_string();
    };
    // For paths, keep the filename portion to avoid leaking directories.
    let p = std::path::Path::new(s);
    p.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| s.to_string())
}

fn executor_error_kind(err: &ExecutorError) -> &'static str {
    match err {
        ExecutorError::PolicyDenied => "policy_denied",
        ExecutorError::UnsupportedLane => "unsupported_lane",
        ExecutorError::Timeout => "timeout",
        ExecutorError::EmptyCommand => "empty_command",
        ExecutorError::CommandNotAllowed => "command_not_allowed",
        ExecutorError::Wasm(_) => "wasm_error",
        ExecutorError::Io(_) => "io_error",
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ExecutorError {
    #[error("policy denied")]
    PolicyDenied,
    #[error("unsupported execution lane")]
    UnsupportedLane,
    #[error("execution timed out")]
    Timeout,
    #[error("command must be non-empty")]
    EmptyCommand,
    #[error("command not allowed by policy")]
    CommandNotAllowed,
    #[error("wasm execution failed: {0}")]
    Wasm(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

fn is_program_allowed(program: &str) -> bool {
    if let Ok(raw) = env::var("AKC_EXEC_ALLOWLIST") {
        // Empty variable means "no commands allowed".
        if raw.trim().is_empty() {
            return false;
        }
        return raw.split(':').any(|entry| entry == program);
    }

    // Default: extremely restrictive. Only allow `echo` for tests/dev when no
    // explicit allowlist has been configured.
    matches!(program, "echo")
}

fn workspace_root() -> Result<PathBuf, std::io::Error> {
    // Root can be overridden for tests/CI; default is a relative `.akc` directory.
    // We eagerly create it and canonicalize so later containment checks are stable.
    let root_raw = env::var("AKC_EXEC_ROOT").unwrap_or_else(|_| ".akc".to_string());
    let root = PathBuf::from(root_raw);
    fs::create_dir_all(&root)?;
    root.canonicalize()
}

fn clamp_bytes(s: String, max_bytes: Option<u64>) -> String {
    let Some(max) = max_bytes else { return s };
    let max_usize: usize = max.min(usize::MAX as u64) as usize;
    if s.len() <= max_usize {
        return s;
    }
    s[..max_usize].to_string()
}

fn canonicalize_within(base: &PathBuf, raw: &PathBuf) -> Result<PathBuf, ExecutorError> {
    // Ensure the path exists so canonicalize resolves symlinks and removes `..`.
    fs::create_dir_all(raw)?;
    let canonical = raw.canonicalize()?;
    if !canonical.starts_with(base) {
        return Err(ExecutorError::PolicyDenied);
    }
    Ok(canonical)
}

fn run_process_lane(request: ExecRequest) -> Result<ExecResponse, ExecutorError> {
    if request.command.is_empty() {
        return Err(ExecutorError::EmptyCommand);
    }

    let tenant_id: TenantId = request.tenant_id.clone();
    let run_id: RunId = request.run_id.clone();

    let root = workspace_root()?;
    let workdir_raw = root
        .join("tenants")
        .join(&tenant_id.0)
        .join("runs")
        .join(&run_id.0);
    fs::create_dir_all(&workdir_raw)?;
    let workdir = workdir_raw.canonicalize()?;

    let mut cmd_iter = request.command.into_iter();
    let program = cmd_iter
        .next()
        .expect("command is non-empty; we checked above");

    if !is_program_allowed(&program) {
        log_event(
            LogLevel::Warn,
            "exec_policy_denied",
            &tenant_id,
            &run_id,
            json!({
                "reason": "command_not_in_allowlist",
                "program": program,
            }),
        );
        return Err(ExecutorError::CommandNotAllowed);
    }

    let mut child = Command::new(&program);
    child.args(cmd_iter);

    // Use the provided cwd when present, but enforce tenant/run workspace
    // containment to prevent accidental cross-tenant or host filesystem access.
    if let Some(cwd_raw) = request.cwd {
        let cwd_path = PathBuf::from(&cwd_raw);
        let resolved_raw = if cwd_path.is_absolute() {
            cwd_path
        } else {
            // For relative paths, disallow `..` segments (path traversal).
            if cwd_path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(ExecutorError::PolicyDenied);
            }
            workdir.join(cwd_path)
        };

        let resolved = canonicalize_within(&workdir, &resolved_raw)?;
        child.current_dir(resolved);
    } else {
        child.current_dir(&workdir);
    }

    // Start from a scrubbed environment. Keep a minimal PATH so allowlisted
    // commands like `echo` can be resolved without requiring absolute paths.
    child.env_clear();
    child.env("PATH", "/usr/bin:/bin");
    child.env("AKC_TENANT_ID", &tenant_id.0);
    child.env("AKC_RUN_ID", &run_id.0);
    for (k, v) in request.env {
        child.env(k, v);
    }

    child.stdin(Stdio::piped());
    child.stdout(Stdio::piped());
    child.stderr(Stdio::piped());

    let mut child = child.spawn()?;

    if let Some(stdin_text) = request.stdin_text {
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write as _;
            stdin.write_all(stdin_text.as_bytes())?;
        }
    }

    let wall_time_limit = request
        .limits
        .wall_time_ms
        .map(|ms| (ms, Duration::from_millis(ms)));

    let output = if let Some((wall_ms, deadline_delta)) = wall_time_limit {
        let start = Instant::now();
        loop {
            if let Some(_status) = child.try_wait()? {
                // Process has exited; collect its accumulated output.
                break child.wait_with_output()?;
            }

            if start.elapsed() >= deadline_delta {
                // Best-effort kill on timeout; still collect whatever output exists.
                let _ = child.kill();
                let timed_out_output = child.wait_with_output()?;

                let stdout = String::from_utf8_lossy(&timed_out_output.stdout).into_owned();
                let mut stderr = String::new();
                stderr.push_str(&format!("execution timed out after {} ms\n", wall_ms));
                stderr.push_str(&String::from_utf8_lossy(&timed_out_output.stderr));

                let stdout = clamp_bytes(stdout, request.limits.stdout_max_bytes);
                let stderr = clamp_bytes(stderr, request.limits.stderr_max_bytes);

                log_event(
                    LogLevel::Info,
                    "exec_complete",
                    &tenant_id,
                    &run_id,
                    json!({
                        "lane": "process",
                        "program": program,
                        "ok": false,
                        "exit_code": 124,
                        "timeout": true,
                        "stdout_bytes": stdout.len(),
                        "stderr_bytes": stderr.len(),
                    }),
                );

                return Ok(ExecResponse {
                    tenant_id,
                    run_id,
                    ok: false,
                    // Use a conventional timeout exit code similar to `timeout(1)`.
                    exit_code: 124,
                    stdout,
                    stderr,
                });
            }

            // Avoid busy-waiting.
            thread::sleep(Duration::from_millis(10));
        }
    } else {
        child.wait_with_output()?
    };

    let exit_code = output.status.code().unwrap_or(-1);
    let stdout = clamp_bytes(
        String::from_utf8_lossy(&output.stdout).into_owned(),
        request.limits.stdout_max_bytes,
    );
    let stderr = clamp_bytes(
        String::from_utf8_lossy(&output.stderr).into_owned(),
        request.limits.stderr_max_bytes,
    );

    let ok = exit_code == 0;
    log_event(
        LogLevel::Info,
        "exec_complete",
        &tenant_id,
        &run_id,
        json!({
            "lane": "process",
            "program": program,
            "ok": ok,
            "exit_code": exit_code,
            "timeout": false,
            "stdout_bytes": stdout.len(),
            "stderr_bytes": stderr.len(),
        }),
    );

    Ok(ExecResponse {
        tenant_id,
        run_id,
        ok,
        exit_code,
        stdout,
        stderr,
    })
}

fn run_wasm_lane(request: ExecRequest) -> Result<ExecResponse, ExecutorError> {
    if request.command.is_empty() {
        return Err(ExecutorError::EmptyCommand);
    }

    let tenant_id: TenantId = request.tenant_id.clone();
    let run_id: RunId = request.run_id.clone();

    // WASM lane also reserves a workspace directory for artifacts/logs, even though
    // WASI P1 is configured with no preopened dirs (no filesystem access).
    let root = workspace_root()?;
    let workdir_raw = root
        .join("tenants")
        .join(&tenant_id.0)
        .join("runs")
        .join(&run_id.0);
    fs::create_dir_all(&workdir_raw)?;
    let _workdir = workdir_raw.canonicalize()?;

    let mut cmd_iter = request.command.into_iter();
    let module_path = cmd_iter
        .next()
        .expect("command is non-empty; we checked above");
    let module_id = compact_artifact_id(Some(&module_path));

    // Lane A (WASM): execute a WASI core module with strict limits.
    //
    // For v1, this is WASI Preview 1 (`wasi_snapshot_preview1`) because it's the
    // simplest stable target to run core modules. Component Model / WASI Preview 2
    // can be layered on later without changing the outer request/response schema.
    let mut config = Config::new();
    config.consume_fuel(true);
    config.epoch_interruption(true);

    let engine = Engine::new(&config).map_err(|e| ExecutorError::Wasm(e.to_string()))?;
    let module =
        Module::from_file(&engine, &module_path).map_err(|e| ExecutorError::Wasm(e.to_string()))?;

    let args: Vec<String> = std::iter::once(module_path.clone())
        .chain(cmd_iter)
        .collect();

    let stdout_cap: usize = request
        .limits
        .stdout_max_bytes
        .unwrap_or(1024 * 1024)
        .min(usize::MAX as u64) as usize;
    let stderr_cap: usize = request
        .limits
        .stderr_max_bytes
        .unwrap_or(1024 * 1024)
        .min(usize::MAX as u64) as usize;

    let stdout_pipe = pipe::MemoryOutputPipe::new(stdout_cap);
    let stderr_pipe = pipe::MemoryOutputPipe::new(stderr_cap);

    // Build a minimal WASI context:
    // - No preopened dirs (no filesystem access)
    // - No inherited env (only what request provides)
    // - Deterministic args
    let mut wasi_builder = WasiCtxBuilder::new();
    wasi_builder.args(&args);
    for (k, v) in &request.env {
        wasi_builder.env(k, v);
    }
    wasi_builder.env("AKC_TENANT_ID", &tenant_id.0);
    wasi_builder.env("AKC_RUN_ID", &run_id.0);
    wasi_builder.stdout(stdout_pipe.clone());
    wasi_builder.stderr(stderr_pipe.clone());

    let wasi_ctx: WasiP1Ctx = wasi_builder.build_p1();
    let mut store: Store<WasiP1Ctx> = Store::new(&engine, wasi_ctx);

    // Fuel budget (CPU-ish). If a wall-clock limit is present, pick a conservative
    // proportional budget; otherwise choose a bounded default.
    let fuel: u64 = request
        .limits
        .wall_time_ms
        .map(|ms| ms.saturating_mul(50_000).clamp(5_000_000, 2_000_000_000))
        .unwrap_or(50_000_000);
    store
        .set_fuel(fuel)
        .map_err(|e| ExecutorError::Wasm(e.to_string()))?;

    // Wall-clock timeout via epoch interruption.
    if let Some(wall_ms) = request.limits.wall_time_ms {
        store.set_epoch_deadline(1);
        let engine_for_timer = engine.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(wall_ms));
            engine_for_timer.increment_epoch();
        });
    }

    let mut linker: Linker<WasiP1Ctx> = Linker::new(&engine);
    preview1::add_to_linker_sync(&mut linker, |ctx: &mut WasiP1Ctx| ctx)
        .map_err(|e| ExecutorError::Wasm(e.to_string()))?;

    let instance = linker
        .instantiate(&mut store, &module)
        .map_err(|e| ExecutorError::Wasm(e.to_string()))?;

    let start = instance
        .get_typed_func::<(), ()>(&mut store, "_start")
        .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "main"))
        .map_err(|_e| ExecutorError::UnsupportedLane)?;

    let result = start.call(&mut store, ());

    let stdout = String::from_utf8_lossy(stdout_pipe.contents().as_ref()).into_owned();
    let stderr = String::from_utf8_lossy(stderr_pipe.contents().as_ref()).into_owned();

    match result {
        Ok(()) => {
            log_event(
                LogLevel::Info,
                "exec_complete",
                &tenant_id,
                &run_id,
                json!({
                    "lane": "wasm",
                    "module": module_id,
                    "ok": true,
                    "exit_code": 0,
                    "timeout": false,
                    "stdout_bytes": stdout.len(),
                    "stderr_bytes": stderr.len(),
                    "trap": serde_json::Value::Null,
                }),
            );

            Ok(ExecResponse {
                tenant_id,
                run_id,
                ok: true,
                exit_code: 0,
                stdout,
                stderr,
            })
        }
        Err(e) => {
            // Map common traps into structured outcomes.
            let mut exit_code: i32 = 1;
            let ok: bool = false;
            let mut err_text: String = e.to_string();
            let mut trap_kind: Option<&'static str> = None;

            if let Some(trap) = e.downcast_ref::<Trap>() {
                match trap {
                    Trap::Interrupt => {
                        exit_code = 124;
                        err_text = "wasm execution timed out".to_string();
                        trap_kind = Some("interrupt");
                    }
                    Trap::OutOfFuel => {
                        exit_code = 137;
                        err_text = "wasm cpu budget exceeded".to_string();
                        trap_kind = Some("out_of_fuel");
                    }
                    _ => {}
                }
            }

            let stderr_final = if stderr.is_empty() {
                err_text
            } else {
                format!("{err_text}\n{stderr}")
            };

            log_event(
                LogLevel::Warn,
                "exec_complete",
                &tenant_id,
                &run_id,
                json!({
                    "lane": "wasm",
                    "module": module_id,
                    "ok": ok,
                    "exit_code": exit_code,
                    "timeout": exit_code == 124,
                    "stdout_bytes": stdout.len(),
                    "stderr_bytes": stderr_final.len(),
                    "trap": trap_kind.unwrap_or("unknown"),
                }),
            );

            Ok(ExecResponse {
                tenant_id,
                run_id,
                ok,
                exit_code,
                stdout,
                stderr: stderr_final,
            })
        }
    }
}

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
            "wall_time_ms": request.limits.wall_time_ms,
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
