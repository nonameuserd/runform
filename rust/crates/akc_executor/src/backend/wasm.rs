use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::{ExecRequest, ExecResponse, RunId, TenantId};
use serde_json::json;
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;
use wasmtime::{
    Config, Engine, ExternType, Linker, Module, Store, StoreLimits, StoreLimitsBuilder, Trap,
};
use wasmtime_wasi::p2::pipe;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtxBuilder};

use crate::env_policy::filter_request_env;
use crate::util::ensure_safe_absolute_path_string;
use crate::util::{compact_artifact_id, workspace_root};
use crate::ExecutorError;
use crate::{
    WASM_EXIT_CPU_FUEL_EXHAUSTED, WASM_EXIT_MEMORY_LIMIT_EXCEEDED, WASM_EXIT_TIMEOUT,
    WASM_EXIT_UNSUPPORTED_PLATFORM_CAPABILITY,
};

const WASM_DEFAULT_STDIO_MAX_BYTES: u64 = 1024 * 1024;
const WASM_FUEL_MAX: u64 = 2_000_000_000;
const WASM_PAGE_SIZE_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, Copy)]
enum WasmErrorKind {
    Timeout,
    CpuFuelExhausted,
    MemoryLimitExceeded,
    UnsupportedPlatformCapability,
    RuntimeTrap,
}

impl WasmErrorKind {
    fn code(self) -> &'static str {
        match self {
            Self::Timeout => "WASM_TIMEOUT",
            Self::CpuFuelExhausted => "WASM_CPU_FUEL_EXHAUSTED",
            Self::MemoryLimitExceeded => "WASM_MEMORY_LIMIT_EXCEEDED",
            Self::UnsupportedPlatformCapability => "WASM_UNSUPPORTED_PLATFORM_CAPABILITY",
            Self::RuntimeTrap => "WASM_RUNTIME_TRAP",
        }
    }

    fn exit_code(self) -> i32 {
        match self {
            Self::Timeout => WASM_EXIT_TIMEOUT,
            Self::CpuFuelExhausted => WASM_EXIT_CPU_FUEL_EXHAUSTED,
            Self::MemoryLimitExceeded => WASM_EXIT_MEMORY_LIMIT_EXCEEDED,
            Self::UnsupportedPlatformCapability => WASM_EXIT_UNSUPPORTED_PLATFORM_CAPABILITY,
            Self::RuntimeTrap => 1,
        }
    }
}

struct WasiStoreData {
    wasi: WasiP1Ctx,
    limits: StoreLimits,
}

struct EpochDeadlineGuard {
    cancel_tx: mpsc::Sender<()>,
    thread_handle: thread::JoinHandle<()>,
}

impl EpochDeadlineGuard {
    fn start(engine: Engine, wall_time_ms: u64) -> Self {
        let (cancel_tx, cancel_rx) = mpsc::channel::<()>();
        let sleep_duration = Duration::from_millis(wall_time_ms.max(1));
        let thread_handle = thread::spawn(move || {
            if cancel_rx.recv_timeout(sleep_duration).is_err() {
                engine.increment_epoch();
            }
        });
        Self {
            cancel_tx,
            thread_handle,
        }
    }

    fn cancel(self) {
        let _ = self.cancel_tx.send(());
        let _ = self.thread_handle.join();
    }
}

fn classify_error_text(message: &str) -> Option<WasmErrorKind> {
    let lower = message.to_ascii_lowercase();
    if lower.contains("memory") && (lower.contains("limit") || lower.contains("maximum")) {
        return Some(WasmErrorKind::MemoryLimitExceeded);
    }
    if lower.contains("unsupported")
        && (lower.contains("platform") || lower.contains("windows") || lower.contains("capability"))
    {
        return Some(WasmErrorKind::UnsupportedPlatformCapability);
    }
    None
}

fn wasm_error_marker(kind: WasmErrorKind, message: &str) -> String {
    // Stable marker prefix for machine parsing at Python/policy layers.
    format!(
        "AKC_WASM_ERROR code={} exit_code={} message={}",
        kind.code(),
        kind.exit_code(),
        message
    )
}

fn fuel_budget_from_cpu_fuel(limit: Option<u64>) -> Option<u64> {
    limit.map(|fuel| fuel.clamp(1, WASM_FUEL_MAX))
}

fn effective_cpu_fuel_budget(cpu_fuel: Option<u64>) -> Option<u64> {
    fuel_budget_from_cpu_fuel(cpu_fuel)
}

fn memory_limit_usize(limit_bytes: Option<u64>) -> Result<Option<usize>, ExecutorError> {
    match limit_bytes {
        Some(bytes) => {
            if bytes > (usize::MAX as u64) {
                return Err(ExecutorError::Wasm(format!(
                    "wasm memory_bytes exceeds platform addressable range: {bytes}"
                )));
            }
            Ok(Some(bytes as usize))
        }
        None => Ok(None),
    }
}

fn module_minimum_defined_memory_bytes(module: &Module) -> Option<u64> {
    module
        .exports()
        .filter_map(|export| match export.ty() {
            ExternType::Memory(memory_ty) => Some(memory_ty.minimum()),
            _ => None,
        })
        .max()
        .map(|pages| pages.saturating_mul(WASM_PAGE_SIZE_BYTES))
}

fn wasm_failure_response(
    tenant_id: TenantId,
    run_id: RunId,
    stdout: String,
    stderr: String,
    kind: WasmErrorKind,
    message: String,
) -> ExecResponse {
    let marker = wasm_error_marker(kind, &message);
    let stderr_final = if stderr.is_empty() {
        marker
    } else {
        format!("{marker}\n{stderr}")
    };
    ExecResponse {
        tenant_id,
        run_id,
        ok: false,
        exit_code: kind.exit_code(),
        stdout,
        stderr: stderr_final,
    }
}

fn unsupported_platform_response(
    tenant_id: TenantId,
    run_id: RunId,
    message: impl Into<String>,
) -> ExecResponse {
    wasm_failure_response(
        tenant_id,
        run_id,
        String::new(),
        String::new(),
        WasmErrorKind::UnsupportedPlatformCapability,
        message.into(),
    )
}

fn wasm_platform_preflight(_request: &ExecRequest) -> Option<String> {
    #[cfg(windows)]
    {
        if _request.limits.wall_time_ms.is_some() {
            return Some(
                "Windows WASM lane does not support wall-time enforcement; use Linux/macOS for strict WASM execution or switch to the process/docker lane"
                    .to_string(),
            );
        }
    }

    None
}

fn canonicalize_existing_abs(raw: &str) -> Result<PathBuf, ExecutorError> {
    let p: &Path = ensure_safe_absolute_path_string(raw)?;
    if !p.exists() {
        return Err(ExecutorError::Wasm(format!(
            "wasm preopen path does not exist: {raw}"
        )));
    }
    p.canonicalize().map_err(ExecutorError::from)
}

fn collect_preopens(
    request: &ExecRequest,
) -> Result<Vec<(PathBuf, String, DirPerms, FilePerms)>, ExecutorError> {
    let mut hosts: BTreeSet<PathBuf> = BTreeSet::new();
    let mut host_raw_by_canonical: BTreeMap<PathBuf, String> = BTreeMap::new();
    for raw in request.fs_policy.preopen_dirs.iter() {
        let canonical = canonicalize_existing_abs(raw)?;
        if !canonical.is_dir() {
            return Err(ExecutorError::Wasm(format!(
                "wasm preopen path is not a directory: {}",
                canonical.display()
            )));
        }
        if let Some(first_raw) = host_raw_by_canonical.get(&canonical) {
            return Err(ExecutorError::Wasm(format!(
                "wasm preopen_dirs contains duplicate canonical mount target: {} (from {:?} and {:?})",
                canonical.display(),
                first_raw,
                raw
            )));
        }
        host_raw_by_canonical.insert(canonical.clone(), raw.clone());
        hosts.insert(canonical);
    }
    let mut writable: BTreeSet<PathBuf> = BTreeSet::new();
    for raw in request.fs_policy.allowed_write_paths.iter() {
        let canonical = canonicalize_existing_abs(raw)?;
        if !canonical.is_dir() {
            return Err(ExecutorError::Wasm(format!(
                "wasm allowed_write_paths entry is not a directory: {}",
                canonical.display()
            )));
        }
        writable.insert(canonical);
    }
    for dir in writable.iter() {
        if !hosts.contains(dir) {
            return Err(ExecutorError::Wasm(format!(
                "wasm allowed_write_paths must be subset of preopen_dirs: {}",
                dir.display()
            )));
        }
    }

    // Deterministic host -> guest mapping:
    // - sort by canonical host path (BTreeSet iteration order)
    // - assign stable mount points: /mnt/0, /mnt/1, ...
    Ok(hosts
        .into_iter()
        .enumerate()
        .map(|(idx, host)| {
            let guest = format!("/mnt/{idx}");
            let (dir_perms, file_perms) = if writable.contains(&host) {
                (
                    DirPerms::READ | DirPerms::MUTATE,
                    FilePerms::READ | FilePerms::WRITE,
                )
            } else {
                (DirPerms::READ, FilePerms::READ)
            };
            (host, guest, dir_perms, file_perms)
        })
        .collect())
}

pub(crate) fn run_wasm_lane(request: ExecRequest) -> Result<ExecResponse, ExecutorError> {
    if request.command.is_empty() {
        return Err(ExecutorError::EmptyCommand);
    }
    if !request.fs_policy.allowed_read_paths.is_empty() {
        return Err(ExecutorError::Wasm(
            "allowed_read_paths is unsupported for wasm lane; use preopen_dirs (+ allowed_write_paths for writable mounts)".to_string(),
        ));
    }

    let tenant_id: TenantId = request.tenant_id.clone();
    let run_id: RunId = request.run_id.clone();

    if let Some(message) = wasm_platform_preflight(&request) {
        return Ok(unsupported_platform_response(tenant_id, run_id, message));
    }

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

    let preopens = collect_preopens(&request)?;

    let mut cmd_iter = request.command.into_iter();
    let module_path = cmd_iter
        .next()
        .expect("command is non-empty; we checked above");
    let module_id = compact_artifact_id(Some(&module_path));

    let mut config = Config::new();
    if request.limits.wall_time_ms.is_some() {
        config.epoch_interruption(true);
    }
    if request.limits.cpu_fuel.is_some() {
        config.consume_fuel(true);
    }

    let engine = Engine::new(&config).map_err(|e| ExecutorError::Wasm(e.to_string()))?;
    let module =
        Module::from_file(&engine, &module_path).map_err(|e| ExecutorError::Wasm(e.to_string()))?;

    if let Some(limit_bytes) = request.limits.memory_bytes {
        if let Some(min_required_bytes) = module_minimum_defined_memory_bytes(&module) {
            if min_required_bytes > limit_bytes {
                return Ok(wasm_failure_response(
                    tenant_id,
                    run_id,
                    String::new(),
                    String::new(),
                    WasmErrorKind::MemoryLimitExceeded,
                    format!(
                        "wasm module minimum linear memory ({min_required_bytes} bytes) exceeds configured memory_bytes limit ({limit_bytes} bytes)"
                    ),
                ));
            }
        }
    }

    let args: Vec<String> = std::iter::once(module_path.clone())
        .chain(cmd_iter)
        .collect();

    let stdout_cap: usize = request
        .limits
        .stdout_max_bytes
        .unwrap_or(WASM_DEFAULT_STDIO_MAX_BYTES)
        .min(usize::MAX as u64) as usize;
    let stderr_cap: usize = request
        .limits
        .stderr_max_bytes
        .unwrap_or(WASM_DEFAULT_STDIO_MAX_BYTES)
        .min(usize::MAX as u64) as usize;

    let stdout_pipe = pipe::MemoryOutputPipe::new(stdout_cap);
    let stderr_pipe = pipe::MemoryOutputPipe::new(stderr_cap);

    let mut wasi_builder = WasiCtxBuilder::new();
    wasi_builder.args(&args);
    for (k, v) in filter_request_env(&tenant_id, &run_id, &request.env) {
        wasi_builder.env(&k, &v);
    }
    wasi_builder.env("AKC_TENANT_ID", &tenant_id.0);
    wasi_builder.env("AKC_RUN_ID", &run_id.0);
    wasi_builder.stdout(stdout_pipe.clone());
    wasi_builder.stderr(stderr_pipe.clone());
    for (host, guest, dir_perms, file_perms) in preopens.iter() {
        wasi_builder
            .preopened_dir(host, guest, *dir_perms, *file_perms)
            .map_err(|e| ExecutorError::Wasm(e.to_string()))?;
    }

    let wasi_ctx: WasiP1Ctx = wasi_builder.build_p1();
    let store_limits: StoreLimits = match memory_limit_usize(request.limits.memory_bytes)? {
        Some(bytes) => StoreLimitsBuilder::new().memory_size(bytes).build(),
        None => StoreLimitsBuilder::new().build(),
    };
    let mut store: Store<WasiStoreData> = Store::new(
        &engine,
        WasiStoreData {
            wasi: wasi_ctx,
            limits: store_limits,
        },
    );
    store.limiter(|state| &mut state.limits);

    let cpu_fuel_budget: Option<u64> = effective_cpu_fuel_budget(request.limits.cpu_fuel);
    if let Some(fuel) = cpu_fuel_budget {
        store
            .set_fuel(fuel)
            .map_err(|e| ExecutorError::Wasm(e.to_string()))?;
    }
    let mut linker: Linker<WasiStoreData> = Linker::new(&engine);
    preview1::add_to_linker_sync(&mut linker, |ctx: &mut WasiStoreData| &mut ctx.wasi)
        .map_err(|e| ExecutorError::Wasm(e.to_string()))?;

    let instance = match linker.instantiate(&mut store, &module) {
        Ok(instance) => instance,
        Err(e) => {
            let msg = e.to_string();
            if let Some(kind) = classify_error_text(&msg) {
                return Ok(wasm_failure_response(
                    tenant_id,
                    run_id,
                    String::new(),
                    String::new(),
                    kind,
                    msg,
                ));
            }
            return Err(ExecutorError::Wasm(msg));
        }
    };

    let start = instance
        .get_typed_func::<(), ()>(&mut store, "_start")
        .or_else(|_| instance.get_typed_func::<(), ()>(&mut store, "main"))
        .map_err(|_e| ExecutorError::UnsupportedLane)?;

    let epoch_guard = if request.limits.wall_time_ms.is_some() {
        store.set_epoch_deadline(1);
        store.epoch_deadline_trap();
        Some(EpochDeadlineGuard::start(
            engine.clone(),
            request.limits.wall_time_ms.unwrap_or(1),
        ))
    } else {
        None
    };

    let result = start.call(&mut store, ());
    if let Some(guard) = epoch_guard {
        guard.cancel();
    }

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
            let mut error_kind: WasmErrorKind = WasmErrorKind::RuntimeTrap;
            let ok: bool = false;
            let mut err_text: String = e.to_string();
            let mut trap_kind: Option<&'static str> = None;

            if let Some(trap) = e.downcast_ref::<Trap>() {
                match trap {
                    Trap::Interrupt => {
                        error_kind = WasmErrorKind::Timeout;
                        err_text = "wasm wall-time budget exceeded".to_string();
                        trap_kind = Some("interrupt");
                    }
                    Trap::OutOfFuel => {
                        error_kind = WasmErrorKind::CpuFuelExhausted;
                        err_text = "wasm cpu/fuel budget exhausted".to_string();
                        trap_kind = Some("out_of_fuel");
                    }
                    _ => {}
                }
            }
            if let Some(kind) = classify_error_text(&err_text) {
                if matches!(kind, WasmErrorKind::MemoryLimitExceeded) {
                    trap_kind = Some("memory_limit");
                    err_text = "wasm linear memory limit exceeded".to_string();
                }
                error_kind = kind;
            }

            let exit_code = error_kind.exit_code();
            let marker = wasm_error_marker(error_kind, &err_text);

            let stderr_final = if stderr.is_empty() {
                marker
            } else {
                format!("{marker}\n{stderr}")
            };
            let timed_out = matches!(error_kind, WasmErrorKind::Timeout);

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
                    "timeout": timed_out,
                    "stdout_bytes": stdout.len(),
                    "stderr_bytes": stderr_final.len(),
                    "trap": trap_kind.unwrap_or("unknown"),
                    "error_code": error_kind.code(),
                    "fuel_budget_effective": cpu_fuel_budget,
                    "fuel_budget_wall_time": serde_json::Value::Null,
                    "fuel_budget_cpu": cpu_fuel_budget,
                    "cpu_fuel_limit": request.limits.cpu_fuel,
                    "wall_time_ms_limit": request.limits.wall_time_ms,
                    "memory_limit_bytes": request.limits.memory_bytes,
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
