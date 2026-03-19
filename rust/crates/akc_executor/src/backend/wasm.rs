use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::{ExecRequest, ExecResponse, RunId, TenantId};
use serde_json::json;
use std::fs;
use wasmtime::{Config, Engine, Linker, Module, Store, Trap};
use wasmtime_wasi::p2::pipe;
use wasmtime_wasi::preview1::{self, WasiP1Ctx};
use wasmtime_wasi::WasiCtxBuilder;

use crate::env_policy::filter_request_env;
use crate::util::{compact_artifact_id, workspace_root};
use crate::ExecutorError;

pub(crate) fn run_wasm_lane(request: ExecRequest) -> Result<ExecResponse, ExecutorError> {
    if request.command.is_empty() {
        return Err(ExecutorError::EmptyCommand);
    }

    #[cfg(windows)]
    if request.limits.wall_time_ms.is_some() {
        return Err(ExecutorError::Wasm(
            "wasm wall-time limits are currently unsupported on Windows".to_string(),
        ));
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

    let mut config = Config::new();
    // Wall-timeouts are enforced via fuel exhaustion (`Trap::OutOfFuel`) on
    // supported platforms. Windows is rejected above because Wasmtime aborts
    // the process on the timeout path in our CI environment.
    config.consume_fuel(true);

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

    let mut wasi_builder = WasiCtxBuilder::new();
    wasi_builder.args(&args);
    for (k, v) in filter_request_env(&tenant_id, &run_id, &request.env) {
        wasi_builder.env(&k, &v);
    }
    wasi_builder.env("AKC_TENANT_ID", &tenant_id.0);
    wasi_builder.env("AKC_RUN_ID", &run_id.0);
    wasi_builder.stdout(stdout_pipe.clone());
    wasi_builder.stderr(stderr_pipe.clone());

    let wasi_ctx: WasiP1Ctx = wasi_builder.build_p1();
    let mut store: Store<WasiP1Ctx> = Store::new(&engine, wasi_ctx);

    let fuel: u64 = request
        .limits
        .wall_time_ms
        .map(|ms| ms.saturating_mul(50_000).clamp(5_000_000, 2_000_000_000))
        .unwrap_or(50_000_000);
    store
        .set_fuel(fuel)
        .map_err(|e| ExecutorError::Wasm(e.to_string()))?;

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
            let timed_out = matches!(trap_kind, Some("interrupt" | "out_of_fuel"));

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
