use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::{ExecRequest, ExecResponse, RunId, TenantId};
use serde_json::json;

use crate::backend::process::limits;
use crate::env_policy::filter_request_env;
use crate::fs_policy::enforce_fs_policy;
use crate::util::{canonicalize_within, clamp_bytes, workspace_root};
use crate::ExecutorError;

#[cfg(unix)]
use std::os::unix::process::CommandExt as _;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecBackend {
    Native,
    #[cfg(target_os = "linux")]
    Bwrap,
    Docker,
}

pub(crate) fn parse_exec_backend() -> ExecBackend {
    let raw = env::var("AKC_EXEC_BACKEND").unwrap_or_else(|_| "native".to_string());
    match raw.as_str() {
        "native" => ExecBackend::Native,
        #[cfg(target_os = "linux")]
        "bwrap" => ExecBackend::Bwrap,
        "docker" => ExecBackend::Docker,
        _ => ExecBackend::Native,
    }
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

pub(crate) fn run_process_lane_native(
    mut request: ExecRequest,
) -> Result<ExecResponse, ExecutorError> {
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

    // In the portable native backend we only support allowlisting paths within the
    // tenant/run workspace.
    enforce_fs_policy(&workdir, &request.fs_policy)?;

    let command: Vec<String> = std::mem::take(&mut request.command);
    let mut cmd_iter = command.into_iter();
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
    for (k, v) in filter_request_env(&tenant_id, &run_id, &request.env) {
        child.env(k, v);
    }

    child.stdin(Stdio::piped());
    child.stdout(Stdio::piped());
    child.stderr(Stdio::piped());

    // Optional Linux cgroup v2 memory limiter (best-effort; still apply rlimits below).
    #[cfg(target_os = "linux")]
    let cgroup_guard =
        limits::LinuxCgroupV2Guard::try_create(&tenant_id, &run_id, request.limits.memory_bytes);

    // On Unix, create a fresh session so we can kill the entire process tree by
    // signaling the process group on timeout.
    #[cfg(unix)]
    {
        let memory_bytes: Option<u64> = request.limits.memory_bytes;
        unsafe {
            child.pre_exec(move || {
                // setsid() makes the child the leader of a new session and process group.
                // If it fails, abort spawn so we don't lose process-tree kill semantics.
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                // Best-effort memory enforcement on Unix (macOS/Linux/etc).
                limits::try_apply_unix_memory_rlimits(memory_bytes)?;
                Ok(())
            });
        }
    }

    let mut child = child.spawn()?;
    #[cfg(unix)]
    let child_pgid: i32 = child.id() as i32;

    #[cfg(target_os = "linux")]
    if let Some(ref guard) = cgroup_guard {
        // Best-effort: if adding fails, proceed with rlimits-only.
        let _ = guard.add_pid(child.id());
    }

    #[cfg(windows)]
    let _job_setup = crate::windows_job::try_create_and_assign_job(
        &child,
        request.limits.memory_bytes,
        request.limits.wall_time_ms,
    );
    #[cfg(windows)]
    {
        let report = &_job_setup.report;
        let level = if report.job_assigned {
            LogLevel::Info
        } else {
            // If job assignment fails, we may not be able to guarantee process-tree cleanup.
            LogLevel::Warn
        };
        log_event(
            level,
            "exec_windows_job_setup",
            &tenant_id,
            &run_id,
            json!({
                "job_assigned": report.job_assigned,
                "memory_limit_set": report.memory_limit_set,
                "memory_bytes": request.limits.memory_bytes,
                "cpu_rate_percent": report.cpu_rate_percent,
                "cpu_rate_set": report.cpu_rate_set,
                "priority_class": report.priority_class,
            }),
        );
    }

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
                // Kill semantics must terminate the full process tree.
                // On Windows, a per-exec Job Object (when created) guarantees this.
                #[cfg(windows)]
                {
                    if let Some(job) = _job_setup.job.as_ref() {
                        job.terminate();
                    } else {
                        let _ = child.kill();
                    }
                }
                #[cfg(unix)]
                {
                    // Kill the whole process group (negative PID targets PGID).
                    unsafe {
                        let _ = libc::kill(-child_pgid, libc::SIGKILL);
                    }
                }
                #[cfg(all(not(windows), not(unix)))]
                {
                    // Best-effort kill on timeout; still collect whatever output exists.
                    let _ = child.kill();
                }
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

pub(crate) fn run_process_lane_docker_denied(
    request: ExecRequest,
) -> Result<ExecResponse, ExecutorError> {
    let tenant_id: TenantId = request.tenant_id.clone();
    let run_id: RunId = request.run_id.clone();
    log_event(
        LogLevel::Warn,
        "exec_policy_denied",
        &tenant_id,
        &run_id,
        json!({
            "reason": "docker_backend_not_implemented",
        }),
    );
    Err(ExecutorError::PolicyDenied)
}
