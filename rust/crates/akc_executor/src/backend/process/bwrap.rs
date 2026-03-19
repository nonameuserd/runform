use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::{ExecRequest, ExecResponse, RunId, TenantId};
use serde_json::json;

use crate::backend::process::limits;
use crate::env_policy::filter_request_env;
use crate::util::{
    canonicalize_within, clamp_bytes, ensure_safe_absolute_path_string, workspace_root,
};
use crate::ExecutorError;

#[cfg(unix)]
use std::os::unix::process::CommandExt as _;

fn bwrap_path() -> Option<PathBuf> {
    let candidates: [&str; 3] = ["/usr/bin/bwrap", "/bin/bwrap", "bwrap"];
    for c in candidates {
        let p = PathBuf::from(c);
        if p.is_absolute() {
            if p.exists() {
                return Some(p);
            }
        } else {
            // Best-effort: rely on PATH lookup in Command; we can't pre-check without `which`.
            return Some(p);
        }
    }
    None
}

fn push_ro_bind_if_exists(args: &mut Vec<String>, host: &str) {
    if Path::new(host).exists() {
        args.push("--ro-bind".to_string());
        args.push(host.to_string());
        args.push(host.to_string());
    }
}

#[cfg(target_os = "linux")]
fn has_cap_net_admin() -> bool {
    // Used to decide whether `--unshare-net` is likely to succeed.
    // Without CAP_NET_ADMIN, bubblewrap may fail configuring loopback
    // (e.g. `RTM_NEWADDR: Operation not permitted`), which should not
    // prevent running sandboxed processes in best-effort environments.
    let Ok(status) = std::fs::read_to_string("/proc/self/status") else {
        return false;
    };
    let cap_eff_line = status
        .lines()
        .find(|l| l.starts_with("CapEff:"))
        .unwrap_or_default();

    // CapEff is a hex bitmap (often 16 hex digits) like: `CapEff:\t0000000000000400`
    let hex = cap_eff_line.trim_start_matches("CapEff:").trim();
    let Ok(v) = u64::from_str_radix(hex, 16) else {
        return false;
    };

    // CAP_NET_ADMIN is capability number 12.
    let cap_net_admin_bit: u64 = 1u64 << 12;
    (v & cap_net_admin_bit) != 0
}

#[cfg(not(target_os = "linux"))]
fn has_cap_net_admin() -> bool {
    false
}

fn is_program_allowed(program: &str) -> bool {
    if let Ok(raw) = std::env::var("AKC_EXEC_ALLOWLIST") {
        if raw.trim().is_empty() {
            return false;
        }
        return raw.split(':').any(|entry| entry == program);
    }
    matches!(program, "echo")
}

pub(crate) fn run_process_lane_bwrap(
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

    let bwrap = bwrap_path().ok_or(ExecutorError::PolicyDenied)?;

    let mut args: Vec<String> = vec![
        "--die-with-parent".to_string(),
        "--new-session".to_string(),
        "--proc".to_string(),
        "/proc".to_string(),
        "--dev".to_string(),
        "/dev".to_string(),
        "--tmpfs".to_string(),
        "/tmp".to_string(),
    ];

    // Best-effort network isolation:
    // - when the caller indicates networking should be disabled, prefer unsharing net
    // - but if we can't (missing CAP_NET_ADMIN), skip it instead of failing exec
    if !request.capabilities.network && has_cap_net_admin() {
        args.insert(2, "--unshare-net".to_string());
    }

    // Allowlisted runtime mounts required for most dynamically-linked binaries.
    // These are mounted read-only; the workspace remains the only RW tree by default.
    push_ro_bind_if_exists(&mut args, "/usr");
    push_ro_bind_if_exists(&mut args, "/bin");
    push_ro_bind_if_exists(&mut args, "/lib");
    push_ro_bind_if_exists(&mut args, "/lib64");

    // Workspace RW (tenant/run scoped).
    let workdir_s: String = workdir.to_string_lossy().into_owned();
    args.push("--bind".to_string());
    args.push(workdir_s.clone());
    args.push(workdir_s.clone());

    // Ensure parent directories exist in the sandbox for file binds (e.g. /etc/hosts).
    let mut ensure_dirs: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    for p in request.fs_policy.allowed_read_paths.iter() {
        let host: &Path = ensure_safe_absolute_path_string(p)?;
        if let Some(parent) = host.parent() {
            if parent != Path::new("/") {
                ensure_dirs.insert(parent.to_string_lossy().into_owned());
            }
        }
    }
    for p in request.fs_policy.allowed_write_paths.iter() {
        let host: &Path = ensure_safe_absolute_path_string(p)?;
        if let Some(parent) = host.parent() {
            if parent != Path::new("/") {
                ensure_dirs.insert(parent.to_string_lossy().into_owned());
            }
        }
    }
    for d in ensure_dirs {
        args.push("--dir".to_string());
        args.push(d);
    }

    // Explicit allowlisted mounts.
    for p in request.fs_policy.allowed_read_paths.iter() {
        let host: &Path = ensure_safe_absolute_path_string(p)?;
        let canon = host.canonicalize()?;
        let canon_s = canon.to_string_lossy().into_owned();
        args.push("--ro-bind".to_string());
        args.push(canon_s.clone());
        args.push(canon_s);
    }
    for p in request.fs_policy.allowed_write_paths.iter() {
        let host: &Path = ensure_safe_absolute_path_string(p)?;
        if host.exists() {
            let canon = host.canonicalize()?;
            let canon_s = canon.to_string_lossy().into_owned();
            args.push("--bind".to_string());
            args.push(canon_s.clone());
            args.push(canon_s);
        } else {
            let parent: &Path = host.parent().ok_or(ExecutorError::PolicyDenied)?;
            if !parent.exists() {
                return Err(ExecutorError::PolicyDenied);
            }
            let parent_canon = parent.canonicalize()?;
            let parent_s = parent_canon.to_string_lossy().into_owned();
            args.push("--bind".to_string());
            args.push(parent_s.clone());
            args.push(parent_s);
        }
    }

    // chdir: prefer request cwd (if present), otherwise workdir.
    let chdir_target: String = if let Some(cwd_raw) = request.cwd.as_deref() {
        let cwd_path = PathBuf::from(cwd_raw);
        let resolved_raw = if cwd_path.is_absolute() {
            cwd_path
        } else {
            if cwd_path
                .components()
                .any(|c| matches!(c, std::path::Component::ParentDir))
            {
                return Err(ExecutorError::PolicyDenied);
            }
            workdir.join(cwd_path)
        };
        // Canonicalize and ensure within workspace (still a useful guard even under bwrap).
        let resolved = canonicalize_within(&workdir, &resolved_raw)?;
        resolved.to_string_lossy().into_owned()
    } else {
        workdir_s.clone()
    };
    args.push("--chdir".to_string());
    args.push(chdir_target);

    // Program + args.
    args.push("--".to_string());
    args.push(program.clone());
    args.extend(cmd_iter);

    let mut child = Command::new(bwrap);
    child.args(args);

    // Scrub env and pass only intended vars.
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
    let cgroup_guard =
        limits::LinuxCgroupV2Guard::try_create(&tenant_id, &run_id, request.limits.memory_bytes);

    // On Unix, create a fresh session so we can kill the whole process tree by
    // signaling the process group on timeout.
    //
    // Note: bubblewrap is already invoked with `--new-session`, but doing it at
    // the OS spawn layer keeps the timeout semantics consistent across backends.
    #[cfg(unix)]
    {
        let memory_bytes: Option<u64> = request.limits.memory_bytes;
        unsafe {
            child.pre_exec(move || {
                if libc::setsid() == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                limits::try_apply_unix_memory_rlimits(memory_bytes)?;
                Ok(())
            });
        }
    }

    let mut child = child.spawn()?;
    #[cfg(unix)]
    let child_pgid: i32 = child.id() as i32;

    if let Some(ref guard) = cgroup_guard {
        let _ = guard.add_pid(child.id());
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
                break child.wait_with_output()?;
            }
            if start.elapsed() >= deadline_delta {
                #[cfg(unix)]
                unsafe {
                    let _ = libc::kill(-child_pgid, libc::SIGKILL);
                }
                #[cfg(not(unix))]
                {
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
                        "backend": "bwrap",
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
                    exit_code: 124,
                    stdout,
                    stderr,
                });
            }
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
            "backend": "bwrap",
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
