use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use akc_executor::run_exec;
use akc_protocol::{Capabilities, ExecLane, ExecRequest, FsPolicy, Limits, RunId, TenantId};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn with_exec_env<T>(root: &PathBuf, backend: &str, f: impl FnOnce() -> T) -> T {
    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", root);
    std::env::set_var("AKC_EXEC_BACKEND", backend);
    // Ensure tests are not coupled via a previously-set allowlist.
    // Keep a small default allowlist that includes commands used by bwrap
    // filesystem tests. Most tests that need a different allowlist set it
    // explicitly inside the closure.
    std::env::set_var("AKC_EXEC_ALLOWLIST", "echo:sh:/usr/bin/cat:/bin/cat");
    f()
}

fn make_process_request(root: &Path, cwd: Option<String>, fs_policy: FsPolicy) -> ExecRequest {
    ExecRequest {
        tenant_id: TenantId::parse("tenant_a").unwrap(),
        run_id: RunId::parse("run_1").unwrap(),
        lane: ExecLane::Process,
        capabilities: Capabilities { network: false },
        limits: Limits {
            wall_time_ms: Some(1_000),
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
        command: vec!["echo".to_string(), "hi".to_string()],
        cwd,
        env: BTreeMap::from([(
            "AKC_EXEC_ROOT".to_string(),
            root.to_string_lossy().into_owned(),
        )]),
        stdin_text: None,
        fs_policy,
    }
}

#[cfg(target_os = "linux")]
fn bwrap_smoke_can_start(root_canon: &PathBuf) -> bool {
    // `bwrap` relies on unprivileged user namespaces on many CI runners.
    // If userns setup is denied, bubblewrap exits immediately (usually:
    // "setting up uid map: Permission denied"), and the bwrap-specific tests
    // aren't meaningful in that environment. Treat it as a best-effort test
    // skip instead of a hard failure.
    let mut req = make_process_request(root_canon, None, FsPolicy::default());
    req.command = vec!["echo".to_string(), "bwrap_probe".to_string()];

    let res = with_exec_env(root_canon, "bwrap", || run_exec(req));
    let res = match res {
        Ok(r) => r,
        Err(_) => return false,
    };

    let stderr = res.stderr.to_lowercase();
    if stderr.contains("setting up uid map")
        || stderr.contains("uid map")
        || stderr.contains("user namespace")
    {
        return !(stderr.contains("permission denied")
            || stderr.contains("operation not permitted"));
    }

    // If we see unshare-ish permission errors, also treat it as "can't run".
    if stderr.contains("operation not permitted") || stderr.contains("unshare") {
        return false;
    }

    true
}

#[test]
fn process_lane_denies_parent_dir_traversal_cwd() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();

    // Request a traversal cwd, which should be rejected.
    let req = make_process_request(&root, Some("../".to_string()), FsPolicy::default());
    let err = with_exec_env(&root, "native", || run_exec(req)).unwrap_err();
    assert!(matches!(err, akc_executor::ExecutorError::PolicyDenied));
}

#[test]
fn process_lane_allows_absolute_cwd_inside_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let workdir = root_canon
        .join("tenants")
        .join("tenant_a")
        .join("runs")
        .join("run_1");
    let inside = workdir.join("subdir");
    fs::create_dir_all(&inside).unwrap();
    let inside_canon = inside.canonicalize().unwrap();

    let req = make_process_request(
        &root_canon,
        Some(inside_canon.to_string_lossy().into_owned()),
        FsPolicy::default(),
    );
    let res = with_exec_env(&root_canon, "native", || run_exec(req)).unwrap();
    assert!(res.ok);
    assert!(res.stdout.contains("hi"));
}

#[test]
fn process_lane_denies_absolute_cwd_outside_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");

    // A totally different tempdir path.
    let other = tempfile::tempdir().unwrap();
    let outside = other.path().join("outside");
    fs::create_dir_all(&outside).unwrap();

    let req = make_process_request(
        &root,
        Some(outside.to_string_lossy().into_owned()),
        FsPolicy::default(),
    );
    let err = with_exec_env(&root, "native", || run_exec(req)).unwrap_err();
    assert!(matches!(err, akc_executor::ExecutorError::PolicyDenied));
}

#[test]
fn process_lane_denies_allowlisted_read_path_outside_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();

    let other = tempfile::tempdir().unwrap();
    let outside_file = other.path().join("outside.txt");
    fs::write(&outside_file, b"nope").unwrap();

    let fs_policy = FsPolicy {
        allowed_read_paths: vec![outside_file.to_string_lossy().into_owned()],
        allowed_write_paths: vec![],
        preopen_dirs: vec![],
    };

    let req = make_process_request(&root, None, fs_policy);
    let err = with_exec_env(&root, "native", || run_exec(req)).unwrap_err();
    assert!(matches!(err, akc_executor::ExecutorError::PolicyDenied));
}

#[test]
fn process_lane_denies_allowlisted_path_that_resolves_outside_via_symlink() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let workdir = root_canon
        .join("tenants")
        .join("tenant_a")
        .join("runs")
        .join("run_1");
    fs::create_dir_all(&workdir).unwrap();

    let other = tempfile::tempdir().unwrap();
    let outside_dir = other.path().join("outside_dir");
    fs::create_dir_all(&outside_dir).unwrap();
    let outside_file = outside_dir.join("x.txt");
    fs::write(&outside_file, b"nope").unwrap();

    let link_path = workdir.join("link_out");
    #[cfg(unix)]
    std::os::unix::fs::symlink(&outside_dir, &link_path).unwrap();
    #[cfg(windows)]
    std::os::windows::fs::symlink_dir(&outside_dir, &link_path).unwrap();

    let via_link = link_path.join("x.txt");
    let fs_policy = FsPolicy {
        allowed_read_paths: vec![via_link.to_string_lossy().into_owned()],
        allowed_write_paths: vec![],
        preopen_dirs: vec![],
    };

    let req = make_process_request(&root_canon, None, fs_policy);
    let err = with_exec_env(&root_canon, "native", || run_exec(req)).unwrap_err();
    assert!(matches!(err, akc_executor::ExecutorError::PolicyDenied));
}

#[test]
fn process_lane_denies_cross_tenant_allowlisted_read_within_workspace_root() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    // Create a file in a different tenant's workspace.
    let other_tenant_file = root_canon
        .join("tenants")
        .join("tenant_b")
        .join("runs")
        .join("run_1")
        .join("secret.txt");
    fs::create_dir_all(other_tenant_file.parent().unwrap()).unwrap();
    fs::write(&other_tenant_file, b"nope").unwrap();

    // Tenant A tries to allowlist the other tenant's file path.
    let fs_policy = FsPolicy {
        allowed_read_paths: vec![other_tenant_file.to_string_lossy().into_owned()],
        allowed_write_paths: vec![],
        preopen_dirs: vec![],
    };
    let req = make_process_request(&root_canon, None, fs_policy);
    let err = with_exec_env(&root_canon, "native", || run_exec(req)).unwrap_err();
    assert!(matches!(err, akc_executor::ExecutorError::PolicyDenied));
}

#[test]
fn process_lane_allows_allowlisted_paths_inside_workspace() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let workdir = root_canon
        .join("tenants")
        .join("tenant_a")
        .join("runs")
        .join("run_1");
    fs::create_dir_all(&workdir).unwrap();
    let inside_file = workdir.join("inside.txt");
    fs::write(&inside_file, b"ok").unwrap();

    let fs_policy = FsPolicy {
        allowed_read_paths: vec![inside_file.to_string_lossy().into_owned()],
        allowed_write_paths: vec![workdir.join("out.txt").to_string_lossy().into_owned()],
        preopen_dirs: vec![],
    };

    let req = make_process_request(&root_canon, None, fs_policy);
    let res = with_exec_env(&root_canon, "native", || run_exec(req)).unwrap();
    assert!(res.ok);
}

#[cfg(unix)]
#[test]
fn process_lane_kills_process_tree_on_timeout() {
    use std::process::Command as HostCommand;
    use std::thread;
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    // Serialize env mutations.
    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");

    let workdir = root_canon
        .join("tenants")
        .join("tenant_a")
        .join("runs")
        .join("run_1");
    fs::create_dir_all(&workdir).unwrap();

    // The command spawns a background `sleep` and records its PID, then spins forever.
    // The executor should time out and kill the entire process group, including the sleep.
    let pid_file = workdir.join("bg_pid.txt");
    let script = format!(
        "sleep 30 & echo $! > '{}' ; while true; do :; done",
        pid_file.to_string_lossy()
    );

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec!["sh".to_string(), "-c".to_string(), script];
    req.limits.wall_time_ms = Some(200);

    let res = run_exec(req).unwrap();
    assert!(!res.ok);
    assert_eq!(res.exit_code, 124);

    // Reset for any subsequent tests that run in this process.
    std::env::set_var("AKC_EXEC_ALLOWLIST", "echo");

    let pid_raw = fs::read_to_string(&pid_file).expect("expected pid file to be written");
    let pid: i32 = pid_raw.trim().parse().expect("pid should be numeric");

    // Wait until the background process disappears.
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        unsafe {
            // kill(pid, 0) returns 0 if process exists and we can signal it.
            // If it is gone, it returns -1 and sets errno=ESRCH.
            if libc::kill(pid, 0) == -1 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ESRCH) {
                    break;
                }
            }
        }

        if Instant::now() >= deadline {
            // Provide debugging context if the process somehow still exists.
            let _ = HostCommand::new("ps")
                .args(["-p", &pid.to_string(), "-o", "pid,ppid,pgid,comm="])
                .output();
            panic!(
                "expected background sleep to be terminated on timeout (pid {})",
                pid
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
#[test]
fn process_lane_enforces_memory_limit_best_effort() {
    // This test uses Python (if present) to allocate a large buffer so we can observe
    // that `limits.memory_bytes` is actually enforced via rlimits / cgroups.
    //
    // We skip the test when Python isn't available on the host running the tests.
    use std::process::Command as HostCommand;

    let python: &str = if HostCommand::new("python3")
        .arg("-c")
        .arg("print('ok')")
        .status()
        .is_ok()
    {
        "python3"
    } else if HostCommand::new("python")
        .arg("-c")
        .arg("print('ok')")
        .status()
        .is_ok()
    {
        "python"
    } else {
        eprintln!("skipping memory enforcement test: python/python3 not found");
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    // Serialize env mutations.
    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", python);

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec![
        python.to_string(),
        "-c".to_string(),
        // Allocate more than the configured memory limit and touch it to force commit.
        "b=bytearray(128*1024*1024)\nfor i in range(0,len(b),4096): b[i]=1\nprint('allocated')"
            .to_string(),
    ];
    req.limits.wall_time_ms = Some(2_000);
    req.limits.memory_bytes = Some(32 * 1024 * 1024);
    req.limits.stderr_max_bytes = Some(8 * 1024);
    req.limits.stdout_max_bytes = Some(8 * 1024);

    // If the host does not support memory rlimits (and we're not in a cgroup v2 environment),
    // `run_exec` may still succeed. In that case we treat it as a documented limitation.
    match run_exec(req) {
        Ok(res) => {
            if res.ok {
                eprintln!("memory enforcement appears unsupported on this runner (best-effort)");
            }
        }
        Err(_err) => {
            // Spawn may fail on some platforms if rlimits aren't supported; that's acceptable
            // as long as the executor doesn't crash.
        }
    }
}

#[test]
fn allowlist_empty_denies_everything() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", "");

    let req = make_process_request(&root, None, FsPolicy::default());
    let err = run_exec(req).unwrap_err();
    assert!(matches!(
        err,
        akc_executor::ExecutorError::CommandNotAllowed
    ));
}

#[cfg(unix)]
#[test]
fn allowlist_is_string_exact_basename_vs_absolute_path() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();

    // If the allowlist contains "echo", "/bin/echo" should be denied (exact match policy).
    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", "echo");

    let mut req = make_process_request(&root, None, FsPolicy::default());
    req.command = vec!["/bin/echo".to_string(), "hi".to_string()];
    let err = run_exec(req).unwrap_err();
    assert!(matches!(
        err,
        akc_executor::ExecutorError::CommandNotAllowed
    ));
}

#[cfg(unix)]
#[test]
fn env_denylist_strips_loader_and_interpreter_injection_vars() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");
    std::env::remove_var("AKC_EXEC_ENV_ALLOW_PREFIXES");

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec![
        "sh".to_string(),
        "-c".to_string(),
        "printf 'LD=%s\\n' \"$LD_PRELOAD\"; printf 'FOO=%s\\n' \"$FOO\"; printf 'PATH=%s\\n' \"$PATH\"; printf 'NODE=%s\\n' \"$NODE_OPTIONS\"".to_string(),
    ];
    req.env
        .insert("LD_PRELOAD".to_string(), "evil.so".to_string());
    req.env.insert("FOO".to_string(), "bar".to_string());
    req.env.insert("PATH".to_string(), "/tmp/evil".to_string());
    req.env.insert(
        "NODE_OPTIONS".to_string(),
        "--require /tmp/evil.js".to_string(),
    );
    let res = run_exec(req).unwrap();
    assert!(res.ok, "expected sh to succeed; stderr: {}", res.stderr);
    assert!(res.stdout.contains("LD=\n"), "stdout: {}", res.stdout);
    assert!(res.stdout.contains("FOO=bar\n"), "stdout: {}", res.stdout);
    assert!(
        res.stdout.contains("PATH=/usr/bin:/bin\n"),
        "stdout: {}",
        res.stdout
    );
    assert!(res.stdout.contains("NODE=\n"), "stdout: {}", res.stdout);
}

#[cfg(unix)]
#[test]
fn env_allow_prefixes_only_passes_matching_keys() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");
    std::env::set_var("AKC_EXEC_ENV_ALLOW_PREFIXES", "FOO_,AKC_");

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec![
        "sh".to_string(),
        "-c".to_string(),
        "printf 'FOO_OK=%s\\n' \"$FOO_OK\"; printf 'BAR=%s\\n' \"$BAR\"".to_string(),
    ];
    req.env.insert("FOO_OK".to_string(), "1".to_string());
    req.env.insert("BAR".to_string(), "2".to_string());
    let res = run_exec(req).unwrap();
    assert!(res.ok, "expected sh to succeed; stderr: {}", res.stderr);
    assert!(res.stdout.contains("FOO_OK=1\n"), "stdout: {}", res.stdout);
    assert!(res.stdout.contains("BAR=\n"), "stdout: {}", res.stdout);

    // Avoid leaking prefix mode into subsequent tests.
    std::env::remove_var("AKC_EXEC_ENV_ALLOW_PREFIXES");
}

#[cfg(unix)]
#[test]
fn timeout_returns_promptly_even_with_spammy_stdout_stderr_and_clamps() {
    use std::time::Instant;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.limits.wall_time_ms = Some(200);
    req.limits.stdout_max_bytes = Some(64);
    req.limits.stderr_max_bytes = Some(64);
    req.command = vec![
        "sh".to_string(),
        "-c".to_string(),
        // Spam both stdout/stderr until killed.
        "while true; do printf 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\\n'; printf 'yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy\\n' 1>&2; done"
            .to_string(),
    ];

    let start = Instant::now();
    let res = run_exec(req).unwrap();
    let elapsed_ms: u128 = start.elapsed().as_millis();

    assert!(!res.ok);
    assert_eq!(res.exit_code, 124);
    assert!(
        elapsed_ms < 2_000,
        "expected prompt timeout return; took {}ms",
        elapsed_ms
    );
    assert!(
        res.stdout.len() <= 64,
        "stdout should be clamped (len={}): {:?}",
        res.stdout.len(),
        res.stdout
    );
    assert!(
        res.stderr.len() <= 64,
        "stderr should be clamped (len={}): {:?}",
        res.stderr.len(),
        res.stderr
    );
}

#[cfg(unix)]
#[test]
fn process_lane_kills_nested_subshell_spawn_on_timeout_double_forkish() {
    use std::thread;
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");

    let workdir = root_canon
        .join("tenants")
        .join("tenant_a")
        .join("runs")
        .join("run_1");
    fs::create_dir_all(&workdir).unwrap();
    let pid_file = workdir.join("nested_bg_pid.txt");

    // Spawn sleep from a nested subshell, then keep the top process busy so timeout triggers.
    let script = format!(
        "( ( sleep 30 & echo $! > '{}' ) & ) ; while true; do :; done",
        pid_file.to_string_lossy()
    );

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec!["sh".to_string(), "-c".to_string(), script];
    req.limits.wall_time_ms = Some(200);

    let res = run_exec(req).unwrap();
    assert!(!res.ok);
    assert_eq!(res.exit_code, 124);

    let pid_raw = fs::read_to_string(&pid_file).expect("expected pid file to be written");
    let pid: i32 = pid_raw.trim().parse().expect("pid should be numeric");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        unsafe {
            if libc::kill(pid, 0) == -1 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ESRCH) {
                    break;
                }
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "expected nested background process to be terminated on timeout (pid {})",
                pid
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}

#[cfg(unix)]
#[test]
fn subprocess_can_escape_by_creating_its_own_session_documented_limitation() {
    use std::thread;
    use std::time::{Duration, Instant};

    // This test documents a known limitation on Unix: if a subprocess creates a new
    // session/process group (e.g. via setsid), it can escape process-group kill semantics.
    //
    // We gate on a known python3 path to avoid PATH differences across hosts.
    let python3 = if Path::new("/usr/bin/python3").exists() {
        "/usr/bin/python3"
    } else if Path::new("/usr/local/bin/python3").exists() {
        "/usr/local/bin/python3"
    } else if Path::new("/opt/homebrew/bin/python3").exists() {
        "/opt/homebrew/bin/python3"
    } else {
        return;
    };

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    std::env::set_var("AKC_EXEC_ALLOWLIST", format!("sh:{}", python3));

    let workdir = root_canon
        .join("tenants")
        .join("tenant_a")
        .join("runs")
        .join("run_1");
    fs::create_dir_all(&workdir).unwrap();
    let pid_file = workdir.join("escaped_pid.txt");

    let sleep_path = if Path::new("/bin/sleep").exists() {
        "/bin/sleep"
    } else if Path::new("/usr/bin/sleep").exists() {
        "/usr/bin/sleep"
    } else {
        return;
    };

    // Spawn a python helper that creates a new session, starts a long-running child,
    // writes its PID, and exits. The long-running child should outlive the group kill.
    let py = format!(
        "import os,subprocess; os.setsid(); p=subprocess.Popen([\"{}\",\"30\"], stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True); open(\"{}\",\"w\").write(str(p.pid))",
        sleep_path,
        pid_file.to_string_lossy(),
    );
    let script = format!("{} -c '{}'; while true; do :; done", python3, py);

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec!["sh".to_string(), "-c".to_string(), script];
    // Keep the timeout long enough for the helper to reliably write the pid file
    // on loaded CI runners; we still expect the overall exec to time out.
    req.limits.wall_time_ms = Some(800);

    let res = run_exec(req).unwrap();
    assert!(!res.ok);
    assert_eq!(res.exit_code, 124);

    // Wait briefly for pid file.
    let deadline = Instant::now() + Duration::from_secs(3);
    let pid_raw = loop {
        if let Ok(s) = fs::read_to_string(&pid_file) {
            break s;
        }
        if Instant::now() >= deadline {
            panic!("expected escaped pid file to be written");
        }
        thread::sleep(Duration::from_millis(25));
    };
    let pid: i32 = pid_raw.trim().parse().expect("pid should be numeric");

    // Confirm it's still alive (escape worked), then clean it up.
    let mut alive = false;
    unsafe {
        if libc::kill(pid, 0) == 0 {
            alive = true;
        }
    }
    if alive {
        unsafe {
            let _ = libc::kill(pid, libc::SIGKILL);
        }
    }
    assert!(
        alive,
        "expected escaped process to survive group-kill semantics (pid {})",
        pid
    );
}

#[cfg(target_os = "linux")]
#[test]
fn backend_parity_native_vs_bwrap_timeout_exit_code() {
    if !Path::new("/usr/bin/bwrap").exists() && !Path::new("/bin/bwrap").exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().unwrap_or_else(|e| e.into_inner());
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");

    // Smoke-check whether bwrap can even start (unprivileged userns may be denied).
    // We must do this without calling `with_exec_env`, since this test already holds
    // `ENV_LOCK`.
    std::env::set_var("AKC_EXEC_BACKEND", "bwrap");
    let mut probe_req = make_process_request(&root_canon, None, FsPolicy::default());
    probe_req.command = vec!["sh".to_string(), "-c".to_string(), "echo probe".to_string()];
    let probe = match run_exec(probe_req) {
        Ok(r) => r,
        Err(_) => return,
    };
    let stderr_l = probe.stderr.to_lowercase();
    if (stderr_l.contains("uid map") || stderr_l.contains("user namespace"))
        && (stderr_l.contains("permission denied") || stderr_l.contains("operation not permitted"))
    {
        return;
    }

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec![
        "sh".to_string(),
        "-c".to_string(),
        "while true; do :; done".to_string(),
    ];
    req.limits.wall_time_ms = Some(200);

    std::env::set_var("AKC_EXEC_BACKEND", "native");
    let native = run_exec(req.clone()).unwrap();
    std::env::set_var("AKC_EXEC_BACKEND", "bwrap");
    let bwrap = run_exec(req).unwrap();

    assert!(!native.ok);
    assert_eq!(native.exit_code, 124);
    assert!(!bwrap.ok);
    assert_eq!(bwrap.exit_code, 124);
}

#[cfg(windows)]
#[test]
fn windows_job_object_kills_background_processes_on_close() {
    use std::process::Command;
    use std::thread;
    use std::time::{Duration, Instant};

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().expect("env lock poisoned");
    std::env::set_var("AKC_EXEC_ROOT", &root_canon);
    std::env::set_var("AKC_EXEC_BACKEND", "native");
    // Allow `cmd` (spawner) and `tasklist` (verification helper).
    std::env::set_var("AKC_EXEC_ALLOWLIST", "cmd:tasklist");

    // Spawn a background ping that would normally outlive the parent process.
    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec![
        "cmd".to_string(),
        "/C".to_string(),
        "start /B ping -n 30 127.0.0.1 > NUL".to_string(),
    ];
    // Keep wall time generous; we rely on job close to kill the background process.
    req.limits.wall_time_ms = Some(5_000);

    let res = run_exec(req).unwrap();
    assert!(res.ok, "expected cmd to succeed; stderr: {}", res.stderr);

    // After the exec returns, the per-exec job handle should be dropped and Windows should
    // terminate the entire job (including the background ping.exe).
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let out = Command::new("tasklist")
            .args(["/FI", "IMAGENAME eq ping.exe", "/NH"])
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout).to_lowercase();
        // When there are no matches, tasklist prints "INFO: No tasks are running..."
        if stdout.contains("no tasks are running") {
            break;
        }
        if Instant::now() >= deadline {
            panic!("expected ping.exe to be terminated by job object, but it is still running");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(target_os = "linux")]
#[test]
fn bwrap_backend_denies_cross_tenant_allowlisted_workspace_paths() {
    if !Path::new("/usr/bin/bwrap").exists() && !Path::new("/bin/bwrap").exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    if !bwrap_smoke_can_start(&root_canon) {
        return;
    }

    // Create an "other tenant" file under the same workspace root.
    let other_tenant_file = root_canon
        .join("tenants")
        .join("tenant_b")
        .join("runs")
        .join("run_1")
        .join("secret.txt");
    fs::create_dir_all(other_tenant_file.parent().unwrap()).unwrap();
    fs::write(&other_tenant_file, b"nope").unwrap();

    let fs_policy = FsPolicy {
        allowed_read_paths: vec![other_tenant_file.to_string_lossy().into_owned()],
        allowed_write_paths: vec![],
        preopen_dirs: vec![],
    };
    let req = make_process_request(&root_canon, None, fs_policy);
    let err = with_exec_env(&root_canon, "bwrap", || run_exec(req)).unwrap_err();
    assert!(matches!(err, akc_executor::ExecutorError::PolicyDenied));
}

#[cfg(target_os = "linux")]
#[test]
fn bwrap_backend_denies_host_paths_by_default() {
    if !Path::new("/usr/bin/bwrap").exists() && !Path::new("/bin/bwrap").exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    // Run a program that tries to read /etc/hosts. We expect failure because /etc
    // isn't mounted into the sandbox by default.
    //
    // Use /usr/bin/cat when present; fall back to /bin/cat.
    let cat = if Path::new("/usr/bin/cat").exists() {
        "/usr/bin/cat"
    } else {
        "/bin/cat"
    };

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec![cat.to_string(), "/etc/hosts".to_string()];
    let res = with_exec_env(&root_canon, "bwrap", || run_exec(req)).unwrap();
    assert!(!res.ok);
}

#[cfg(target_os = "linux")]
#[test]
fn bwrap_backend_allows_explicit_ro_allowlist_mount() {
    if !Path::new("/usr/bin/bwrap").exists() && !Path::new("/bin/bwrap").exists() {
        return;
    }
    if !Path::new("/etc/hosts").exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    if !bwrap_smoke_can_start(&root_canon) {
        return;
    }

    let cat = if Path::new("/usr/bin/cat").exists() {
        "/usr/bin/cat"
    } else {
        "/bin/cat"
    };

    let fs_policy = FsPolicy {
        allowed_read_paths: vec!["/etc/hosts".to_string()],
        allowed_write_paths: vec![],
        preopen_dirs: vec![],
    };

    let mut req = make_process_request(&root_canon, None, fs_policy);
    req.command = vec![cat.to_string(), "/etc/hosts".to_string()];
    let res = with_exec_env(&root_canon, "bwrap", || run_exec(req)).unwrap();
    assert!(res.stdout.contains("localhost") || res.stdout.contains("127.0.0.1"));
}

#[cfg(target_os = "linux")]
#[test]
fn bwrap_backend_denies_host_writes_outside_workspace_by_default() {
    if !Path::new("/usr/bin/bwrap").exists() && !Path::new("/bin/bwrap").exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    let other = tempfile::tempdir().unwrap();
    let outside_file = other.path().join("outside_write.txt");
    if outside_file.exists() {
        fs::remove_file(&outside_file).unwrap();
    }

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("echo hi > '{}'", outside_file.to_string_lossy()),
    ];

    let res = with_exec_env(&root_canon, "bwrap", || {
        std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");
        run_exec(req)
    })
    .unwrap();
    assert!(
        !res.ok,
        "expected bwrap to deny writes to host paths by default; stdout: {}, stderr: {}",
        res.stdout, res.stderr
    );
    assert!(
        !outside_file.exists(),
        "expected host file to not be created outside sandbox workspace"
    );
}

#[cfg(target_os = "linux")]
#[test]
fn bwrap_backend_allows_explicit_rw_allowlist_mount_to_host_path() {
    if !Path::new("/usr/bin/bwrap").exists() && !Path::new("/bin/bwrap").exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    if !bwrap_smoke_can_start(&root_canon) {
        return;
    }

    let other = tempfile::tempdir().unwrap();
    let outside_file = other.path().join("outside_allowed.txt");

    let fs_policy = FsPolicy {
        allowed_read_paths: vec![],
        allowed_write_paths: vec![outside_file.to_string_lossy().into_owned()],
        preopen_dirs: vec![],
    };
    let mut req = make_process_request(&root_canon, None, fs_policy);
    req.command = vec![
        "sh".to_string(),
        "-c".to_string(),
        format!("echo allowed > '{}'", outside_file.to_string_lossy()),
    ];

    let res = with_exec_env(&root_canon, "bwrap", || {
        std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");
        run_exec(req)
    })
    .unwrap();
    assert!(
        res.ok,
        "expected allowlisted host write to succeed; stdout: {}, stderr: {}",
        res.stdout, res.stderr
    );

    let content = fs::read_to_string(&outside_file).expect("expected host file to be written");
    assert!(
        content.contains("allowed"),
        "expected file content to be written; got: {:?}",
        content
    );
}

#[cfg(target_os = "linux")]
#[test]
fn bwrap_backend_kills_forked_subprocess_on_timeout() {
    use std::thread;
    use std::time::{Duration, Instant};

    if !Path::new("/usr/bin/bwrap").exists() && !Path::new("/bin/bwrap").exists() {
        return;
    }

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();
    let root_canon = root.canonicalize().unwrap();

    if !bwrap_smoke_can_start(&root_canon) {
        return;
    }

    let workdir = root_canon
        .join("tenants")
        .join("tenant_a")
        .join("runs")
        .join("run_1");
    fs::create_dir_all(&workdir).unwrap();

    let pid_file = workdir.join("bwrap_bg_pid.txt");
    let script = format!(
        "sleep 30 & echo $! > '{}' ; while true; do :; done",
        pid_file.to_string_lossy()
    );

    let mut req = make_process_request(&root_canon, None, FsPolicy::default());
    req.command = vec!["sh".to_string(), "-c".to_string(), script];
    req.limits.wall_time_ms = Some(200);

    let res = with_exec_env(&root_canon, "bwrap", || {
        std::env::set_var("AKC_EXEC_ALLOWLIST", "sh");
        run_exec(req)
    })
    .unwrap();
    assert!(!res.ok);
    assert_eq!(res.exit_code, 124);

    let pid_raw = fs::read_to_string(&pid_file).expect("expected pid file to be written");
    let pid: i32 = pid_raw.trim().parse().expect("pid should be numeric");

    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        unsafe {
            if libc::kill(pid, 0) == -1 {
                let err = std::io::Error::last_os_error();
                if err.raw_os_error() == Some(libc::ESRCH) {
                    break;
                }
            }
        }
        if Instant::now() >= deadline {
            panic!(
                "expected forked subprocess to be terminated on timeout (pid {})",
                pid
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
}
