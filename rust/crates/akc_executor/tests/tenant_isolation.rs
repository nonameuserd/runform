use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use akc_executor::run_exec;
use akc_protocol::{Capabilities, ExecLane, ExecRequest, Limits, RunId, TenantId};

static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

fn with_exec_root<T>(root: &PathBuf, f: impl FnOnce() -> T) -> T {
    let lock = ENV_LOCK.get_or_init(|| Mutex::new(()));
    let _guard = lock.lock().expect("env lock poisoned");
    std::env::set_var("AKC_EXEC_ROOT", root);
    f()
}

fn make_process_request(root: &Path, cwd: Option<String>) -> ExecRequest {
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
    }
}

#[test]
fn process_lane_denies_parent_dir_traversal_cwd() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join(".akc");
    fs::create_dir_all(&root).unwrap();

    // Request a traversal cwd, which should be rejected.
    let req = make_process_request(&root, Some("../".to_string()));
    let err = with_exec_root(&root, || run_exec(req)).unwrap_err();
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
    );
    let res = with_exec_root(&root_canon, || run_exec(req)).unwrap();
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

    let req = make_process_request(&root, Some(outside.to_string_lossy().into_owned()));
    let err = with_exec_root(&root, || run_exec(req)).unwrap_err();
    assert!(matches!(err, akc_executor::ExecutorError::PolicyDenied));
}
