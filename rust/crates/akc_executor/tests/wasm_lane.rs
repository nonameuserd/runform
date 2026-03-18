use std::collections::BTreeMap;
use std::fs;

use akc_executor::run_exec;
use akc_protocol::{Capabilities, ExecLane, ExecRequest, FsPolicy, Limits, RunId, TenantId};

fn make_request_with_limits(module_path: String, limits: Limits) -> ExecRequest {
    ExecRequest {
        tenant_id: TenantId::parse("tenant_a").unwrap(),
        run_id: RunId::parse("run_1").unwrap(),
        lane: ExecLane::Wasm,
        capabilities: Capabilities { network: false },
        limits,
        command: vec![module_path],
        cwd: None,
        env: BTreeMap::new(),
        stdin_text: None,
        fs_policy: FsPolicy::default(),
    }
}

fn make_request(module_path: String) -> ExecRequest {
    make_request_with_limits(
        module_path,
        Limits {
            wall_time_ms: Some(250),
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
    )
}

#[test]
fn wasm_lane_runs_simple_wasi_module() {
    // A minimal WASI module that prints "hi\n" then exits.
    //
    // (module
    //   (import "wasi_snapshot_preview1" "fd_write"
    //     (func $fd_write (param i32 i32 i32 i32) (result i32)))
    //   (memory 1) (export "memory" (memory 0))
    //   (data (i32.const 8) "hi\n")
    //   (func $_start (export "_start")
    //     (i32.store (i32.const 0) (i32.const 8))   ;; iov.ptr
    //     (i32.store (i32.const 4) (i32.const 3))   ;; iov.len
    //     (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 20))
    //     drop))
    let wat = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 8) "hi\n")
          (func $_start (export "_start")
            (i32.store (i32.const 0) (i32.const 8))
            (i32.store (i32.const 4) (i32.const 3))
            (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 20))
            drop))
    "#;

    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("hello.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request(wasm_path.to_string_lossy().into_owned());
    let res = run_exec(req).unwrap();

    assert!(res.ok);
    assert_eq!(res.exit_code, 0);
    assert!(res.stdout.contains("hi"));
}

#[test]
fn wasm_lane_times_out_on_infinite_loop() {
    let wat = r#"
        (module
          (func $_start (export "_start")
            (loop $top
              br $top)))
    "#;

    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("loop.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request(wasm_path.to_string_lossy().into_owned());
    let res = run_exec(req).unwrap();

    assert!(!res.ok);
    assert!(
        res.exit_code == 124 || res.exit_code == 137,
        "expected timeout-like exit code, got {} (stderr: {})",
        res.exit_code,
        res.stderr
    );
}

#[test]
fn wasm_lane_truncates_stdout_to_limit() {
    // Writes more than `stdout_max_bytes` to FD 1 via WASI `fd_write`.
    let wat = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 64) "0123456789")
          (func $_start (export "_start")
            (local $i i32)
            ;; iov at 0: { ptr=64, len=10 }
            (i32.store (i32.const 0) (i32.const 64))
            (i32.store (i32.const 4) (i32.const 10))
            (local.set $i (i32.const 0))
            (loop $top
              (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 20))
              drop
              (local.set $i (i32.add (local.get $i) (i32.const 1)))
              (br_if $top (i32.lt_u (local.get $i) (i32.const 500))))))
    "#;

    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("spam_stdout.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: Some(250),
            memory_bytes: None,
            stdout_max_bytes: Some(64),
            stderr_max_bytes: Some(1024),
        },
    );
    let res = run_exec(req).unwrap();

    assert!(res.ok, "expected ok, stderr: {}", res.stderr);
    assert_eq!(res.exit_code, 0);
    assert_eq!(res.stdout.len(), 64, "stdout was not truncated as expected");
    assert!(
        res.stdout.chars().all(|c| c.is_ascii_digit()),
        "unexpected stdout content"
    );
}

#[test]
fn wasm_lane_truncates_stderr_to_limit() {
    // Writes more than `stderr_max_bytes` to FD 2 via WASI `fd_write`.
    let wat = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 64) "abcdefghij")
          (func $_start (export "_start")
            (local $i i32)
            ;; iov at 0: { ptr=64, len=10 }
            (i32.store (i32.const 0) (i32.const 64))
            (i32.store (i32.const 4) (i32.const 10))
            (local.set $i (i32.const 0))
            (loop $top
              (call $fd_write (i32.const 2) (i32.const 0) (i32.const 1) (i32.const 20))
              drop
              (local.set $i (i32.add (local.get $i) (i32.const 1)))
              (br_if $top (i32.lt_u (local.get $i) (i32.const 500))))))
    "#;

    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("spam_stderr.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: Some(250),
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(64),
        },
    );
    let res = run_exec(req).unwrap();

    assert!(res.ok, "expected ok, stderr: {}", res.stderr);
    assert_eq!(res.exit_code, 0);
    assert_eq!(res.stderr.len(), 64, "stderr was not truncated as expected");
    assert!(
        res.stderr.chars().all(|c| c.is_ascii_lowercase()),
        "unexpected stderr content"
    );
}
