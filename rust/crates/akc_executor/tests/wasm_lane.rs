use std::collections::BTreeMap;
use std::fs;

use akc_executor::run_exec;
use akc_protocol::{Capabilities, ExecLane, ExecRequest, Limits, RunId, TenantId};

fn make_request(module_path: String) -> ExecRequest {
    ExecRequest {
        tenant_id: TenantId::parse("tenant_a").unwrap(),
        run_id: RunId::parse("run_1").unwrap(),
        lane: ExecLane::Wasm,
        capabilities: Capabilities { network: false },
        limits: Limits {
            wall_time_ms: Some(250),
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
        command: vec![module_path],
        cwd: None,
        env: BTreeMap::new(),
        stdin_text: None,
    }
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
