use std::collections::BTreeMap;
use std::fs;

use akc_executor::run_exec;
use akc_executor::ExecutorError;
use akc_executor::{WASM_EXIT_MEMORY_LIMIT_EXCEEDED, WASM_EXIT_TIMEOUT};
use akc_protocol::{Capabilities, ExecLane, ExecRequest, FsPolicy, Limits, RunId, TenantId};

/// Minimal rights for `path_open` with `creat|trunc` + `fd_read|fd_write` on the new fd.
/// Matches `$rights` bit layout in `wasi_snapshot_preview1.witx` (see `typenames.witx`).
const PATH_OPEN_RIGHTS: i64 = (1i64 << 1)   // fd_read
    + (1i64 << 6)   // fd_write
    + (1i64 << 10)  // path_create_file (oflags::creat)
    + (1i64 << 13)  // path_open
    + (1i64 << 19); // path_filestat_set_size (oflags::trunc)

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
            wall_time_ms: None,
            cpu_fuel: None,
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

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: Some(250),
            cpu_fuel: None,
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
    );
    let result = run_exec(req);

    #[cfg(windows)]
    {
        let res = result.unwrap();
        assert!(!res.ok);
        assert_eq!(
            res.exit_code,
            akc_executor::WASM_EXIT_UNSUPPORTED_PLATFORM_CAPABILITY
        );
        assert!(
            res.stderr.contains("AKC_WASM_ERROR"),
            "expected structured wasm marker, got: {}",
            res.stderr
        );
    }

    #[cfg(not(windows))]
    {
        let res = result.unwrap();
        assert!(!res.ok);
        assert_eq!(
            res.exit_code, WASM_EXIT_TIMEOUT,
            "expected deterministic timeout code, got {} (stderr: {})",
            res.exit_code, res.stderr
        );
        assert!(
            res.stderr.contains("AKC_WASM_ERROR"),
            "expected structured wasm marker, got: {}",
            res.stderr
        );
    }
}

#[test]
fn wasm_lane_memory_limit_exceeded_has_stable_error_code() {
    // Module requires 2 pages (128KiB) but runtime cap is 1 page (64KiB).
    let wat = r#"
        (module
          (memory 2)
          (func $_start (export "_start")))
    "#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("mem_grow.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: Some(1_000),
            cpu_fuel: None,
            memory_bytes: Some(64 * 1024),
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
    );
    let res = run_exec(req).unwrap();

    assert!(!res.ok, "expected memory limit failure");
    assert_eq!(res.exit_code, WASM_EXIT_MEMORY_LIMIT_EXCEEDED);
    assert!(
        res.stderr.contains("WASM_MEMORY_LIMIT_EXCEEDED"),
        "expected deterministic memory-limit code marker, stderr={:?}",
        res.stderr
    );
}

#[test]
fn wasm_lane_cpu_fuel_exhaustion_has_stable_error_code() {
    let wat = r#"
        (module
          (func $_start (export "_start")
            (loop $top
              br $top)))
    "#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("cpu_loop.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: Some(5_000),
            cpu_fuel: Some(10_000),
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
    );
    let res = run_exec(req).unwrap();
    assert!(!res.ok);
    assert_eq!(res.exit_code, akc_executor::WASM_EXIT_CPU_FUEL_EXHAUSTED);
    assert!(
        res.stderr.contains("WASM_CPU_FUEL_EXHAUSTED"),
        "expected deterministic cpu/fuel code marker, stderr={:?}",
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
            wall_time_ms: None,
            cpu_fuel: None,
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
            wall_time_ms: None,
            cpu_fuel: None,
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

#[test]
fn wasm_lane_rejects_nonexistent_preopen_dir() {
    let wat = r#"(module (func $_start (export "_start")))"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("noop.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let missing = dir.path().join("does-not-exist");
    let mut req = make_request(wasm_path.to_string_lossy().into_owned());
    req.fs_policy.preopen_dirs = vec![missing.to_string_lossy().into_owned()];

    let err = run_exec(req).unwrap_err();
    match err {
        ExecutorError::Wasm(msg) => {
            assert!(msg.contains("does not exist"), "unexpected message: {msg}");
        }
        other => panic!("expected wasm error, got {other:?}"),
    }
}

#[test]
fn wasm_lane_rejects_process_allowlist_fs_fields() {
    let wat = r#"(module (func $_start (export "_start")))"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("noop2.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let mut req = make_request(wasm_path.to_string_lossy().into_owned());
    req.fs_policy.allowed_read_paths = vec![dir.path().to_string_lossy().into_owned()];

    let err = run_exec(req).unwrap_err();
    match err {
        ExecutorError::Wasm(msg) => {
            assert!(
                msg.contains("allowed_read_paths"),
                "unexpected message: {msg}"
            );
        }
        other => panic!("expected wasm error, got {other:?}"),
    }
}

#[test]
fn wasm_lane_rejects_write_paths_not_in_preopens() {
    let wat = r#"(module (func $_start (export "_start")))"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("noop3.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let preopen = dir.path().join("ro");
    fs::create_dir_all(&preopen).unwrap();
    let write_not_preopened = dir.path().join("rw-other");
    fs::create_dir_all(&write_not_preopened).unwrap();

    let mut req = make_request(wasm_path.to_string_lossy().into_owned());
    req.fs_policy.preopen_dirs = vec![preopen.to_string_lossy().into_owned()];
    req.fs_policy.allowed_write_paths = vec![write_not_preopened.to_string_lossy().into_owned()];

    let err = run_exec(req).unwrap_err();
    match err {
        ExecutorError::Wasm(msg) => {
            assert!(
                msg.contains("subset of preopen_dirs"),
                "unexpected message: {msg}"
            );
        }
        other => panic!("expected wasm error, got {other:?}"),
    }
}

#[test]
fn wasm_lane_rejects_duplicate_canonical_preopen_dirs() {
    let wat = r#"(module (func $_start (export "_start")))"#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("noop4.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let preopen = dir.path().join("dup");
    fs::create_dir_all(&preopen).unwrap();
    let preopen_abs = preopen
        .canonicalize()
        .unwrap()
        .to_string_lossy()
        .into_owned();

    let mut req = make_request(wasm_path.to_string_lossy().into_owned());
    req.fs_policy.preopen_dirs = vec![preopen_abs.clone(), preopen_abs];

    let err = run_exec(req).unwrap_err();
    match err {
        ExecutorError::Wasm(msg) => {
            assert!(
                msg.contains("duplicate canonical mount target"),
                "unexpected message: {msg}"
            );
        }
        other => panic!("expected wasm error, got {other:?}"),
    }
}

#[test]
fn wasm_lane_without_preopens_denies_guest_fd3_path_open() {
    let wat = ro_denial_probe_wat(PATH_OPEN_RIGHTS);
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("no_preopen_probe.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request(wasm_path.to_string_lossy().into_owned());
    let res = run_exec(req).unwrap();

    assert!(res.ok, "stderr: {}", res.stderr);
    assert_eq!(res.exit_code, 0);
    assert!(
        res.stdout.contains("PASS"),
        "expected denied fd3 path_open without declared preopens, got stdout={:?}",
        res.stdout
    );
    assert!(!res.stdout.contains("FAIL"));
}

/// Create via `path_open` + `fd_write` + `fd_close`; guest mounts a single preopen at fd 3.
fn write_paper_via_wasi_preopen_wat(rights: i64) -> String {
    // Memory: 512="out.txt", 520="ok", 256=opened fd slot; iov at 0 for fd_write.
    // OFLAGS_CREAT|OFLAGS_TRUNC = 1|8 = 9.
    format!(
        r#"
        (module
          (import "wasi_snapshot_preview1" "path_open" (func $path_open
            (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_write" (func $fd_write
            (param i32 i32 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_close" (func $fd_close
            (param i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 512) "out.txt")
          (data (i32.const 520) "ok")
          (func $_start (export "_start")
            (local $e i32)
            (local $f i32)
            (local.set $e (call $path_open
              (i32.const 3)
              (i32.const 0)
              (i32.const 512)
              (i32.const 7)
              (i32.const 9)
              (i64.const {rights})
              (i64.const {rights})
              (i32.const 0)
              (i32.const 256)))
            (if (i32.ne (local.get $e) (i32.const 0))
              (then (return)))
            (local.set $f (i32.load (i32.const 256)))
            (i32.store (i32.const 0) (i32.const 520))
            (i32.store (i32.const 4) (i32.const 2))
            (drop (call $fd_write (local.get $f) (i32.const 0) (i32.const 1) (i32.const 700)))
            (drop (call $fd_close (local.get $f)))
          ))
        "#
    )
}

/// RO preopen should deny `path_open` that requests write/create; report via stdout.
fn ro_denial_probe_wat(rights: i64) -> String {
    format!(
        r#"
        (module
          (import "wasi_snapshot_preview1" "path_open" (func $path_open
            (param i32 i32 i32 i32 i32 i64 i64 i32 i32) (result i32)))
          (import "wasi_snapshot_preview1" "fd_write" (func $fd_write
            (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 512) "out.txt")
          (data (i32.const 600) "PASS\n")
          (data (i32.const 608) "FAIL\n")
          (func $_start (export "_start")
            (local $e i32)
            (local.set $e (call $path_open
              (i32.const 3)
              (i32.const 0)
              (i32.const 512)
              (i32.const 7)
              (i32.const 9)
              (i64.const {rights})
              (i64.const {rights})
              (i32.const 0)
              (i32.const 256)))
            (if (i32.eq (local.get $e) (i32.const 0))
              (then
                (i32.store (i32.const 0) (i32.const 608))
                (i32.store (i32.const 4) (i32.const 5))
                (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 700))))
              (else
                (i32.store (i32.const 0) (i32.const 600))
                (i32.store (i32.const 4) (i32.const 5))
                (drop (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 700)))))
          ))
        "#
    )
}

#[test]
fn wasm_lane_rw_preopen_allows_host_file_write() {
    let wat = write_paper_via_wasi_preopen_wat(PATH_OPEN_RIGHTS);
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let rw = dir.path().join("rw");
    fs::create_dir_all(&rw).unwrap();
    let wasm_path = dir.path().join("write_paper.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let mut req = make_request(wasm_path.to_string_lossy().into_owned());
    let rw_abs = rw.canonicalize().unwrap();
    req.fs_policy.preopen_dirs = vec![rw_abs.to_string_lossy().into_owned()];
    req.fs_policy.allowed_write_paths = vec![rw_abs.to_string_lossy().into_owned()];

    let res = run_exec(req).unwrap();
    assert!(res.ok, "stderr: {}", res.stderr);
    assert_eq!(res.exit_code, 0);

    let out = rw.join("out.txt");
    assert!(out.is_file(), "expected host file under rw preopen");
    let body = fs::read_to_string(&out).unwrap();
    assert_eq!(body, "ok");
}

#[test]
fn wasm_lane_ro_preopen_denies_create_write_path_open() {
    let wat = ro_denial_probe_wat(PATH_OPEN_RIGHTS);
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ro = dir.path().join("ro");
    fs::create_dir_all(&ro).unwrap();
    let wasm_path = dir.path().join("ro_probe.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let mut req = make_request(wasm_path.to_string_lossy().into_owned());
    req.fs_policy.preopen_dirs = vec![ro.canonicalize().unwrap().to_string_lossy().into_owned()];

    let res = run_exec(req).unwrap();
    assert!(res.ok, "stderr: {}", res.stderr);
    assert_eq!(res.exit_code, 0);
    assert!(
        res.stdout.contains("PASS"),
        "expected PASS on denied path_open, got stdout={:?}",
        res.stdout
    );
    assert!(
        !res.stdout.contains("FAIL"),
        "writable path_open should fail on ro preopen, got stdout={:?}",
        res.stdout
    );

    assert!(
        !ro.join("out.txt").exists(),
        "guest must not create out.txt on read-only preopen"
    );
}

#[cfg(not(windows))]
#[test]
fn wasm_lane_prefers_cpu_fuel_failure_when_cpu_budget_is_lower_than_wall_time_budget() {
    let wat = r#"
        (module
          (func $_start (export "_start")
            (loop $top
              br $top)))
    "#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("cpu_budget_precedence.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: Some(5_000),
            cpu_fuel: Some(1_000),
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
    );
    let res = run_exec(req).unwrap();

    assert!(!res.ok);
    assert_eq!(res.exit_code, akc_executor::WASM_EXIT_CPU_FUEL_EXHAUSTED);
    assert!(res.stderr.contains("WASM_CPU_FUEL_EXHAUSTED"));
}

#[cfg(not(windows))]
#[test]
fn wasm_lane_prefers_timeout_when_wall_time_budget_is_lower_than_cpu_budget() {
    let wat = r#"
        (module
          (func $_start (export "_start")
            (loop $top
              br $top)))
    "#;
    let wasm_bytes = wat::parse_str(wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("wall_budget_precedence.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: Some(50),
            cpu_fuel: Some(2_000_000_000),
            memory_bytes: None,
            stdout_max_bytes: Some(1024),
            stderr_max_bytes: Some(1024),
        },
    );
    let res = run_exec(req).unwrap();

    assert!(!res.ok);
    assert_eq!(res.exit_code, WASM_EXIT_TIMEOUT);
    assert!(res.stderr.contains("WASM_TIMEOUT"));
}

#[test]
fn wasm_lane_caps_stdout_and_stderr_independently_in_same_run() {
    let wat = r#"
        (module
          (import "wasi_snapshot_preview1" "fd_write"
            (func $fd_write (param i32 i32 i32 i32) (result i32)))
          (memory 1)
          (export "memory" (memory 0))
          (data (i32.const 64) "0123456789")
          (data (i32.const 96) "abcdefghij")
          (func $_start (export "_start")
            (local $i i32)
            (i32.store (i32.const 0) (i32.const 64))
            (i32.store (i32.const 4) (i32.const 10))
            (i32.store (i32.const 8) (i32.const 96))
            (i32.store (i32.const 12) (i32.const 10))
            (local.set $i (i32.const 0))
            (loop $top
              (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 20))
              drop
              (call $fd_write (i32.const 2) (i32.const 8) (i32.const 1) (i32.const 24))
              drop
              (local.set $i (i32.add (local.get $i) (i32.const 1)))
              (br_if $top (i32.lt_u (local.get $i) (i32.const 500))))))
    "#
    .to_string();

    let wasm_bytes = wat::parse_str(&wat).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let wasm_path = dir.path().join("spam_both.wasm");
    fs::write(&wasm_path, wasm_bytes).unwrap();

    let req = make_request_with_limits(
        wasm_path.to_string_lossy().into_owned(),
        Limits {
            wall_time_ms: None,
            cpu_fuel: None,
            memory_bytes: None,
            stdout_max_bytes: Some(64),
            stderr_max_bytes: Some(32),
        },
    );
    let res = run_exec(req).unwrap();

    assert!(res.ok, "expected ok, stderr: {}", res.stderr);
    assert_eq!(res.exit_code, 0);
    assert_eq!(res.stdout.len(), 64);
    assert_eq!(res.stderr.len(), 32);
    assert!(res.stdout.chars().all(|c| c.is_ascii_digit()));
    assert!(res.stderr.chars().all(|c| c.is_ascii_lowercase()));
}
