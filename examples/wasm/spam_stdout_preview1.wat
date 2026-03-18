(module
  (import "wasi_snapshot_preview1" "fd_write"
    (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (memory 1)
  (export "memory" (memory 0))
  (data (i32.const 64) "0123456789")
  (func $_start (export "_start")
    (local $i i32)
    ;; iov[0] = { ptr=64, len=10 }
    (i32.store (i32.const 0) (i32.const 64))
    (i32.store (i32.const 4) (i32.const 10))
    (local.set $i (i32.const 0))
    (loop $top
      (call $fd_write (i32.const 1) (i32.const 0) (i32.const 1) (i32.const 20))
      drop
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br_if $top (i32.lt_u (local.get $i) (i32.const 500))))))
