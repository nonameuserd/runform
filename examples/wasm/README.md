## WASM lane examples (WASI Preview 1)

These `.wat` examples are **WASI Preview 1** modules intended for the Rust executor’s WASM lane (`akc_executor`, `ExecLane::Wasm`).

### Convert WAT → WASM

If you have `wat2wasm` installed (from [wabt](https://github.com/WebAssembly/wabt)), you can compile:

```bash
wat2wasm examples/wasm/hello_preview1.wat -o /tmp/hello_preview1.wasm
wat2wasm examples/wasm/spam_stdout_preview1.wat -o /tmp/spam_stdout_preview1.wasm
```

### Run via `akc-exec`

Build `akc-exec`:

```bash
cargo build -p akc_executor --bin akc-exec
```

Then run the WASM lane by sending an `ExecRequest` JSON to stdin (example):

```bash
cat <<'JSON' | cargo run -p akc_executor --bin akc-exec
{
  "tenant_id": "tenant_a",
  "run_id": "run_1",
  "lane": { "type": "wasm" },
  "capabilities": { "network": false },
  "limits": { "wall_time_ms": 250, "memory_bytes": null, "stdout_max_bytes": 64, "stderr_max_bytes": 1024 },
  "command": ["/tmp/hello_preview1.wasm"],
  "cwd": null,
  "env": {},
  "stdin_text": null,
  "fs_policy": { "allowed_read_paths": [], "allowed_write_paths": [], "preopen_dirs": [] }
}
JSON
```

Notes:
- The WASM lane currently uses **WASI Preview 1** (`wasi_snapshot_preview1` imports).
- By default, the lane runs with **no preopened directories** (no filesystem access).
