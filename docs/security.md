# Security

## Purpose

This document describes AKC’s **defense-in-depth** security model: tenant isolation, capability boundaries, and where enforcement is **hard** vs **best-effort**. It spans the **Python** `akc` package (CLI, compile loop, runtime) and the **Rust** crates (`akc_executor`, `akc_ingest`, `akc_protocol`).

**Related docs**

- [runtime-execution.md](runtime-execution.md) — runtime routing (`subprocess`, `http`, coordination), adapter behavior, and policy gates for `akc runtime`.
- [oss-security-requirements.md](oss-security-requirements.md) — CI/release security and correctness gates, verifier, and evidence expectations.
- [architecture.md](architecture.md) — end-to-end system shape.

## Code map (where to read this in the repo)

| Area | Primary locations |
| --- | --- |
| Compile-time sandbox selection | `src/akc/execute/factory.py` (`create_sandbox_executor`), `src/akc/execute/strong.py`, `src/akc/execute/dev.py` |
| Python executors | `src/akc/compile/executors.py` (`SubprocessExecutor`, `DockerExecutor`) |
| Rust bridge / protocol payloads | `src/akc/compile/rust_bridge.py`, `src/akc/compile/execute/rust_executor.py` (`RustExecutor`) |
| Tenant-scoped secrets helper | `src/akc/execute/secrets.py` (`SecretsScopeConfig`) |
| Rust executor implementation | `rust/crates/akc_executor/` (bin `akc-exec`), `rust/crates/akc_protocol/` |
| Rust ingestion CLI | `rust/crates/akc_ingest/` (bin `akc-ingest`) |
| Runtime HTTP + subprocess policy | `src/akc/runtime/http_execute.py`, `src/akc/runtime/adapters/local_depth.py`, `src/akc/runtime/action_routing.py` |

## Status: enforced vs best-effort

AKC uses multiple isolation layers depending on **lane**, **sandbox mode**, and **platform**. This document is explicit about what is:

- **Enforced**: AKC rejects the request or the OS/runtime prevents the action.
- **Best-effort**: AKC attempts containment/limits, but the host OS may not strictly enforce it.

## Assets and trust boundaries

AKC treats these as untrusted inputs:

- Generated or externally supplied artifacts executed during the Plan → Generate → Execute → Repair loop
- Ingested documents/messages/APIs that may contain malicious payloads
- Runtime bundles, IR, and operational contracts that drive execution routing (policy must gate mutating or subprocess behavior)

AKC treats these as trusted (when authenticated/authorized by the caller):

- Tenant identity and run metadata (`tenant_id`, `run_id`, `repo_id` where applicable)
- The policy layer that evaluates limits and allowlists before execution
- The per-tenant filesystem namespace and runtime configuration

## Threat model (high level)

The main security goals are:

- Prevent escape from the execution sandbox to the host filesystem or host environment (strength depends on lane; see compile vs runtime sections).
- Prevent cross-tenant data access (no cross-tenant reads/writes, caching, or indexing).
- Constrain resource usage (CPU time, memory, wall-clock time, stdout/stderr size).

We assume the attacker can:

- Provide malicious inputs to ingestion and execution
- Attempt path traversal, symlink attacks, and environment manipulation within the sandbox
- Attempt denial-of-service (resource exhaustion) or data exfiltration

## Tenant isolation guarantees (hard requirement)

Every execution request must include:

- `tenant_id` (string; Rust bridge additionally restricts characters to alphanumerics, `-`, `_`)
- `run_id` (uuid-like or equivalent; validated for safe directory names in Python executors)

**Workspace layout (Python executors)**  
Subprocess and Docker executors namespace work under a configurable `work_root`:

- Scope directory: `{work_root}/{tenant_id}/{repo_id}/`
- Optional per-run directory when `run_id` is set: `{work_root}/{tenant_id}/{repo_id}/{run_id}/`

For `akc compile`, `work_root` defaults to the project outputs base: `<outputs_root>/<tenant_id>/<repo_id>/` (see `src/akc/cli/compile.py`), not a fictional `./.akc/tenants/...` path. If you override `--work-root`, that path becomes the root for executor namespacing.

No cross-tenant caching:

- extracted docs/messages
- embeddings/index shards
- build artifacts
- WASM modules (unless keyed by tenant_id + a content hash)

Policy evaluation happens before execution/ingestion:

- **Network is denied** in Rust `akc-exec`: requests that set `capabilities.network=true` are rejected (`network_capability_denied`). The Python compile/runtime surfaces may expose separate “allow network” flags for non-Rust paths; they do not override Rust’s hard denial inside `akc-exec`.
- **Command execution** in the **Rust process lane** is allowlisted (via `AKC_EXEC_ALLOWLIST`; default is extremely restrictive). The **Python `SubprocessExecutor` used in `--sandbox dev`** does not use this allowlist; it relies on controller-chosen commands plus cwd/env/output limits (see [Compile loop: execution surfaces](#compile-loop-execution-surfaces)).
- **Filesystem policy** is validated in Rust; **enforcement strength depends on backend/platform** (see process lane sections).
- **Resource limits** are applied, but **memory enforcement is best-effort** outside Linux cgroups/Windows Jobs.

## Capability system

Execution and ingestion APIs use an explicit capability model:

- The caller provides *what is allowed* (inputs, mounts, limits).
- The Rust executor enforces *what is actually accessible* by default denying:
  - network (**enforced**: `capabilities.network=true` is always rejected in `akc-exec`)
  - dangerous environment variable injection (**enforced** in Rust: denylist, with optional prefix allowlist via `AKC_EXEC_ENV_ALLOW_PREFIXES`)
  - ambient filesystem access (**enforced on Linux `bwrap` backend; best-effort on native/macOS**)

Capabilities must be:

- logged by correlation id (tenant/run) without logging sensitive payloads
- validated before any process starts or any untrusted code runs

## Secrets scoping (per-tenant)

AKC supports tenant-scoped secrets injection into the execution environment via `SecretsScopeConfig` (`src/akc/execute/secrets.py`).

The injector follows a configurable host-env convention (defaults shown):

- Host environment variables expected: `AKC_SECRET_{tenant_id}_{secret_name}`
- Secrets injected into the child environment as: `AKC_SECRET_{secret_name}`

Only secrets whose `tenant_id` matches the active request scope are injected; other tenants’ keys are ignored. Optional `allowed_secret_names` restricts which names may be passed through (empty tuple means “any name allowed for that tenant prefix”).

The `akc compile` CLI currently wires `secrets_scope=None`; programmatic use via `SandboxFactoryConfig` / `StrongSandboxExecutor` can supply a `SecretsScopeConfig`. Treat injected values as sensitive (do not log raw secrets).

## Compile loop: execution surfaces

`akc compile` chooses an `Executor` implementation as follows (see `src/akc/cli/compile.py`):

1. **`--use-rust-exec`**  
   Uses `RustExecutor` directly with `--rust-exec-lane process|wasm` and `--rust-exec-mode cli|pyo3`. This bypasses `create_sandbox_executor` and talks to `akc-exec` (or the `akc_rust` extension) for **both** process and WASM lanes. The CLI may set `allow_network` on the Rust config from `--rust-allow-network` or `--sandbox-allow-network`, but **`akc-exec` still hard-denies `capabilities.network=true`** (policy denial). Use Docker or dev subprocess paths if you need the Python-side network toggle to take effect.

2. **Otherwise** — `create_sandbox_executor` (`src/akc/execute/factory.py`):
   - **`--sandbox dev` (default)** — `DevSandboxExecutor` → **`SubprocessExecutor`**: Python `subprocess` with tenant-scoped cwd under `work_root`, sanitized env, best-effort network hygiene (proxy env cleared when network is disabled), rlimits, and stdout/stderr caps. **Not** a Linux mount namespace; **not** Rust `AKC_EXEC_ALLOWLIST`.
   - **`--sandbox strong`** — `StrongSandboxExecutor` wrapping either:
     - **`DockerExecutor`** (Python, invokes the `docker` CLI): default for `--strong-lane-preference docker`, or for `auto` when Docker is on PATH. This is the “strong Docker lane” described below.
     - **`RustExecutor` with lane `wasm`**: for `--strong-lane-preference wasm`, or for `auto` when Docker is unavailable but the Rust surface is present.

Strong lane defaults (memory, stdout/stderr caps, Docker flags) are aligned with `SandboxStrongConfig` in `src/akc/execute/strong.py` and CLI defaults in `src/akc/cli/compile.py`.

## Execution sandbox: two lanes (defense-in-depth)

AKC’s **Rust** executor supports two isolation strategies and chooses per task. The **Python** compile flow may use **Docker** (strong) or **Rust WASM** / **Rust process** (`--use-rust-exec`) instead of the dev subprocess path.

### Strong Docker lane (default for CLI `--sandbox strong`)

The default strong lane in the CLI is **Docker via `DockerExecutor`**, not the Rust process backend. When Docker is selected, AKC assembles `docker run` with the following defaults:

- `--network none`
- `--memory 1073741824` (`--sandbox-memory-mb 1024`)
- `--pids-limit 256` (`--docker-pids-limit 256`)
- `--user 65532:65532` unless `--docker-user` is supplied
- `--tmpfs /tmp` unless one or more `--docker-tmpfs` flags are supplied
- `--read-only`
- `--security-opt no-new-privileges`
- `--cap-drop ALL`
- stdout/stderr capture capped at `2048 KiB` each (`--sandbox-stdout-max-kb`, `--sandbox-stderr-max-kb`)

Optional Docker hardening flags exposed by the CLI:

- `--docker-user`
- `--docker-tmpfs` (repeatable)
- `--docker-seccomp-profile`
- `--docker-apparmor-profile`
- `--docker-ulimit-nofile`
- `--docker-ulimit-nproc`
- `--docker-cpus`

Docker hardening preflight is fail-closed when Docker-specific controls are requested. AKC rejects the run before launch when:

- Docker-only flags are used outside `--sandbox strong`
- `--strong-lane-preference wasm` is selected with Docker-only flags
- `--strong-lane-preference auto` would fall back away from Docker while Docker-only hardening is configured
- tmpfs, user, seccomp/AppArmor identifiers, or ulimits are malformed
- an absolute `--docker-seccomp-profile` path does not exist or is not a file
- `--docker-apparmor-profile` is requested on a host without AppArmor support

Docker strong guarantees by host/runtime:

| Scope | Enforced by AKC | Enforced by runtime | Best-effort / platform notes |
| --- | --- | --- | --- |
| Lane selection | Docker-specific hardening is rejected unless Docker strong can actually apply it | N/A | `auto` is only acceptable when no Docker-specific control would be dropped |
| Command shape | AKC validates and assembles `--user`, `--tmpfs`, `--security-opt`, `--ulimit`, `--read-only`, `--cap-drop ALL` deterministically | Docker receives the exact flags AKC emits | None once launch begins |
| Network | `--sandbox-allow-network` defaults to deny | `--network none` blocks container egress | Any allowed-network exception must come from policy/config, not fallback behavior |
| Rootfs and temp writes | `--read-only` and tmpfs mounts are requested by default | Docker enforces read-only rootfs and writable tmpfs paths | Runtime semantics depend on the container runtime's Linux kernel boundary |
| Privilege reduction | Non-root user default, `no-new-privileges`, `cap-drop ALL` | Docker/kernel enforce the configured user and privilege flags | Images that assume root may fail to start; this is expected, not a silent downgrade |
| Seccomp | AKC validates identifier syntax and absolute-path existence | Docker applies the default profile or the configured `seccomp=...` profile | Effective syscall filtering is runtime-defined; verify on the target Docker Engine/kernel |
| AppArmor | AKC fails closed if profile is requested on an unsupported host | Docker applies `apparmor=...` only on Linux hosts with AppArmor enabled | Not portable to Docker Desktop/macOS/Windows hosts |
| Resource limits | AKC validates requested values | Docker applies memory, PID, CPU, and ulimit controls | Exact enforcement quality depends on the host runtime/cgroup environment |
| Output caps | AKC captures stdout/stderr with configured byte caps | N/A | Large output can still consume runtime resources before capture is truncated |

Operational guidance:

- Prefer `--strong-lane-preference docker` for production hardening. It fails closed when Docker is unavailable.
- Use `--strong-lane-preference auto` only during migration when WASM fallback is acceptable and no Docker-specific hardening flag is required.
- Prefer Linux Docker Engine for production attestations. Docker Desktop on macOS/Windows still receives the same flags, but enforcement happens inside the Linux VM and AppArmor is generally unavailable.

### Lane A: WASM execution (portable, capability-based)

When feasible, generated guest code runs as WebAssembly using:

- Wasmtime
- WASI Preview 1 (preview1)

The host defines a narrow interface for:

- passing input blobs
- returning structured results
- emitting events/log records

Default restrictions:

- filesystem is denied (no preopened directories)
- network is denied (no host networking exposed)

WASM filesystem contract:

- explicit preopens are required for any guest filesystem access
- no implicit host filesystem exposure is allowed
- `allowed_read_paths` is rejected for WASM lane (use `preopen_dirs`)
- `allowed_write_paths` must be an explicit subset of `preopen_dirs`
- top-level `akc compile` exposes WASM filesystem flags:
  - `--wasm-preopen-dir <ABS_DIR>` (repeatable)
  - `--wasm-allow-write-dir <ABS_DIR>` (repeatable, subset of preopens)
  - these flags require explicit WASM lane selection and are not applied to docker/process lanes

WASM path-normalization controls (CLI):

- `--wasm-fs-normalize-existing-paths` canonicalizes existing `preopen_dirs` and
  `allowed_write_paths` before subset validation and request emission.
- `--wasm-fs-normalization-profile strict|relaxed` controls unresolved paths:
  - `strict` (default): fail closed on missing/unresolvable paths.
  - `relaxed`: keep unresolved paths as-is and defer final enforcement to Rust runtime.

Recommended profile guidance:

- **production / policy-enforced runs**: use normalization with strict profile.
  - Example:
    `akc compile --sandbox strong --strong-lane-preference wasm --wasm-fs-normalize-existing-paths --wasm-fs-normalization-profile strict ...`
- **developer convenience / migration**: use normalization with relaxed profile when
  path aliases or symlink spellings are common and strict canonicalization would
  cause friction.
  - Example:
    `akc compile --sandbox strong --strong-lane-preference wasm --wasm-fs-normalize-existing-paths --wasm-fs-normalization-profile relaxed ...`

Limits enforced by the host:

- **wall-clock timeout**: elapsed-time deadline via Wasmtime epoch interruption on supported platforms
- **CPU budgets**: deterministic fuel cap from `cpu_fuel` when provided
- **maximum stdout/stderr bytes**: in-memory capped pipes
- **memory limits**: explicit Wasmtime linear memory cap when `memory_bytes` is provided

Deterministic WASM limits contract:

- `wall_time_ms`:
  - when set, AKC arms a real elapsed-time deadline using Wasmtime epoch interruption.
  - on timeout, execution traps with `WASM_TIMEOUT` instead of being inferred from fuel exhaustion.
  - Windows remains fail-closed for strict/prod compile runs because this guarantee is not supported there.
- `cpu_fuel`:
  - when set, applied as an explicit fuel budget with deterministic clamp:
    - `cpu_fuel_budget = clamp(cpu_fuel, 1, 2_000_000_000)`
  - `cpu_fuel` is independent of `wall_time_ms`; when both are set, the first boundary reached wins:
    - elapsed-time deadline -> `WASM_TIMEOUT`
    - CPU/fuel budget -> `WASM_CPU_FUEL_EXHAUSTED`
  - this keeps timeout and CPU exhaustion policy-friendly and unambiguous.
- `memory_bytes`:
  - when set, applied as a Wasmtime store linear-memory limit (fail-closed on limit exceed).
  - when unset, no explicit linear-memory cap is requested by AKC.
- `stdout_max_bytes` / `stderr_max_bytes`:
  - each stream is captured with a deterministic in-memory cap.
  - default per-stream cap is `1 MiB` when not specified (Rust constant `WASM_DEFAULT_STDIO_MAX_BYTES`).

Deterministic WASM error semantics (stable for policy/tests):

- WASM lane emits a machine-readable first stderr line:
  - `AKC_WASM_ERROR code=<CODE> exit_code=<N> message=<TEXT>`
- Stable error codes and exit codes:
  - timeout: `WASM_TIMEOUT` -> exit code `124`
  - cpu/fuel exhaustion: `WASM_CPU_FUEL_EXHAUSTED` -> exit code `137`
  - memory limit exceeded: `WASM_MEMORY_LIMIT_EXCEEDED` -> exit code `138`
  - unsupported platform capability: `WASM_UNSUPPORTED_PLATFORM_CAPABILITY` -> exit code `78`
- Windows note:
  - wall-time limit requests in WASM lane are currently unsupported and return
    `WASM_UNSUPPORTED_PLATFORM_CAPABILITY` with exit code `78` instead of degrading.

WASM platform capability matrix:

| Platform | Supported guarantees | Unsupported controls | Remediation guidance |
| --- | --- | --- | --- |
| Linux | Engine availability probe, capability-based preopens, deterministic wall-time and CPU fuel budgeting, linear memory cap, stdout/stderr caps | None beyond the lane-wide contract (`allowed_read_paths` is unsupported for WASM) | Preferred target for strict/prod WASM runs |
| macOS | Engine availability probe, capability-based preopens, deterministic wall-time and CPU fuel budgeting, linear memory cap, stdout/stderr caps | None beyond the lane-wide contract (`allowed_read_paths` is unsupported for WASM) | Supported for strict/prod WASM runs when the Rust surface is installed |
| Windows | Engine availability probe, capability-based preopens, CPU fuel budgeting, linear memory cap, stdout/stderr caps | Wall-time enforcement for WASM compile/test stages | Use Linux/macOS for strict/prod WASM runs, or switch to the process/Docker lane |

WASM preflight behavior:

- Compile performs a preflight before execution when WASM is explicitly requested or selected.
- Engine availability is checked first:
  - missing `akc-exec` / `akc_rust` fails fast with remediation instead of falling through to a later runtime failure.
- In enforced/strict profiles, unsupported platform guarantees fail closed:
  - Windows + WASM compile runs are rejected up front because compile stages require bounded wall-time execution.
- Outside enforced/strict profiles, AKC still returns `WASM_UNSUPPORTED_PLATFORM_CAPABILITY` with the stable first-line marker rather than silently broadening access.

WASM compile CLI controls:

- `--sandbox-cpu-fuel <N>` sets an explicit CPU fuel budget for Rust/WASM execution.
  - must be `> 0`
  - pairs cleanly with `--sandbox strong --strong-lane-preference wasm`
  - deterministic behavior:
    - binding CPU limit -> `WASM_CPU_FUEL_EXHAUSTED`
    - binding wall-time deadline -> `WASM_TIMEOUT`

### Lane B: OS process sandbox (real OS semantics)

When tasks require real OS semantics, generated code may run via an OS sandboxed child process **in Rust** (`akc-exec`, process lane). Separately, **`--sandbox dev`** uses Python `SubprocessExecutor` (see [Compile loop](#compile-loop-execution-surfaces)); that path is weaker and intended for local development.

Core isolation controls (Rust process lane):

- dedicated per-run working directory
- constrained environment (env scrubbing; optional `AKC_EXEC_ENV_ALLOW_PREFIXES` for request keys)
- wall-clock timeout with process-tree termination
- stdout/stderr byte caps (post-collection clamp)
- memory limits (best-effort on Unix; stronger options on Linux/Windows)
- restricted filesystem view (**strong on Linux `bwrap`**, **best-effort on native/macOS**)

### Process lane: what is enforced per platform/backend

The `process` lane has multiple backends selected by `AKC_EXEC_BACKEND`:

- `native` (portable; default)
- `bwrap` (Linux only; requires Bubblewrap)
- `docker` (**not implemented** in Rust; requests are denied — production Docker isolation for compile is via **Python `DockerExecutor`**, not this backend)

#### Linux + `AKC_EXEC_BACKEND=bwrap` (strongest process-lane isolation)

**Enforced**

- **Network off**: Bubblewrap runs with `--unshare-net`.
- **Filesystem namespace**: the child sees only:
  - the per-tenant/run workspace mounted read-write
  - explicitly allowlisted host paths mounted read-only or read-write (as requested)
  - minimal runtime trees mounted read-only (e.g. `/usr`, `/bin`, `/lib*`) to run dynamically linked binaries
  - `/tmp` is a `tmpfs`
- **Environment scrubbing**: `env_clear` + minimal `PATH` + `AKC_TENANT_ID` / `AKC_RUN_ID` + filtered request env.
- **Command allowlist**: program must be in `AKC_EXEC_ALLOWLIST` (default: only `echo` for dev/tests).
- **Timeout cleanup**: on Unix, the executor kills the whole process group on wall-time timeout.

**Best-effort**

- **Memory limits**:
  - Unix `RLIMIT_AS` is applied when supported, but is not a perfect RSS cap and may be partially enforced.
  - On Linux runners, an **optional cgroup v2** memory limiter may be created (auto-enabled in CI best-effort; controlled via `AKC_EXEC_CGROUPV2`).
- **Stdout/stderr caps**: output is clamped after capture (large outputs still cost host resources during execution).

#### macOS + `AKC_EXEC_BACKEND=native` (developer-friendly, best-effort FS isolation)

macOS does not use Bubblewrap. The native backend focuses on tenant/run workspace layout and policy validation.

**Enforced**

- **Network off**: same as Linux—requests with network capability enabled are rejected.
- **Per-tenant/run working directory**: `cwd` is forced within the tenant/run workspace; relative traversal via `..` is rejected.
- **Environment scrubbing**: `env_clear` + minimal `PATH` + `AKC_TENANT_ID` / `AKC_RUN_ID` + filtered request env.
- **Command allowlist**: program must be in `AKC_EXEC_ALLOWLIST` (default: only `echo` for dev/tests).
- **Timeout cleanup**: on Unix, the executor kills the whole process group on wall-time timeout.

**Best-effort / limitations**

- **Filesystem isolation**: the native backend does **not** create a new filesystem namespace (no chroot/container).
  - The request’s `fs_policy` is **validated** and, in the native backend, is limited to paths **within the tenant/run workspace**.
  - However, a spawned program can still attempt to open arbitrary absolute paths on the host (e.g. `/etc/hosts`) because the OS view is shared. Treat native/macOS as **workspace containment + policy validation**, not a hard filesystem sandbox.
- **Memory limits**: `RLIMIT_AS` is attempted; some macOS setups return `EINVAL`/`ENOSYS` and the executor proceeds without failing the run.

#### Windows + `AKC_EXEC_BACKEND=native` (Job Objects for tree kill + memory caps)

Windows uses the native backend, and attempts to create a **per-exec Job Object** and assign the spawned process to it.

**Enforced (when Job assignment succeeds)**

- **Process-tree cleanup**: on timeout, the executor terminates the Job, which terminates the entire process tree.
- **Memory limit**: if `limits.memory_bytes` is provided, the Job memory limit is set for the whole Job.

**Enforced (even without a Job)**

- **Network off**: requests with network capability enabled are rejected.
- **Per-tenant/run working directory**: `cwd` is forced within the tenant/run workspace; relative traversal via `..` is rejected.
- **Environment scrubbing**: `env_clear` + minimal `PATH` + `AKC_TENANT_ID` / `AKC_RUN_ID` + filtered request env.
- **Command allowlist**: program must be in `AKC_EXEC_ALLOWLIST` (default: only `echo` for dev/tests).

**Best-effort / limitations**

- **Job assignment can fail** (e.g., if the runner is already in a Job that disallows nested Jobs). In that case:
  - timeout kill falls back to best-effort `Child::kill()`
  - full process-tree kill semantics are not guaranteed
- **Filesystem isolation**: Windows native does not create a filesystem namespace; it relies on workspace containment + policy validation.
- **CPU throttling and priority**: optional best-effort policies can be applied via:
  - `AKC_EXEC_WIN_CPU_RATE_PERCENT` (Job CPU hard-cap when supported)
  - `AKC_EXEC_WIN_PRIORITY_CLASS` (process priority class)

#### Linux + `AKC_EXEC_BACKEND=native`

Same semantics as macOS native: **no filesystem namespace**. Prefer `bwrap` on Linux when you need enforceable filesystem isolation.

## Runtime execution (`akc runtime`)

The long-running runtime uses **adapters** and **policy** distinct from the compile-time executor stack. In particular:

- **`LocalDepthRuntimeAdapter`** can run **`subprocess`** and **`http`** routes when enabled by bundle metadata and runtime policy (allowlisted argv basenames, bounded HTTP via `akc.runtime.http_execute`, minimal env).
- **`NativeRuntimeAdapter`** stubs most actions but runs real **`coordination.step`** work when coordination is present.

Authoritative routing, gates, and evidence paths are documented in [runtime-execution.md](runtime-execution.md). Treat runtime as **policy-gated**: never assume subprocess or outbound HTTP is allowed without explicit bundle + envelope configuration.

## Ingestion safety model

Ingestion normalizes untrusted content into deterministic chunk records suitable for indexing.

- **Python connectors** parse and chunk in-process; treat inputs as untrusted data (not trusted code).
- **Optional Rust path**: `akc-ingest` via CLI/pyo3 (`akc compile` / ingest flags such as `--use-rust-ingest-docs`) produces normalized records consumed by Python.

Isolation principles:

- ingestion should be treated as parsing untrusted payloads (no direct execution of document contents as code)
- tenant-scoped output records include `tenant_id`
- normalization is deterministic (stable ordering and stable ids) to reduce the chance of adversarial non-determinism affecting downstream compilation

## Observability and logging

We record structured events for executor/ingest calls with:

- `tenant_id`, `run_id` (and related scope fields)
- selected lane (`process` / `wasm`) and surface (`cli` vs `pyo3` where applicable)
- requested limits (timeout/memory/stdout/stderr caps)
- outcome (`ok`, exit code, timeout flag, policy denial)

Python’s `rust_bridge` emits compact JSON log lines for bridge operations; Rust uses `akc_protocol` observability helpers.

Logging requirements:

- Always include tenant/run ids for correlation.
- Never log secrets, raw large payloads, or cross-tenant information.
- Prefer redaction/sampling for high-volume events.

## Non-goals

This model is defense-in-depth. It does not claim absolute safety against all possible sandbox escape vulnerabilities, but it:

- minimizes the attack surface
- enforces strict capabilities and resource budgets where the selected lane supports them
- creates audit trails for incident investigation

Continuous assurance (CI jobs, verifier gates, supply-chain checks) is summarized in [oss-security-requirements.md](oss-security-requirements.md).
