# Security

## Purpose

This document describes the security model for AKC's execution sandbox (Rust `akc_executor`) and the supporting ingestion CLI (Rust `akc_ingest`), with a focus on defense-in-depth and **tenant isolation**.

## Status: enforced vs best-effort

AKC uses multiple isolation layers depending on **lane** and **platform**. This document is explicit about what is:
- **Enforced**: AKC rejects the request or the OS/runtime prevents the action.
- **Best-effort**: AKC attempts containment/limits, but the host OS may not strictly enforce it.

## Assets and trust boundaries

AKC treats these as untrusted inputs:
- Generated or externally supplied artifacts executed during the Plan → Generate → Execute → Repair loop
- Ingested documents/messages/APIs that may contain malicious payloads

AKC treats these as trusted (when authenticated/authorized by the caller):
- Tenant identity and run metadata (`tenant_id`, `run_id`)
- The policy layer that evaluates limits and allowlists before execution
- The per-tenant filesystem namespace and runtime configuration

## Threat model (high level)

The main security goals are:
- Prevent escape from the execution sandbox to the host filesystem or host environment.
- Prevent cross-tenant data access (no cross-tenant reads/writes, caching, or indexing).
- Constrain resource usage (CPU time, memory, wall-clock time, stdout/stderr size).

We assume the attacker can:
- Provide malicious inputs to ingestion and execution
- Attempt path traversal, symlink attacks, and environment manipulation within the sandbox
- Attempt denial-of-service (resource exhaustion) or data exfiltration

## Tenant isolation guarantees (hard requirement)

Every request must include:
- `tenant_id` (string)
- `run_id` (uuid-like or equivalent)

All artifacts are namespaced by tenant:
- Working directories live under a configured root, e.g. `./.akc/tenants/{tenant_id}/runs/{run_id}/...`.

No cross-tenant caching:
- extracted docs/messages
- embeddings/index shards
- build artifacts
- WASM modules (unless keyed by tenant_id + a content hash)

Policy evaluation happens before execution/ingestion:
- **Network is denied** by the executor today (requests that set `capabilities.network=true` are rejected).
- **Command execution is allowlisted** (via `AKC_EXEC_ALLOWLIST`; default is extremely restrictive).
- **Filesystem policy** is validated, but **enforcement strength depends on backend/platform** (see below).
- **Resource limits** are applied, but **memory enforcement is best-effort** outside Linux cgroups/Windows Jobs.

## Capability system

Execution and ingestion APIs use an explicit capability model:
- The caller provides *what is allowed* (inputs, mounts, limits).
- The executor enforces *what is actually accessible* by default denying:
  - network (**enforced**: currently always denied)
  - dangerous environment variable injection (**enforced**: denylist, with optional prefix allowlist)
  - ambient filesystem access (**enforced on Linux `bwrap` backend; best-effort on native/macOS**)

Capabilities must be:
- logged by correlation id (tenant/run) without logging sensitive payloads
- validated before any process starts or any untrusted code runs

## Execution sandbox: two lanes (defense-in-depth)

AKC's executor supports two isolation strategies and chooses per task:

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

Limits enforced by the host:
- **wall-clock timeout** (epoch interruption)
- **CPU budgets** (fuel)
- **maximum stdout/stderr bytes** (in-memory capped pipes)
- **memory limits**: best-effort today (WASM uses Wasmtime limits for CPU/time/output; explicit linear-memory caps are not currently the primary control boundary)

### Lane B: OS process sandbox (real OS semantics)

When tasks require real OS semantics, generated code may run via an OS sandboxed child process.

Core isolation controls:
- dedicated per-run working directory
- constrained environment (env scrubbing)
- wall-clock timeout with process-tree termination
- stdout/stderr byte caps (post-collection clamp)
- memory limits (best-effort on Unix; stronger options on Linux/Windows)
- restricted filesystem view (**strong on Linux `bwrap`**, **best-effort on native/macOS**)

### Process lane: what is enforced per platform/backend

The `process` lane has multiple backends selected by `AKC_EXEC_BACKEND`:
- `native` (portable; default)
- `bwrap` (Linux only; requires Bubblewrap)
- `docker` (currently not implemented; requests are denied)

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

## Ingestion safety model

The ingestion CLI normalizes untrusted content into deterministic chunk records suitable for indexing.

Isolation principles:
- ingestion should be treated as parsing untrusted payloads (no direct execution)
- tenant-scoped output records always include `tenant_id`
- normalization is deterministic (stable ordering and stable ids) to reduce the chance of adversarial non-determinism affecting downstream compilation

## Observability and logging

We record structured events for each executor/ingest call (Rust and Python bridges) with:
- `tenant_id`, `run_id`
- selected lane (`process` / `wasm`)
- invocation path (`cli` vs `pyo3`)
- requested limits (timeout/memory/stdout/stderr caps)
- outcome (`ok`, exit code, timeout flag, policy denial)

Logging requirements:
- Always include tenant/run ids for correlation.
- Never log secrets, raw large payloads, or cross-tenant information.
- Prefer redaction/sampling for high-volume events.

## Non-goals

This model is defense-in-depth. It does not claim absolute safety against all possible sandbox escape vulnerabilities, but it:
- minimizes the attack surface
- enforces strict capabilities and resource budgets
- creates audit trails for incident investigation

