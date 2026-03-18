# Security

## Purpose

This document describes the security model for AKC's execution sandbox (Rust `akc_executor`) and the supporting ingestion CLI (Rust `akc_ingest`), with a focus on defense-in-depth and **tenant isolation**.

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
- Deny network by default.
- Allowlist file mounts for the specific request.
- Cap runtime and memory per tenant (quotas).

## Capability system

Execution and ingestion APIs use an explicit capability model:
- The caller provides *what is allowed* (inputs, mounts, limits).
- The executor enforces *what is actually accessible* by default denying:
  - network, unless explicitly permitted
  - ambient filesystem access (only allowlisted paths are mounted)
  - uncontrolled environment variable access (environment is scrubbed and/or restricted)

Capabilities must be:
- logged by correlation id (tenant/run) without logging sensitive payloads
- validated before any process starts or any untrusted code runs

## Execution sandbox: two lanes (defense-in-depth)

AKC's executor supports two isolation strategies and chooses per task:

### Lane A: WASM execution (portable, capability-based)

When feasible, generated guest code runs as WebAssembly using:
- Wasmtime
- WASI Preview 2

The host defines a narrow interface for:
- passing input blobs
- returning structured results
- emitting events/log records

Default restrictions:
- filesystem/network are denied by default
- optional read-only mounts are allowed only for specific inputs

Limits enforced by the host:
- maximum memory (via page limits)
- wall-clock timeout
- CPU budgets (e.g., fuel/epoch interruption)
- maximum stdout/stderr bytes

### Lane B: OS process sandbox (real OS semantics)

When tasks require real OS semantics, generated code may run via an OS sandboxed child process.

Core isolation controls:
- dedicated per-run working directory
- constrained environment (env scrubbing)
- resource limits (CPU time, RSS, file size)
- restricted filesystem view (best-effort isolation on the developer OS)

Platform strategy:
- Linux: prefer stronger hardening in CI (namespaces/filesystem isolation + syscall filtering + optional filesystem rules).
- macOS: prefer strict working-dir isolation + env scrubbing + time/memory limits where OS sandboxing is limited.
- Windows: prefer job objects + restricted token where feasible; otherwise rely on strict working-dir isolation + quotas.

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

