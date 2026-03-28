# Getting Started

## Requirements

- Python 3.11+
- [uv](https://docs.astral.sh/uv/)

## Install

```bash
git clone https://github.com/nonameuserd/runform.git
cd runform
uv sync
```

Useful extras:

```bash
uv sync --extra dev
uv sync --extra ingest-all
uv sync --extra mcp-serve
uv sync --extra vectorstore-pg
uv sync --extra delivery-providers
```

After `uv sync`, either activate the virtualenv or use `uv run`:

```bash
source .venv/bin/activate   # Windows: .venv\Scripts\activate
akc --help
```

## Bootstrap a project

Create `.akc/project.json` and the default local compile policy stub:

```bash
akc init
```

Useful variant for an existing repository:

```bash
akc init --detect
```

That also writes `.akc/project_profile.json` with detected toolchain and repository metadata.

## Safe local demo

This path stays local and avoids working-tree writes.

```bash
# 1. Index local docs with a deterministic offline embedder
akc ingest \
  --tenant-id demo \
  --connector docs \
  --input ./docs \
  --embedder hash \
  --index-backend sqlite

# 2. Compile into artifacts only
akc compile \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --artifact-only

# 3. Verify artifacts
akc verify \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out

# 4. Open the local viewer
akc view \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  web
```

`akc compile` defaults to `scoped_apply`, so `--artifact-only` is the safest starting point for docs, demos, and CI.

## Hosted LLM compile

Compile, living, and assistant now support hosted LLM backends, but offline remains the default.

Hosted generation is explicit:

```bash
export OPENAI_API_KEY=...

akc compile \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --artifact-only \
  --llm-backend openai \
  --llm-model gpt-4.1 \
  --llm-allow-network
```

If you select `openai`, `anthropic`, or `gemini` without `--llm-allow-network`, AKC fails closed before making any outbound request.

## Git-aware scoped apply

AKC's Git integration is part of the `scoped_apply` compile path. When your `--apply-scope-root` points at a Git working tree, AKC can create a topic branch and commit the applied patch for that run.

```bash
akc compile \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --compile-realization-mode scoped_apply \
  --apply-scope-root "$PWD" \
  --git-branch-per-run \
  --git-commit
```

Current behavior:

- branch name format is `akc/compile/<patch_sha_prefix>`
- commits stage only the patch-touched paths, not the whole repository
- rollback snapshots of touched files are written under `.akc/rollback/` by default
- if Git integration is requested but `git` is missing or the scope root is not a Git repo, compile records a fail-closed denial instead of applying anyway

## Environment model

AKC currently exposes two related environment models:

- operator/runtime safety profiles use `dev`, `staging`, and `prod`
- compile-time delivery plans use `local`, `staging`, and `production`

For the full mapping, promotion defaults, and delivery-lane behavior, see [environment-model.md](environment-model.md).

## Validation-backed verify

If you have a validator registry under `configs/validation/validator_bindings.v1.yaml`, you can execute observability and mobile validators during verify:

```bash
akc verify \
  --tenant-id demo \
  --repo-id runform \
  --outputs-root ./out \
  --execute-validators
```

Use `.akc/project.json` to point at a non-default registry:

```json
{
  "validation": {
    "bindings_path": "configs/validation/validator_bindings.v1.yaml"
  }
}
```

## Common commands

### Ingest repository code

```bash
akc ingest \
  --tenant-id demo \
  --connector codebase \
  --input . \
  --embedder hash \
  --index-backend sqlite
```

Supported connectors today:

- `docs`
- `codebase`
- `openapi`
- `slack`
- `discord`
- `telegram`
- `whatsapp`
- `mcp`

Supported index backends today:

- `memory`
- `sqlite`
- `pgvector`

## Assistant mode

Single-turn planning:

```bash
akc assistant --mode plan -p "show me the next command to inspect the latest compile outputs"
```

Interactive loop:

```bash
akc assistant
```

Weighted memory is opt-in. Enable it globally with:

```bash
export AKC_WEIGHTED_MEMORY_ENABLED=1
```

You can also enable it per invocation with memory flags such as `--memory-policy-path`, `--memory-pin`, `--memory-boost`, or `--memory-budget-tokens`.

Assistant can also use a hosted planner with the same shared flags:

```bash
akc assistant \
  --llm-backend openai \
  --llm-model gpt-4.1-mini \
  --llm-allow-network \
  -p "show recent compile runs"
```

## MCP server

Install the extra first:

```bash
uv sync --extra mcp-serve
```

Then run the read-only MCP server:

```bash
akc mcp serve
```

`akc mcp serve` supports `stdio`, `streamable-http`, and `sse` transports.

## Runtime surfaces

The `runtime` subtree operates on emitted runtime bundles:

```bash
akc runtime --help
akc runtime coordination-plan --bundle /path/to/runtime_bundle.json
```

Primary runtime subcommands:

- `start`
- `coordination-plan`
- `stop`
- `status`
- `events`
- `reconcile`
- `checkpoint`
- `replay`
- `autopilot`

See [runtime-execution.md](runtime-execution.md) for runtime routing, adapters, reconcile, and autopilot details.

## Viewer

The viewer is local-first and read-only:

```bash
akc view --tenant-id demo --repo-id runform --outputs-root ./out tui
akc view --tenant-id demo --repo-id runform --outputs-root ./out web
akc view --tenant-id demo --repo-id runform --outputs-root ./out export
```

See [viewer.md](viewer.md) and [viewer-trust-boundary.md](viewer-trust-boundary.md).

## Progressive adoption

AKC supports a practical adoption ladder:

- Observer: `akc init --detect` and `akc ingest --connector codebase`
- Advisor: `akc compile --artifact-only`
- Copilot: `akc compile --compile-realization-mode scoped_apply --apply-scope-root ...`
- Compiler/Autonomy: runtime, living recompile, and autopilot workflows after policy and operational gates are in place

For first use in a real repository, start with Observer or Advisor.

## Where to go next

- [Configuration](configuration.md)
- [CLI command reference](cli-commands.md)
- [Architecture](architecture.md)
- [Validation](validation.md)
- [Artifact contracts](artifact-contracts.md)
- [Compile-time skills](compile-skills.md)
- [Security](security.md)
