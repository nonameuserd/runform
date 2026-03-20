from __future__ import annotations

import argparse
import sys

from akc import __version__

from .compile import cmd_compile
from .drift import cmd_drift, cmd_watch
from .eval import cmd_eval
from .ingest import cmd_ingest, cmd_slack_list_channels
from .living import cmd_living_recompile
from .metrics import cmd_metrics
from .verify import cmd_verify
from .view import cmd_view

__all__ = [
    "cmd_compile",
    "cmd_drift",
    "cmd_watch",
    "cmd_eval",
    "cmd_ingest",
    "cmd_living_recompile",
    "cmd_slack_list_channels",
    "cmd_metrics",
    "cmd_view",
    "cmd_verify",
    "main",
]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="akc", description="Agentic Knowledge Compiler")
    parser.add_argument("--version", action="version", version=f"akc {__version__}")

    sub = parser.add_subparsers(dest="command", required=True)

    ingest = sub.add_parser(
        "ingest",
        help="Ingest sources into a vector index (connectors and index backends are pluggable)",
    )
    ingest.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    ingest.add_argument(
        "--connector",
        required=True,
        choices=["docs", "openapi", "slack"],
        help="Connector: docs, openapi, slack (extend via ingest modules)",
    )
    ingest.add_argument(
        "--input",
        required=True,
        help="Connector input (docs root path, OpenAPI spec path/URL, or Slack channel id)",
    )

    ingest.add_argument("--verbose", action="store_true", help="Enable debug logging")

    ingest.add_argument(
        "--embedder",
        choices=["hash", "openai", "gemini", "none"],
        default="none",
        help=(
            "Embedding provider (default: none, offline). "
            "Use openai/gemini only when explicitly configured."
        ),
    )
    ingest.add_argument("--openai-api-key", help="OpenAI-compatible API key")
    ingest.add_argument("--openai-base-url", help="OpenAI-compatible base URL (default: OpenAI)")
    ingest.add_argument("--openai-model", help="OpenAI-compatible embedding model")
    ingest.add_argument("--gemini-api-key", help="Gemini API key")
    ingest.add_argument("--gemini-base-url", help="Gemini base URL")
    ingest.add_argument("--gemini-model", help="Gemini embedding model")

    ingest.add_argument(
        "--index-backend",
        choices=["memory", "sqlite", "pgvector"],
        default="memory",
        help=(
            "Pluggable vector index backend (default: memory; use sqlite/pgvector for persistence)"
        ),
    )
    ingest.add_argument("--no-index", action="store_true", help="Run ingest without indexing")
    ingest.add_argument("--sqlite-path", help="SQLite DB path (for --index-backend sqlite)")
    ingest.add_argument("--pg-dsn", help="Postgres DSN (for --index-backend pgvector)")
    ingest.add_argument(
        "--pg-dimension",
        type=int,
        help="Embedding vector dimension (required for --index-backend pgvector)",
    )
    ingest.add_argument("--pg-table", default="akc_documents", help="Postgres table name")

    ingest.add_argument("--no-incremental", action="store_true", help="Disable incremental mode")
    ingest.add_argument(
        "--skip-sources-with-errors",
        action="store_true",
        help="Skip per-source connector errors (best-effort ingestion)",
    )

    ingest.add_argument("--no-state", action="store_true", help="Disable state file read/write")
    ingest.add_argument(
        "--state-dir",
        help="Base directory for default state file location (default: CWD)",
    )
    ingest.add_argument("--state-path", help="Explicit path for state file")

    ingest.add_argument("--no-chunking", action="store_true", help="Disable chunking (Phase 1)")
    ingest.add_argument(
        "--chunk-size-chars",
        type=int,
        default=2000,
        help="Chunk size in characters (default: 2000)",
    )
    ingest.add_argument(
        "--chunk-overlap-chars",
        type=int,
        default=200,
        help="Chunk overlap in characters (default: 200)",
    )
    ingest.add_argument(
        "--use-rust-ingest-docs",
        action="store_true",
        help="Use Rust `akc-ingest` docs ingestion for docs connector chunking (experimental).",
    )
    ingest.add_argument(
        "--rust-ingest-min-bytes",
        type=int,
        default=None,
        help="Only use Rust docs ingest for sources larger than this byte threshold.",
    )
    ingest.add_argument(
        "--rust-ingest-mode",
        choices=["cli", "pyo3"],
        default="cli",
        help="Rust ingest backend mode (cli=subprocess JSON boundary; pyo3=PyO3 module).",
    )

    ingest.add_argument("--query", help="Run a similarity query after ingest")
    ingest.add_argument("-k", type=int, default=5, help="Number of results for --query")

    ingest.add_argument("--slack-token", help="Slack bot/user token (or set AKC_SLACK_TOKEN)")
    ingest.add_argument(
        "--slack-oldest",
        help="Oldest Slack timestamp (e.g. 1700000000.000000) to bound history",
    )
    ingest.add_argument(
        "--slack-latest",
        help="Latest Slack timestamp (e.g. 1710000000.000000) to bound history",
    )
    ingest.add_argument(
        "--slack-history-limit",
        type=int,
        default=200,
        help="Slack history/replies page size (default: 200)",
    )
    ingest.add_argument(
        "--slack-max-threads",
        type=int,
        default=200,
        help="Cap number of thread roots processed (default: 200)",
    )
    ingest.add_argument(
        "--slack-max-answers",
        type=int,
        default=3,
        help="Max answers included per thread (default: 3)",
    )
    ingest.add_argument(
        "--slack-include-bot-answers",
        action="store_true",
        help="Allow bot messages as answers (default: false)",
    )

    ingest.set_defaults(func=cmd_ingest)

    slack = sub.add_parser("slack", help="Slack utilities")
    slack.add_argument("--verbose", action="store_true", help="Enable debug logging")
    slack_sub = slack.add_subparsers(dest="slack_command", required=True)

    slack_list = slack_sub.add_parser("list-channels", help="List Slack channels")
    slack_list.add_argument(
        "--slack-token",
        help="Slack bot/user token (or set AKC_SLACK_TOKEN)",
    )
    slack_list.set_defaults(func=cmd_slack_list_channels)

    drift = sub.add_parser("drift", help="Detect drift between sources and emitted outputs")
    drift.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    drift.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    drift.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root (contains <tenant>/<repo>/manifest.json)",
    )
    drift.add_argument(
        "--ingest-state",
        help="Path to ingestion state file to fingerprint for changed_sources detection",
    )
    drift.add_argument(
        "--baseline-path",
        help=(
            "Explicit baseline path (default: "
            "<outputs_root>/<tenant>/<repo>/.akc/living/baseline.json)"
        ),
    )
    drift.add_argument(
        "--update-baseline",
        action="store_true",
        help="Update baseline to current sources/manifest fingerprint and exit 0",
    )
    drift.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    drift.add_argument("--verbose", action="store_true", help="Enable debug logging")
    drift.set_defaults(func=cmd_drift)

    watch = sub.add_parser("watch", help="Watch ingestion state and run drift checks (polling)")
    watch.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    watch.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    watch.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root (contains <tenant>/<repo>/manifest.json)",
    )
    watch.add_argument(
        "--ingest-state",
        help="Path to ingestion state file to watch (required)",
    )
    watch.add_argument(
        "--baseline-path",
        help=(
            "Explicit baseline path (default: "
            "<outputs_root>/<tenant>/<repo>/.akc/living/baseline.json)"
        ),
    )
    watch.add_argument(
        "--poll-interval-s",
        type=float,
        default=1.0,
        help="Polling interval seconds (default: 1.0)",
    )
    watch.add_argument(
        "--debounce-s",
        type=float,
        default=0.75,
        help="Debounce window seconds (default: 0.75)",
    )
    watch.add_argument(
        "--exit-on-drift",
        action="store_true",
        help="Exit with code 2 on first detected drift",
    )
    watch.add_argument("--verbose", action="store_true", help="Enable debug logging")
    watch.set_defaults(func=cmd_watch)

    living = sub.add_parser(
        "living-recompile",
        help="Detect drift (source changes) and safe recompile impacted outputs",
    )
    living.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    living.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    living.add_argument(
        "--outputs-root",
        required=True,
        help=(
            "Outputs root (contains <tenant>/<repo>/...). Used for accepted artifacts + plan state."
        ),
    )
    living.add_argument(
        "--ingest-state",
        required=True,
        help="Path to ingestion state JSON used to detect changed sources.",
    )
    living.add_argument(
        "--baseline-path",
        help=(
            "Explicit baseline path (default: "
            "<outputs_root>/<tenant>/<repo>/.akc/living/baseline.json)"
        ),
    )
    living.add_argument(
        "--goal",
        help="High-level goal/description for this compile run (default: 'Compile repository')",
    )
    living.add_argument(
        "--eval-suite-path",
        help=(
            "Eval suite JSON path used for regression thresholds (default: "
            "configs/evals/intent_system_v1.json)"
        ),
        default="configs/evals/intent_system_v1.json",
    )
    living.add_argument(
        "--policy-mode",
        choices=["audit_only", "enforce"],
        default="enforce",
        help="Policy behavior for tool authorization.",
    )
    living.add_argument(
        "--canary-mode",
        choices=["quick", "thorough"],
        default="quick",
        help="Canary compile budget preset.",
    )
    living.add_argument(
        "--accept-mode",
        choices=["quick", "thorough"],
        default="thorough",
        help="Acceptance compile budget preset.",
    )
    living.add_argument(
        "--canary-test-mode",
        choices=["smoke", "full"],
        default="smoke",
        help="Test gate level for canary compilation.",
    )
    living.add_argument(
        "--allow-network",
        action="store_true",
        help="Allow network egress in the sandbox (default: deny).",
    )
    living.add_argument(
        "--llm-mode",
        choices=["offline", "custom"],
        default="offline",
        help=(
            "LLM backend selection for living recompile. "
            "offline uses deterministic built-in backend; custom loads --llm-backend-class."
        ),
    )
    living.add_argument(
        "--llm-backend-class",
        help=(
            "Custom LLM backend class path for living recompile "
            "('<module>:<Class>' or '<module>.<Class>'). "
            "Class must implement LLMBackend and have a no-arg constructor."
        ),
    )
    living.add_argument(
        "--update-baseline-on-accept",
        action="store_true",
        default=True,
        help="Update living baseline after acceptance succeeds (default: on).",
    )
    living.add_argument(
        "--skip-other-pending",
        action="store_true",
        default=True,
        help="Skip other non-impacted pending plan steps during living recompile.",
    )
    living.add_argument(
        "--opa-policy-path",
        help="Optional Rego policy bundle/file path used for tool authorization decisions.",
    )
    living.add_argument(
        "--opa-decision-path",
        default="data.akc.allow",
        help="OPA decision path to evaluate (default: data.akc.allow).",
    )
    living.set_defaults(func=cmd_living_recompile)

    verify = sub.add_parser(
        "verify",
        help=(
            "Verify emitted artifacts for a tenant/repo: "
            "tests, verifier results, and optional formal checks"
        ),
    )
    verify.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    verify.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    verify.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root (contains <tenant>/<repo>/.akc/manifest.json)",
    )
    verify.add_argument(
        "--mode",
        choices=["strict", "relaxed"],
        default="strict",
        help="Verification mode: strict fails on any issue; relaxed downgrades some failures",
    )
    verify.add_argument(
        "--dafny",
        action="store_true",
        help="Run Dafny formal checks if the tool and sources are available",
    )
    verify.add_argument(
        "--verus",
        action="store_true",
        help="Run Verus formal checks if the tool and sources are available",
    )
    verify.add_argument(
        "--show-findings",
        action="store_true",
        help="Print individual verifier findings when present",
    )
    verify.add_argument("--verbose", action="store_true", help="Enable debug logging")
    verify.set_defaults(func=cmd_verify)

    compile_cmd = sub.add_parser(
        "compile",
        help=(
            "Run the compile loop for a tenant/repo and emit a manifest and test artifacts. "
            "Uses an offline LLM backend by default (no API keys required)."
        ),
    )
    compile_cmd.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    compile_cmd.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    compile_cmd.add_argument(
        "--outputs-root",
        required=True,
        dest="outputs_root",
        help="Outputs root (contains <tenant>/<repo>/manifest.json)",
    )
    compile_cmd.add_argument(
        "--output-dir",
        dest="outputs_root",
        help="Alias for --outputs-root",
    )
    compile_cmd.add_argument(
        "--goal",
        help="High-level goal/description for this compile run (default: 'Compile repository')",
    )
    compile_cmd.add_argument(
        "--mode",
        choices=["quick", "thorough"],
        default="quick",
        help="Compile mode preset: quick (lower cost) or thorough (higher coverage)",
    )
    compile_cmd.add_argument(
        "--replay-mode",
        choices=["live", "llm_vcr", "full_replay", "partial_replay"],
        default="live",
        help=(
            "Replay policy for compile passes: "
            "live (call model+tools), llm_vcr (replay model), "
            "full_replay (replay model+tools), partial_replay (replay model, run tools)."
        ),
    )
    compile_cmd.add_argument(
        "--policy-mode",
        choices=["audit_only", "enforce"],
        default="enforce",
        help=(
            "Policy behavior for tool authorization. "
            "audit_only logs denied actions but continues; enforce blocks denied actions."
        ),
    )
    compile_cmd.add_argument(
        "--opa-policy-path",
        help="Optional Rego policy bundle/file path used for tool authorization decisions.",
    )
    compile_cmd.add_argument(
        "--opa-decision-path",
        default="data.akc.allow",
        help="OPA decision path to evaluate (default: data.akc.allow).",
    )
    compile_cmd.add_argument(
        "--cost-input-per-1k-tokens-usd",
        dest="cost_input_per_1k_usd",
        type=float,
        default=0.0,
        help="Input token billing rate in USD per 1K tokens (default: 0).",
    )
    compile_cmd.add_argument(
        "--cost-output-per-1k-tokens-usd",
        dest="cost_output_per_1k_usd",
        type=float,
        default=0.0,
        help="Output token billing rate in USD per 1K tokens (default: 0).",
    )
    compile_cmd.add_argument(
        "--cost-tool-call-usd",
        type=float,
        default=0.0,
        help="Per tool-call billing rate in USD (default: 0).",
    )
    compile_cmd.add_argument(
        "--replay-manifest-path",
        help=(
            "Optional path to a prior .akc/run/*.manifest.json to seed replay decisions/caches. "
            "If omitted, compile tries the latest scoped run manifest under outputs."
        ),
    )
    compile_cmd.add_argument(
        "--partial-replay-passes",
        help=(
            "Comma-separated pass names to re-run in partial_replay mode "
            "(allowed: plan,retrieve,generate,execute,repair,verify). "
            "If omitted, behavior is derived from the replay manifest."
        ),
    )
    compile_cmd.add_argument(
        "--work-root",
        help=(
            "Optional executor work root. "
            "Defaults to <outputs_root>/<tenant>/<repo> to preserve tenant isolation."
        ),
    )
    compile_cmd.add_argument(
        "--schema-version",
        type=int,
        default=1,
        help="Artifact schema version to emit (default: 1)",
    )
    compile_cmd.add_argument(
        "--use-rust-exec",
        action="store_true",
        help="Use Rust-backed sandboxed Execute (akc-exec / akc_rust) instead of subprocess.",
    )
    compile_cmd.add_argument(
        "--sandbox",
        choices=["dev", "strong"],
        default="dev",
        help=(
            "Execution isolation mode. "
            "dev=subprocess with best-effort caps; "
            "strong=containerized boundary by default with optional WASM lane selection."
        ),
    )
    compile_cmd.add_argument(
        "--strong-lane-preference",
        choices=["docker", "wasm", "auto"],
        default="docker",
        help=(
            "Strong sandbox backend preference. "
            "docker=always Docker, wasm=require Rust WASM lane, "
            "auto=prefer Docker and fallback to WASM when Docker is unavailable."
        ),
    )
    compile_cmd.add_argument(
        "--sandbox-memory-mb",
        type=int,
        default=1024,
        help="Sandbox memory cap in MiB (default: 1024).",
    )
    compile_cmd.add_argument(
        "--sandbox-cpu-fuel",
        type=int,
        default=None,
        help=(
            "Optional WASM CPU/fuel budget for Rust execution. "
            "When set, must be > 0. Lower values fail faster with deterministic "
            "WASM_CPU_FUEL_EXHAUSTED."
        ),
    )
    compile_cmd.add_argument(
        "--sandbox-allow-network",
        action="store_true",
        help="Allow network egress in the sandbox (default: deny).",
    )
    compile_cmd.add_argument(
        "--sandbox-stdout-max-kb",
        type=int,
        default=2048,
        help="Max captured stdout in KiB (default: 2048).",
    )
    compile_cmd.add_argument(
        "--sandbox-stderr-max-kb",
        type=int,
        default=2048,
        help="Max captured stderr in KiB (default: 2048).",
    )
    compile_cmd.add_argument(
        "--docker-image",
        default="python:3.12-slim",
        help="Docker image for strong sandbox (default: python:3.12-slim).",
    )
    compile_cmd.add_argument(
        "--docker-pids-limit",
        type=int,
        default=256,
        help="Docker --pids-limit for strong sandbox (default: 256).",
    )
    compile_cmd.add_argument(
        "--docker-cpus",
        type=float,
        default=None,
        help="Docker --cpus value for strong sandbox (default: unset).",
    )
    compile_cmd.add_argument(
        "--docker-user",
        default=None,
        help=("Docker --user value for strong sandbox. Defaults to 65532:65532 when omitted."),
    )
    compile_cmd.add_argument(
        "--docker-tmpfs",
        action="append",
        default=[],
        help=(
            "Container path to mount as tmpfs for Docker strong sandbox. "
            "Repeat the flag to add multiple mounts. Defaults to /tmp."
        ),
    )
    compile_cmd.add_argument(
        "--docker-seccomp-profile",
        default=None,
        help=(
            "Docker seccomp profile path or identifier for strong sandbox. "
            "Absolute paths must exist and are enforced fail-closed."
        ),
    )
    compile_cmd.add_argument(
        "--docker-apparmor-profile",
        default=None,
        help=(
            "Docker AppArmor profile name for strong sandbox. "
            "Fails closed when AppArmor is unavailable."
        ),
    )
    compile_cmd.add_argument(
        "--docker-ulimit-nofile",
        default=None,
        help=("Docker nofile ulimit for strong sandbox ('<soft>' or '<soft>:<hard>')."),
    )
    compile_cmd.add_argument(
        "--docker-ulimit-nproc",
        default=None,
        help=("Docker nproc ulimit for strong sandbox ('<soft>' or '<soft>:<hard>')."),
    )
    compile_cmd.add_argument(
        "--rust-exec-mode",
        choices=["cli", "pyo3"],
        default="cli",
        help="Rust executor backend mode (cli=subprocess JSON boundary; pyo3=PyO3 module).",
    )
    compile_cmd.add_argument(
        "--rust-exec-lane",
        choices=["process", "wasm"],
        default="process",
        help="Rust execution lane (process=OS process sandbox; wasm=experimental strong boundary).",
    )
    compile_cmd.add_argument(
        "--rust-allow-network",
        action="store_true",
        help="Allow network capability in the Rust executor (default: deny).",
    )
    compile_cmd.add_argument(
        "--wasm-fs-normalize-existing-paths",
        action="store_true",
        help=(
            "For WASM lane fs policy checks, canonicalize existing preopen/write paths "
            "before subset validation and request emission."
        ),
    )
    compile_cmd.add_argument(
        "--wasm-fs-normalization-profile",
        choices=["strict", "relaxed"],
        default="strict",
        help=(
            "WASM fs normalization policy. strict=fail closed on missing/unresolvable "
            "paths, relaxed=leave unresolved paths as-is for Rust-side enforcement."
        ),
    )
    compile_cmd.add_argument(
        "--wasm-preopen-dir",
        action="append",
        default=[],
        help=(
            "Absolute host directory to preopen for the WASM lane. "
            "Repeat the flag to mount multiple directories."
        ),
    )
    compile_cmd.add_argument(
        "--wasm-allow-write-dir",
        action="append",
        default=[],
        help=(
            "Absolute host directory that the WASM lane may mutate. "
            "Must be a subset of --wasm-preopen-dir. Repeatable."
        ),
    )
    compile_cmd.add_argument("--verbose", action="store_true", help="Enable debug logging")
    compile_cmd.set_defaults(func=cmd_compile)

    eval_cmd = sub.add_parser(
        "eval",
        help=(
            "Run a versioned eval suite (intent->system tasks) with deterministic checks, "
            "selective judge scoring, and regression gates."
        ),
    )
    eval_cmd.add_argument(
        "--suite-path",
        required=True,
        help="Path to eval suite JSON/YAML file",
    )
    eval_cmd.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root for running/reading task manifests",
    )
    eval_cmd.add_argument(
        "--baseline-report-path",
        help="Optional prior eval report JSON for regression comparison",
    )
    eval_cmd.add_argument(
        "--report-out",
        help="Optional path to write current eval report JSON",
    )
    eval_cmd.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    eval_cmd.set_defaults(func=cmd_eval)

    metrics = sub.add_parser(
        "metrics",
        help=(
            "Query control-plane cost metrics from "
            "<outputs_root>/<tenant>/.akc/control/metrics.sqlite"
        ),
    )
    metrics.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    metrics.add_argument(
        "--repo-id",
        help="Optional repo identifier; when set, limits rollups/runs to one repo.",
    )
    metrics.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root that contains tenant-scoped .akc/control/metrics.sqlite",
    )
    metrics.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Max recent runs to include (default: 20)",
    )
    metrics.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    metrics.set_defaults(func=cmd_metrics)

    view = sub.add_parser(
        "view",
        help="Read-only local viewer over plan state and emitted artifacts",
    )
    view.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    view.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    view.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root (contains <tenant>/<repo>/manifest.json and .akc/* artifacts)",
    )
    view.add_argument(
        "--plan-base-dir",
        help="Base directory that contains `.akc/plan` (default: CWD); viewer is read-only",
    )
    view.add_argument(
        "--schema-version",
        type=int,
        default=1,
        help="Artifact schema version to validate against (default: 1)",
    )

    view_sub = view.add_subparsers(dest="view_command", required=True)

    view_tui = view_sub.add_parser("tui", help="Interactive terminal UI (curses)")
    view_tui.set_defaults(func=cmd_view)

    view_web = view_sub.add_parser("web", help="Generate a static HTML viewer bundle")
    view_web.add_argument("--out-dir", help="Output directory for the static viewer bundle")
    view_web.set_defaults(func=cmd_view)

    view_export = view_sub.add_parser(
        "export", help="Export a portable evidence bundle (dir + zip)"
    )
    view_export.add_argument("--out-dir", help="Output directory for the export bundle")
    view_export.add_argument(
        "--include-all-evidence",
        action="store_true",
        help="Include all manifest-referenced artifacts (default: true)",
    )
    view_export.add_argument(
        "--no-zip",
        dest="zip",
        action="store_false",
        help="Do not create a .zip alongside the export directory",
    )
    view_export.set_defaults(func=cmd_view, zip=True, include_all_evidence=True)

    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        code = int(args.func(args))
    except BrokenPipeError:  # pragma: no cover
        # Allow piping to tools like head.
        raise SystemExit(0) from None
    except KeyboardInterrupt:
        raise SystemExit(130) from None
    sys.exit(code)
