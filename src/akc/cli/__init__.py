from __future__ import annotations

import argparse
import sys

from akc import __version__

from .compile import cmd_compile
from .drift import cmd_drift, cmd_watch
from .ingest import cmd_ingest, cmd_slack_list_channels
from .verify import cmd_verify
from .view import cmd_view

__all__ = [
    "cmd_compile",
    "cmd_drift",
    "cmd_watch",
    "cmd_ingest",
    "cmd_slack_list_channels",
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
        "--rust-exec-mode",
        choices=["cli", "pyo3"],
        default="cli",
        help="Rust executor backend mode (cli=subprocess JSON boundary; pyo3=PyO3 module).",
    )
    compile_cmd.add_argument(
        "--rust-exec-lane",
        choices=["process", "wasm"],
        default="process",
        help="Rust execution lane (process=OS process sandbox; wasm=reserved for future).",
    )
    compile_cmd.add_argument(
        "--rust-allow-network",
        action="store_true",
        help="Allow network capability in the Rust executor (default: deny).",
    )
    compile_cmd.add_argument("--verbose", action="store_true", help="Enable debug logging")
    compile_cmd.set_defaults(func=cmd_compile)

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
