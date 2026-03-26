from __future__ import annotations

import argparse
import sys
from pathlib import Path

from akc import __version__

from .compile import cmd_compile
from .control import (
    cmd_control_forensics_export,
    cmd_control_incident_export,
    cmd_control_index_rebuild,
    cmd_control_manifest_diff,
    cmd_control_playbook_run,
    cmd_control_policy_bundle_effective_profile,
    cmd_control_policy_bundle_show,
    cmd_control_policy_bundle_validate,
    cmd_control_policy_bundle_write,
    cmd_control_replay_forensics,
    cmd_control_replay_plan,
    cmd_control_runs_label_set,
    cmd_control_runs_list,
    cmd_control_runs_show,
)
from .control_bot import cmd_control_bot_serve, cmd_control_bot_validate_config
from .drift import cmd_drift, cmd_watch
from .eval import cmd_eval
from .ingest import cmd_ingest, cmd_slack_list_channels
from .init import cmd_init, register_init_parser
from .living import cmd_living_recompile, cmd_living_webhook_serve
from .living_doctor import cmd_living_doctor
from .mcp_serve import register_mcp_parser
from .metrics import cmd_metrics
from .policy import cmd_policy_explain
from .runtime import (
    cmd_runtime_autopilot,
    cmd_runtime_checkpoint,
    cmd_runtime_coordination_plan,
    cmd_runtime_events,
    cmd_runtime_reconcile,
    cmd_runtime_replay,
    cmd_runtime_start,
    cmd_runtime_status,
    cmd_runtime_stop,
)
from .verify import cmd_verify

__all__ = [
    "cmd_compile",
    "cmd_drift",
    "cmd_watch",
    "cmd_eval",
    "cmd_ingest",
    "cmd_living_recompile",
    "cmd_living_webhook_serve",
    "cmd_living_doctor",
    "cmd_slack_list_channels",
    "cmd_control_forensics_export",
    "cmd_control_incident_export",
    "cmd_control_index_rebuild",
    "cmd_control_manifest_diff",
    "cmd_control_playbook_run",
    "cmd_control_policy_bundle_effective_profile",
    "cmd_control_policy_bundle_show",
    "cmd_control_policy_bundle_validate",
    "cmd_control_policy_bundle_write",
    "cmd_control_replay_forensics",
    "cmd_control_replay_plan",
    "cmd_control_runs_label_set",
    "cmd_control_runs_list",
    "cmd_control_runs_show",
    "cmd_control_bot_validate_config",
    "cmd_control_bot_serve",
    "cmd_metrics",
    "cmd_policy_explain",
    "cmd_runtime_checkpoint",
    "cmd_runtime_coordination_plan",
    "cmd_runtime_autopilot",
    "cmd_runtime_events",
    "cmd_runtime_reconcile",
    "cmd_runtime_replay",
    "cmd_runtime_start",
    "cmd_runtime_status",
    "cmd_runtime_stop",
    "cmd_view",
    "cmd_verify",
    "cmd_init",
    "main",
]


def cmd_view(args: argparse.Namespace) -> int:
    from .view import cmd_view as _cmd_view

    return int(_cmd_view(args))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="akc", description="Agentic Knowledge Compiler")
    parser.add_argument("--version", action="version", version=f"akc {__version__}")

    sub = parser.add_subparsers(dest="command", required=True)

    register_init_parser(sub)

    ingest = sub.add_parser(
        "ingest",
        help="Ingest sources into a vector index (connectors and index backends are pluggable)",
    )
    ingest.add_argument(
        "--tenant-id",
        default=None,
        help="Tenant identifier (or set AKC_TENANT_ID / .akc/project.json tenant_id)",
    )
    ingest.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=("Developer-role UX profile (default: AKC_DEVELOPER_ROLE_PROFILE, else .akc/project.json, else classic)."),
    )
    ingest.add_argument(
        "--connector",
        required=True,
        choices=["docs", "openapi", "slack", "discord", "telegram", "whatsapp", "mcp"],
        help=(
            "Connector: docs, openapi, slack, discord, telegram, whatsapp (Cloud API webhook captures), "
            "mcp (mcp requires ingest-mcp extra)"
        ),
    )
    ingest.add_argument(
        "--input",
        required=True,
        help=(
            "Connector input: docs root, OpenAPI spec path/URL, Slack/Discord channel id, Telegram placeholder, "
            "comma-separated WhatsApp webhook payload file/dir paths, MCP server name / path to MCP JSON"
        ),
    )

    ingest.add_argument("--verbose", action="store_true", help="Enable debug logging")

    ingest.add_argument(
        "--embedder",
        choices=["hash", "openai", "gemini", "none"],
        default="none",
        help=("Embedding provider (default: none, offline). Use openai/gemini only when explicitly configured."),
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
        help=("Pluggable vector index backend (default: memory; use sqlite/pgvector for persistence)"),
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

    ingest.add_argument("--discord-token", help="Discord bot token (or set AKC_DISCORD_TOKEN)")
    ingest.add_argument("--discord-guild-id", help="Discord guild id (only used for list-channels utilities)")
    ingest.add_argument("--discord-oldest", help="Oldest Discord message id boundary (treated as `after`)")
    ingest.add_argument("--discord-latest", help="Latest Discord message id boundary (treated as `before`)")
    ingest.add_argument(
        "--discord-history-limit",
        type=int,
        default=200,
        help="Discord history/replies fetch limit (default: 200)",
    )
    ingest.add_argument(
        "--discord-max-threads",
        type=int,
        default=200,
        help="Cap number of thread roots processed (default: 200)",
    )
    ingest.add_argument(
        "--discord-max-answers",
        type=int,
        default=3,
        help="Max answers included per thread (default: 3)",
    )
    ingest.add_argument(
        "--discord-include-bot-answers",
        action="store_true",
        help="Allow bot messages as answers (default: false)",
    )
    ingest.add_argument(
        "--discord-timeout-s",
        type=float,
        default=30.0,
        help="Discord API request timeout seconds (default: 30)",
    )
    ingest.add_argument(
        "--discord-max-retries",
        type=int,
        default=3,
        help="Discord API max retries for 429/5xx (default: 3)",
    )

    ingest.add_argument("--telegram-token", help="Telegram bot token (or set AKC_TELEGRAM_TOKEN)")
    ingest.add_argument(
        "--telegram-allowed-updates",
        default=None,
        help='Comma-separated allowed_updates forwarded to getUpdates (e.g. "message,edited_message")',
    )
    ingest.add_argument(
        "--telegram-chat-ids",
        default=None,
        help='Comma-separated chat id allowlist (e.g. "123,-456")',
    )
    ingest.add_argument(
        "--telegram-max-updates",
        type=int,
        default=1000,
        help="Max updates drained per run (default: 1000)",
    )
    ingest.add_argument(
        "--telegram-timeout-s",
        type=int,
        default=50,
        help="Telegram getUpdates long-poll timeout seconds (default: 50)",
    )
    ingest.add_argument(
        "--telegram-request-timeout-s",
        type=float,
        default=70.0,
        help="Telegram HTTP request timeout seconds (default: 70)",
    )
    ingest.add_argument(
        "--telegram-max-retries",
        type=int,
        default=3,
        help="Telegram API max retries for transient errors (default: 3)",
    )
    ingest.add_argument(
        "--telegram-offset-state-path",
        default=None,
        help="Optional explicit path for Telegram offset state JSON (separate from --state-path)",
    )
    ingest.add_argument(
        "--telegram-initial-offset",
        type=int,
        default=None,
        help="Optional initial update_id offset when no Telegram offset state exists",
    )

    ingest.add_argument(
        "--whatsapp-phone-number-id",
        default=None,
        help="Optional filter: only ingest messages for this WhatsApp Business phone_number_id",
    )
    ingest.add_argument(
        "--whatsapp-waba-id",
        default=None,
        help="Optional filter: only ingest messages for this WhatsApp Business Account id",
    )
    ingest.add_argument(
        "--whatsapp-state-path",
        default=None,
        help="Optional JSON path for cross-run WhatsApp message-id dedupe (recommended for incremental runs)",
    )
    ingest.add_argument(
        "--whatsapp-max-seen-ids",
        type=int,
        default=5000,
        help="Max message ids retained in WhatsApp dedupe state (default: 5000)",
    )
    ingest.add_argument(
        "--whatsapp-max-documents",
        type=int,
        default=5000,
        help="Safety cap: max documents emitted per WhatsApp ingest run (default: 5000)",
    )
    ingest.add_argument(
        "--whatsapp-verify-signatures",
        action="store_true",
        help=(
            "Require X-Hub-Signature-256 on stored webhook envelopes (--whatsapp-app-secret or "
            "AKC_WHATSAPP_APP_SECRET required)"
        ),
    )
    ingest.add_argument(
        "--whatsapp-app-secret",
        default=None,
        help=(
            "WhatsApp / Meta app secret for Sig Verif (or set AKC_WHATSAPP_APP_SECRET); "
            "used if --whatsapp-verify-signatures"
        ),
    )

    ingest.add_argument(
        "--mcp-config",
        default=".akc/mcp-ingest.json",
        help="Path to multi-server MCP config JSON (default: .akc/mcp-ingest.json)",
    )
    ingest.add_argument(
        "--mcp-uri-prefix",
        default=None,
        help="Only ingest MCP resources whose URI starts with this prefix",
    )
    ingest.add_argument(
        "--mcp-static-prompt",
        default=None,
        help="Optional extra UTF-8 text source to ingest alongside MCP resources (akc://static-prompt)",
    )
    ingest.add_argument(
        "--mcp-timeout-s",
        type=float,
        default=120.0,
        help="Per-session timeout for MCP stdio/HTTP operations in seconds (default: 120; 0 = no limit)",
    )
    ingest.add_argument(
        "--assertion-index-root",
        metavar="DIR",
        help=(
            "Optional outputs root (same as compile --outputs-root). When set, merges doc-derived "
            "assertions into <DIR>/<tenant>/<repo>/.akc/knowledge/assertions.sqlite per ingested source."
        ),
    )
    ingest.add_argument(
        "--assertion-index-repo-id",
        default="default",
        help="Repo segment for assertion index scope (must match compile --repo-id when merging at compile).",
    )
    ingest.add_argument(
        "--assertion-index-max-per-batch",
        type=int,
        default=256,
        help="Cap doc-derived assertions indexed per ingested source batch (default: 256).",
    )

    ingest.set_defaults(func=cmd_ingest)

    register_mcp_parser(sub)

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
        help=("Explicit baseline path (default: <outputs_root>/<tenant>/<repo>/.akc/living/baseline.json)"),
    )
    drift.add_argument(
        "--update-baseline",
        action="store_true",
        help="Update baseline to current sources/manifest fingerprint and exit 0",
    )
    drift.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    drift.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=(
            "Read-only: resolve developer-role profile for output context "
            "(AKC_DEVELOPER_ROLE_PROFILE / .akc/project.json; does not change drift logic)."
        ),
    )
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
        help=("Explicit baseline path (default: <outputs_root>/<tenant>/<repo>/.akc/living/baseline.json)"),
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
    watch.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=(
            "Read-only: resolve developer-role profile for output context "
            "(AKC_DEVELOPER_ROLE_PROFILE / .akc/project.json; does not change drift logic)."
        ),
    )
    watch.add_argument("--verbose", action="store_true", help="Enable debug logging")
    watch.set_defaults(func=cmd_watch)

    living = sub.add_parser(
        "living-recompile",
        help="Detect drift (source changes) and safe recompile impacted outputs",
    )
    living.add_argument(
        "--tenant-id",
        default=None,
        help="Tenant identifier (or set AKC_TENANT_ID / .akc/project.json tenant_id)",
    )
    living.add_argument(
        "--repo-id",
        default=None,
        help="Repo identifier (or set AKC_REPO_ID / .akc/project.json repo_id)",
    )
    living.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=("Developer-role UX profile (default: AKC_DEVELOPER_ROLE_PROFILE, else .akc/project.json, else classic)."),
    )
    living.add_argument(
        "--outputs-root",
        default=None,
        help=("Outputs root (contains <tenant>/<repo>/...). Or set AKC_OUTPUTS_ROOT / .akc/project.json outputs_root."),
    )
    living.add_argument(
        "--ingest-state",
        required=True,
        help="Path to ingestion state JSON used to detect changed sources.",
    )
    living.add_argument(
        "--baseline-path",
        help=("Explicit baseline path (default: <outputs_root>/<tenant>/<repo>/.akc/living/baseline.json)"),
    )
    living.add_argument(
        "--goal",
        help="High-level goal/description for this compile run (default: 'Compile repository')",
    )
    living.add_argument(
        "--eval-suite-path",
        help=("Eval suite JSON path used for regression thresholds (default: configs/evals/intent_system_v1.json)"),
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
        default=None,
        help=(
            "OPA decision path to evaluate (default: data.akc.allow; "
            "or AKC_OPA_DECISION_PATH / .akc/project.json opa_decision_path)."
        ),
    )
    living.add_argument(
        "--living-automation-profile",
        default=None,
        help=(
            "Phase E living automation profile: off | living_loop_v1 | living_loop_unattended_v1 "
            "(or AKC_LIVING_AUTOMATION_PROFILE / .akc/project.json living_automation_profile)."
        ),
    )
    living.set_defaults(func=cmd_living_recompile)

    living_wh = sub.add_parser(
        "living-webhook-serve",
        help=(
            "Signed HTTP webhook receiver: fleet recompile_triggers/living_drift payloads → one-shot living recompile"
        ),
    )
    living_wh.add_argument(
        "--bind",
        default="127.0.0.1",
        help="Listen address (default 127.0.0.1; bind 0.0.0.0 only behind a reverse proxy/TLS)",
    )
    living_wh.add_argument("--port", type=int, default=8787, help="Listen port (default 8787)")
    living_wh.add_argument(
        "--secret",
        default=None,
        help="Shared secret for X-AKC-Signature (or set AKC_LIVING_WEBHOOK_SECRET)",
    )
    living_wh.add_argument(
        "--cwd",
        default=".",
        help="Directory for .akc/project.json and relative paths (default: current directory)",
    )
    living_wh.add_argument(
        "--ingest-state",
        default=None,
        help="Ingest state JSON path (or AKC_INGEST_STATE_PATH / project ingest_state_path)",
    )
    living_wh.add_argument(
        "--tenant-allowlist",
        default=None,
        help="Comma-separated tenant ids to accept, or * (default *; or AKC_LIVING_WEBHOOK_TENANT_ALLOWLIST)",
    )
    living_wh.add_argument(
        "--outputs-root-allowlist",
        default=None,
        help=(
            "Comma-separated directory roots: payload outputs_root must resolve under one of these "
            "(or AKC_LIVING_WEBHOOK_OUTPUTS_ROOT_ALLOWLIST; default: outputs_root from .akc/project.json)"
        ),
    )
    living_wh.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=("Developer-role UX profile (AKC_DEVELOPER_ROLE_PROFILE / .akc/project.json)."),
    )
    living_wh.add_argument(
        "--eval-suite-path",
        default="configs/evals/intent_system_v1.json",
        help="Eval suite JSON path (relative to --cwd unless absolute)",
    )
    living_wh.add_argument(
        "--goal",
        default=None,
        help="Compile goal string (default: Compile repository)",
    )
    living_wh.add_argument(
        "--policy-mode",
        choices=["audit_only", "enforce"],
        default="enforce",
        help="Policy behavior for tool authorization.",
    )
    living_wh.add_argument(
        "--canary-mode",
        choices=["quick", "thorough"],
        default="quick",
        help="Canary compile budget preset.",
    )
    living_wh.add_argument(
        "--accept-mode",
        choices=["quick", "thorough"],
        default="thorough",
        help="Acceptance compile budget preset.",
    )
    living_wh.add_argument(
        "--canary-test-mode",
        choices=["smoke", "full"],
        default="smoke",
        help="Test gate level for canary compilation.",
    )
    living_wh.add_argument(
        "--allow-network",
        action="store_true",
        help="Allow network egress in the sandbox (default: deny).",
    )
    living_wh.add_argument(
        "--llm-mode",
        choices=["offline", "custom"],
        default="offline",
        help="LLM backend for living recompile (offline or custom --llm-backend-class).",
    )
    living_wh.add_argument(
        "--llm-backend-class",
        help="Custom LLM backend class when --llm-mode custom.",
    )
    living_wh.add_argument(
        "--update-baseline-on-accept",
        action="store_true",
        default=True,
        help="Update living baseline after acceptance succeeds (default: on).",
    )
    living_wh.add_argument(
        "--skip-other-pending",
        action="store_true",
        default=True,
        help="Skip other non-impacted pending plan steps during living recompile.",
    )
    living_wh.add_argument(
        "--opa-policy-path",
        help="Optional Rego policy bundle/file path (or AKC_OPA_POLICY_PATH / project).",
    )
    living_wh.add_argument(
        "--opa-decision-path",
        default=None,
        help="OPA decision path (default data.akc.allow; or env / project).",
    )
    living_wh.add_argument(
        "--living-automation-profile",
        default=None,
        help="Living automation profile (AKC_LIVING_AUTOMATION_PROFILE / project).",
    )
    living_wh.add_argument("--verbose", action="store_true", help="Enable debug logging")
    living_wh.set_defaults(func=cmd_living_webhook_serve)

    living_doctor = sub.add_parser(
        "living-doctor",
        help="Validate unattended living wiring (profile, paths, eval suite hooks, claim alignment)",
    )
    living_doctor.add_argument(
        "--tenant-id",
        default=None,
        help="Tenant identifier (or set AKC_TENANT_ID / .akc/project.json tenant_id)",
    )
    living_doctor.add_argument(
        "--repo-id",
        default=None,
        help="Repo identifier (or set AKC_REPO_ID / .akc/project.json repo_id)",
    )
    living_doctor.add_argument(
        "--outputs-root",
        default=None,
        help=("Outputs root (contains <tenant>/<repo>/...). Or set AKC_OUTPUTS_ROOT / .akc/project.json outputs_root."),
    )
    living_doctor.add_argument(
        "--ingest-state-path",
        default=None,
        help=(
            "Ingest state JSON path for checks (or AKC_INGEST_STATE_PATH / project ingest_state_path). "
            "Required when profile or claim requests unattended validation."
        ),
    )
    living_doctor.add_argument(
        "--eval-suite-path",
        default=None,
        help="Eval suite JSON (default: configs/evals/intent_system_v1.json under cwd)",
    )
    living_doctor.add_argument(
        "--living-automation-profile",
        default=None,
        help="Override profile resolution for this command (else env / project)",
    )
    living_doctor.add_argument(
        "--relaxed-baseline",
        action="store_true",
        help="Warn instead of fail when .akc/living/baseline.json is missing",
    )
    living_doctor.add_argument(
        "--lease-backend",
        choices=["filesystem", "k8s"],
        default=None,
        help=("Expected autopilot lease backend for single-writer checks (or AKC_AUTOPILOT_LEASE_BACKEND)"),
    )
    living_doctor.add_argument(
        "--lease-namespace",
        default=None,
        help="Expected Kubernetes lease namespace when using lease_backend=k8s (or env override)",
    )
    living_doctor.add_argument(
        "--expect-replicas",
        type=int,
        default=None,
        help="Declared replica count for deployment warnings (or AKC_AUTOPILOT_EXPECT_REPLICAS)",
    )
    living_doctor.add_argument("--verbose", action="store_true", help="Enable debug logging")
    living_doctor.set_defaults(func=cmd_living_doctor)

    verify = sub.add_parser(
        "verify",
        help=("Verify emitted artifacts for a tenant/repo: tests, verifier results, and optional formal checks"),
    )
    verify.add_argument(
        "--tenant-id",
        default=None,
        help="Tenant identifier (or set AKC_TENANT_ID / .akc/project.json tenant_id)",
    )
    verify.add_argument(
        "--repo-id",
        default=None,
        help="Repo identifier (or set AKC_REPO_ID / .akc/project.json repo_id)",
    )
    verify.add_argument(
        "--outputs-root",
        default=None,
        help=(
            "Outputs root (contains <tenant>/<repo> artifact tree). "
            "Or set AKC_OUTPUTS_ROOT / .akc/project.json outputs_root."
        ),
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
    verify.add_argument(
        "--run-id",
        help=(
            "Compile run id for operational verify/replay attestation checks "
            "(default: latest run manifest for tenant/repo when coupling is enabled)"
        ),
    )
    verify.add_argument(
        "--no-operational-coupling",
        action="store_true",
        help="Disable default operational coupling verification for this command",
    )
    verify.add_argument(
        "--living-unattended",
        action="store_true",
        help="Run unattended living checks (same as akc living-doctor; requires scope outputs layout)",
    )
    verify.add_argument(
        "--living-unattended-relaxed-baseline",
        action="store_true",
        help="With --living-unattended, warn instead of fail when living baseline.json is missing",
    )
    verify.add_argument(
        "--living-automation-profile",
        default=None,
        help="Override living automation profile for --living-unattended (CLI > env > project)",
    )
    verify.add_argument(
        "--lease-backend",
        choices=["filesystem", "k8s"],
        default=None,
        help=(
            "With --living-unattended: expected autopilot lease backend for single-writer checks "
            "(CLI overrides AKC_AUTOPILOT_LEASE_BACKEND)"
        ),
    )
    verify.add_argument(
        "--lease-namespace",
        default=None,
        help=(
            "With --living-unattended: Kubernetes lease namespace when lease_backend=k8s "
            "(CLI overrides AKC_AUTOPILOT_LEASE_NAMESPACE)"
        ),
    )
    verify.add_argument(
        "--expect-replicas",
        type=int,
        default=None,
        help=(
            "With --living-unattended: declared replica count for warnings "
            "(CLI overrides AKC_AUTOPILOT_EXPECT_REPLICAS)"
        ),
    )
    verify.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=("Developer-role UX profile (default: AKC_DEVELOPER_ROLE_PROFILE, else .akc/project.json, else classic)."),
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
    compile_cmd.add_argument(
        "--tenant-id",
        default=None,
        help="Tenant identifier (or set AKC_TENANT_ID / .akc/project.json tenant_id)",
    )
    compile_cmd.add_argument(
        "--repo-id",
        default=None,
        help="Repo identifier (or set AKC_REPO_ID / .akc/project.json repo_id)",
    )
    compile_cmd.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=("Developer-role UX profile (default: AKC_DEVELOPER_ROLE_PROFILE, else .akc/project.json, else classic)."),
    )
    compile_cmd.add_argument(
        "--outputs-root",
        default=None,
        dest="outputs_root",
        help="Outputs root (contains <tenant>/<repo>/manifest.json); or AKC_OUTPUTS_ROOT / .akc/project.json",
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
        "--intent-file",
        dest="intent_file",
        help=(
            "Optional tenant+repo-scoped intent JSON/YAML file (intent-backed run). "
            "When provided, compile derives objectives/constraints/success criteria from it."
        ),
    )
    compile_cmd.add_argument(
        "--mode",
        choices=["quick", "thorough"],
        default="quick",
        help="Compile mode preset: quick (lower cost) or thorough (higher coverage)",
    )
    compile_cmd.add_argument(
        "--compile-realization-mode",
        choices=["artifact_only", "scoped_apply"],
        default="scoped_apply",
        help=(
            "Compile realization: scoped_apply (default, policy-gated patch apply under "
            "--apply-scope-root or --work-root) or artifact_only (opt-in, no working-tree writes)."
        ),
    )
    compile_cmd.add_argument(
        "--apply-scope-root",
        default=None,
        help=(
            "Absolute directory for scoped_apply (tenant/repo work tree). "
            "When omitted under scoped_apply, defaults to --work-root / outputs scope root."
        ),
    )
    compile_cmd.add_argument(
        "--promotion-mode",
        choices=["artifact_only", "staged_apply", "live_apply"],
        default=None,
        help=(
            "Promotion state machine mode. Default resolves to staged_apply in non-dev "
            "environments and artifact_only in dev."
        ),
    )
    compile_cmd.add_argument(
        "--require-deployable-steps",
        dest="require_deployable_steps",
        action="store_true",
        default=None,
        help=(
            "Fail closed when deployable intent resolves to an empty actionable plan. "
            "Default: enabled for staged_apply/live_apply."
        ),
    )
    compile_cmd.add_argument(
        "--no-require-deployable-steps",
        dest="require_deployable_steps",
        action="store_false",
        help="Disable fail-closed empty-plan gating for deployable intents.",
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
        help=(
            "Optional Rego policy path for tool authorization "
            "(or AKC_OPA_POLICY_PATH / .akc/project.json opa_policy_path)."
        ),
    )
    compile_cmd.add_argument(
        "--opa-decision-path",
        default=None,
        help=(
            "OPA decision path to evaluate (default: data.akc.allow; "
            "or AKC_OPA_DECISION_PATH / .akc/project.json opa_decision_path)."
        ),
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
        help=("Optional executor work root. Defaults to <outputs_root>/<tenant>/<repo> to preserve tenant isolation."),
    )
    compile_cmd.add_argument(
        "--schema-version",
        type=int,
        default=1,
        help="Artifact schema version to emit (default: 1)",
    )
    compile_cmd.add_argument(
        "--ir-operational-structure-policy",
        choices=["off", "warn", "error"],
        default=None,
        help=(
            "IR workflow/intent operational gate (default: warn). "
            "When omitted, AKC_IR_OPERATIONAL_STRUCTURE_POLICY is used if set."
        ),
    )
    compile_cmd.add_argument(
        "--ir-graph-integrity-policy",
        choices=["off", "warn", "error"],
        default=None,
        help=(
            "IR graph integrity gate (depends_on, knowledge hub, deployables). "
            "When omitted, AKC_IR_GRAPH_INTEGRITY_POLICY if set, else inherit from operational policy."
        ),
    )
    compile_cmd.add_argument(
        "--artifact-consistency-policy",
        choices=["off", "warn", "error"],
        default=None,
        help=(
            "Cross-artifact consistency gate when enforced by the compile session. "
            "When omitted, AKC_ARTIFACT_CONSISTENCY_POLICY is used if set (default: warn)."
        ),
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
        help=("Docker AppArmor profile name for strong sandbox. Fails closed when AppArmor is unavailable."),
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
        help=("Absolute host directory to preopen for the WASM lane. Repeat the flag to mount multiple directories."),
    )
    compile_cmd.add_argument(
        "--wasm-allow-write-dir",
        action="append",
        default=[],
        help=(
            "Absolute host directory that the WASM lane may mutate. Must be a subset of --wasm-preopen-dir. Repeatable."
        ),
    )
    compile_cmd.add_argument(
        "--stored-assertion-index",
        choices=["off", "merge"],
        default="off",
        help=(
            "Merge ingest-built `.akc/knowledge/assertions.sqlite` rows that match retrieved doc_ids "
            "(use same outputs root + repo id as ingest; default: off)."
        ),
    )
    compile_cmd.add_argument(
        "--stored-assertion-index-max-rows",
        type=int,
        default=64,
        help="Max indexed assertions to merge per compile retrieve (default: 64).",
    )
    compile_cmd.add_argument(
        "--no-operator-knowledge-decisions",
        action="store_true",
        help="Do not apply `.akc/knowledge/decisions.json` overrides for this run.",
    )
    compile_cmd.add_argument(
        "--runtime-bundle-embed-system-ir",
        action="store_true",
        help=(
            "Embed full system IR JSON in the runtime bundle (larger artifact; "
            "for air-gapped transfer or debugging; default off)."
        ),
    )
    compile_cmd.add_argument(
        "--compile-skills-mode",
        choices=["off", "default_only", "explicit", "auto"],
        default=None,
        help=(
            "Agent Skills (SKILL.md) injection into patch LLM system prompt: "
            "off=none; default_only=bundled AKC skill; explicit=default + --compile-skill / project compile_skills; "
            "auto=explicit plus keyword-scored extras (respects disable-model-invocation). "
            "Default: default_only, or explicit when compile_skills / --compile-skill are set. "
            "Optional global scan root: AKC_SKILLS_ROOT (single directory, for headless automation)."
        ),
    )
    compile_cmd.add_argument(
        "--compile-skill",
        action="append",
        default=[],
        metavar="NAME",
        help="Activate a skill by manifest name (repeatable; merged with .akc/project.json compile_skills).",
    )
    compile_cmd.add_argument(
        "--compile-skill-extra-root",
        action="append",
        default=[],
        metavar="PATH",
        help=(
            "Extra absolute directory scanned for Agent Skills packages (child dirs with SKILL.md). "
            "Relative paths are resolved from the current working directory. Repeatable; merged with "
            "ControllerConfig.compile_skill_extra_roots. See also AKC_SKILLS_ROOT for a single env-provided root."
        ),
    )
    compile_cmd.add_argument(
        "--compile-skill-max-file-bytes",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Maximum bytes read per discovered SKILL.md (ControllerConfig.compile_skill_max_file_bytes). "
            "Overrides .akc/project.json compile_skill_max_file_bytes when set."
        ),
    )
    compile_cmd.add_argument(
        "--compile-skill-max-total-bytes",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Maximum UTF-8 bytes for the injected Agent Skills system preamble "
            "(ControllerConfig.compile_skill_max_total_bytes). "
            "Overrides .akc/project.json compile_skill_max_total_bytes when set."
        ),
    )
    compile_cmd.add_argument(
        "--compile-mcp",
        action="store_true",
        help=(
            "Enable optional compile-time MCP (requires ingest-mcp extra). "
            "Servers are defined in JSON (default: <work-root>/.akc/mcp-ingest.json); "
            "extends tool allowlist with mcp.resource.read and mcp.tool.call."
        ),
    )
    compile_cmd.add_argument(
        "--compile-mcp-config",
        default=None,
        help="Path to multi-server MCP JSON (default: <work-root>/.akc/mcp-ingest.json when --compile-mcp is set).",
    )
    compile_cmd.add_argument(
        "--compile-mcp-server",
        default=None,
        help="Logical server name from the MCP config (else default_server from the JSON file).",
    )
    compile_cmd.add_argument(
        "--compile-mcp-resource",
        action="append",
        default=[],
        help="MCP resource URI to read at retrieve (repeatable).",
    )
    compile_cmd.add_argument(
        "--compile-mcp-tool",
        action="append",
        default=[],
        metavar="JSON",
        help='MCP tools/call spec as JSON, e.g. {"tool_name":"echo","arguments":{"message":"hi"}} (repeatable).',
    )
    compile_cmd.add_argument(
        "--compile-mcp-tools-generate-only",
        action="store_true",
        help="Run configured --compile-mcp-tool only on generate iterations, not repair.",
    )
    compile_cmd.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="CLI output format for auxiliary diagnostics (e.g. structured policy denial on compile failure).",
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

    runtime = sub.add_parser("runtime", help="Operate the runtime control plane surface")
    runtime_sub = runtime.add_subparsers(dest="runtime_command", required=True)

    runtime_start = runtime_sub.add_parser("start", help="Start a runtime run from a runtime bundle")
    runtime_start.add_argument("--bundle", help="Path to runtime_bundle.json")
    runtime_start.add_argument(
        "--mode",
        choices=["simulate", "enforce", "canary"],
        default=None,
        help="Runtime/reconcile mode",
    )
    runtime_start.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=("Developer-role UX profile (default: AKC_DEVELOPER_ROLE_PROFILE, else .akc/project.json, else classic)."),
    )
    runtime_start.add_argument(
        "--outputs-root",
        default=None,
        help=(
            "Outputs root for locating bundles and state. "
            "Or set AKC_OUTPUTS_ROOT / .akc/project.json outputs_root. "
            "When omitted, inferred from bundle path when --bundle is set."
        ),
    )
    runtime_start.add_argument(
        "--strict-intent-authority",
        action="store_true",
        help=(
            "Load normalized intent from .akc/intent, verify stable_intent_sha256, and require "
            "intent_policy_projection to match a fresh projection (also controllable via "
            "runtime_policy_envelope.intent_authority_strict on the bundle)."
        ),
    )
    runtime_start.add_argument(
        "--coordination-parallel-dispatch",
        choices=["inherit", "enabled", "disabled"],
        default="inherit",
        help="Override coordination parallel dispatch mode (default: inherit from bundle contract).",
    )
    runtime_start.add_argument(
        "--coordination-max-in-flight-steps",
        type=int,
        help="Override coordination max in-flight steps when parallel dispatch is enabled.",
    )
    runtime_start.add_argument(
        "--coordination-max-in-flight-per-role",
        type=int,
        help="Override coordination max in-flight steps per role when parallel dispatch is enabled.",
    )
    runtime_start.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format for diagnostics (structured policy denial on policy-related failures).",
    )
    runtime_start.add_argument(
        "--delivery-target-lane",
        choices=["staging", "production"],
        default=None,
        help=(
            "Maps aggregate health timestamps to staging vs production delivery lifecycle fields "
            "(default: AKC_DELIVERY_TARGET_LANE, else staging)."
        ),
    )
    runtime_start.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_start.set_defaults(func=cmd_runtime_start)

    runtime_coordination_plan = runtime_sub.add_parser(
        "coordination-plan",
        help="Print deterministic coordination schedule layers from a runtime bundle JSON",
    )
    runtime_coordination_plan.add_argument(
        "--bundle",
        required=True,
        help="Path to runtime_bundle.json",
    )
    runtime_coordination_plan.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_coordination_plan.set_defaults(func=cmd_runtime_coordination_plan)

    runtime_stop = runtime_sub.add_parser("stop", help="Request stop for a runtime run")
    runtime_stop.add_argument("--runtime-run-id", required=True, help="Runtime run identifier")
    runtime_stop.add_argument(
        "--outputs-root",
        default=".",
        help="Outputs root used to locate runtime state (default: current directory)",
    )
    runtime_stop.add_argument("--tenant-id", help="Optional tenant scope hint")
    runtime_stop.add_argument("--repo-id", help="Optional repo scope hint")
    runtime_stop.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_stop.set_defaults(func=cmd_runtime_stop)

    runtime_status = runtime_sub.add_parser("status", help="Show runtime run status")
    runtime_status.add_argument("--runtime-run-id", required=True, help="Runtime run identifier")
    runtime_status.add_argument(
        "--outputs-root",
        default=".",
        help="Outputs root used to locate runtime state (default: current directory)",
    )
    runtime_status.add_argument("--tenant-id", help="Optional tenant scope hint")
    runtime_status.add_argument("--repo-id", help="Optional repo scope hint")
    runtime_status.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_status.set_defaults(func=cmd_runtime_status)

    runtime_events = runtime_sub.add_parser("events", help="Show runtime event transcript")
    runtime_events.add_argument("--runtime-run-id", required=True, help="Runtime run identifier")
    runtime_events.add_argument(
        "--outputs-root",
        default=".",
        help="Outputs root used to locate runtime state (default: current directory)",
    )
    runtime_events.add_argument("--tenant-id", help="Optional tenant scope hint")
    runtime_events.add_argument("--repo-id", help="Optional repo scope hint")
    runtime_events.add_argument("--follow", action="store_true", help="Follow event output")
    runtime_events.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_events.set_defaults(func=cmd_runtime_events)

    runtime_reconcile = runtime_sub.add_parser(
        "reconcile",
        help="Run reconcile for an existing runtime run",
    )
    runtime_reconcile.add_argument("--runtime-run-id", required=True, help="Runtime run identifier")
    runtime_reconcile.add_argument(
        "--outputs-root",
        default=".",
        help="Outputs root used to locate runtime state (default: current directory)",
    )
    runtime_reconcile.add_argument("--tenant-id", help="Optional tenant scope hint")
    runtime_reconcile.add_argument("--repo-id", help="Optional repo scope hint")
    mode_group = runtime_reconcile.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--dry-run", action="store_true", help="Simulate reconcile only")
    mode_group.add_argument("--apply", action="store_true", help="Apply reconcile operations")
    runtime_reconcile.add_argument(
        "--watch",
        action="store_true",
        help="Run a bounded multi-iteration reconcile loop (level-triggered resync)",
    )
    runtime_reconcile.add_argument(
        "--watch-interval-sec",
        type=float,
        default=5.0,
        help="Sleep between reconcile iterations when --watch is set (default: 5)",
    )
    runtime_reconcile.add_argument(
        "--watch-max-iterations",
        type=int,
        default=30,
        help="Maximum reconcile iterations when --watch is set (default: 30)",
    )
    runtime_reconcile.add_argument(
        "--strict-intent-authority",
        action="store_true",
        help="Same as runtime start: verify intent store + projection before loading the bundle.",
    )
    runtime_reconcile.add_argument(
        "--coordination-parallel-dispatch",
        choices=["inherit", "enabled", "disabled"],
        default="inherit",
        help="Override coordination parallel dispatch mode (default: inherit from bundle contract).",
    )
    runtime_reconcile.add_argument(
        "--coordination-max-in-flight-steps",
        type=int,
        help="Override coordination max in-flight steps when parallel dispatch is enabled.",
    )
    runtime_reconcile.add_argument(
        "--coordination-max-in-flight-per-role",
        type=int,
        help="Override coordination max in-flight steps per role when parallel dispatch is enabled.",
    )
    runtime_reconcile.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_reconcile.set_defaults(func=cmd_runtime_reconcile)

    runtime_checkpoint = runtime_sub.add_parser("checkpoint", help="Show runtime checkpoint")
    runtime_checkpoint.add_argument("--runtime-run-id", required=True, help="Runtime run identifier")
    runtime_checkpoint.add_argument(
        "--outputs-root",
        default=".",
        help="Outputs root used to locate runtime state (default: current directory)",
    )
    runtime_checkpoint.add_argument("--tenant-id", help="Optional tenant scope hint")
    runtime_checkpoint.add_argument("--repo-id", help="Optional repo scope hint")
    runtime_checkpoint.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_checkpoint.set_defaults(func=cmd_runtime_checkpoint)

    runtime_replay = runtime_sub.add_parser("replay", help="Replay runtime evidence")
    runtime_replay.add_argument("--runtime-run-id", required=True, help="Runtime run identifier")
    runtime_replay.add_argument(
        "--mode",
        required=True,
        choices=["runtime_replay", "reconcile_replay"],
        help="Replay mode",
    )
    runtime_replay.add_argument(
        "--outputs-root",
        default=".",
        help="Outputs root used to locate runtime state (default: current directory)",
    )
    runtime_replay.add_argument("--tenant-id", help="Optional tenant scope hint")
    runtime_replay.add_argument("--repo-id", help="Optional repo scope hint")
    runtime_replay.add_argument("--verbose", action="store_true", help="Enable debug logging")
    runtime_replay.set_defaults(func=cmd_runtime_replay)

    runtime_autopilot = runtime_sub.add_parser(
        "autopilot",
        help="Run always-on living recompile + reliability KPIs controller loop",
    )
    runtime_autopilot.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root containing tenant/repo scopes and autopilot persistence (.akc/autopilot/)",
    )
    runtime_autopilot.add_argument(
        "--ingest-state-path",
        required=True,
        help="Path to ingest state JSON used for drift checks (passed to safe recompile)",
    )
    runtime_autopilot.add_argument("--tenant-id", help="Optional tenant scope hint")
    runtime_autopilot.add_argument("--repo-id", help="Optional repo scope hint")
    runtime_autopilot.add_argument(
        "--eval-suite-path",
        default="configs/evals/intent_system_v1.json",
        help="Path to eval suite file passed to the acceptance compilation",
    )
    runtime_autopilot.add_argument(
        "--policy-mode",
        choices=["audit_only", "enforce"],
        default="enforce",
        help="Policy mode used by safe recompile (default: enforce)",
    )
    runtime_autopilot.add_argument(
        "--canary-mode",
        choices=["quick", "thorough"],
        default="quick",
        help="Canary eval depth (default: quick)",
    )
    runtime_autopilot.add_argument(
        "--accept-mode",
        choices=["quick", "thorough"],
        default="thorough",
        help="Acceptance eval depth (default: thorough)",
    )
    runtime_autopilot.add_argument(
        "--living-check-interval-sec",
        type=float,
        default=3600.0,
        help="Min seconds between drift checks per tenant/repo scope (default: 3600)",
    )
    runtime_autopilot.add_argument(
        "--scoreboard-window-ms",
        type=int,
        default=7 * 24 * 60 * 60 * 1000,
        help="Reliability scoreboard KPI window size (ms, default: 7d)",
    )
    runtime_autopilot.add_argument("--max-iterations", type=int, help="Optional bounded iterations for testing")
    runtime_autopilot.add_argument("--goal", default="Compile repository", help="Controller goal string")
    runtime_autopilot.add_argument(
        "--controller-id",
        help="Stable controller identifier used for lease leadership",
    )
    runtime_autopilot.add_argument(
        "--lease-backend",
        choices=["filesystem", "k8s"],
        default="filesystem",
        help="Lease backend for controller leadership (default: filesystem)",
    )
    runtime_autopilot.add_argument(
        "--lease-name",
        help="Optional lease name override (default: derived from tenant/repo scope)",
    )
    runtime_autopilot.add_argument(
        "--lease-namespace",
        help="Optional lease namespace (reserved for distributed backends like Kubernetes)",
    )
    runtime_autopilot.add_argument(
        "--scope-registry-path",
        help="Optional explicit scope registry JSON path (array of {tenant_id,repo_id})",
    )
    runtime_autopilot.add_argument(
        "--env-profile",
        choices=["dev", "staging", "prod"],
        default="staging",
        help=(
            "Safety profile for artifacts and (with --unattended-defaults) drift pacing: "
            "staging = more frequent checks; prod = longer intervals and tighter budgets/escalation "
            "(default: staging)."
        ),
    )
    runtime_autopilot.add_argument(
        "--living-automation-profile",
        default=None,
        help=(
            "off | living_loop_v1 | living_loop_unattended_v1 "
            "(AKC_LIVING_AUTOMATION_PROFILE / project; bridge + eval defaults apply to loop profiles)."
        ),
    )
    runtime_autopilot.add_argument("--verbose", action="store_true", help="Enable debug logging")

    # Autonomy budgets: explicit values unless --unattended-defaults (living_loop_unattended_v1).
    runtime_autopilot.add_argument(
        "--unattended-defaults",
        action="store_true",
        help=("Use env-profile matrix for autonomy budgets and drift interval (requires living_loop_unattended_v1)."),
    )
    runtime_autopilot.add_argument(
        "--max-mutations-per-day",
        type=int,
        default=None,
        help="Max allowed live mutations per day per tenant/repo scope (required unless --unattended-defaults)",
    )
    runtime_autopilot.add_argument(
        "--max-concurrent-rollouts",
        type=int,
        default=None,
        help="Max concurrent runtime rollouts per tenant/repo scope (required unless --unattended-defaults)",
    )
    runtime_autopilot.add_argument(
        "--rollback-budget-per-day",
        type=int,
        default=None,
        help="Max allowed rollback-chain counts per day per tenant/repo scope (required unless --unattended-defaults)",
    )
    runtime_autopilot.add_argument(
        "--max-consecutive-rollout-failures",
        type=int,
        default=None,
        help=("Escalate after N consecutive rollout failures per scope (required unless --unattended-defaults)"),
    )
    runtime_autopilot.add_argument(
        "--max-rollbacks-per-day-before-escalation",
        type=int,
        default=None,
        help="Escalate after N rollbacks/day per tenant/repo scope (required unless --unattended-defaults)",
    )
    runtime_autopilot.add_argument(
        "--cooldown-after-failure-ms",
        type=int,
        default=None,
        help="Cooldown after compile/runtime failure (ms) (required unless --unattended-defaults)",
    )
    runtime_autopilot.add_argument(
        "--cooldown-after-policy-deny-ms",
        type=int,
        default=None,
        help="Cooldown after promotion denial (ms) (required unless --unattended-defaults)",
    )
    runtime_autopilot.set_defaults(func=cmd_runtime_autopilot)

    metrics = sub.add_parser(
        "metrics",
        help=("Query control-plane cost metrics from <outputs_root>/<tenant>/.akc/control/metrics.sqlite"),
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

    policy = sub.add_parser(
        "policy",
        help="Policy governance metadata and decision explainability (read-only)",
    )
    policy_sub = policy.add_subparsers(dest="policy_command", required=True)
    policy_explain = policy_sub.add_parser(
        "explain",
        help="Show policy provenance and recorded decisions for a run (manifest inline or ref)",
    )
    policy_explain.add_argument("--manifest", help="Path to <run_id>.manifest.json")
    policy_explain.add_argument("--run-id", help="Run id (requires --outputs-root, --tenant-id, --repo-id)")
    policy_explain.add_argument("--tenant-id", help="Tenant id (for --run-id resolution)")
    policy_explain.add_argument("--repo-id", help="Repo id (for --run-id resolution)")
    policy_explain.add_argument(
        "--outputs-root",
        help="Outputs root containing <tenant>/<repo>/.akc/run/ (for --run-id resolution)",
    )
    policy_explain.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    policy_explain.add_argument(
        "--record-audit",
        action="store_true",
        help="Append JSON line to <outputs_root>/<tenant>/.akc/control/control_audit.jsonl",
    )
    policy_explain.add_argument(
        "--audit-actor",
        default=None,
        help="Actor name for audit row (default: $USER or AKC_AUDIT_ACTOR)",
    )
    policy_explain.set_defaults(func=cmd_policy_explain)

    control = sub.add_parser(
        "control",
        help=(
            "Operator control-plane indexes (cross-run catalog under "
            "<outputs_root>/<tenant>/.akc/control/operations.sqlite)"
        ),
    )
    control_sub = control.add_subparsers(dest="control_group", required=True)

    control_runs = control_sub.add_parser("runs", help="Query the operations index for runs")
    runs_sub = control_runs.add_subparsers(dest="runs_action", required=True)

    cr_list = runs_sub.add_parser("list", help="List indexed runs (filters optional)")
    cr_list.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    cr_list.add_argument(
        "--repo-id",
        help="Optional repo identifier; limits results to one repo (normalized like outputs paths)",
    )
    cr_list.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root that contains <tenant>/.akc/control/operations.sqlite",
    )
    cr_list.add_argument(
        "--since-ms",
        type=int,
        help="Include runs with updated_at_ms >= this value",
    )
    cr_list.add_argument(
        "--until-ms",
        type=int,
        help="Include runs with updated_at_ms <= this value",
    )
    cr_list.add_argument(
        "--intent-sha256",
        help="Filter to runs with this stable_intent_sha256 (64-char hex)",
    )
    cr_list.add_argument(
        "--has-recompile-triggers",
        choices=["any", "yes", "no"],
        default="any",
        help="Filter by recompile_trigger_count>0 (default: any)",
    )
    cr_list.add_argument(
        "--runtime-evidence",
        choices=["any", "yes", "no"],
        default="any",
        help="Filter by runtime evidence present (inline or control_plane ref; default: any)",
    )
    cr_list.add_argument(
        "--limit",
        type=int,
        default=50,
        help="Max runs to return (default: 50, max 500)",
    )
    cr_list.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    cr_list.set_defaults(func=cmd_control_runs_list)

    cr_show = runs_sub.add_parser("show", help="Show one run and indexed sidecar pointers")
    cr_show.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    cr_show.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    cr_show.add_argument("--run-id", required=True, help="Compile run identifier")
    cr_show.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root that contains <tenant>/.akc/control/operations.sqlite",
    )
    cr_show.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    cr_show.set_defaults(func=cmd_control_runs_show)

    cr_label = runs_sub.add_parser("label", help="Operator tags in the operations index")
    label_sub = cr_label.add_subparsers(dest="label_action", required=True)
    cr_label_set = label_sub.add_parser(
        "set",
        help="Set one label on a run row (tenant-scoped; run should already be indexed)",
    )
    cr_label_set.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    cr_label_set.add_argument("--repo-id", required=True, help="Repo identifier (required)")
    cr_label_set.add_argument("--run-id", required=True, help="Compile run identifier")
    cr_label_set.add_argument("--label-key", required=True, help="Label key (non-empty)")
    cr_label_set.add_argument("--label-value", required=True, help="Label value (non-empty)")
    cr_label_set.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root that contains <tenant>/.akc/control/operations.sqlite",
    )
    cr_label_set.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    cr_label_set.set_defaults(func=cmd_control_runs_label_set)

    control_index = control_sub.add_parser("index", help="Maintain control-plane indexes")
    index_sub = control_index.add_subparsers(dest="index_action", required=True)
    ci_rebuild = index_sub.add_parser(
        "rebuild",
        help="Rebuild operations.sqlite by scanning <tenant>/*/.akc/run/*.manifest.json",
    )
    ci_rebuild.add_argument("--tenant-id", required=True, help="Tenant identifier (required)")
    ci_rebuild.add_argument(
        "--outputs-root",
        required=True,
        help="Outputs root to scan for tenant-scoped manifests",
    )
    ci_rebuild.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="Output format (default: text)",
    )
    ci_rebuild.set_defaults(func=cmd_control_index_rebuild)

    control_manifest = control_sub.add_parser("manifest", help="Compare run manifests (read-only)")
    manifest_sub = control_manifest.add_subparsers(dest="manifest_action", required=True)
    cm_diff = manifest_sub.add_parser(
        "diff",
        help="Diff two manifests (paths or run ids): intent hash, control_plane refs, passes, partial replay",
    )
    cm_diff.add_argument("--manifest-a", help="Path to first *.manifest.json")
    cm_diff.add_argument("--manifest-b", help="Path to second *.manifest.json")
    cm_diff.add_argument("--run-id-a", help="Run id when resolving via operations index layout")
    cm_diff.add_argument("--run-id-b", help="Run id when resolving via operations index layout")
    cm_diff.add_argument(
        "--outputs-root",
        help="Outputs root (required with --run-id-a/b): <root>/<tenant>/<repo>/.akc/run/…",
    )
    cm_diff.add_argument("--tenant-id", help="Tenant id (required with --run-id-a/b)")
    cm_diff.add_argument("--repo-id", help="Repo id (required with --run-id-a/b)")
    cm_diff.add_argument(
        "--evaluation-modes",
        help="Comma-separated evaluation modes to compute mandatory partial-replay passes (optional)",
    )
    cm_diff.add_argument(
        "--format",
        choices=["json", "text"],
        default="json",
        help="Output format (default: json)",
    )
    cm_diff.set_defaults(func=cmd_control_manifest_diff)

    control_replay = control_sub.add_parser("replay", help="Replay forensics (read-only)")
    replay_sub = control_replay.add_subparsers(dest="replay_action", required=True)
    cr_forensics = replay_sub.add_parser(
        "forensics",
        help="Summarize replay_decisions.json (pass triggers, inputs snapshots)",
    )
    cr_forensics.add_argument(
        "--replay-decisions",
        type=Path,
        help="Path to replay_decisions.json (alternative to run manifest resolution)",
    )
    cr_forensics.add_argument("--outputs-root", help="Outputs root (with --run-id)")
    cr_forensics.add_argument("--tenant-id", help="Tenant id (with --run-id)")
    cr_forensics.add_argument("--repo-id", help="Repo id (with --run-id)")
    cr_forensics.add_argument("--run-id", help="Run id to locate manifest and replay_decisions ref")
    cr_forensics.add_argument(
        "--format",
        choices=["markdown", "json"],
        default="markdown",
        help="Output format (default: markdown)",
    )
    cr_forensics.set_defaults(func=cmd_control_replay_forensics)

    cr_plan = replay_sub.add_parser(
        "plan",
        help="Effective partial replay pass set + suggested akc compile flags (JSON; no auto-execution)",
    )
    cr_plan.add_argument(
        "--manifest",
        type=Path,
        help="Path to run manifest (infers scope); alternative to run-id quadruple",
    )
    cr_plan.add_argument("--outputs-root", help="Outputs root (with --tenant-id, --repo-id, --run-id)")
    cr_plan.add_argument("--tenant-id", help="Tenant id (with --run-id)")
    cr_plan.add_argument("--repo-id", help="Repo id (with --run-id)")
    cr_plan.add_argument("--run-id", help="Run id under .akc/run/*.manifest.json")
    cr_plan.add_argument(
        "--evaluation-modes",
        help="Comma-separated evaluation modes for mandatory partial-replay union (optional; else manifest modes)",
    )
    cr_plan.add_argument(
        "--out",
        type=Path,
        help="Write replay_plan.json to this path (default: stdout only)",
    )
    cr_plan.add_argument(
        "--format",
        choices=["json", "text"],
        default="json",
        help="stdout format (default: json)",
    )
    cr_plan.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default=None,
        help=(
            "Optional override for suggested compile argv and replay_plan manifest profile fields "
            "(default: use manifest control_plane.developer_role_profile when present)."
        ),
    )
    cr_plan.set_defaults(func=cmd_control_replay_plan)

    control_incident = control_sub.add_parser("incident", help="Slim incident export bundles (read-only)")
    incident_sub = control_incident.add_subparsers(dest="incident_action", required=True)
    ci_export = incident_sub.add_parser(
        "export",
        help="Copy manifest, replay_decisions, costs, runtime evidence, knowledge snapshot into a small bundle",
    )
    ci_export.add_argument(
        "--manifest",
        type=Path,
        help="Path to run manifest (infers outputs_root and scope); alternative to run-id quadruple",
    )
    ci_export.add_argument("--outputs-root", help="Outputs root (with --tenant-id, --repo-id, --run-id)")
    ci_export.add_argument("--tenant-id", help="Tenant id (with --run-id)")
    ci_export.add_argument("--repo-id", help="Repo id (with --run-id)")
    ci_export.add_argument("--run-id", help="Run id under .akc/run/*.manifest.json")
    ci_export.add_argument(
        "--out-dir",
        help="Bundle directory (default: <scope>/.akc/viewer/incident/<timestamp>)",
    )
    ci_export.add_argument(
        "--no-zip",
        action="store_true",
        help="Do not write a sibling .zip of the bundle directory",
    )
    ci_export.add_argument(
        "--max-file-mb",
        type=int,
        default=8,
        help="Skip copying individual artifacts larger than this many MiB (default: 8)",
    )
    ci_export.add_argument(
        "--include-runtime-bundle-pointer",
        action="store_true",
        help="Include runtime_bundle path+sha in SUMMARY.json (does not copy the bundle bytes)",
    )
    ci_export.add_argument(
        "--signer-identity",
        help="Optional signer identity to include in export metadata signature block",
    )
    ci_export.add_argument(
        "--signature",
        help="Optional detached signature string to include in export metadata signature block",
    )
    ci_export.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="stdout format (default: text)",
    )
    ci_export.set_defaults(func=cmd_control_incident_export)

    control_forensics = control_sub.add_parser(
        "forensics",
        help="Cross-signal forensics bundle (replay, coordination, OTel, knowledge, operations index)",
    )
    forensics_sub = control_forensics.add_subparsers(dest="forensics_action", required=True)
    cf_export = forensics_sub.add_parser(
        "export",
        help="Write FORENSICS.json + data/ copies under a single folder (and optional .zip)",
    )
    cf_export.add_argument(
        "--manifest",
        type=Path,
        help="Path to run manifest (infers outputs_root and scope); alternative to run-id quadruple",
    )
    cf_export.add_argument("--outputs-root", help="Outputs root (with --tenant-id, --repo-id, --run-id)")
    cf_export.add_argument("--tenant-id", help="Tenant id (with --run-id)")
    cf_export.add_argument("--repo-id", help="Repo id (with --run-id)")
    cf_export.add_argument("--run-id", help="Run id under .akc/run/*.manifest.json")
    cf_export.add_argument(
        "--out-dir",
        help="Bundle directory (default: <scope>/.akc/viewer/forensics/<timestamp>)",
    )
    cf_export.add_argument(
        "--no-zip",
        action="store_true",
        help="Do not write a sibling .zip of the bundle directory",
    )
    cf_export.add_argument(
        "--max-file-mb",
        type=int,
        default=8,
        help="Skip copying individual artifacts larger than this many MiB (default: 8)",
    )
    cf_export.add_argument(
        "--coordination-audit-tail-lines",
        type=int,
        default=500,
        help="Max lines to copy from the tail of coordination_audit JSONL (default: 500)",
    )
    cf_export.add_argument(
        "--coordination-audit-max-scan-mb",
        type=int,
        default=4,
        help="Max bytes to scan from EOF when tailing coordination_audit (default: 4)",
    )
    cf_export.add_argument(
        "--signer-identity",
        help="Optional signer identity to include in export metadata signature block",
    )
    cf_export.add_argument(
        "--signature",
        help="Optional detached signature string to include in export metadata signature block",
    )
    cf_export.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="stdout format (default: text)",
    )
    cf_export.set_defaults(func=cmd_control_forensics_export)

    control_playbook = control_sub.add_parser(
        "playbook",
        help="Governance playbook: one JSON report linking manifest diff, replay forensics, incident bundle",
    )
    playbook_sub = control_playbook.add_subparsers(dest="playbook_action", required=True)
    cp_run = playbook_sub.add_parser(
        "run",
        help="Run read-only steps for two run ids; writes .akc/control/playbooks/<timestamp>.json",
    )
    cp_run.add_argument("--outputs-root", required=True, help="Outputs root (<root>/<tenant>/<repo>/...)")
    cp_run.add_argument("--tenant-id", required=True, help="Tenant identifier")
    cp_run.add_argument("--repo-id", required=True, help="Repo identifier")
    cp_run.add_argument("--run-id-a", required=True, help="First run id (manifest diff label a)")
    cp_run.add_argument("--run-id-b", required=True, help="Second run id (manifest diff label b)")
    cp_run.add_argument(
        "--focus-run",
        choices=["a", "b"],
        default="b",
        help="Run used for replay forensics + incident export (default: b)",
    )
    cp_run.add_argument(
        "--evaluation-modes",
        help="Comma-separated modes for mandatory partial-replay union in manifest diff (optional)",
    )
    cp_run.add_argument(
        "--shard-id",
        action="append",
        default=[],
        help="Optional shard identifier (repeatable); recorded in report inputs only",
    )
    cp_run.add_argument(
        "--with-policy-explain",
        action="store_true",
        help="Include policy provenance / decision counts for the focus run (no Rego or decision bodies)",
    )
    cp_run.add_argument(
        "--no-operational-coupling",
        action="store_true",
        help="Skip default operational coupling verification step in playbook run",
    )
    cp_run.add_argument(
        "--max-file-mb",
        type=int,
        default=8,
        help="Per-file cap when copying incident bundle artifacts (default: 8)",
    )
    cp_run.add_argument(
        "--include-runtime-bundle-pointer",
        action="store_true",
        help="Include runtime_bundle path+sha in incident SUMMARY.json (does not copy bundle bytes)",
    )
    cp_run.add_argument(
        "--timestamp-utc",
        default=None,
        help="UTC timestamp basename for playbooks/ files (default: now, format %%Y%%m%%dT%%H%%M%%SZ)",
    )
    cp_run.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="stdout format (default: text)",
    )
    cp_run.add_argument(
        "--webhook-url",
        default=None,
        help="POST operator_playbook_completed to this URL (requires --webhook-secret)",
    )
    cp_run.add_argument(
        "--webhook-secret",
        default=None,
        help="HMAC secret for --webhook-url (same signing as fleet webhooks)",
    )
    cp_run.add_argument(
        "--webhook-id",
        default="playbook-cli",
        help="X-AKC-Webhook-Id header for --webhook-url (default: playbook-cli)",
    )
    cp_run.add_argument(
        "--fleet-config",
        type=Path,
        default=None,
        help="Fleet JSON config: notify each webhook subscribed to operator_playbook_completed",
    )
    cp_run.add_argument(
        "--webhook-dry-run",
        action="store_true",
        help="Skip HTTP for webhook delivery (still writes report + audit)",
    )
    cp_run.set_defaults(func=cmd_control_playbook_run)

    control_policy_bundle = control_sub.add_parser(
        "policy-bundle",
        help="Tenant/repo policy bundle (.akc/control/policy_bundle.json; lifecycle metadata, not Rego execution)",
    )
    pb_sub = control_policy_bundle.add_subparsers(dest="policy_bundle_action", required=True)

    pb_validate = pb_sub.add_parser("validate", help="Validate policy_bundle.json against the frozen schema")
    pb_validate.add_argument("--path", type=Path, help="Explicit path to policy_bundle.json")
    pb_validate.add_argument(
        "--outputs-root",
        help="With --tenant-id and --repo-id, targets <root>/<tenant>/<repo>/.akc/control/policy_bundle.json",
    )
    pb_validate.add_argument("--tenant-id", help="Tenant id (with --outputs-root and --repo-id)")
    pb_validate.add_argument("--repo-id", help="Repo id (with --outputs-root and --repo-id)")
    pb_validate.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="stdout format (default: text)",
    )
    pb_validate.set_defaults(func=cmd_control_policy_bundle_validate)

    pb_show = pb_sub.add_parser("show", help="Print policy bundle JSON, fingerprint, and validation status")
    pb_show.add_argument("--path", type=Path, help="Explicit path to policy_bundle.json")
    pb_show.add_argument("--outputs-root", help="With --tenant-id and --repo-id resolves default bundle path")
    pb_show.add_argument("--tenant-id", help="Tenant id (with --outputs-root and --repo-id)")
    pb_show.add_argument("--repo-id", help="Repo id (with --outputs-root and --repo-id)")
    pb_show.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="stdout format (default: text)",
    )
    pb_show.set_defaults(func=cmd_control_policy_bundle_show)

    pb_effective = pb_sub.add_parser(
        "effective-profile",
        help="Resolve the effective tenant/repo governance profile from policy_bundle.json",
    )
    pb_effective.add_argument("--outputs-root", required=True, help="Outputs root for tenant/repo scope")
    pb_effective.add_argument("--tenant-id", required=True, help="Tenant id")
    pb_effective.add_argument("--repo-id", required=True, help="Repo id")
    pb_effective.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="stdout format (default: text)",
    )
    pb_effective.set_defaults(func=cmd_control_policy_bundle_effective_profile)

    pb_write = pb_sub.add_parser(
        "write",
        help="Write a validated bundle from JSON file; updates operations index and appends control audit",
    )
    pb_write.add_argument("--from-file", type=Path, required=True, help="Source JSON (validated before write)")
    pb_write.add_argument("--outputs-root", required=True, help="Outputs root for tenant/repo scope")
    pb_write.add_argument("--tenant-id", required=True, help="Tenant identifier")
    pb_write.add_argument("--repo-id", required=True, help="Repo identifier")
    pb_write.add_argument(
        "--no-audit",
        action="store_true",
        help="Do not append control_audit.jsonl (manual edits are not audited)",
    )
    pb_write.add_argument(
        "--audit-actor",
        default=None,
        help="Actor for audit row (default: $USER or AKC_AUDIT_ACTOR)",
    )
    pb_write.add_argument(
        "--format",
        choices=["text", "json"],
        default="text",
        help="stdout format (default: text)",
    )
    pb_write.set_defaults(func=cmd_control_policy_bundle_write)

    control_bot = sub.add_parser(
        "control-bot",
        help="Dedicated multi-channel operator gateway service (standalone; not fleet HTTP)",
    )
    cb_sub = control_bot.add_subparsers(dest="control_bot_command", required=True)

    cb_validate = cb_sub.add_parser("validate-config", help="Validate control-bot config file (schema + typed checks)")
    cb_validate.add_argument("--config", required=True, help="Path to control-bot config JSON")
    cb_validate.add_argument("--print-json", action="store_true", help="Print normalized JSON after validation")
    cb_validate.add_argument("--verbose", action="store_true", help="Enable debug logging")
    cb_validate.set_defaults(func=cmd_control_bot_validate_config)

    cb_serve = cb_sub.add_parser("serve", help="Run the control-bot gateway HTTP service")
    cb_serve.add_argument("--config", required=True, help="Path to control-bot config JSON")
    cb_serve.add_argument("--bind", default=None, help="Override server.bind from config")
    cb_serve.add_argument("--port", type=int, default=None, help="Override server.port from config")
    cb_serve.add_argument("--verbose", action="store_true", help="Enable debug logging")
    cb_serve.set_defaults(func=cmd_control_bot_serve)

    from akc.cli.deliver import register_deliver_parsers
    from akc.cli.fleet import register_fleet_parsers

    register_deliver_parsers(sub)
    register_fleet_parsers(sub)

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
    view_web.add_argument(
        "--serve",
        action="store_true",
        help=(
            "After writing the bundle, serve it over HTTP on 127.0.0.1 only (developer "
            "convenience; not for remote exposure)"
        ),
    )
    view_web.add_argument(
        "--port",
        type=int,
        default=None,
        dest="serve_port",
        metavar="PORT",
        help="TCP port when using --serve (default: ephemeral port)",
    )
    view_web.set_defaults(func=cmd_view)

    view_export = view_sub.add_parser("export", help="Export a portable evidence bundle (dir + zip)")
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
