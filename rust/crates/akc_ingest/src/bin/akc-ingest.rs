use std::io::{self, Read, Write};

use akc_ingest::ingest;
use akc_protocol::observability::{log_event, log_event_unscoped, LogLevel};
use akc_protocol::{IngestDocsRequest, IngestKind, IngestRequest, RunId, TenantId};
use clap::{Parser, Subcommand};
use serde_json::json;

fn ingest_error_kind(err: &akc_ingest::IngestError) -> &'static str {
    match err {
        akc_ingest::IngestError::Protocol(_) => "protocol_error",
        akc_ingest::IngestError::Io(_) => "io_error",
        akc_ingest::IngestError::UnsupportedKind => "unsupported_kind",
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "akc-ingest",
    about = "AKC fast ingestion CLI (JSON over stdin/stdout or subcommands)"
)]
struct Cli {
    /// Emit records as newline-delimited JSON (one `ChunkRecord` per line).
    #[arg(long)]
    jsonl: bool,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Ingest documentation trees (markdown/html/txt).
    Docs {
        /// Tenant ID (required).
        #[arg(long)]
        tenant_id: String,
        /// Run ID (required).
        #[arg(long)]
        run_id: String,
        /// One or more input paths (files or directories).
        #[arg(required = true)]
        input_paths: Vec<String>,
        /// Max characters per chunk (default: 2000).
        #[arg(long)]
        max_chunk_chars: Option<usize>,
        /// Optional root used to compute stable relative `source_id`s.
        #[arg(long)]
        source_root: Option<String>,
    },
    /// Messaging ingestion (not yet implemented).
    Messaging {},
    /// OpenAPI ingestion (not yet implemented).
    Api {},
}

fn main() {
    let cli: Cli = Cli::parse();

    let request: IngestRequest = match cli.command {
        None => read_request_from_stdin_or_exit(),
        Some(Commands::Docs {
            tenant_id,
            run_id,
            input_paths,
            max_chunk_chars,
            source_root,
        }) => {
            let tenant_id: TenantId = match TenantId::parse(tenant_id) {
                Ok(v) => v,
                Err(e) => {
                    let _ = writeln!(io::stderr(), "{e}");
                    std::process::exit(10);
                }
            };
            let run_id: RunId = match RunId::parse(run_id) {
                Ok(v) => v,
                Err(e) => {
                    let _ = writeln!(io::stderr(), "{e}");
                    std::process::exit(10);
                }
            };
            IngestRequest {
                tenant_id,
                run_id,
                kind: Some(IngestKind::Docs(IngestDocsRequest {
                    input_paths,
                    max_chunk_chars,
                    source_root,
                })),
            }
        }
        Some(Commands::Messaging {}) | Some(Commands::Api {}) => {
            let _ = writeln!(
                io::stderr(),
                "unsupported subcommand in v1; only `docs` is implemented"
            );
            std::process::exit(30);
        }
    };

    let tenant_id = request.tenant_id.clone();
    let run_id = request.run_id.clone();

    let (kind_label, input_paths_count) = match &request.kind {
        None => ("none".to_string(), 0_usize),
        Some(IngestKind::Docs(docs)) => ("docs".to_string(), docs.input_paths.len()),
        Some(IngestKind::Messaging(_)) => ("messaging".to_string(), 0_usize),
        Some(IngestKind::Api(_)) => ("api".to_string(), 0_usize),
    };

    log_event(
        LogLevel::Info,
        "ingest_surface_start",
        &tenant_id,
        &run_id,
        json!({
            "surface": "cli",
            "kind": kind_label,
            "input_paths_count": input_paths_count,
        }),
    );

    std::env::set_var("AKC_INGEST_SURFACE", "cli");

    match ingest(request) {
        Ok(response) => {
            if cli.jsonl {
                for record in &response.records {
                    match serde_json::to_string(record) {
                        Ok(line) => {
                            if writeln!(io::stdout(), "{line}").is_err() {
                                std::process::exit(30);
                            }
                        }
                        Err(err) => {
                            let _ = writeln!(io::stderr(), "failed to serialize record: {err}");
                            std::process::exit(30);
                        }
                    }
                }
                std::process::exit(0);
            }

            match serde_json::to_string(&response) {
                Ok(json) => {
                    if let Err(err) = writeln!(io::stdout(), "{json}") {
                        let _ = writeln!(io::stderr(), "failed to write response: {err}");
                        std::process::exit(30);
                    }
                    log_event(
                        LogLevel::Info,
                        "ingest_surface_complete",
                        &response.tenant_id,
                        &response.run_id,
                        json!({
                            "surface": "cli",
                            "ok": response.ok,
                            "records": response.records.len(),
                        }),
                    );
                    std::process::exit(0);
                }
                Err(err) => {
                    let _ = writeln!(io::stderr(), "failed to serialize IngestResponse: {err}");
                    std::process::exit(30);
                }
            }
        }
        Err(err) => {
            let _ = writeln!(io::stderr(), "{err}");
            // Errors are meta-errors (validation/policy/IO); avoid printing payload-derived fields.
            log_event(
                LogLevel::Error,
                "ingest_surface_error",
                &tenant_id,
                &run_id,
                json!({
                    "surface": "cli",
                    "error_kind": ingest_error_kind(&err),
                    "exit_code": 30,
                }),
            );
            std::process::exit(30);
        }
    }
}

fn read_request_from_stdin_or_exit() -> IngestRequest {
    let mut input = String::new();
    if let Err(err) = io::stdin().read_to_string(&mut input) {
        let _ = writeln!(io::stderr(), "failed to read stdin: {err}");
        log_event_unscoped(
            LogLevel::Error,
            "ingest_request_stdin_read_failed",
            json!({ "exit_code": 30 }),
        );
        std::process::exit(30);
    }

    match serde_json::from_str(&input) {
        Ok(req) => req,
        Err(err) => {
            let _ = writeln!(io::stderr(), "invalid IngestRequest JSON: {err}");
            log_event_unscoped(
                LogLevel::Error,
                "ingest_request_invalid_json",
                json!({ "exit_code": 10 }),
            );
            std::process::exit(10);
        }
    }
}
