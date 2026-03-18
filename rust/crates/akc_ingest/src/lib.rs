//! `akc_ingest` provides high-throughput ingestion + normalization primitives.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::RunId;
use akc_protocol::{
    validate_input_path, ChunkRecord, IngestDocsRequest, IngestKind, IngestRequest, IngestResponse,
};
use serde_json::json;
use sha2::{Digest, Sha256};

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("protocol validation error: {0}")]
    Protocol(#[from] akc_protocol::ProtocolError),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported ingest kind")]
    UnsupportedKind,
}

/// Ingest a request.
pub fn ingest(request: IngestRequest) -> Result<IngestResponse, IngestError> {
    let tenant_id: &akc_protocol::TenantId = &request.tenant_id;
    let run_id: &RunId = &request.run_id;

    let kind_label: &str = match &request.kind {
        None => "none",
        Some(IngestKind::Docs(_)) => "docs",
        Some(IngestKind::Messaging(_)) => "messaging",
        Some(IngestKind::Api(_)) => "api",
    };

    log_event(
        LogLevel::Info,
        "ingest_start",
        tenant_id,
        run_id,
        json!({
            "kind": kind_label,
            "surface": std::env::var("AKC_INGEST_SURFACE").unwrap_or_else(|_| "unknown".to_string()),
        }),
    );

    let mut response = IngestResponse {
        tenant_id: request.tenant_id,
        run_id: request.run_id,
        ok: false,
        records: Vec::new(),
        error: None,
    };

    match request.kind {
        None => {
            // Compatibility with the early v1 stub: no work performed, but ok=true.
            response.ok = true;
            log_event(
                LogLevel::Info,
                "ingest_complete",
                &response.tenant_id,
                &response.run_id,
                json!({
                    "ok": true,
                    "records": 0,
                }),
            );
            Ok(response)
        }
        Some(IngestKind::Docs(docs)) => {
            let records = ingest_docs(&response.tenant_id, &response.run_id, docs)?;
            response.records = records;
            response.ok = true;
            log_event(
                LogLevel::Info,
                "ingest_complete",
                &response.tenant_id,
                &response.run_id,
                json!({
                    "ok": true,
                    "records": response.records.len(),
                }),
            );
            Ok(response)
        }
        Some(IngestKind::Messaging(_)) | Some(IngestKind::Api(_)) => {
            response.error = Some("unsupported ingest kind in v1: only docs is implemented".into());
            log_event(
                LogLevel::Warn,
                "ingest_error",
                &response.tenant_id,
                &response.run_id,
                json!({
                    "error_kind": "unsupported_ingest_kind",
                }),
            );
            Err(IngestError::UnsupportedKind)
        }
    }
}

fn ingest_docs(
    tenant_id: &akc_protocol::TenantId,
    run_id: &RunId,
    docs: IngestDocsRequest,
) -> Result<Vec<ChunkRecord>, IngestError> {
    if docs.input_paths.is_empty() {
        log_event(
            LogLevel::Error,
            "ingest_error",
            tenant_id,
            run_id,
            json!({
                "error_kind": "missing_input_paths",
            }),
        );
        return Err(akc_protocol::ProtocolError::MissingIngestDocsInputPaths.into());
    }

    for p in &docs.input_paths {
        validate_input_path(p)?;
    }

    let max_chunk_chars: usize = docs.max_chunk_chars.unwrap_or(2_000);
    let input_paths_count: usize = docs.input_paths.len();

    let mut files: Vec<PathBuf> = Vec::new();
    for raw in docs.input_paths {
        let path: PathBuf = PathBuf::from(raw);
        collect_files(&path, &mut files)?;
    }

    // Deterministic ordering: sort by UTF-8 path string.
    files.sort_by(|a, b| a.to_string_lossy().cmp(&b.to_string_lossy()));
    let discovered_files: usize = files.len();

    let mut out: Vec<ChunkRecord> = Vec::new();
    for path in files {
        let bytes: Vec<u8> = fs::read(&path)?;
        let text: String = String::from_utf8_lossy(&bytes).to_string();

        let source_id: String = compute_source_id(&path, docs.source_root.as_deref());
        let chunks: Vec<String> = chunk_text(&text, max_chunk_chars);

        for (idx, chunk) in chunks.into_iter().enumerate() {
            let fingerprint: String = sha256_hex(chunk.as_bytes());
            let chunk_id_seed: String = format!("{source_id}:{idx}:{fingerprint}");
            let chunk_id: String = sha256_hex(chunk_id_seed.as_bytes());

            let mut metadata: BTreeMap<String, serde_json::Value> = BTreeMap::new();
            metadata.insert(
                "path".to_string(),
                serde_json::Value::String(path.to_string_lossy().to_string()),
            );
            metadata.insert(
                "chunk_index".to_string(),
                serde_json::Value::Number(serde_json::Number::from(idx as u64)),
            );

            out.push(ChunkRecord {
                tenant_id: tenant_id.clone(),
                source_id: source_id.clone(),
                chunk_id,
                content: chunk,
                metadata,
                fingerprint,
            });
        }
    }

    log_event(
        LogLevel::Info,
        "ingest_docs_complete",
        tenant_id,
        run_id,
        json!({
            "input_paths": input_paths_count,
            "discovered_files": discovered_files,
            "records": out.len(),
            "max_chunk_chars": max_chunk_chars,
        }),
    );

    Ok(out)
}

fn collect_files(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    let md: fs::Metadata = fs::metadata(path)?;
    if md.is_file() {
        if looks_like_supported_doc(path) {
            out.push(path.to_path_buf());
        }
        return Ok(());
    }

    if md.is_dir() {
        let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        // Deterministic traversal: sort by filename.
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            collect_files(&entry.path(), out)?;
        }
    }
    Ok(())
}

fn looks_like_supported_doc(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "md" | "markdown" | "txt" | "html" | "htm"
    )
}

fn compute_source_id(path: &Path, source_root: Option<&str>) -> String {
    if let Some(root_s) = source_root {
        let root: PathBuf = PathBuf::from(root_s);
        if let Ok(rel) = path.strip_prefix(&root) {
            return rel.to_string_lossy().to_string();
        }
    }
    path.to_string_lossy().to_string()
}

fn chunk_text(input: &str, max_chars: usize) -> Vec<String> {
    if max_chars == 0 {
        return vec![input.to_string()];
    }

    // Simple, deterministic chunking:
    // - split on blank lines first (paragraph-ish)
    // - then pack into chunks up to max_chars
    let paras: Vec<String> = input
        .split("\n\n")
        .map(|p| p.trim())
        .filter(|p| !p.is_empty())
        .map(|p| p.to_string())
        .collect();

    let mut out: Vec<String> = Vec::new();
    let mut buf = String::new();
    for para in paras {
        if buf.is_empty() {
            buf.push_str(&para);
            continue;
        }
        if buf.len() + 2 + para.len() <= max_chars {
            buf.push_str("\n\n");
            buf.push_str(&para);
            continue;
        }
        out.push(buf);
        buf = para;
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    let digest = h.finalize();
    hex::encode(digest)
}
