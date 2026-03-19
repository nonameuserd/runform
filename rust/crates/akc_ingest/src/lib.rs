//! `akc_ingest` provides high-throughput ingestion + normalization primitives.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::RunId;
use akc_protocol::{
    validate_input_path, ChunkRecord, IngestApiRequest, IngestDocsRequest, IngestKind,
    IngestMessagingRequest, IngestRequest, IngestResponse,
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
        Some(IngestKind::Messaging(messaging)) => {
            let records = ingest_messaging(&response.tenant_id, &response.run_id, messaging)?;
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
        Some(IngestKind::Api(api)) => {
            let records = ingest_api(&response.tenant_id, &response.run_id, api)?;
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

fn looks_like_supported_doc(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "md" | "markdown" | "txt" | "html" | "htm"
    )
}

fn looks_like_supported_messaging(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "jsonl" | "json" | "txt" | "md"
    )
}

fn looks_like_supported_openapi(path: &Path) -> bool {
    let Some(ext) = path.extension().and_then(|e| e.to_str()) else {
        return false;
    };
    matches!(
        ext.to_ascii_lowercase().as_str(),
        "json" | "yaml" | "yml" | "txt" | "md"
    )
}

fn collect_files_with_filter<F>(
    path: &Path,
    out: &mut Vec<PathBuf>,
    filter: &F,
) -> Result<(), std::io::Error>
where
    F: Fn(&Path) -> bool,
{
    let md: fs::Metadata = fs::metadata(path)?;
    if md.is_file() {
        if filter(path) {
            out.push(path.to_path_buf());
        }
        return Ok(());
    }

    if md.is_dir() {
        let mut entries: Vec<fs::DirEntry> = fs::read_dir(path)?.collect::<Result<Vec<_>, _>>()?;
        // Deterministic traversal: sort by filename.
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            collect_files_with_filter(&entry.path(), out, filter)?;
        }
    }
    Ok(())
}

fn collect_files(path: &Path, out: &mut Vec<PathBuf>) -> Result<(), std::io::Error> {
    collect_files_with_filter(path, out, &looks_like_supported_doc)
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

fn ingest_from_root_with_filter<F>(
    tenant_id: &akc_protocol::TenantId,
    run_id: &RunId,
    root_raw: &str,
    filter: F,
    kind_log_label: &str,
) -> Result<Vec<ChunkRecord>, IngestError>
where
    F: Fn(&Path) -> bool,
{
    validate_input_path(root_raw)?;

    let root_path: PathBuf = PathBuf::from(root_raw);
    let root_meta = fs::metadata(&root_path)?;
    let source_root: Option<String> = if root_meta.is_dir() {
        Some(root_raw.to_string())
    } else {
        None
    };

    let mut files: Vec<PathBuf> = Vec::new();
    collect_files_with_filter(&root_path, &mut files, &filter)?;

    let max_chunk_chars: usize = 2_000;
    let input_files_count: usize = files.len();

    // Deterministic ordering: sort by computed source_id.
    let mut files_with_source: Vec<(String, PathBuf)> = files
        .into_iter()
        .map(|p| {
            let source_id = compute_source_id(&p, source_root.as_deref());
            (source_id, p)
        })
        .collect();
    files_with_source.sort_by(|a, b| a.0.cmp(&b.0));

    let mut out: Vec<ChunkRecord> = Vec::new();
    for (source_id, path) in files_with_source {
        let bytes: Vec<u8> = fs::read(&path)?;
        let text: String = String::from_utf8_lossy(&bytes).to_string();
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
        &format!("ingest_{kind_log_label}_complete"),
        tenant_id,
        run_id,
        json!({
            "input_root": root_raw,
            "input_files": input_files_count,
            "records": out.len(),
            "max_chunk_chars": max_chunk_chars,
        }),
    );

    Ok(out)
}

fn ingest_messaging(
    tenant_id: &akc_protocol::TenantId,
    run_id: &RunId,
    messaging: IngestMessagingRequest,
) -> Result<Vec<ChunkRecord>, IngestError> {
    let export_path = messaging
        .export_path
        .as_deref()
        .ok_or(akc_protocol::ProtocolError::InvalidInputPath)?;

    ingest_from_root_with_filter(
        tenant_id,
        run_id,
        export_path,
        looks_like_supported_messaging,
        "messaging",
    )
}

fn ingest_api(
    tenant_id: &akc_protocol::TenantId,
    run_id: &RunId,
    api: IngestApiRequest,
) -> Result<Vec<ChunkRecord>, IngestError> {
    let openapi_path = api
        .openapi_path
        .as_deref()
        .ok_or(akc_protocol::ProtocolError::InvalidInputPath)?;

    ingest_from_root_with_filter(
        tenant_id,
        run_id,
        openapi_path,
        looks_like_supported_openapi,
        "api",
    )
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

#[cfg(test)]
mod tests {
    use super::*;
    use akc_protocol::{
        IngestApiRequest, IngestDocsRequest, IngestKind, IngestMessagingRequest, IngestRequest,
        RunId, TenantId,
    };
    use std::fs;
    use std::path::PathBuf;

    fn mk_file(path: &Path, contents: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, contents.as_bytes()).unwrap();
    }

    fn ingest_docs_tree(root: &Path, max_chunk_chars: Option<usize>) -> Vec<ChunkRecord> {
        let req: IngestRequest = IngestRequest {
            tenant_id: TenantId::parse("tenant_a").unwrap(),
            run_id: RunId::parse("run_1").unwrap(),
            kind: Some(IngestKind::Docs(IngestDocsRequest {
                input_paths: vec![root.to_string_lossy().to_string()],
                max_chunk_chars,
                source_root: Some(root.to_string_lossy().to_string()),
            })),
        };
        ingest(req).unwrap().records
    }

    #[test]
    fn docs_ingest_is_deterministic_traversal_and_ordering() {
        let td = tempfile::tempdir().unwrap();
        let root: &Path = td.path();

        // Intentionally "unsorted" names and nesting to ensure traversal sorting is enforced.
        mk_file(&root.join("b.md"), "B1\n\nB2\n");
        mk_file(&root.join("a.md"), "A1\n");
        mk_file(&root.join("sub/z.txt"), "Z1\n");
        mk_file(&root.join("sub/a.txt"), "A_sub\n");
        mk_file(&root.join("sub/deeper/m.html"), "<p>m</p>\n");

        let r1: Vec<ChunkRecord> = ingest_docs_tree(root, Some(50));
        let r2: Vec<ChunkRecord> = ingest_docs_tree(root, Some(50));
        assert_eq!(r1, r2, "docs ingest must be reproducible for same inputs");

        let source_ids: Vec<String> = r1.iter().map(|r| r.source_id.clone()).collect();
        let mut sorted: Vec<String> = source_ids.clone();
        sorted.sort();
        assert_eq!(
            source_ids, sorted,
            "records must be emitted in stable, sorted source_id order"
        );

        // Ensure per-source chunk indices are monotonic and start at 0.
        let mut per_source_next: BTreeMap<String, u64> = BTreeMap::new();
        for rec in &r1 {
            let idx_v = rec
                .metadata
                .get("chunk_index")
                .and_then(|v| v.as_u64())
                .expect("chunk_index metadata must be present and u64");
            let next: &mut u64 = per_source_next.entry(rec.source_id.clone()).or_insert(0);
            assert_eq!(idx_v, *next, "chunk_index must be sequential per source");
            *next += 1;
        }
    }

    #[test]
    fn docs_ingest_respects_max_chunk_chars_and_has_stable_ids() {
        let td = tempfile::tempdir().unwrap();
        let root: &Path = td.path();

        let content = [
            "para-one-aaaaa",
            "para-two-bbbbb",
            "para-three-ccccc",
            "para-four-ddddd",
        ]
        .join("\n\n");
        mk_file(&root.join("doc.md"), &content);

        let records: Vec<ChunkRecord> = ingest_docs_tree(root, Some(25));
        assert!(!records.is_empty());
        for rec in &records {
            assert!(
                rec.content.len() <= 25,
                "chunk must not exceed max_chunk_chars"
            );
            assert_eq!(
                rec.fingerprint,
                sha256_hex(rec.content.as_bytes()),
                "fingerprint must be sha256_hex(content)"
            );
        }

        // Validate chunk_id derivation for the first record.
        let first: &ChunkRecord = &records[0];
        let idx: usize = first
            .metadata
            .get("chunk_index")
            .and_then(|v| v.as_u64())
            .unwrap() as usize;
        let seed: String = format!("{}:{}:{}", first.source_id, idx, first.fingerprint);
        assert_eq!(
            first.chunk_id,
            sha256_hex(seed.as_bytes()),
            "chunk_id must be sha256_hex(source_id:idx:fingerprint)"
        );

        // Ensure `path` metadata is present and stable.
        let p: &str = first
            .metadata
            .get("path")
            .and_then(|v| v.as_str())
            .expect("path metadata must be present");
        assert!(
            PathBuf::from(p).is_absolute(),
            "path metadata must be absolute"
        );
    }

    fn ingest_messaging_tree(root: &Path) -> Vec<ChunkRecord> {
        let req: IngestRequest = IngestRequest {
            tenant_id: TenantId::parse("tenant_a").unwrap(),
            run_id: RunId::parse("run_1").unwrap(),
            kind: Some(IngestKind::Messaging(IngestMessagingRequest {
                export_path: Some(root.to_string_lossy().to_string()),
            })),
        };
        ingest(req).unwrap().records
    }

    fn ingest_api_tree(root: &Path) -> Vec<ChunkRecord> {
        let req: IngestRequest = IngestRequest {
            tenant_id: TenantId::parse("tenant_a").unwrap(),
            run_id: RunId::parse("run_1").unwrap(),
            kind: Some(IngestKind::Api(IngestApiRequest {
                openapi_path: Some(root.to_string_lossy().to_string()),
            })),
        };
        ingest(req).unwrap().records
    }

    #[test]
    fn messaging_ingest_is_deterministic_and_has_stable_ids() {
        let td = tempfile::tempdir().unwrap();
        let root: &Path = td.path();

        let para = "a".repeat(1000);
        let content = [para.clone(), para.clone(), para.clone()].join("\n\n");

        // Intentionally "unsorted" names and nested nesting to ensure traversal sorting is enforced.
        mk_file(&root.join("b.jsonl"), &content);
        mk_file(&root.join("a.jsonl"), &content);
        mk_file(&root.join("sub/z.txt"), &content);

        let r1: Vec<ChunkRecord> = ingest_messaging_tree(root);
        let r2: Vec<ChunkRecord> = ingest_messaging_tree(root);
        assert_eq!(
            r1, r2,
            "messaging ingest must be reproducible for same inputs"
        );

        let source_ids: Vec<String> = r1.iter().map(|r| r.source_id.clone()).collect();
        let mut sorted: Vec<String> = source_ids.clone();
        sorted.sort();
        assert_eq!(
            source_ids, sorted,
            "records must be emitted in stable, sorted source_id order"
        );

        // Ensure per-source chunk indices are monotonic and start at 0.
        let mut per_source_next: BTreeMap<String, u64> = BTreeMap::new();
        for rec in &r1 {
            let idx_v = rec
                .metadata
                .get("chunk_index")
                .and_then(|v| v.as_u64())
                .expect("chunk_index metadata must be present and u64");
            let next: &mut u64 = per_source_next.entry(rec.source_id.clone()).or_insert(0);
            assert_eq!(idx_v, *next, "chunk_index must be sequential per source");
            *next += 1;
        }

        // Validate chunk_id derivation for the first record.
        let first: &ChunkRecord = &r1[0];
        let idx: usize = first
            .metadata
            .get("chunk_index")
            .and_then(|v| v.as_u64())
            .unwrap() as usize;
        let seed: String = format!("{}:{}:{}", first.source_id, idx, first.fingerprint);
        assert_eq!(
            first.chunk_id,
            sha256_hex(seed.as_bytes()),
            "chunk_id must be sha256_hex(source_id:idx:fingerprint)"
        );
        assert_eq!(
            first.fingerprint,
            sha256_hex(first.content.as_bytes()),
            "fingerprint must be sha256_hex(content)"
        );
    }

    #[test]
    fn api_ingest_is_deterministic_and_has_stable_ids() {
        let td = tempfile::tempdir().unwrap();
        let root: &Path = td.path();

        let para = "b".repeat(1000);
        let content = [para.clone(), para.clone(), para.clone()].join("\n\n");

        mk_file(&root.join("b.json"), &content);
        mk_file(&root.join("a.json"), &content);

        let r1: Vec<ChunkRecord> = ingest_api_tree(root);
        let r2: Vec<ChunkRecord> = ingest_api_tree(root);
        assert_eq!(r1, r2, "api ingest must be reproducible for same inputs");

        // At least one file discovered and chunked.
        assert!(!r1.is_empty());

        let source_ids: Vec<String> = r1.iter().map(|r| r.source_id.clone()).collect();
        let mut sorted: Vec<String> = source_ids.clone();
        sorted.sort();
        assert_eq!(
            source_ids, sorted,
            "records must be emitted in stable, sorted source_id order"
        );

        let first: &ChunkRecord = &r1[0];
        let idx: usize = first
            .metadata
            .get("chunk_index")
            .and_then(|v| v.as_u64())
            .unwrap() as usize;
        let seed: String = format!("{}:{}:{}", first.source_id, idx, first.fingerprint);
        assert_eq!(
            first.chunk_id,
            sha256_hex(seed.as_bytes()),
            "chunk_id must be sha256_hex(source_id:idx:fingerprint)"
        );
        assert_eq!(
            first.fingerprint,
            sha256_hex(first.content.as_bytes()),
            "fingerprint must be sha256_hex(content)"
        );
    }
}
