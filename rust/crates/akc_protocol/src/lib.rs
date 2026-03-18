//! Shared request/response types for AKC Rust components.
//!
//! This crate is intentionally small and dependency-light because it defines the stable
//! subprocess JSON boundary and (later) PyO3 boundary.

pub mod observability;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("tenant_id is required")]
    MissingTenantId,
    #[error("tenant_id contains invalid characters")]
    InvalidTenantId,
    #[error("run_id is required")]
    MissingRunId,
    #[error("run_id contains invalid characters")]
    InvalidRunId,
    #[error("ingest docs input_paths is required")]
    MissingIngestDocsInputPaths,
    #[error("input path contains invalid characters")]
    InvalidInputPath,
}

/// Tenant identifier.
///
/// Invariants:
/// - non-empty
/// - ASCII letters/digits plus `-` and `_`
/// - no path separators (prevents path traversal when used in directory names)
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct TenantId(pub String);

impl TenantId {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ProtocolError> {
        let raw_s: String = raw.into();
        if raw_s.is_empty() {
            return Err(ProtocolError::MissingTenantId);
        }
        if !raw_s
            .bytes()
            .all(|b: u8| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(ProtocolError::InvalidTenantId);
        }
        Ok(Self(raw_s))
    }
}

impl TryFrom<String> for TenantId {
    type Error = ProtocolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<TenantId> for String {
    fn from(value: TenantId) -> Self {
        value.0
    }
}

/// Run identifier (uuid-like string).
///
/// We intentionally avoid committing to a UUID parser here (to keep this crate light);
/// callers may enforce stricter validation.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct RunId(pub String);

impl RunId {
    pub fn parse(raw: impl Into<String>) -> Result<Self, ProtocolError> {
        let raw_s: String = raw.into();
        if raw_s.is_empty() {
            return Err(ProtocolError::MissingRunId);
        }
        if !raw_s
            .bytes()
            .all(|b: u8| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(ProtocolError::InvalidRunId);
        }
        Ok(Self(raw_s))
    }
}

impl TryFrom<String> for RunId {
    type Error = ProtocolError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::parse(value)
    }
}

impl From<RunId> for String {
    fn from(value: RunId) -> Self {
        value.0
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Limits {
    pub wall_time_ms: Option<u64>,
    pub memory_bytes: Option<u64>,
    pub stdout_max_bytes: Option<u64>,
    pub stderr_max_bytes: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    pub network: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExecLane {
    Wasm,
    Process,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecRequest {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub lane: ExecLane,
    pub capabilities: Capabilities,
    pub limits: Limits,
    /// Command to execute; the first entry is the program, the rest are arguments.
    pub command: Vec<String>,
    /// Optional working directory for the command. When omitted, the executor
    /// will choose a tenant/run-scoped directory.
    pub cwd: Option<String>,
    /// Environment variables for the child process. Implementations may choose
    /// to start from a scrubbed environment and selectively pass through values.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Optional stdin payload for the child process.
    pub stdin_text: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecResponse {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub ok: bool,
    pub exit_code: i32,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestRequest {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    /// Optional ingest kind. When omitted, the ingest implementation may
    /// choose a default for compatibility with early v1 stubs.
    #[serde(default)]
    pub kind: Option<IngestKind>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResponse {
    pub tenant_id: TenantId,
    pub run_id: RunId,
    pub ok: bool,
    /// Normalized records produced by the ingest operation.
    ///
    /// For high-throughput streaming, callers may prefer the CLI `--jsonl` mode
    /// which emits one record per line instead of returning a large bundle.
    #[serde(default)]
    pub records: Vec<ChunkRecord>,
    /// Optional human-readable error message (validation/policy/internal).
    #[serde(default)]
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum IngestKind {
    Docs(IngestDocsRequest),
    Messaging(IngestMessagingRequest),
    Api(IngestApiRequest),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestDocsRequest {
    /// Input files or directories. Directories are walked recursively.
    ///
    /// Paths are treated as provided; implementations may convert to absolute
    /// paths before opening.
    #[serde(default)]
    pub input_paths: Vec<String>,
    /// Maximum UTF-8 characters per chunk.
    #[serde(default)]
    pub max_chunk_chars: Option<usize>,
    /// Optional hint for stable source identifiers (e.g. a repo root).
    #[serde(default)]
    pub source_root: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestMessagingRequest {
    #[serde(default)]
    pub export_path: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestApiRequest {
    #[serde(default)]
    pub openapi_path: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRecord {
    pub tenant_id: TenantId,
    pub source_id: String,
    pub chunk_id: String,
    pub content: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
    pub fingerprint: String,
}

pub fn validate_input_path(raw: &str) -> Result<(), ProtocolError> {
    if raw.is_empty() {
        return Err(ProtocolError::InvalidInputPath);
    }
    // Reject obviously dangerous path strings. The executor and ingest crates may
    // enforce more strict rules (canonicalization + allowlists).
    if raw.contains('\0') {
        return Err(ProtocolError::InvalidInputPath);
    }
    let _p: &Path = Path::new(raw);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tenant_id_validation_allows_safe_chars() {
        let tid: TenantId = TenantId::parse("acme_co-1").unwrap();
        assert_eq!(tid.0, "acme_co-1");
    }

    #[test]
    fn tenant_id_validation_rejects_slashes() {
        let err: ProtocolError = TenantId::parse("evil/tenant").unwrap_err();
        assert!(matches!(err, ProtocolError::InvalidTenantId));
    }

    #[test]
    fn tenant_id_deserialize_enforces_invariants() {
        let err: serde_json::Error =
            serde_json::from_str::<TenantId>("\"evil/tenant\"").unwrap_err();
        assert!(err.to_string().contains("tenant_id"));
    }

    #[test]
    fn run_id_deserialize_enforces_invariants() {
        let err: serde_json::Error = serde_json::from_str::<RunId>("\"..\"").unwrap_err();
        assert!(err.to_string().contains("run_id"));
    }
}
