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
    #[error("filesystem policy path is invalid or unsafe")]
    InvalidFsPolicyPath,
    #[error("cwd is invalid or unsafe")]
    InvalidCwd,
    #[error("preopen_dirs is only allowed for wasm lane")]
    PreopenDirsRequiresWasmLane,
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize)]
pub struct FsPolicy {
    /// Explicit allowlist of host paths that may be opened for read access.
    ///
    /// Validation is enforced at deserialize time:
    /// - must be absolute
    /// - must not contain `..` or `.` components
    /// - must not contain NUL bytes
    #[serde(default)]
    pub allowed_read_paths: Vec<String>,
    /// Explicit allowlist of host paths that may be opened for write access.
    ///
    /// Same validation rules as `allowed_read_paths`.
    #[serde(default)]
    pub allowed_write_paths: Vec<String>,
    /// WASI preopened dirs (future-proofing for a WASM lane with filesystem access).
    ///
    /// Validation is enforced at deserialize time. Additionally, `ExecRequest` enforces
    /// that this is empty unless `lane == wasm`.
    #[serde(default)]
    pub preopen_dirs: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
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
    /// Filesystem capability/policy for this execution.
    ///
    /// Defaults to deny-by-default (no allowlisted paths, no preopens).
    #[serde(default)]
    pub fs_policy: FsPolicy,
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

fn validate_safe_abs_path(raw: &str) -> Result<(), ProtocolError> {
    if raw.is_empty() || raw.contains('\0') {
        return Err(ProtocolError::InvalidFsPolicyPath);
    }
    let p: &Path = Path::new(raw);
    if !p.is_absolute() {
        return Err(ProtocolError::InvalidFsPolicyPath);
    }
    if p.components().any(|c| {
        matches!(
            c,
            std::path::Component::ParentDir | std::path::Component::CurDir
        )
    }) {
        return Err(ProtocolError::InvalidFsPolicyPath);
    }
    Ok(())
}

fn validate_cwd(raw: &str) -> Result<(), ProtocolError> {
    if raw.is_empty() || raw.contains('\0') {
        return Err(ProtocolError::InvalidCwd);
    }
    let p: &Path = Path::new(raw);
    if p.is_absolute() {
        return Ok(());
    }
    // For relative cwd values, reject traversal-ish components early.
    if p.components().any(|c| {
        matches!(
            c,
            std::path::Component::ParentDir | std::path::Component::CurDir
        )
    }) {
        return Err(ProtocolError::InvalidCwd);
    }
    Ok(())
}

impl<'de> Deserialize<'de> for FsPolicy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawFsPolicy {
            #[serde(default)]
            allowed_read_paths: Vec<String>,
            #[serde(default)]
            allowed_write_paths: Vec<String>,
            #[serde(default)]
            preopen_dirs: Vec<String>,
        }

        let raw: RawFsPolicy = RawFsPolicy::deserialize(deserializer)?;

        for p in raw.allowed_read_paths.iter() {
            validate_safe_abs_path(p).map_err(serde::de::Error::custom)?;
        }
        for p in raw.allowed_write_paths.iter() {
            validate_safe_abs_path(p).map_err(serde::de::Error::custom)?;
        }
        for p in raw.preopen_dirs.iter() {
            validate_safe_abs_path(p).map_err(serde::de::Error::custom)?;
        }

        Ok(Self {
            allowed_read_paths: raw.allowed_read_paths,
            allowed_write_paths: raw.allowed_write_paths,
            preopen_dirs: raw.preopen_dirs,
        })
    }
}

impl<'de> Deserialize<'de> for ExecRequest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawExecRequest {
            tenant_id: TenantId,
            run_id: RunId,
            lane: ExecLane,
            capabilities: Capabilities,
            limits: Limits,
            command: Vec<String>,
            cwd: Option<String>,
            #[serde(default)]
            env: BTreeMap<String, String>,
            stdin_text: Option<String>,
            #[serde(default)]
            fs_policy: FsPolicy,
        }

        let raw: RawExecRequest = RawExecRequest::deserialize(deserializer)?;

        if let Some(cwd_raw) = raw.cwd.as_deref() {
            validate_cwd(cwd_raw).map_err(serde::de::Error::custom)?;
        }

        if raw.lane != ExecLane::Wasm && !raw.fs_policy.preopen_dirs.is_empty() {
            return Err(serde::de::Error::custom(
                ProtocolError::PreopenDirsRequiresWasmLane,
            ));
        }

        Ok(Self {
            tenant_id: raw.tenant_id,
            run_id: raw.run_id,
            lane: raw.lane,
            capabilities: raw.capabilities,
            limits: raw.limits,
            command: raw.command,
            cwd: raw.cwd,
            env: raw.env,
            stdin_text: raw.stdin_text,
            fs_policy: raw.fs_policy,
        })
    }
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

    #[test]
    fn fs_policy_deserialize_rejects_relative_paths() {
        let err: serde_json::Error =
            serde_json::from_str::<FsPolicy>(r#"{"allowed_read_paths":["relative"]}"#).unwrap_err();
        assert!(err.to_string().contains("filesystem policy"));
    }

    #[test]
    fn exec_request_deserialize_rejects_preopen_dirs_for_process_lane() {
        let json = r#"
        {
          "tenant_id": "tenant_a",
          "run_id": "run_1",
          "lane": { "type": "process" },
          "capabilities": { "network": false },
          "limits": {},
          "command": ["echo", "hi"],
          "fs_policy": { "preopen_dirs": ["/tmp"] }
        }
        "#;
        let err: serde_json::Error = serde_json::from_str::<ExecRequest>(json).unwrap_err();
        assert!(err.to_string().contains("preopen_dirs"));
    }
}
