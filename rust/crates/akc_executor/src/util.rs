use std::fs;
use std::path::{Component, Path, PathBuf};

use crate::ExecutorError;

pub(crate) fn compact_artifact_id(command0: Option<&str>) -> String {
    let Some(s) = command0 else {
        return "".to_string();
    };
    // For paths, keep the filename portion to avoid leaking directories.
    let p = std::path::Path::new(s);
    p.file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| s.to_string())
}

pub(crate) fn workspace_root() -> Result<PathBuf, std::io::Error> {
    // Root can be overridden for tests/CI; default is a relative `.akc` directory.
    // We eagerly create it and canonicalize so later containment checks are stable.
    let root_raw = std::env::var("AKC_EXEC_ROOT").unwrap_or_else(|_| ".akc".to_string());
    let root = PathBuf::from(root_raw);
    fs::create_dir_all(&root)?;
    root.canonicalize()
}

pub(crate) fn clamp_bytes(s: String, max_bytes: Option<u64>) -> String {
    let Some(max) = max_bytes else {
        return s;
    };
    let max_usize: usize = max.min(usize::MAX as u64) as usize;
    if s.len() <= max_usize {
        return s;
    }
    // Truncate without violating UTF-8 boundaries (never panic).
    let mut idx: usize = max_usize;
    while idx > 0 && !s.is_char_boundary(idx) {
        idx -= 1;
    }
    s[..idx].to_string()
}

pub(crate) fn canonicalize_within(base: &PathBuf, raw: &PathBuf) -> Result<PathBuf, ExecutorError> {
    // Ensure the path exists so canonicalize resolves symlinks and removes `..`.
    fs::create_dir_all(raw)?;
    let canonical = raw.canonicalize()?;
    if !canonical.starts_with(base) {
        return Err(ExecutorError::PolicyDenied);
    }
    Ok(canonical)
}

pub(crate) fn ensure_safe_absolute_path_string(raw: &str) -> Result<&Path, ExecutorError> {
    if raw.is_empty() || raw.contains('\0') {
        return Err(ExecutorError::PolicyDenied);
    }
    let p: &Path = Path::new(raw);
    if !p.is_absolute() {
        return Err(ExecutorError::PolicyDenied);
    }
    if p.components()
        .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
    {
        return Err(ExecutorError::PolicyDenied);
    }
    Ok(p)
}

#[cfg(test)]
mod tests {
    use super::clamp_bytes;

    #[test]
    fn clamp_bytes_is_utf8_safe() {
        let s = "€€€".to_string(); // 3 bytes per char
        let out = clamp_bytes(s.clone(), Some(1));
        assert_eq!(out, "");

        let out = clamp_bytes(s.clone(), Some(3));
        assert_eq!(out, "€");

        let out = clamp_bytes(s, Some(4));
        assert_eq!(out, "€");
    }
}
