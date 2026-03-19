use std::path::{Path, PathBuf};

use akc_protocol::FsPolicy;

use crate::util::ensure_safe_absolute_path_string;
use crate::ExecutorError;

fn canonicalize_allowlisted_path_within(
    workspace: &PathBuf,
    raw: &str,
    require_exists: bool,
) -> Result<PathBuf, ExecutorError> {
    let p: &Path = ensure_safe_absolute_path_string(raw)?;
    if require_exists {
        let canon = p.canonicalize()?;
        if !canon.starts_with(workspace) {
            return Err(ExecutorError::PolicyDenied);
        }
        return Ok(canon);
    }

    // For write allowlists we allow non-existent leaf paths (e.g. outputs), but we
    // still require the canonicalized parent directory to be within the workspace.
    if p.exists() {
        let canon = p.canonicalize()?;
        if !canon.starts_with(workspace) {
            return Err(ExecutorError::PolicyDenied);
        }
        return Ok(canon);
    }

    let parent: &Path = p.parent().ok_or(ExecutorError::PolicyDenied)?;
    if !parent.exists() {
        return Err(ExecutorError::PolicyDenied);
    }
    let parent_canon = parent.canonicalize()?;
    if !parent_canon.starts_with(workspace) {
        return Err(ExecutorError::PolicyDenied);
    }
    let file_name = p.file_name().ok_or(ExecutorError::PolicyDenied)?;
    Ok(parent_canon.join(file_name))
}

pub(crate) fn enforce_fs_policy(
    workspace: &PathBuf,
    fs_policy: &FsPolicy,
) -> Result<(), ExecutorError> {
    // Default deny: any requested host paths must be within the tenant/run workspace.
    for p in fs_policy.allowed_read_paths.iter() {
        let _canon = canonicalize_allowlisted_path_within(workspace, p, true)?;
    }
    for p in fs_policy.allowed_write_paths.iter() {
        let _canon = canonicalize_allowlisted_path_within(workspace, p, false)?;
    }
    Ok(())
}
