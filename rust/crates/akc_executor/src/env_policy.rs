use std::collections::BTreeMap;

use akc_protocol::observability::{log_event, LogLevel};
use akc_protocol::{RunId, TenantId};
use serde_json::json;

/// Parse `AKC_EXEC_ENV_ALLOW_PREFIXES` into a list of prefixes.
///
/// - Empty/unset => no prefix allowlist (denylist-only mode)
/// - Otherwise => only keys matching at least one prefix are eligible (still subject to denylist)
fn allow_prefixes() -> Option<Vec<String>> {
    let raw = std::env::var("AKC_EXEC_ENV_ALLOW_PREFIXES").ok()?;
    let prefixes: Vec<String> = raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect();
    if prefixes.is_empty() {
        None
    } else {
        Some(prefixes)
    }
}

fn is_dangerous_env_key(key: &str) -> bool {
    // Conservative denylist of env vars commonly used for loader / interpreter injection.
    // Keep this list intentionally broad; env is not a capability boundary.
    //
    // Notes:
    // - We intentionally deny overriding PATH because the executor sets a minimal PATH
    //   and the request should not be able to expand it (host compromise risk).
    // - We deny dynamic loader vars by prefix and common exact names.
    // - We deny "option" vars for popular runtimes that can inject code at startup.
    if key == "PATH" {
        return true;
    }

    if key.starts_with("LD_") || key.starts_with("DYLD_") {
        return true;
    }

    matches!(
        key,
        // Linux/glibc
        "LD_PRELOAD" | "LD_LIBRARY_PATH" | "LD_AUDIT" | "LD_DEBUG" | "LD_DEBUG_OUTPUT"
        // macOS dyld
        | "DYLD_INSERT_LIBRARIES" | "DYLD_LIBRARY_PATH" | "DYLD_FALLBACK_LIBRARY_PATH"
        | "DYLD_FRAMEWORK_PATH" | "DYLD_FALLBACK_FRAMEWORK_PATH"
        // Python
        | "PYTHONPATH" | "PYTHONHOME" | "PYTHONSTARTUP"
        // Node
        | "NODE_OPTIONS"
        // Ruby
        | "RUBYOPT" | "RUBYLIB"
        // Perl
        | "PERL5OPT" | "PERL5LIB"
        // Shell
        | "BASH_ENV" | "ENV" | "PROMPT_COMMAND"
    )
}

fn is_key_allowed_by_prefix(key: &str, prefixes: &[String]) -> bool {
    prefixes.iter().any(|p| key.starts_with(p))
}

/// Filter request-provided env vars according to the executor env policy.
///
/// - Always denies known-dangerous keys
/// - Optionally enforces a prefix allowlist via `AKC_EXEC_ENV_ALLOW_PREFIXES`
/// - Emits an `exec_env_key_denied` log for denied keys
pub(crate) fn filter_request_env(
    tenant_id: &TenantId,
    run_id: &RunId,
    env: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let prefixes: Option<Vec<String>> = allow_prefixes();
    let mut out: Vec<(String, String)> = Vec::with_capacity(env.len());

    for (k, v) in env.iter() {
        let denied: bool = is_dangerous_env_key(k)
            || prefixes
                .as_ref()
                .is_some_and(|p| !is_key_allowed_by_prefix(k, p));

        if denied {
            log_event(
                LogLevel::Warn,
                "exec_env_key_denied",
                tenant_id,
                run_id,
                json!({
                    "key": k,
                }),
            );
            continue;
        }

        out.push((k.clone(), v.clone()));
    }

    out
}
