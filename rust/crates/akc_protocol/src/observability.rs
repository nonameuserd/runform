use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::{RunId, TenantId};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    Error = 1,
    Warn = 2,
    Info = 3,
    Debug = 4,
    Trace = 5,
}

impl LogLevel {
    pub fn as_str(self) -> &'static str {
        match self {
            LogLevel::Error => "error",
            LogLevel::Warn => "warn",
            LogLevel::Info => "info",
            LogLevel::Debug => "debug",
            LogLevel::Trace => "trace",
        }
    }
}

fn current_level() -> LogLevel {
    let raw = std::env::var("AKC_LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    let raw = raw.trim().to_ascii_lowercase();
    match raw.as_str() {
        "error" => LogLevel::Error,
        "warn" | "warning" => LogLevel::Warn,
        "debug" => LogLevel::Debug,
        "trace" => LogLevel::Trace,
        // Default + fallback.
        _ => LogLevel::Info,
    }
}

fn should_log(level: LogLevel) -> bool {
    // Lower number = more severe; allow everything >= current threshold by numeric compare.
    level <= current_level()
}

fn ts_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Emit a single-line JSON log record to `stderr`.
///
/// This is intentionally dependency-light (no tracing/log subscribers) so it can be
/// used from both CLI binaries and the PyO3 embedding path.
pub fn log_event(level: LogLevel, event: &str, tenant_id: &TenantId, run_id: &RunId, extra: Value) {
    if !should_log(level) {
        return;
    }

    let mut obj: Map<String, Value> = Map::new();
    obj.insert("ts_unix_ms".to_string(), Value::from(ts_unix_ms()));
    obj.insert("level".to_string(), Value::from(level.as_str()));
    obj.insert("event".to_string(), Value::from(event));
    obj.insert("pid".to_string(), Value::from(std::process::id()));
    obj.insert("tenant_id".to_string(), Value::from(tenant_id.0.clone()));
    obj.insert("run_id".to_string(), Value::from(run_id.0.clone()));

    if let Some(extra_obj) = extra.as_object() {
        for (k, v) in extra_obj {
            obj.insert(k.clone(), v.clone());
        }
    }

    // One-line JSON for easy ingestion into log pipelines.
    eprintln!("{}", Value::Object(obj));
}

/// Emit a single-line JSON log record without tenant/run scoping (e.g. invalid JSON).
pub fn log_event_unscoped(level: LogLevel, event: &str, extra: Value) {
    if !should_log(level) {
        return;
    }

    let mut obj: Map<String, Value> = Map::new();
    obj.insert("ts_unix_ms".to_string(), Value::from(ts_unix_ms()));
    obj.insert("level".to_string(), Value::from(level.as_str()));
    obj.insert("event".to_string(), Value::from(event));
    obj.insert("pid".to_string(), Value::from(std::process::id()));
    obj.insert("tenant_id".to_string(), Value::Null);
    obj.insert("run_id".to_string(), Value::Null);

    if let Some(extra_obj) = extra.as_object() {
        for (k, v) in extra_obj {
            obj.insert(k.clone(), v.clone());
        }
    }

    eprintln!("{}", Value::Object(obj));
}
