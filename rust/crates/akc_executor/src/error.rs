use thiserror::Error;

#[derive(Debug, Error)]
pub enum ExecutorError {
    #[error("policy denied")]
    PolicyDenied,
    #[error("unsupported execution lane")]
    UnsupportedLane,
    #[error("execution timed out")]
    Timeout,
    #[error("command must be non-empty")]
    EmptyCommand,
    #[error("command not allowed by policy")]
    CommandNotAllowed,
    #[error("wasm execution failed: {0}")]
    Wasm(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

pub(crate) fn executor_error_kind(err: &ExecutorError) -> &'static str {
    match err {
        ExecutorError::PolicyDenied => "policy_denied",
        ExecutorError::UnsupportedLane => "unsupported_lane",
        ExecutorError::Timeout => "timeout",
        ExecutorError::EmptyCommand => "empty_command",
        ExecutorError::CommandNotAllowed => "command_not_allowed",
        ExecutorError::Wasm(_) => "wasm_error",
        ExecutorError::Io(_) => "io_error",
    }
}
