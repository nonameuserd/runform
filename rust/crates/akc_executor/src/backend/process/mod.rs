mod native;

#[cfg(target_os = "linux")]
mod bwrap;

mod limits;

pub(crate) fn run_process_lane(
    request: akc_protocol::ExecRequest,
) -> Result<akc_protocol::ExecResponse, crate::ExecutorError> {
    let backend = native::parse_exec_backend();
    match backend {
        native::ExecBackend::Native => native::run_process_lane_native(request),
        native::ExecBackend::Docker => native::run_process_lane_docker_denied(request),
        #[cfg(target_os = "linux")]
        native::ExecBackend::Bwrap => bwrap::run_process_lane_bwrap(request),
    }
}
