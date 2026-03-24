package akc

# Starter policy for AKC compile tool authorization.
# Decision path used by CLI defaults to: data.akc.allow

default allow := false
default reason := "policy.opa.deny"

allowed_actions := {"llm.complete", "executor.run"}
allowed_executor_stages := {"tests_smoke", "tests_full"}

capability_matches {
  input.capability.tenant_id == input.scope.tenant_id
  input.capability.repo_id == input.scope.repo_id
  input.capability.action == input.action
}

has_deny {
  deny_reasons[_]
}

deny_reasons["policy.capability.scope_or_action_mismatch"] {
  not capability_matches
}

deny_reasons["policy.default_deny.action_not_allowlisted"] {
  not allowed_actions[input.action]
}

deny_reasons["policy.executor.stage_not_allowed"] {
  input.action == "executor.run"
  not allowed_executor_stages[input.context.stage]
}

deny_reasons["policy.executor.wasm_context_missing"] {
  input.action == "executor.run"
  input.context.backend == "wasm"
  not input.context.wasm
}

deny_reasons["policy.executor.wasm.network_flag_missing"] {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_boolean(input.context.wasm.network_enabled)
}

deny_reasons["policy.executor.wasm.preopen_dirs_missing"] {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.preopen_dirs)
}

deny_reasons["policy.executor.wasm.writable_preopen_dirs_missing"] {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.writable_preopen_dirs)
}

deny_reasons["policy.executor.wasm.read_only_preopen_dirs_missing"] {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.read_only_preopen_dirs)
}

deny_reasons["policy.executor.wasm.limits_tuple_missing"] {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.limits_tuple)
}

deny_reasons["policy.executor.wasm.platform_profile_missing"] {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not input.context.wasm.platform_capability_profile
}

deny_reasons["policy.executor.docker_context_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  not input.context.docker
}

deny_reasons["policy.executor.docker.network_mode_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_string(input.context.docker.network_mode)
}

deny_reasons["policy.executor.docker.read_only_rootfs_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.read_only_rootfs)
}

deny_reasons["policy.executor.docker.no_new_privileges_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.no_new_privileges)
}

deny_reasons["policy.executor.docker.cap_drop_all_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.cap_drop_all)
}

deny_reasons["policy.executor.docker.user_presence_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.user_present)
}

deny_reasons["policy.executor.docker.security_profiles_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_object(input.context.docker.security_profiles)
}

deny_reasons["policy.executor.docker.limits_missing"] {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_object(input.context.docker.limits)
}

allow {
  capability_matches
  allowed_actions[input.action]
  input.action == "llm.complete"
  not has_deny
}

allow {
  capability_matches
  input.action == "executor.run"
  allowed_executor_stages[input.context.stage]
  not has_deny
}

reason = msg {
  msgs := sort([x | deny_reasons[x]])
  count(msgs) > 0
  msg := msgs[0]
}

reason = "policy.opa.allow" {
  allow
}
