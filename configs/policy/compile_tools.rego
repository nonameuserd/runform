package akc

import rego.v1

# Starter policy for AKC compile tool authorization.
# Decision path used by CLI defaults to: data.akc.allow

default allow := false
default reason := "policy.opa.deny"

allowed_actions := {"llm.complete", "executor.run"}
allowed_executor_stages := {"tests_smoke", "tests_full"}

capability_matches if {
  input.capability.tenant_id == input.scope.tenant_id
  input.capability.repo_id == input.scope.repo_id
  input.capability.action == input.action
}

has_deny if {
  deny_reasons[_]
}

deny_reasons contains "policy.capability.scope_or_action_mismatch" if {
  not capability_matches
}

deny_reasons contains "policy.default_deny.action_not_allowlisted" if {
  not allowed_actions[input.action]
}

deny_reasons contains "policy.executor.stage_not_allowed" if {
  input.action == "executor.run"
  not allowed_executor_stages[input.context.stage]
}

deny_reasons contains "policy.executor.wasm_context_missing" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  not input.context.wasm
}

deny_reasons contains "policy.executor.wasm.network_flag_missing" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_boolean(input.context.wasm.network_enabled)
}

deny_reasons contains "policy.executor.wasm.preopen_dirs_missing" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.preopen_dirs)
}

deny_reasons contains "policy.executor.wasm.writable_preopen_dirs_missing" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.writable_preopen_dirs)
}

deny_reasons contains "policy.executor.wasm.read_only_preopen_dirs_missing" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.read_only_preopen_dirs)
}

deny_reasons contains "policy.executor.wasm.limits_tuple_missing" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not is_array(input.context.wasm.limits_tuple)
}

deny_reasons contains "policy.executor.wasm.platform_profile_missing" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  input.context.wasm
  not input.context.wasm.platform_capability_profile
}

deny_reasons contains "policy.executor.docker_context_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not input.context.docker
}

deny_reasons contains "policy.executor.docker.network_mode_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_string(input.context.docker.network_mode)
}

deny_reasons contains "policy.executor.docker.read_only_rootfs_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.read_only_rootfs)
}

deny_reasons contains "policy.executor.docker.no_new_privileges_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.no_new_privileges)
}

deny_reasons contains "policy.executor.docker.cap_drop_all_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.cap_drop_all)
}

deny_reasons contains "policy.executor.docker.user_presence_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_boolean(input.context.docker.user_present)
}

deny_reasons contains "policy.executor.docker.security_profiles_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_object(input.context.docker.security_profiles)
}

deny_reasons contains "policy.executor.docker.limits_missing" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  input.context.docker
  not is_object(input.context.docker.limits)
}

allow if {
  capability_matches
  allowed_actions[input.action]
  input.action == "llm.complete"
  not has_deny
}

allow if {
  capability_matches
  input.action == "executor.run"
  allowed_executor_stages[input.context.stage]
  not has_deny
}

reason := msg if {
  msgs := sort([x | x := deny_reasons[_]])
  count(msgs) > 0
  msg := msgs[0]
}

reason := "policy.opa.allow" if {
  allow
}
