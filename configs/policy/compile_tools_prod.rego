package akc

import rego.v1

# "Prod" policy profile for AKC compile tool authorization.
# Goal: tighten execution boundaries and keep WASM safety controls auditable.

default allow := false
default reason := "policy.opa.deny"

allowed_actions := {"llm.complete", "executor.run"}
allowed_executor_stages := {"tests_smoke", "tests_full"}

approved_executor_repos := {
  "my-repo",
  "repo-prod",
}

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

deny_reasons contains "policy.prod.repo_not_approved_for_executor" if {
  input.action == "executor.run"
  not approved_executor_repos[input.scope.repo_id]
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

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  profile := object.get(input.context.wasm, "platform_capability_profile", {})
  unsupported := object.get(profile, "unsupported_controls", [])
  count(unsupported) > 0
  control := sort(unsupported)[0]
  msg := sprintf("policy.prod.wasm.unsupported_control_required.%s", [control])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  preopens := object.get(input.context.wasm, "preopen_dirs", [])
  raw := preopens[_]
  path := lower(sprintf("%v", [raw]))
  path == "/"
  msg := sprintf("policy.prod.wasm.disallowed_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  preopens := object.get(input.context.wasm, "preopen_dirs", [])
  raw := preopens[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/etc")
  msg := sprintf("policy.prod.wasm.disallowed_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  preopens := object.get(input.context.wasm, "preopen_dirs", [])
  raw := preopens[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/proc")
  msg := sprintf("policy.prod.wasm.disallowed_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  preopens := object.get(input.context.wasm, "preopen_dirs", [])
  raw := preopens[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/sys")
  msg := sprintf("policy.prod.wasm.disallowed_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  preopens := object.get(input.context.wasm, "preopen_dirs", [])
  raw := preopens[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/dev")
  msg := sprintf("policy.prod.wasm.disallowed_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  preopens := object.get(input.context.wasm, "preopen_dirs", [])
  raw := preopens[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/users")
  msg := sprintf("policy.prod.wasm.disallowed_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  preopens := object.get(input.context.wasm, "preopen_dirs", [])
  raw := preopens[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/home")
  msg := sprintf("policy.prod.wasm.disallowed_preopen_path.%s", [path])
}

deny_reasons contains "policy.prod.wasm.network_requires_explicit_exception" if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  object.get(input.context.wasm, "network_enabled", false)
  not object.get(input.context.wasm, "network_exception", "")
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  writable := object.get(input.context.wasm, "writable_preopen_dirs", [])
  raw := writable[_]
  path := lower(sprintf("%v", [raw]))
  path == "/"
  msg := sprintf("policy.prod.wasm.disallowed_writable_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  writable := object.get(input.context.wasm, "writable_preopen_dirs", [])
  raw := writable[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/etc")
  msg := sprintf("policy.prod.wasm.disallowed_writable_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  writable := object.get(input.context.wasm, "writable_preopen_dirs", [])
  raw := writable[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/proc")
  msg := sprintf("policy.prod.wasm.disallowed_writable_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  writable := object.get(input.context.wasm, "writable_preopen_dirs", [])
  raw := writable[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/sys")
  msg := sprintf("policy.prod.wasm.disallowed_writable_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  writable := object.get(input.context.wasm, "writable_preopen_dirs", [])
  raw := writable[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/dev")
  msg := sprintf("policy.prod.wasm.disallowed_writable_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  writable := object.get(input.context.wasm, "writable_preopen_dirs", [])
  raw := writable[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/users")
  msg := sprintf("policy.prod.wasm.disallowed_writable_preopen_path.%s", [path])
}

deny_reasons contains msg if {
  input.action == "executor.run"
  input.context.backend == "wasm"
  writable := object.get(input.context.wasm, "writable_preopen_dirs", [])
  raw := writable[_]
  path := lower(sprintf("%v", [raw]))
  startswith(path, "/home")
  msg := sprintf("policy.prod.wasm.disallowed_writable_preopen_path.%s", [path])
}

deny_reasons contains "policy.prod.docker.read_only_rootfs_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not object.get(input.context.docker, "read_only_rootfs", false)
}

deny_reasons contains "policy.prod.docker.no_new_privileges_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not object.get(input.context.docker, "no_new_privileges", false)
}

deny_reasons contains "policy.prod.docker.cap_drop_all_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not object.get(input.context.docker, "cap_drop_all", false)
}

deny_reasons contains "policy.prod.docker.non_root_user_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not object.get(input.context.docker, "user_is_non_root", false)
}

deny_reasons contains "policy.prod.docker.seccomp_profile_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not object.get(object.get(input.context.docker, "security_profiles", {}), "seccomp", "")
}

deny_reasons contains "policy.prod.docker.seccomp_profile_unconfined" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  lower(sprintf("%v", [object.get(object.get(input.context.docker, "security_profiles", {}), "seccomp", "")])) == "unconfined"
}

deny_reasons contains "policy.prod.docker.apparmor_profile_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  platform := object.get(input.context.docker, "platform", {})
  object.get(platform, "apparmor_available", false)
  not object.get(object.get(input.context.docker, "security_profiles", {}), "apparmor", "")
}

deny_reasons contains "policy.prod.docker.apparmor_profile_unconfined" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  lower(sprintf("%v", [object.get(object.get(input.context.docker, "security_profiles", {}), "apparmor", "")])) == "unconfined"
}

deny_reasons contains "policy.prod.docker.tmpfs_tmp_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  mounts := object.get(input.context.docker, "tmpfs_mounts", [])
  not "/tmp" in mounts
}

deny_reasons contains "policy.prod.docker.memory_limit_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not is_number(object.get(object.get(input.context.docker, "limits", {}), "memory_bytes", null))
}

deny_reasons contains "policy.prod.docker.memory_limit_positive" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  is_number(object.get(object.get(input.context.docker, "limits", {}), "memory_bytes", null))
  object.get(object.get(input.context.docker, "limits", {}), "memory_bytes", 0) <= 0
}

deny_reasons contains "policy.prod.docker.pids_limit_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not is_number(object.get(object.get(input.context.docker, "limits", {}), "pids_limit", null))
}

deny_reasons contains "policy.prod.docker.pids_limit_positive" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  is_number(object.get(object.get(input.context.docker, "limits", {}), "pids_limit", null))
  object.get(object.get(input.context.docker, "limits", {}), "pids_limit", 0) <= 0
}

deny_reasons contains "policy.prod.docker.ulimit_nofile_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not object.get(object.get(input.context.docker, "limits", {}), "ulimit_nofile", "")
}

deny_reasons contains "policy.prod.docker.ulimit_nproc_required" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  not object.get(object.get(input.context.docker, "limits", {}), "ulimit_nproc", "")
}

deny_reasons contains "policy.prod.docker.network_requires_explicit_exception" if {
  input.action == "executor.run"
  input.context.backend == "docker"
  object.get(input.context.docker, "network_enabled", false)
  not object.get(input.context.docker, "network_exception", "")
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
  approved_executor_repos[input.scope.repo_id]
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
