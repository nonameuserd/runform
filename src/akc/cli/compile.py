from __future__ import annotations

import argparse
import importlib.util
import shutil
import sys
from dataclasses import replace
from pathlib import Path
from typing import Literal, cast

from akc.compile import (
    Budget,
    CompileSession,
    ControllerConfig,
    CostRates,
    RustExecutor,
    TierConfig,
)
from akc.compile.controller_config import TestMode
from akc.compile.executors import (
    _validate_docker_security_identifier,
    _validate_docker_tmpfs_mounts,
    _validate_docker_ulimit,
    _validate_docker_user,
)
from akc.compile.interfaces import Executor, LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.compile.rust_bridge import BackendMode, ExecLane, RustExecConfig
from akc.run.manifest import REPLAYABLE_PASSES, ReplayMode

from .common import configure_logging


def _rust_exec_available(*, mode: BackendMode, exec_bin: str = "akc-exec") -> bool:
    """Best-effort availability probe for Rust execution surfaces."""

    if mode == "pyo3":
        return importlib.util.find_spec("akc_rust") is not None
    return shutil.which(exec_bin) is not None


def _platform_label() -> str:
    if sys.platform.startswith("linux"):
        return "Linux"
    if sys.platform == "darwin":
        return "macOS"
    if sys.platform in {"win32", "cygwin"}:
        return "Windows"
    return sys.platform


def _strict_wasm_profile(*, policy_mode: str, wasm_normalization_strict: bool) -> bool:
    return policy_mode == "enforce" or wasm_normalization_strict


def _requested_wasm_backend(
    *,
    use_rust_exec: bool,
    rust_exec_lane: ExecLane,
    sandbox_mode: str,
    strong_lane_preference: str,
) -> bool:
    if use_rust_exec:
        return rust_exec_lane == "wasm"
    return sandbox_mode == "strong" and strong_lane_preference == "wasm"


def _resolved_wasm_backend(
    *,
    use_rust_exec: bool,
    rust_exec_lane: ExecLane,
    selected_backend: str,
) -> bool:
    if use_rust_exec:
        return rust_exec_lane == "wasm"
    return selected_backend.endswith("-wasm")


def _emit_wasm_preflight_failure(*, summary: str, details: tuple[str, ...]) -> int:
    print(f"WASM preflight failed: {summary}")
    for detail in details:
        print(f"  - {detail}")
    return 2


def _emit_docker_preflight_failure(*, summary: str, details: tuple[str, ...]) -> int:
    print(f"Docker preflight failed: {summary}")
    for detail in details:
        print(f"  - {detail}")
    return 2


def _emit_policy_preflight_failure(*, summary: str, details: tuple[str, ...]) -> int:
    print(f"Policy preflight failed: {summary}")
    for detail in details:
        print(f"  - {detail}")
    return 2


def _parse_multi_flag_paths(values: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if not values:
        return ()
    parsed: list[str] = []
    for raw in values:
        text = str(raw).strip()
        if not text:
            continue
        parsed.append(text)
    return tuple(parsed)


def _docker_hardening_flags_supplied(args: argparse.Namespace) -> bool:
    return any(
        (
            getattr(args, "docker_user", None) is not None,
            bool(getattr(args, "docker_tmpfs", [])),
            getattr(args, "docker_seccomp_profile", None) is not None,
            getattr(args, "docker_apparmor_profile", None) is not None,
            getattr(args, "docker_ulimit_nofile", None) is not None,
            getattr(args, "docker_ulimit_nproc", None) is not None,
        )
    )


def _docker_apparmor_available() -> bool:
    if not sys.platform.startswith("linux"):
        return False
    enabled_path = Path("/sys/module/apparmor/parameters/enabled")
    try:
        return enabled_path.read_text(encoding="utf-8").strip().lower().startswith("y")
    except OSError:
        return False


def _docker_cli_available() -> bool:
    try:
        return shutil.which("docker") is not None
    except (AttributeError, OSError):
        return False


def _opa_cli_available() -> bool:
    try:
        return shutil.which("opa") is not None
    except (AttributeError, OSError):
        return False


def _resolve_docker_hardening_args(
    args: argparse.Namespace,
) -> tuple[str | None, tuple[str, ...], str | None, str | None, str | None, str | None]:
    docker_user_raw = getattr(args, "docker_user", None)
    docker_user = "65532:65532" if docker_user_raw is None else str(docker_user_raw)
    docker_tmpfs = _parse_multi_flag_paths(getattr(args, "docker_tmpfs", [])) or ("/tmp",)
    docker_seccomp_profile = getattr(args, "docker_seccomp_profile", None)
    docker_apparmor_profile = getattr(args, "docker_apparmor_profile", None)
    docker_ulimit_nofile = getattr(args, "docker_ulimit_nofile", None)
    docker_ulimit_nproc = getattr(args, "docker_ulimit_nproc", None)
    return (
        _validate_docker_user(docker_user),
        _validate_docker_tmpfs_mounts(docker_tmpfs),
        _validate_docker_security_identifier(
            docker_seccomp_profile,
            field_name="docker seccomp profile",
            allow_absolute_path=True,
        ),
        _validate_docker_security_identifier(
            docker_apparmor_profile,
            field_name="docker apparmor profile",
            allow_absolute_path=False,
        ),
        _validate_docker_ulimit(docker_ulimit_nofile, field_name="docker ulimit nofile"),
        _validate_docker_ulimit(docker_ulimit_nproc, field_name="docker ulimit nproc"),
    )


def _preflight_docker_hardening(
    *,
    args: argparse.Namespace,
    sandbox_mode: str,
    strong_lane_preference: str,
    docker_available: bool,
) -> int | None:
    docker_hardening_requested = _docker_hardening_flags_supplied(args)
    docker_config_relevant = (
        docker_hardening_requested
        or (
            not bool(getattr(args, "use_rust_exec", False))
            and sandbox_mode == "strong"
            and strong_lane_preference != "wasm"
        )
    )
    if docker_hardening_requested:
        if bool(getattr(args, "use_rust_exec", False)):
            return _emit_docker_preflight_failure(
                summary="Docker hardening flags require the strong Docker lane",
                details=(
                    "remove Docker-specific flags or disable `--use-rust-exec`",
                    "Docker hardening is not applied to direct Rust execution lanes",
                ),
            )
        if sandbox_mode != "strong":
            return _emit_docker_preflight_failure(
                summary="Docker hardening flags require `--sandbox strong`",
                details=(
                    "the dev sandbox ignores Docker-only runtime controls",
                    "re-run with `--sandbox strong` to enforce the configured hardening",
                ),
            )
        if strong_lane_preference == "wasm":
            return _emit_docker_preflight_failure(
                summary=(
                    "Docker hardening flags are incompatible with `--strong-lane-preference wasm`"
                ),
                details=(
                    "Docker hardening cannot be enforced when the strong lane is locked to WASM",
                    "choose `--strong-lane-preference docker|auto` or remove the Docker flags",
                ),
            )
        if strong_lane_preference == "auto" and not docker_available:
            return _emit_docker_preflight_failure(
                summary=(
                    "configured Docker hardening would be dropped because Docker is unavailable"
                ),
                details=(
                    "install Docker or change to `--strong-lane-preference docker` "
                    "after Docker is available",
                    "do not rely on `auto` fallback when Docker-specific hardening is required",
                ),
            )

    if not docker_config_relevant:
        return None

    try:
        (
            _docker_user,
            _docker_tmpfs,
            docker_seccomp_profile,
            docker_apparmor_profile,
            _docker_ulimit_nofile,
            _docker_ulimit_nproc,
        ) = _resolve_docker_hardening_args(args)
    except ValueError as exc:
        return _emit_docker_preflight_failure(
            summary="invalid Docker hardening configuration",
            details=(str(exc),),
        )

    if docker_seccomp_profile is not None and docker_seccomp_profile.startswith("/"):
        seccomp_path = Path(docker_seccomp_profile)
        if not seccomp_path.exists():
            return _emit_docker_preflight_failure(
                summary="configured seccomp profile path is unavailable",
                details=(
                    f"path not found: {docker_seccomp_profile}",
                    "provide an existing absolute profile path or remove --docker-seccomp-profile",
                ),
            )
        if not seccomp_path.is_file():
            return _emit_docker_preflight_failure(
                summary="configured seccomp profile path is not a file",
                details=(
                    f"path is not a file: {docker_seccomp_profile}",
                    "point --docker-seccomp-profile at a readable seccomp profile file",
                ),
            )

    if docker_apparmor_profile is not None and not _docker_apparmor_available():
        return _emit_docker_preflight_failure(
            summary="configured AppArmor profile is unavailable on this host",
            details=(
                f"host platform: {_platform_label()}",
                "AppArmor profiles require a Linux host with AppArmor enabled",
            ),
        )

    return None


class _OfflineLLM(LLMBackend):
    """Deterministic, offline LLM backend for the CLI.

    This backend never calls external services. It produces a minimal, valid
    unified diff that touches both code and tests so the controller's
    tests-by-default and policy logic remain exercised.
    """

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: str,
        request: LLMRequest,
    ) -> LLMResponse:
        # The offline backend ignores the incoming request and returns
        # a deterministic patch that exercises both code and tests.
        _ = request
        text = "\n".join(
            [
                "--- a/src/akc_compiled.py",
                "+++ b/src/akc_compiled.py",
                "@@",
                f"+# compiled stage={stage} tenant={scope.tenant_id} repo={scope.repo_id}",
                "",
                "--- a/tests/test_akc_compiled.py",
                "+++ b/tests/test_akc_compiled.py",
                "@@",
                "+def test_compiled_smoke():",
                "+    assert True",
                "",
            ]
        )
        return LLMResponse(text=text, raw=None, usage=None)


def _build_compile_config(
    *,
    mode: str,
    policy_mode: str,
    opa_policy_path: str | None,
    opa_decision_path: str,
    cost_input_per_1k_tokens_usd: float,
    cost_output_per_1k_tokens_usd: float,
    cost_tool_call_usd: float,
) -> ControllerConfig:
    """Construct a ControllerConfig preset for CLI compile.

    - quick: conservative budget, smoke+periodic full tests.
    - thorough: larger budget, full tests every iteration.
    """

    tiers: dict[Literal["small", "medium", "large"], TierConfig] = {
        "small": TierConfig(name="small", llm_model="offline-small", temperature=0.0),
        "medium": TierConfig(name="medium", llm_model="offline-medium", temperature=0.2),
        "large": TierConfig(name="large", llm_model="offline-large", temperature=0.3),
    }

    if mode == "thorough":
        budget = Budget(max_llm_calls=12, max_repairs_per_step=4, max_iterations_total=8)
        test_mode: TestMode = "full"
        full_every: int | None = None
    else:
        budget = Budget(max_llm_calls=4, max_repairs_per_step=2, max_iterations_total=4)
        test_mode = "smoke"
        full_every = 2

    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=budget,
        test_mode=test_mode,
        full_test_every_n_iterations=full_every,
        policy_mode="audit_only" if policy_mode == "audit_only" else "enforce",
        tool_allowlist=("llm.complete", "executor.run"),
        cost_rates=CostRates(
            input_per_1k_tokens_usd=float(cost_input_per_1k_tokens_usd),
            output_per_1k_tokens_usd=float(cost_output_per_1k_tokens_usd),
            tool_call_usd=float(cost_tool_call_usd),
        ),
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
        metadata={"mode": mode},
    )


def cmd_compile(args: argparse.Namespace) -> int:
    """Run the compile loop for a tenant+repo scope."""

    configure_logging(verbose=args.verbose)

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()
    outputs_root.mkdir(parents=True, exist_ok=True)

    # Keep memory and artifacts scoped under <outputs_root>/<tenant>/<repo>.
    base = outputs_root / scope.tenant_id / scope.repo_id
    memory_db = base / ".akc" / "memory.sqlite"
    memory_db.parent.mkdir(parents=True, exist_ok=True)

    session = CompileSession.from_sqlite(
        tenant_id=scope.tenant_id,
        repo_id=scope.repo_id,
        sqlite_path=str(memory_db),
        index=None,
    )

    work_root = base if args.work_root is None else Path(args.work_root).expanduser()
    sandbox_memory_mb = int(getattr(args, "sandbox_memory_mb", 1024))
    sandbox_memory_bytes = max(1, sandbox_memory_mb) * 1024 * 1024
    sandbox_cpu_fuel_raw = getattr(args, "sandbox_cpu_fuel", None)
    sandbox_cpu_fuel = int(sandbox_cpu_fuel_raw) if sandbox_cpu_fuel_raw is not None else None
    if sandbox_cpu_fuel is not None and sandbox_cpu_fuel <= 0:
        print("--sandbox-cpu-fuel must be > 0 when set")
        return 2
    stdout_max_kb = int(getattr(args, "sandbox_stdout_max_kb", 2048))
    stderr_max_kb = int(getattr(args, "sandbox_stderr_max_kb", 2048))
    stdout_max_bytes = max(0, stdout_max_kb) * 1024
    stderr_max_bytes = max(0, stderr_max_kb) * 1024
    allow_network = bool(getattr(args, "sandbox_allow_network", False))
    sandbox_mode = str(getattr(args, "sandbox", "dev"))
    strong_lane_preference = str(getattr(args, "strong_lane_preference", "docker"))
    docker_available = _docker_cli_available()
    rust_exec_mode_raw = getattr(args, "rust_exec_mode", "cli")
    rust_exec_mode: BackendMode = "pyo3" if rust_exec_mode_raw == "pyo3" else "cli"
    rust_exec_lane_raw = getattr(args, "rust_exec_lane", "process")
    rust_exec_lane: ExecLane = "wasm" if rust_exec_lane_raw == "wasm" else "process"
    wasm_fs_normalize_existing_paths = bool(
        getattr(args, "wasm_fs_normalize_existing_paths", False)
    )
    wasm_preopen_dirs = _parse_multi_flag_paths(getattr(args, "wasm_preopen_dir", []))
    wasm_allowed_write_dirs = _parse_multi_flag_paths(getattr(args, "wasm_allow_write_dir", []))
    wasm_fs_normalization_profile = str(
        getattr(args, "wasm_fs_normalization_profile", "strict")
    ).strip()
    wasm_fs_normalization_strict = wasm_fs_normalization_profile != "relaxed"
    rust_available = _rust_exec_available(mode=rust_exec_mode)
    wasm_strict_profile = _strict_wasm_profile(
        policy_mode=str(args.policy_mode),
        wasm_normalization_strict=wasm_fs_normalization_strict,
    )
    wasm_requested = _requested_wasm_backend(
        use_rust_exec=bool(getattr(args, "use_rust_exec", False)),
        rust_exec_lane=rust_exec_lane,
        sandbox_mode=sandbox_mode,
        strong_lane_preference=strong_lane_preference,
    )
    if (wasm_preopen_dirs or wasm_allowed_write_dirs) and not wasm_requested:
        return _emit_wasm_preflight_failure(
            summary="WASM filesystem flags require explicit WASM lane selection",
            details=(
                "set `--sandbox strong --strong-lane-preference wasm` or "
                "`--use-rust-exec --rust-exec-lane wasm`",
                "WASM filesystem preopens are not applied to docker/process lanes",
            ),
        )
    docker_preflight_error = _preflight_docker_hardening(
        args=args,
        sandbox_mode=sandbox_mode,
        strong_lane_preference=strong_lane_preference,
        docker_available=docker_available,
    )
    if docker_preflight_error is not None:
        return docker_preflight_error
    opa_policy_path = getattr(args, "opa_policy_path", None)
    if opa_policy_path is not None and str(opa_policy_path).strip() and not _opa_cli_available():
        return _emit_policy_preflight_failure(
            summary="configured OPA policy requires the `opa` CLI, but it is unavailable",
            details=(
                f"policy path: {opa_policy_path}",
                "install the `opa` binary on PATH or omit --opa-policy-path",
            ),
        )
    (
        docker_user,
        docker_tmpfs_mounts,
        docker_seccomp_profile,
        docker_apparmor_profile,
        docker_ulimit_nofile,
        docker_ulimit_nproc,
    ) = _resolve_docker_hardening_args(args)

    executor: Executor
    selected_backend = "dev-subprocess"

    if wasm_requested and not rust_available:
        remediation = (
            "Install the Rust WASM execution surface (`akc-exec` on PATH or `akc_rust` for "
            "`--rust-exec-mode pyo3`)."
        )
        if bool(getattr(args, "use_rust_exec", False)):
            remediation += " Or choose `--rust-exec-lane process`."
        else:
            remediation += " Or choose `--strong-lane-preference docker|auto`."
        use_rust_exec = bool(getattr(args, "use_rust_exec", False))
        wasm_profile = "strict" if wasm_strict_profile else "relaxed"
        requested_backend = "rust-exec-wasm" if use_rust_exec else "strong-wasm"
        return _emit_wasm_preflight_failure(
            summary="requested WASM backend is unavailable on this host",
            details=(
                f"profile={wasm_profile} platform={_platform_label()}",
                f"requested_backend={requested_backend}",
                remediation,
            ),
        )

    if bool(getattr(args, "use_rust_exec", False)):
        rust_cfg = RustExecConfig(
            mode=rust_exec_mode,
            lane=rust_exec_lane,
            allow_network=bool(getattr(args, "rust_allow_network", False)) or allow_network,
            memory_bytes=sandbox_memory_bytes,
            cpu_fuel=sandbox_cpu_fuel,
            stdout_max_bytes=stdout_max_bytes or None,
            stderr_max_bytes=stderr_max_bytes or None,
            allowed_write_paths=wasm_allowed_write_dirs,
            preopen_dirs=wasm_preopen_dirs,
            wasm_normalize_existing_paths=wasm_fs_normalize_existing_paths,
            wasm_normalization_strict=wasm_fs_normalization_strict,
        )
        executor = RustExecutor(rust_cfg=rust_cfg, work_root=work_root)
        selected_backend = f"rust-{rust_exec_mode}-{rust_exec_lane}"
    else:
        from akc.execute import SandboxFactoryConfig, create_sandbox_executor

        executor_cfg = SandboxFactoryConfig(
            sandbox_mode=sandbox_mode,  # type: ignore[arg-type]
            work_root=str(work_root),
            allow_network=allow_network,
            memory_bytes=sandbox_memory_bytes,
            cpu_fuel=sandbox_cpu_fuel,
            stdout_max_bytes=stdout_max_bytes or None,
            stderr_max_bytes=stderr_max_bytes or None,
            docker_image=str(getattr(args, "docker_image", "python:3.12-slim")),
            docker_pids_limit=int(getattr(args, "docker_pids_limit", 256)),
            docker_cpus=getattr(args, "docker_cpus", None),
            docker_user=docker_user,
            docker_tmpfs_mounts=docker_tmpfs_mounts,
            docker_seccomp_profile=docker_seccomp_profile,
            docker_apparmor_profile=docker_apparmor_profile,
            docker_ulimit_nofile=docker_ulimit_nofile,
            docker_ulimit_nproc=docker_ulimit_nproc,
            rust_exec_mode=str(rust_exec_mode),  # type: ignore[arg-type]
            strong_lane_preference=strong_lane_preference,  # type: ignore[arg-type]
            rust_available_override=rust_available,
            wasm_normalize_existing_paths=wasm_fs_normalize_existing_paths,
            wasm_normalization_strict=wasm_fs_normalization_strict,
            preopen_dirs=wasm_preopen_dirs,
            allowed_write_paths=wasm_allowed_write_dirs,
            secrets_scope=None,
        )
        executor = create_sandbox_executor(cfg=executor_cfg)
        if sandbox_mode == "dev":
            selected_backend = "dev-subprocess"
        else:
            if strong_lane_preference == "wasm":
                selected_backend = f"rust-{rust_exec_mode}-wasm"
            elif strong_lane_preference == "docker":
                selected_backend = "docker"
            else:
                selected_backend = (
                    "docker"
                    if docker_available
                    else (f"rust-{rust_exec_mode}-wasm" if rust_available else "docker-unavailable")
                )
    if (
        wasm_strict_profile
        and _resolved_wasm_backend(
            use_rust_exec=bool(getattr(args, "use_rust_exec", False)),
            rust_exec_lane=rust_exec_lane,
            selected_backend=selected_backend,
        )
        and _platform_label() == "Windows"
    ):
        remediation = (
            "Use Linux/macOS for strict WASM runs, or switch to "
            "`--strong-lane-preference docker` / `--rust-exec-lane process`."
        )
        return _emit_wasm_preflight_failure(
            summary="strict WASM compile runs on Windows cannot guarantee wall-time enforcement",
            details=(
                "requested feature: bounded wall-time test execution for compile stages",
                "unsupported control: Wasmtime/WASM wall-time timeout on Windows",
                remediation,
            ),
        )
    config = _build_compile_config(
        mode=str(args.mode),
        policy_mode=str(args.policy_mode),
        opa_policy_path=str(args.opa_policy_path) if args.opa_policy_path is not None else None,
        opa_decision_path=str(args.opa_decision_path),
        cost_input_per_1k_tokens_usd=float(getattr(args, "cost_input_per_1k_usd", 0.0)),
        cost_output_per_1k_tokens_usd=float(getattr(args, "cost_output_per_1k_usd", 0.0)),
        cost_tool_call_usd=float(getattr(args, "cost_tool_call_usd", 0.0)),
    )
    if selected_backend == "docker":
        default_test_command: tuple[str, ...] | None = ("python", "-m", "pytest", "-q")
    elif selected_backend.startswith("rust-") and selected_backend.endswith("-wasm"):
        default_test_command = None
    else:
        default_test_command = (sys.executable, "-m", "pytest", "-q")
    if default_test_command is not None:
        config = replace(config, test_command=default_test_command)
    llm = _OfflineLLM()

    goal = args.goal or "Compile repository"
    partial_replay_passes: tuple[str, ...] | None = None
    raw_partial = getattr(args, "partial_replay_passes", None)
    if raw_partial is not None and str(raw_partial).strip():
        partial_replay_passes = tuple(p.strip() for p in str(raw_partial).split(",") if p.strip())
        invalid = [p for p in partial_replay_passes if p not in REPLAYABLE_PASSES]
        if invalid:
            print(
                "Invalid --partial-replay-passes values: "
                f"{invalid}. Allowed: {','.join(REPLAYABLE_PASSES)}"
            )
            return 2
        if str(getattr(args, "replay_mode", "live")) != "partial_replay":
            print("--partial-replay-passes requires --replay-mode partial_replay")
            return 2

    print(f"Running compile for scope={scope.tenant_id}/{scope.repo_id}")
    print(f"  goal: {goal}")
    print(f"  outputs_root: {outputs_root}")
    print(f"  work_root: {work_root}")
    print(
        "  sandbox: "
        f"{sandbox_mode} (allow_network={allow_network}, memory_mb={sandbox_memory_mb}, "
        f"cpu_fuel={sandbox_cpu_fuel if sandbox_cpu_fuel is not None else 'none'})"
    )
    if sandbox_mode == "strong":
        print(f"  strong_lane_preference: {strong_lane_preference}")
        ulimit_nofile_s = docker_ulimit_nofile if docker_ulimit_nofile is not None else "unset"
        ulimit_nproc_s = docker_ulimit_nproc if docker_ulimit_nproc is not None else "unset"
        print(
            "  docker_hardening: "
            f"user={docker_user if docker_user is not None else 'unset'}, "
            f"tmpfs={','.join(docker_tmpfs_mounts) if docker_tmpfs_mounts else 'none'}, "
            f"seccomp={'set' if docker_seccomp_profile is not None else 'default'}, "
            f"apparmor={'set' if docker_apparmor_profile is not None else 'default'}, "
            f"ulimit_nofile={ulimit_nofile_s}, "
            f"ulimit_nproc={ulimit_nproc_s}"
        )
    print(f"  sandbox_backend: {selected_backend}")
    if (
        sandbox_mode == "strong"
        and strong_lane_preference == "wasm"
        and not bool(getattr(args, "use_rust_exec", False))
        and not rust_available
    ):
        print("  note: Rust execution surface unavailable for requested wasm strong lane")

    result = session.run(
        goal=goal,
        llm=llm,
        executor=executor,
        config=config,
        outputs_root=outputs_root,
        schema_version=int(getattr(args, "schema_version", 1)),
        replay_mode=cast(ReplayMode, str(getattr(args, "replay_mode", "live"))),
        replay_manifest_path=getattr(args, "replay_manifest_path", None),
        partial_replay_passes=partial_replay_passes,
    )

    manifest_path = base / "manifest.json"
    print(f"  status: {result.status}")
    print(f"  manifest: {manifest_path}")

    if result.status == "succeeded":
        if not manifest_path.exists():
            print("WARNING: compile succeeded but manifest.json was not found")
        return 0

    print("Compile did not succeed within budget; see emitted artifacts for details.")
    return 2
