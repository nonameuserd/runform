from __future__ import annotations

import argparse
import importlib.util
import json
import os
import shutil
import sys
from dataclasses import replace
from pathlib import Path
from typing import Literal, TypeAlias, cast

from akc.compile import (
    Budget,
    CompileSession,
    CostRates,
    RustExecutor,
    TierConfig,
)
from akc.compile.controller_config import (
    CompileMcpToolSpec,
    CompileMcpToolStage,
    CompileSkillsMode,
    ControllerConfig,
    TestMode,
)
from akc.compile.executors import (
    _validate_docker_security_identifier,
    _validate_docker_tmpfs_mounts,
    _validate_docker_ulimit,
    _validate_docker_user,
)
from akc.compile.interfaces import Executor, LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.compile.rust_bridge import BackendMode, ExecLane, RustExecConfig
from akc.control.policy_bundle import resolve_governance_profile_for_scope
from akc.control.policy_denial_explain import compile_extract_policy_denial, print_policy_denial
from akc.intent import IntentCompilerError
from akc.memory.models import JSONValue
from akc.promotion import normalize_promotion_mode, requires_deployable_steps, resolve_default_promotion_mode
from akc.run.manifest import REPLAYABLE_PASSES, ReplayMode
from akc.utils.fingerprint import stable_json_fingerprint

from .common import configure_logging
from .profile_defaults import (
    resolve_compile_profile_defaults,
    resolve_developer_role_profile,
    resolve_optional_project_string,
)
from .project_config import AkcProjectConfig, load_akc_project_config

IrGraphIntegrityPolicy: TypeAlias = Literal["off", "warn", "error"] | None


def resolve_compile_policies(
    *,
    cli_ir_operational: str | None,
    cli_ir_graph: str | None,
    cli_artifact_consistency: str | None,
) -> tuple[Literal["off", "warn", "error"], IrGraphIntegrityPolicy, Literal["off", "warn", "error"]]:
    """Resolve IR and artifact policy levels: CLI wins, then env, then defaults."""

    def _from_env(name: str) -> str | None:
        raw = os.environ.get(name)
        if raw is None:
            return None
        s = str(raw).strip().lower()
        if not s:
            return None
        if s not in {"off", "warn", "error"}:
            raise ValueError(f"{name} must be one of: off, warn, error (got {raw!r})")
        return s

    op = cli_ir_operational if cli_ir_operational is not None else _from_env("AKC_IR_OPERATIONAL_STRUCTURE_POLICY")
    if op is None:
        op = "warn"

    graph_raw = cli_ir_graph if cli_ir_graph is not None else _from_env("AKC_IR_GRAPH_INTEGRITY_POLICY")
    graph: IrGraphIntegrityPolicy = None if graph_raw is None else cast(Literal["off", "warn", "error"], graph_raw)

    ac_raw = (
        cli_artifact_consistency
        if cli_artifact_consistency is not None
        else _from_env("AKC_ARTIFACT_CONSISTENCY_POLICY")
    )
    if ac_raw is None:
        ac: Literal["off", "warn", "error"] = "warn"
    else:
        ac = cast(Literal["off", "warn", "error"], ac_raw)

    op_lit = cast(Literal["off", "warn", "error"], op)
    return op_lit, graph, ac


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


def _emit_compile_failure_details(*, base: Path) -> None:
    tests_dir = base / ".akc" / "tests"
    if tests_dir.is_dir():
        candidates = sorted(
            tests_dir.glob("*.json"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
        for fp in candidates:
            try:
                raw = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(raw, dict):
                continue
            stage = str(raw.get("stage") or "unknown")
            exit_code = raw.get("exit_code")
            command = raw.get("command")
            stdout = str(raw.get("stdout") or "").strip()
            stderr = str(raw.get("stderr") or "").strip()
            print(f"  last_execution_stage: {stage}")
            if isinstance(command, list) and command:
                print(f"  last_execution_command: {' '.join(str(x) for x in command)}")
            if exit_code is not None:
                print(f"  last_execution_exit_code: {int(exit_code)}")
            text = stderr or stdout
            label = "stderr" if stderr else "stdout"
            if text:
                print(f"  last_execution_{label}:")
                for line in text.splitlines()[:20]:
                    print(f"    {line}")
            return

    run_dir = base / ".akc" / "run"
    if not run_dir.is_dir():
        return
    log_files = sorted(
        run_dir.glob("*.log.txt"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for fp in log_files:
        try:
            lines = fp.read_text(encoding="utf-8").splitlines()
        except Exception:
            continue
        if not lines:
            continue
        print("  run_log_tail:")
        for line in lines[-10:]:
            print(f"    {line}")
        return


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
    docker_config_relevant = docker_hardening_requested or (
        not bool(getattr(args, "use_rust_exec", False))
        and sandbox_mode == "strong"
        and strong_lane_preference != "wasm"
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
                summary=("Docker hardening flags are incompatible with `--strong-lane-preference wasm`"),
                details=(
                    "Docker hardening cannot be enforced when the strong lane is locked to WASM",
                    "choose `--strong-lane-preference docker|auto` or remove the Docker flags",
                ),
            )
        if strong_lane_preference == "auto" and not docker_available:
            return _emit_docker_preflight_failure(
                summary=("configured Docker hardening would be dropped because Docker is unavailable"),
                details=(
                    "install Docker or change to `--strong-lane-preference docker` after Docker is available",
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
        # deterministic stage-specific payloads.
        _ = request
        if stage == "system_design":
            return LLMResponse(
                text=json.dumps(
                    {
                        "spec_version": 1,
                        "tenant_id": scope.tenant_id,
                        "repo_id": scope.repo_id,
                        "system_id": "offline-system",
                        "services": [{"name": "compile-controller", "role": "orchestrator"}],
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )
        if stage == "orchestration_spec":
            return LLMResponse(
                text=json.dumps(
                    {
                        "spec_version": 1,
                        "tenant_id": scope.tenant_id,
                        "repo_id": scope.repo_id,
                        "state_machine": {
                            "initial_state": "start",
                            "transitions": [
                                {"from": "start", "event": "compile", "to": "done"},
                            ],
                        },
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )
        if stage == "agent_coordination":
            return LLMResponse(
                text=json.dumps(
                    {
                        "spec_version": 1,
                        "tenant_id": scope.tenant_id,
                        "repo_id": scope.repo_id,
                        "agent_roles": {"planner": {"tools": ["llm.complete"]}},
                        "coordination_graph": {"nodes": ["planner"], "edges": []},
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )
        if stage == "deployment_config":
            return LLMResponse(
                text=json.dumps(
                    {
                        "docker_compose": {"services": {"app": {"read_only": True}}},
                        "kubernetes": {"securityContext": {"runAsNonRoot": True}},
                    },
                    sort_keys=True,
                ),
                raw=None,
                usage=None,
            )

        # Default controller stages return a deterministic unified diff that
        # exercises both code and tests. Hunks must be valid for GNU ``patch(1)``
        # so opt-in ``scoped_apply`` can apply offline stubs under a scope root.
        text = "\n".join(
            [
                "--- /dev/null",
                "+++ b/src/akc_compiled.py",
                "@@ -0,0 +1,1 @@",
                f"+# compiled stage={stage} tenant={scope.tenant_id} repo={scope.repo_id}",
                "",
                "--- /dev/null",
                "+++ b/tests/test_akc_compiled.py",
                "@@ -0,0 +1,3 @@",
                "+def test_compiled_smoke():",
                "+    assert True",
                "+",
                "",
            ]
        )
        return LLMResponse(text=text, raw=None, usage=None)


def _extend_tool_allowlist_for_mcp(base: tuple[str, ...]) -> tuple[str, ...]:
    extra = ("mcp.resource.read", "mcp.tool.call")
    seen = set(base)
    out = list(base)
    for x in extra:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return tuple(out)


def configure_compile_mcp_from_cli(
    config: ControllerConfig,
    *,
    project_root: Path,
    compile_mcp: bool,
    mcp_config: str | None,
    mcp_server: str | None,
    mcp_resources: list[str],
    mcp_tool_jsons: list[str],
    tools_generate_only: bool,
) -> ControllerConfig:
    """Enable compile-time MCP from CLI flags (shared server JSON with ``akc ingest``)."""

    if not compile_mcp:
        return config
    cfg_path = Path(mcp_config).expanduser() if mcp_config else project_root / ".akc" / "mcp-ingest.json"
    cfg_path = cfg_path.resolve()
    if not cfg_path.is_file():
        raise SystemExit(
            f"compile MCP config not found: {cfg_path}\n"
            "Add a multi-server MCP JSON (see `akc ingest` / docs) or pass --compile-mcp-config PATH."
        )
    tools: list[CompileMcpToolSpec] = []
    for raw in mcp_tool_jsons:
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as e:
            raise SystemExit(f"Invalid --compile-mcp-tool JSON: {e}") from e
        if not isinstance(obj, dict):
            raise SystemExit("--compile-mcp-tool must be a JSON object")
        tname = str(obj.get("tool_name") or obj.get("name") or "").strip()
        if not tname:
            raise SystemExit('--compile-mcp-tool requires string "tool_name" (or alias "name")')
        args = obj.get("arguments")
        if args is None:
            args_dict: dict[str, JSONValue] = {}
        elif isinstance(args, dict):
            args_dict = {str(k): cast(JSONValue, v) for k, v in args.items()}
        else:
            raise SystemExit("--compile-mcp-tool arguments must be a JSON object when set")
        tools.append(CompileMcpToolSpec(tool_name=tname, arguments=args_dict))
    res_uris = tuple(str(x).strip() for x in mcp_resources if str(x).strip())
    if not res_uris and not tools:
        raise SystemExit("--compile-mcp requires at least one --compile-mcp-resource and/or --compile-mcp-tool")
    stages: tuple[CompileMcpToolStage, ...] = ("generate",) if tools_generate_only else ("generate", "repair")
    srv = str(mcp_server).strip() if mcp_server is not None and str(mcp_server).strip() else None
    return replace(
        config,
        tool_allowlist=_extend_tool_allowlist_for_mcp(config.tool_allowlist),
        compile_mcp_enabled=True,
        compile_mcp_config_path=str(cfg_path),
        compile_mcp_server=srv,
        compile_mcp_resource_uris=res_uris,
        compile_mcp_tools=tuple(tools),
        compile_mcp_tool_stages=stages,
    )


def _build_compile_config(
    *,
    mode: str,
    policy_mode: str,
    opa_policy_path: str | None,
    opa_decision_path: str,
    cost_input_per_1k_tokens_usd: float,
    cost_output_per_1k_tokens_usd: float,
    cost_tool_call_usd: float,
    compile_realization_mode: Literal["artifact_only", "scoped_apply"],
    apply_scope_root: str | None,
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

    base_tool_allow: tuple[str, ...] = ("llm.complete", "executor.run")
    tool_allowlist: tuple[str, ...] = (
        base_tool_allow + ("compile.patch.apply",) if compile_realization_mode == "scoped_apply" else base_tool_allow
    )

    return ControllerConfig(
        tiers=tiers,
        stage_tiers={"generate": "small", "repair": "small"},
        budget=budget,
        test_mode=test_mode,
        full_test_every_n_iterations=full_every,
        policy_mode="audit_only" if policy_mode == "audit_only" else "enforce",
        tool_allowlist=tool_allowlist,
        cost_rates=CostRates(
            input_per_1k_tokens_usd=float(cost_input_per_1k_tokens_usd),
            output_per_1k_tokens_usd=float(cost_output_per_1k_tokens_usd),
            tool_call_usd=float(cost_tool_call_usd),
        ),
        opa_policy_path=opa_policy_path,
        opa_decision_path=opa_decision_path,
        compile_realization_mode=compile_realization_mode,
        apply_scope_root=apply_scope_root,
        metadata={"mode": mode},
    )


def _merge_compile_skills_from_sources(
    *,
    config: ControllerConfig,
    proj: AkcProjectConfig | None,
    cli_skill_names: list[str],
    cli_mode: str | None,
    cli_extra_skill_roots: list[str],
    cli_max_file_bytes: int | None = None,
    cli_max_total_bytes: int | None = None,
    project_dir: Path,
) -> ControllerConfig:
    """Merge project.json + CLI compile skill names, mode, roots, and byte caps into ``ControllerConfig``."""

    names: list[str] = []
    if proj is not None:
        names.extend(proj.compile_skills)
    for raw in cli_skill_names:
        s = str(raw).strip()
        if s:
            names.append(s)
    seen: set[str] = set()
    deduped: list[str] = []
    for n in names:
        if n in seen:
            continue
        seen.add(n)
        deduped.append(n)

    mode_raw = cli_mode if cli_mode is not None else (proj.compile_skills_mode if proj is not None else None)
    mode_str = str(mode_raw).strip().lower() if mode_raw is not None and str(mode_raw).strip() else None
    if mode_str is None or mode_str == "":
        mode_str = "explicit" if deduped else str(config.compile_skills_mode)
    allowed_modes = {"off", "default_only", "explicit", "auto"}
    if mode_str not in allowed_modes:
        raise SystemExit(f"Invalid compile skills mode {mode_raw!r}; expected one of {sorted(allowed_modes)}")
    roots = tuple(proj.skill_roots) if proj is not None else ()

    resolved_extras: list[Path] = []
    seen_extra: set[str] = set()
    for p in config.compile_skill_extra_roots:
        rp = p.expanduser().resolve()
        key = str(rp)
        if key in seen_extra:
            continue
        seen_extra.add(key)
        resolved_extras.append(rp)
    for raw in cli_extra_skill_roots:
        s = str(raw).strip()
        if not s:
            continue
        p = Path(s).expanduser()
        p = (project_dir / p).resolve() if not p.is_absolute() else p.resolve()
        key = str(p)
        if key in seen_extra:
            continue
        seen_extra.add(key)
        resolved_extras.append(p)

    file_bytes = cli_max_file_bytes
    if file_bytes is None and proj is not None:
        file_bytes = proj.compile_skill_max_file_bytes
    if file_bytes is not None and int(file_bytes) <= 0:
        raise SystemExit("compile_skill_max_file_bytes must be > 0")
    total_bytes = cli_max_total_bytes
    if total_bytes is None and proj is not None:
        total_bytes = proj.compile_skill_max_total_bytes
    if total_bytes is not None and int(total_bytes) <= 0:
        raise SystemExit("compile_skill_max_total_bytes must be > 0")

    cfg = replace(
        config,
        compile_skills_mode=cast(CompileSkillsMode, mode_str),
        compile_skill_allowlist=tuple(deduped),
        compile_skill_relative_roots=roots,
        compile_skill_extra_roots=tuple(resolved_extras),
    )
    if file_bytes is not None:
        cfg = replace(cfg, compile_skill_max_file_bytes=int(file_bytes))
    if total_bytes is not None:
        cfg = replace(cfg, compile_skill_max_total_bytes=int(total_bytes))
    return cfg


def cmd_compile(args: argparse.Namespace) -> int:
    """Run the compile loop for a tenant+repo scope."""

    configure_logging(verbose=args.verbose)

    cwd = Path.cwd()
    proj = load_akc_project_config(cwd)
    tenant_r = resolve_optional_project_string(
        cli_value=getattr(args, "tenant_id", None),
        env_key="AKC_TENANT_ID",
        file_value=proj.tenant_id if proj is not None else None,
        env=os.environ,
    )
    repo_r = resolve_optional_project_string(
        cli_value=getattr(args, "repo_id", None),
        env_key="AKC_REPO_ID",
        file_value=proj.repo_id if proj is not None else None,
        env=os.environ,
    )
    outputs_r = resolve_optional_project_string(
        cli_value=getattr(args, "outputs_root", None),
        env_key="AKC_OUTPUTS_ROOT",
        file_value=proj.outputs_root if proj is not None else None,
        env=os.environ,
    )
    if tenant_r.value is None:
        raise SystemExit(
            "Missing tenant id: provide --tenant-id, set AKC_TENANT_ID, or add tenant_id to .akc/project.json"
        )
    if repo_r.value is None:
        raise SystemExit("Missing repo id: provide --repo-id, set AKC_REPO_ID, or add repo_id to .akc/project.json")
    if outputs_r.value is None:
        raise SystemExit(
            "Missing outputs root: provide --outputs-root, set AKC_OUTPUTS_ROOT, "
            "or add outputs_root to .akc/project.json"
        )
    args.tenant_id = tenant_r.value
    args.repo_id = repo_r.value
    args.outputs_root = outputs_r.value

    opa_policy_r = resolve_optional_project_string(
        cli_value=getattr(args, "opa_policy_path", None),
        env_key="AKC_OPA_POLICY_PATH",
        file_value=proj.opa_policy_path if proj is not None else None,
        env=os.environ,
    )
    opa_decision_r = resolve_optional_project_string(
        cli_value=getattr(args, "opa_decision_path", None),
        env_key="AKC_OPA_DECISION_PATH",
        file_value=proj.opa_decision_path if proj is not None else None,
        env=os.environ,
    )
    opa_policy_effective: str | None = opa_policy_r.value
    opa_decision_effective: str = str(opa_decision_r.value or "data.akc.allow").strip() or "data.akc.allow"

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()
    outputs_root.mkdir(parents=True, exist_ok=True)

    # Keep memory and artifacts scoped under <outputs_root>/<tenant>/<repo>.
    base = outputs_root / scope.tenant_id / scope.repo_id
    governance_profile = resolve_governance_profile_for_scope(base)
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
    dev_role_resolved = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=cwd,
        env=os.environ,
        project=proj,
    )
    developer_role_profile = dev_role_resolved.value
    profile_resolved = resolve_compile_profile_defaults(
        profile=developer_role_profile,
        governance_profile=governance_profile,
        sandbox=str(getattr(args, "sandbox", "dev")),
        strong_lane_preference=str(getattr(args, "strong_lane_preference", "docker")),
        policy_mode=str(getattr(args, "policy_mode", "enforce")),
        replay_mode=str(getattr(args, "replay_mode", "live")),
        promotion_mode=getattr(args, "promotion_mode", None),
        stored_assertion_index=str(getattr(args, "stored_assertion_index", "off")),
    )
    sandbox_mode = str(profile_resolved["sandbox"].value)
    strong_lane_preference = str(profile_resolved["strong_lane_preference"].value)
    policy_mode_effective = str(profile_resolved["policy_mode"].value)
    replay_mode_effective = str(profile_resolved["replay_mode"].value)
    promotion_mode_explicit = cast(str | None, profile_resolved["promotion_mode"].value)
    stored_idx = str(profile_resolved["stored_assertion_index"].value)
    docker_available = _docker_cli_available()
    rust_exec_mode_raw = getattr(args, "rust_exec_mode", "cli")
    rust_exec_mode: BackendMode = "pyo3" if rust_exec_mode_raw == "pyo3" else "cli"
    rust_exec_lane_raw = getattr(args, "rust_exec_lane", "process")
    rust_exec_lane: ExecLane = "wasm" if rust_exec_lane_raw == "wasm" else "process"
    wasm_fs_normalize_existing_paths = bool(getattr(args, "wasm_fs_normalize_existing_paths", False))
    wasm_preopen_dirs = _parse_multi_flag_paths(getattr(args, "wasm_preopen_dir", []))
    wasm_allowed_write_dirs = _parse_multi_flag_paths(getattr(args, "wasm_allow_write_dir", []))
    wasm_fs_normalization_profile = str(getattr(args, "wasm_fs_normalization_profile", "strict")).strip()
    wasm_fs_normalization_strict = wasm_fs_normalization_profile != "relaxed"
    rust_available = _rust_exec_available(mode=rust_exec_mode)
    wasm_strict_profile = _strict_wasm_profile(
        policy_mode=policy_mode_effective,
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
                "set `--sandbox strong --strong-lane-preference wasm` or `--use-rust-exec --rust-exec-lane wasm`",
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
    if opa_policy_effective is not None and str(opa_policy_effective).strip() and not _opa_cli_available():
        return _emit_policy_preflight_failure(
            summary="configured OPA policy requires the `opa` CLI, but it is unavailable",
            details=(
                f"policy path: {opa_policy_effective}",
                "install the `opa` binary on PATH or unset policy path env / project file / --opa-policy-path",
            ),
        )
    docker_config_relevant = _docker_hardening_flags_supplied(args) or (
        not bool(getattr(args, "use_rust_exec", False))
        and sandbox_mode == "strong"
        and strong_lane_preference != "wasm"
    )
    if docker_config_relevant:
        (
            docker_user,
            docker_tmpfs_mounts,
            docker_seccomp_profile,
            docker_apparmor_profile,
            docker_ulimit_nofile,
            docker_ulimit_nproc,
        ) = _resolve_docker_hardening_args(args)
    else:
        docker_user = None
        docker_tmpfs_mounts = ()
        docker_seccomp_profile = None
        docker_apparmor_profile = None
        docker_ulimit_nofile = None
        docker_ulimit_nproc = None

    executor: Executor
    selected_backend = "dev-subprocess"

    if wasm_requested and not rust_available:
        remediation = (
            "Install the Rust WASM execution surface (`akc-exec` on PATH or `akc_rust` for `--rust-exec-mode pyo3`)."
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
    realization_mode = str(getattr(args, "compile_realization_mode", "scoped_apply")).strip()
    if realization_mode not in ("artifact_only", "scoped_apply"):
        print("--compile-realization-mode must be artifact_only or scoped_apply")
        return 2
    apply_scope_root: str | None = None
    if realization_mode == "scoped_apply":
        asr = getattr(args, "apply_scope_root", None)
        if asr is not None and str(asr).strip():
            apply_scope_root = str(Path(asr).expanduser().resolve())
        else:
            apply_scope_root = str(work_root.expanduser().resolve())
    compile_rm = cast(Literal["artifact_only", "scoped_apply"], realization_mode)
    config = _build_compile_config(
        mode=str(args.mode),
        policy_mode=policy_mode_effective,
        opa_policy_path=opa_policy_effective,
        opa_decision_path=opa_decision_effective,
        cost_input_per_1k_tokens_usd=float(getattr(args, "cost_input_per_1k_usd", 0.0)),
        cost_output_per_1k_tokens_usd=float(getattr(args, "cost_output_per_1k_usd", 0.0)),
        cost_tool_call_usd=float(getattr(args, "cost_tool_call_usd", 0.0)),
        compile_realization_mode=compile_rm,
        apply_scope_root=apply_scope_root,
    )
    config = _merge_compile_skills_from_sources(
        config=config,
        proj=proj,
        cli_skill_names=list(getattr(args, "compile_skill", None) or []),
        cli_mode=getattr(args, "compile_skills_mode", None),
        cli_extra_skill_roots=list(getattr(args, "compile_skill_extra_root", None) or []),
        cli_max_file_bytes=getattr(args, "compile_skill_max_file_bytes", None),
        cli_max_total_bytes=getattr(args, "compile_skill_max_total_bytes", None),
        project_dir=cwd,
    )
    promotion_mode = resolve_default_promotion_mode(
        explicit=promotion_mode_explicit,
        sandbox_mode=sandbox_mode,
    )
    _ = normalize_promotion_mode(promotion_mode)
    require_deployable = requires_deployable_steps(
        promotion_mode=promotion_mode,
        explicit=getattr(args, "require_deployable_steps", None),
    )
    md = dict(config.metadata or {})
    md["promotion_mode"] = promotion_mode
    md["require_deployable_steps"] = bool(require_deployable)
    config = replace(config, metadata=md)
    op_pol, graph_pol, ac_pol = resolve_compile_policies(
        cli_ir_operational=getattr(args, "ir_operational_structure_policy", None),
        cli_ir_graph=getattr(args, "ir_graph_integrity_policy", None),
        cli_artifact_consistency=getattr(args, "artifact_consistency_policy", None),
    )
    config = replace(
        config,
        ir_operational_structure_policy=op_pol,
        ir_graph_integrity_policy=graph_pol,
        artifact_consistency_policy=ac_pol,
    )
    if selected_backend == "docker":
        default_test_command: tuple[str, ...] | None = (
            "python",
            "-m",
            "pytest",
            "-q",
        )
    elif selected_backend.startswith("rust-") and selected_backend.endswith("-wasm"):
        default_test_command = None
    else:
        default_test_command = (sys.executable, "-m", "pytest", "-q")
    if default_test_command is not None:
        config = replace(config, test_command=default_test_command)
    if stored_idx not in ("off", "merge"):
        print("--stored-assertion-index must be off or merge")
        return 2
    config = replace(
        config,
        stored_assertion_index_mode=stored_idx,  # type: ignore[arg-type]
        stored_assertion_index_max_rows=int(getattr(args, "stored_assertion_index_max_rows", 64)),
        apply_operator_knowledge_decisions=not bool(getattr(args, "no_operator_knowledge_decisions", False)),
        runtime_bundle_embed_system_ir=bool(getattr(args, "runtime_bundle_embed_system_ir", False)),
    )
    if governance_profile is not None:
        config = config.with_governance_profile(
            assurance_mode=governance_profile.assurance_mode,
            verifier_enforcement=governance_profile.verifier_enforcement,
            provider_allowlist=governance_profile.provider_allowlist,
            max_errors_before_block=governance_profile.max_errors_before_block,
            rollout_stage=governance_profile.rollout_stage,
        )
    profile_decisions: dict[str, JSONValue] = {
        "developer_role_profile": developer_role_profile,
        "resolved": {
            "sandbox": {"value": sandbox_mode, "source": profile_resolved["sandbox"].source},
            "strong_lane_preference": {
                "value": strong_lane_preference,
                "source": profile_resolved["strong_lane_preference"].source,
            },
            "policy_mode": {"value": policy_mode_effective, "source": profile_resolved["policy_mode"].source},
            "replay_mode": {"value": replay_mode_effective, "source": profile_resolved["replay_mode"].source},
            "stored_assertion_index": {
                "value": stored_idx,
                "source": profile_resolved["stored_assertion_index"].source,
            },
            "promotion_mode_explicit": {
                "value": promotion_mode_explicit,
                "source": profile_resolved["promotion_mode"].source,
            },
            "intent_bootstrap_from_store": {
                "value": bool(profile_resolved["intent_bootstrap_from_store"].value),
                "source": profile_resolved["intent_bootstrap_from_store"].source,
            },
            "auto_seed_deployable_step": {
                "value": bool(profile_resolved["auto_seed_deployable_step"].value),
                "source": profile_resolved["auto_seed_deployable_step"].source,
            },
        },
        "sandbox_backend": selected_backend,
        "sandbox_memory_mb": sandbox_memory_mb,
        "sandbox_allow_network": allow_network,
        "fingerprint_sha256": "",
    }
    profile_decisions["fingerprint_sha256"] = stable_json_fingerprint(
        {k: v for k, v in profile_decisions.items() if k != "fingerprint_sha256"}
    )
    md2 = dict(config.metadata or {})
    md2["developer_role_profile"] = developer_role_profile
    md2["developer_profile_auto_seed_step"] = bool(profile_resolved["auto_seed_deployable_step"].value)
    md2["developer_profile_intent_bootstrap_from_store"] = bool(profile_resolved["intent_bootstrap_from_store"].value)
    config = replace(config, metadata=md2)
    config = configure_compile_mcp_from_cli(
        config,
        project_root=work_root.resolve(),
        compile_mcp=bool(getattr(args, "compile_mcp", False)),
        mcp_config=getattr(args, "compile_mcp_config", None),
        mcp_server=getattr(args, "compile_mcp_server", None),
        mcp_resources=list(getattr(args, "compile_mcp_resource", None) or []),
        mcp_tool_jsons=list(getattr(args, "compile_mcp_tool", None) or []),
        tools_generate_only=bool(getattr(args, "compile_mcp_tools_generate_only", False)),
    )
    llm = _OfflineLLM()

    goal = args.goal or "Compile repository"
    intent_file = getattr(args, "intent_file", None)
    partial_replay_passes: tuple[str, ...] | None = None
    raw_partial = getattr(args, "partial_replay_passes", None)
    if raw_partial is not None and str(raw_partial).strip():
        partial_replay_passes = tuple(p.strip() for p in str(raw_partial).split(",") if p.strip())
        invalid = [p for p in partial_replay_passes if p not in REPLAYABLE_PASSES]
        if invalid:
            print(f"Invalid --partial-replay-passes values: {invalid}. Allowed: {','.join(REPLAYABLE_PASSES)}")
            return 2
        if replay_mode_effective != "partial_replay":
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
    print(f"  compile_realization_mode: {realization_mode}")
    if apply_scope_root is not None:
        print(f"  apply_scope_root: {apply_scope_root}")
    print(f"  developer_role_profile: {developer_role_profile}")
    print(f"  promotion_mode: {promotion_mode} (require_deployable_steps={bool(require_deployable)})")
    if bool(getattr(args, "compile_mcp", False)):
        print(f"  compile_mcp: enabled (config={config.compile_mcp_config_path})")
        print(f"    resources: {list(config.compile_mcp_resource_uris)}")
        print(f"    tools: {[t.tool_name for t in config.compile_mcp_tools]}")
        print(f"    tool_stages: {list(config.compile_mcp_tool_stages)}")
    if (
        sandbox_mode == "strong"
        and strong_lane_preference == "wasm"
        and not bool(getattr(args, "use_rust_exec", False))
        and not rust_available
    ):
        print("  note: Rust execution surface unavailable for requested wasm strong lane")

    try:
        result = session.run(
            goal=goal,
            llm=llm,
            executor=executor,
            config=config,
            outputs_root=outputs_root,
            schema_version=int(getattr(args, "schema_version", 1)),
            replay_mode=cast(ReplayMode, replay_mode_effective),
            replay_manifest_path=getattr(args, "replay_manifest_path", None),
            partial_replay_passes=partial_replay_passes,
            intent_file=intent_file,
            developer_role_profile=developer_role_profile,
            developer_profile_decisions=profile_decisions,
            skills_project_root=work_root,
        )
    except IntentCompilerError as exc:
        print(f"Intent compilation failed: {exc}")
        return 2

    manifest_path = base / "manifest.json"
    print(f"  status: {result.status}")
    print(f"  manifest: {manifest_path}")

    if result.status == "succeeded":
        if not manifest_path.exists():
            print("WARNING: compile succeeded but manifest.json was not found")
        return 0

    print("Compile did not succeed within budget; see emitted artifacts for details.")
    _emit_compile_failure_details(base=base)
    fmt = str(getattr(args, "format", "text"))
    denial = compile_extract_policy_denial(
        result,
        scope_root=base,
        tenant_id=str(scope.tenant_id),
        repo_id=str(scope.repo_id),
        outputs_root=str(outputs_root.expanduser().resolve()),
        opa_policy_path=opa_policy_effective,
        opa_decision_path=opa_decision_effective,
    )
    if denial is not None:
        print_policy_denial(payload=denial, format_mode=fmt)
    return 2
