from __future__ import annotations

import argparse
from pathlib import Path

from akc.compile import (
    Budget,
    CompileSession,
    ControllerConfig,
    RustExecutor,
    SubprocessExecutor,
    TierConfig,
)
from akc.compile.controller_config import TestMode, TierName
from akc.compile.interfaces import Executor, LLMBackend, LLMRequest, LLMResponse, TenantRepoScope
from akc.compile.rust_bridge import BackendMode, ExecLane, RustExecConfig

from .common import configure_logging


class _OfflineLLM(LLMBackend):
    """Deterministic, offline LLM backend for the CLI.

    This backend never calls external services. It produces a minimal, valid
    unified diff that touches both code and tests so the controller's
    tests-by-default and policy logic remain exercised.
    """

    def complete(self, *, scope: TenantRepoScope, stage: str, request: LLMRequest) -> LLMResponse:
        # `request` is part of the LLMBackend interface but the offline backend
        # intentionally ignores it.
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


def _build_compile_config(*, mode: str) -> ControllerConfig:
    """Construct a ControllerConfig preset for CLI compile.

    - quick: conservative budget, smoke+periodic full tests.
    - thorough: larger budget, full tests every iteration.
    """

    tiers: dict[TierName, TierConfig] = {
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
    executor: Executor
    if bool(getattr(args, "use_rust_exec", False)):
        rust_exec_mode_raw = getattr(args, "rust_exec_mode", "cli")
        rust_exec_mode: BackendMode = "pyo3" if rust_exec_mode_raw == "pyo3" else "cli"
        rust_exec_lane_raw = getattr(args, "rust_exec_lane", "process")
        rust_exec_lane: ExecLane = "wasm" if rust_exec_lane_raw == "wasm" else "process"
        rust_cfg = RustExecConfig(
            mode=rust_exec_mode,
            lane=rust_exec_lane,
            allow_network=bool(getattr(args, "rust_allow_network", False)),
        )
        executor = RustExecutor(rust_cfg=rust_cfg, work_root=work_root)
    else:
        executor = SubprocessExecutor(work_root=work_root)
    config = _build_compile_config(mode=str(args.mode))
    llm = _OfflineLLM()

    goal = args.goal or "Compile repository"

    print(f"Running compile for scope={scope.tenant_id}/{scope.repo_id}")
    print(f"  goal: {goal}")
    print(f"  outputs_root: {outputs_root}")
    print(f"  work_root: {work_root}")

    result = session.run(
        goal=goal,
        llm=llm,
        executor=executor,
        config=config,
        outputs_root=outputs_root,
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
