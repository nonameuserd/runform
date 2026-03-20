from __future__ import annotations

import argparse
import importlib
from pathlib import Path
from typing import Literal

from akc.compile.interfaces import LLMBackend
from akc.living.safe_recompile import safe_recompile_on_drift

from .common import configure_logging


def _parse_policy_mode(raw: object) -> Literal["audit_only", "enforce"]:
    v = str(raw or "enforce").strip()
    if v == "audit_only":
        return "audit_only"
    if v == "enforce":
        return "enforce"
    raise SystemExit(f"invalid --policy-mode: {raw!r} (expected audit_only|enforce)")


def _parse_canary_accept_mode(raw: object) -> Literal["quick", "thorough"]:
    v = str(raw or "").strip()
    if v == "quick":
        return "quick"
    if v == "thorough":
        return "thorough"
    raise SystemExit(f"invalid mode: {raw!r} (expected quick|thorough)")


def _parse_canary_test_mode(raw: object) -> Literal["smoke", "full"]:
    v = str(raw or "").strip()
    if v == "smoke":
        return "smoke"
    if v == "full":
        return "full"
    raise SystemExit(f"invalid --canary-test-mode: {raw!r} (expected smoke|full)")


def _load_llm_backend_class(*, class_path: str) -> LLMBackend:
    raw = str(class_path).strip()
    if not raw:
        raise ValueError("llm backend class path must be non-empty")
    if ":" in raw:
        module_name, class_name = raw.split(":", 1)
    else:
        module_name, _, class_name = raw.rpartition(".")
    module_name = module_name.strip()
    class_name = class_name.strip()
    if not module_name or not class_name:
        raise ValueError(
            "invalid llm backend class path; expected '<module>:<Class>' or '<module>.<Class>'"
        )
    mod = importlib.import_module(module_name)
    cls = getattr(mod, class_name, None)
    if cls is None:
        raise ValueError(f"llm backend class not found: {raw}")
    inst = cls()
    if not isinstance(inst, LLMBackend):
        raise ValueError(f"llm backend does not implement LLMBackend: {raw}")
    return inst


def cmd_living_recompile(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    llm_backend: LLMBackend | None = None
    llm_mode = str(getattr(args, "llm_mode", "offline"))
    if llm_mode == "custom":
        class_path = str(getattr(args, "llm_backend_class", "")).strip()
        if not class_path:
            raise SystemExit("--llm-backend-class is required when --llm-mode custom")
        try:
            llm_backend = _load_llm_backend_class(class_path=class_path)
        except Exception as e:
            raise SystemExit(f"failed to load custom llm backend: {e}") from e

    code = safe_recompile_on_drift(
        tenant_id=str(args.tenant_id),
        repo_id=str(args.repo_id),
        outputs_root=Path(str(args.outputs_root)),
        ingest_state_path=Path(str(args.ingest_state)),
        baseline_path=(
            Path(str(args.baseline_path)).expanduser()
            if getattr(args, "baseline_path", None) is not None
            else None
        ),
        eval_suite_path=Path(
            str(getattr(args, "eval_suite_path", "configs/evals/intent_system_v1.json"))
        ),
        goal=str(getattr(args, "goal", "Compile repository")),
        policy_mode=_parse_policy_mode(getattr(args, "policy_mode", "enforce")),
        canary_mode=_parse_canary_accept_mode(getattr(args, "canary_mode", "quick")),
        accept_mode=_parse_canary_accept_mode(getattr(args, "accept_mode", "thorough")),
        canary_test_mode=_parse_canary_test_mode(getattr(args, "canary_test_mode", "smoke")),
        allow_network=bool(getattr(args, "allow_network", False)),
        llm_backend=llm_backend,
        update_baseline_on_accept=bool(getattr(args, "update_baseline_on_accept", True)),
        skip_other_pending=bool(getattr(args, "skip_other_pending", True)),
        opa_policy_path=(
            str(args.opa_policy_path)
            if getattr(args, "opa_policy_path", None) is not None
            else None
        ),
        opa_decision_path=str(getattr(args, "opa_decision_path", "data.akc.allow")),
    )
    return int(code)
