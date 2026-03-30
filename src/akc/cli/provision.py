from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.utils.fingerprint import stable_json_fingerprint


def _project_dir(args: argparse.Namespace) -> Path:
    raw = getattr(args, "project_dir", None)
    return Path(raw).resolve() if raw else Path.cwd()


def _read_json(path: Path) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must decode to a JSON object")
    return raw


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _artifact_ref(path: Path, payload: dict[str, Any], *, project_dir: Path) -> dict[str, str]:
    return {
        "path": str(path.resolve().relative_to(project_dir.resolve())),
        "fingerprint": stable_json_fingerprint(payload),
    }


def _provision_root(project_dir: Path, provision_id: str) -> Path:
    return project_dir.resolve() / ".akc" / "provision" / provision_id


def _safe_id(value: str) -> str:
    out = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in str(value).strip())
    while "--" in out:
        out = out.replace("--", "-")
    return out.strip("-") or "provision"


def _resolve_infra_artifacts(*, project_dir: Path, run_id: str) -> tuple[dict[str, Any], Path, dict[str, Any], Path]:
    base = project_dir.resolve() / ".akc" / "infra"
    infra_path = base / f"{run_id}.infra_plan.json"
    iac_path = base / f"{run_id}.iac_manifest.json"
    if not infra_path.is_file():
        raise FileNotFoundError(f"infra plan not found: {infra_path}")
    if not iac_path.is_file():
        raise FileNotFoundError(f"iac manifest not found: {iac_path}")
    return _read_json(infra_path), infra_path, _read_json(iac_path), iac_path


def _backend_value(args: argparse.Namespace, iac_manifest: dict[str, Any]) -> str:
    raw = str(getattr(args, "backend", "") or "").strip()
    if raw:
        return raw
    preferred = str(iac_manifest.get("preferred_backend") or "terraform").strip()
    return preferred or "terraform"


def _environment_value(args: argparse.Namespace) -> str:
    raw = str(getattr(args, "environment", "staging") or "staging").strip().lower()
    return raw if raw in {"staging", "production"} else "staging"


def _desired_fingerprint(
    *,
    compile_run_id: str,
    backend: str,
    environment: str,
    infra_plan: dict[str, Any],
    iac_manifest: dict[str, Any],
) -> str:
    return stable_json_fingerprint(
        {
            "compile_run_id": compile_run_id,
            "backend": backend,
            "environment": environment,
            "infra_plan": infra_plan,
            "iac_manifest": iac_manifest,
        }
    )


def _tool_bin(*, backend: str) -> str | None:
    if backend == "terraform":
        raw = str(os.environ.get("AKC_TERRAFORM_BIN", "")).strip()
        return raw or shutil.which("terraform")
    raw = str(os.environ.get("AKC_CDK_BIN", "")).strip()
    return raw or shutil.which("cdk")


def _workspace_root(*, project_dir: Path, iac_manifest: dict[str, Any], backend: str) -> Path:
    ws = iac_manifest.get("workspaces")
    if not isinstance(ws, dict):
        raise ValueError("iac manifest workspaces must be an object")
    row = ws.get(backend)
    if not isinstance(row, dict):
        raise ValueError(f"iac manifest missing workspace for backend {backend!r}")
    root = row.get("workspace_root")
    if not isinstance(root, str) or not root.strip():
        raise ValueError(f"iac manifest workspace_root missing for backend {backend!r}")
    return project_dir.resolve() / root.strip()


def _command_rows(*, backend: str, tool_bin: str) -> list[list[str]]:
    if backend == "terraform":
        return [
            [tool_bin, "init", "-backend=false"],
            [tool_bin, "plan", "-refresh=false", "-lock=false", "-input=false"],
        ]
    return [
        [tool_bin, "synth"],
        [tool_bin, "diff", "--no-color"],
    ]


def _apply_rows(*, backend: str, tool_bin: str) -> list[list[str]]:
    if backend == "terraform":
        return [[tool_bin, "apply", "-auto-approve"]]
    return [[tool_bin, "deploy", "--require-approval", "never"]]


def _run_commands(*, commands: list[list[str]], cwd: Path) -> tuple[list[dict[str, Any]], bool]:
    rows: list[dict[str, Any]] = []
    ok = True
    for command in commands:
        proc = subprocess.run(command, cwd=str(cwd), capture_output=True, text=True, check=False)
        row = {
            "command": list(command),
            "exit_code": int(proc.returncode),
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
        rows.append(row)
        if proc.returncode != 0:
            ok = False
            break
    return rows, ok


def _status_from_preflight(*, issues: list[str], commands_ok: bool) -> str:
    if issues:
        return "blocked"
    return "ready" if commands_ok else "failed"


def _session_path(project_dir: Path, provision_id: str) -> Path:
    return _provision_root(project_dir, provision_id) / "session.json"


def _plan_path(project_dir: Path, provision_id: str) -> Path:
    return _provision_root(project_dir, provision_id) / "plan.json"


def _apply_path(project_dir: Path, provision_id: str) -> Path:
    return _provision_root(project_dir, provision_id) / "apply.json"


def _load_session(project_dir: Path, provision_id: str) -> dict[str, Any]:
    path = _session_path(project_dir, provision_id)
    if not path.is_file():
        raise FileNotFoundError(f"provision session not found: {path}")
    return _read_json(path)


def _latest_session_for_run(project_dir: Path, run_id: str) -> tuple[str, dict[str, Any]] | None:
    provision_root = project_dir.resolve() / ".akc" / "provision"
    if not provision_root.is_dir():
        return None
    latest: tuple[int, str, dict[str, Any]] | None = None
    for path in provision_root.glob("*/session.json"):
        payload = _read_json(path)
        if str(payload.get("compile_run_id") or "").strip() != run_id.strip():
            continue
        updated_raw = payload.get("updated_at_ms")
        updated_at = int(updated_raw) if isinstance(updated_raw, int) else 0
        provision_id = str(payload.get("provision_id") or path.parent.name).strip()
        if latest is None or updated_at >= latest[0]:
            latest = (updated_at, provision_id, payload)
    if latest is None:
        return None
    return latest[1], latest[2]


def cmd_provision_plan(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    run_id = str(getattr(args, "run_id", "") or "").strip()
    if not run_id:
        print("akc provision plan: --run-id is required", file=sys.stderr)
        return 2
    infra_plan, infra_path, iac_manifest, iac_path = _resolve_infra_artifacts(project_dir=project_dir, run_id=run_id)
    backend = _backend_value(args, iac_manifest)
    environment = _environment_value(args)
    provision_id = _safe_id(str(getattr(args, "provision_id", "") or f"{run_id}-{backend}-{environment}"))
    desired_fingerprint = _desired_fingerprint(
        compile_run_id=run_id,
        backend=backend,
        environment=environment,
        infra_plan=infra_plan,
        iac_manifest=iac_manifest,
    )
    issues: list[str] = []
    supported = iac_manifest.get("supported_backends")
    if not isinstance(supported, list) or backend not in [str(item) for item in supported]:
        issues.append(f"unsupported_backend:{backend}")
    readiness = infra_plan.get("provisioning_readiness")
    if isinstance(readiness, dict) and str(readiness.get("status") or "").strip() == "blocked":
        for item in cast(list[Any], readiness.get("provisioning_blockers") or []):
            sid = str(item).strip()
            if sid:
                issues.append(sid)
    tool_bin = _tool_bin(backend=backend)
    if tool_bin is None:
        issues.append(f"tool_unavailable:{backend}")
    workspace_root = _workspace_root(project_dir=project_dir, iac_manifest=iac_manifest, backend=backend)
    if not workspace_root.is_dir():
        issues.append(f"workspace_missing:{workspace_root}")
    commands: list[dict[str, Any]] = []
    commands_ok = False
    if not issues and tool_bin is not None:
        commands, commands_ok = _run_commands(
            commands=_command_rows(backend=backend, tool_bin=tool_bin), cwd=workspace_root
        )
    status = _status_from_preflight(issues=issues, commands_ok=commands_ok)
    now_ms = int(time.time() * 1000)
    plan_payload = apply_schema_envelope(
        obj={
            "provision_id": provision_id,
            "compile_run_id": run_id,
            "tenant_id": str(infra_plan.get("tenant_id") or ""),
            "repo_id": str(infra_plan.get("repo_id") or ""),
            "backend": backend,
            "environment": environment,
            "status": status,
            "desired_fingerprint": desired_fingerprint,
            "infra_plan_ref": _artifact_ref(infra_path, infra_plan, project_dir=project_dir),
            "iac_manifest_ref": _artifact_ref(iac_path, iac_manifest, project_dir=project_dir),
            "commands": commands,
            "preflight": {
                "issues": issues,
                "workspace_root": str(workspace_root.relative_to(project_dir.resolve()))
                if workspace_root.exists()
                else str(workspace_root),
                "tool_bin": tool_bin,
            },
            "created_at_ms": now_ms,
        },
        kind="provision_plan",
        version=1,
    )
    plan_path = _plan_path(project_dir, provision_id)
    _write_json(plan_path, plan_payload)
    session_payload = apply_schema_envelope(
        obj={
            "provision_id": provision_id,
            "compile_run_id": run_id,
            "tenant_id": str(infra_plan.get("tenant_id") or ""),
            "repo_id": str(infra_plan.get("repo_id") or ""),
            "backend": backend,
            "environment": environment,
            "status": "planned" if status == "ready" else status,
            "desired_fingerprint": desired_fingerprint,
            "infra_plan_ref": _artifact_ref(infra_path, infra_plan, project_dir=project_dir),
            "iac_manifest_ref": _artifact_ref(iac_path, iac_manifest, project_dir=project_dir),
            "latest_plan_ref": _artifact_ref(plan_path, plan_payload, project_dir=project_dir),
            "latest_apply_ref": None,
            "created_at_ms": now_ms,
            "updated_at_ms": now_ms,
        },
        kind="provision_session",
        version=1,
    )
    _write_json(_session_path(project_dir, provision_id), session_payload)
    print(
        json.dumps(
            {
                "ok": status == "ready",
                "provision_id": provision_id,
                "status": status,
                "plan_ref": session_payload["latest_plan_ref"],
                "issues": issues,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if status == "ready" else 2


def cmd_provision_apply(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    provision_id = str(getattr(args, "provision_id", "") or "").strip()
    if not provision_id:
        print("akc provision apply: --provision-id is required", file=sys.stderr)
        return 2
    session = _load_session(project_dir, provision_id)
    plan_payload = _read_json(_plan_path(project_dir, provision_id))
    compile_run_id = str(session.get("compile_run_id") or "").strip()
    infra_plan, infra_path, iac_manifest, iac_path = _resolve_infra_artifacts(
        project_dir=project_dir, run_id=compile_run_id
    )
    backend = str(session.get("backend") or "").strip() or str(plan_payload.get("backend") or "terraform")
    environment = str(session.get("environment") or "").strip() or str(plan_payload.get("environment") or "staging")
    desired_fingerprint = _desired_fingerprint(
        compile_run_id=compile_run_id,
        backend=backend,
        environment=environment,
        infra_plan=infra_plan,
        iac_manifest=iac_manifest,
    )
    issues: list[str] = []
    if str(plan_payload.get("status") or "").strip() != "ready":
        issues.append("plan_not_ready")
    if str(session.get("desired_fingerprint") or "").strip() != desired_fingerprint:
        issues.append("plan_fingerprint_mismatch")
    if environment == "production" and not bool(getattr(args, "approve_production", False)):
        issues.append("production_approval_required")
    tool_bin = _tool_bin(backend=backend)
    if tool_bin is None:
        issues.append(f"tool_unavailable:{backend}")
    workspace_root = _workspace_root(project_dir=project_dir, iac_manifest=iac_manifest, backend=backend)
    if not workspace_root.is_dir():
        issues.append(f"workspace_missing:{workspace_root}")
    commands: list[dict[str, Any]] = []
    commands_ok = False
    if not issues and tool_bin is not None:
        commands, commands_ok = _run_commands(
            commands=_apply_rows(backend=backend, tool_bin=tool_bin), cwd=workspace_root
        )
    status = "applied" if not issues and commands_ok else ("blocked" if issues else "failed")
    now_ms = int(time.time() * 1000)
    apply_payload = apply_schema_envelope(
        obj={
            "provision_id": provision_id,
            "compile_run_id": compile_run_id,
            "backend": backend,
            "environment": environment,
            "status": status,
            "desired_fingerprint": desired_fingerprint,
            "commands": commands,
            "created_at_ms": now_ms,
            "issues": issues,
        },
        kind="provision_apply",
        version=1,
    )
    apply_path = _apply_path(project_dir, provision_id)
    _write_json(apply_path, apply_payload)
    session_payload = apply_schema_envelope(
        obj={
            "provision_id": provision_id,
            "compile_run_id": compile_run_id,
            "tenant_id": str(infra_plan.get("tenant_id") or ""),
            "repo_id": str(infra_plan.get("repo_id") or ""),
            "backend": backend,
            "environment": environment,
            "status": status,
            "desired_fingerprint": desired_fingerprint,
            "infra_plan_ref": _artifact_ref(infra_path, infra_plan, project_dir=project_dir),
            "iac_manifest_ref": _artifact_ref(iac_path, iac_manifest, project_dir=project_dir),
            "latest_plan_ref": session.get("latest_plan_ref"),
            "latest_apply_ref": _artifact_ref(apply_path, apply_payload, project_dir=project_dir),
            "created_at_ms": int(session.get("created_at_ms") or now_ms),
            "updated_at_ms": now_ms,
        },
        kind="provision_session",
        version=1,
    )
    _write_json(_session_path(project_dir, provision_id), session_payload)
    print(
        json.dumps(
            {
                "ok": status == "applied",
                "provision_id": provision_id,
                "status": status,
                "apply_ref": session_payload["latest_apply_ref"],
                "issues": issues,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if status == "applied" else 2


def cmd_provision_status(args: argparse.Namespace) -> int:
    project_dir = _project_dir(args)
    provision_id = str(getattr(args, "provision_id", "") or "").strip()
    run_id = str(getattr(args, "run_id", "") or "").strip()
    if not provision_id:
        if not run_id:
            print("akc provision status: --provision-id or --run-id is required", file=sys.stderr)
            return 2
        latest = _latest_session_for_run(project_dir, run_id)
        if latest is None:
            print(f"akc provision status: no provision sessions found for run_id={run_id}", file=sys.stderr)
            return 2
        provision_id, session = latest
    else:
        session = _load_session(project_dir, provision_id)
    payload: dict[str, Any] = {"provision_id": provision_id, "session": session}
    plan_path = _plan_path(project_dir, provision_id)
    if plan_path.is_file():
        payload["plan"] = _read_json(plan_path)
    apply_path = _apply_path(project_dir, provision_id)
    if apply_path.is_file():
        payload["apply"] = _read_json(apply_path)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


def register_provision_parsers(sub: Any) -> None:
    provision = sub.add_parser(
        "provision",
        help="Provision synthesized cloud infrastructure from AKC .akc/infra artifacts",
    )
    provision.add_argument(
        "--project-dir",
        type=Path,
        default=None,
        help="Project root containing .akc/ (default: current working directory)",
    )
    provision_sub = provision.add_subparsers(dest="provision_command", required=True)

    plan = provision_sub.add_parser("plan", help="Validate IaC prerequisites and run plan/synth evidence")
    plan.add_argument("--run-id", required=True, help="Compile run id whose .akc/infra artifacts should be used")
    plan.add_argument("--backend", choices=["terraform", "aws_cdk"], default=None)
    plan.add_argument("--environment", choices=["staging", "production"], default="staging")
    plan.add_argument("--provision-id", default=None, help="Optional stable provision session id override")
    plan.set_defaults(func=cmd_provision_plan)

    apply = provision_sub.add_parser("apply", help="Apply a previously planned provision session")
    apply.add_argument("--provision-id", required=True)
    apply.add_argument(
        "--approve-production",
        action="store_true",
        help="Required when applying a production provision session",
    )
    apply.set_defaults(func=cmd_provision_apply)

    status = provision_sub.add_parser("status", help="Show provision session and latest plan/apply artifacts")
    status.add_argument("--provision-id", default=None)
    status.add_argument("--run-id", default=None)
    status.set_defaults(func=cmd_provision_status)
