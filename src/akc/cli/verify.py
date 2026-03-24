from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, cast

from akc.compile.interfaces import TenantRepoScope
from akc.compile.operational_verify import verify_run_operational_coupling
from akc.control.operator_workflows import resolve_run_manifest_path
from akc.control.policy_bundle import resolve_governance_profile_for_scope
from akc.memory.models import JSONValue
from akc.run.manifest import RunManifest
from akc.runtime.providers.factory import (
    external_deployment_providers_enabled,
    mutating_deployment_providers_enabled,
)

from .common import configure_logging
from .living_doctor import run_living_unattended_checks
from .profile_defaults import resolve_developer_role_profile, resolve_optional_project_string
from .project_config import load_akc_project_config


def _run_formal_command(
    *,
    argv: list[str],
    cwd: Path | None,
    name: str,
    strict: bool,
) -> tuple[bool, str]:
    """Run a formal tool command, returning (ok, summary)."""

    try:
        proc = subprocess.run(
            argv,
            cwd=str(cwd) if cwd is not None else None,
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        msg = f"{name}: executable not found (skipping{' with failure' if strict else ''})"
        return (not strict, msg)
    except Exception as exc:  # pragma: no cover - defensive
        msg = f"{name}: failed to run command: {exc!r}"
        return (False, msg if strict else msg + " (ignored)")

    ok = proc.returncode == 0
    if ok:
        return True, f"{name}: ok (exit_code=0)"

    summary = proc.stderr.strip() or proc.stdout.strip() or f"{name}: failed"
    if strict:
        return False, f"{name}: FAILED (exit_code={proc.returncode})\n{summary}"
    return (
        False,
        f"{name}: failed but ignored in relaxed mode (exit_code={proc.returncode})\n{summary}",
    )


def cmd_verify(args: argparse.Namespace) -> int:
    """Verify emitted artifacts: tests + verifier + optional formal checks."""

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

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()
    strict = args.mode == "strict"
    dev_res = resolve_developer_role_profile(
        cli_value=getattr(args, "developer_role_profile", None),
        cwd=cwd,
        env=os.environ,
        project=proj,
    )

    base = outputs_root / scope.tenant_id / scope.repo_id
    tests_dir = base / ".akc" / "tests"
    verif_dir = base / ".akc" / "verification"

    def _latest_run_id(scope_base: Path) -> str | None:
        run_dir = scope_base / ".akc" / "run"
        if not run_dir.is_dir():
            return None
        rows = sorted(run_dir.glob("*.manifest.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not rows:
            return None
        name = rows[0].name
        if name.endswith(".manifest.json"):
            return name[: -len(".manifest.json")]
        return None

    run_id_raw = getattr(args, "run_id", None)
    run_id_effective: str | None = (
        str(run_id_raw).strip() if isinstance(run_id_raw, str) and run_id_raw.strip() else None
    )
    if not bool(getattr(args, "no_operational_coupling", False)) and run_id_effective is None:
        run_id_effective = _latest_run_id(base) if base.exists() else None

    manifest_path_resolved: str | None = None
    if run_id_effective:
        try:
            mp = resolve_run_manifest_path(
                manifest_path=None,
                outputs_root=outputs_root,
                tenant_id=scope.tenant_id,
                repo_id=scope.repo_id,
                run_id=run_id_effective,
            )
            if mp.is_file():
                manifest_path_resolved = str(mp.resolve())
        except Exception:
            manifest_path_resolved = None

    any_fail = False

    # Summarize test artifacts.
    print(f"Verifying artifacts for scope={scope.tenant_id}/{scope.repo_id}")
    print(f"  tenant_id: {scope.tenant_id} (source: {tenant_r.source})")
    print(f"  repo_id: {scope.repo_id} (source: {repo_r.source})")
    print(f"  outputs_root: {outputs_root} (source: {outputs_r.source})")
    print(f"  developer_role_profile: {dev_res.value} (source: {dev_res.source})")
    print(f"  manifest_path: {manifest_path_resolved or '(none resolved)'}")
    print("Preflight (gating hints):")
    if base.exists():
        gp = resolve_governance_profile_for_scope(base)
        if gp is not None:
            stage = gp.rollout_stage or "(unset)"
            print(
                f"  governance_profile: assurance_mode={gp.assurance_mode} "
                f"verifier_enforcement={gp.verifier_enforcement} rollout_stage={stage}"
            )
        else:
            print("  governance_profile: (no policy bundle at scope)")
    else:
        print("  governance_profile: (scope directory not present yet)")
    print(
        "  deployment_providers: "
        f"external={external_deployment_providers_enabled()} "
        f"mutating={mutating_deployment_providers_enabled()} "
        "(AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER / AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER == 1)"
    )
    exec_allow = os.environ.get("AKC_EXEC_ALLOWLIST")
    print(f"  AKC_EXEC_ALLOWLIST: {'set' if exec_allow else 'unset'}")
    if base.exists():
        verif_dir.mkdir(parents=True, exist_ok=True)
        sidecar = verif_dir / "verify_developer_context.v1.json"
        sidecar.write_text(
            json.dumps(
                {
                    "schema_kind": "akc_verify_developer_context",
                    "version": 1,
                    "developer_role_profile": dev_res.value,
                    "developer_role_profile_resolution_source": dev_res.source,
                    "tenant_id": scope.tenant_id,
                    "tenant_id_resolution_source": tenant_r.source,
                    "repo_id": scope.repo_id,
                    "repo_id_resolution_source": repo_r.source,
                    "outputs_root": str(outputs_root.resolve()),
                    "outputs_root_resolution_source": outputs_r.source,
                    "run_id": run_id_effective,
                    "manifest_path": manifest_path_resolved,
                },
                indent=2,
                sort_keys=True,
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        try:
            rel_sc = sidecar.resolve().relative_to(base.resolve())
            print(f"  verify_context_sidecar: {rel_sc.as_posix()}")
        except ValueError:
            print(f"  verify_context_sidecar: {sidecar}")
    if not base.exists():
        print(f"ERROR: outputs directory not found: {base}")
        return 2

    if bool(getattr(args, "living_unattended", False)):
        lu_ok, lu_lines = run_living_unattended_checks(
            cwd=cwd,
            env=os.environ,
            project=proj,
            tenant_id=scope.tenant_id,
            repo_id=scope.repo_id,
            outputs_root=outputs_root,
            eval_suite_path=None,
            ingest_state_path=None,
            relaxed_baseline=bool(getattr(args, "living_unattended_relaxed_baseline", False)),
            living_automation_profile_cli=getattr(args, "living_automation_profile", None),
            lease_backend=getattr(args, "lease_backend", None),
            lease_namespace=getattr(args, "lease_namespace", None),
            expect_replicas=getattr(args, "expect_replicas", None),
        )
        print("Living unattended checks (verify):")
        for line in lu_lines:
            print(f"  {line}")
        if not lu_ok:
            print("Verification failed (living unattended).", file=sys.stderr)
            return 2

    if tests_dir.is_dir():
        test_files = sorted(tests_dir.glob("*.json"))
        print(f"  found {len(test_files)} test artifact(s) under .akc/tests")
        for p in test_files:
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception as exc:  # pragma: no cover - defensive
                print(f"  - {p.name}: unreadable JSON ({exc!r})")
                any_fail = True
                continue
            exit_code = int(data.get("exit_code", 0))
            stage = str(data.get("stage") or "unknown")
            cmd = " ".join(str(c) for c in (data.get("command") or []))
            print(f"  - {p.name}: stage={stage} exit_code={exit_code} command={cmd}")
            if exit_code != 0:
                any_fail = True
    else:
        print("  note: no .akc/tests directory found (skipping test artifact checks)")

    # Summarize verifier results.
    if verif_dir.is_dir():
        ver_files = sorted(p for p in verif_dir.glob("*.json") if p.name != "verify_developer_context.v1.json")
        print(f"  found {len(ver_files)} verification result(s) under .akc/verification")
        for p in ver_files:
            try:
                data = json.loads(p.read_text(encoding="utf-8"))
            except Exception as exc:  # pragma: no cover - defensive
                print(f"  - {p.name}: unreadable JSON ({exc!r})")
                any_fail = True
                continue
            passed = bool(data.get("passed"))
            findings = data.get("findings") or []
            print(f"  - {p.name}: passed={passed} findings={len(findings)}")
            if not passed:
                any_fail = True
                if args.show_findings:
                    for f in findings:
                        code = f.get("code")
                        msg = f.get("message")
                        sev = f.get("severity", "error")
                        print(f"      [{sev}] {code}: {msg}")
    else:
        print("  note: no .akc/verification directory found (skipping verifier checks)")

    def _json_sha256(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def _write_operational_assurance_sidecar(*, run_id: str, op: object) -> None:
        op_obj = cast(Any, op)
        rel = getattr(op_obj, "assurance_rel_path", None)
        payload = getattr(op_obj, "assurance_result", None)
        if not isinstance(rel, str) or not rel.strip() or not isinstance(payload, dict):
            return
        out_path = (base / rel).resolve()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        rel_norm = str(Path(rel).as_posix())
        pointer = {"path": rel_norm, "sha256": _json_sha256(out_path)}
        try:
            mpath = resolve_run_manifest_path(
                manifest_path=None,
                outputs_root=outputs_root,
                tenant_id=scope.tenant_id,
                repo_id=scope.repo_id,
                run_id=run_id,
            )
        except Exception:
            return
        if not mpath.is_file():
            return
        manifest = RunManifest.from_json_file(mpath)
        cp: dict[str, JSONValue] = dict(manifest.control_plane) if isinstance(manifest.control_plane, dict) else {}
        cp["operational_assurance_ref"] = cast(JSONValue, pointer)
        cp["operational_assurance_passed"] = bool(getattr(op_obj, "passed", False))
        cp["operational_assurance_enforcement_mode"] = str(getattr(op_obj, "enforcement_mode", "blocking"))
        scope_root = base.resolve()
        gp_rel = ".akc/control/governance_profile.resolved.json"
        gp_path = (scope_root / gp_rel).resolve()
        if gp_path.is_file():
            cp["governance_profile_ref"] = {"path": gp_rel, "sha256": _json_sha256(gp_path)}
        manifest_obj = manifest.to_json_obj()
        manifest_obj["control_plane"] = cp
        mpath.write_text(
            json.dumps(manifest_obj, sort_keys=True, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    if run_id_effective:
        try:
            op_result = verify_run_operational_coupling(
                outputs_root=outputs_root,
                scope=scope,
                run_id=run_id_effective,
                strict_manifest=strict,
            )
        except Exception as exc:
            print(
                f"Operational verification for run_id={run_id_effective}: FAILED\n  {exc}",
                file=sys.stderr,
            )
            return 2

        _write_operational_assurance_sidecar(run_id=run_id_effective, op=op_result)
        print(f"Operational verification for run_id={op_result.run_id}:")
        print(f"  authority: {op_result.authority}")
        print(f"  enforcement_mode: {op_result.enforcement_mode}")
        print(f"  report_present: {'yes' if op_result.had_report else 'no'}")
        if op_result.recomputed_attestation_fingerprint_sha256:
            print(f"  recomputed_attestation_sha256: {op_result.recomputed_attestation_fingerprint_sha256}")
        if op_result.stored_attestation_fingerprint_sha256:
            print(f"  stored_attestation_sha256: {op_result.stored_attestation_fingerprint_sha256}")
        if strict:
            status = "checked" if op_result.checked_manifest_strict else "skipped"
            print(f"  strict_manifest_consistency: {status}")
        print(f"  passed: {op_result.passed}")
        if op_result.advisory_only and not op_result.passed:
            print("  advisory_outcome: findings present but non-blocking under governance policy")
        if args.show_findings and op_result.findings:
            for finding in op_result.findings:
                print(f"    [{finding.severity}] {finding.code}: {finding.message}")
        if not op_result.blocking_passed:
            any_fail = True
    elif not bool(getattr(args, "no_operational_coupling", False)):
        print("  note: no run manifest found for operational coupling (skipping)")

    # Optional formal tools.
    formal_messages: list[str] = []
    if args.dafny:
        dfy_path = Path("formal/dafny/budget_policy.dfy")
        if not dfy_path.exists():
            msg = f"dafny: formal/dafny/budget_policy.dfy not found (skipping{' with failure' if strict else ''})"
            formal_messages.append(msg)
            if strict:
                any_fail = True
        else:
            ok, msg = _run_formal_command(
                argv=["dafny", "verify", str(dfy_path)],
                cwd=None,
                name="dafny",
                strict=strict,
            )
            formal_messages.append(msg)
            if not ok and strict:
                any_fail = True

    if args.verus:
        verus_root = Path("formal/verus")
        cargo_toml = verus_root / "Cargo.toml"
        if not cargo_toml.exists():
            msg = f"verus: formal/verus/Cargo.toml not found (skipping{' with failure' if strict else ''})"
            formal_messages.append(msg)
            if strict:
                any_fail = True
        else:
            ok, msg = _run_formal_command(
                argv=["cargo", "verus"],
                cwd=verus_root,
                name="verus",
                strict=strict,
            )
            formal_messages.append(msg)
            if not ok and strict:
                any_fail = True

    if formal_messages:
        print("Formal checks:")
        for line in formal_messages:
            print(f"  - {line}")

    if any_fail:
        print("Verification failed.")
        return 2

    print("Verification passed.")
    return 0
