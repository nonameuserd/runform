from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from akc.compile.interfaces import TenantRepoScope
from .common import configure_logging


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
    return False, f"{name}: failed but ignored in relaxed mode (exit_code={proc.returncode})\n{summary}"


def cmd_verify(args: argparse.Namespace) -> int:
    """Verify emitted artifacts: tests + verifier + optional formal checks."""

    configure_logging(verbose=args.verbose)

    scope = TenantRepoScope(tenant_id=args.tenant_id, repo_id=args.repo_id)
    outputs_root = Path(args.outputs_root).expanduser()
    strict = args.mode == "strict"

    base = outputs_root / scope.tenant_id / scope.repo_id
    tests_dir = base / ".akc" / "tests"
    verif_dir = base / ".akc" / "verification"

    any_fail = False

    # Summarize test artifacts.
    print(f"Verifying artifacts for scope={scope.tenant_id}/{scope.repo_id}")
    print(f"  outputs_root: {outputs_root}")
    if not base.exists():
        print(f"ERROR: outputs directory not found: {base}")
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
        ver_files = sorted(verif_dir.glob("*.json"))
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

    # Optional formal tools.
    formal_messages: list[str] = []
    if args.dafny:
        dfy_path = Path("formal/dafny/budget_policy.dfy")
        if not dfy_path.exists():
            msg = (
                "dafny: formal/dafny/budget_policy.dfy not found "
                f"(skipping{' with failure' if strict else ''})"
            )
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
            msg = (
                "verus: formal/verus/Cargo.toml not found "
                f"(skipping{' with failure' if strict else ''})"
            )
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

