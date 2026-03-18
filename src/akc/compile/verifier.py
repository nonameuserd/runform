"""Phase 5: Agentic verifier (ProofWright-style) gate.

This module is intentionally deterministic and dependency-free. It provides
policy-driven checks that can veto promotion of a candidate even after tests pass.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, Protocol, runtime_checkable

from akc.compile.interfaces import ExecutionResult, TenantRepoScope
from akc.memory.models import JSONValue, now_ms, require_non_empty

VerifierSeverity = Literal["error", "warning"]


@dataclass(frozen=True, slots=True)
class VerifierFinding:
    code: str
    message: str
    severity: VerifierSeverity = "error"
    evidence: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.code, name="finding.code")
        require_non_empty(self.message, name="finding.message")
        if self.severity not in ("error", "warning"):
            raise ValueError("finding.severity must be 'error' or 'warning'")

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "code": self.code,
            "message": self.message,
            "severity": self.severity,
            "evidence": dict(self.evidence) if self.evidence else None,
        }
        return {k: v for k, v in obj.items() if v is not None}


@dataclass(frozen=True, slots=True)
class VerifierResult:
    scope: TenantRepoScope
    plan_id: str
    step_id: str
    passed: bool
    findings: tuple[VerifierFinding, ...]
    checked_at_ms: int
    policy: Mapping[str, JSONValue]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "scope": {"tenant_id": self.scope.tenant_id, "repo_id": self.scope.repo_id},
            "plan_id": self.plan_id,
            "step_id": self.step_id,
            "passed": bool(self.passed),
            "checked_at_ms": int(self.checked_at_ms),
            "policy": dict(self.policy),
            "findings": [f.to_json_obj() for f in self.findings],
        }


@dataclass(frozen=True, slots=True)
class VerifierPolicy:
    """Policy knobs for the deterministic verifier gate."""

    enabled: bool = True
    # If strict, warnings also veto promotion.
    strict: bool = True
    check_patch_format: bool = True
    check_paths: bool = True
    check_budget_accounting: bool = True

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "enabled": bool(self.enabled),
            "strict": bool(self.strict),
            "check_patch_format": bool(self.check_patch_format),
            "check_paths": bool(self.check_paths),
            "check_budget_accounting": bool(self.check_budget_accounting),
        }


@runtime_checkable
class Verifier(Protocol):
    def verify(
        self,
        *,
        scope: TenantRepoScope,
        plan_id: str,
        step_id: str,
        candidate_patch: str,
        execution: ExecutionResult | None,
        accounting: Mapping[str, Any],
        budget: Any,
        policy: VerifierPolicy,
    ) -> VerifierResult:
        """Return a structured verification result for gating promotion."""


def _extract_patch_paths(patch_text: str) -> list[str]:
    paths: set[str] = set()
    for raw in (patch_text or "").splitlines():
        line = raw.strip()
        if line.startswith("+++ "):
            p = line[4:].strip()
            if p.startswith("b/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
        elif line.startswith("--- "):
            p = line[4:].strip()
            if p.startswith("a/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
    return sorted(paths)


def _is_path_suspicious(p: str) -> tuple[bool, str]:
    p2 = str(p or "").strip()
    if p2 == "":
        return True, "empty"
    if p2.startswith(("/", "\\")):
        return True, "absolute"
    if p2.startswith("~"):
        return True, "tilde"
    if ":" in p2.split("/", 1)[0]:
        # "C:\..." or "C:foo" style drive-prefix.
        return True, "drive_prefix"
    # Reject traversal segments.
    parts = [seg for seg in p2.replace("\\", "/").split("/") if seg not in ("", ".")]
    if any(seg == ".." for seg in parts):
        return True, "traversal"
    return False, ""


@dataclass(frozen=True, slots=True)
class DeterministicVerifier(Verifier):
    """Default production verifier: deterministic policy checks only."""

    def verify(
        self,
        *,
        scope: TenantRepoScope,
        plan_id: str,
        step_id: str,
        candidate_patch: str,
        execution: ExecutionResult | None,
        accounting: Mapping[str, Any],
        budget: Any,
        policy: VerifierPolicy,
    ) -> VerifierResult:
        require_non_empty(plan_id, name="plan_id")
        require_non_empty(step_id, name="step_id")

        findings: list[VerifierFinding] = []

        if not policy.enabled:
            return VerifierResult(
                scope=scope,
                plan_id=plan_id,
                step_id=step_id,
                passed=True,
                findings=(),
                checked_at_ms=now_ms(),
                policy=policy.to_json_obj(),
            )

        patch = candidate_patch or ""
        if policy.check_patch_format:
            has_file_header = ("--- " in patch) and ("+++ " in patch)
            if not has_file_header:
                findings.append(
                    VerifierFinding(
                        code="patch.missing_file_headers",
                        message="candidate patch is not a unified diff with file headers",
                        evidence={"hint": "expected lines starting with '--- ' and '+++ '"},
                    )
                )
            if "\x00" in patch:
                findings.append(
                    VerifierFinding(
                        code="patch.contains_nul",
                        message="candidate patch contains NUL byte(s)",
                        evidence=None,
                    )
                )

        touched = _extract_patch_paths(patch)
        if policy.check_paths:
            if not touched:
                findings.append(
                    VerifierFinding(
                        code="patch.no_paths",
                        message="candidate patch did not declare any touched file paths",
                        evidence=None,
                    )
                )
            for p in touched:
                suspicious, reason = _is_path_suspicious(p)
                if suspicious:
                    findings.append(
                        VerifierFinding(
                            code="patch.path_suspicious",
                            message=f"candidate patch path rejected: {p}",
                            evidence={"path": p, "reason": reason},
                        )
                    )
                if p.startswith(".akc/") or p == ".akc":
                    findings.append(
                        VerifierFinding(
                            code="patch.touches_internal_artifacts",
                            message="candidate patch must not modify .akc/ emitted artifacts",
                            evidence={"path": p},
                            severity="warning",
                        )
                    )

        if policy.check_budget_accounting:
            # Guard against controller regressions: verifier should be able to show
            # why a promotion is disallowed when budget would be exceeded.
            try:
                llm_calls = int(accounting.get("llm_calls", 0))
                max_llm_calls = int(budget.max_llm_calls)
                if llm_calls > max_llm_calls:
                    findings.append(
                        VerifierFinding(
                            code="budget.llm_calls_exceeded",
                            message="accounting exceeds configured max_llm_calls",
                            evidence={"llm_calls": llm_calls, "max_llm_calls": max_llm_calls},
                        )
                    )
            except Exception:  # pragma: no cover
                findings.append(
                    VerifierFinding(
                        code="budget.accounting_unparseable",
                        message="could not validate budget accounting fields",
                        severity="warning",
                        evidence={"keys": list(accounting.keys())},
                    )
                )

        # If strict: any finding vetoes. If relaxed: only errors veto.
        if policy.strict:
            passed = len(findings) == 0
        else:
            passed = all(f.severity != "error" for f in findings)

        # Keep a minimal execution sanity check signal.
        if execution is None:
            findings.append(
                VerifierFinding(
                    code="execution.missing",
                    message="no execution result present for promotable candidate",
                    severity="warning",
                    evidence=None,
                )
            )
            if policy.strict:
                passed = False

        return VerifierResult(
            scope=scope,
            plan_id=plan_id,
            step_id=step_id,
            passed=passed,
            findings=tuple(findings),
            checked_at_ms=now_ms(),
            policy=policy.to_json_obj(),
        )
