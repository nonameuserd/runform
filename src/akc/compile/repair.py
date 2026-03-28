"""Repair policy for the Phase 3 compile loop (test-driven).

This module is intentionally dependency-free and focuses on:
- parsing execution/test failures into a stable, JSON-serializable summary
- producing a deterministic repair prompt that requests patch-shaped output

The controller is responsible for enforcing budgets and tier policy.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from akc.compile.interfaces import ExecutionResult
from akc.compile.ir_prompt_context import (
    format_active_objectives_for_prompt,
    format_linked_constraints_for_prompt,
    format_success_criteria_for_prompt,
)
from akc.memory.models import JSONValue, require_non_empty

_PYTEST_FAIL_LINE_RE = re.compile(r"^FAILED\s+(?P<test>.+?)\s+-\s+(?P<reason>.+?)\s*$")
_PYTEST_SHORT_SUMMARY_HEADER_RE = re.compile(
    r"^=+\s*short test summary info\s*=+\s*$",
    re.IGNORECASE,
)
_TRACEBACK_HEADER_RE = re.compile(r"^=+\s*FAILURES\s*=+\s*$|^Traceback \(most recent call last\):\s*$")


@dataclass(frozen=True, slots=True)
class FailureSummary:
    """A conservative summary of an execution failure."""

    exit_code: int
    failing_tests: tuple[str, ...] = ()
    reasons: tuple[str, ...] = ()
    traceback_excerpt: str | None = None
    raw_tail: str | None = None

    def to_json_obj(self) -> dict[str, JSONValue]:
        obj: dict[str, JSONValue] = {
            "exit_code": int(self.exit_code),
            "failing_tests": list(self.failing_tests),
            "reasons": list(self.reasons),
            "traceback_excerpt": self.traceback_excerpt,
            "raw_tail": self.raw_tail,
        }
        return obj


def parse_execution_failure(*, result: ExecutionResult, max_lines: int = 80) -> FailureSummary:
    """Parse an execution failure (best-effort).

    This currently focuses on pytest output patterns, but always returns a stable summary.
    """

    max_lines_i = int(max_lines)
    if max_lines_i <= 0:
        raise ValueError("max_lines must be > 0")

    combined = (result.stdout or "") + ("\n" + (result.stderr or "") if (result.stderr or "") else "")
    lines = combined.splitlines()

    # Scan for pytest "short test summary info" section.
    failing: list[str] = []
    reasons: list[str] = []
    in_short_summary = False
    for ln in lines:
        if _PYTEST_SHORT_SUMMARY_HEADER_RE.match(ln.strip()):
            in_short_summary = True
            continue
        if in_short_summary:
            if ln.strip().startswith("="):  # end banner / next section
                # allow a single banner line after the header, then stop on next banner
                continue
            m = _PYTEST_FAIL_LINE_RE.match(ln.strip())
            if m:
                failing.append(m.group("test").strip())
                reasons.append(m.group("reason").strip())

    # Traceback excerpt: find a failures/traceback header near the end and take a slice.
    traceback_excerpt: str | None = None
    header_idx: int | None = None
    for i in range(len(lines) - 1, -1, -1):
        if _TRACEBACK_HEADER_RE.match(lines[i].strip()):
            header_idx = i
            break
    if header_idx is not None:
        excerpt = "\n".join(lines[header_idx : header_idx + max_lines_i]).strip()
        traceback_excerpt = excerpt or None

    raw_tail = "\n".join(lines[-max_lines_i:]).strip() if lines else None

    return FailureSummary(
        exit_code=int(result.exit_code),
        failing_tests=tuple(failing),
        reasons=tuple(reasons),
        traceback_excerpt=traceback_excerpt,
        raw_tail=raw_tail,
    )


def build_repair_prompt(
    *,
    goal: str,
    ir_context: Mapping[str, Any],
    plan_trace: Mapping[str, Any],
    step_id: str,
    step_title: str,
    intent_id: str,
    active_objectives: list[Mapping[str, Any]],
    linked_constraints: list[Mapping[str, Any]],
    active_success_criteria: list[Mapping[str, Any]],
    retrieved_context: Mapping[str, Any],
    last_generation_text: str,
    failure: FailureSummary,
    verifier_feedback: Mapping[str, Any] | None = None,
) -> str:
    """Build a repair prompt that strongly encourages patch-shaped output."""

    require_non_empty(goal, name="goal")
    require_non_empty(step_id, name="step_id")
    require_non_empty(step_title, name="step_title")
    require_non_empty(intent_id, name="intent_id")
    require_non_empty(last_generation_text, name="last_generation_text")

    f = failure.to_json_obj()
    vf = dict(verifier_feedback) if verifier_feedback is not None else None

    return (
        "You are repairing a codebase change in an AKC compile loop.\n\n"
        f"Goal:\n{goal}\n\n"
        f"Intent context (active objectives/constraints/acceptance):\n"
        f"- intent_id: {intent_id}\n"
        f"- active_objectives:\n{format_active_objectives_for_prompt(list(active_objectives))}\n\n"
        f"- linked_constraints:\n{format_linked_constraints_for_prompt(list(linked_constraints))}\n\n"
        f"- active_success_criteria:\n{format_success_criteria_for_prompt(list(active_success_criteria))}\n\n"
        f"IR (compact structural graph):\n"
        f"{json.dumps(dict(ir_context), sort_keys=True, ensure_ascii=False)}\n\n"
        f"Plan execution trace:\n"
        f"{json.dumps(dict(plan_trace), sort_keys=True, ensure_ascii=False)}\n\n"
        f"Current step:\n- id: {step_id}\n- title: {step_title}\n\n"
        f"Retrieved context (index + code memory):\n{dict(retrieved_context)}\n\n"
        "Last generation output (may contain the attempted patch):\n"
        f"{last_generation_text}\n\n"
        "Verifier feedback (if any):\n"
        f"{vf}\n\n"
        "Execution failure summary (parsed):\n"
        f"{f}\n\n"
        "Task:\n"
        "- Diagnose the root cause of the failure.\n"
        "- Produce a minimal fix that makes tests pass.\n"
        "- Keep the result production-ready on the touched path: retain real validation, error handling, "
        "configuration wiring, and edge-case behavior required by the context.\n"
        "- Do not assume time-sensitive facts such as current APIs, library behavior, product surfaces, or "
        "documentation details.\n"
        "- Verify time-sensitive details from configured sources when available; if they cannot be verified, "
        "do not guess or invent specifics.\n"
        "- Do not hardcode secrets, fake credentials, dummy values, or local-machine-specific paths.\n"
        "- Do not make the repair pass by weakening tests, bypassing safety checks, removing observability, "
        "or introducing silent no-op fallbacks.\n"
        "- Do not leave TODO/FIXME-only scaffolding, fake implementations, mock-only runtime behavior, or "
        "incomplete scaffolding unless explicitly required by the intent.\n"
        "- Preserve surrounding interface and data compatibility unless the intent clearly requires a "
        "breaking change.\n"
        "- If the failure indicates missing/insufficient tests, add or update tests "
        "in the patch.\n\n"
        "Output format (strict):\n"
        "- Return ONLY a unified diff (git-style) patch.\n"
        "- Do not include prose, explanations, or Markdown fences.\n"
        "- The patch must be tenant-safe: never read/write outside this repo "
        "and never mix tenants.\n"
    )
