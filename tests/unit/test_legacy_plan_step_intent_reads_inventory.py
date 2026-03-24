"""Contract: legacy plan-step ``inputs`` blobs are only read in audited locations.

Direct ``inputs.get("active_objectives" | "linked_constraints" | "active_success_criteria")``
must remain confined to:

- ``akc.intent.resolve`` — canonical :func:`~akc.intent.resolve.resolve_compile_intent_context` path
- ``akc.compile.ir_builder`` — optional merge from step inputs into intent node properties
- ``akc.memory.why_conflicts`` — drift detection against linked constraint ids (Phase 3)

Add new reads only by extending the allowlist with a short audit note in the PR.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_AKC_SRC = _REPO_ROOT / "src" / "akc"

# Paths are repo-relative for stable diffs and review.
_LEGACY_INPUTS_GET_ALLOWLIST_REL = frozenset(
    {
        "src/akc/intent/resolve.py",
        "src/akc/compile/ir_builder.py",
        "src/akc/memory/why_conflicts.py",
    }
)

_LEGACY_INPUTS_GET_PATTERN = re.compile(
    r'\binputs\.get\(\s*["\'](active_objectives|linked_constraints|active_success_criteria)["\']\s*\)'
)


def _py_files_under_akc() -> list[Path]:
    return sorted(_AKC_SRC.rglob("*.py"))


@pytest.mark.parametrize("py_path", _py_files_under_akc(), ids=lambda p: str(p.relative_to(_REPO_ROOT)))
def test_legacy_plan_step_inputs_get_is_allowlisted(py_path: Path) -> None:
    rel = str(py_path.relative_to(_REPO_ROOT))
    text = py_path.read_text(encoding="utf-8")
    for line_no, line in enumerate(text.splitlines(), start=1):
        if _LEGACY_INPUTS_GET_PATTERN.search(line):
            assert rel in _LEGACY_INPUTS_GET_ALLOWLIST_REL, (
                f"Un-audited legacy plan-step inputs.get on {rel}:{line_no}: {line.strip()}"
            )


def test_legacy_allowlist_paths_exist() -> None:
    for rel in _LEGACY_INPUTS_GET_ALLOWLIST_REL:
        assert (_REPO_ROOT / rel).is_file(), f"missing allowlist file: {rel}"
