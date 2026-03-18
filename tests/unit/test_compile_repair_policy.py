from __future__ import annotations

from akc.compile.interfaces import ExecutionResult
from akc.compile.repair import build_repair_prompt, parse_execution_failure


def test_parse_execution_failure_extracts_pytest_short_summary_failed_lines() -> None:
    out = """
============================= test session starts =============================
platform darwin -- Python 3.13.1, pytest-8.2.0
collected 2 items

tests/test_x.py F.

================================== FAILURES ===================================
______________________________ test_addition _______________________________

>       assert 1 + 1 == 3
E       assert (1 + 1) == 3

tests/test_x.py:10: AssertionError
=========================== short test summary info ============================
FAILED tests/test_x.py::test_addition - assert (1 + 1) == 3
========================= 1 failed, 1 passed in 0.05s =========================
""".strip()
    failure = parse_execution_failure(result=ExecutionResult(exit_code=1, stdout=out, stderr=""))
    assert failure.exit_code == 1
    assert "tests/test_x.py::test_addition" in failure.failing_tests
    assert any("assert (1 + 1) == 3" in r for r in failure.reasons)
    assert failure.raw_tail is not None and "short test summary info" in failure.raw_tail


def test_build_repair_prompt_includes_failure_json_and_strict_patch_instructions() -> None:
    out = """
=========================== short test summary info ============================
FAILED tests/test_x.py::test_addition - boom
""".strip()
    failure = parse_execution_failure(result=ExecutionResult(exit_code=1, stdout=out, stderr=""))
    prompt = build_repair_prompt(
        goal="Fix failing test",
        plan_json={"id": "p1"},
        step_id="s1",
        step_title="Step",
        retrieved_context={"documents": [], "code_memory_items": []},
        last_generation_text="diff --git a/x b/x",
        failure=failure,
    )
    assert "Execution failure summary (parsed)" in prompt
    assert "max" not in prompt  # prompt builder itself does not enforce budgets
    assert "Return ONLY a unified diff" in prompt
