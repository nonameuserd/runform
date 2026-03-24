from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from akc.compile.interfaces import ExecutionResult
from akc.memory.models import PlanState

RunStatus = Literal["succeeded", "failed", "budget_exhausted"]


@dataclass(frozen=True, slots=True)
class Candidate:
    """A single generate→execute attempt."""

    tier: str
    stage: str
    llm_text: str
    touched_paths: tuple[str, ...]
    test_paths: tuple[str, ...]
    execution: ExecutionResult | None
    execution_stage: str | None
    execution_command: list[str] | None
    score: int
    attempt_idx: int
    created_at_ms: int

    def to_json_obj(self) -> dict[str, Any]:
        exe = None
        if self.execution is not None:
            exe = {
                "stage": self.execution_stage,
                "command": list(self.execution_command or []),
                "exit_code": int(self.execution.exit_code),
                "stdout": self.execution.stdout,
                "stderr": self.execution.stderr,
                "duration_ms": self.execution.duration_ms,
            }
        return {
            "tier": self.tier,
            "stage": self.stage,
            "llm_text": self.llm_text,
            "touched_paths": list(self.touched_paths),
            "test_paths": list(self.test_paths),
            "has_test_changes": bool(self.test_paths),
            "execution": exe,
            "score": int(self.score),
            "attempt_idx": int(self.attempt_idx),
            "created_at_ms": int(self.created_at_ms),
        }


@dataclass(frozen=True, slots=True)
class ControllerResult:
    status: RunStatus
    plan: PlanState
    best_candidate: Candidate | None
    accounting: dict[str, Any]
    # Compile vs acceptance separation:
    # - compile_succeeded means the candidate passed the execute+verifier gates.
    # - intent_satisfied means intent success_criteria accepted the candidate.
    compile_succeeded: bool
    intent_satisfied: bool
