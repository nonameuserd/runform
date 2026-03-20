from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from akc.compile.repair import FailureSummary, build_repair_prompt
from akc.ir import IRDocument
from akc.memory.models import PlanState


class IRGeneratePromptPass(Protocol):
    """Phase-3 generate pass prompt builder consuming IR."""

    def build_prompt(
        self,
        *,
        ir_doc: IRDocument,
        goal: str,
        plan: PlanState,
        retrieved_context: Mapping[str, Any],
        test_policy: Mapping[str, Any],
        stage: str,
    ) -> str: ...


class IRRepairPromptPass(Protocol):
    """Phase-3 repair pass prompt builder consuming IR."""

    def build_prompt(
        self,
        *,
        ir_doc: IRDocument,
        goal: str,
        plan: PlanState,
        step_id: str,
        step_title: str,
        retrieved_context: Mapping[str, Any],
        last_generation_text: str,
        failure: FailureSummary,
        verifier_feedback: Mapping[str, Any] | None,
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class DefaultIRGeneratePromptPass:
    """Default IR-first prompt builder for the generate stage."""

    def build_prompt(
        self,
        *,
        ir_doc: IRDocument,
        goal: str,
        plan: PlanState,
        retrieved_context: Mapping[str, Any],
        test_policy: Mapping[str, Any],
        stage: str,
    ) -> str:
        # Prefix the prompt with an IR fingerprint so the prompt key and
        # cached candidate mapping become IR-sensitive.
        ir_fingerprint = ir_doc.fingerprint()
        return (
            f"IR fingerprint: {ir_fingerprint}\n\n"
            f"Goal:\n{goal}\n\n"
            f"Plan:\n{plan.to_json_obj()}\n\n"
            f"Retrieved context:\n{retrieved_context}\n\n"
            f"Test policy:\n{dict(test_policy)}\n\n"
            f"Stage: {stage}\n\n"
            "Output format:\n"
            "- Return ONLY a unified diff (git-style) patch.\n"
            "- Do not include prose, explanations, or Markdown fences.\n"
            "- The patch must be tenant-safe: never read/write outside this repo "
            "and never mix tenants.\n"
            "- By default, include relevant test changes in the same patch "
            "(add/update tests that cover your change).\n"
        )


@dataclass(frozen=True, slots=True)
class DefaultIRRepairPromptPass:
    """Default IR-first prompt builder for the repair stage."""

    def build_prompt(
        self,
        *,
        ir_doc: IRDocument,
        goal: str,
        plan: PlanState,
        step_id: str,
        step_title: str,
        retrieved_context: Mapping[str, Any],
        last_generation_text: str,
        failure: FailureSummary,
        verifier_feedback: Mapping[str, Any] | None,
    ) -> str:
        ir_fingerprint = ir_doc.fingerprint()
        prompt = build_repair_prompt(
            goal=goal,
            plan_json=plan.to_json_obj(),
            step_id=step_id,
            step_title=step_title,
            retrieved_context=retrieved_context,
            last_generation_text=last_generation_text,
            failure=failure,
            verifier_feedback=verifier_feedback,
        )
        return f"IR fingerprint: {ir_fingerprint}\n\n{prompt}"
