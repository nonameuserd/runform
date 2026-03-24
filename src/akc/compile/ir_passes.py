from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from akc.compile.ir_prompt_context import (
    compact_ir_document_for_prompt,
    format_active_objectives_for_prompt,
    format_linked_constraints_for_prompt,
    format_prompt_json_section,
    format_success_criteria_for_prompt,
    plan_execution_trace_for_prompt,
)
from akc.compile.repair import FailureSummary, build_repair_prompt
from akc.ir import IRDocument
from akc.memory.models import PlanState


class IRGeneratePromptPass(Protocol):
    """Phase-3 generate pass prompt builder consuming IR."""

    def build_prompt(
        self,
        *,
        ir_doc: IRDocument,
        intent_id: str,
        active_objectives: list[Mapping[str, Any]],
        linked_constraints: list[Mapping[str, Any]],
        active_success_criteria: list[Mapping[str, Any]],
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
        intent_id: str,
        active_objectives: list[Mapping[str, Any]],
        linked_constraints: list[Mapping[str, Any]],
        active_success_criteria: list[Mapping[str, Any]],
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
        intent_id: str,
        active_objectives: list[Mapping[str, Any]],
        linked_constraints: list[Mapping[str, Any]],
        active_success_criteria: list[Mapping[str, Any]],
        goal: str,
        plan: PlanState,
        retrieved_context: Mapping[str, Any],
        test_policy: Mapping[str, Any],
        stage: str,
    ) -> str:
        # Prefix the prompt with an IR fingerprint so the prompt key and
        # cached candidate mapping become IR-sensitive.
        ir_fingerprint = ir_doc.fingerprint()

        ir_compact = compact_ir_document_for_prompt(ir_doc)
        plan_trace = plan_execution_trace_for_prompt(plan)
        head = (
            f"IR fingerprint: {ir_fingerprint}\n\n"
            f"Intent context (active objectives/constraints/acceptance):\n"
            f"- intent_id: {intent_id}\n"
            f"- active_objectives:\n{format_active_objectives_for_prompt(list(active_objectives))}\n\n"
            f"- linked_constraints:\n{format_linked_constraints_for_prompt(list(linked_constraints))}\n\n"
            f"- active_success_criteria:\n{format_success_criteria_for_prompt(list(active_success_criteria))}\n\n"
            f"Goal:\n{goal}\n\n"
        )
        tail = (
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
        return (
            head
            + format_prompt_json_section("IR (compact structural graph):", ir_compact)
            + format_prompt_json_section("Plan execution trace:", plan_trace)
            + tail
        )


@dataclass(frozen=True, slots=True)
class DefaultIRRepairPromptPass:
    """Default IR-first prompt builder for the repair stage."""

    def build_prompt(
        self,
        *,
        ir_doc: IRDocument,
        intent_id: str,
        active_objectives: list[Mapping[str, Any]],
        linked_constraints: list[Mapping[str, Any]],
        active_success_criteria: list[Mapping[str, Any]],
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
            ir_context=compact_ir_document_for_prompt(ir_doc),
            plan_trace=plan_execution_trace_for_prompt(plan),
            step_id=step_id,
            step_title=step_title,
            intent_id=intent_id,
            active_objectives=active_objectives,
            linked_constraints=linked_constraints,
            active_success_criteria=active_success_criteria,
            retrieved_context=retrieved_context,
            last_generation_text=last_generation_text,
            failure=failure,
            verifier_feedback=verifier_feedback,
        )
        return f"IR fingerprint: {ir_fingerprint}\n\n{prompt}"
