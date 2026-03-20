from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from akc.compile.interfaces import LLMMessage
from akc.compile.ir_passes import IRGeneratePromptPass, IRRepairPromptPass
from akc.compile.patch_emitter import (
    ModelCallNeeded,
    ResolvedPatch,
    resolve_patch_candidate_from_prompt,
)
from akc.ir import IRDocument
from akc.memory.models import PlanState, PlanStep, now_ms
from akc.run.manifest import RunManifest
from akc.run.vcr import llm_vcr_prompt_key


@dataclass(frozen=True, slots=True)
class _StaticGenPass:
    """Test prompt pass that returns stable user prompt."""

    text: str = "GEN_PROMPT"

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
        _ = (ir_doc, goal, plan, retrieved_context, test_policy, stage)
        return self.text


@dataclass(frozen=True, slots=True)
class _StaticRepairPass:
    text: str = "REPAIR_PROMPT"

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
        failure: Any,
        verifier_feedback: Mapping[str, Any] | None,
    ) -> str:
        _ = (
            ir_doc,
            goal,
            plan,
            step_id,
            step_title,
            retrieved_context,
            last_generation_text,
            failure,
            verifier_feedback,
        )
        return self.text


def _mk_plan(*, tenant_id: str, repo_id: str, plan_id: str, step_id: str) -> PlanState:
    t = now_ms()
    return PlanState(
        id=plan_id,
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="do something",
        status="active",
        created_at_ms=t,
        updated_at_ms=t,
        steps=(
            PlanStep(
                id=step_id,
                title="step",
                status="pending",
                order_idx=0,
                inputs={},
                outputs={},
            ),
        ),
        next_step_id=step_id,
        budgets={},
        last_feedback={},
    )


def test_resolve_patch_candidate_uses_llm_vcr_prompt_key_and_extracts_touched_paths() -> None:
    tenant_id = "t1"
    repo_id = "r1"
    plan_id = "p1"
    step_id = "s1"
    goal = "goal"

    plan = _mk_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id, step_id=step_id)
    ir_doc = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=())

    gen_pass: IRGeneratePromptPass = _StaticGenPass()
    repair_pass: IRRepairPromptPass = _StaticRepairPass()

    tier_name = "small"
    tier_model = "fake-small"
    replay_mode: Literal["llm_vcr"] = "llm_vcr"
    temperature = 0.0
    max_output_tokens = 123

    llm_messages = [
        LLMMessage(role="system", content="You are an AKC compile loop assistant."),
        LLMMessage(role="user", content="GEN_PROMPT"),
    ]
    llm_metadata = {
        "tier": tier_name,
        "tier_model": tier_model,
        "plan_id": plan.id,
        "step_id": step_id,
        "replay_mode": replay_mode,
    }
    expected_prompt_key = llm_vcr_prompt_key(
        messages=llm_messages,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        metadata=llm_metadata,
    )

    patch_text = "\n".join(
        [
            "--- a/src/a.py",
            "+++ b/src/a.py",
            "@@",
            "+print(1)",
            "",
            "--- a/tests/test_a.py",
            "+++ b/tests/test_a.py",
            "@@",
            "+def test_a():",
            "+    assert True",
            "",
        ]
    )

    manifest = RunManifest(
        run_id="run_1",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="0" * 64,
        replay_mode=replay_mode,
        llm_vcr={expected_prompt_key: patch_text},
    )

    resolved = resolve_patch_candidate_from_prompt(
        stage="generate",
        plan=plan,
        ir_doc=ir_doc,
        step_id=step_id,
        tier_name=tier_name,
        tier_model=tier_model,
        temperature=temperature,
        max_output_tokens=max_output_tokens,
        goal=goal,
        retrieved_context={},
        test_policy={"require_tests_for_non_test_changes": False},
        replay_mode=replay_mode,
        replay_manifest=manifest,
        should_call_model=False,
        generate_prompt_pass=gen_pass,
        repair_prompt_pass=repair_pass,
    )

    assert isinstance(resolved, ResolvedPatch)
    assert resolved.candidate.patch_text == patch_text
    assert set(resolved.candidate.touched_paths) == {"src/a.py", "tests/test_a.py"}


def test_resolve_patch_candidate_returns_model_call_needed_when_should_call_model_true() -> None:
    tenant_id = "t1"
    repo_id = "r1"
    plan_id = "p1"
    step_id = "s1"

    plan = _mk_plan(tenant_id=tenant_id, repo_id=repo_id, plan_id=plan_id, step_id=step_id)
    ir_doc = IRDocument(tenant_id=tenant_id, repo_id=repo_id, nodes=())

    gen_pass: IRGeneratePromptPass = _StaticGenPass()
    repair_pass: IRRepairPromptPass = _StaticRepairPass()

    manifest = RunManifest(
        run_id="run_2",
        tenant_id=tenant_id,
        repo_id=repo_id,
        ir_sha256="1" * 64,
        replay_mode="live",
    )

    resolved = resolve_patch_candidate_from_prompt(
        stage="generate",
        plan=plan,
        ir_doc=ir_doc,
        step_id=step_id,
        tier_name="small",
        tier_model="fake-small",
        temperature=0.0,
        max_output_tokens=10,
        goal="goal",
        retrieved_context={},
        test_policy={"require_tests_for_non_test_changes": False},
        replay_mode="live",
        replay_manifest=manifest,
        should_call_model=True,
        generate_prompt_pass=gen_pass,
        repair_prompt_pass=repair_pass,
    )

    assert isinstance(resolved, ModelCallNeeded)
    assert resolved.user_prompt == "GEN_PROMPT"
