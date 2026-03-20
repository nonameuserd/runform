from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal

from akc.compile.interfaces import LLMMessage, LLMRequest
from akc.compile.ir_passes import IRGeneratePromptPass, IRRepairPromptPass
from akc.compile.patch_utils import extract_touched_paths
from akc.compile.repair import FailureSummary
from akc.ir import IRDocument
from akc.memory.models import PlanState
from akc.run.manifest import RunManifest
from akc.run.vcr import llm_vcr_prompt_key

StageName = Literal["generate", "repair"]


@dataclass(frozen=True, slots=True)
class PatchCandidate:
    patch_text: str
    touched_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ModelCallNeeded:
    llm_stage: StageName
    llm_request: LLMRequest
    prompt_key: str
    user_prompt: str


@dataclass(frozen=True, slots=True)
class ResolvedPatch:
    candidate: PatchCandidate
    prompt_key: str
    patch_sha256: str


def candidate_from_patch_text(*, patch_text: str) -> PatchCandidate:
    return PatchCandidate(
        patch_text=patch_text,
        touched_paths=tuple(extract_touched_paths(patch_text)),
    )


def patch_sha256_hex(*, patch_text: str) -> str:
    """Compute deterministic patch content fingerprint for auditability."""

    return hashlib.sha256(patch_text.encode("utf-8")).hexdigest()


def _cached_candidate_text(*, plan: PlanState, step_id: str) -> str | None:
    step = next((s for s in plan.steps if s.id == step_id), None)
    if step is None:
        return None
    outputs = dict(step.outputs or {})
    best = outputs.get("best_candidate")
    if isinstance(best, dict):
        text = best.get("llm_text")
        if isinstance(text, str) and text.strip():
            return text
    return None


def _manifest_pass_metadata(
    *, replay_manifest: RunManifest | None, pass_name: StageName
) -> dict[str, Any] | None:
    if replay_manifest is None:
        return None
    for rec in replay_manifest.passes:
        if rec.name == pass_name and isinstance(rec.metadata, dict):
            return dict(rec.metadata)
    return None


def _replayed_candidate_text(
    *,
    plan: PlanState,
    step_id: str,
    stage: StageName,
    replay_manifest: RunManifest | None,
) -> str | None:
    text = _cached_candidate_text(plan=plan, step_id=step_id)
    if text:
        return text

    md = _manifest_pass_metadata(replay_manifest=replay_manifest, pass_name=stage)
    if md is None and stage == "repair":
        md = _manifest_pass_metadata(replay_manifest=replay_manifest, pass_name="generate")
    if md is None:
        return None

    raw = md.get("llm_text")
    if isinstance(raw, str) and raw.strip():
        return raw
    return None


def resolve_patch_candidate_from_prompt(
    *,
    stage: StageName,
    plan: PlanState,
    ir_doc: IRDocument,
    step_id: str,
    tier_name: str,
    tier_model: str,
    temperature: float,
    max_output_tokens: int | None,
    goal: str,
    retrieved_context: Mapping[str, Any],
    test_policy: Mapping[str, Any] | None = None,
    # Repair-only inputs:
    step_title: str | None = None,
    last_generation_text: str | None = None,
    failure: FailureSummary | None = None,
    verifier_feedback: Mapping[str, Any] | None = None,
    # Replay knobs:
    replay_mode: str,
    replay_manifest: RunManifest,
    should_call_model: bool,
    generate_prompt_pass: IRGeneratePromptPass,
    repair_prompt_pass: IRRepairPromptPass,
    # NOTE: keep the signature compatible in one place; this is intentionally
    # long but explicit to preserve tenant isolation boundaries.
    # user_prompt override for debugging is intentionally omitted.
) -> ModelCallNeeded | ResolvedPatch | None:
    # Build the user-facing prompt string via IR-first prompt passes.
    if stage == "generate":
        if test_policy is None:
            raise ValueError("test_policy is required for generate stage")
        user_prompt = generate_prompt_pass.build_prompt(
            ir_doc=ir_doc,
            goal=goal,
            plan=plan,
            retrieved_context=retrieved_context,
            test_policy=test_policy,
            stage=stage,
        )
    else:
        if step_title is None or last_generation_text is None or failure is None:
            raise ValueError(
                "step_title, last_generation_text, and failure are required for repair stage"
            )
        user_prompt = repair_prompt_pass.build_prompt(
            ir_doc=ir_doc,
            goal=goal,
            plan=plan,
            step_id=step_id,
            step_title=step_title,
            retrieved_context=retrieved_context,
            last_generation_text=last_generation_text,
            failure=failure,
            verifier_feedback=verifier_feedback,
        )

    llm_messages = [
        LLMMessage(role="system", content="You are an AKC compile loop assistant."),
        LLMMessage(role="user", content=user_prompt),
    ]
    llm_request = LLMRequest(
        messages=llm_messages,
        temperature=float(temperature),
        max_output_tokens=int(max_output_tokens) if max_output_tokens is not None else None,
        metadata={
            "tier": tier_name,
            "tier_model": tier_model,
            "plan_id": plan.id,
            "step_id": step_id,
            "replay_mode": replay_mode,
        },
    )
    prompt_key = llm_vcr_prompt_key(
        messages=llm_messages,
        temperature=float(temperature),
        max_output_tokens=int(max_output_tokens) if max_output_tokens is not None else None,
        metadata=llm_request.metadata,
    )

    if should_call_model:
        return ModelCallNeeded(
            llm_stage=stage,
            llm_request=llm_request,
            prompt_key=prompt_key,
            user_prompt=user_prompt,
        )

    llm_vcr = dict(replay_manifest.llm_vcr or {})
    cached_text = llm_vcr.get(prompt_key)
    if not cached_text:
        cached_text = _replayed_candidate_text(
            plan=plan,
            step_id=step_id,
            stage=stage,
            replay_manifest=replay_manifest,
        )
    if not cached_text:
        return None

    return ResolvedPatch(
        candidate=candidate_from_patch_text(patch_text=cached_text),
        prompt_key=prompt_key,
        patch_sha256=patch_sha256_hex(patch_text=cached_text),
    )
