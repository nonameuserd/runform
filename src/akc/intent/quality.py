from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, cast

from akc.intent.models import ALLOWED_QUALITY_DIMENSION_IDS, QualityContract, QualityDimensionId
from akc.memory.models import JSONValue


def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return float(value)


def _is_test_path(path: str) -> bool:
    p = str(path).strip().replace("\\", "/")
    if not p:
        return False
    base = p.split("/")[-1]
    return p.startswith("tests/") or base.startswith("test_") or base.endswith("_test.py")


def _repeated_line_ratio(text: str) -> float:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) < 4:
        return 0.0
    counts: dict[str, int] = {}
    for ln in lines:
        counts[ln] = counts.get(ln, 0) + 1
    repeated = sum(c for c in counts.values() if c > 1)
    return float(repeated) / float(len(lines))


@dataclass(frozen=True, slots=True)
class QualityDimensionScore:
    dimension_id: QualityDimensionId
    score: float
    target_score: float
    gate_min_score: float | None
    weight: float
    enforcement_stage: str
    reasons: tuple[str, ...]
    evidence: dict[str, JSONValue]

    def to_json_obj(self) -> dict[str, JSONValue]:
        below_target = float(self.score) < float(self.target_score)
        gate_failed = (
            self.enforcement_stage == "gate"
            and self.gate_min_score is not None
            and float(self.score) < float(self.gate_min_score)
        )
        return {
            "dimension_id": self.dimension_id,
            "score": float(self.score),
            "target_score": float(self.target_score),
            "gate_min_score": float(self.gate_min_score) if self.gate_min_score is not None else None,
            "weight": float(self.weight),
            "enforcement_stage": self.enforcement_stage,
            "below_target": bool(below_target),
            "gate_failed": bool(gate_failed),
            "reasons": list(self.reasons),
            "evidence": dict(self.evidence),
        }


@dataclass(frozen=True, slots=True)
class QualityScorecard:
    overall_weighted_score: float
    dimensions: tuple[QualityDimensionScore, ...]

    def gate_failed_dimensions(self) -> tuple[str, ...]:
        out: list[str] = []
        for d in self.dimensions:
            if d.enforcement_stage == "gate" and d.gate_min_score is not None and d.score < d.gate_min_score:
                out.append(d.dimension_id)
        return tuple(out)

    def advisory_dimensions(self) -> tuple[str, ...]:
        out: list[str] = []
        for d in self.dimensions:
            if d.enforcement_stage == "advisory" and d.score < d.target_score:
                out.append(d.dimension_id)
        return tuple(out)

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "overall_weighted_score": float(self.overall_weighted_score),
            "gate_failed_dimensions": list(self.gate_failed_dimensions()),
            "advisory_dimensions": list(self.advisory_dimensions()),
            "dimensions": [d.to_json_obj() for d in self.dimensions],
        }


def _dimension_score_taste(
    *,
    patch_text: str,
    touched_paths: Sequence[str],
) -> tuple[float, tuple[str, ...], dict[str, JSONValue]]:
    score = 0.92
    reasons: list[str] = []
    patch_lower = str(patch_text or "").lower()
    patch_len = len(str(patch_text or ""))
    if patch_len > 100_000:
        score -= 0.35
        reasons.append("very_large_patch")
    elif patch_len > 50_000:
        score -= 0.15
        reasons.append("large_patch")

    placeholder_hits = 0
    for needle in ("todo", "fixme", "lorem ipsum", "xxx", "placeholder"):
        if needle in patch_lower:
            placeholder_hits += 1
    if placeholder_hits > 0:
        score -= min(0.25, 0.06 * float(placeholder_hits))
        reasons.append("placeholder_or_noise_tokens")

    repeated = _repeated_line_ratio(str(patch_text or ""))
    if repeated > 0.35:
        score -= 0.2
        reasons.append("high_repeated_line_ratio")

    path_count = len([p for p in touched_paths if str(p).strip()])
    if path_count > 40:
        score -= 0.2
        reasons.append("very_wide_change_surface")
    elif path_count > 20:
        score -= 0.1
        reasons.append("wide_change_surface")

    evidence: dict[str, JSONValue] = {
        "patch_length_chars": patch_len,
        "placeholder_hits": int(placeholder_hits),
        "repeated_line_ratio": float(round(repeated, 6)),
        "touched_path_count": int(path_count),
    }
    return _clamp01(score), tuple(reasons), evidence


def _dimension_score_domain_knowledge(
    *,
    retrieved_context: Mapping[str, Any] | None,
) -> tuple[float, tuple[str, ...], dict[str, JSONValue]]:
    reasons: list[str] = []
    if not isinstance(retrieved_context, Mapping):
        return 0.45, ("retrieval_context_missing",), {"documents": 0, "code_memory_items": 0}

    raw_docs = retrieved_context.get("documents")
    docs = len(raw_docs) if isinstance(raw_docs, list) else 0
    raw_mem = retrieved_context.get("code_memory_items")
    mem = len(raw_mem) if isinstance(raw_mem, list) else 0
    evidence_count = docs + mem

    score = 0.5
    if evidence_count > 0:
        score = 0.7 + min(0.3, float(evidence_count) / 20.0 * 0.3)
    else:
        reasons.append("no_retrieval_evidence")

    evidence: dict[str, JSONValue] = {
        "documents": int(docs),
        "code_memory_items": int(mem),
        "retrieval_evidence_count": int(evidence_count),
    }
    return _clamp01(score), tuple(reasons), evidence


def _dimension_score_judgment(
    *,
    patch_text: str,
    accounting: Mapping[str, Any],
    verifier_passed: bool,
    execution_exit_code: int,
) -> tuple[float, tuple[str, ...], dict[str, JSONValue]]:
    score = 0.9
    reasons: list[str] = []
    raw_policy = accounting.get("policy_decisions") if isinstance(accounting, Mapping) else None
    denied = 0
    allowed = 0
    if isinstance(raw_policy, list):
        for row in raw_policy:
            if not isinstance(row, Mapping):
                continue
            if bool(row.get("allowed")):
                allowed += 1
            else:
                denied += 1
    if denied > 0:
        score -= min(0.45, 0.15 * float(denied))
        reasons.append("policy_denials_present")

    if not verifier_passed:
        score -= 0.2
        reasons.append("verifier_not_passed")

    if int(execution_exit_code) != 0:
        score -= 0.15
        reasons.append("tests_or_execute_failed")

    patch_lower = str(patch_text or "").lower()
    if "allow_network=true" in patch_lower or "disable_security" in patch_lower:
        score -= 0.2
        reasons.append("unsafe_permission_pattern")

    evidence: dict[str, JSONValue] = {
        "policy_denied_count": int(denied),
        "policy_allowed_count": int(allowed),
        "verifier_passed": bool(verifier_passed),
        "execution_exit_code": int(execution_exit_code),
    }
    return _clamp01(score), tuple(reasons), evidence


def _dimension_score_instincts(
    *,
    accounting: Mapping[str, Any],
    execution_exit_code: int,
) -> tuple[float, tuple[str, ...], dict[str, JSONValue]]:
    score = 0.75
    reasons: list[str] = []
    repairs = int(accounting.get("repair_iterations", 0)) if isinstance(accounting, Mapping) else 0
    if repairs > 0 and int(execution_exit_code) == 0:
        score += 0.12
        reasons.append("adaptive_repair_recovery")
    if repairs > 2:
        score -= 0.22
        reasons.append("many_repairs_needed")
    if int(execution_exit_code) != 0:
        score -= 0.3
        reasons.append("failed_to_recover")

    evidence: dict[str, JSONValue] = {
        "repair_iterations": int(repairs),
        "execution_exit_code": int(execution_exit_code),
    }
    return _clamp01(score), tuple(reasons), evidence


def _dimension_score_user_empathy(
    *,
    patch_text: str,
    touched_paths: Sequence[str],
) -> tuple[float, tuple[str, ...], dict[str, JSONValue]]:
    score = 0.7
    reasons: list[str] = []
    patch_lower = str(patch_text or "").lower()

    has_recovery_language = any(x in patch_lower for x in ("try again", "help", "recover", "invalid", "please"))
    if has_recovery_language:
        score += 0.15

    if "traceback" in patch_lower or "internal error" in patch_lower:
        score -= 0.15
        reasons.append("technical_error_language")

    if "raise exception(" in patch_lower:
        score -= 0.1
        reasons.append("generic_exception_usage")

    docs_touched = any(str(p).replace("\\", "/").startswith("docs/") for p in touched_paths)
    if docs_touched:
        score += 0.1

    evidence: dict[str, JSONValue] = {
        "has_recovery_language": bool(has_recovery_language),
        "docs_touched": bool(docs_touched),
    }
    return _clamp01(score), tuple(reasons), evidence


def _dimension_score_engineering_discipline(
    *,
    touched_paths: Sequence[str],
    accounting: Mapping[str, Any],
    execution_exit_code: int,
    verifier_passed: bool,
) -> tuple[float, tuple[str, ...], dict[str, JSONValue]]:
    reasons: list[str] = []
    score = 0.25

    test_paths = [p for p in touched_paths if _is_test_path(str(p))]
    if test_paths:
        score += 0.3
    else:
        reasons.append("no_test_paths_touched")

    if int(execution_exit_code) == 0:
        score += 0.3
    else:
        reasons.append("execution_not_passing")

    if verifier_passed:
        score += 0.15
    else:
        reasons.append("verifier_not_passing")

    repairs = int(accounting.get("repair_iterations", 0)) if isinstance(accounting, Mapping) else 0
    if repairs <= 1:
        score += 0.1
    elif repairs > 3:
        score -= 0.15
        reasons.append("high_repair_iterations")

    policy_rows = accounting.get("policy_decisions") if isinstance(accounting, Mapping) else None
    policy_denied = 0
    if isinstance(policy_rows, list):
        for row in policy_rows:
            if isinstance(row, Mapping) and not bool(row.get("allowed")):
                policy_denied += 1
    if policy_denied > 0:
        score -= min(0.2, 0.05 * float(policy_denied))
        reasons.append("policy_denials_present")

    evidence: dict[str, JSONValue] = {
        "test_paths_touched": int(len(test_paths)),
        "execution_exit_code": int(execution_exit_code),
        "verifier_passed": bool(verifier_passed),
        "repair_iterations": int(repairs),
        "policy_denied_count": int(policy_denied),
    }
    return _clamp01(score), tuple(reasons), evidence


def _score_dimension(
    *,
    dimension_id: QualityDimensionId,
    patch_text: str,
    touched_paths: Sequence[str],
    accounting: Mapping[str, Any],
    retrieved_context: Mapping[str, Any] | None,
    execution_exit_code: int,
    verifier_passed: bool,
) -> tuple[float, tuple[str, ...], dict[str, JSONValue]]:
    if dimension_id == "taste":
        return _dimension_score_taste(patch_text=patch_text, touched_paths=touched_paths)
    if dimension_id == "domain_knowledge":
        return _dimension_score_domain_knowledge(retrieved_context=retrieved_context)
    if dimension_id == "judgment":
        return _dimension_score_judgment(
            patch_text=patch_text,
            accounting=accounting,
            verifier_passed=verifier_passed,
            execution_exit_code=execution_exit_code,
        )
    if dimension_id == "instincts":
        return _dimension_score_instincts(accounting=accounting, execution_exit_code=execution_exit_code)
    if dimension_id == "user_empathy":
        return _dimension_score_user_empathy(patch_text=patch_text, touched_paths=touched_paths)
    return _dimension_score_engineering_discipline(
        touched_paths=touched_paths,
        accounting=accounting,
        execution_exit_code=execution_exit_code,
        verifier_passed=verifier_passed,
    )


def evaluate_quality_contract(
    *,
    quality_contract: QualityContract,
    patch_text: str,
    touched_paths: Sequence[str],
    accounting: Mapping[str, Any],
    retrieved_context: Mapping[str, Any] | None,
    execution_exit_code: int,
    verifier_passed: bool,
) -> QualityScorecard:
    dims: list[QualityDimensionScore] = []
    total_weight = 0.0
    weighted_score = 0.0

    available_evidence: set[str] = set()
    if any(_is_test_path(str(p)) for p in touched_paths):
        available_evidence.add("tests_touched")
    if isinstance(retrieved_context, Mapping):
        docs_raw = retrieved_context.get("documents")
        if isinstance(docs_raw, list) and len(docs_raw) > 0:
            available_evidence.add("retrieval_documents")
        mem_raw = retrieved_context.get("code_memory_items")
        if isinstance(mem_raw, list) and len(mem_raw) > 0:
            available_evidence.add("code_memory_items")
    policy_raw = accounting.get("policy_decisions")
    if isinstance(policy_raw, list) and len(policy_raw) > 0:
        available_evidence.add("policy_decisions")
    traces_raw = accounting.get("trace_spans")
    if isinstance(traces_raw, list) and len(traces_raw) > 0:
        available_evidence.add("trace_spans")
    if bool(verifier_passed):
        available_evidence.add("verifier_passed")
    if int(execution_exit_code) == 0:
        available_evidence.add("execution_passed")

    for dimension_id in ALLOWED_QUALITY_DIMENSION_IDS:
        spec = quality_contract.dimensions[dimension_id]
        score, reasons, evidence = _score_dimension(
            dimension_id=dimension_id,
            patch_text=patch_text,
            touched_paths=touched_paths,
            accounting=accounting,
            retrieved_context=retrieved_context,
            execution_exit_code=execution_exit_code,
            verifier_passed=verifier_passed,
        )
        missing_reqs: list[str] = []
        for req in spec.evidence_requirements:
            if req not in available_evidence:
                missing_reqs.append(req)
        if missing_reqs:
            score = _clamp01(score - min(0.2, 0.04 * float(len(missing_reqs))))
            reasons = (*reasons, f"missing_evidence_requirements:{','.join(sorted(missing_reqs))}")
            evidence = dict(evidence)
            evidence["missing_evidence_requirements"] = cast(JSONValue, sorted(missing_reqs))

        dim = QualityDimensionScore(
            dimension_id=dimension_id,
            score=float(score),
            target_score=float(spec.target_score),
            gate_min_score=(float(spec.gate_min_score) if spec.gate_min_score is not None else None),
            weight=float(spec.weight),
            enforcement_stage=str(spec.enforcement_stage),
            reasons=tuple(sorted({str(x) for x in reasons if str(x).strip()})),
            evidence=evidence,
        )
        dims.append(dim)
        total_weight += float(spec.weight)
        weighted_score += float(spec.weight) * float(score)

    overall = (weighted_score / total_weight) if total_weight > 0 else 0.0
    dims_sorted = tuple(sorted(dims, key=lambda d: d.dimension_id))
    return QualityScorecard(overall_weighted_score=_clamp01(overall), dimensions=dims_sorted)
