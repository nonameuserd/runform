from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Literal, cast

from akc.ir.provenance import ProvenancePointer
from akc.memory.models import JSONValue, require_non_empty
from akc.utils.fingerprint import stable_json_fingerprint

AssertionKind = Literal["hard", "soft"]
Polarity = Literal[-1, 1]


def _require_polarity(value: int, *, name: str) -> int:
    if not isinstance(value, int) or value not in (-1, 1):
        raise ValueError(f"{name} must be an int in {{-1, 1}}; got {value!r}")
    return value


def _sorted_unique_str(values: Any) -> tuple[str, ...]:
    if not isinstance(values, (list, tuple)):
        raise ValueError("values must be a list/tuple of strings")
    out: list[str] = []
    for v in values:
        if not isinstance(v, str) or not v.strip():
            raise ValueError("all values must be non-empty strings")
        out.append(v.strip())
    # Order-insensitive for deterministic fingerprints.
    return tuple(sorted(set(out)))


@dataclass(frozen=True, slots=True)
class CanonicalConstraint:
    """Canonicalized constraint semantics for stable assertion IDs.

    This is intentionally evidence-agnostic: IDs are derived only from the
    canonical semantics.
    """

    subject: str
    predicate: str
    object: str | None
    polarity: int
    scope: str
    kind: AssertionKind
    summary: str

    assertion_id: str = field(init=False)
    semantic_fingerprint: str = field(init=False)

    def __post_init__(self) -> None:
        require_non_empty(self.subject, name="canonical_constraint.subject")
        require_non_empty(self.predicate, name="canonical_constraint.predicate")
        require_non_empty(self.scope, name="canonical_constraint.scope")
        require_non_empty(self.summary, name="canonical_constraint.summary")

        if self.kind not in ("hard", "soft"):
            raise ValueError("canonical_constraint.kind must be 'hard' or 'soft'")

        obj = self.object
        if obj is not None and (not isinstance(obj, str) or not obj.strip()):
            raise ValueError("canonical_constraint.object must be None or non-empty string")

        p = _require_polarity(self.polarity, name="canonical_constraint.polarity")

        payload = {
            "subject": self.subject.strip(),
            "predicate": self.predicate.strip(),
            "object": obj.strip() if isinstance(obj, str) else None,
            "polarity": int(p),
            "scope": self.scope.strip(),
            "kind": str(self.kind),
            "summary": self.summary.strip(),
        }
        sem_fp = stable_json_fingerprint(payload)
        object.__setattr__(self, "assertion_id", f"assertion_{sem_fp[:16]}")
        object.__setattr__(self, "semantic_fingerprint", sem_fp)

    def to_semantics_json_obj(self) -> dict[str, JSONValue]:
        obj = self.object
        payload: dict[str, JSONValue] = {
            "subject": self.subject.strip(),
            "predicate": self.predicate.strip(),
            "object": obj.strip() if isinstance(obj, str) else None,
            "polarity": int(self.polarity),
            "scope": self.scope.strip(),
            "kind": self.kind,
            "summary": self.summary.strip(),
        }
        # Exclude computed fields from semantic hashing payload.
        return payload

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "assertion_id": self.assertion_id,
            "semantic_fingerprint": self.semantic_fingerprint,
            **self.to_semantics_json_obj(),
        }
        return out

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> CanonicalConstraint:
        kind_raw = str(obj.get("kind", "hard"))
        if kind_raw not in ("hard", "soft"):
            kind_raw = "hard"
        return CanonicalConstraint(
            subject=str(obj.get("subject", "")),
            predicate=str(obj.get("predicate", "")),
            object=obj.get("object") if obj.get("object") is None else str(obj.get("object")),
            polarity=int(obj.get("polarity", 0)),
            scope=str(obj.get("scope", "")),
            kind=cast(AssertionKind, kind_raw),
            summary=str(obj.get("summary", "")),
        )


@dataclass(frozen=True, slots=True)
class CanonicalDecision:
    """Canonical decision record for a constraint/assertion."""

    assertion_id: str
    selected: bool
    resolved: bool
    conflict_resolution_target_assertion_ids: tuple[str, ...] = ()
    evidence_doc_ids: tuple[str, ...] = ()
    # B3: optional operator-authored rationale (included in semantic fingerprint when set).
    rationale: str | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.assertion_id, name="canonical_decision.assertion_id")

        # Sanitization + deterministic ordering.
        targets_in = self.conflict_resolution_target_assertion_ids
        if not isinstance(targets_in, tuple):
            object.__setattr__(self, "conflict_resolution_target_assertion_ids", tuple(targets_in))
            targets_in = self.conflict_resolution_target_assertion_ids
        object.__setattr__(
            self,
            "conflict_resolution_target_assertion_ids",
            _sorted_unique_str(list(targets_in)),
        )

        evidence_in = self.evidence_doc_ids
        if not isinstance(evidence_in, tuple):
            object.__setattr__(self, "evidence_doc_ids", tuple(evidence_in))
            evidence_in = self.evidence_doc_ids
        object.__setattr__(
            self,
            "evidence_doc_ids",
            _sorted_unique_str(list(evidence_in)),
        )

    def to_json_obj(self) -> dict[str, JSONValue]:
        out: dict[str, JSONValue] = {
            "assertion_id": self.assertion_id,
            "selected": bool(self.selected),
            "resolved": bool(self.resolved),
            "conflict_resolution_target_assertion_ids": list(self.conflict_resolution_target_assertion_ids),
            "evidence_doc_ids": list(self.evidence_doc_ids),
        }
        if self.rationale is not None and str(self.rationale).strip():
            out["rationale"] = str(self.rationale).strip()
        return out

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> CanonicalDecision:
        rat_raw = obj.get("rationale")
        rationale = str(rat_raw).strip() if isinstance(rat_raw, str) and str(rat_raw).strip() else None
        return CanonicalDecision(
            assertion_id=str(obj.get("assertion_id", "")),
            selected=bool(obj.get("selected", False)),
            resolved=bool(obj.get("resolved", False)),
            conflict_resolution_target_assertion_ids=tuple(
                [
                    str(x).strip()
                    for x in (obj.get("conflict_resolution_target_assertion_ids") or [])
                    if isinstance(x, str) and x.strip()
                ]
            ),
            evidence_doc_ids=tuple([str(x).strip() for x in (obj.get("evidence_doc_ids") or []) if str(x).strip()]),
            rationale=rationale,
        )


@dataclass(frozen=True, slots=True)
class EvidenceMapping:
    """Evidence mapping for a single canonical assertion."""

    evidence_doc_ids: tuple[str, ...]
    resolved_provenance_pointers: tuple[ProvenancePointer, ...]

    def __post_init__(self) -> None:
        # Enforce deterministic, sanitized doc ids.
        object.__setattr__(self, "evidence_doc_ids", _sorted_unique_str(self.evidence_doc_ids))
        if not isinstance(self.resolved_provenance_pointers, tuple):
            object.__setattr__(
                self,
                "resolved_provenance_pointers",
                tuple(self.resolved_provenance_pointers),
            )

        for ptr in self.resolved_provenance_pointers:
            if not isinstance(ptr, ProvenancePointer):
                raise ValueError("resolved_provenance_pointers must be ProvenancePointer instances")

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "evidence_doc_ids": list(self.evidence_doc_ids),
            "resolved_provenance_pointers": [ptr.to_json_obj() for ptr in self.resolved_provenance_pointers],
        }

    @staticmethod
    def from_json_obj(obj: dict[str, Any]) -> EvidenceMapping:
        doc_ids_raw = obj.get("evidence_doc_ids") or []
        if not isinstance(doc_ids_raw, list):
            raise ValueError("evidence_doc_ids must be a list")
        ptrs_raw = obj.get("resolved_provenance_pointers") or []
        if not isinstance(ptrs_raw, list):
            raise ValueError("resolved_provenance_pointers must be a list")

        ptrs: list[ProvenancePointer] = []
        for p in ptrs_raw:
            if not isinstance(p, dict):
                raise ValueError("resolved_provenance_pointers[] must be objects")
            ptrs.append(ProvenancePointer.from_json_obj(p))

        return EvidenceMapping(
            evidence_doc_ids=_sorted_unique_str(doc_ids_raw),
            resolved_provenance_pointers=tuple(ptrs),
        )

    def to_provenance_pointer_fingerprint_items(self) -> list[dict[str, JSONValue]]:
        """Order-insensitive items used for provenance fingerprinting.

        Includes only `doc_id` + `sha256` (per requested canonical shape).
        """

        items: list[dict[str, JSONValue]] = []
        for ptr in self.resolved_provenance_pointers:
            items.append({"doc_id": ptr.source_id, "sha256": ptr.sha256})
        # Make deterministic across runs even if input pointer order changes.
        items.sort(key=lambda x: (str(x.get("doc_id", "")), str(x.get("sha256", ""))))
        return items


@dataclass(frozen=True, slots=True)
class KnowledgeSnapshot:
    """Knowledge layer snapshot with canonical semantics + evidence mapping."""

    canonical_constraints: tuple[CanonicalConstraint, ...]
    canonical_decisions: tuple[CanonicalDecision, ...]
    evidence_by_assertion: dict[str, EvidenceMapping]
    # Optional per-assertion scores used for deterministic conflict resolution and why-graph
    # alignment. When empty, resolvers infer strength from evidence_doc_id counts.
    evidence_strength_by_assertion: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.canonical_constraints, tuple):
            raise ValueError("canonical_constraints must be a tuple")
        if not isinstance(self.canonical_decisions, tuple):
            raise ValueError("canonical_decisions must be a tuple")
        if not isinstance(self.evidence_by_assertion, dict):
            raise ValueError("evidence_by_assertion must be a dict")

        seen_constraints: set[str] = set()
        for c in self.canonical_constraints:
            if not isinstance(c, CanonicalConstraint):
                raise ValueError("canonical_constraints[] must be CanonicalConstraint")
            if c.assertion_id in seen_constraints:
                raise ValueError(f"duplicate canonical constraint assertion_id: {c.assertion_id}")
            seen_constraints.add(c.assertion_id)

        seen_decisions: set[str] = set()
        for d in self.canonical_decisions:
            if d.assertion_id not in seen_constraints:
                raise ValueError(
                    f"canonical_decision.assertion_id={d.assertion_id!r} not present in canonical_constraints"
                )
            if d.assertion_id in seen_decisions:
                raise ValueError(f"duplicate canonical decision for assertion_id: {d.assertion_id}")
            seen_decisions.add(d.assertion_id)

        for assertion_id, mapping in self.evidence_by_assertion.items():
            if not isinstance(assertion_id, str) or not assertion_id.strip():
                raise ValueError("evidence_by_assertion keys must be non-empty strings")
            aid = assertion_id.strip()
            if aid not in seen_constraints:
                raise ValueError(f"evidence_by_assertion has key {aid!r} not present in canonical_constraints")
            if not isinstance(mapping, EvidenceMapping):
                raise ValueError("evidence_by_assertion values must be EvidenceMapping")

        # Enforce sanitized keys: avoid accidental whitespace differences.
        sanitized = {k.strip(): v for k, v in self.evidence_by_assertion.items()}
        object.__setattr__(self, "evidence_by_assertion", sanitized)

        strength_sanitized: dict[str, float] = {}
        for raw_k, raw_v in self.evidence_strength_by_assertion.items():
            if not isinstance(raw_k, str) or not raw_k.strip():
                raise ValueError("evidence_strength_by_assertion keys must be non-empty strings")
            k = raw_k.strip()
            if k not in seen_constraints:
                raise ValueError(f"evidence_strength_by_assertion key {k!r} not present in canonical_constraints")
            if not isinstance(raw_v, (int, float)) or not math.isfinite(float(raw_v)):
                raise ValueError(f"evidence_strength_by_assertion[{k!r}] must be a finite float")
            strength_sanitized[k] = float(raw_v)
        object.__setattr__(self, "evidence_strength_by_assertion", strength_sanitized)

    def to_json_obj(self) -> dict[str, JSONValue]:
        evidence_sorted: list[dict[str, JSONValue]] = []
        for assertion_id in sorted(self.evidence_by_assertion.keys()):
            evidence_sorted.append(
                {
                    "assertion_id": assertion_id,
                    "evidence": self.evidence_by_assertion[assertion_id].to_json_obj(),
                }
            )
        out: dict[str, JSONValue] = {
            "canonical_constraints": cast(JSONValue, [c.to_json_obj() for c in self.canonical_constraints]),
            "canonical_decisions": cast(JSONValue, [d.to_json_obj() for d in self.canonical_decisions]),
            "evidence_by_assertion": cast(JSONValue, evidence_sorted),
        }
        if self.evidence_strength_by_assertion:
            out["evidence_strength_by_assertion"] = {
                k: float(v) for k, v in sorted(self.evidence_strength_by_assertion.items())
            }
        return out

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> KnowledgeSnapshot:
        if not isinstance(obj, Mapping):
            raise ValueError("knowledge_snapshot must be a JSON object")

        constraints_raw = obj.get("canonical_constraints") or []
        if not isinstance(constraints_raw, list):
            raise ValueError("canonical_constraints must be a list")
        constraints: list[CanonicalConstraint] = []
        for i, c in enumerate(constraints_raw):
            if not isinstance(c, dict):
                raise ValueError(f"canonical_constraints[{i}] must be an object")
            constraints.append(CanonicalConstraint.from_json_obj(c))

        decisions_raw = obj.get("canonical_decisions") or []
        if not isinstance(decisions_raw, list):
            raise ValueError("canonical_decisions must be a list")
        decisions: list[CanonicalDecision] = []
        for i, d in enumerate(decisions_raw):
            if not isinstance(d, dict):
                raise ValueError(f"canonical_decisions[{i}] must be an object")
            decisions.append(CanonicalDecision.from_json_obj(d))

        evidence_by_assertion: dict[str, EvidenceMapping] = {}
        evidence_raw = obj.get("evidence_by_assertion")
        if isinstance(evidence_raw, list):
            for i, entry in enumerate(evidence_raw):
                if not isinstance(entry, dict):
                    raise ValueError(f"evidence_by_assertion[{i}] must be an object")
                aid_raw = entry.get("assertion_id")
                if not isinstance(aid_raw, str) or not aid_raw.strip():
                    raise ValueError(f"evidence_by_assertion[{i}].assertion_id must be a non-empty string")
                ev = entry.get("evidence")
                if not isinstance(ev, dict):
                    raise ValueError(f"evidence_by_assertion[{i}].evidence must be an object")
                evidence_by_assertion[aid_raw.strip()] = EvidenceMapping.from_json_obj(ev)
        elif isinstance(evidence_raw, dict):
            # Backward compatibility: map-shaped evidence (not emitted by to_json_obj).
            for aid, ev in evidence_raw.items():
                if not isinstance(aid, str) or not aid.strip():
                    raise ValueError("evidence_by_assertion keys must be non-empty strings")
                if not isinstance(ev, dict):
                    raise ValueError("evidence_by_assertion values must be objects")
                evidence_by_assertion[aid.strip()] = EvidenceMapping.from_json_obj(ev)
        elif evidence_raw not in (None, {}):
            raise ValueError("evidence_by_assertion must be a list or object")

        strength: dict[str, float] = {}
        strength_raw = obj.get("evidence_strength_by_assertion")
        if isinstance(strength_raw, dict):
            for k, v in strength_raw.items():
                if not isinstance(k, str) or not k.strip():
                    raise ValueError("evidence_strength_by_assertion keys must be non-empty strings")
                if not isinstance(v, (int, float)) or not math.isfinite(float(v)):
                    raise ValueError(f"evidence_strength_by_assertion[{k!r}] must be a finite float")
                strength[k.strip()] = float(v)

        return KnowledgeSnapshot(
            canonical_constraints=tuple(constraints),
            canonical_decisions=tuple(decisions),
            evidence_by_assertion=evidence_by_assertion,
            evidence_strength_by_assertion=strength,
        )


def knowledge_semantic_fingerprint(*, snapshot: KnowledgeSnapshot) -> str:
    """Order-insensitive semantic fingerprint (evidence-agnostic)."""

    constraints_sorted = sorted(snapshot.canonical_constraints, key=lambda c: c.assertion_id)
    decisions_sorted = sorted(snapshot.canonical_decisions, key=lambda d: d.assertion_id)

    payload: dict[str, JSONValue] = {
        "canonical_constraints": [c.to_semantics_json_obj() for c in constraints_sorted],
        "canonical_decisions": [d.to_json_obj() for d in decisions_sorted],
    }
    return stable_json_fingerprint(payload)


def knowledge_provenance_fingerprint(*, snapshot: KnowledgeSnapshot) -> str:
    """Order-insensitive provenance fingerprint (includes evidence pointers)."""

    constraints_sorted = sorted(snapshot.canonical_constraints, key=lambda c: c.assertion_id)
    decisions_sorted = sorted(snapshot.canonical_decisions, key=lambda d: d.assertion_id)
    evidence_sorted_items: list[dict[str, JSONValue]] = []
    for assertion_id in sorted(snapshot.evidence_by_assertion.keys()):
        m = snapshot.evidence_by_assertion[assertion_id]
        evidence_sorted_items.append(
            {
                "assertion_id": assertion_id,
                "evidence_doc_ids": cast(JSONValue, list(m.evidence_doc_ids)),
                # Fingerprint items ignore pointer ordering.
                "resolved_provenance_pointers": cast(JSONValue, m.to_provenance_pointer_fingerprint_items()),
            }
        )

    payload: dict[str, JSONValue] = {
        "canonical_constraints": cast(JSONValue, [c.to_semantics_json_obj() for c in constraints_sorted]),
        "canonical_decisions": cast(JSONValue, [d.to_json_obj() for d in decisions_sorted]),
        "evidence_by_assertion": cast(JSONValue, evidence_sorted_items),
    }
    return stable_json_fingerprint(payload)
