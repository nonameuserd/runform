from __future__ import annotations

import json
import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast

from akc.memory.models import JSONValue
from akc.path_security import safe_resolve_path, safe_resolve_scoped_path
from akc.utils.fingerprint import stable_json_fingerprint

MemorySurface = Literal["compile", "assistant"]

_DEFAULT_WEIGHTS: dict[str, float] = {
    "relevance": 0.40,
    "importance": 0.20,
    "reliability": 0.15,
    "recency": 0.15,
    "usage": 0.10,
}

_DEFAULT_HALF_LIFE_DAYS: dict[str, float] = {
    "code_memory": 14.0,
    "document": 60.0,
    "assistant_turn": 7.0,
    "assistant_command": 7.0,
    "knowledge": 30.0,
    "default": 30.0,
}

_DEFAULT_SOURCE_PRIORITY: dict[str, int] = {
    "assistant_command": 0,
    "assistant_turn": 1,
    "code_memory": 2,
    "document": 3,
    "knowledge": 4,
    "default": 10,
}

_DEFAULT_TOKEN_BUDGET: dict[str, int] = {
    "compile": 1200,
    "assistant": 900,
}


@dataclass(frozen=True, slots=True)
class MemoryPolicy:
    weights: Mapping[str, float]
    half_life_days_by_source: Mapping[str, float]
    source_priority: Mapping[str, int]
    token_budget_by_surface: Mapping[str, int]
    pinned_bonus: float = 2.0
    default_pins: tuple[str, ...] = ()
    default_boosts: Mapping[str, float] | None = None
    path: str | None = None
    score_version: str = "salience-v1"

    def fingerprint(self) -> str:
        obj: dict[str, JSONValue] = {
            "weights": {str(k): float(v) for k, v in sorted(self.weights.items())},
            "half_life_days_by_source": {str(k): float(v) for k, v in sorted(self.half_life_days_by_source.items())},
            "source_priority": {str(k): int(v) for k, v in sorted(self.source_priority.items())},
            "token_budget_by_surface": {str(k): int(v) for k, v in sorted(self.token_budget_by_surface.items())},
            "pinned_bonus": float(self.pinned_bonus),
            "default_pins": cast(
                JSONValue,
                sorted({str(x).strip() for x in self.default_pins if str(x).strip()}),
            ),
            "default_boosts": (
                {str(k): float(v) for k, v in sorted((self.default_boosts or {}).items())}
                if self.default_boosts
                else {}
            ),
            "score_version": str(self.score_version),
        }
        return stable_json_fingerprint(obj)

    def budget_tokens(self, *, surface: MemorySurface, runtime_override: int | None = None) -> int:
        if runtime_override is not None and int(runtime_override) > 0:
            return int(runtime_override)
        raw = self.token_budget_by_surface.get(surface)
        if isinstance(raw, int) and raw > 0:
            return int(raw)
        return int(_DEFAULT_TOKEN_BUDGET[surface])


@dataclass(frozen=True, slots=True)
class SalienceCandidate:
    stable_id: str
    source: str
    text: str
    created_at_ms: int | None = None
    last_used_at_ms: int | None = None
    use_count: int = 0
    pinned: bool = False
    relevance_hint: float | None = None
    importance: float = 0.5
    reliability: float = 0.5
    explicit_boost: float = 0.0
    metadata: Mapping[str, JSONValue] | None = None

    def token_estimate(self) -> int:
        return estimate_token_count(self.text)


@dataclass(frozen=True, slots=True)
class ScoredCandidate:
    candidate: SalienceCandidate
    total_score: float
    score_breakdown: Mapping[str, float]
    source_priority: int
    token_estimate: int


def estimate_token_count(text: str) -> int:
    s = str(text or "")
    if not s:
        return 1
    return max(1, int(math.ceil(len(s) / 4.0)))


def parse_memory_boost_overrides(values: Sequence[str] | None) -> dict[str, float]:
    out: dict[str, float] = {}
    for raw in values or ():
        text = str(raw).strip()
        if not text:
            continue
        if ":" not in text:
            raise ValueError(f"memory boost must be <id>:<float>, got {raw!r}")
        key, val = text.rsplit(":", 1)
        kid = str(key).strip()
        if not kid:
            raise ValueError(f"memory boost id must be non-empty: {raw!r}")
        try:
            f = float(val)
        except ValueError as exc:
            raise ValueError(f"memory boost value must be float: {raw!r}") from exc
        if not math.isfinite(f):
            raise ValueError(f"memory boost value must be finite: {raw!r}")
        out[kid] = float(f)
    return out


def parse_memory_pin_overrides(values: Sequence[str] | None) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in values or ():
        key = str(raw).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return tuple(out)


def load_memory_policy(
    *,
    root: Path,
    policy_path: str | None,
) -> MemoryPolicy:
    base = safe_resolve_path(root)
    if policy_path is not None and str(policy_path).strip():
        candidate = Path(policy_path).expanduser()
    else:
        candidate = safe_resolve_scoped_path(base, ".akc", "memory_policy.json")
    payload: dict[str, Any] = {}
    if candidate.is_file():
        raw = json.loads(candidate.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("memory policy must be a JSON object")
        payload = raw

    weights = dict(_DEFAULT_WEIGHTS)
    raw_weights = payload.get("weights")
    if isinstance(raw_weights, Mapping):
        for k in weights:
            v = raw_weights.get(k)
            if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v)):
                weights[k] = float(v)
    total = sum(max(0.0, float(v)) for v in weights.values())
    weights = {k: max(0.0, float(v)) / total for k, v in weights.items()} if total > 0 else dict(_DEFAULT_WEIGHTS)

    half_life = dict(_DEFAULT_HALF_LIFE_DAYS)
    raw_hl = payload.get("half_life_days")
    if isinstance(raw_hl, Mapping):
        for k, v in raw_hl.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool) and float(v) >= 0.0:
                half_life[str(k).strip()] = float(v)

    src_priority = dict(_DEFAULT_SOURCE_PRIORITY)
    raw_pri = payload.get("source_priority")
    if isinstance(raw_pri, Mapping):
        for k, v in raw_pri.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool):
                src_priority[str(k).strip()] = int(v)

    token_budget = dict(_DEFAULT_TOKEN_BUDGET)
    raw_budget = payload.get("token_budget")
    if isinstance(raw_budget, Mapping):
        for surface in ("compile", "assistant"):
            v = raw_budget.get(surface)
            if isinstance(v, (int, float)) and not isinstance(v, bool) and int(v) > 0:
                token_budget[surface] = int(v)

    pinned_bonus = 2.0
    raw_pb = payload.get("pinned_bonus")
    if isinstance(raw_pb, (int, float)) and not isinstance(raw_pb, bool) and math.isfinite(float(raw_pb)):
        pinned_bonus = float(raw_pb)

    default_pins = parse_memory_pin_overrides(payload.get("pins") if isinstance(payload.get("pins"), list) else None)
    default_boosts: dict[str, float] | None = None
    raw_boosts = payload.get("boosts")
    if isinstance(raw_boosts, Mapping):
        default_boosts = {}
        for k, v in raw_boosts.items():
            if isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(float(v)):
                key = str(k).strip()
                if key:
                    default_boosts[key] = float(v)

    return MemoryPolicy(
        weights=weights,
        half_life_days_by_source=half_life,
        source_priority=src_priority,
        token_budget_by_surface=token_budget,
        pinned_bonus=pinned_bonus,
        default_pins=default_pins,
        default_boosts=default_boosts,
        path=str(candidate) if candidate.is_file() else None,
    )


def _tokenize(text: str) -> set[str]:
    out: set[str] = set()
    token = []
    for ch in str(text).lower():
        if ch.isalnum() or ch == "_":
            token.append(ch)
            continue
        if token:
            t = "".join(token)
            if len(t) > 1:
                out.add(t)
            token = []
    if token:
        t = "".join(token)
        if len(t) > 1:
            out.add(t)
    return out


def _lexical_relevance(*, query: str, text: str) -> float:
    q = _tokenize(query)
    if not q:
        return 0.0
    d = _tokenize(text)
    if not d:
        return 0.0
    inter = len(q.intersection(d))
    if inter <= 0:
        return 0.0
    return float(inter / max(1, len(q)))


def _recency_score(
    *,
    now_ms: int,
    candidate: SalienceCandidate,
    policy: MemoryPolicy,
) -> float:
    if candidate.last_used_at_ms is not None:
        ts = int(candidate.last_used_at_ms)
    elif candidate.created_at_ms is not None:
        ts = int(candidate.created_at_ms)
    else:
        ts = int(now_ms)
    age_days = max(0.0, (float(now_ms) - float(ts)) / 86_400_000.0)
    hl = float(
        policy.half_life_days_by_source.get(
            candidate.source,
            policy.half_life_days_by_source.get("default", _DEFAULT_HALF_LIFE_DAYS["default"]),
        )
    )
    if hl <= 0:
        return 0.0
    # True half-life decay: score halves when age == half-life.
    return float(math.exp(math.log(0.5) * (age_days / hl)))


def _usage_score(use_count: int) -> float:
    return float(min(1.0, math.log1p(max(0, int(use_count))) / math.log(10.0)))


def score_candidates(
    *,
    candidates: Sequence[SalienceCandidate],
    query: str,
    policy: MemoryPolicy,
    now_ms: int,
    pins: Sequence[str] | None = None,
    boosts: Mapping[str, float] | None = None,
) -> list[ScoredCandidate]:
    merged_pins = set(policy.default_pins).union(set(parse_memory_pin_overrides(pins)))
    merged_boosts: dict[str, float] = dict(policy.default_boosts or {})
    for k, v in (boosts or {}).items():
        key = str(k).strip()
        if key and math.isfinite(float(v)):
            merged_boosts[key] = float(v)

    out: list[ScoredCandidate] = []
    for cand in candidates:
        rel = (
            float(cand.relevance_hint)
            if cand.relevance_hint is not None
            else _lexical_relevance(query=query, text=cand.text)
        )
        imp = max(0.0, min(1.0, float(cand.importance)))
        relia = max(0.0, min(1.0, float(cand.reliability)))
        rec = _recency_score(now_ms=now_ms, candidate=cand, policy=policy)
        usg = _usage_score(cand.use_count)
        boost = float(cand.explicit_boost) + float(merged_boosts.get(cand.stable_id, 0.0))
        pinned = bool(cand.pinned or cand.stable_id in merged_pins)
        if pinned:
            boost += float(policy.pinned_bonus)
        total = (
            float(policy.weights.get("relevance", _DEFAULT_WEIGHTS["relevance"])) * rel
            + float(policy.weights.get("importance", _DEFAULT_WEIGHTS["importance"])) * imp
            + float(policy.weights.get("reliability", _DEFAULT_WEIGHTS["reliability"])) * relia
            + float(policy.weights.get("recency", _DEFAULT_WEIGHTS["recency"])) * rec
            + float(policy.weights.get("usage", _DEFAULT_WEIGHTS["usage"])) * usg
            + boost
        )
        pri = int(policy.source_priority.get(cand.source, policy.source_priority.get("default", 10)))
        breakdown = {
            "relevance": rel,
            "importance": imp,
            "reliability": relia,
            "recency": rec,
            "usage": usg,
            "boost": boost,
            "pinned": 1.0 if pinned else 0.0,
        }
        out.append(
            ScoredCandidate(
                candidate=SalienceCandidate(
                    stable_id=cand.stable_id,
                    source=cand.source,
                    text=cand.text,
                    created_at_ms=cand.created_at_ms,
                    last_used_at_ms=cand.last_used_at_ms,
                    use_count=cand.use_count,
                    pinned=pinned,
                    relevance_hint=cand.relevance_hint,
                    importance=cand.importance,
                    reliability=cand.reliability,
                    explicit_boost=cand.explicit_boost,
                    metadata=cand.metadata,
                ),
                total_score=total,
                score_breakdown=breakdown,
                source_priority=pri,
                token_estimate=cand.token_estimate(),
            )
        )
    out.sort(key=lambda s: (-float(s.total_score), int(s.source_priority), str(s.candidate.stable_id)))
    return out


def pack_by_token_budget(
    *,
    scored: Sequence[ScoredCandidate],
    budget_tokens: int,
) -> tuple[list[ScoredCandidate], list[ScoredCandidate]]:
    budget = max(1, int(budget_tokens))
    selected: list[ScoredCandidate] = []
    evicted: list[ScoredCandidate] = []
    used = 0

    # First, reserve pinned entries as non-evictable.
    for item in scored:
        if not item.candidate.pinned:
            continue
        selected.append(item)
        used += item.token_estimate

    for item in scored:
        if item in selected or item in evicted:
            continue
        if used + item.token_estimate <= budget:
            selected.append(item)
            used += item.token_estimate
        else:
            evicted.append(item)
    return selected, evicted


def build_extractive_compaction(
    *,
    evicted: Sequence[ScoredCandidate],
    max_items: int = 8,
    max_chars_per_item: int = 180,
) -> dict[str, JSONValue]:
    def _extract_citation(item: ScoredCandidate) -> dict[str, JSONValue] | None:
        meta = item.candidate.metadata if isinstance(item.candidate.metadata, Mapping) else None
        if meta is not None:
            nested = meta.get("citation")
            if isinstance(nested, Mapping):
                out = {
                    str(k): v
                    for k, v in nested.items()
                    if str(k).strip() and isinstance(v, (str, int, float, bool, dict, list, type(None)))
                }
                if out:
                    return out
            for key in ("doc_id", "item_id", "assertion_id", "constraint_id"):
                raw = meta.get(key)
                if isinstance(raw, str) and raw.strip():
                    return {key: raw.strip()}

        sid = str(item.candidate.stable_id)
        if ":" in sid:
            source, ident = sid.split(":", 1)
            if source == "document":
                return {"doc_id": ident}
            if source == "code_memory":
                return {"item_id": ident}
            if source == "knowledge":
                return {"assertion_id": ident}
        return {"memory_id": sid}

    rows: list[dict[str, JSONValue]] = []
    for item in evicted[: max(0, int(max_items))]:
        snippet = " ".join(str(item.candidate.text).split())
        if len(snippet) > max_chars_per_item:
            snippet = snippet[: max_chars_per_item - 3] + "..."
        citation = _extract_citation(item)
        rows.append(
            {
                "memory_id": item.candidate.stable_id,
                "source": item.candidate.source,
                "summary": snippet,
                "score": float(item.total_score),
                "token_estimate": int(item.token_estimate),
                "citation": citation,
            }
        )
    return {
        "mode": "extractive_with_citations",
        "entries": cast(list[JSONValue], rows),
        "count": len(rows),
    }
