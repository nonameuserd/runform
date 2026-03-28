from __future__ import annotations

import json
from pathlib import Path

from akc.memory.salience import (
    SalienceCandidate,
    build_extractive_compaction,
    load_memory_policy,
    pack_by_token_budget,
    parse_memory_boost_overrides,
    score_candidates,
)


def test_salience_tie_break_deterministic_by_source_priority_then_id(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "memory_policy.json").write_text(
        json.dumps(
            {
                "weights": {"relevance": 1.0, "importance": 0.0, "reliability": 0.0, "recency": 0.0, "usage": 0.0},
                "source_priority": {"document": 3, "code_memory": 2, "default": 10},
            }
        ),
        encoding="utf-8",
    )
    policy = load_memory_policy(root=tmp_path, policy_path=None)
    cands = [
        SalienceCandidate(stable_id="document:b", source="document", text="alpha"),
        SalienceCandidate(stable_id="code_memory:a", source="code_memory", text="alpha"),
        SalienceCandidate(stable_id="document:a", source="document", text="alpha"),
    ]
    scored = score_candidates(candidates=cands, query="alpha", policy=policy, now_ms=1)
    assert [s.candidate.stable_id for s in scored] == ["code_memory:a", "document:a", "document:b"]


def test_salience_pinned_entries_survive_budget_pressure(tmp_path: Path) -> None:
    policy = load_memory_policy(root=tmp_path, policy_path=None)
    cands = [
        SalienceCandidate(
            stable_id="document:pin",
            source="document",
            text=("x " * 400).strip(),
            pinned=True,
            importance=0.1,
            reliability=0.1,
        ),
        SalienceCandidate(
            stable_id="document:small",
            source="document",
            text="x",
            importance=1.0,
            reliability=1.0,
        ),
    ]
    scored = score_candidates(candidates=cands, query="x", policy=policy, now_ms=1)
    selected, _ = pack_by_token_budget(scored=scored, budget_tokens=32)
    assert any(x.candidate.stable_id == "document:pin" for x in selected)


def test_salience_load_policy_overrides_defaults(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "memory_policy.json").write_text(
        json.dumps(
            {
                "weights": {"relevance": 0.5, "importance": 0.5, "reliability": 0, "recency": 0, "usage": 0},
                "token_budget": {"compile": 321, "assistant": 123},
                "pins": ["document:a"],
                "boosts": {"document:b": 0.3},
            }
        ),
        encoding="utf-8",
    )
    policy = load_memory_policy(root=tmp_path, policy_path=None)
    assert policy.budget_tokens(surface="compile") == 321
    assert "document:a" in policy.default_pins
    assert policy.default_boosts is not None and float(policy.default_boosts["document:b"]) == 0.3


def test_salience_parse_memory_boost_accepts_ids_with_colons() -> None:
    parsed = parse_memory_boost_overrides(["document:doc-1:0.75", "code_memory:item-2:-0.5"])
    assert parsed["document:doc-1"] == 0.75
    assert parsed["code_memory:item-2"] == -0.5


def test_salience_compaction_emits_source_citation_fields(tmp_path: Path) -> None:
    policy = load_memory_policy(root=tmp_path, policy_path=None)
    scored = score_candidates(
        candidates=[
            SalienceCandidate(stable_id="document:doc-1", source="document", text="doc text"),
            SalienceCandidate(stable_id="code_memory:item-2", source="code_memory", text="memory text"),
        ],
        query="",
        policy=policy,
        now_ms=1,
    )
    # Evict all to exercise compaction payload shape.
    compaction = build_extractive_compaction(evicted=scored, max_items=4, max_chars_per_item=120)
    entries = compaction.get("entries")
    assert isinstance(entries, list)
    assert entries and isinstance(entries[0], dict)
    first = entries[0]
    assert "citation" in first


def test_salience_half_life_decay_halves_score_at_half_life(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "memory_policy.json").write_text(
        json.dumps(
            {
                "weights": {"relevance": 0.0, "importance": 0.0, "reliability": 0.0, "recency": 1.0, "usage": 0.0},
                "half_life_days": {"document": 10.0, "default": 10.0},
            }
        ),
        encoding="utf-8",
    )
    policy = load_memory_policy(root=tmp_path, policy_path=None)
    now_ms = 10 * 86_400_000
    scored = score_candidates(
        candidates=[SalienceCandidate(stable_id="document:doc-1", source="document", text="x", created_at_ms=0)],
        query="",
        policy=policy,
        now_ms=now_ms,
    )
    assert len(scored) == 1
    assert abs(float(scored[0].score_breakdown["recency"]) - 0.5) < 1e-6
