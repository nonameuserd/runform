from __future__ import annotations

import json
from pathlib import Path

from akc.knowledge.observability import (
    build_knowledge_observation_payload,
    compute_unresolved_knowledge_conflict_count,
    group_mediation_events_by_conflict_group,
    summarize_knowledge_governance,
)


def test_build_knowledge_observation_payload_groups_and_supersession() -> None:
    env = {
        "schema_kind": "akc_knowledge_mediation_report",
        "mediation_report": {
            "events": [
                {
                    "kind": "supersedes",
                    "conflict_group_id": "cg-a",
                    "winner_assertion_id": "w1",
                    "loser_assertion_id": "l1",
                },
                {
                    "kind": "ambiguous_conflict_resolution",
                    "conflict_group_id": "cg-b",
                    "mediation_resolved": False,
                    "chosen_assertion_id": "x",
                },
            ]
        },
    }
    out = build_knowledge_observation_payload(
        knowledge_envelope=None,
        conflict_reports=(),
        knowledge_mediation_envelope=env,
    )
    assert len(out["mediation_events"]) == 2
    assert set(out["conflict_groups"].keys()) == {"cg-a", "cg-b"}
    assert len(out["supersession_hints"]) == 1
    assert out["supersession_hints"][0]["winner_assertion_id"] == "w1"
    assert out["unresolved_knowledge_conflicts_count"] == 1
    assert ".akc/knowledge/mediation.json" in out["knowledge_paths"]["mediation"]


def test_compute_unresolved_counts_distinct_groups() -> None:
    ev = [
        {"mediation_resolved": False, "conflict_group_id": "g1"},
        {"mediation_resolved": False, "conflict_group_id": "g1"},
        {"mediation_resolved": False, "conflict_group_id": "g2"},
        {"mediation_resolved": True, "conflict_group_id": "g3"},
    ]
    assert compute_unresolved_knowledge_conflict_count(ev) == 2


def test_group_mediation_events_ungrouped_bucket() -> None:
    ev = [{"kind": "x", "mediation_resolved": False}]
    g = group_mediation_events_by_conflict_group(ev)
    assert "__ungrouped__" in g


def test_summarize_knowledge_governance_reads_mediation(tmp_path: Path) -> None:
    kd = tmp_path / ".akc" / "knowledge"
    kd.mkdir(parents=True)
    report = {
        "events": [
            {
                "kind": "ambiguous_conflict_resolution",
                "conflict_group_id": "cg",
                "mediation_resolved": False,
            }
        ]
    }
    (kd / "mediation.json").write_text(
        json.dumps(
            {
                "schema_kind": "akc_knowledge_mediation_report",
                "schema_version": 1,
                "tenant_id": "t",
                "repo_id": "r",
                "mediation_report": report,
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    s = summarize_knowledge_governance(scope_root=tmp_path)
    assert s["unresolved_knowledge_conflicts_count"] == 1
    assert s["knowledge_paths_present"]["mediation"] is True
    assert s["knowledge_paths_present"]["snapshot"] is False
