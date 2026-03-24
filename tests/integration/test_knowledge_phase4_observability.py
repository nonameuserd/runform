from __future__ import annotations

import json
from pathlib import Path

from akc.compile.interfaces import TenantRepoScope
from akc.knowledge import (
    CanonicalConstraint,
    CanonicalDecision,
    EvidenceMapping,
    KnowledgeSnapshot,
)
from akc.knowledge.observability import (
    build_knowledge_observation_payload,
    compute_knowledge_governance_counts,
    summarize_knowledge_governance,
)
from akc.knowledge.persistence import (
    write_knowledge_mediation_report_artifact,
    write_knowledge_snapshot_artifacts,
)
from akc.memory.plan_state import JsonFilePlanStateStore
from akc.outputs.emitters import JsonManifestEmitter
from akc.outputs.models import OutputArtifact, OutputBundle
from akc.viewer import ViewerInputs, load_viewer_snapshot
from akc.viewer.export import export_bundle


def _phase4_sample_snapshot() -> tuple[KnowledgeSnapshot, frozenset[str]]:
    """One hard (intent), two soft — one maps to intent ids, one simulates doc-derived."""

    hard = CanonicalConstraint(
        subject="api",
        predicate="forbidden",
        object=None,
        polarity=1,
        scope="repo-x",
        kind="hard",
        summary="Network egress to third parties is forbidden for this service.",
    )
    soft_intent = CanonicalConstraint(
        subject="logs",
        predicate="required",
        object=None,
        polarity=1,
        scope="repo-x",
        kind="soft",
        summary="Audit logs must be retained for 90 days per policy.",
    )
    soft_doc = CanonicalConstraint(
        subject="widgets",
        predicate="must",
        object=None,
        polarity=1,
        scope="repo-x",
        kind="soft",
        summary="The component MUST validate all widget identifiers before persistence.",
    )
    # Mirrors compile-time intent map keys (intent-sourced assertions only in this fixture).
    intent_ids = frozenset({hard.assertion_id, soft_intent.assertion_id})
    decisions = (
        CanonicalDecision(
            assertion_id=hard.assertion_id,
            selected=True,
            resolved=True,
            evidence_doc_ids=("doc-a",),
        ),
        CanonicalDecision(
            assertion_id=soft_intent.assertion_id,
            selected=True,
            resolved=True,
            evidence_doc_ids=("doc-b",),
        ),
        CanonicalDecision(
            assertion_id=soft_doc.assertion_id,
            selected=True,
            resolved=True,
            evidence_doc_ids=("doc-c",),
        ),
    )
    ev = {
        hard.assertion_id: EvidenceMapping(evidence_doc_ids=("doc-a",), resolved_provenance_pointers=()),
        soft_intent.assertion_id: EvidenceMapping(evidence_doc_ids=("doc-b",), resolved_provenance_pointers=()),
        soft_doc.assertion_id: EvidenceMapping(evidence_doc_ids=("doc-c",), resolved_provenance_pointers=()),
    }
    snap = KnowledgeSnapshot(
        canonical_constraints=(hard, soft_intent, soft_doc),
        canonical_decisions=decisions,
        evidence_by_assertion=ev,
    )
    return snap, intent_ids


def test_viewer_snapshot_and_export_surface_mediation_observability(tmp_path: Path) -> None:
    """Phase 4: mediation artifact → viewer snapshot → knowledge_obs.json (integration-style)."""

    tenant_id = "tenant-a"
    repo_id = "repo-a"
    plan_store = JsonFilePlanStateStore(base_dir=tmp_path)
    plan = plan_store.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="observability",
        initial_steps=["one"],
    )

    outputs_root = tmp_path / "outputs"
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    bundle = OutputBundle(
        scope=scope,
        name="session",
        artifacts=(
            OutputArtifact.from_text(
                path=".akc/notes/x.txt",
                text="ok\n",
                media_type="text/plain; charset=utf-8",
                metadata={"plan_id": plan.id, "step_id": plan.steps[0].id},
            ),
        ),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=outputs_root)

    scope_dir = outputs_root / tenant_id / repo_id
    write_knowledge_mediation_report_artifact(
        scope_dir,
        tenant_id=tenant_id,
        repo_id=repo_id,
        mediation_report={
            "policy": "defer_to_intent",
            "events": [
                {
                    "kind": "ambiguous_conflict_resolution",
                    "conflict_group_id": "cg-int",
                    "mediation_resolved": False,
                    "defer_to_intent": True,
                }
            ],
        },
    )

    snap = load_viewer_snapshot(
        ViewerInputs(
            tenant_id=tenant_id,
            repo_id=repo_id,
            outputs_root=outputs_root,
            plan_base_dir=tmp_path,
        )
    )
    assert snap.knowledge_mediation_envelope is not None
    obs = build_knowledge_observation_payload(
        knowledge_envelope=snap.knowledge_envelope,
        conflict_reports=snap.conflict_reports,
        knowledge_mediation_envelope=snap.knowledge_mediation_envelope,
    )
    assert obs["unresolved_knowledge_conflicts_count"] == 1
    assert "cg-int" in obs["conflict_groups"]

    gov = summarize_knowledge_governance(scope_root=scope_dir)
    assert gov["unresolved_knowledge_conflicts_count"] == 1
    assert gov["knowledge_paths_present"]["mediation"] is True

    export_dir = tmp_path / "export"
    export_bundle(snapshot=snap, out_dir=export_dir, make_zip=False)
    kobs = json.loads((export_dir / "data" / "knowledge_obs.json").read_text(encoding="utf-8"))
    assert kobs["unresolved_knowledge_conflicts_count"] == 1
    assert (export_dir / "files" / ".akc" / "knowledge" / "mediation.json").is_file()


def test_knowledge_governance_counts_surface_in_snapshot_export_and_summarize(tmp_path: Path) -> None:
    """Phase 4: governance stats (conflicts, doc-derived split, evidence coverage) for CI/exports."""

    tenant_id = "tenant-b"
    repo_id = "repo-b"
    plan_store = JsonFilePlanStateStore(base_dir=tmp_path)
    plan = plan_store.create_plan(
        tenant_id=tenant_id,
        repo_id=repo_id,
        goal="gov",
        initial_steps=["one"],
    )

    outputs_root = tmp_path / "outputs"
    scope = TenantRepoScope(tenant_id=tenant_id, repo_id=repo_id)
    bundle = OutputBundle(
        scope=scope,
        name="session",
        artifacts=(
            OutputArtifact.from_text(
                path=".akc/notes/y.txt",
                text="ok\n",
                media_type="text/plain; charset=utf-8",
                metadata={"plan_id": plan.id, "step_id": plan.steps[0].id},
            ),
        ),
    )
    JsonManifestEmitter().emit(bundle=bundle, root=outputs_root)

    scope_dir = outputs_root / tenant_id / repo_id
    ks, intent_ids = _phase4_sample_snapshot()
    write_knowledge_snapshot_artifacts(
        scope_dir,
        tenant_id=tenant_id,
        repo_id=repo_id,
        snapshot=ks,
        run_id=plan.id,
        intent_assertion_ids=intent_ids,
    )
    write_knowledge_mediation_report_artifact(
        scope_dir,
        tenant_id=tenant_id,
        repo_id=repo_id,
        mediation_report={"policy": "defer_to_intent", "events": []},
    )

    expected = compute_knowledge_governance_counts(snapshot=ks, intent_assertion_ids=intent_ids)
    assert expected["canonical_assertions_total"] == 3
    assert expected["hard_assertions_count"] == 1
    assert expected["soft_assertions_count"] == 2
    assert expected["intent_backed_assertion_ids_count"] == 2
    assert expected["doc_derived_soft_assertions_count"] == 1
    assert expected["assertions_with_evidence_doc_ids_count"] == 3
    assert expected["evidence_doc_coverage_fraction"] == 1.0
    assert expected["distinct_evidence_doc_ids_count"] == 3

    gov = summarize_knowledge_governance(scope_root=scope_dir)
    assert gov["knowledge_paths_present"]["snapshot"] is True
    assert gov["unresolved_knowledge_conflicts_count"] == 0
    assert gov["knowledge_governance"]["doc_derived_soft_assertions_count"] == 1
    assert gov["knowledge_governance"]["evidence_doc_coverage_fraction"] == 1.0

    snap = load_viewer_snapshot(
        ViewerInputs(
            tenant_id=tenant_id,
            repo_id=repo_id,
            outputs_root=outputs_root,
            plan_base_dir=tmp_path,
        )
    )
    assert snap.knowledge_envelope is not None
    assert snap.knowledge_envelope.get("knowledge_governance") is not None

    obs = build_knowledge_observation_payload(
        knowledge_envelope=snap.knowledge_envelope,
        conflict_reports=snap.conflict_reports,
        knowledge_mediation_envelope=snap.knowledge_mediation_envelope,
    )
    assert obs["knowledge_governance"]["distinct_evidence_doc_ids_count"] == 3

    export_dir = tmp_path / "export2"
    export_bundle(snapshot=snap, out_dir=export_dir, make_zip=False)
    kobs = json.loads((export_dir / "data" / "knowledge_obs.json").read_text(encoding="utf-8"))
    assert kobs["knowledge_governance"]["canonical_assertions_total"] == 3
