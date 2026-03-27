"""Guardrails for tests/fixtures/knowledge_domains corpus + coverage matrix."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.integration.knowledge_domain_coverage_registry import KD_DOC_IDS_ALLOWLIST

_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "knowledge_domains"
_QUALITY_DIMS = {
    "taste",
    "domain_knowledge",
    "judgment",
    "instincts",
    "user_empathy",
    "engineering_discipline",
}
_HARD_GATE_DIMS = {
    "domain_knowledge",
    "judgment",
    "engineering_discipline",
}


def _load_json(name: str) -> dict:
    path = _FIXTURES / name
    assert path.is_file(), f"missing {path}"
    return json.loads(path.read_text(encoding="utf-8"))


def test_corpus_manifest_paths_resolve_and_non_empty() -> None:
    manifest = _load_json("corpus_manifest.json")
    chunks = manifest.get("chunks") or []
    assert isinstance(chunks, list) and chunks
    for ch in chunks:
        rel = ch.get("path")
        assert isinstance(rel, str) and rel
        p = (_FIXTURES / rel).resolve()
        assert p.is_file(), f"missing chunk file {p}"
        text = p.read_text(encoding="utf-8")
        assert text.strip(), f"empty fixture {p}"
        assert ch.get("doc_id")
        meta = ch.get("metadata") or {}
        kind = meta.get("ingest_source_kind")
        assert kind in {"docs", "openapi", "messaging"}


def test_domain_coverage_matrix_domains_reference_fixture_prefixes() -> None:
    matrix = _load_json("domain_coverage_matrix.json")
    domains = matrix.get("domains") or []
    assert len(domains) >= 3
    prefixes = {d.get("fixture_prefix") for d in domains if isinstance(d, dict)}
    for prefix in prefixes:
        assert isinstance(prefix, str) and prefix.endswith("/")
        assert (_FIXTURES / prefix).is_dir(), f"missing domain dir {_FIXTURES / prefix}"


def test_domain_coverage_matrix_quality_expectations_cover_all_dimensions() -> None:
    matrix = _load_json("domain_coverage_matrix.json")
    defaults = matrix.get("quality_contract_defaults") or {}
    required = set(defaults.get("required_dimensions") or [])
    assert required == _QUALITY_DIMS
    hard_gate = set(defaults.get("hard_gate_dimensions") or [])
    assert hard_gate == _HARD_GATE_DIMS
    for domain in matrix.get("domains") or []:
        assert isinstance(domain, dict)
        expectations = domain.get("quality_evidence_expectations")
        assert isinstance(expectations, dict)
        assert set(expectations.keys()) == _QUALITY_DIMS
        for values in expectations.values():
            assert isinstance(values, list)


@pytest.mark.parametrize(
    "doc_id",
    [
        "kd-sec-net-docs-firewall",
        "kd-conflict-retention-v1",
        "kd-doc-derived-controls",
    ],
)
def test_manifest_contains_anchor_doc_ids(doc_id: str) -> None:
    manifest = _load_json("corpus_manifest.json")
    ids = {c.get("doc_id") for c in manifest["chunks"]}
    assert doc_id in ids


def test_manifest_doc_ids_match_integration_allowlist_contract() -> None:
    manifest = _load_json("corpus_manifest.json")
    manifest_ids = {
        str(c.get("doc_id") or "").strip() for c in manifest.get("chunks", []) if str(c.get("doc_id") or "").strip()
    }
    assert manifest_ids, "expected non-empty corpus manifest doc_id set"
    assert manifest_ids == KD_DOC_IDS_ALLOWLIST
