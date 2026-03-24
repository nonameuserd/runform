from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_retrieval_proof_gate.py"
    spec = importlib.util.spec_from_file_location("check_retrieval_proof_gate", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_retrieval_proof_gate_passes_with_default_fixtures() -> None:
    mod = _load_module()
    fixtures_path = (
        Path(__file__).resolve().parents[2] / "configs" / "evals" / "fixtures" / "retrieval_proof_cases.json"
    )
    ok, report = mod.check_retrieval_proof_gate(fixtures_path=fixtures_path)
    assert ok is True
    assert report["passed"] is True
    assert report["failures"] == []


def test_retrieval_proof_gate_fails_when_expectations_drift(tmp_path: Path) -> None:
    mod = _load_module()
    fixtures_path = tmp_path / "retrieval-proof.json"
    fixtures_path.write_text(
        json.dumps(
            {
                "cases": [
                    {
                        "case_id": "drifted",
                        "constraint_text": "service must authenticate requests with token verification",
                        "top_k": 2,
                        "documents": [
                            {
                                "doc_id": "doc-b",
                                "title": "Auth",
                                "content": "Service authenticate requests token verification.",
                            },
                            {
                                "doc_id": "doc-a",
                                "title": "Auth duplicate",
                                "content": "Service authenticate requests token verification.",
                            },
                        ],
                        "expected_doc_ids": ["doc-b", "doc-a"],
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    ok, report = mod.check_retrieval_proof_gate(fixtures_path=fixtures_path)
    assert ok is False
    assert report["passed"] is False
    assert len(report["failures"]) == 1
    assert report["failures"][0]["case_id"] == "drifted"


def test_retrieval_proof_gate_fails_when_fixture_missing(tmp_path: Path) -> None:
    mod = _load_module()
    missing = tmp_path / "missing.json"
    ok, report = mod.check_retrieval_proof_gate(fixtures_path=missing)
    assert ok is False
    assert report["passed"] is False
    assert "missing" in str(report.get("reason", ""))
