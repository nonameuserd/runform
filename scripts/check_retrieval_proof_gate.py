from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.compile.knowledge_extractor import _select_evidence_doc_ids


@dataclass(frozen=True, slots=True)
class RetrievalProofGateFailure:
    case_id: str
    expected_doc_ids: tuple[str, ...]
    actual_doc_ids: tuple[str, ...]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "expected_doc_ids": list(self.expected_doc_ids),
            "actual_doc_ids": list(self.actual_doc_ids),
        }


def _load_json_obj(path: Path, *, what: str) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{what} must be a JSON object")
    return raw


def check_retrieval_proof_gate(
    *,
    fixtures_path: Path,
) -> tuple[bool, dict[str, Any]]:
    if not fixtures_path.is_file():
        return False, {"passed": False, "reason": f"retrieval proof fixture missing: {fixtures_path}"}

    root = _load_json_obj(fixtures_path, what="retrieval proof fixtures")
    cases_raw = root.get("cases")
    if not isinstance(cases_raw, list) or not cases_raw:
        return False, {"passed": False, "reason": "retrieval proof fixtures must include non-empty list: cases"}

    failures: list[RetrievalProofGateFailure] = []
    checked_case_ids: list[str] = []
    for row in cases_raw:
        if not isinstance(row, dict):
            raise ValueError("each retrieval proof case must be a JSON object")
        case_id = str(row.get("case_id") or "").strip()
        if not case_id:
            raise ValueError("retrieval proof case missing non-empty case_id")
        checked_case_ids.append(case_id)
        constraint_text = str(row.get("constraint_text") or "")
        top_k_raw = row.get("top_k", 3)
        try:
            top_k = int(top_k_raw)
        except Exception as exc:  # pragma: no cover - defensive
            raise ValueError(f"case {case_id}: top_k must be an integer") from exc
        documents_raw = row.get("documents")
        if not isinstance(documents_raw, list):
            raise ValueError(f"case {case_id}: documents must be a list")
        documents: list[dict[str, Any]] = []
        for item in documents_raw:
            if not isinstance(item, dict):
                raise ValueError(f"case {case_id}: each documents[] row must be an object")
            documents.append(item)
        expected_raw = row.get("expected_doc_ids")
        if not isinstance(expected_raw, list):
            raise ValueError(f"case {case_id}: expected_doc_ids must be a list")
        expected = tuple(str(x).strip() for x in expected_raw if str(x).strip())
        actual = _select_evidence_doc_ids(
            constraint_text=constraint_text,
            documents=documents,
            top_k=top_k,
        )
        if actual != expected:
            failures.append(
                RetrievalProofGateFailure(
                    case_id=case_id,
                    expected_doc_ids=expected,
                    actual_doc_ids=actual,
                )
            )

    passed = len(failures) == 0
    return passed, {
        "passed": passed,
        "fixtures_path": str(fixtures_path),
        "cases_checked": checked_case_ids,
        "failures": [f.to_json_obj() for f in failures],
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Check retrieval-proof fixture expectations for evidence selection")
    ap.add_argument(
        "--fixtures-path",
        default="configs/evals/fixtures/retrieval_proof_cases.json",
        help="Path to retrieval proof fixture JSON",
    )
    ap.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    args = ap.parse_args()

    ok, report = check_retrieval_proof_gate(
        fixtures_path=Path(str(args.fixtures_path)).expanduser().resolve(),
    )
    if str(args.format) == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"passed: {bool(report.get('passed', False))}")
        if report.get("failures"):
            print("failures:")
            for item in report["failures"]:
                print(f"- {item}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
