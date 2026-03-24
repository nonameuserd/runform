from __future__ import annotations

import argparse
import asyncio
import json
import warnings
from pathlib import Path
from typing import Any

from akc.compile.knowledge_extractor import _select_evidence_doc_ids


def _load_json_obj(path: Path, *, what: str) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{what} must be a JSON object")
    return raw


async def _score_id_metrics(
    *,
    retrieved_ids: list[str],
    reference_ids: list[str],
) -> tuple[float, float]:
    """Run Ragas ID-based context precision/recall (offline, no LLM)."""
    # Ragas 0.4 deprecates some top-level metric imports; keep stderr clean in CI.
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from ragas import RunConfig
            from ragas.dataset_schema import SingleTurnSample
            from ragas.metrics import IDBasedContextPrecision, IDBasedContextRecall
    except ModuleNotFoundError:
        retrieved = {str(x).strip() for x in retrieved_ids if str(x).strip()}
        reference = {str(x).strip() for x in reference_ids if str(x).strip()}
        overlap = len(retrieved & reference)
        precision = float(overlap / len(retrieved)) if retrieved else 1.0
        recall = float(overlap / len(reference)) if reference else 1.0
        return precision, recall

    cfg = RunConfig()
    precision_m = IDBasedContextPrecision()
    recall_m = IDBasedContextRecall()
    precision_m.init(cfg)
    recall_m.init(cfg)

    sample = SingleTurnSample(
        retrieved_context_ids=list(retrieved_ids),
        reference_context_ids=list(reference_ids),
    )
    precision = await precision_m.single_turn_ascore(sample)
    recall = await recall_m.single_turn_ascore(sample)
    return float(precision), float(recall)


def check_ragas_retrieval_metrics(
    *,
    fixtures_path: Path,
    min_id_precision: float,
    min_id_recall: float,
) -> tuple[bool, dict[str, Any]]:
    """
    Evaluate the same deterministic retrieval fixtures as ``check_retrieval_proof_gate`` using
    Ragas **offline** ID-based metrics (no embeddings, no API calls).

    This complements the exact doc-id ordering gate by exercising the external Ragas metric stack
    on fixed corpora.
    """
    if min_id_precision < 0.0 or min_id_precision > 1.0:
        raise ValueError("min_id_precision must be in [0,1]")
    if min_id_recall < 0.0 or min_id_recall > 1.0:
        raise ValueError("min_id_recall must be in [0,1]")

    if not fixtures_path.is_file():
        return False, {"passed": False, "reason": f"retrieval proof fixture missing: {fixtures_path}"}

    root = _load_json_obj(fixtures_path, what="retrieval proof fixtures")
    cases_raw = root.get("cases")
    if not isinstance(cases_raw, list) or not cases_raw:
        return False, {"passed": False, "reason": "retrieval proof fixtures must include non-empty list: cases"}

    case_reports: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []

    async def _run_all() -> None:
        for row in cases_raw:
            if not isinstance(row, dict):
                raise ValueError("each retrieval proof case must be a JSON object")
            case_id = str(row.get("case_id") or "").strip()
            if not case_id:
                raise ValueError("retrieval proof case missing non-empty case_id")
            constraint_text = str(row.get("constraint_text") or "")
            top_k_raw = row.get("top_k", 3)
            top_k = int(top_k_raw)
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
            expected = [str(x).strip() for x in expected_raw if str(x).strip()]
            actual = _select_evidence_doc_ids(
                constraint_text=constraint_text,
                documents=documents,
                top_k=top_k,
            )
            precision, recall = await _score_id_metrics(
                retrieved_ids=list(actual),
                reference_ids=expected,
            )
            deterministic_match = list(actual) == expected
            ok_case = deterministic_match and precision >= min_id_precision - 1e-9 and recall >= min_id_recall - 1e-9
            entry = {
                "case_id": case_id,
                "deterministic_doc_id_match": deterministic_match,
                "expected_doc_ids": expected,
                "actual_doc_ids": list(actual),
                "id_based_context_precision": precision,
                "id_based_context_recall": recall,
                "passed": ok_case,
            }
            case_reports.append(entry)
            if not ok_case:
                failures.append(
                    {
                        "case_id": case_id,
                        "reason": "deterministic mismatch or Ragas ID metric below threshold",
                        **{k: v for k, v in entry.items() if k not in {"case_id"}},
                    }
                )

    asyncio.run(_run_all())

    passed = len(failures) == 0
    return passed, {
        "passed": passed,
        "fixtures_path": str(fixtures_path),
        "min_id_precision": min_id_precision,
        "min_id_recall": min_id_recall,
        "scoring_backend": _ragas_backend(),
        "ragas_version": _ragas_version(),
        "cases": case_reports,
        "failures": failures,
    }


def _ragas_version() -> str:
    try:
        import ragas

        return str(getattr(ragas, "__version__", "unknown"))
    except Exception:  # pragma: no cover - defensive
        return "unavailable"


def _ragas_backend() -> str:
    return "ragas" if _ragas_version() != "unavailable" else "fallback"


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Offline Ragas ID-based retrieval metrics on deterministic retrieval-proof fixtures"
    )
    ap.add_argument(
        "--fixtures-path",
        default="configs/evals/fixtures/retrieval_proof_cases.json",
        help="Path to retrieval proof fixture JSON (same as check_retrieval_proof_gate)",
    )
    ap.add_argument(
        "--min-id-precision",
        type=float,
        default=1.0,
        help="Minimum Ragas id_based_context_precision per case (0..1)",
    )
    ap.add_argument(
        "--min-id-recall",
        type=float,
        default=1.0,
        help="Minimum Ragas id_based_context_recall per case (0..1)",
    )
    ap.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    args = ap.parse_args()

    ok, report = check_ragas_retrieval_metrics(
        fixtures_path=Path(str(args.fixtures_path)).expanduser().resolve(),
        min_id_precision=float(args.min_id_precision),
        min_id_recall=float(args.min_id_recall),
    )
    if str(args.format) == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"passed: {bool(report.get('passed', False))}")
        print(f"backend: {report.get('scoring_backend')}")
        print(f"ragas: {report.get('ragas_version')}")
        for row in report.get("cases", []):
            cid = row.get("case_id")
            print(
                f"- {cid}: precision={row.get('id_based_context_precision')} "
                f"recall={row.get('id_based_context_recall')} match={row.get('deterministic_doc_id_match')}"
            )
        if report.get("failures"):
            print("failures:")
            for item in report["failures"]:
                print(f"- {item}")
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
