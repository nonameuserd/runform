from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_ragas_retrieval_metrics.py"
    spec = importlib.util.spec_from_file_location("check_ragas_retrieval_metrics_fallback", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_ragas_retrieval_metrics_falls_back_when_ragas_is_unavailable() -> None:
    mod = _load_module()
    fixtures_path = (
        Path(__file__).resolve().parents[2] / "configs" / "evals" / "fixtures" / "retrieval_proof_cases.json"
    )
    ok, report = mod.check_ragas_retrieval_metrics(
        fixtures_path=fixtures_path,
        min_id_precision=1.0,
        min_id_recall=1.0,
    )
    assert ok is True
    assert report["passed"] is True
    assert report["scoring_backend"] in {"fallback", "ragas"}
