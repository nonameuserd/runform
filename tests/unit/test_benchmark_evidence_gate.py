from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path


def _load_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "check_benchmark_evidence_gate.py"
    spec = importlib.util.spec_from_file_location("check_benchmark_evidence_gate", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module


def test_benchmark_evidence_gate_passes(tmp_path: Path) -> None:
    mod = _load_module()
    report_path = tmp_path / "latest-report.json"
    report_path.write_text(
        json.dumps(
            {
                "benchmark_summary": {
                    "groups": {
                        "core": {
                            "sample_count": 2,
                            "compression_factor_vs_baseline_avg": 1.5,
                            "pass_rate": 1.0,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    ok, report = mod.check_benchmark_evidence_gate(
        report_path=report_path,
        min_sample_count=1,
        min_compression_factor=1.0,
        min_pass_rate=1.0,
        benchmark_group="core",
    )
    assert ok is True
    assert report["passed"] is True


def test_benchmark_evidence_gate_fails_for_low_compression(tmp_path: Path) -> None:
    mod = _load_module()
    report_path = tmp_path / "latest-report.json"
    report_path.write_text(
        json.dumps(
            {
                "benchmark_summary": {
                    "groups": {
                        "core": {
                            "sample_count": 2,
                            "compression_factor_vs_baseline_avg": 0.8,
                            "pass_rate": 1.0,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    ok, report = mod.check_benchmark_evidence_gate(
        report_path=report_path,
        min_sample_count=1,
        min_compression_factor=1.0,
        min_pass_rate=1.0,
        benchmark_group="core",
    )
    assert ok is False
    assert report["passed"] is False
    assert len(report["failures"]) >= 1


def test_benchmark_evidence_gate_fails_for_low_pass_rate(tmp_path: Path) -> None:
    mod = _load_module()
    report_path = tmp_path / "latest-report.json"
    report_path.write_text(
        json.dumps(
            {
                "benchmark_summary": {
                    "groups": {
                        "core": {
                            "sample_count": 2,
                            "compression_factor_vs_baseline_avg": 2.0,
                            "pass_rate": 0.5,
                        }
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    ok, report = mod.check_benchmark_evidence_gate(
        report_path=report_path,
        min_sample_count=1,
        min_compression_factor=1.0,
        min_pass_rate=1.0,
        benchmark_group="core",
    )
    assert ok is False
    assert report["passed"] is False
    assert any(item.get("metric") == "pass_rate" for item in report["failures"])
