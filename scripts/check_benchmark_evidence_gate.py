from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def _load_json_obj(path: Path, *, what: str) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{what} must be a JSON object")
    return raw


def check_benchmark_evidence_gate(
    *,
    report_path: Path,
    min_sample_count: int,
    min_compression_factor: float,
    min_pass_rate: float,
    benchmark_group: str | None = None,
) -> tuple[bool, dict[str, Any]]:
    if min_sample_count <= 0:
        raise ValueError("min_sample_count must be > 0")
    if min_compression_factor <= 0:
        raise ValueError("min_compression_factor must be > 0")
    if min_pass_rate < 0.0 or min_pass_rate > 1.0:
        raise ValueError("min_pass_rate must be in [0,1]")
    if not report_path.is_file():
        return False, {"passed": False, "reason": f"benchmark report missing: {report_path}"}
    report = _load_json_obj(report_path, what="benchmark report")
    bench = report.get("benchmark_summary")
    if not isinstance(bench, dict):
        return False, {"passed": False, "reason": "benchmark_summary missing from eval report"}
    groups = bench.get("groups")
    if not isinstance(groups, dict) or not groups:
        return False, {"passed": False, "reason": "benchmark_summary.groups is empty"}
    selected = [benchmark_group] if benchmark_group is not None else sorted(str(k) for k in groups)
    failures: list[dict[str, Any]] = []
    for group in selected:
        row = groups.get(group)
        if not isinstance(row, dict):
            failures.append({"group": group, "reason": "group missing"})
            continue
        sample_count = int(row.get("sample_count", 0) or 0)
        compression = float(row.get("compression_factor_vs_baseline_avg", 0.0) or 0.0)
        pass_rate = float(row.get("pass_rate", 0.0) or 0.0)
        if sample_count < min_sample_count:
            failures.append(
                {
                    "group": group,
                    "metric": "sample_count",
                    "actual": sample_count,
                    "expected_gte": min_sample_count,
                }
            )
        if compression < min_compression_factor:
            failures.append(
                {
                    "group": group,
                    "metric": "compression_factor_vs_baseline_avg",
                    "actual": compression,
                    "expected_gte": min_compression_factor,
                }
            )
        if pass_rate < min_pass_rate:
            failures.append(
                {
                    "group": group,
                    "metric": "pass_rate",
                    "actual": pass_rate,
                    "expected_gte": min_pass_rate,
                }
            )
    passed = len(failures) == 0
    return passed, {
        "passed": passed,
        "report_path": str(report_path),
        "groups_checked": selected,
        "min_sample_count": min_sample_count,
        "min_compression_factor": min_compression_factor,
        "min_pass_rate": min_pass_rate,
        "failures": failures,
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Check eval benchmark evidence gate")
    ap.add_argument("--report-path", required=True, help="Path to eval report JSON")
    ap.add_argument("--min-sample-count", type=int, default=1, help="Minimum sample_count per benchmark group")
    ap.add_argument(
        "--min-compression-factor",
        type=float,
        default=1.0,
        help="Minimum compression_factor_vs_baseline_avg per benchmark group",
    )
    ap.add_argument(
        "--min-pass-rate",
        type=float,
        default=1.0,
        help="Minimum deterministic pass rate per benchmark group",
    )
    ap.add_argument("--benchmark-group", help="Optional group name to check; defaults to all groups")
    ap.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    args = ap.parse_args()
    ok, report = check_benchmark_evidence_gate(
        report_path=Path(str(args.report_path)).expanduser().resolve(),
        min_sample_count=int(args.min_sample_count),
        min_compression_factor=float(args.min_compression_factor),
        min_pass_rate=float(args.min_pass_rate),
        benchmark_group=(str(args.benchmark_group).strip() if args.benchmark_group else None),
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
