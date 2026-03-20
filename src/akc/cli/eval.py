from __future__ import annotations

import argparse
import json
from pathlib import Path

from akc.evals import run_eval_suite


def cmd_eval(args: argparse.Namespace) -> int:
    """Run eval suite with deterministic checks and regression gates."""

    out_format = str(getattr(args, "format", "text"))

    suite_path = Path(str(args.suite_path)).expanduser()
    outputs_root = Path(str(args.outputs_root)).expanduser()
    baseline_report_path = (
        Path(str(args.baseline_report_path)).expanduser()
        if getattr(args, "baseline_report_path", None)
        else None
    )

    try:
        report = run_eval_suite(
            suite_path=suite_path,
            outputs_root=outputs_root,
            baseline_report_path=baseline_report_path,
        )
        report_json = report.to_json_obj()
        if getattr(args, "report_out", None):
            out_fp = Path(str(args.report_out)).expanduser()
            out_fp.parent.mkdir(parents=True, exist_ok=True)
            out_fp.write_text(json.dumps(report_json, indent=2, sort_keys=True), encoding="utf-8")

        if out_format == "json":
            print(json.dumps(report_json, indent=2, sort_keys=True))
        else:
            print(f"Eval suite: {report.suite_version}")
            print(
                "Summary: "
                f"total={int(report.summary.get('tasks_total', 0.0))} "
                f"passed={int(report.summary.get('tasks_passed', 0.0))} "
                f"failed={int(report.summary.get('tasks_failed', 0.0))} "
                f"success_rate={report.summary.get('success_rate', 0.0):.3f} "
                f"avg_repairs={report.summary.get('avg_repair_iterations', 0.0):.3f}"
            )
            if report.gate_violations:
                print("Regression gate violations:")
                for v in report.gate_violations:
                    print(
                        f"- {v.gate}: {v.message} "
                        f"(actual={v.actual:.4f}, expected={v.expected:.4f})"
                    )
            else:
                print("Regression gate violations: none")
            print(f"Result: {'PASS' if report.passed else 'FAIL'}")
        return 0 if report.passed else 2
    except Exception as e:
        # Phase D hardening: fail-fast with a structured JSON error report
        # (no stack trace output) so CI can reliably parse failures.
        err_obj: dict[str, object] = {
            "passed": False,
            "suite_path": str(suite_path),
            "outputs_root": str(outputs_root),
            "baseline_report_path": (
                str(baseline_report_path) if baseline_report_path is not None else None
            ),
            "error": {
                "type": e.__class__.__name__,
                "message": str(e),
            },
            "gate_violations": [],
            "summary": {},
        }

        if getattr(args, "report_out", None):
            out_fp = Path(str(args.report_out)).expanduser()
            out_fp.parent.mkdir(parents=True, exist_ok=True)
            out_fp.write_text(json.dumps(err_obj, indent=2, sort_keys=True), encoding="utf-8")

        if out_format == "json":
            print(json.dumps(err_obj, indent=2, sort_keys=True))
        else:
            print(f"Eval suite failed: {e.__class__.__name__}: {e}")

        return 2
