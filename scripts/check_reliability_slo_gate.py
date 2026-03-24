from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class ScoreboardGateFailure:
    scope: str
    window_start_ms: int
    window_end_ms: int
    metric: str
    comparator: str
    actual: float
    expected: float

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "scope": self.scope,
            "window_start_ms": int(self.window_start_ms),
            "window_end_ms": int(self.window_end_ms),
            "metric": self.metric,
            "comparator": self.comparator,
            "actual": float(self.actual),
            "expected": float(self.expected),
        }


def _load_json_obj(path: Path, *, what: str) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{what} must be a JSON object")
    return raw


def _scope_key(obj: dict[str, Any]) -> str:
    tenant_id = str(obj.get("tenant_id", "")).strip()
    repo_id = str(obj.get("repo_id", "")).strip()
    if not tenant_id or not repo_id:
        raise ValueError("scoreboard must include non-empty tenant_id and repo_id")
    return f"{tenant_id}/{repo_id}"


def _matches_scope(*, obj: dict[str, Any], tenant_id: str | None, repo_id: str | None) -> bool:
    t = str(obj.get("tenant_id", "")).strip()
    r = str(obj.get("repo_id", "")).strip()
    if tenant_id is not None and t != tenant_id:
        return False
    return not (repo_id is not None and r != repo_id)


def _scoreboard_kpi(obj: dict[str, Any]) -> dict[str, Any]:
    kpi = obj.get("kpi")
    if not isinstance(kpi, dict):
        raise ValueError("scoreboard.kpi must be an object")
    return kpi


def _as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _evaluate_scoreboard(
    *,
    scope: str,
    scoreboard: dict[str, Any],
    targets: dict[str, Any],
) -> list[ScoreboardGateFailure]:
    failures: list[ScoreboardGateFailure] = []
    kpi = _scoreboard_kpi(scoreboard)
    ws = int(scoreboard.get("window_start_ms", 0) or 0)
    we = int(scoreboard.get("window_end_ms", 0) or 0)

    for metric, rule_raw in targets.items():
        if not isinstance(rule_raw, dict):
            raise ValueError(f"kpi target rule for {metric!r} must be an object")
        actual = _as_float(kpi.get(metric))
        if actual is None:
            failures.append(
                ScoreboardGateFailure(
                    scope=scope,
                    window_start_ms=ws,
                    window_end_ms=we,
                    metric=metric,
                    comparator="present",
                    actual=float("nan"),
                    expected=0.0,
                )
            )
            continue
        if "gte" in rule_raw:
            expected = _as_float(rule_raw.get("gte"))
            if expected is None:
                raise ValueError(f"kpi target {metric}.gte must be numeric")
            if actual < expected:
                failures.append(
                    ScoreboardGateFailure(
                        scope=scope,
                        window_start_ms=ws,
                        window_end_ms=we,
                        metric=metric,
                        comparator="gte",
                        actual=actual,
                        expected=expected,
                    )
                )
        if "lte" in rule_raw:
            expected = _as_float(rule_raw.get("lte"))
            if expected is None:
                raise ValueError(f"kpi target {metric}.lte must be numeric")
            if actual > expected:
                failures.append(
                    ScoreboardGateFailure(
                        scope=scope,
                        window_start_ms=ws,
                        window_end_ms=we,
                        metric=metric,
                        comparator="lte",
                        actual=actual,
                        expected=expected,
                    )
                )
    return failures


def check_reliability_slo_gate(
    *,
    outputs_root: Path,
    targets_path: Path,
    tenant_id: str | None = None,
    repo_id: str | None = None,
) -> tuple[bool, dict[str, Any]]:
    targets_obj = _load_json_obj(targets_path, what="reliability target config")
    window_obj = targets_obj.get("window")
    if not isinstance(window_obj, dict):
        raise ValueError("target config must include object field: window")
    required_windows = int(window_obj.get("required_consecutive_windows", 2) or 2)
    if required_windows <= 0:
        raise ValueError("required_consecutive_windows must be > 0")
    targets = targets_obj.get("kpi_targets")
    if not isinstance(targets, dict) or not targets:
        raise ValueError("target config must include non-empty object field: kpi_targets")

    scoreboards: dict[str, list[dict[str, Any]]] = {}
    for fp in outputs_root.rglob("*.reliability_scoreboard.v1.json"):
        obj = _load_json_obj(fp, what=f"scoreboard at {fp}")
        if not _matches_scope(obj=obj, tenant_id=tenant_id, repo_id=repo_id):
            continue
        scope = _scope_key(obj)
        scoreboards.setdefault(scope, []).append(obj)

    if not scoreboards:
        report = {
            "passed": False,
            "reason": "no reliability scoreboards found for requested scope",
            "required_consecutive_windows": required_windows,
            "scopes_evaluated": 0,
            "failures": [],
        }
        return False, report

    all_failures: list[ScoreboardGateFailure] = []
    per_scope_windows: dict[str, list[dict[str, Any]]] = {}
    for scope, rows in scoreboards.items():
        ordered = sorted(rows, key=lambda row: int(row.get("window_end_ms", 0) or 0))
        per_scope_windows[scope] = ordered[-required_windows:]

    for scope, rows in per_scope_windows.items():
        if len(rows) < required_windows:
            all_failures.append(
                ScoreboardGateFailure(
                    scope=scope,
                    window_start_ms=0,
                    window_end_ms=0,
                    metric="required_consecutive_windows",
                    comparator="gte",
                    actual=float(len(rows)),
                    expected=float(required_windows),
                )
            )
            continue
        for row in rows:
            all_failures.extend(_evaluate_scoreboard(scope=scope, scoreboard=row, targets=targets))

    passed = len(all_failures) == 0
    report = {
        "passed": passed,
        "required_consecutive_windows": required_windows,
        "scopes_evaluated": len(per_scope_windows),
        "window_counts": {scope: len(rows) for scope, rows in per_scope_windows.items()},
        "failures": [f.to_json_obj() for f in all_failures],
    }
    return passed, report


def main() -> int:
    ap = argparse.ArgumentParser(description="Check reliability SLO gate over autopilot scoreboards")
    ap.add_argument("--outputs-root", required=True, help="Outputs root containing tenant/repo autopilot scoreboards")
    ap.add_argument(
        "--targets-path",
        default="configs/slo/reliability_scoreboard_targets.json",
        help="Path to reliability SLO targets JSON",
    )
    ap.add_argument("--tenant-id", help="Optional tenant scope filter")
    ap.add_argument("--repo-id", help="Optional repo scope filter")
    ap.add_argument("--format", choices=["text", "json"], default="text", help="Output format")
    args = ap.parse_args()

    ok, report = check_reliability_slo_gate(
        outputs_root=Path(str(args.outputs_root)).expanduser().resolve(),
        targets_path=Path(str(args.targets_path)).expanduser().resolve(),
        tenant_id=(str(args.tenant_id).strip() if args.tenant_id else None),
        repo_id=(str(args.repo_id).strip() if args.repo_id else None),
    )
    if str(args.format) == "json":
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(f"passed: {bool(report.get('passed', False))}")
        print(f"scopes_evaluated: {int(report.get('scopes_evaluated', 0))}")
        print(f"required_consecutive_windows: {int(report.get('required_consecutive_windows', 0))}")
        failures = report.get("failures") or []
        if failures:
            print("failures:")
            for item in failures:
                if not isinstance(item, dict):
                    continue
                print(
                    "- "
                    f"{item.get('scope')} metric={item.get('metric')} comparator={item.get('comparator')} "
                    f"actual={item.get('actual')} expected={item.get('expected')} "
                    f"window=[{item.get('window_start_ms')},{item.get('window_end_ms')}]"
                )

    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
