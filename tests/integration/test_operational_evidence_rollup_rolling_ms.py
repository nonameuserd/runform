"""Cross-run operational evidence rollup (``operational_evidence_window.v1``) + ``rolling_ms`` semantics."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from akc.compile.controller_config import Budget
from akc.intent.models import OperationalValidityParams
from akc.intent.operational_eval import (
    evaluate_operational_spec,
    load_merged_runtime_evidence_from_rollup_path,
    operational_eval_inputs_fingerprint,
)

# Import graph: touch compile config before ``akc.intent`` (avoids circular import on collection).
_: type[Budget] = Budget


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest().lower()


def _terminal_row(*, ts: int, run_id: str) -> dict[str, object]:
    return {
        "evidence_type": "terminal_health",
        "timestamp": ts,
        "runtime_run_id": run_id,
        "payload": {
            "resource_id": "__runtime_aggregate__",
            "health_status": "healthy",
            "aggregate": True,
        },
    }


def _write_evidence_file(path: Path, rows: list[dict[str, object]]) -> str:
    text = json.dumps(rows, sort_keys=True, separators=(",", ":"))
    path.write_text(text + "\n", encoding="utf-8")
    return _sha256_text(text + "\n")


def _rollup_obj(
    *,
    start: int,
    end: int,
    exports: list[tuple[str, str]],
) -> dict[str, object]:
    return {
        "schema_version": 1,
        "schema_id": "akc:operational_evidence_window:v1",
        "window_start_ms": start,
        "window_end_ms": end,
        "runtime_evidence_exports": [{"path": p, "sha256": h} for p, h in exports],
    }


def _rolling_params(*, rollup_rel: str, rolling_ms: int) -> OperationalValidityParams:
    return OperationalValidityParams.from_mapping(
        {
            "spec_version": 1,
            "window": "rolling_ms",
            "rolling_window_ms": rolling_ms,
            "predicate_kind": "presence",
            "signals": [{"evidence_type": "terminal_health", "payload_path": "health_status"}],
            "expected_evidence_types": ["terminal_health"],
            "evidence_rollup_rel_path": rollup_rel,
        }
    )


def test_rolling_ms_passes_when_records_inside_window_two_exports(tmp_path: Path) -> None:
    repo = tmp_path / "tenant-x" / "repo-y"
    ver = repo / ".akc" / "verification"
    ver.mkdir(parents=True, exist_ok=True)
    ev_a = repo / ".akc" / "ev" / "a.json"
    ev_b = repo / ".akc" / "ev" / "b.json"
    ev_a.parent.mkdir(parents=True, exist_ok=True)

    h_a = _write_evidence_file(ev_a, [_terminal_row(ts=2000, run_id="r1")])
    h_b = _write_evidence_file(ev_b, [_terminal_row(ts=7000, run_id="r2")])

    rollup_path = ver / "window.json"
    exports = [
        (str(ev_a.relative_to(repo)), h_a),
        (str(ev_b.relative_to(repo)), h_b),
    ]
    rollup_path.write_text(
        json.dumps(_rollup_obj(start=1000, end=9000, exports=exports), indent=2),
        encoding="utf-8",
    )

    merged, meta = load_merged_runtime_evidence_from_rollup_path(rollup_path=rollup_path, repo_outputs_root=repo)
    params = _rolling_params(rollup_rel=".akc/verification/window.json", rolling_ms=20_000)
    verdict = evaluate_operational_spec(
        params=params,
        evidence=merged,
        rolling_rollup_meta=meta,
        success_criterion_id="sc-roll",
    )
    assert verdict.passed
    names = {str(r.get("check_name", "")) for r in verdict.per_criterion}
    assert "rolling_window_span" in names
    assert "rolling_evidence_timestamps" in names


def test_rolling_ms_fails_when_timestamp_outside_declared_window(tmp_path: Path) -> None:
    repo = tmp_path / "tenant-x" / "repo-y"
    ver = repo / ".akc" / "verification"
    ver.mkdir(parents=True, exist_ok=True)
    ev_a = repo / "bad.json"
    h_a = _write_evidence_file(ev_a, [_terminal_row(ts=999_999, run_id="r1")])

    rollup_path = ver / "window.json"
    rollup_path.write_text(
        json.dumps(
            _rollup_obj(
                start=1000,
                end=9000,
                exports=[(str(ev_a.relative_to(repo)), h_a)],
            ),
            indent=2,
        ),
        encoding="utf-8",
    )

    merged, meta = load_merged_runtime_evidence_from_rollup_path(rollup_path=rollup_path, repo_outputs_root=repo)
    params = _rolling_params(rollup_rel=".akc/verification/window.json", rolling_ms=50_000)
    verdict = evaluate_operational_spec(
        params=params,
        evidence=merged,
        rolling_rollup_meta=meta,
        success_criterion_id="sc-roll",
    )
    assert not verdict.passed
    assert any("rolling_evidence_timestamps" in str(x.get("check_name", "")) for x in verdict.per_criterion)


def test_rolling_ms_fails_when_window_span_exceeds_intent_rolling_window_ms(tmp_path: Path) -> None:
    repo = tmp_path / "tenant-x" / "repo-y"
    ver = repo / ".akc" / "verification"
    ver.mkdir(parents=True, exist_ok=True)
    ev_a = repo / "one.json"
    h_a = _write_evidence_file(ev_a, [_terminal_row(ts=5000, run_id="r1")])

    rollup_path = ver / "window.json"
    rollup_path.write_text(
        json.dumps(
            _rollup_obj(start=1000, end=9000, exports=[(str(ev_a.relative_to(repo)), h_a)]),
            indent=2,
        ),
        encoding="utf-8",
    )

    merged, meta = load_merged_runtime_evidence_from_rollup_path(rollup_path=rollup_path, repo_outputs_root=repo)
    params = _rolling_params(rollup_rel=".akc/verification/window.json", rolling_ms=1000)
    verdict = evaluate_operational_spec(
        params=params,
        evidence=merged,
        rolling_rollup_meta=meta,
        success_criterion_id="sc-roll",
    )
    assert not verdict.passed
    assert any("rolling_window_span" in str(x.get("check_name", "")) for x in verdict.per_criterion)


def test_fingerprint_changes_when_rollup_export_membership_changes(tmp_path: Path) -> None:
    repo = tmp_path / "tenant-x" / "repo-y"
    ver = repo / ".akc" / "verification"
    ver.mkdir(parents=True, exist_ok=True)
    ev_a = repo / "a.json"
    ev_b = repo / "b.json"
    h_a = _write_evidence_file(ev_a, [_terminal_row(ts=3000, run_id="r1")])
    h_b = _write_evidence_file(ev_b, [_terminal_row(ts=4000, run_id="r2")])

    rollup_one = ver / "one.json"
    rollup_two = ver / "two.json"
    rollup_one.write_text(
        json.dumps(_rollup_obj(start=1000, end=9000, exports=[(str(ev_a.relative_to(repo)), h_a)]), indent=2),
        encoding="utf-8",
    )
    rollup_two.write_text(
        json.dumps(
            _rollup_obj(
                start=1000,
                end=9000,
                exports=[(str(ev_a.relative_to(repo)), h_a), (str(ev_b.relative_to(repo)), h_b)],
            ),
            indent=2,
        ),
        encoding="utf-8",
    )

    m1, meta1 = load_merged_runtime_evidence_from_rollup_path(rollup_path=rollup_one, repo_outputs_root=repo)
    m2, meta2 = load_merged_runtime_evidence_from_rollup_path(rollup_path=rollup_two, repo_outputs_root=repo)
    params = _rolling_params(rollup_rel=".akc/verification/placeholder.json", rolling_ms=50_000)

    fp1 = operational_eval_inputs_fingerprint(
        params=params,
        evidence=m1,
        rolling_rollup_meta=meta1,
    )
    fp2 = operational_eval_inputs_fingerprint(
        params=params,
        evidence=m2,
        rolling_rollup_meta=meta2,
    )
    assert fp1 != fp2
