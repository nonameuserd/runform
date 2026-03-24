"""SQLite-backed operations index: cross-run catalog (identity, health, sidecar pointers)."""

from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from pathlib import Path
from typing import Any, cast

from akc.control.policy_bundle import (
    POLICY_BUNDLE_REL_PATH,
    fingerprint_policy_bundle_bytes,
    index_fields_from_document,
    load_policy_bundle_json_bytes,
    validate_policy_bundle_document,
)
from akc.memory.models import JSONValue, normalize_repo_id, require_non_empty
from akc.run.manifest import RunManifest

logger = logging.getLogger(__name__)

RUN_LABEL_KEY_MAX_LEN = 128
RUN_LABEL_VALUE_MAX_LEN = 512

_CONTROL_PLANE_REF_KEYS: tuple[tuple[str, str], ...] = (
    ("runtime_evidence_ref", "runtime_evidence_ref"),
    ("policy_decisions_ref", "policy_decisions_ref"),
    ("coordination_audit_ref", "coordination_audit_ref"),
    ("replay_decisions_ref", "replay_decisions_ref"),
    ("recompile_triggers_ref", "recompile_triggers_ref"),
    ("operational_validity_report_ref", "operational_validity_report_ref"),
    ("operational_assurance_ref", "operational_assurance_ref"),
    ("governance_profile_ref", "governance_profile_ref"),
)
_MAX_OPERATIONAL_PREDICATE_SUMMARY_ROWS = 16


def validate_run_label_key_value(*, label_key: str, label_value: str) -> tuple[str, str]:
    """Bounds for operator ``run_labels`` / HTTP label writes (SQLite-safe, matches manifest normalization)."""

    ks = str(label_key).strip()
    vs = str(label_value).strip()
    require_non_empty(ks, name="label_key")
    require_non_empty(vs, name="label_value")
    if len(ks) > RUN_LABEL_KEY_MAX_LEN:
        raise ValueError(f"label_key exceeds max length ({RUN_LABEL_KEY_MAX_LEN})")
    if len(vs) > RUN_LABEL_VALUE_MAX_LEN:
        raise ValueError(f"label_value exceeds max length ({RUN_LABEL_VALUE_MAX_LEN})")
    return ks, vs


def operations_sqlite_path(*, outputs_root: str | Path, tenant_id: str) -> Path:
    """Tenant-scoped path: ``<outputs_root>/<tenant>/.akc/control/operations.sqlite``."""
    require_non_empty(tenant_id, name="tenant_id")
    return Path(outputs_root).expanduser().resolve() / tenant_id.strip() / ".akc" / "control" / "operations.sqlite"


def try_upsert_operations_index_from_manifest(
    manifest_path: str | Path,
    *,
    outputs_root: Path | None = None,
) -> None:
    """Fail-soft hook for compile/runtime manifest writes (logs at debug on failure)."""

    try:
        OperationsIndex.upsert_from_manifest_path(manifest_path, outputs_root=outputs_root)
    except Exception:
        logger.debug("operations index upsert failed for %s", manifest_path, exc_info=True)


def _resolve_outputs_root(manifest_path: Path, outputs_root: Path | None) -> Path:
    if outputs_root is not None:
        return Path(outputs_root).expanduser().resolve()
    inferred = infer_outputs_root_from_run_manifest_path(manifest_path)
    if inferred is None:
        raise ValueError(f"cannot infer outputs_root from manifest path: {manifest_path}")
    return inferred


def infer_outputs_root_from_run_manifest_path(manifest_path: Path) -> Path | None:
    """Infer ``outputs_root`` from ``.../<tenant>/<repo>/.akc/run/<id>.manifest.json``."""

    parts = manifest_path.resolve().parts
    if not manifest_path.name.endswith(".manifest.json"):
        return None
    try:
        akc_i = parts.index(".akc")
    except ValueError:
        return None
    if akc_i < 2 or akc_i + 1 >= len(parts) or parts[akc_i + 1] != "run":
        return None
    return Path(*parts[: akc_i - 2])


def _validate_manifest_path_matches_record(
    *,
    manifest_path: Path,
    outputs_root: Path,
    manifest: RunManifest,
) -> tuple[str, str]:
    rel = manifest_path.resolve().relative_to(outputs_root.resolve())
    parts = rel.parts
    if len(parts) < 5 or parts[-2] != "run" or ".akc" not in parts:
        raise ValueError("manifest path is not under <outputs_root>/<tenant>/<repo>/.akc/run/")
    akc_i = parts.index(".akc")
    tenant_seg = parts[akc_i - 2]
    repo_seg = parts[akc_i - 1]
    if tenant_seg != manifest.tenant_id.strip():
        raise ValueError("manifest path tenant_id does not match manifest record")
    if normalize_repo_id(repo_seg) != normalize_repo_id(manifest.repo_id):
        raise ValueError("manifest path repo_id does not match manifest record")
    return tenant_seg, repo_seg


def _manifest_rel_path(*, outputs_root: Path, manifest_path: Path) -> str:
    return str(manifest_path.resolve().relative_to(outputs_root.resolve()))


def _normalized_run_labels(raw: object) -> dict[str, str]:
    """String labels for ``run_labels`` control_plane field (bounded for SQLite safety)."""

    if not isinstance(raw, dict):
        return {}
    out: dict[str, str] = {}
    for k, v in raw.items():
        ks = str(k).strip()
        if not ks or len(ks) > 128:
            continue
        if not isinstance(v, str):
            continue
        vs = v.strip()
        if not vs or len(vs) > 512:
            continue
        out[ks] = vs
        if len(out) >= 32:
            break
    return out


def _operational_validity_passed_cell(control_plane: dict[str, Any] | None) -> int | None:
    if not isinstance(control_plane, dict):
        return None
    raw = control_plane.get("operational_validity_passed")
    if raw is True:
        return 1
    if raw is False:
        return 0
    return None


def _time_compression_metric_cell(control_plane: dict[str, Any] | None, *, key: str) -> float | None:
    if not isinstance(control_plane, dict):
        return None
    metrics = control_plane.get("time_compression_metrics")
    if not isinstance(metrics, dict):
        return None
    raw = metrics.get(key)
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return float(raw)
    return None


def _policy_provenance_columns(control_plane: dict[str, Any] | None) -> tuple[str | None, str | None, str | None]:
    if not isinstance(control_plane, dict):
        return None, None, None

    def _cell(key: str) -> str | None:
        raw = control_plane.get(key)
        s = str(raw).strip() if raw is not None else ""
        return s or None

    return _cell("policy_bundle_id"), _cell("policy_git_sha"), _cell("rego_pack_version")


def _policy_bundle_artifact_from_join(
    *,
    bundle_rel_path: str | None,
    fingerprint_sha256: str | None,
    last_modified_ms: int | None,
    rollout_stage: str | None,
    revision_id: str | None,
) -> dict[str, JSONValue] | None:
    if not bundle_rel_path or not fingerprint_sha256:
        return None
    out: dict[str, JSONValue] = {
        "rel_path": str(bundle_rel_path),
        "fingerprint_sha256": str(fingerprint_sha256),
    }
    if last_modified_ms is not None:
        out["last_modified_ms"] = int(last_modified_ms)
    if rollout_stage:
        out["rollout_stage"] = str(rollout_stage)
    if revision_id:
        out["revision_id"] = str(revision_id)
    return out


def _bounded_summary_str(value: object, *, max_len: int = 240) -> str | None:
    s = str(value).strip() if value is not None else ""
    if not s:
        return None
    return s[:max_len]


def _normalize_predicate_summary_row(raw: object) -> dict[str, JSONValue] | None:
    if not isinstance(raw, dict):
        return None
    out: dict[str, JSONValue] = {"passed": bool(raw.get("passed"))}
    for key in ("success_criterion_id", "predicate_kind", "signal_key", "message"):
        sv = _bounded_summary_str(raw.get(key))
        if sv is not None:
            out[key] = sv
    details = raw.get("details")
    if isinstance(details, dict):
        detail_out: dict[str, JSONValue] = {}
        for key in (
            "metric_name",
            "payload_path",
            "event_type",
            "health_status",
            "comparator",
            "target",
            "observed",
            "matched_spans",
            "good_spans",
            "fraction",
            "numerator_sum",
            "denominator_sum",
            "ratio",
            "series_distinct",
            "points_considered",
            "threshold",
            "expected",
            "actual",
            "value",
            "reason",
        ):
            if key not in details:
                continue
            dv = details.get(key)
            if isinstance(dv, (bool, int, float)):
                detail_out[key] = dv
            else:
                ds = _bounded_summary_str(dv)
                if ds is not None:
                    detail_out[key] = ds
        if detail_out:
            out["details"] = detail_out
    return out


def _load_operational_predicate_summary(
    *,
    scope_root: Path,
    control_plane: dict[str, Any] | None,
) -> dict[str, JSONValue] | None:
    if not isinstance(control_plane, dict):
        return None
    ref = control_plane.get("operational_validity_report_ref")
    if not isinstance(ref, dict):
        return None
    rel = str(ref.get("path", "")).strip()
    if not rel:
        return None
    target = (scope_root / rel).resolve()
    scope_r = scope_root.resolve()
    try:
        target.relative_to(scope_r)
    except ValueError:
        return None
    if not target.is_file():
        return None
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    pred_rows = raw.get("predicate_results")
    if not isinstance(pred_rows, list):
        return None
    total = 0
    failed = 0
    sample: list[JSONValue] = []
    failing: list[JSONValue] = []
    for item in pred_rows:
        normalized = _normalize_predicate_summary_row(item)
        if normalized is None:
            continue
        total += 1
        if len(sample) < _MAX_OPERATIONAL_PREDICATE_SUMMARY_ROWS:
            sample.append(normalized)
        if normalized.get("passed") is False:
            failed += 1
            if len(failing) < _MAX_OPERATIONAL_PREDICATE_SUMMARY_ROWS:
                failing.append(normalized)
    if total == 0:
        return None
    return {
        "total_count": total,
        "failed_count": failed,
        "failing": failing,
        "sample": sample,
    }


def _decode_json_cell(raw: object) -> JSONValue | None:
    if raw in (None, ""):
        return None
    try:
        return cast(JSONValue, json.loads(str(raw)))
    except (TypeError, ValueError, json.JSONDecodeError):
        return None


def _ingest_repo_policy_bundle(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    repo_id: str,
    scope_root: Path,
) -> None:
    """Index ``.akc/control/policy_bundle.json`` fingerprint + lifecycle fields."""

    rel = POLICY_BUNDLE_REL_PATH
    target = (scope_root / ".akc" / "control" / "policy_bundle.json").resolve()
    scope_r = scope_root.resolve()
    try:
        target.relative_to(scope_r)
    except ValueError:
        return
    if not target.is_file():
        conn.execute(
            "DELETE FROM repo_policy_bundle WHERE tenant_id = ? AND repo_id = ?",
            (tenant_id, repo_id),
        )
        return
    try:
        data = target.read_bytes()
        mtime_ms = int(target.stat().st_mtime * 1000)
    except OSError:
        return
    try:
        doc = load_policy_bundle_json_bytes(data)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
        conn.execute(
            "DELETE FROM repo_policy_bundle WHERE tenant_id = ? AND repo_id = ?",
            (tenant_id, repo_id),
        )
        return
    errs = validate_policy_bundle_document(doc)
    if errs:
        conn.execute(
            "DELETE FROM repo_policy_bundle WHERE tenant_id = ? AND repo_id = ?",
            (tenant_id, repo_id),
        )
        return
    fp = fingerprint_policy_bundle_bytes(data)
    rollout, revision = index_fields_from_document(doc)
    conn.execute(
        """
        INSERT INTO repo_policy_bundle (
            tenant_id, repo_id, bundle_rel_path, fingerprint_sha256, last_modified_ms,
            rollout_stage, revision_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(tenant_id, repo_id) DO UPDATE SET
            bundle_rel_path=excluded.bundle_rel_path,
            fingerprint_sha256=excluded.fingerprint_sha256,
            last_modified_ms=excluded.last_modified_ms,
            rollout_stage=excluded.rollout_stage,
            revision_id=excluded.revision_id
        """,
        (tenant_id, repo_id, rel, fp, mtime_ms, rollout, revision),
    )


def _ingest_repo_knowledge_decisions(
    conn: sqlite3.Connection,
    *,
    tenant_id: str,
    repo_id: str,
    scope_root: Path,
) -> None:
    """Index ``.akc/knowledge/decisions.json`` fingerprint + mtime for operator overrides."""

    rel = ".akc/knowledge/decisions.json"
    target = (scope_root / ".akc" / "knowledge" / "decisions.json").resolve()
    scope_r = scope_root.resolve()
    try:
        target.relative_to(scope_r)
    except ValueError:
        return
    if not target.is_file():
        conn.execute(
            "DELETE FROM repo_knowledge_decisions WHERE tenant_id = ? AND repo_id = ?",
            (tenant_id, repo_id),
        )
        return
    try:
        data = target.read_bytes()
        mtime_ms = int(target.stat().st_mtime * 1000)
    except OSError:
        return
    fp = hashlib.sha256(data).hexdigest()
    conn.execute(
        """
        INSERT INTO repo_knowledge_decisions (
            tenant_id, repo_id, decisions_rel_path, fingerprint_sha256, last_modified_ms
        ) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(tenant_id, repo_id) DO UPDATE SET
            decisions_rel_path=excluded.decisions_rel_path,
            fingerprint_sha256=excluded.fingerprint_sha256,
            last_modified_ms=excluded.last_modified_ms
        """,
        (tenant_id, repo_id, rel, fp, mtime_ms),
    )


def _migrate_ops_schema(conn: sqlite3.Connection) -> None:
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(runs)").fetchall()}
    if "policy_bundle_id" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN policy_bundle_id TEXT")
    if "policy_git_sha" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN policy_git_sha TEXT")
    if "rego_pack_version" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN rego_pack_version TEXT")
    if "operational_validity_passed" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN operational_validity_passed INTEGER")
    if "operational_predicate_summary_json" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN operational_predicate_summary_json TEXT")
    if "intent_to_healthy_runtime_ms" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN intent_to_healthy_runtime_ms REAL")
    if "compile_to_healthy_runtime_ms" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN compile_to_healthy_runtime_ms REAL")
    if "compression_factor_vs_baseline" not in cols:
        conn.execute("ALTER TABLE runs ADD COLUMN compression_factor_vs_baseline REAL")
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS repo_knowledge_decisions (
            tenant_id TEXT NOT NULL,
            repo_id TEXT NOT NULL,
            decisions_rel_path TEXT NOT NULL,
            fingerprint_sha256 TEXT,
            last_modified_ms INTEGER,
            PRIMARY KEY (tenant_id, repo_id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS repo_policy_bundle (
            tenant_id TEXT NOT NULL,
            repo_id TEXT NOT NULL,
            bundle_rel_path TEXT NOT NULL,
            fingerprint_sha256 TEXT,
            last_modified_ms INTEGER,
            rollout_stage TEXT,
            revision_id TEXT,
            PRIMARY KEY (tenant_id, repo_id)
        )
        """
    )


def _pass_counts(manifest: RunManifest) -> tuple[int, int, int]:
    succeeded = failed = skipped = 0
    for p in manifest.passes:
        if p.status == "succeeded":
            succeeded += 1
        elif p.status == "failed":
            failed += 1
        else:
            skipped += 1
    return succeeded, failed, skipped


def _aggregate_health_from_manifest(manifest: RunManifest) -> str | None:
    for rec in reversed(manifest.runtime_evidence):
        if rec.evidence_type != "terminal_health":
            continue
        if rec.payload.get("aggregate") is True:
            hs = rec.payload.get("health_status")
            if isinstance(hs, str) and hs.strip():
                return hs.strip().lower()
    for rec in reversed(manifest.runtime_evidence):
        if rec.evidence_type == "terminal_health":
            hs = rec.payload.get("health_status")
            if isinstance(hs, str) and hs.strip():
                return hs.strip().lower()
    return None


def _runtime_evidence_present(manifest: RunManifest, control_plane: dict[str, Any] | None) -> bool:
    if len(manifest.runtime_evidence) > 0:
        return True
    if not control_plane:
        return False
    ref = control_plane.get("runtime_evidence_ref")
    if not isinstance(ref, dict):
        return False
    path = str(ref.get("path", "")).strip()
    sha = str(ref.get("sha256", "")).strip()
    return bool(path and len(sha) == 64)


def _count_recompile_triggers(*, scope_root: Path, control_plane: dict[str, Any] | None) -> int:
    if not control_plane:
        return 0
    ref = control_plane.get("recompile_triggers_ref")
    if not isinstance(ref, dict):
        return 0
    rel = str(ref.get("path", "")).strip()
    if not rel:
        return 0
    target = (scope_root / rel).resolve()
    scope_r = scope_root.resolve()
    try:
        target.relative_to(scope_r)
    except ValueError:
        return 0
    if not target.is_file():
        return 0
    try:
        raw = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    if not isinstance(raw, dict):
        return 0
    triggers = raw.get("triggers")
    if isinstance(triggers, list):
        return len(triggers)
    return 0


def _sidecar_rows(
    *,
    manifest: RunManifest,
    control_plane: dict[str, Any] | None,
) -> list[tuple[str, str, str | None]]:
    rows: list[tuple[str, str, str | None]] = []
    for field, ptr in (
        ("ir_document", manifest.ir_document),
        ("knowledge_snapshot", manifest.knowledge_snapshot),
        ("runtime_bundle", manifest.runtime_bundle),
        ("runtime_event_transcript", manifest.runtime_event_transcript),
    ):
        if ptr is not None:
            rows.append((field, ptr.path.strip(), ptr.sha256))
    if control_plane:
        for json_key, kind in _CONTROL_PLANE_REF_KEYS:
            ref = control_plane.get(json_key)
            if not isinstance(ref, dict):
                continue
            pth = str(ref.get("path", "")).strip()
            if not pth:
                continue
            sha_raw = ref.get("sha256")
            sha = str(sha_raw).strip().lower() if sha_raw is not None else None
            if sha is not None and (len(sha) != 64 or any(c not in "0123456789abcdef" for c in sha)):
                sha = None
            rows.append((kind, pth, sha))
    if manifest.output_hashes:
        run_prefix = f".akc/run/{manifest.run_id.strip()}"
        for rel, digest in manifest.output_hashes.items():
            rs = str(rel).strip()
            if not rs.startswith(run_prefix):
                continue
            if rs.endswith(".costs.json"):
                rows.append(("costs", rs, str(digest).strip().lower() if digest else None))
            elif rs.endswith(".spans.json"):
                rows.append(("trace_spans", rs, str(digest).strip().lower() if digest else None))
            elif rs.endswith(".otel.jsonl"):
                rows.append(("otel_trace_export", rs, str(digest).strip().lower() if digest else None))
    return rows


class OperationsIndex:
    """Cross-run catalog under ``<outputs_root>/<tenant>/.akc/control/operations.sqlite``."""

    sqlite_path: str | Path

    def __init__(self, sqlite_path: str | Path) -> None:
        self.sqlite_path = sqlite_path

    def _db_path(self) -> Path:
        return Path(self.sqlite_path).expanduser()

    def _connect(self) -> sqlite3.Connection:
        p = self._db_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(p))
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                tenant_id TEXT NOT NULL,
                repo_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                manifest_rel_path TEXT NOT NULL,
                stable_intent_sha256 TEXT,
                replay_mode TEXT NOT NULL,
                pass_succeeded INTEGER NOT NULL DEFAULT 0,
                pass_failed INTEGER NOT NULL DEFAULT 0,
                pass_skipped INTEGER NOT NULL DEFAULT 0,
                recompile_trigger_count INTEGER NOT NULL DEFAULT 0,
                runtime_evidence_present INTEGER NOT NULL DEFAULT 0,
                aggregate_health TEXT,
                policy_bundle_id TEXT,
                policy_git_sha TEXT,
                rego_pack_version TEXT,
                intent_to_healthy_runtime_ms REAL,
                compile_to_healthy_runtime_ms REAL,
                compression_factor_vs_baseline REAL,
                PRIMARY KEY (tenant_id, repo_id, run_id)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_sidecars (
                tenant_id TEXT NOT NULL,
                repo_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                rel_path TEXT NOT NULL,
                sha256 TEXT,
                PRIMARY KEY (tenant_id, repo_id, run_id, kind)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS run_labels (
                tenant_id TEXT NOT NULL,
                repo_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                label_key TEXT NOT NULL,
                label_value TEXT NOT NULL,
                PRIMARY KEY (tenant_id, repo_id, run_id, label_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS automation_checkpoints (
                dedupe_key TEXT PRIMARY KEY,
                tenant_id TEXT NOT NULL,
                repo_id TEXT NOT NULL,
                run_id TEXT NOT NULL,
                action TEXT NOT NULL,
                policy_version TEXT NOT NULL,
                shard_id TEXT NOT NULL,
                status TEXT NOT NULL,
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at_ms INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                last_result_json TEXT,
                updated_at_ms INTEGER NOT NULL
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_runs_updated ON runs(updated_at_ms)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_runs_tenant ON runs(tenant_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_runs_tenant_repo ON runs(tenant_id, repo_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ops_runs_intent ON runs(tenant_id, stable_intent_sha256)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_ops_automation_tenant_status "
            "ON automation_checkpoints(tenant_id, status, next_attempt_at_ms)"
        )
        _migrate_ops_schema(conn)
        return conn

    @staticmethod
    def upsert_from_manifest_path(manifest_path: str | Path, *, outputs_root: Path | None = None) -> None:
        mp = Path(manifest_path).expanduser().resolve()
        manifest = RunManifest.from_json_file(mp)
        root = _resolve_outputs_root(mp, outputs_root)
        _validate_manifest_path_matches_record(manifest_path=mp, outputs_root=root, manifest=manifest)
        repo_id_store = normalize_repo_id(manifest.repo_id)
        sqlite_p = operations_sqlite_path(outputs_root=root, tenant_id=manifest.tenant_id.strip())
        idx = OperationsIndex(sqlite_p)
        idx._upsert_run(
            manifest=manifest,
            manifest_path=mp,
            outputs_root=root,
            repo_id_store=repo_id_store,
        )

    def _upsert_run(
        self,
        *,
        manifest: RunManifest,
        manifest_path: Path,
        outputs_root: Path,
        repo_id_store: str,
    ) -> None:
        control_plane = manifest.control_plane
        cp_payload: dict[str, Any] | None = dict(control_plane) if isinstance(control_plane, dict) else None
        scope_root = manifest_path.resolve().parent.parent.parent
        trig_count = _count_recompile_triggers(scope_root=scope_root, control_plane=cp_payload)
        ev_present = 1 if _runtime_evidence_present(manifest, cp_payload) else 0
        ps, pf, psk = _pass_counts(manifest)
        health = _aggregate_health_from_manifest(manifest)
        ov_pass = _operational_validity_passed_cell(cp_payload)
        intent_to_healthy_runtime_ms = _time_compression_metric_cell(cp_payload, key="intent_to_healthy_runtime_ms")
        compile_to_healthy_runtime_ms = _time_compression_metric_cell(cp_payload, key="compile_to_healthy_runtime_ms")
        compression_factor_vs_baseline = _time_compression_metric_cell(cp_payload, key="compression_factor_vs_baseline")
        ov_summary = _load_operational_predicate_summary(scope_root=scope_root, control_plane=cp_payload)
        ov_summary_json = json.dumps(ov_summary, sort_keys=True) if ov_summary is not None else None
        rel_manifest = _manifest_rel_path(outputs_root=outputs_root, manifest_path=manifest_path)
        try:
            updated_ms = int(manifest_path.stat().st_mtime * 1000)
        except OSError:
            updated_ms = int(time.time() * 1000)

        tenant_key = manifest.tenant_id.strip()
        run_key = manifest.run_id.strip()
        stable = manifest.stable_intent_sha256
        pb, pg, rv = _policy_provenance_columns(cp_payload)

        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (
                    tenant_id, repo_id, run_id, updated_at_ms, manifest_rel_path,
                    stable_intent_sha256, replay_mode,
                    pass_succeeded, pass_failed, pass_skipped,
                    recompile_trigger_count, runtime_evidence_present, aggregate_health,
                    policy_bundle_id, policy_git_sha, rego_pack_version,
                    operational_validity_passed, operational_predicate_summary_json,
                    intent_to_healthy_runtime_ms, compile_to_healthy_runtime_ms, compression_factor_vs_baseline
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id, run_id) DO UPDATE SET
                    updated_at_ms=excluded.updated_at_ms,
                    manifest_rel_path=excluded.manifest_rel_path,
                    stable_intent_sha256=excluded.stable_intent_sha256,
                    replay_mode=excluded.replay_mode,
                    pass_succeeded=excluded.pass_succeeded,
                    pass_failed=excluded.pass_failed,
                    pass_skipped=excluded.pass_skipped,
                    recompile_trigger_count=excluded.recompile_trigger_count,
                    runtime_evidence_present=excluded.runtime_evidence_present,
                    aggregate_health=excluded.aggregate_health,
                    policy_bundle_id=excluded.policy_bundle_id,
                    policy_git_sha=excluded.policy_git_sha,
                    rego_pack_version=excluded.rego_pack_version,
                    operational_validity_passed=excluded.operational_validity_passed,
                    operational_predicate_summary_json=excluded.operational_predicate_summary_json,
                    intent_to_healthy_runtime_ms=excluded.intent_to_healthy_runtime_ms,
                    compile_to_healthy_runtime_ms=excluded.compile_to_healthy_runtime_ms,
                    compression_factor_vs_baseline=excluded.compression_factor_vs_baseline
                """,
                (
                    tenant_key,
                    repo_id_store,
                    run_key,
                    updated_ms,
                    rel_manifest,
                    stable,
                    str(manifest.replay_mode),
                    ps,
                    pf,
                    psk,
                    int(trig_count),
                    ev_present,
                    health,
                    pb,
                    pg,
                    rv,
                    ov_pass,
                    ov_summary_json,
                    intent_to_healthy_runtime_ms,
                    compile_to_healthy_runtime_ms,
                    compression_factor_vs_baseline,
                ),
            )
            conn.execute(
                "DELETE FROM run_sidecars WHERE tenant_id = ? AND repo_id = ? AND run_id = ?",
                (tenant_key, repo_id_store, run_key),
            )
            for kind, rpath, sha in _sidecar_rows(manifest=manifest, control_plane=cp_payload):
                conn.execute(
                    """
                    INSERT INTO run_sidecars (tenant_id, repo_id, run_id, kind, rel_path, sha256)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(tenant_id, repo_id, run_id, kind) DO UPDATE SET
                        rel_path=excluded.rel_path,
                        sha256=excluded.sha256
                    """,
                    (tenant_key, repo_id_store, run_key, kind, rpath, sha),
                )
            if cp_payload is not None and "run_labels" in cp_payload:
                labels_obj = cp_payload.get("run_labels")
                if isinstance(labels_obj, dict):
                    conn.execute(
                        "DELETE FROM run_labels WHERE tenant_id = ? AND repo_id = ? AND run_id = ?",
                        (tenant_key, repo_id_store, run_key),
                    )
                    for lk, lv in _normalized_run_labels(labels_obj).items():
                        conn.execute(
                            """
                            INSERT INTO run_labels (tenant_id, repo_id, run_id, label_key, label_value)
                            VALUES (?, ?, ?, ?, ?)
                            ON CONFLICT(tenant_id, repo_id, run_id, label_key) DO UPDATE SET
                                label_value=excluded.label_value
                            """,
                            (tenant_key, repo_id_store, run_key, lk, lv),
                        )
            _ingest_repo_knowledge_decisions(
                conn,
                tenant_id=tenant_key,
                repo_id=repo_id_store,
                scope_root=scope_root,
            )
            _ingest_repo_policy_bundle(
                conn,
                tenant_id=tenant_key,
                repo_id=repo_id_store,
                scope_root=scope_root,
            )

    @classmethod
    def sync_repo_policy_bundle_for_scope(
        cls,
        *,
        outputs_root: str | Path,
        tenant_id: str,
        repo_id: str,
    ) -> None:
        """Re-read ``policy_bundle.json`` for one repo into the tenant operations index."""

        root = Path(outputs_root).expanduser().resolve()
        t = tenant_id.strip()
        r = normalize_repo_id(repo_id)
        scope_root = root / t / r
        sqlite_p = operations_sqlite_path(outputs_root=root, tenant_id=t)
        idx = cls(sqlite_p)
        with idx._connect() as conn:
            _ingest_repo_policy_bundle(conn, tenant_id=t, repo_id=r, scope_root=scope_root)

    @classmethod
    def rebuild_for_tenant(cls, *, outputs_root: str | Path, tenant_id: str) -> int:
        """Scan ``<outputs_root>/<tenant>/*/ .akc/run/*.manifest.json`` and upsert."""

        require_non_empty(tenant_id, name="tenant_id")
        root = Path(outputs_root).expanduser().resolve()
        tenant_dir = root / tenant_id.strip()
        if not tenant_dir.is_dir():
            return 0
        n = 0
        for repo_dir in sorted(tenant_dir.iterdir()):
            if not repo_dir.is_dir():
                continue
            run_dir = repo_dir / ".akc" / "run"
            if not run_dir.is_dir():
                continue
            for mp in sorted(run_dir.glob("*.manifest.json")):
                try:
                    cls.upsert_from_manifest_path(mp, outputs_root=root)
                    n += 1
                except Exception:
                    logger.debug("rebuild skip %s", mp, exc_info=True)
        return n

    def list_runs(
        self,
        *,
        tenant_id: str,
        repo_id: str | None = None,
        since_ms: int | None = None,
        until_ms: int | None = None,
        stable_intent_sha256: str | None = None,
        has_recompile_triggers: bool | None = None,
        runtime_evidence_present: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, JSONValue]]:
        require_non_empty(tenant_id, name="tenant_id")
        lim = max(1, min(int(limit), 500))
        off = max(0, int(offset))
        clauses: list[str] = ["r.tenant_id = ?"]
        params: list[object] = [tenant_id.strip()]
        if repo_id is not None and str(repo_id).strip():
            clauses.append("r.repo_id = ?")
            params.append(normalize_repo_id(str(repo_id)))
        if since_ms is not None:
            clauses.append("r.updated_at_ms >= ?")
            params.append(int(since_ms))
        if until_ms is not None:
            clauses.append("r.updated_at_ms <= ?")
            params.append(int(until_ms))
        if stable_intent_sha256 is not None and str(stable_intent_sha256).strip():
            clauses.append("r.stable_intent_sha256 = ?")
            params.append(str(stable_intent_sha256).strip().lower())
        if has_recompile_triggers is True:
            clauses.append("r.recompile_trigger_count > 0")
        elif has_recompile_triggers is False:
            clauses.append("r.recompile_trigger_count = 0")
        if runtime_evidence_present is True:
            clauses.append("r.runtime_evidence_present = 1")
        elif runtime_evidence_present is False:
            clauses.append("r.runtime_evidence_present = 0")
        where_sql = " AND ".join(clauses)
        params.append(lim)
        params.append(off)
        with self._connect() as conn:
            rows = conn.execute(
                f"""
                SELECT
                    r.tenant_id, r.repo_id, r.run_id, r.updated_at_ms, r.manifest_rel_path,
                    r.stable_intent_sha256, r.replay_mode,
                    r.pass_succeeded, r.pass_failed, r.pass_skipped,
                    r.recompile_trigger_count, r.runtime_evidence_present, r.aggregate_health,
                    r.policy_bundle_id, r.policy_git_sha, r.rego_pack_version,
                    r.operational_validity_passed, r.operational_predicate_summary_json,
                    r.intent_to_healthy_runtime_ms, r.compile_to_healthy_runtime_ms, r.compression_factor_vs_baseline,
                    pb.bundle_rel_path, pb.fingerprint_sha256, pb.last_modified_ms,
                    pb.rollout_stage, pb.revision_id
                FROM runs r
                LEFT JOIN repo_policy_bundle pb
                  ON r.tenant_id = pb.tenant_id AND r.repo_id = pb.repo_id
                WHERE {where_sql}
                ORDER BY r.updated_at_ms DESC, r.rowid DESC
                LIMIT ? OFFSET ?
                """,
                tuple(params),
            ).fetchall()
        out: list[dict[str, JSONValue]] = []
        for row in rows:
            ov_raw = row[16]
            ov_out: bool | None = None if ov_raw is None else bool(int(ov_raw))
            ov_summary = _decode_json_cell(row[17])
            pba = _policy_bundle_artifact_from_join(
                bundle_rel_path=str(row[21]) if row[21] else None,
                fingerprint_sha256=str(row[22]) if row[22] else None,
                last_modified_ms=int(row[23]) if row[23] is not None else None,
                rollout_stage=str(row[24]) if row[24] else None,
                revision_id=str(row[25]) if row[25] else None,
            )
            out.append(
                {
                    "tenant_id": str(row[0]),
                    "repo_id": str(row[1]),
                    "run_id": str(row[2]),
                    "updated_at_ms": int(row[3]),
                    "manifest_rel_path": str(row[4]),
                    "stable_intent_sha256": str(row[5]) if row[5] else None,
                    "replay_mode": str(row[6]),
                    "pass_succeeded": int(row[7]),
                    "pass_failed": int(row[8]),
                    "pass_skipped": int(row[9]),
                    "recompile_trigger_count": int(row[10]),
                    "runtime_evidence_present": bool(row[11]),
                    "aggregate_health": str(row[12]) if row[12] else None,
                    "policy_bundle_id": str(row[13]) if row[13] else None,
                    "policy_git_sha": str(row[14]) if row[14] else None,
                    "rego_pack_version": str(row[15]) if row[15] else None,
                    "operational_validity_passed": ov_out,
                    "operational_predicate_summary": ov_summary,
                    "intent_to_healthy_runtime_ms": float(row[18]) if row[18] is not None else None,
                    "compile_to_healthy_runtime_ms": float(row[19]) if row[19] is not None else None,
                    "compression_factor_vs_baseline": float(row[20]) if row[20] is not None else None,
                    "policy_bundle_artifact": pba,
                }
            )
        return out

    def get_run(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        run_id: str,
    ) -> dict[str, Any] | None:
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(repo_id, name="repo_id")
        require_non_empty(run_id, name="run_id")
        rnorm = normalize_repo_id(repo_id)
        pb_row: tuple[Any, ...] | None = None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    tenant_id, repo_id, run_id, updated_at_ms, manifest_rel_path,
                    stable_intent_sha256, replay_mode,
                    pass_succeeded, pass_failed, pass_skipped,
                    recompile_trigger_count, runtime_evidence_present, aggregate_health,
                    policy_bundle_id, policy_git_sha, rego_pack_version,
                    operational_validity_passed, operational_predicate_summary_json
                FROM runs
                WHERE tenant_id = ? AND repo_id = ? AND run_id = ?
                """,
                (tenant_id.strip(), rnorm, run_id.strip()),
            ).fetchone()
            if row is None:
                return None
            know = conn.execute(
                """
                SELECT decisions_rel_path, fingerprint_sha256, last_modified_ms
                FROM repo_knowledge_decisions
                WHERE tenant_id = ? AND repo_id = ?
                """,
                (tenant_id.strip(), rnorm),
            ).fetchone()
            pb_row = conn.execute(
                """
                SELECT bundle_rel_path, fingerprint_sha256, last_modified_ms, rollout_stage, revision_id
                FROM repo_policy_bundle
                WHERE tenant_id = ? AND repo_id = ?
                """,
                (tenant_id.strip(), rnorm),
            ).fetchone()
            srows = conn.execute(
                """
                SELECT kind, rel_path, sha256
                FROM run_sidecars
                WHERE tenant_id = ? AND repo_id = ? AND run_id = ?
                ORDER BY kind ASC, rel_path ASC
                """,
                (tenant_id.strip(), rnorm, run_id.strip()),
            ).fetchall()
        sidecars: list[dict[str, JSONValue]] = [
            {
                "kind": str(sr[0]),
                "rel_path": str(sr[1]),
                "sha256": str(sr[2]) if sr[2] else None,
            }
            for sr in srows
        ]
        labels_rows = self._list_labels(tenant_id=tenant_id.strip(), repo_id=rnorm, run_id=run_id.strip())
        knowledge_decisions: dict[str, JSONValue] | None = None
        if know is not None:
            knowledge_decisions = {
                "decisions_rel_path": str(know[0]),
                "fingerprint_sha256": str(know[1]) if know[1] else None,
                "last_modified_ms": int(know[2]) if know[2] is not None else None,
            }

        ov_raw = row[16]
        ov_pass: bool | None = None if ov_raw is None else bool(int(ov_raw))
        ov_summary = _decode_json_cell(row[17])
        policy_bundle_artifact: dict[str, JSONValue] | None = None
        if pb_row is not None:
            policy_bundle_artifact = _policy_bundle_artifact_from_join(
                bundle_rel_path=str(pb_row[0]) if pb_row[0] else None,
                fingerprint_sha256=str(pb_row[1]) if pb_row[1] else None,
                last_modified_ms=int(pb_row[2]) if pb_row[2] is not None else None,
                rollout_stage=str(pb_row[3]) if pb_row[3] else None,
                revision_id=str(pb_row[4]) if pb_row[4] else None,
            )
        out: dict[str, Any] = {
            "tenant_id": str(row[0]),
            "repo_id": str(row[1]),
            "run_id": str(row[2]),
            "updated_at_ms": int(row[3]),
            "manifest_rel_path": str(row[4]),
            "stable_intent_sha256": str(row[5]) if row[5] else None,
            "replay_mode": str(row[6]),
            "pass_succeeded": int(row[7]),
            "pass_failed": int(row[8]),
            "pass_skipped": int(row[9]),
            "recompile_trigger_count": int(row[10]),
            "runtime_evidence_present": bool(row[11]),
            "aggregate_health": str(row[12]) if row[12] else None,
            "policy_bundle_id": str(row[13]) if row[13] else None,
            "policy_git_sha": str(row[14]) if row[14] else None,
            "rego_pack_version": str(row[15]) if row[15] else None,
            "operational_validity_passed": ov_pass,
            "operational_predicate_summary": ov_summary,
            "knowledge_decisions": knowledge_decisions,
            "policy_bundle_artifact": policy_bundle_artifact,
            "sidecars": sidecars,
            "labels": labels_rows,
        }
        return out

    def _list_labels(self, *, tenant_id: str, repo_id: str, run_id: str) -> dict[str, str]:
        with self._connect() as conn:
            lrows = conn.execute(
                """
                SELECT label_key, label_value
                FROM run_labels
                WHERE tenant_id = ? AND repo_id = ? AND run_id = ?
                ORDER BY label_key ASC
                """,
                (tenant_id, repo_id, run_id),
            ).fetchall()
        return {str(a[0]): str(a[1]) for a in lrows}

    def get_label_value(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        run_id: str,
        label_key: str,
    ) -> str | None:
        """Return stored value for ``label_key``, or ``None`` if absent."""

        rnorm = normalize_repo_id(repo_id)
        lk = str(label_key).strip()
        if not lk:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT label_value
                FROM run_labels
                WHERE tenant_id = ? AND repo_id = ? AND run_id = ? AND label_key = ?
                """,
                (tenant_id.strip(), rnorm, run_id.strip(), lk),
            ).fetchone()
        if row is None:
            return None
        return str(row[0])

    def upsert_label(
        self,
        *,
        tenant_id: str,
        repo_id: str,
        run_id: str,
        label_key: str,
        label_value: str,
    ) -> None:
        """Set one operator tag; persists until overwritten or cleared via manifest ``run_labels``."""

        lk, lv = validate_run_label_key_value(label_key=label_key, label_value=label_value)
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO run_labels (tenant_id, repo_id, run_id, label_key, label_value)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(tenant_id, repo_id, run_id, label_key) DO UPDATE SET
                    label_value=excluded.label_value
                """,
                (
                    tenant_id.strip(),
                    normalize_repo_id(repo_id),
                    run_id.strip(),
                    lk,
                    lv,
                ),
            )

    def get_automation_checkpoint(self, *, dedupe_key: str) -> dict[str, JSONValue] | None:
        key = str(dedupe_key).strip()
        if not key:
            return None
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT
                    dedupe_key, tenant_id, repo_id, run_id, action, policy_version, shard_id,
                    status, attempts, next_attempt_at_ms, last_error, last_result_json, updated_at_ms
                FROM automation_checkpoints
                WHERE dedupe_key = ?
                """,
                (key,),
            ).fetchone()
        if row is None:
            return None
        parsed = _decode_json_cell(row[11])
        return {
            "dedupe_key": str(row[0]),
            "tenant_id": str(row[1]),
            "repo_id": str(row[2]),
            "run_id": str(row[3]),
            "action": str(row[4]),
            "policy_version": str(row[5]),
            "shard_id": str(row[6]),
            "status": str(row[7]),
            "attempts": int(row[8]),
            "next_attempt_at_ms": int(row[9]),
            "last_error": str(row[10]) if row[10] else None,
            "last_result": parsed if parsed is not None else None,
            "updated_at_ms": int(row[12]),
        }

    def upsert_automation_checkpoint(
        self,
        *,
        dedupe_key: str,
        tenant_id: str,
        repo_id: str,
        run_id: str,
        action: str,
        policy_version: str,
        shard_id: str,
        status: str,
        attempts: int,
        next_attempt_at_ms: int,
        last_error: str | None = None,
        last_result: dict[str, JSONValue] | None = None,
        updated_at_ms: int | None = None,
    ) -> None:
        require_non_empty(dedupe_key, name="dedupe_key")
        require_non_empty(tenant_id, name="tenant_id")
        require_non_empty(repo_id, name="repo_id")
        require_non_empty(run_id, name="run_id")
        require_non_empty(action, name="action")
        require_non_empty(policy_version, name="policy_version")
        require_non_empty(shard_id, name="shard_id")
        require_non_empty(status, name="status")
        now_ms = int(time.time() * 1000) if updated_at_ms is None else int(updated_at_ms)
        result_json = json.dumps(last_result, sort_keys=True) if last_result is not None else None
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO automation_checkpoints (
                    dedupe_key, tenant_id, repo_id, run_id, action, policy_version, shard_id,
                    status, attempts, next_attempt_at_ms, last_error, last_result_json, updated_at_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(dedupe_key) DO UPDATE SET
                    status=excluded.status,
                    attempts=excluded.attempts,
                    next_attempt_at_ms=excluded.next_attempt_at_ms,
                    last_error=excluded.last_error,
                    last_result_json=excluded.last_result_json,
                    updated_at_ms=excluded.updated_at_ms
                """,
                (
                    dedupe_key.strip(),
                    tenant_id.strip(),
                    normalize_repo_id(repo_id),
                    run_id.strip(),
                    action.strip(),
                    policy_version.strip(),
                    shard_id.strip(),
                    status.strip(),
                    int(attempts),
                    int(next_attempt_at_ms),
                    str(last_error)[:2048] if last_error else None,
                    result_json,
                    now_ms,
                ),
            )
