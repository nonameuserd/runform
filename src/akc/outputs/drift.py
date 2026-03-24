from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_artifact_json
from akc.compile.interfaces import TenantRepoScope
from akc.memory.models import JSONValue, require_non_empty
from akc.outputs.fingerprints import (
    IngestStateFingerprint,
    fingerprint_file_bytes,
    stable_json_fingerprint,
)

DriftKind = Literal[
    "changed_sources",
    "changed_outputs",
    "missing_manifest",
    "changed_intent",
    "changed_knowledge_semantic",
    "changed_knowledge_provenance",
    # Post-runtime operational attestation did not pass while outputs/sources/knowledge contracts match.
    "operational_validity_failed",
]
DriftSeverity = Literal["low", "med", "high"]


@dataclass(frozen=True, slots=True)
class DriftFinding:
    kind: DriftKind
    severity: DriftSeverity
    details: Mapping[str, JSONValue]

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "kind": self.kind,
            "severity": self.severity,
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class DriftReport:
    scope: TenantRepoScope
    findings: Sequence[DriftFinding]

    def has_drift(self) -> bool:
        return bool(self.findings)

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "scope": {"tenant_id": self.scope.tenant_id, "repo_id": self.scope.repo_id},
            "findings": [f.to_json_obj() for f in self.findings],
        }

    def render_text(self) -> str:
        if not self.findings:
            return "OK: no drift detected.\n"
        lines: list[str] = ["DRIFT detected:"]
        for f in self.findings:
            lines.append(f"- {f.kind} ({f.severity})")
            for k in sorted(f.details.keys()):
                lines.append(f"  - {k}: {f.details[k]}")
        return "\n".join(lines) + "\n"


def _scope_dir(*, root: Path, scope: TenantRepoScope) -> Path:
    return (root / scope.tenant_id / scope.repo_id).resolve()


def _read_json_object(path: Path, *, what: str) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        raise
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError(f"{what} must be a JSON object")
    return loaded


def _write_json_object(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _upsert_manifest_artifact(
    *,
    manifest_obj: dict[str, Any],
    scope_dir: Path,
    relpath: str,
    media_type: str,
    metadata: Mapping[str, Any] | None = None,
) -> None:
    artifacts = manifest_obj.get("artifacts")
    if not isinstance(artifacts, list):
        return
    entry = {
        "path": relpath,
        "media_type": media_type,
        "sha256": _sha256_file(scope_dir / relpath),
        "size_bytes": (scope_dir / relpath).stat().st_size,
        "metadata": dict(metadata) if metadata else None,
    }
    for idx, artifact in enumerate(artifacts):
        if isinstance(artifact, dict) and str(artifact.get("path", "")).strip() == relpath:
            artifacts[idx] = entry
            return
    artifacts.append(entry)


def _update_manifest_living_metadata(
    *,
    scope: TenantRepoScope,
    outputs_root: Path,
    check_id: str,
    checked_at_ms: int,
    source: str,
    drift_path: Path,
    triggers_path: Path,
) -> None:
    manifest_path = _scope_dir(root=outputs_root, scope=scope) / "manifest.json"
    if not manifest_path.exists():
        return
    manifest_obj = _read_json_object(manifest_path, what="manifest.json")
    scope_dir = _scope_dir(root=outputs_root, scope=scope)
    drift_relpath = f".akc/living/{drift_path.name}"
    triggers_relpath = f".akc/living/{triggers_path.name}"
    _upsert_manifest_artifact(
        manifest_obj=manifest_obj,
        scope_dir=scope_dir,
        relpath=drift_relpath,
        media_type="application/json; charset=utf-8",
        metadata={"kind": "living_drift_report", "check_id": check_id, "source": source},
    )
    _upsert_manifest_artifact(
        manifest_obj=manifest_obj,
        scope_dir=scope_dir,
        relpath=triggers_relpath,
        media_type="application/json; charset=utf-8",
        metadata={"kind": "recompile_triggers", "check_id": check_id, "source": source},
    )
    metadata = manifest_obj.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}
    metadata["living_artifacts"] = {
        "latest_check_id": check_id,
        "checked_at_ms": checked_at_ms,
        "source": source,
        "order": ["drift_report", "recompile_triggers"],
        "groups": {"living": ["drift_report", "recompile_triggers"]},
        "artifacts": {
            "drift_report": drift_relpath,
            "recompile_triggers": triggers_relpath,
        },
        "output_hashes": {
            drift_relpath: _sha256_file(drift_path),
            triggers_relpath: _sha256_file(triggers_path),
        },
    }
    manifest_obj["metadata"] = metadata
    _write_json_object(manifest_path, manifest_obj)


def _manifest_expected_hashes(manifest_obj: Mapping[str, Any]) -> dict[str, str]:
    arts = manifest_obj.get("artifacts")
    if not isinstance(arts, list):
        raise ValueError("manifest.artifacts must be a list")
    out: dict[str, str] = {}
    for a in arts:
        if not isinstance(a, dict):
            raise ValueError("manifest.artifacts[] must be objects")
        p = a.get("path")
        h = a.get("sha256")
        if not isinstance(p, str) or not p.strip():
            raise ValueError("manifest.artifacts[].path must be a non-empty string")
        if not isinstance(h, str) or len(h) < 16:
            raise ValueError("manifest.artifacts[].sha256 must be a hex string")
        out[p] = h
    return out


def _ensure_under_root(*, root: Path, p: Path) -> None:
    """Reject path traversal outside the scoped outputs directory."""
    root_r = root.resolve()
    p_r = p.resolve()
    try:
        p_r.relative_to(root_r)
    except ValueError as e:
        raise ValueError("manifest artifact path escapes scoped outputs directory") from e


def drift_report(
    *,
    scope: TenantRepoScope,
    outputs_root: str | Path,
    ingest_fingerprint: IngestStateFingerprint | None = None,
    baseline_path: str | Path | None = None,
    intent_semantic_fingerprint: str | None = None,
    intent_goal_text_fingerprint: str | None = None,
    knowledge_semantic_fingerprint: str | None = None,
    knowledge_provenance_fingerprint: str | None = None,
) -> DriftReport:
    """Compute drift for `scope` under an outputs root.

    Drift categories implemented (Phase 4 minimal):
    - missing_manifest: no `manifest.json` present for the scope
    - changed_outputs: artifact file hash mismatch / missing artifacts
    - changed_sources: ingestion-state fingerprint differs from last recorded baseline (optional)
    - changed_intent: intent semantic fingerprint differs from last recorded baseline (optional)
    - changed_knowledge_semantic: knowledge semantic fingerprint differs from last recorded baseline (optional)
    - changed_knowledge_provenance: knowledge provenance fingerprint differs from last recorded baseline (optional)
    """

    require_non_empty(scope.tenant_id, name="scope.tenant_id")
    require_non_empty(scope.repo_id, name="scope.repo_id")
    root = Path(outputs_root).expanduser().resolve()
    scoped = _scope_dir(root=root, scope=scope)

    manifest_path = scoped / "manifest.json"
    findings: list[DriftFinding] = []
    try:
        manifest = _read_json_object(manifest_path, what="manifest.json")
    except FileNotFoundError:
        findings.append(
            DriftFinding(
                kind="missing_manifest",
                severity="high",
                details={"manifest_path": str(manifest_path)},
            )
        )
        return DriftReport(scope=scope, findings=findings)

    expected = _manifest_expected_hashes(manifest)
    missing: list[str] = []
    mismatched: list[str] = []
    invalid: list[str] = []
    for relpath, exp_hash in expected.items():
        fp = (scoped / relpath).resolve()
        try:
            _ensure_under_root(root=scoped, p=fp)
        except ValueError:
            invalid.append(relpath)
            continue
        try:
            actual_hash = fingerprint_file_bytes(path=fp)
        except FileNotFoundError:
            missing.append(relpath)
            continue
        if actual_hash != exp_hash:
            mismatched.append(relpath)

    if invalid or missing or mismatched:
        invalid_v: list[JSONValue] = [str(p) for p in invalid]
        missing_v: list[JSONValue] = [str(p) for p in missing]
        mismatched_v: list[JSONValue] = [str(p) for p in mismatched]
        findings.append(
            DriftFinding(
                kind="changed_outputs",
                severity="high",
                details={
                    "manifest_path": str(manifest_path),
                    "invalid_artifacts": invalid_v,
                    "missing_artifacts": missing_v,
                    "mismatched_artifacts": mismatched_v,
                },
            )
        )

    baseline: dict[str, Any] = {}
    bpath: Path | None = None
    if baseline_path is not None:
        bpath = Path(baseline_path).expanduser()
        try:
            baseline = _read_json_object(bpath, what="baseline")
        except FileNotFoundError:
            baseline = {}

    if ingest_fingerprint is not None and bpath is not None:
        # Baseline "contract" anchoring: compare the manifest.json itself.
        # This catches cases where sources may be unchanged but the stored
        # output bundle is stale/corrupt relative to the last accepted baseline.
        current_manifest_sha = stable_json_fingerprint(manifest)
        prior_manifest_sha = baseline.get("manifest_sha256")
        if isinstance(prior_manifest_sha, str) and prior_manifest_sha and prior_manifest_sha != current_manifest_sha:
            findings.append(
                DriftFinding(
                    kind="changed_outputs",
                    severity="high",
                    details={
                        "baseline_path": str(bpath),
                        "previous_manifest_sha256": prior_manifest_sha,
                        "current_manifest_sha256": current_manifest_sha,
                    },
                )
            )

        prior = baseline.get("sources_sha256")
        if isinstance(prior, str) and prior and prior != ingest_fingerprint.sha256:
            findings.append(
                DriftFinding(
                    kind="changed_sources",
                    severity="med",
                    details={
                        "baseline_path": str(bpath),
                        "previous_sources_sha256": prior,
                        "current_sources_sha256": ingest_fingerprint.sha256,
                        "keys_included": ingest_fingerprint.keys_included,
                    },
                )
            )

    if bpath is not None and intent_semantic_fingerprint is not None:
        prior_intent_sem = baseline.get("intent_semantic_fingerprint")
        if isinstance(prior_intent_sem, str) and prior_intent_sem and prior_intent_sem != intent_semantic_fingerprint:
            findings.append(
                DriftFinding(
                    kind="changed_intent",
                    severity="high",
                    details={
                        "baseline_path": str(bpath),
                        "previous_intent_semantic_fingerprint": prior_intent_sem,
                        "current_intent_semantic_fingerprint": intent_semantic_fingerprint,
                        "previous_intent_goal_text_fingerprint": baseline.get("intent_goal_text_fingerprint"),
                        "current_intent_goal_text_fingerprint": intent_goal_text_fingerprint,
                    },
                )
            )

    if bpath is not None and knowledge_semantic_fingerprint is not None:
        prior_knowledge_sem = baseline.get("knowledge_semantic_fingerprint")
        if (
            isinstance(prior_knowledge_sem, str)
            and prior_knowledge_sem
            and prior_knowledge_sem != knowledge_semantic_fingerprint
        ):
            findings.append(
                DriftFinding(
                    kind="changed_knowledge_semantic",
                    severity="high",
                    details={
                        "baseline_path": str(bpath),
                        "previous_knowledge_semantic_fingerprint": prior_knowledge_sem,
                        "current_knowledge_semantic_fingerprint": knowledge_semantic_fingerprint,
                    },
                )
            )

    if bpath is not None and knowledge_provenance_fingerprint is not None:
        prior_knowledge_prov = baseline.get("knowledge_provenance_fingerprint")
        if (
            isinstance(prior_knowledge_prov, str)
            and prior_knowledge_prov
            and prior_knowledge_prov != knowledge_provenance_fingerprint
        ):
            findings.append(
                DriftFinding(
                    kind="changed_knowledge_provenance",
                    severity="high",
                    details={
                        "baseline_path": str(bpath),
                        "previous_knowledge_provenance_fingerprint": prior_knowledge_prov,
                        "current_knowledge_provenance_fingerprint": knowledge_provenance_fingerprint,
                    },
                )
            )

    return DriftReport(scope=scope, findings=findings)


def extend_drift_report(report: DriftReport, extra_findings: Sequence[DriftFinding]) -> DriftReport:
    """Append findings (e.g. runtime-only drift) to an existing fingerprint-based drift report."""

    if not extra_findings:
        return report
    return DriftReport(scope=report.scope, findings=tuple(report.findings) + tuple(extra_findings))


def write_baseline(
    *,
    scope: TenantRepoScope,
    outputs_root: str | Path,
    ingest_fingerprint: IngestStateFingerprint | None,
    baseline_path: str | Path,
    intent_semantic_fingerprint: str | None = None,
    intent_goal_text_fingerprint: str | None = None,
    knowledge_semantic_fingerprint: str | None = None,
    knowledge_provenance_fingerprint: str | None = None,
) -> Path:
    """Write/update a baseline file used for `changed_sources` drift detection."""

    require_non_empty(scope.tenant_id, name="scope.tenant_id")
    require_non_empty(scope.repo_id, name="scope.repo_id")
    p = Path(baseline_path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)

    payload: dict[str, Any] = {
        "scope": {"tenant_id": scope.tenant_id, "repo_id": scope.repo_id},
    }
    if ingest_fingerprint is not None:
        payload["sources_sha256"] = ingest_fingerprint.sha256
        payload["sources"] = ingest_fingerprint.to_json_obj()
        # Store best-effort per-source fingerprints so we can compute changed
        # source sets (used by "living systems" safe recompile).
        try:
            state_raw = Path(ingest_fingerprint.state_path).expanduser().read_text(encoding="utf-8")
            loaded = json.loads(state_raw)
            if isinstance(loaded, dict):
                prefix = f"{scope.tenant_id}::"
                filtered: dict[str, Any] = {
                    k: v for k, v in loaded.items() if isinstance(k, str) and k.startswith(prefix)
                }
                payload["sources_by_key"] = filtered
        except Exception:
            # Baseline can still be written and drift detection still works via sources_sha256.
            pass

    if intent_semantic_fingerprint is not None:
        payload["intent_semantic_fingerprint"] = intent_semantic_fingerprint
    if intent_goal_text_fingerprint is not None:
        payload["intent_goal_text_fingerprint"] = intent_goal_text_fingerprint

    if knowledge_semantic_fingerprint is not None:
        payload["knowledge_semantic_fingerprint"] = knowledge_semantic_fingerprint
    if knowledge_provenance_fingerprint is not None:
        payload["knowledge_provenance_fingerprint"] = knowledge_provenance_fingerprint

    # Also record a manifest fingerprint as a cheap “contract” anchor.
    root = Path(outputs_root).expanduser().resolve()
    scoped = _scope_dir(root=root, scope=scope)
    manifest_path = scoped / "manifest.json"
    if manifest_path.exists():
        payload["manifest_sha256"] = stable_json_fingerprint(_read_json_object(manifest_path, what="manifest"))
        payload["manifest_path"] = str(manifest_path)

    tmp = p.with_suffix(p.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(p)
    return p


def write_drift_artifacts(
    *,
    scope: TenantRepoScope,
    outputs_root: str | Path,
    report: DriftReport,
    check_id: str,
    triggers: Sequence[Mapping[str, Any]] = (),
    checked_at_ms: int | None = None,
    baseline_path: str | Path | None = None,
    source: str = "drift_check",
) -> tuple[Path, Path]:
    """Persist structured living drift artifacts under the tenant-scoped root."""

    require_non_empty(check_id, name="check_id")
    checked_at = int(checked_at_ms if checked_at_ms is not None else time.time() * 1000)
    root = Path(outputs_root).expanduser().resolve()
    scoped = _scope_dir(root=root, scope=scope)
    living_dir = scoped / ".akc" / "living"
    living_dir.mkdir(parents=True, exist_ok=True)

    baseline_manifest_sha256: str | None = None
    baseline_str: str | None = None
    if baseline_path is not None:
        baseline_str = str(Path(baseline_path).expanduser())
        try:
            baseline_obj = _read_json_object(Path(baseline_path).expanduser(), what="baseline")
            raw_manifest_sha = baseline_obj.get("manifest_sha256")
            if isinstance(raw_manifest_sha, str) and len(raw_manifest_sha) == 64:
                baseline_manifest_sha256 = raw_manifest_sha
        except FileNotFoundError:
            baseline_manifest_sha256 = None

    drift_payload: dict[str, Any] = apply_schema_envelope(
        obj={
            **report.to_json_obj(),
            "checked_at_ms": checked_at,
            "baseline_path": baseline_str,
            "baseline_manifest_sha256": baseline_manifest_sha256,
        },
        kind="living_drift_report",
    )
    validate_artifact_json(obj=drift_payload, kind="living_drift_report")

    triggers_payload: dict[str, Any] = apply_schema_envelope(
        obj={
            "tenant_id": scope.tenant_id,
            "repo_id": scope.repo_id,
            "run_id": check_id,
            "check_id": check_id,
            "checked_at_ms": checked_at,
            "source": source,
            "triggers": [
                {
                    **dict(item),
                    **({"checked_at_ms": checked_at} if "checked_at_ms" not in item else {}),
                    **({"source": source} if "source" not in item else {}),
                }
                for item in triggers
                if isinstance(item, Mapping)
            ],
        },
        kind="recompile_triggers",
    )
    validate_artifact_json(obj=triggers_payload, kind="recompile_triggers")

    drift_path = living_dir / f"{check_id}.drift.json"
    triggers_path = living_dir / f"{check_id}.triggers.json"
    drift_path.write_text(
        json.dumps(drift_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    triggers_path.write_text(
        json.dumps(triggers_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    _update_manifest_living_metadata(
        scope=scope,
        outputs_root=root,
        check_id=check_id,
        checked_at_ms=checked_at,
        source=source,
        drift_path=drift_path,
        triggers_path=triggers_path,
    )
    return drift_path, triggers_path
