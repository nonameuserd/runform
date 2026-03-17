from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from akc.compile.interfaces import TenantRepoScope
from akc.memory.models import JSONValue, require_non_empty
from akc.outputs.fingerprints import IngestStateFingerprint, fingerprint_file_bytes, stable_json_fingerprint


DriftKind = Literal["changed_sources", "changed_outputs", "missing_manifest"]
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
) -> DriftReport:
    """Compute drift for `scope` under an outputs root.

    Drift categories implemented (Phase 4 minimal):
    - missing_manifest: no `manifest.json` present for the scope
    - changed_outputs: artifact file hash mismatch / missing artifacts
    - changed_sources: ingestion-state fingerprint differs from last recorded baseline (optional)
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
        findings.append(
            DriftFinding(
                kind="changed_outputs",
                severity="high",
                details={
                    "manifest_path": str(manifest_path),
                    "invalid_artifacts": invalid,
                    "missing_artifacts": missing,
                    "mismatched_artifacts": mismatched,
                },
            )
        )

    if ingest_fingerprint is not None and baseline_path is not None:
        bpath = Path(baseline_path).expanduser()
        try:
            baseline = _read_json_object(bpath, what="baseline")
        except FileNotFoundError:
            baseline = {}
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

    return DriftReport(scope=scope, findings=findings)


def write_baseline(
    *,
    scope: TenantRepoScope,
    outputs_root: str | Path,
    ingest_fingerprint: IngestStateFingerprint | None,
    baseline_path: str | Path,
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

