from __future__ import annotations

from pathlib import Path

from akc.memory.models import normalize_repo_id, require_non_empty
from akc.run.manifest import RunManifest


def load_run_manifest(
    *,
    path: str | Path,
    expected_tenant_id: str | None = None,
    expected_repo_id: str | None = None,
) -> RunManifest:
    manifest = RunManifest.from_json_file(path)
    if expected_tenant_id is not None and manifest.tenant_id != expected_tenant_id:
        raise ValueError("run_manifest tenant_id does not match expected scope")
    if expected_repo_id is not None and normalize_repo_id(manifest.repo_id) != normalize_repo_id(expected_repo_id):
        raise ValueError("run_manifest repo_id does not match expected scope")
    return manifest


def find_latest_run_manifest(
    *,
    outputs_root: str | Path,
    tenant_id: str,
    repo_id: str,
) -> Path | None:
    require_non_empty(tenant_id, name="tenant_id")
    require_non_empty(repo_id, name="repo_id")
    base = Path(outputs_root) / tenant_id / normalize_repo_id(repo_id) / ".akc" / "run"
    if not base.exists():
        return None
    files = sorted(base.glob("*.manifest.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None
