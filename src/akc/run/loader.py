from __future__ import annotations

from pathlib import Path

from akc.memory.models import normalize_repo_id, normalize_tenant_id
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
    try:
        root = Path(outputs_root).expanduser().resolve()
    except OSError:
        return None
    tenant_seg = normalize_tenant_id(tenant_id)
    repo_seg = normalize_repo_id(repo_id)
    base = root / tenant_seg / repo_seg / ".akc" / "run"
    try:
        base_resolved = base.resolve()
    except OSError:
        return None
    if not base_resolved.is_relative_to(root):
        return None
    if not base_resolved.exists():
        return None
    files = sorted(base_resolved.glob("*.manifest.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    return files[0] if files else None
