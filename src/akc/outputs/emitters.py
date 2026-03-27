from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, runtime_checkable

from akc.compile.interfaces import TenantRepoScope
from akc.memory.models import require_non_empty
from akc.outputs.models import OutputBundle
from akc.path_security import safe_resolve_path


def _scope_dir(*, root: Path, scope: TenantRepoScope) -> Path:
    require_non_empty(scope.tenant_id, name="scope.tenant_id")
    require_non_empty(scope.repo_id, name="scope.repo_id")
    return root / scope.tenant_id / scope.repo_id


def _ensure_under_root(*, root: Path, p: Path) -> None:
    root_r = root.resolve()
    p_r = p.resolve()
    try:
        p_r.relative_to(root_r)
    except ValueError as e:  # pragma: no cover
        raise ValueError("output path must be within emitter root") from e


@runtime_checkable
class Emitter(Protocol):
    """Persist output bundles somewhere (filesystem, object store, etc.)."""

    def emit(self, *, bundle: OutputBundle, root: str | Path) -> list[Path]:
        """Persist the bundle and return written paths."""


@dataclass(frozen=True, slots=True)
class FileSystemEmitter(Emitter):
    """Write artifacts to the local filesystem under `root/<tenant>/<repo>/...`."""

    create_parents: bool = True

    def emit(self, *, bundle: OutputBundle, root: str | Path) -> list[Path]:
        root_p = safe_resolve_path(root)
        if self.create_parents:
            root_p.mkdir(parents=True, exist_ok=True)

        out_dir = _scope_dir(root=root_p, scope=bundle.scope)
        out_dir.mkdir(parents=True, exist_ok=True)
        _ensure_under_root(root=root_p, p=out_dir)

        written: list[Path] = []
        for a in bundle.artifacts:
            fp = out_dir / a.path
            # Ensure the resolved path stays inside the scoped dir.
            _ensure_under_root(root=out_dir, p=fp)
            fp.parent.mkdir(parents=True, exist_ok=True)
            fp.write_bytes(a.content)
            written.append(fp)
        return written


@dataclass(frozen=True, slots=True)
class JsonManifestEmitter(Emitter):
    """Write a `manifest.json` for the bundle (plus artifacts via FileSystemEmitter)."""

    manifest_name: str = "manifest.json"
    artifacts: FileSystemEmitter = FileSystemEmitter()

    def emit(self, *, bundle: OutputBundle, root: str | Path) -> list[Path]:
        require_non_empty(self.manifest_name, name="manifest_name")
        root_p = safe_resolve_path(root)
        root_p.mkdir(parents=True, exist_ok=True)

        written = self.artifacts.emit(bundle=bundle, root=root_p)
        out_dir = _scope_dir(root=root_p, scope=bundle.scope)
        out_dir.mkdir(parents=True, exist_ok=True)
        _ensure_under_root(root=root_p, p=out_dir)

        mpath = out_dir / self.manifest_name
        _ensure_under_root(root=out_dir, p=mpath)
        mpath.write_text(json.dumps(bundle.to_manifest_obj(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        written.append(mpath)
        return written
