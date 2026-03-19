from __future__ import annotations

import json
import shutil
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from akc.memory.models import require_non_empty

from .models import EvidenceRef, ViewerSnapshot
from .snapshot import ViewerError


@dataclass(frozen=True, slots=True)
class ExportResult:
    root: Path
    zip_path: Path | None = None
    copied_files: int = 0


def _ensure_under(*, root: Path, p: Path) -> None:
    root_r = root.resolve()
    p_r = p.resolve()
    try:
        p_r.relative_to(root_r)
    except ValueError as e:
        raise ViewerError("refused to write outside export root") from e


def _safe_copy(*, src_root: Path, relpath: str, dst_root: Path) -> int:
    # Copy src_root/relpath to dst_root/relpath with strict under-root checks.
    rel = Path(relpath)
    src = (src_root / rel).resolve()
    try:
        src.relative_to(src_root.resolve())
    except ValueError as e:
        raise ViewerError(f"refused to read outside scoped outputs dir: {relpath}") from e
    if not src.exists():
        return 0

    dst = (dst_root / rel).resolve()
    _ensure_under(root=dst_root, p=dst)
    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    return 1


def _iter_evidence(snapshot: ViewerSnapshot, *, include_all: bool) -> Iterable[EvidenceRef]:
    if include_all:
        return list(snapshot.evidence.all)
    # Otherwise only include per-step evidence (dedup by relpath).
    seen: set[str] = set()
    refs: list[EvidenceRef] = []
    for step_id, items in snapshot.evidence.by_step.items():
        _ = step_id
        for r in items:
            if r.relpath in seen:
                continue
            seen.add(r.relpath)
            refs.append(r)
    refs.sort(key=lambda r: r.relpath)
    return refs


def export_bundle(
    *,
    snapshot: ViewerSnapshot,
    out_dir: Path,
    include_all_evidence: bool = True,
    make_zip: bool = True,
) -> ExportResult:
    """Export a portable, local-first bundle for later inspection.

    The bundle contains:
    - data/plan.json (plan state)
    - data/manifest.json (when present)
    - files/<relpath> for each evidence artifact referenced by the manifest
    """

    out_dir = out_dir.expanduser()
    out_dir.mkdir(parents=True, exist_ok=True)
    out_dir = out_dir.resolve()

    data_dir = (out_dir / "data").resolve()
    _ensure_under(root=out_dir, p=data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)

    # Write plan snapshot as JSON (schema envelope is already present).
    (data_dir / "plan.json").write_text(
        json.dumps(snapshot.plan.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False)
        + "\n",
        encoding="utf-8",
    )

    if snapshot.manifest is not None:
        (data_dir / "manifest.json").write_text(
            json.dumps(dict(snapshot.manifest), indent=2, sort_keys=True, ensure_ascii=False)
            + "\n",
            encoding="utf-8",
        )

    files_dir = (out_dir / "files").resolve()
    _ensure_under(root=out_dir, p=files_dir)
    files_dir.mkdir(parents=True, exist_ok=True)

    copied = 0
    for r in _iter_evidence(snapshot, include_all=include_all_evidence):
        copied += _safe_copy(
            src_root=snapshot.scoped_outputs_dir, relpath=r.relpath, dst_root=files_dir
        )

    zip_path: Path | None = None
    if make_zip:
        require_non_empty(snapshot.inputs.tenant_id, name="tenant_id")
        require_non_empty(snapshot.inputs.repo_id, name="repo_id")
        zip_path = out_dir.with_suffix(".zip")
        with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
            for p in sorted(out_dir.rglob("*")):
                if p.is_dir():
                    continue
                arcname = p.relative_to(out_dir)
                zf.write(p, arcname=str(arcname))

    return ExportResult(root=out_dir, zip_path=zip_path, copied_files=copied)
