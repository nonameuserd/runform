from __future__ import annotations

import contextlib
import html
import json
from dataclasses import dataclass
from importlib.resources import files
from importlib.resources.abc import Traversable
from pathlib import Path
from typing import Final

from akc.knowledge.observability import build_knowledge_observation_payload

from .export import _safe_copy
from .models import ViewerSnapshot
from .snapshot import ViewerError

VIEWER_UI_VERSION: Final[str] = "1"


@dataclass(frozen=True, slots=True)
class WebBuildResult:
    root: Path
    index_html: Path
    copied_files: int


def _viewer_static_files() -> Traversable:
    return files("akc.viewer.static")


def _write_packaged_static(*, dst_static: Path) -> None:
    dst_static.mkdir(parents=True, exist_ok=True)
    root = _viewer_static_files()
    for name in ("viewer.css", "viewer.js"):
        asset = root.joinpath(name)
        if not asset.is_file():
            raise ViewerError(f"missing packaged viewer asset: {name!r}")
        (dst_static / name).write_bytes(asset.read_bytes())


def _render_index_html() -> str:
    tpl = _viewer_static_files().joinpath("index.html")
    if not tpl.is_file():
        raise ViewerError("missing packaged viewer template: index.html")
    raw = tpl.read_text(encoding="utf-8")
    return raw.replace(
        "__VIEWER_UI_VERSION__",
        html.escape(VIEWER_UI_VERSION, quote=True),
    )


def build_static_viewer(*, snapshot: ViewerSnapshot, out_dir: Path) -> WebBuildResult:
    """Build a local, static HTML viewer bundle (read-only)."""

    out_dir = out_dir.expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    data_dir = (out_dir / "data").resolve()
    data_dir.mkdir(parents=True, exist_ok=True)
    files_dir = (out_dir / "files").resolve()
    files_dir.mkdir(parents=True, exist_ok=True)
    static_dir = (out_dir / "static").resolve()

    (data_dir / "plan.json").write_text(
        json.dumps(snapshot.plan.to_json_obj(), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    if snapshot.manifest is not None:
        (data_dir / "manifest.json").write_text(
            json.dumps(dict(snapshot.manifest), indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    else:
        # Keep fetch() failure predictable: no manifest file.
        with contextlib.suppress(FileNotFoundError):
            (data_dir / "manifest.json").unlink()

    kobs_obj = build_knowledge_observation_payload(
        knowledge_envelope=snapshot.knowledge_envelope,
        conflict_reports=snapshot.conflict_reports,
        knowledge_mediation_envelope=snapshot.knowledge_mediation_envelope,
    )
    (data_dir / "knowledge_obs.json").write_text(
        json.dumps(kobs_obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    panels_obj = snapshot.operator_panels
    if panels_obj is None:
        panels_obj = {"forensics": None, "playbook": None, "profile_panel": None, "autopilot": None}
    (data_dir / "operator_panels.json").write_text(
        json.dumps(panels_obj, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    # Copy all referenced artifact files for local opening/downloading.
    copied = 0
    if snapshot.manifest is not None:
        for a in snapshot.manifest.get("artifacts") or []:
            relpath = str(a.get("path") or "").strip()
            if not relpath:
                continue
            copied += _safe_copy(src_root=snapshot.scoped_outputs_dir, relpath=relpath, dst_root=files_dir)
    for rel in (
        ".akc/knowledge/snapshot.json",
        ".akc/knowledge/snapshot.fingerprint.json",
        ".akc/knowledge/mediation.json",
    ):
        copied += _safe_copy(src_root=snapshot.scoped_outputs_dir, relpath=rel, dst_root=files_dir)

    _write_packaged_static(dst_static=static_dir)

    # Write HTML last for atomic-ish success.
    index = (out_dir / "index.html").resolve()
    try:
        index.write_text(_render_index_html(), encoding="utf-8")
    except OSError as e:  # pragma: no cover
        raise ViewerError(f"failed to write static viewer: {index}") from e

    return WebBuildResult(root=out_dir, index_html=index, copied_files=copied)
