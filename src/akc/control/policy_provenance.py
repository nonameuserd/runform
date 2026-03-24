"""Record which Rego/OPA bundle governed a run (manifest control_plane metadata)."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

_ENV_KEYS: tuple[tuple[str, str], ...] = (
    ("policy_bundle_id", "AKC_POLICY_BUNDLE_ID"),
    ("policy_git_sha", "AKC_POLICY_GIT_SHA"),
    ("rego_pack_version", "AKC_REGO_PACK_VERSION"),
)


def _try_git_head_near(start: Path) -> str | None:
    cur = start.resolve()
    if cur.is_file():
        cur = cur.parent
    for _ in range(12):
        if (cur / ".git").exists():
            try:
                cp = subprocess.run(
                    ["git", "-C", str(cur), "rev-parse", "HEAD"],
                    capture_output=True,
                    text=True,
                    check=False,
                    timeout=8,
                )
                if int(cp.returncode) != 0:
                    return None
                h = (cp.stdout or "").strip()
                if len(h) >= 7:
                    return h
                return None
            except (OSError, subprocess.SubprocessError):
                return None
        parent = cur.parent
        if parent == cur:
            break
        cur = parent
    return None


def apply_env_policy_provenance(control_plane: dict[str, Any]) -> None:
    """Set provenance keys from environment when non-empty (overrides existing)."""

    if not isinstance(control_plane, dict):
        raise TypeError("control_plane must be a dict")
    for json_key, env_name in _ENV_KEYS:
        raw = str(os.environ.get(env_name, "") or "").strip()
        if raw:
            control_plane[json_key] = raw


def merge_policy_provenance_for_compile_control_plane(
    control_plane: dict[str, Any],
    *,
    opa_policy_path: str | None,
) -> None:
    """Stamp compile-time control_plane: env wins; optional git HEAD near ``opa_policy_path``."""

    apply_env_policy_provenance(control_plane)
    existing = str(control_plane.get("policy_git_sha", "") or "").strip()
    if existing:
        return
    p = str(opa_policy_path or "").strip()
    if not p:
        return
    g = _try_git_head_near(Path(p))
    if g:
        control_plane["policy_git_sha"] = g
