"""Deterministic, opt-in working-tree apply for strict unified-diff artifacts.

Fail-closed: invalid patches, suspicious paths, paths outside the scope root, or
missing ``patch(1)`` result in an explicit denial record and **no** filesystem mutation.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.compile.artifact_passes import ParsedPatchArtifact, parse_patch_artifact_strict
from akc.compile.verifier import patch_path_suspicious


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha256_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    return _sha256_bytes(path.read_bytes())


def _normalize_patch_text(patch_text: str) -> str:
    """Normalize patch text for consistent ``patch(1)`` behavior across platforms."""

    normalized = str(patch_text).replace("\r\n", "\n").replace("\r", "\n")
    if normalized and not normalized.endswith("\n"):
        normalized += "\n"
    return normalized


def _rel_path_confined(rel: str, scope_root: Path) -> tuple[bool, str | None]:
    root = scope_root.expanduser().resolve()
    if not root.is_absolute():
        return False, "scope_root_not_absolute"
    candidate = (root / rel).resolve()
    try:
        candidate.relative_to(root)
    except ValueError:
        return False, "path_escapes_scope_root"
    return True, None


def preflight_scoped_apply(
    *,
    patch_text: str,
    scope_root: Path,
) -> tuple[bool, str | None, ParsedPatchArtifact | None]:
    """Validate strict patch shape and path policy before any ``patch`` invocation."""

    parsed = parse_patch_artifact_strict(text=patch_text)
    if parsed is None:
        return False, "invalid_strict_patch_artifact", None
    sr = scope_root.expanduser().resolve()
    paths = sorted(set(parsed.touched_paths))
    for p in paths:
        if p.startswith(".akc/") or p == ".akc":
            return False, "patch_touches_internal_artifacts", None
        suspicious, reason = patch_path_suspicious(p)
        if suspicious:
            return False, f"suspicious_path:{reason}", None
        ok, err = _rel_path_confined(p, sr)
        if not ok:
            return False, err or "path_not_confined", None
    return True, None, parsed


def _patch_base_cmd(patch_bin: str) -> list[str]:
    return [patch_bin, "-p1", "--forward", "--batch"]


def _stage_patch_inputs(*, scope_root: Path, staged_root: Path, rel_paths: list[str]) -> None:
    for rel in rel_paths:
        src = scope_root / rel
        dst = staged_root / rel
        dst.parent.mkdir(parents=True, exist_ok=True)
        if src.is_file():
            shutil.copy2(src, dst)


def _run_patch(
    *,
    patch_bin: str,
    cwd: Path,
    patch_text: str,
) -> subprocess.CompletedProcess[bytes]:
    cmd = _patch_base_cmd(patch_bin)
    normalized_patch_text = _normalize_patch_text(patch_text)
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        input=normalized_patch_text.encode("utf-8"),
        capture_output=True,
        check=False,
    )


@dataclass(frozen=True, slots=True)
class ScopedApplyAccounting:
    """Structured compile accounting entry for :data:`compile_scoped_apply`."""

    compile_realization_mode: Literal["artifact_only", "scoped_apply"]
    attempted: bool
    applied: bool
    deny_reason: str | None
    reject_reason: str | None
    policy_blocked: bool
    scope_root: str | None
    patch_sha256: str
    patch_binary: str | None
    files: tuple[dict[str, Any], ...]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "compile_realization_mode": self.compile_realization_mode,
            "attempted": self.attempted,
            "applied": self.applied,
            "deny_reason": self.deny_reason,
            "reject_reason": self.reject_reason,
            "policy_blocked": self.policy_blocked,
            "scope_root": self.scope_root,
            "patch_sha256": self.patch_sha256,
            "patch_binary": self.patch_binary,
            "files": list(self.files),
        }


def run_scoped_apply_pipeline(
    *,
    compile_realization_mode: Literal["artifact_only", "scoped_apply"],
    apply_scope_root: str | None,
    patch_text: str,
    patch_sha256: str,
) -> dict[str, Any]:
    """Run preflight and optional ``patch(1)`` apply under ``apply_scope_root``.

    Tenant isolation: ``apply_scope_root`` must be an absolute path to the allowed
    working tree for this tenant/repo scope (typically compile ``work_root``).
    """

    if compile_realization_mode == "artifact_only":
        return ScopedApplyAccounting(
            compile_realization_mode="artifact_only",
            attempted=False,
            applied=False,
            deny_reason=None,
            reject_reason=None,
            policy_blocked=False,
            scope_root=None,
            patch_sha256=patch_sha256,
            patch_binary=None,
            files=(),
        ).to_json_obj()

    root_raw = str(apply_scope_root or "").strip()
    if not root_raw:
        return ScopedApplyAccounting(
            compile_realization_mode="scoped_apply",
            attempted=True,
            applied=False,
            deny_reason="apply_scope_root_unset",
            reject_reason=None,
            policy_blocked=True,
            scope_root=None,
            patch_sha256=patch_sha256,
            patch_binary=None,
            files=(),
        ).to_json_obj()

    scope_root = Path(root_raw).expanduser().resolve()
    normalized_patch_text = _normalize_patch_text(patch_text)
    patch_bin = shutil.which("patch")
    if patch_bin is None:
        return ScopedApplyAccounting(
            compile_realization_mode="scoped_apply",
            attempted=True,
            applied=False,
            deny_reason="patch_binary_unavailable",
            reject_reason=None,
            policy_blocked=False,
            scope_root=str(scope_root),
            patch_sha256=patch_sha256,
            patch_binary=None,
            files=(),
        ).to_json_obj()

    ok, reason, parsed = preflight_scoped_apply(patch_text=normalized_patch_text, scope_root=scope_root)
    if not ok or parsed is None:
        return ScopedApplyAccounting(
            compile_realization_mode="scoped_apply",
            attempted=True,
            applied=False,
            deny_reason=reason or "preflight_failed",
            reject_reason=reason,
            policy_blocked=False,
            scope_root=str(scope_root),
            patch_sha256=patch_sha256,
            patch_binary=patch_bin,
            files=(),
        ).to_json_obj()

    paths = sorted(set(parsed.touched_paths))
    before_rows: list[dict[str, Any]] = []
    for rel in paths:
        fp = scope_root / rel
        before_rows.append(
            {
                "path": rel,
                "sha256_before": _sha256_file(fp),
            }
        )

    with tempfile.TemporaryDirectory(prefix="akc-scoped-apply-") as tmp_dir:
        staged_root = Path(tmp_dir)
        _stage_patch_inputs(scope_root=scope_root, staged_root=staged_root, rel_paths=paths)
        check_res = _run_patch(patch_bin=patch_bin, cwd=staged_root, patch_text=normalized_patch_text)
    if int(check_res.returncode) != 0:
        stderr = (check_res.stderr or b"").decode("utf-8", errors="replace").strip()
        stdout = (check_res.stdout or b"").decode("utf-8", errors="replace").strip()
        msg = stderr or stdout or f"staged patch apply exit {check_res.returncode}"
        return ScopedApplyAccounting(
            compile_realization_mode="scoped_apply",
            attempted=True,
            applied=False,
            deny_reason="patch_check_failed",
            reject_reason=msg,
            policy_blocked=False,
            scope_root=str(scope_root),
            patch_sha256=patch_sha256,
            patch_binary=patch_bin,
            files=tuple(before_rows),
        ).to_json_obj()

    apply_res = _run_patch(patch_bin=patch_bin, cwd=scope_root, patch_text=normalized_patch_text)
    if int(apply_res.returncode) != 0:
        stderr = (apply_res.stderr or b"").decode("utf-8", errors="replace").strip()
        stdout = (apply_res.stdout or b"").decode("utf-8", errors="replace").strip()
        msg = stderr or stdout or f"patch apply exit {apply_res.returncode}"
        return ScopedApplyAccounting(
            compile_realization_mode="scoped_apply",
            attempted=True,
            applied=False,
            deny_reason="patch_apply_failed",
            reject_reason=msg,
            policy_blocked=False,
            scope_root=str(scope_root),
            patch_sha256=patch_sha256,
            patch_binary=patch_bin,
            files=tuple(before_rows),
        ).to_json_obj()

    file_out: list[dict[str, Any]] = []
    for row in before_rows:
        rel = str(row["path"])
        fp = scope_root / rel
        file_out.append(
            {
                "path": rel,
                "sha256_before": row.get("sha256_before"),
                "sha256_after": _sha256_file(fp),
            }
        )

    return ScopedApplyAccounting(
        compile_realization_mode="scoped_apply",
        attempted=True,
        applied=True,
        deny_reason=None,
        reject_reason=None,
        policy_blocked=False,
        scope_root=str(scope_root),
        patch_sha256=patch_sha256,
        patch_binary=patch_bin,
        files=tuple(file_out),
    ).to_json_obj()
