"""Deterministic, opt-in working-tree apply for strict unified-diff artifacts.

Fail-closed: invalid patches, suspicious paths, paths outside the scope root, or
missing ``patch(1)`` result in an explicit denial record and **no** filesystem mutation.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
import tempfile
from collections.abc import Mapping
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


def _normalize_mutation_prefixes(prefixes: tuple[str, ...] | list[str] | None) -> tuple[str, ...]:
    if not prefixes:
        return ()
    out: list[str] = []
    for raw in prefixes:
        s = str(raw).strip()
        if not s:
            continue
        s = s.replace("\\", "/")
        if s.startswith("/"):
            # Fail-closed: mutation allowlist must be relative prefixes.
            continue
        if s in (".", "./"):
            # Explicitly disallow "root" prefix.
            continue
        if not s.endswith("/"):
            s = s + "/"
        out.append(s)
    # Stable ordering for accounting/policy context.
    seen: set[str] = set()
    deduped: list[str] = []
    for p in out:
        if p in seen:
            continue
        seen.add(p)
        deduped.append(p)
    return tuple(deduped)


def _path_allowed_by_prefixes(rel: str, prefixes: tuple[str, ...]) -> bool:
    if not prefixes:
        return False
    r = str(rel).strip().replace("\\", "/")
    return any(r.startswith(p) for p in prefixes)


def preflight_scoped_apply(
    *,
    patch_text: str,
    scope_root: Path,
    mutation_paths: tuple[str, ...] | list[str] | None = None,
) -> tuple[bool, str | None, ParsedPatchArtifact | None]:
    """Validate strict patch shape and path policy before any ``patch`` invocation."""

    parsed = parse_patch_artifact_strict(text=patch_text)
    if parsed is None:
        return False, "invalid_strict_patch_artifact", None
    sr = scope_root.expanduser().resolve()
    paths = sorted(set(parsed.touched_paths))
    allow_prefixes = _normalize_mutation_prefixes(mutation_paths)
    if mutation_paths is not None and not allow_prefixes:
        # Explicitly configured allowlist that results in "allow nothing".
        return False, "mutation_paths_empty_or_invalid", None
    for p in paths:
        if p.startswith(".akc/") or p == ".akc":
            return False, "patch_touches_internal_artifacts", None
        suspicious, reason = patch_path_suspicious(p)
        if suspicious:
            return False, f"suspicious_path:{reason}", None
        ok, err = _rel_path_confined(p, sr)
        if not ok:
            return False, err or "path_not_confined", None
        if mutation_paths is not None and not _path_allowed_by_prefixes(p, allow_prefixes):
            return False, "path_not_in_mutation_allowlist", None
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
    mutation_paths: tuple[str, ...]
    rollback_snapshot_ref: dict[str, Any] | None
    git: dict[str, Any] | None
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
            "mutation_paths": list(self.mutation_paths),
            "rollback_snapshot_ref": (
                dict(self.rollback_snapshot_ref) if self.rollback_snapshot_ref is not None else None
            ),
            "git": dict(self.git) if self.git is not None else None,
            "files": list(self.files),
        }


def run_scoped_apply_pipeline(
    *,
    compile_realization_mode: Literal["artifact_only", "scoped_apply"],
    apply_scope_root: str | None,
    patch_text: str,
    patch_sha256: str,
    mutation_paths: tuple[str, ...] | list[str] | None = None,
    rollback_snapshots_enabled: bool = True,
    git_branch_per_run: bool = False,
    git_commit: bool = False,
    git_commit_message: str | None = None,
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
            mutation_paths=(),
            rollback_snapshot_ref=None,
            git=None,
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
            mutation_paths=tuple(_normalize_mutation_prefixes(mutation_paths)),
            rollback_snapshot_ref=None,
            git=None,
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
            mutation_paths=tuple(_normalize_mutation_prefixes(mutation_paths)),
            rollback_snapshot_ref=None,
            git=None,
            files=(),
        ).to_json_obj()

    ok, reason, parsed = preflight_scoped_apply(
        patch_text=normalized_patch_text,
        scope_root=scope_root,
        mutation_paths=mutation_paths,
    )
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
            mutation_paths=tuple(_normalize_mutation_prefixes(mutation_paths)),
            rollback_snapshot_ref=None,
            git=None,
            files=(),
        ).to_json_obj()

    paths = sorted(set(parsed.touched_paths))
    allow_prefixes = _normalize_mutation_prefixes(mutation_paths)
    before_rows: list[dict[str, Any]] = []
    for rel in paths:
        fp = scope_root / rel
        before_rows.append(
            {
                "path": rel,
                "sha256_before": _sha256_file(fp),
            }
        )

    rollback_ref: dict[str, Any] | None = None
    if rollback_snapshots_enabled:
        # Snapshot touched files under tenant/repo local `.akc/rollback/`.
        # This is not impacted by patch path policy because patches are denied from touching `.akc/`.
        snap_dir = scope_root / ".akc" / "rollback" / patch_sha256[:16]
        try:
            snap_dir.mkdir(parents=True, exist_ok=True)
            for row in before_rows:
                rel = str(row["path"])
                src = scope_root / rel
                dst = snap_dir / rel
                dst.parent.mkdir(parents=True, exist_ok=True)
                if src.is_file():
                    shutil.copy2(src, dst)
            meta_path = snap_dir / "snapshot.json"
            meta_path.write_text(
                str(
                    {
                        "patch_sha256": patch_sha256,
                        "mutation_paths": list(allow_prefixes),
                        "files": list(before_rows),
                    }
                ),
                encoding="utf-8",
            )
            rollback_ref = {
                "path": str(snap_dir.relative_to(scope_root)),
                "kind": "file_copy",
            }
        except Exception as exc:
            return ScopedApplyAccounting(
                compile_realization_mode="scoped_apply",
                attempted=True,
                applied=False,
                deny_reason="rollback_snapshot_failed",
                reject_reason=str(exc),
                policy_blocked=False,
                scope_root=str(scope_root),
                patch_sha256=patch_sha256,
                patch_binary=patch_bin,
                mutation_paths=tuple(allow_prefixes),
                rollback_snapshot_ref=None,
                git=None,
                files=tuple(before_rows),
            ).to_json_obj()

    git_ctx: dict[str, Any] | None = None
    if git_branch_per_run or git_commit:
        git_ctx = {"branch_per_run": bool(git_branch_per_run), "commit": bool(git_commit)}
        git_bin = shutil.which("git")
        if git_bin is None:
            return ScopedApplyAccounting(
                compile_realization_mode="scoped_apply",
                attempted=True,
                applied=False,
                deny_reason="git_binary_unavailable",
                reject_reason=None,
                policy_blocked=False,
                scope_root=str(scope_root),
                patch_sha256=patch_sha256,
                patch_binary=patch_bin,
                mutation_paths=tuple(allow_prefixes),
                rollback_snapshot_ref=rollback_ref,
                git=git_ctx,
                files=tuple(before_rows),
            ).to_json_obj()
        # Verify scope_root is a git repo (fail-closed when git options are requested).
        probe = subprocess.run(
            [git_bin, "-C", str(scope_root), "rev-parse", "--is-inside-work-tree"],
            capture_output=True,
            check=False,
        )
        if int(probe.returncode) != 0:
            return ScopedApplyAccounting(
                compile_realization_mode="scoped_apply",
                attempted=True,
                applied=False,
                deny_reason="git_not_a_repo",
                reject_reason=(probe.stderr or b"").decode("utf-8", errors="replace").strip() or None,
                policy_blocked=False,
                scope_root=str(scope_root),
                patch_sha256=patch_sha256,
                patch_binary=patch_bin,
                mutation_paths=tuple(allow_prefixes),
                rollback_snapshot_ref=rollback_ref,
                git=git_ctx,
                files=tuple(before_rows),
            ).to_json_obj()
        if git_branch_per_run:
            branch_name = f"akc/compile/{patch_sha256[:12]}"
            git_ctx["branch_name"] = branch_name
            chk = subprocess.run(
                [git_bin, "-C", str(scope_root), "checkout", "-b", branch_name],
                capture_output=True,
                check=False,
            )
            if int(chk.returncode) != 0:
                return ScopedApplyAccounting(
                    compile_realization_mode="scoped_apply",
                    attempted=True,
                    applied=False,
                    deny_reason="git_branch_create_failed",
                    reject_reason=(chk.stderr or b"").decode("utf-8", errors="replace").strip() or None,
                    policy_blocked=False,
                    scope_root=str(scope_root),
                    patch_sha256=patch_sha256,
                    patch_binary=patch_bin,
                    mutation_paths=tuple(allow_prefixes),
                    rollback_snapshot_ref=rollback_ref,
                    git=git_ctx,
                    files=tuple(before_rows),
                ).to_json_obj()

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
            mutation_paths=tuple(allow_prefixes),
            rollback_snapshot_ref=rollback_ref,
            git=git_ctx,
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
            mutation_paths=tuple(allow_prefixes),
            rollback_snapshot_ref=rollback_ref,
            git=git_ctx,
            files=tuple(before_rows),
        ).to_json_obj()

    if git_ctx is not None and git_commit:
        git_bin = shutil.which("git")
        if git_bin is not None:
            msg = str(git_commit_message).strip() if git_commit_message is not None else ""
            if not msg:
                msg = f"AKC scoped_apply {patch_sha256[:12]}"
            git_ctx["commit_message"] = msg
            # Best-effort: stage only touched paths (confined to allowlist by preflight).
            add_res = subprocess.run(
                [git_bin, "-C", str(scope_root), "add", "--"] + [str(p) for p in paths],
                capture_output=True,
                check=False,
            )
            if int(add_res.returncode) != 0:
                err_txt = (add_res.stderr or b"").decode("utf-8", errors="replace").strip()
                git_ctx["commit_error"] = err_txt or "git add failed"
            else:
                commit_res = subprocess.run(
                    [git_bin, "-C", str(scope_root), "commit", "-m", msg],
                    capture_output=True,
                    check=False,
                )
                if int(commit_res.returncode) != 0:
                    git_ctx["commit_error"] = (
                        (commit_res.stderr or b"").decode("utf-8", errors="replace").strip()
                        or (commit_res.stdout or b"").decode("utf-8", errors="replace").strip()
                        or "git commit failed"
                    )

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
        mutation_paths=tuple(allow_prefixes),
        rollback_snapshot_ref=rollback_ref,
        git=git_ctx,
        files=tuple(file_out),
    ).to_json_obj()


def git_scoped_apply_recovery_notes(
    *,
    scope_root: str,
    accounting_git: Mapping[str, Any] | None,
    rollback_snapshot_ref: Mapping[str, Any] | None,
) -> list[str]:
    """Suggest git-native rollback steps after a scoped_apply run (operator hints).

    Tenant-safe: only stringifies values already recorded under tenant/repo accounting;
    does not execute git or read the filesystem.
    """

    notes: list[str] = []
    g = dict(accounting_git) if accounting_git is not None else {}
    root = str(scope_root).strip()
    if not root:
        return notes
    if g.get("branch_per_run") and g.get("branch_name"):
        bn = str(g["branch_name"])
        notes.append(
            f"Leave topic branch: git -C {root!r} checkout - (or delete later: git -C {root!r} branch -D {bn})"
        )
    if g.get("commit") and not g.get("commit_error"):
        notes.append(f"Undo last commit (keep changes unstaged): git -C {root!r} reset --soft HEAD~1")
        notes.append(f"Hard undo last commit: git -C {root!r} reset --hard HEAD~1")
    snap = dict(rollback_snapshot_ref) if rollback_snapshot_ref is not None else {}
    rel = snap.get("path")
    if isinstance(rel, str) and rel.strip():
        notes.append(
            f"File-copy rollback snapshot under {rel!r} relative to repo root "
            "(restore files from snapshot dir if patch apply partially failed)."
        )
    return notes
