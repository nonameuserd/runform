from __future__ import annotations

from pathlib import Path

import pytest

from akc.path_security import (
    coerce_safe_path_string,
    expanduser_resolve_trusted_invoker,
    require_path_under_resolved_root,
    resolve_absolute_path_under_allowlist_bases,
    sanitize_artifact_token,
)


def test_coerce_safe_path_string_rejects_nul() -> None:
    with pytest.raises(ValueError, match="NUL"):
        coerce_safe_path_string("/tmp/a\x00b")


def test_resolve_absolute_path_under_allowlist_bases(tmp_path: Path) -> None:
    base = (tmp_path / "allowed").resolve()
    base.mkdir()
    sub = base / "t" / "r"
    sub.mkdir(parents=True)
    bases = (base,)
    got = resolve_absolute_path_under_allowlist_bases(str(sub), allowed_bases=bases)
    assert got == sub.resolve()


def test_expanduser_resolve_trusted_invoker(tmp_path: Path) -> None:
    p = tmp_path / "x"
    p.mkdir()
    assert expanduser_resolve_trusted_invoker(str(p)) == p.resolve()


def test_sanitize_artifact_token_allowlisted() -> None:
    assert sanitize_artifact_token("step-1.a", label="x") == "step-1.a"


def test_sanitize_artifact_token_rejects_path_shape() -> None:
    bad = sanitize_artifact_token("../evil", label="step_id")
    assert bad.startswith("step_id_")
    assert "/" not in bad
    assert ".." not in bad


def test_require_path_under_resolved_root_ok(tmp_path: Path) -> None:
    base = tmp_path / "out"
    base.mkdir()
    child = base / "a" / "b.txt"
    got = require_path_under_resolved_root(child, root=base)
    assert got == child.resolve()


def test_require_path_under_resolved_root_rejects_escape(tmp_path: Path) -> None:
    base = tmp_path / "out"
    base.mkdir()
    outside = tmp_path / "other" / "x.txt"
    outside.parent.mkdir()
    with pytest.raises(ValueError, match="escapes"):
        require_path_under_resolved_root(outside, root=base)
