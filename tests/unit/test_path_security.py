from __future__ import annotations

from pathlib import Path

import pytest

from akc.path_security import (
    coerce_safe_path_string,
    expanduser_resolve_trusted_invoker,
    resolve_absolute_path_under_allowlist_bases,
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
