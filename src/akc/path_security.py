"""Path handling for externally influenced filesystem strings.

Webhook ``outputs_root`` values are confined by resolving ``(allowed_base / rel)``
after proving the requested path is rooted at ``allowed_base``, instead of calling
``Path(user_string).resolve()`` directly (CodeQL path-injection sink). CLI-style
paths reject embedded NUL bytes, then expand ``~`` and resolve at the trusted
caller boundary; higher-level checks still constrain baselines and tenant scope.
"""

from __future__ import annotations

from pathlib import Path


def coerce_safe_path_string(raw: str | Path) -> str:
    """Return stripped text or raise if the path string is unusable for resolution."""
    s = str(raw).strip()
    if "\x00" in s:
        raise ValueError("path must not contain NUL bytes")
    return s


def expanduser_resolve_trusted_invoker(raw: str | Path) -> Path:
    """Expand ``~`` and resolve paths at the CLI / in-process dispatch trust boundary.

    Baselines and tenant/repo directories are confined under ``outputs_root`` by
    :func:`akc.living.safe_recompile.safe_recompile_on_drift` after this step.
    """
    s = coerce_safe_path_string(raw)
    return Path(s).expanduser().resolve()  # codeql[py/path-injection]: Trusted invoker boundary; NUL rejected above.


def resolve_absolute_path_under_allowlist_bases(
    path_str: str,
    *,
    allowed_bases: tuple[Path, ...],
) -> Path | None:
    """If ``path_str`` is an absolute path under one of ``allowed_bases``, return its resolved path."""
    s = path_str.strip()
    if not s or s.startswith("~") or "\x00" in s:
        return None
    if not allowed_bases:
        return None
    try:
        candidate = Path(s)
        if not candidate.is_absolute():
            return None
    except ValueError:
        return None
    for base in allowed_bases:
        try:
            base_r = base.resolve()
        except OSError:
            continue
        try:
            rel = candidate.relative_to(base_r)
        except ValueError:
            continue
        if rel.is_absolute():
            continue
        # ``rel`` is a pure suffix of ``candidate`` with respect to ``base_r``; reject any
        # parent hops (should not occur from ``relative_to``, but keeps intent explicit).
        if any(p == ".." for p in rel.parts):
            continue
        try:
            resolved = (base_r / rel).resolve()  # codeql[py/path-injection]: allowlist only; verified below.
        except OSError:
            continue
        try:
            if resolved.is_relative_to(base_r):
                return resolved
        except ValueError:
            continue
    return None
