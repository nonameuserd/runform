from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any


def _is_safe_plain_scalar(s: str) -> bool:
    if s == "":
        return False
    # YAML plain scalars have lots of edge cases; keep this conservative.
    forbidden = set(":\n\r\t#[]{}&*!|>'\"%`")
    if any(ch in forbidden for ch in s):
        return False
    if s.strip() != s:
        return False
    lower = s.lower()
    if lower in {"null", "true", "false", "~"}:
        return False
    if s[0] in "-?,":
        return False
    return True


def _yaml_quote(s: str) -> str:
    # Double-quoted scalar with minimal escapes.
    escaped = (
        s.replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
    )
    return f'"{escaped}"'


def _yaml_scalar(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, int):
        return str(v)
    if isinstance(v, float):
        # Keep deterministic and avoid scientific notation surprises for whole-ish floats.
        return str(v)
    if isinstance(v, str):
        return v if _is_safe_plain_scalar(v) else _yaml_quote(v)
    raise TypeError(f"unsupported YAML scalar type: {type(v).__name__}")


def dump_yaml(value: Any) -> str:
    """Deterministic YAML dump for a limited JSON-ish subset.

    Supported:
    - dict[str, Any] (keys must be str)
    - list/tuple
    - scalars: str, int, float, bool, None
    """

    lines: list[str] = []

    def emit(node: Any, indent: int) -> None:
        pad = " " * indent
        if isinstance(node, Mapping):
            for k in sorted(node.keys()):
                if not isinstance(k, str):
                    raise TypeError("YAML mapping keys must be str")
                v = node[k]
                key = k if _is_safe_plain_scalar(k) else _yaml_quote(k)
                if isinstance(v, (Mapping, Sequence)) and not isinstance(v, (str, bytes)):
                    lines.append(f"{pad}{key}:")
                    emit(v, indent + 2)
                else:
                    lines.append(f"{pad}{key}: {_yaml_scalar(v)}")
            return

        if isinstance(node, Sequence) and not isinstance(node, (str, bytes)):
            for item in node:
                if isinstance(item, (Mapping, Sequence)) and not isinstance(item, (str, bytes)):
                    lines.append(f"{pad}-")
                    emit(item, indent + 2)
                else:
                    lines.append(f"{pad}- {_yaml_scalar(item)}")
            return

        # Root scalar
        lines.append(f"{pad}{_yaml_scalar(node)}")

    emit(value, 0)
    return "\n".join(lines) + "\n"

