from __future__ import annotations

IR_SCHEMA_KIND = "akc_ir"
IR_SCHEMA_VERSION = 2
IR_FORMAT_VERSION = "2.0"
SUPPORTED_IR_VERSIONS: frozenset[tuple[int, str]] = frozenset(
    {
        (1, "1.0"),
        (2, "2.0"),
    }
)


def require_supported_ir_version(*, schema_version: int, format_version: str) -> None:
    version = (int(schema_version), str(format_version).strip())
    if version not in SUPPORTED_IR_VERSIONS:
        raise ValueError(
            "unsupported ir version: got "
            f"schema_version={schema_version}, format_version={format_version}; "
            f"supported={sorted(SUPPORTED_IR_VERSIONS)}"
        )
