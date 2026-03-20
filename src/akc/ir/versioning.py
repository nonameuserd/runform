from __future__ import annotations

IR_SCHEMA_KIND = "akc_ir"
IR_SCHEMA_VERSION = 1
IR_FORMAT_VERSION = "1.0"


def require_supported_ir_version(*, schema_version: int, format_version: str) -> None:
    if int(schema_version) != IR_SCHEMA_VERSION:
        raise ValueError(
            f"unsupported ir schema_version={schema_version}; expected {IR_SCHEMA_VERSION}"
        )
    if str(format_version).strip() != IR_FORMAT_VERSION:
        raise ValueError(
            f"unsupported ir format_version={format_version}; expected {IR_FORMAT_VERSION}"
        )
