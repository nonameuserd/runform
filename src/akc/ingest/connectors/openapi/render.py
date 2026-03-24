from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any, Final

_SAFE_WS_RE: Final[re.Pattern[str]] = re.compile(r"[ \t]{2,}")


def render_endpoint_index(spec: Mapping[str, Any], ops: list[tuple[str, str, Mapping[str, Any]]]) -> str:
    title = None
    info = spec.get("info")
    if isinstance(info, dict):
        t = info.get("title")
        if isinstance(t, str) and t.strip():
            title = t.strip()
    lines: list[str] = []
    if title is not None:
        lines.append(f"OpenAPI: {title}")
    lines.append("Endpoints:")
    for path, method, op in ops:
        summary = op.get("summary")
        summary_s = summary.strip() if isinstance(summary, str) else ""
        operation_id = op.get("operationId")
        op_id_s = operation_id.strip() if isinstance(operation_id, str) else ""
        suffix = ""
        if op_id_s:
            suffix = f" ({op_id_s})"
        elif summary_s:
            suffix = f" — {summary_s}"
        lines.append(f"- {method.upper()} {path}{suffix}")
    return "\n".join(lines).strip()


def render_operation(
    *,
    path: str,
    method: str,
    op: Mapping[str, Any],
    servers: list[Any] | None = None,
    security: list[Any] | None = None,
    security_schemes: Mapping[str, Any] | None = None,
    max_chars: int,
) -> str:
    summary = op.get("summary")
    desc = op.get("description")
    tags = op.get("tags")
    operation_id = op.get("operationId")

    lines: list[str] = [f"{method} {path}"]
    rendered_servers = render_servers(servers)
    if rendered_servers:
        lines.append("servers:")
        lines.extend("  " + ln for ln in rendered_servers.splitlines())
    rendered_security = render_security(security, security_schemes=security_schemes)
    if rendered_security:
        lines.append("auth:")
        lines.extend("  " + ln for ln in rendered_security.splitlines())
    if isinstance(operation_id, str) and operation_id.strip():
        lines.append(f"operationId: {operation_id.strip()}")
    if isinstance(tags, list) and tags and all(isinstance(t, str) for t in tags):
        lines.append("tags: " + ", ".join(t.strip() for t in tags if t.strip()))
    if isinstance(summary, str) and summary.strip():
        lines.append("summary: " + normalize_ws(summary))
    if isinstance(desc, str) and desc.strip():
        lines.append("description: " + normalize_ws(desc))

    params = op.get("parameters")
    if isinstance(params, list) and params:
        rendered = render_parameters(params)
        if rendered:
            lines.append("")
            lines.append("parameters:")
            lines.extend("  " + ln for ln in rendered.splitlines())

    request_body = op.get("requestBody")
    if isinstance(request_body, dict):
        rb = render_request_body(request_body)
        if rb:
            lines.append("")
            lines.append("requestBody:")
            lines.extend("  " + ln for ln in rb.splitlines())

    responses = op.get("responses")
    if isinstance(responses, dict) and responses:
        rr = render_responses(responses)
        if rr:
            lines.append("")
            lines.append("responses:")
            lines.extend("  " + ln for ln in rr.splitlines())

    out = "\n".join(lines).strip()
    return truncate(out, max_chars=max_chars)


def render_component_schema(*, name: str, schema: Mapping[str, Any], max_chars: int) -> str:
    header = f"components.schemas.{name}"
    body = json_dump(schema)
    return truncate(header + "\n" + body, max_chars=max_chars)


def render_servers(servers: list[Any] | None) -> str:
    if not isinstance(servers, list) or not servers:
        return ""
    out: list[str] = []
    for s in servers:
        if not isinstance(s, dict):
            continue
        url = s.get("url")
        if not isinstance(url, str) or not url.strip():
            continue
        desc = s.get("description")
        desc_s = normalize_ws(desc) if isinstance(desc, str) and desc.strip() else ""
        out.append(f"- {url.strip()}{(' — ' + desc_s) if desc_s else ''}")
    return "\n".join(out).strip()


def render_security(security: list[Any] | None, *, security_schemes: Mapping[str, Any] | None) -> str:
    if not isinstance(security, list) or not security:
        return ""
    # Render requirements as a friendly list of scheme names (and types, if known).
    out: list[str] = []
    for req in security:
        if not isinstance(req, dict) or not req:
            continue
        parts: list[str] = []
        for name in req:
            if not isinstance(name, str) or not name.strip():
                continue
            typ_s = ""
            if isinstance(security_schemes, dict):
                scheme = security_schemes.get(name)
                if isinstance(scheme, dict):
                    typ = scheme.get("type")
                    if isinstance(typ, str) and typ.strip():
                        extra: list[str] = []
                        if typ == "http":
                            sch = scheme.get("scheme")
                            if isinstance(sch, str) and sch.strip():
                                extra.append(sch.strip())
                            bf = scheme.get("bearerFormat")
                            if isinstance(bf, str) and bf.strip():
                                extra.append(bf.strip())
                        if typ == "apiKey":
                            loc = scheme.get("in")
                            if isinstance(loc, str) and loc.strip():
                                extra.append(loc.strip())
                        typ_s = typ.strip() + (f" ({' '.join(extra)})" if extra else "")
            parts.append(f"{name}{(': ' + typ_s) if typ_s else ''}")
        if parts:
            out.append("- " + ", ".join(parts))
    return "\n".join(out).strip()


def render_parameters(params: list[Any]) -> str:
    out: list[str] = []
    for p in params:
        if not isinstance(p, dict):
            continue
        name = p.get("name")
        loc = p.get("in")
        required = p.get("required")
        if not isinstance(name, str) or not isinstance(loc, str):
            continue
        schema = p.get("schema")
        schema_s = ""
        if isinstance(schema, dict):
            schema_s = summarize_schema(schema)
        req_s = " required" if required is True else ""
        out.append(f"- {name} ({loc}){req_s}{(': ' + schema_s) if schema_s else ''}")
    return "\n".join(out).strip()


def render_request_body(rb: Mapping[str, Any]) -> str:
    out: list[str] = []
    required = rb.get("required")
    if required is True:
        out.append("required: true")
    content = rb.get("content")
    if isinstance(content, dict) and content:
        out.append("content:")
        for ctype, media in content.items():
            if not isinstance(ctype, str) or not isinstance(media, dict):
                continue
            schema = media.get("schema")
            schema_s = ""
            if isinstance(schema, dict):
                schema_s = summarize_schema(schema)
            out.append(f"  - {ctype}{(': ' + schema_s) if schema_s else ''}")
    return "\n".join(out).strip()


def render_responses(responses: Mapping[str, Any]) -> str:
    out: list[str] = []
    for code, resp in responses.items():
        if not isinstance(code, str) or not isinstance(resp, dict):
            continue
        desc = resp.get("description")
        desc_s = normalize_ws(desc) if isinstance(desc, str) and desc.strip() else ""
        out.append(f"- {code}{(': ' + desc_s) if desc_s else ''}")
        content = resp.get("content")
        if isinstance(content, dict) and content:
            for ctype, media in content.items():
                if not isinstance(ctype, str) or not isinstance(media, dict):
                    continue
                schema = media.get("schema")
                schema_s = ""
                if isinstance(schema, dict):
                    schema_s = summarize_schema(schema)
                out.append(f"  - {ctype}{(': ' + schema_s) if schema_s else ''}")
    return "\n".join(out).strip()


def summarize_schema(schema: Mapping[str, Any]) -> str:
    typ = schema.get("type")
    fmt = schema.get("format")
    ref = schema.get("$ref")
    parts: list[str] = []
    if isinstance(ref, str) and ref.strip():
        parts.append(ref.strip())
    if isinstance(typ, str) and typ.strip():
        parts.append(typ.strip())
    if isinstance(fmt, str) and fmt.strip():
        parts.append(fmt.strip())
    enum = schema.get("enum")
    if isinstance(enum, list) and enum:
        parts.append(f"enum[{len(enum)}]")
    return " ".join(parts).strip()


def normalize_ws(text: str) -> str:
    return _SAFE_WS_RE.sub(" ", text.strip())


def truncate(text: str, *, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    return text[: max(0, max_chars - 20)].rstrip() + "\n…[truncated]"


def json_dump(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True)
