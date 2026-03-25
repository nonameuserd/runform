"""Configuration for MCP ingestion (multi-server JSON + resolution)."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse

from akc.ingest.exceptions import ConnectorError

_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def expand_env_refs(value: str, *, env: Mapping[str, str] | None = None) -> str:
    """Expand ``${VAR}`` segments using *env* (default: ``os.environ``)."""

    base = env or os.environ

    def repl(m: re.Match[str]) -> str:
        key = m.group(1)
        return base.get(key, "")

    return _ENV_REF.sub(repl, value)


def expand_env_mapping(
    data: Mapping[str, str] | None,
    *,
    env: Mapping[str, str] | None = None,
) -> dict[str, str] | None:
    if data is None:
        return None
    out: dict[str, str] = {}
    for k, v in data.items():
        out[str(k)] = expand_env_refs(str(v), env=env)
    return out


TransportName = Literal["stdio", "http"]


@dataclass(frozen=True, slots=True)
class ResolvedMcpServer:
    """Fully resolved MCP server definition for one named entry."""

    name: str
    transport: TransportName
    command: str | None = None
    args: tuple[str, ...] = ()
    env: dict[str, str] | None = None
    cwd: str | None = None
    url: str | None = None
    auth_header_env: str | None = None
    protocol_version: str | None = None


@dataclass(frozen=True, slots=True)
class McpIngestFileConfig:
    servers: dict[str, ResolvedMcpServer]
    default_server: str | None = None


def _as_str(obj: Any, *, field: str) -> str:
    if not isinstance(obj, str) or not obj.strip():
        raise ConnectorError(f"mcp config: {field} must be a non-empty string")
    return obj.strip()


def _as_str_list(obj: Any, *, field: str) -> tuple[str, ...]:
    if obj is None:
        return ()
    if not isinstance(obj, list) or not all(isinstance(x, str) for x in obj):
        raise ConnectorError(f"mcp config: {field} must be a list of strings")
    return tuple(str(x) for x in obj)


def _as_env_dict(obj: Any, *, field: str) -> dict[str, str] | None:
    if obj is None:
        return None
    if not isinstance(obj, dict):
        raise ConnectorError(f"mcp config: {field} must be an object of string keys/values")
    out: dict[str, str] = {}
    for k, v in obj.items():
        if not isinstance(k, str) or not isinstance(v, str):
            raise ConnectorError(f"mcp config: {field} keys/values must be strings")
        out[k] = v
    return out


def _parse_server_block(name: str, raw: Mapping[str, Any]) -> ResolvedMcpServer:
    transport_raw = raw.get("transport")
    if transport_raw not in ("stdio", "http"):
        raise ConnectorError(f"mcp config: server {name!r} needs transport 'stdio' or 'http'")
    transport = cast(TransportName, transport_raw)

    if transport == "stdio":
        cmd = _as_str(raw.get("command"), field=f"servers.{name}.command")
        args = _as_str_list(raw.get("args"), field=f"servers.{name}.args")
        env = expand_env_mapping(_as_env_dict(raw.get("env"), field=f"servers.{name}.env"))
        cwd_raw = raw.get("cwd")
        cwd = str(cwd_raw).strip() if isinstance(cwd_raw, str) and cwd_raw.strip() else None
        return ResolvedMcpServer(
            name=name,
            transport="stdio",
            command=cmd,
            args=args,
            env=env,
            cwd=cwd,
        )

    url = _as_str(raw.get("url"), field=f"servers.{name}.url")
    auth_raw = raw.get("auth_header_env")
    auth_header_env = str(auth_raw).strip() if isinstance(auth_raw, str) and auth_raw.strip() else None
    pv_raw = raw.get("protocol_version")
    protocol_version = str(pv_raw).strip() if isinstance(pv_raw, str) and pv_raw.strip() else None
    return ResolvedMcpServer(
        name=name,
        transport="http",
        url=url,
        auth_header_env=auth_header_env,
        protocol_version=protocol_version,
    )


def load_mcp_ingest_config(path: Path) -> McpIngestFileConfig:
    """Load ``.akc/mcp-ingest.json``-style configuration."""

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise ConnectorError(f"mcp config: cannot read {path}") from e
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ConnectorError(f"mcp config: invalid JSON in {path}") from e
    if not isinstance(data, dict):
        raise ConnectorError("mcp config: root must be a JSON object")

    servers_raw = data.get("servers")
    if not isinstance(servers_raw, dict) or not servers_raw:
        raise ConnectorError("mcp config: 'servers' must be a non-empty object")

    servers: dict[str, ResolvedMcpServer] = {}
    for key, block in servers_raw.items():
        if not isinstance(key, str) or not key.strip():
            raise ConnectorError("mcp config: server names must be non-empty strings")
        if not isinstance(block, dict):
            raise ConnectorError(f"mcp config: server {key!r} must be an object")
        servers[key.strip()] = _parse_server_block(key.strip(), block)

    default_raw = data.get("default_server")
    default_server = str(default_raw).strip() if isinstance(default_raw, str) and default_raw.strip() else None
    if default_server is not None and default_server not in servers:
        raise ConnectorError(f"mcp config: default_server {default_server!r} is not defined in servers")

    return McpIngestFileConfig(servers=servers, default_server=default_server)


def load_inline_server_config(path: Path) -> ResolvedMcpServer:
    """Load a one-off JSON file that describes a single server (no ``servers`` wrapper)."""

    try:
        text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise ConnectorError(f"mcp config: cannot read {path}") from e
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        raise ConnectorError(f"mcp config: invalid JSON in {path}") from e
    if not isinstance(data, dict):
        raise ConnectorError("mcp config: inline file must be a JSON object")
    name = path.stem or "inline"
    return _parse_server_block(name, data)


def validate_mcp_http_url(url: str) -> None:
    """Require HTTPS for remote hosts; allow HTTP only for loopback (MCP local guidance)."""

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ConnectorError(f"mcp http transport: unsupported URL scheme for {url!r}")
    host = (parsed.hostname or "").lower()
    loopback_hosts = {"localhost", "127.0.0.1", "::1", "0:0:0:0:0:0:0:1"}
    if host in loopback_hosts:
        return
    if parsed.scheme != "https":
        raise ConnectorError(
            "mcp http transport: HTTPS is required for non-loopback URLs "
            "(use http://127.0.0.1 or https:// for remote endpoints)."
        )


def resolve_mcp_server(
    *,
    input_value: str,
    config_path: Path,
) -> ResolvedMcpServer:
    """Resolve CLI ``--input`` to a :class:`ResolvedMcpServer`.

    * If *input_value* points to an existing ``.json`` file, it is treated as an inline server definition.
    * Otherwise *input_value* is a logical server name looked up in *config_path* (multi-server file).
    """

    candidate = Path(input_value).expanduser()
    if candidate.is_file() and candidate.suffix.lower() == ".json":
        server = load_inline_server_config(candidate)
        validate_transport_url(server)
        return server

    cfg = load_mcp_ingest_config(config_path)
    name = input_value.strip()
    if name not in cfg.servers:
        raise ConnectorError(
            f"mcp config: unknown server {name!r} in {config_path} (known: {', '.join(sorted(cfg.servers.keys()))})"
        )
    server = cfg.servers[name]
    validate_transport_url(server)
    return server


def validate_transport_url(server: ResolvedMcpServer) -> None:
    if server.transport == "http" and server.url is not None:
        validate_mcp_http_url(server.url)
