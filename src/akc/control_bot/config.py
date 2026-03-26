from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import jsonschema
from pydantic import BaseModel, ConfigDict, Field


class ControlBotConfigError(Exception):
    """Raised when the control-bot config is missing, invalid, or unsafe."""


def _schema_v1() -> dict[str, Any]:
    # Draft 2020-12 schema; kept inline to avoid packaging/data-file pitfalls.
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": "akc.control_bot_config.v1",
        "title": "AKC control-bot config v1",
        "type": "object",
        "additionalProperties": False,
        "required": ["schema", "server", "routing", "identity", "policy", "approval", "storage"],
        "properties": {
            "schema": {"const": "control_bot_config.v1"},
            "server": {
                "type": "object",
                "additionalProperties": False,
                "required": ["bind", "port"],
                "properties": {
                    "bind": {"type": "string", "minLength": 1},
                    "port": {"type": "integer", "minimum": 1, "maximum": 65535},
                    "queue_max": {"type": "integer", "minimum": 1, "maximum": 100000},
                    "worker_threads": {"type": "integer", "minimum": 1, "maximum": 64},
                },
            },
            "channels": {
                "type": "object",
                "additionalProperties": False,
                "required": [],
                "properties": {
                    "slack": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["enabled"],
                        "properties": {
                            "enabled": {"type": "boolean"},
                            "signing_secret": {"type": "string", "minLength": 1},
                        },
                        "allOf": [
                            {
                                "if": {"properties": {"enabled": {"const": True}}},
                                "then": {"required": ["signing_secret"]},
                            }
                        ],
                    },
                    "discord": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["enabled"],
                        "properties": {
                            "enabled": {"type": "boolean"},
                            "public_key": {"type": "string", "minLength": 1},
                        },
                        "allOf": [
                            {
                                "if": {"properties": {"enabled": {"const": True}}},
                                "then": {"required": ["public_key"]},
                            }
                        ],
                    },
                    "telegram": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["enabled"],
                        "properties": {
                            "enabled": {"type": "boolean"},
                            "secret_token": {"type": "string", "minLength": 1},
                            "bot_token": {"type": "string", "minLength": 1},
                            "api_base_url": {"type": "string", "minLength": 1},
                        },
                        "allOf": [
                            {
                                "if": {"properties": {"enabled": {"const": True}}},
                                "then": {"required": ["secret_token"]},
                            }
                        ],
                    },
                    "whatsapp": {
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["enabled"],
                        "properties": {
                            "enabled": {"type": "boolean"},
                            "verify_token": {"type": "string", "minLength": 1},
                            "app_secret": {"type": "string", "minLength": 1},
                            "access_token": {"type": "string", "minLength": 1},
                            "phone_number_id": {"type": "string", "minLength": 1},
                            "api_base_url": {"type": "string", "minLength": 1},
                            "api_version": {"type": "string", "minLength": 1},
                        },
                        "allOf": [
                            {
                                "if": {"properties": {"enabled": {"const": True}}},
                                "then": {"required": ["verify_token", "app_secret"]},
                            }
                        ],
                    },
                },
            },
            "routing": {
                "type": "object",
                "additionalProperties": False,
                "required": ["tenants", "workspaces"],
                "properties": {
                    "tenants": {
                        "type": "array",
                        "minItems": 1,
                        "items": {"type": "string", "minLength": 1},
                    },
                    "workspaces": {
                        "type": "array",
                        "minItems": 0,
                        "items": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["channel", "workspace_id", "tenant_id"],
                            "properties": {
                                "channel": {"type": "string", "enum": ["slack", "discord", "telegram", "whatsapp"]},
                                "workspace_id": {"type": "string", "minLength": 1},
                                "tenant_id": {"type": "string", "minLength": 1},
                            },
                        },
                    },
                },
            },
            "identity": {
                "type": "object",
                "additionalProperties": False,
                "required": ["principal_roles"],
                "properties": {
                    "principal_roles": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["tenant_id", "roles"],
                            "properties": {
                                "tenant_id": {"type": "string", "minLength": 1},
                                "roles": {
                                    "type": "array",
                                    "items": {"type": "string", "minLength": 1},
                                },
                            },
                        },
                    }
                },
            },
            "policy": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "mode": {"type": "string", "enum": ["audit_only", "enforce"]},
                    "opa": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "enabled": {"type": "boolean"},
                            "policy_path": {"type": "string", "minLength": 1},
                            "decision_path": {"type": "string", "minLength": 1},
                            "timeout_ms": {"type": "integer", "minimum": 100, "maximum": 30000},
                        },
                    },
                    "role_allowlist": {
                        "type": "object",
                        "additionalProperties": {
                            "type": "array",
                            "items": {"type": "string", "minLength": 1},
                        },
                    },
                },
            },
            "approval": {
                "type": "object",
                "additionalProperties": False,
                "properties": {
                    "default_ttl_ms": {"type": "integer", "minimum": 1000, "maximum": 604800000},
                    "requires_approval_action_prefixes": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                    },
                    "allow_self_approval": {"type": "boolean"},
                },
            },
            "storage": {
                "type": "object",
                "additionalProperties": False,
                "required": ["state_dir", "sqlite_path"],
                "properties": {
                    "state_dir": {"type": "string", "minLength": 1},
                    "sqlite_path": {"type": "string", "minLength": 1},
                    "audit_log_path": {"type": "string", "minLength": 1},
                },
            },
        },
    }


def validate_control_bot_config_dict(cfg: dict[str, Any]) -> None:
    """Validate config dict against the frozen v1 schema.

    Raises :class:`ControlBotConfigError` with a stable, user-facing message.
    """
    try:
        jsonschema.validate(instance=cfg, schema=_schema_v1())
    except jsonschema.ValidationError as e:
        path = ".".join(str(p) for p in e.absolute_path) if e.absolute_path else "<root>"
        raise ControlBotConfigError(f"invalid config at {path}: {e.message}") from e
    except Exception as e:  # pragma: no cover
        raise ControlBotConfigError(f"failed to validate config: {e}") from e


def load_control_bot_config_file(path: Path) -> dict[str, Any]:
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as e:
        raise ControlBotConfigError(f"failed to read config: {e}") from e
    try:
        data = json.loads(raw)
    except Exception as e:
        raise ControlBotConfigError(f"config is not valid JSON: {e}") from e
    if not isinstance(data, dict):
        raise ControlBotConfigError("config must be a JSON object")
    validate_control_bot_config_dict(data)
    return data


class PrincipalIdentity(BaseModel):
    tenant_id: str = Field(min_length=1)
    roles: tuple[str, ...] = ()


class ServerConfig(BaseModel):
    bind: str = Field(min_length=1)
    port: int = Field(ge=1, le=65535)
    queue_max: int = Field(default=2048, ge=1, le=100000)
    worker_threads: int = Field(default=2, ge=1, le=64)


class SlackChannelConfig(BaseModel):
    enabled: bool = False
    signing_secret: str | None = Field(default=None, min_length=1)


class DiscordChannelConfig(BaseModel):
    enabled: bool = False
    public_key: str | None = Field(default=None, min_length=1)


class TelegramChannelConfig(BaseModel):
    enabled: bool = False
    secret_token: str | None = Field(default=None, min_length=1)
    bot_token: str | None = Field(default=None, min_length=1)
    api_base_url: str = "https://api.telegram.org"


class WhatsAppChannelConfig(BaseModel):
    enabled: bool = False
    verify_token: str | None = Field(default=None, min_length=1)
    app_secret: str | None = Field(default=None, min_length=1)
    access_token: str | None = Field(default=None, min_length=1)
    phone_number_id: str | None = Field(default=None, min_length=1)
    api_base_url: str = "https://graph.facebook.com"
    api_version: str = "v19.0"


class RoutingConfig(BaseModel):
    tenants: tuple[str, ...] = Field(min_length=1)


class WorkspaceRoute(BaseModel):
    channel: Literal["slack", "discord", "telegram", "whatsapp"]
    workspace_id: str = Field(min_length=1)
    tenant_id: str = Field(min_length=1)


class RoutingConfigV1(RoutingConfig):
    workspaces: tuple[WorkspaceRoute, ...] = ()


class PolicyOPAConfig(BaseModel):
    enabled: bool = False
    # URL of OPA policy decision endpoint, typically `http(s)://<opa>/v1/data/<package>/<rule>`.
    policy_path: str | None = None
    decision_path: str = "data.akc.allow"
    timeout_ms: int = Field(default=1500, ge=100, le=30_000)


class PolicyConfig(BaseModel):
    mode: Literal["audit_only", "enforce"] = "enforce"
    opa: PolicyOPAConfig = Field(default_factory=PolicyOPAConfig)
    # Per-role allowlist of action patterns. Patterns support:
    # - exact action id: "status.runtime"
    # - prefix match: "status.*" (matches "status.<anything>")
    role_allowlist: dict[str, list[str]] = Field(default_factory=dict)


class ApprovalConfig(BaseModel):
    default_ttl_ms: int = Field(default=600_000, ge=1000, le=604_800_000)
    requires_approval_action_prefixes: tuple[str, ...] = ("incident.", "mutate.")
    allow_self_approval: bool = False


class StorageConfig(BaseModel):
    state_dir: str = Field(min_length=1)
    sqlite_path: str = Field(min_length=1)
    audit_log_path: str | None = Field(default=None, min_length=1)


class ChannelsConfig(BaseModel):
    slack: SlackChannelConfig = Field(default_factory=SlackChannelConfig)
    discord: DiscordChannelConfig = Field(default_factory=DiscordChannelConfig)
    telegram: TelegramChannelConfig = Field(default_factory=TelegramChannelConfig)
    whatsapp: WhatsAppChannelConfig = Field(default_factory=WhatsAppChannelConfig)


class ControlBotConfigV1(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    schema_id: Literal["control_bot_config.v1"] = Field(alias="schema")
    server: ServerConfig
    channels: ChannelsConfig = Field(default_factory=ChannelsConfig)
    routing: RoutingConfigV1
    identity: dict[str, dict[str, Any]] = Field(default_factory=dict)
    policy: PolicyConfig = Field(default_factory=PolicyConfig)
    approval: ApprovalConfig = Field(default_factory=ApprovalConfig)
    storage: StorageConfig

    def workspace_routes(self) -> tuple[WorkspaceRoute, ...]:
        # Cross-field validation: every route tenant must be in routing.tenants.
        allowed_tenants = {t.strip() for t in self.routing.tenants}
        for r in self.routing.workspaces:
            if r.tenant_id.strip() not in allowed_tenants:
                raise ControlBotConfigError(
                    f"routing.workspaces has tenant_id={r.tenant_id!r} not present in routing.tenants"
                )
        return self.routing.workspaces

    def principal_identities(self) -> dict[str, PrincipalIdentity]:
        raw = self.identity.get("principal_roles")
        if raw is None:
            raise ControlBotConfigError("identity.principal_roles is required")
        if not isinstance(raw, dict):
            raise ControlBotConfigError(
                "identity.principal_roles must be an object mapping principal_id -> {tenant_id, roles}"
            )
        out: dict[str, PrincipalIdentity] = {}
        for pid, val in raw.items():
            if not isinstance(pid, str) or not pid.strip():
                raise ControlBotConfigError("identity.principal_roles has an empty principal_id key")
            if not isinstance(val, dict):
                raise ControlBotConfigError(f"identity.principal_roles[{pid}] must be an object")
            try:
                ident = PrincipalIdentity.model_validate(val)
            except Exception as e:
                raise ControlBotConfigError(f"identity.principal_roles[{pid}] invalid: {e}") from e
            if ident.tenant_id.strip() not in {t.strip() for t in self.routing.tenants}:
                raise ControlBotConfigError(
                    f"identity.principal_roles[{pid}].tenant_id={ident.tenant_id!r} is not in routing.tenants"
                )
            out[pid.strip()] = ident
        if not out:
            raise ControlBotConfigError("identity.principal_roles must contain at least one principal")
        return out

    def _validate_enabled_channel_secrets(self) -> None:
        ch = self.channels
        if ch.slack.enabled and not (ch.slack.signing_secret and ch.slack.signing_secret.strip()):
            raise ControlBotConfigError("channels.slack.signing_secret is required when channels.slack.enabled=true")
        if ch.discord.enabled and not (ch.discord.public_key and ch.discord.public_key.strip()):
            raise ControlBotConfigError("channels.discord.public_key is required when channels.discord.enabled=true")
        if ch.telegram.enabled and not (ch.telegram.secret_token and ch.telegram.secret_token.strip()):
            raise ControlBotConfigError(
                "channels.telegram.secret_token is required when channels.telegram.enabled=true"
            )
        if ch.whatsapp.enabled:
            if not (ch.whatsapp.verify_token and ch.whatsapp.verify_token.strip()):
                raise ControlBotConfigError(
                    "channels.whatsapp.verify_token is required when channels.whatsapp.enabled=true"
                )
            if not (ch.whatsapp.app_secret and ch.whatsapp.app_secret.strip()):
                raise ControlBotConfigError(
                    "channels.whatsapp.app_secret is required when channels.whatsapp.enabled=true"
                )


@dataclass(frozen=True, slots=True)
class LoadedControlBotConfig:
    raw: dict[str, Any]
    model: ControlBotConfigV1
    path: Path


def load_control_bot_config(path: Path) -> LoadedControlBotConfig:
    p = path.expanduser().resolve()
    data = load_control_bot_config_file(p)
    try:
        model = ControlBotConfigV1.model_validate(data)
    except Exception as e:
        raise ControlBotConfigError(f"config failed typed validation: {e}") from e
    # cross-field validation: ensure principal identities resolve and tenant membership holds
    _ = model.principal_identities()
    _ = model.workspace_routes()
    model._validate_enabled_channel_secrets()
    return LoadedControlBotConfig(raw=data, model=model, path=p)
