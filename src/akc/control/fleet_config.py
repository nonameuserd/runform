"""Fleet control-plane configuration: multiple ``outputs_root`` shards, API tokens, webhooks."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from akc.memory.models import require_non_empty

FleetApiRole = Literal["viewer", "operator"]
FleetWebhookEvent = Literal["recompile_triggers", "living_drift", "operator_playbook_completed"]

_KNOWN_API_SCOPES: frozenset[str] = frozenset(
    {
        "runs:read",
        "runs:metadata:write",
        "runs:label",  # backward-compatible alias for metadata writes
        "audit:read",
        "governance:workflow:write",
    }
)


def _allowlist_tuple(raw: object | None) -> tuple[str, ...]:
    if raw is None:
        return ("*",)
    if isinstance(raw, list):
        parts = [str(x).strip() for x in raw if str(x).strip()]
        return tuple(parts) if parts else ("*",)
    return ("*",)


@dataclass(frozen=True, slots=True)
class FleetShardConfig:
    """One physical or logical artifact cell (an ``outputs_root`` tree)."""

    id: str
    outputs_root: Path
    tenant_allowlist: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FleetApiTokenConfig:
    id: str | None
    token: str
    role: FleetApiRole
    tenant_allowlist: tuple[str, ...]
    scopes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class FleetWebhookConfig:
    id: str
    url: str
    secret: str
    events: tuple[FleetWebhookEvent, ...]
    tenant_allowlist: tuple[str, ...]
    page_size: int


@dataclass(frozen=True, slots=True)
class FleetConfig:
    version: int
    shards: tuple[FleetShardConfig, ...]
    api_tokens: tuple[FleetApiTokenConfig, ...]
    allow_anonymous_read: bool
    webhooks: tuple[FleetWebhookConfig, ...]
    webhook_state_path: Path | None


def load_fleet_config(path: str | Path) -> FleetConfig:
    """Load fleet JSON; raises ``ValueError`` on missing required fields."""

    p = Path(path).expanduser().resolve()
    require_non_empty(str(p), name="fleet_config_path")
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except OSError as e:
        raise ValueError(f"cannot read fleet config: {p}") from e
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid JSON in fleet config: {p}") from e
    if not isinstance(raw, dict):
        raise ValueError("fleet config must be a JSON object")
    ver = raw.get("version", 1)
    if int(ver) != 1:
        raise ValueError(f"unsupported fleet config version: {ver!r} (only 1 supported)")

    shards_in = raw.get("shards")
    if not isinstance(shards_in, list) or not shards_in:
        raise ValueError("fleet config must include non-empty 'shards'")
    shards: list[FleetShardConfig] = []
    for s in shards_in:
        if not isinstance(s, dict):
            continue
        sid = str(s.get("id", "")).strip()
        root_s = str(s.get("outputs_root", "")).strip()
        if not sid or not root_s:
            raise ValueError("each shard requires non-empty 'id' and 'outputs_root'")
        shards.append(
            FleetShardConfig(
                id=sid,
                outputs_root=Path(root_s).expanduser().resolve(),
                tenant_allowlist=_allowlist_tuple(s.get("tenant_allowlist")),
            )
        )
    if not shards:
        raise ValueError("no valid shards in fleet config")

    tokens_out: list[FleetApiTokenConfig] = []
    tokens_in = raw.get("api_tokens")
    if isinstance(tokens_in, list):
        for t in tokens_in:
            if not isinstance(t, dict):
                continue
            tok = str(t.get("token", "")).strip()
            role_s = str(t.get("role", "")).strip()
            if not tok or role_s not in ("viewer", "operator"):
                raise ValueError("each api_tokens[] entry needs 'token' and role viewer|operator")
            scopes_raw = t.get("scopes")
            if scopes_raw is None:
                scopes_norm: tuple[str, ...] = (
                    ("runs:read", "runs:metadata:write") if role_s == "operator" else ("runs:read",)
                )
            else:
                if not isinstance(scopes_raw, list) or not scopes_raw:
                    raise ValueError("api_tokens[] scopes must be a non-empty array when set")
                seen: list[str] = []
                for s in scopes_raw:
                    ss = str(s).strip()
                    if not ss:
                        continue
                    if ss not in _KNOWN_API_SCOPES:
                        raise ValueError(f"unknown api token scope: {ss!r} (known: {sorted(_KNOWN_API_SCOPES)})")
                    if ss not in seen:
                        seen.append(ss)
                if not seen:
                    raise ValueError("api_tokens[] scopes must list at least one known scope")
                scopes_norm = tuple(seen)
            tokens_out.append(
                FleetApiTokenConfig(
                    id=str(t.get("id")).strip() if t.get("id") is not None else None,
                    token=tok,
                    role=role_s,  # type: ignore[arg-type]
                    tenant_allowlist=_allowlist_tuple(t.get("tenant_allowlist")),
                    scopes=scopes_norm,
                )
            )

    webhooks_out: list[FleetWebhookConfig] = []
    wh_in = raw.get("webhooks")
    if isinstance(wh_in, list):
        for w in wh_in:
            if not isinstance(w, dict):
                continue
            wid = str(w.get("id", "")).strip()
            url = str(w.get("url", "")).strip()
            secret = str(w.get("secret", "")).strip()
            evs = w.get("events")
            if not wid or not url or not secret:
                raise ValueError("each webhooks[] entry needs id, url, secret")
            if not isinstance(evs, list) or not evs:
                raise ValueError(f"webhook {wid!r} needs non-empty events[]")
            ev_norm: list[FleetWebhookEvent] = []
            for ev in evs:
                es = str(ev).strip()
                if es not in ("recompile_triggers", "living_drift", "operator_playbook_completed"):
                    raise ValueError(f"webhook {wid!r}: unknown event {ev!r}")
                ev_norm.append(es)  # type: ignore[arg-type]
            ps = w.get("page_size", 50)
            try:
                page_size = int(ps)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"webhook {wid!r}: invalid page_size") from exc
            page_size = max(1, min(page_size, 500))
            webhooks_out.append(
                FleetWebhookConfig(
                    id=wid,
                    url=url,
                    secret=secret,
                    events=tuple(ev_norm),
                    tenant_allowlist=_allowlist_tuple(w.get("tenant_allowlist")),
                    page_size=page_size,
                )
            )

    wh_state = raw.get("webhook_state_path")
    wh_path: Path | None
    if isinstance(wh_state, str) and wh_state.strip():
        wh_path = Path(wh_state).expanduser().resolve()
    else:
        wh_path = p.parent / (p.stem + ".webhook_state.json")

    return FleetConfig(
        version=1,
        shards=tuple(shards),
        api_tokens=tuple(tokens_out),
        allow_anonymous_read=bool(raw.get("allow_anonymous_read", False)),
        webhooks=tuple(webhooks_out),
        webhook_state_path=wh_path,
    )


def fleet_config_summary(cfg: FleetConfig) -> dict[str, Any]:
    """Non-secret summary for /health and debugging."""

    return {
        "version": cfg.version,
        "shard_count": len(cfg.shards),
        "shard_ids": [s.id for s in cfg.shards],
        "api_token_count": len(cfg.api_tokens),
        "webhook_count": len(cfg.webhooks),
        "allow_anonymous_read": cfg.allow_anonymous_read,
    }
