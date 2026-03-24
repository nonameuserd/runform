"""Paged webhook delivery for recompile triggers (operations index) and living drift artifacts."""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.control.fleet_catalog import fleet_list_runs_merged, shard_accepts_tenant
from akc.control.fleet_config import FleetConfig, FleetShardConfig, FleetWebhookConfig
from akc.memory.models import normalize_repo_id

logger = logging.getLogger(__name__)


def _webhook_tenant_allowed(wh: FleetWebhookConfig, tenant_id: str) -> bool:
    t = tenant_id.strip()
    if "*" in wh.tenant_allowlist:
        return True
    return t in wh.tenant_allowlist


def list_living_json_touchpoints(*, shard: FleetShardConfig, tenant_id: str) -> list[dict[str, Any]]:
    """Enumerate ``.akc/living/*.json`` under a tenant slice (portable drift / trigger file signals)."""

    out: list[dict[str, Any]] = []
    root = shard.outputs_root
    tdir = root / tenant_id.strip()
    if not tdir.is_dir():
        return out
    for repo_dir in sorted(tdir.iterdir()):
        if not repo_dir.is_dir():
            continue
        living = repo_dir / ".akc" / "living"
        if not living.is_dir():
            continue
        repo_norm = normalize_repo_id(repo_dir.name)
        for f in sorted(living.glob("*.json")):
            try:
                st = f.stat()
            except OSError:
                continue
            rel = f.relative_to(repo_dir)
            out.append(
                {
                    "shard_id": shard.id,
                    "outputs_root": str(root),
                    "tenant_id": tenant_id.strip(),
                    "repo_id": repo_norm,
                    "rel_path": str(rel).replace("\\", "/"),
                    "mtime_ms": int(st.st_mtime * 1000),
                    "size_bytes": int(st.st_size),
                }
            )
    return out


def _load_state(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"version": 1, "watermarks": {}}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"version": 1, "watermarks": {}}
    if not isinstance(raw, dict):
        return {"version": 1, "watermarks": {}}
    wm = raw.get("watermarks")
    if not isinstance(wm, dict):
        raw["watermarks"] = {}
    return raw


def _save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sign_webhook_body(*, secret: str, body: bytes) -> str:
    """Return hex HMAC-SHA256 for ``X-AKC-Signature: v1=<hex>``."""

    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    return mac.hexdigest()


@dataclass(frozen=True, slots=True)
class WebhookDeliveryResult:
    webhook_id: str
    event: str
    http_status: int | None
    item_count: int
    error: str | None


def deliver_fleet_webhooks(
    cfg: FleetConfig,
    *,
    tenants: list[str] | None = None,
    dry_run: bool = False,
    timeout_s: float = 30.0,
) -> list[WebhookDeliveryResult]:
    """POST one page per (webhook, event) pair when new rows exceed stored watermarks.

    When ``tenants`` is None, every tenant that appears on any shard directory is considered
    (best-effort scan of ``<outputs_root>/<tenant_id>/``).
    """

    state_path = cfg.webhook_state_path
    if state_path is None:
        raise ValueError("fleet config has no webhook_state_path")
    state = _load_state(state_path)
    watermarks: dict[str, Any] = state.setdefault("watermarks", {})
    if not isinstance(watermarks, dict):
        watermarks = {}
        state["watermarks"] = watermarks

    tenant_set: set[str] = {t.strip() for t in tenants if str(t).strip()} if tenants else set()
    if not tenant_set:
        for sh in cfg.shards:
            root = sh.outputs_root
            if not root.is_dir():
                continue
            for child in root.iterdir():
                if child.is_dir() and not child.name.startswith("."):
                    tenant_set.add(child.name)

    results: list[WebhookDeliveryResult] = []

    for wh in cfg.webhooks:
        wm_entry: dict[str, Any]
        raw_wm = watermarks.get(wh.id)
        if isinstance(raw_wm, dict):
            wm_entry = raw_wm
        else:
            wm_entry = {}
            watermarks[wh.id] = wm_entry

        for event in wh.events:
            since_ms = 0
            raw_since = wm_entry.get(event)
            if isinstance(raw_since, (int, float)):
                since_ms = int(raw_since)

            if event == "recompile_triggers":
                items = []
                remaining = wh.page_size
                for tenant_id in sorted(tenant_set):
                    if remaining <= 0:
                        break
                    if not _webhook_tenant_allowed(wh, tenant_id):
                        continue
                    batch = fleet_list_runs_merged(
                        cfg.shards,
                        tenant_id=tenant_id,
                        has_recompile_triggers=True,
                        since_ms=since_ms + 1 if since_ms else None,
                        limit=remaining,
                    )
                    items.extend(batch)
                    remaining = wh.page_size - len(items)

                def _run_sort_key(r: dict[str, Any]) -> tuple[int, str, str]:
                    raw = r.get("updated_at_ms", 0)
                    try:
                        ms = int(raw)
                    except (TypeError, ValueError):
                        ms = 0
                    return (-ms, str(r.get("shard_id")), str(r.get("repo_id")))

                items.sort(key=_run_sort_key)
                items = items[: wh.page_size]
                max_ms = since_ms
                for it in items:
                    max_ms = max(max_ms, int(it.get("updated_at_ms", 0)))
            elif event == "living_drift":
                items = []
                for tenant_id in sorted(tenant_set):
                    if not _webhook_tenant_allowed(wh, tenant_id):
                        continue
                    for sh in cfg.shards:
                        if not shard_accepts_tenant(sh, tenant_id):
                            continue
                        for row in list_living_json_touchpoints(shard=sh, tenant_id=tenant_id):
                            if int(row["mtime_ms"]) > since_ms:
                                items.append(row)
                items.sort(key=lambda r: -int(r["mtime_ms"]))
                items = items[: wh.page_size]
                max_ms = since_ms
                for it in items:
                    max_ms = max(max_ms, int(it["mtime_ms"]))
            else:
                continue

            payload = {
                "schema": "akc.fleet.webhook_delivery.v1",
                "webhook_id": wh.id,
                "event": event,
                "items": items,
                "generated_at_ms": int(time.time() * 1000),
            }
            body = json.dumps(payload, sort_keys=True).encode("utf-8")
            sig = sign_webhook_body(secret=wh.secret, body=body)

            if dry_run:
                results.append(
                    WebhookDeliveryResult(
                        webhook_id=wh.id,
                        event=event,
                        http_status=None,
                        item_count=len(items),
                        error=None,
                    )
                )
                continue

            if not items:
                results.append(
                    WebhookDeliveryResult(
                        webhook_id=wh.id,
                        event=event,
                        http_status=None,
                        item_count=0,
                        error=None,
                    )
                )
                continue

            req = urllib.request.Request(
                wh.url,
                data=body,
                method="POST",
                headers={
                    "Content-Type": "application/json; charset=utf-8",
                    "X-AKC-Webhook-Id": wh.id,
                    "X-AKC-Webhook-Event": event,
                    "X-AKC-Signature": f"v1={sig}",
                },
            )
            try:
                with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                    status = getattr(resp, "status", None) or resp.getcode()
            except urllib.error.HTTPError as e:
                status = e.code
                results.append(
                    WebhookDeliveryResult(
                        webhook_id=wh.id,
                        event=event,
                        http_status=int(status),
                        item_count=len(items),
                        error=str(e),
                    )
                )
                continue
            except urllib.error.URLError as e:
                results.append(
                    WebhookDeliveryResult(
                        webhook_id=wh.id,
                        event=event,
                        http_status=None,
                        item_count=len(items),
                        error=str(e),
                    )
                )
                continue

            results.append(
                WebhookDeliveryResult(
                    webhook_id=wh.id,
                    event=event,
                    http_status=int(status) if status is not None else None,
                    item_count=len(items),
                    error=None,
                )
            )
            wm_entry[event] = max_ms
            _save_state(state_path, state)

    return results


def post_signed_fleet_webhook(
    *,
    url: str,
    secret: str,
    webhook_id: str,
    event: str,
    items: list[dict[str, Any]],
    dry_run: bool = False,
    timeout_s: float = 30.0,
) -> WebhookDeliveryResult:
    """POST a single signed JSON payload (same envelope as :func:`deliver_fleet_webhooks`)."""

    payload = {
        "schema": "akc.fleet.webhook_delivery.v1",
        "webhook_id": webhook_id,
        "event": event,
        "items": items,
        "generated_at_ms": int(time.time() * 1000),
    }
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    sig = sign_webhook_body(secret=secret, body=body)

    if dry_run:
        return WebhookDeliveryResult(
            webhook_id=webhook_id,
            event=event,
            http_status=None,
            item_count=len(items),
            error=None,
        )

    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "X-AKC-Webhook-Id": webhook_id,
            "X-AKC-Webhook-Event": event,
            "X-AKC-Signature": f"v1={sig}",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            status = getattr(resp, "status", None) or resp.getcode()
    except urllib.error.HTTPError as e:
        return WebhookDeliveryResult(
            webhook_id=webhook_id,
            event=event,
            http_status=int(e.code),
            item_count=len(items),
            error=str(e),
        )
    except urllib.error.URLError as e:
        return WebhookDeliveryResult(
            webhook_id=webhook_id,
            event=event,
            http_status=None,
            item_count=len(items),
            error=str(e),
        )

    return WebhookDeliveryResult(
        webhook_id=webhook_id,
        event=event,
        http_status=int(status) if status is not None else None,
        item_count=len(items),
        error=None,
    )


def deliver_operator_playbook_completed_webhooks(
    cfg: FleetConfig,
    *,
    tenant_id: str,
    item: dict[str, Any],
    dry_run: bool = False,
    timeout_s: float = 30.0,
) -> list[WebhookDeliveryResult]:
    """Notify subscribers configured for ``operator_playbook_completed`` (no watermark state)."""

    tid = tenant_id.strip()
    results: list[WebhookDeliveryResult] = []
    for wh in cfg.webhooks:
        if "operator_playbook_completed" not in wh.events:
            continue
        if not _webhook_tenant_allowed(wh, tid):
            continue
        results.append(
            post_signed_fleet_webhook(
                url=wh.url,
                secret=wh.secret,
                webhook_id=wh.id,
                event="operator_playbook_completed",
                items=[item],
                dry_run=dry_run,
                timeout_s=timeout_s,
            )
        )
    return results
