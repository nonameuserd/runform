"""Optional HTTP receiver for fleet-style signed webhooks → one-shot ``living_recompile_execute``.

Fleet ``deliver_fleet_webhooks`` POSTs JSON with ``schema: akc.fleet.webhook_delivery.v1`` and
``X-AKC-Signature: v1=<hmac-sha256-hex>`` over the raw body. This server verifies the signature
and runs the same safe recompile path as ``akc living-recompile`` for each scoped item.

Tenant isolation: only items whose ``tenant_id`` matches ``tenant_allowlist`` (``*`` = any) are processed.
Path safety: ``outputs_root`` in the signed payload is accepted only when it resolves to an absolute path
under :attr:`LivingWebhookServerConfig.outputs_root_allowlist` (no ``~`` expansion on webhook-supplied strings).
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Mapping
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from akc.compile.interfaces import LLMBackend
from akc.control.fleet_webhooks import sign_webhook_body
from akc.living.automation_profile import LivingAutomationProfile
from akc.living.dispatch import living_recompile_execute
from akc.memory.models import normalize_repo_id
from akc.path_security import resolve_absolute_path_under_allowlist_bases

logger = logging.getLogger(__name__)

_MAX_BODY_BYTES = 512 * 1024
_SCHEMA = "akc.fleet.webhook_delivery.v1"
_SUPPORTED_EVENTS = frozenset({"recompile_triggers", "living_drift"})


@dataclass(frozen=True, slots=True)
class LivingWebhookServerConfig:
    """Process-wide settings for :func:`run_living_webhook_server`.

    ``outputs_root_allowlist`` restricts payload ``outputs_root`` values to resolved paths under
    those operator-configured directories (see module docstring).
    """

    bind_host: str
    port: int
    secret: str
    ingest_state_path: Path
    tenant_allowlist: frozenset[str]
    outputs_root_allowlist: frozenset[Path]
    living_automation_profile: LivingAutomationProfile
    opa_policy_path: str | None
    opa_decision_path: str
    llm_backend: LLMBackend | None
    # Recompile options (mirror ``akc living-recompile`` defaults unless overridden via env/project).
    eval_suite_path: Path
    goal: str
    policy_mode: str
    canary_mode: str
    accept_mode: str
    canary_test_mode: str
    allow_network: bool
    update_baseline_on_accept: bool
    skip_other_pending: bool


def _tenant_allowed(allowlist: frozenset[str], tenant_id: str) -> bool:
    t = tenant_id.strip()
    if "*" in allowlist:
        return True
    return t in allowlist


def _verify_signature(*, body: bytes, secret: str, header_val: str | None) -> bool:
    if not header_val or not header_val.strip():
        return False
    raw = header_val.strip()
    if not raw.startswith("v1="):
        return False
    expect = raw[3:].strip().lower()
    if len(expect) != 64:
        return False
    got = sign_webhook_body(secret=secret, body=body)
    try:
        import hmac

        return hmac.compare_digest(got.encode("ascii"), expect.encode("ascii"))
    except Exception:
        return False


def _resolved_outputs_root_allowlist_bases(roots: frozenset[Path]) -> tuple[Path, ...]:
    out: list[Path] = []
    for r in roots:
        try:
            out.append(r.expanduser().resolve())
        except OSError:
            continue
    return tuple(out)


def _resolve_payload_outputs_root(oroot_s: str, *, allowed_bases: tuple[Path, ...]) -> Path | None:
    """Map webhook ``outputs_root`` string to a resolved path only if confined to ``allowed_bases``."""

    return resolve_absolute_path_under_allowlist_bases(oroot_s, allowed_bases=allowed_bases)


def _dedupe_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        tid = str(it.get("tenant_id", "")).strip()
        rid = normalize_repo_id(str(it.get("repo_id", "")))
        oroot = str(it.get("outputs_root", "")).strip()
        if not tid or not rid or not oroot:
            continue
        key = (tid, rid, oroot)
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


def _resolve_scopes_from_payload(
    *,
    event: str,
    items: list[dict[str, Any]],
    allowed_bases: tuple[Path, ...],
) -> list[tuple[str, str, Path]]:
    """Return ``(tenant_id, repo_id, outputs_root)`` per item (deduped)."""

    if event not in _SUPPORTED_EVENTS:
        return []
    deduped = _dedupe_items(items)
    out: list[tuple[str, str, Path]] = []
    for it in deduped:
        tid = str(it.get("tenant_id", "")).strip()
        rid = normalize_repo_id(str(it.get("repo_id", "")))
        oroot_s = str(it.get("outputs_root", "")).strip()
        oroot = _resolve_payload_outputs_root(oroot_s, allowed_bases=allowed_bases)
        if oroot is None:
            continue
        out.append((tid, rid, oroot))
    return out


def process_fleet_webhook_payload(
    payload: Mapping[str, Any],
    *,
    cfg: LivingWebhookServerConfig,
) -> tuple[int, dict[str, Any]]:
    """Verify payload shape and run living recompile for each scoped item. Returns (http_status, json_body)."""

    schema = str(payload.get("schema", "")).strip()
    if schema != _SCHEMA:
        return 400, {"error": "invalid_schema", "expected": _SCHEMA}

    event = str(payload.get("event", "")).strip()
    if event not in _SUPPORTED_EVENTS:
        return 400, {"error": "unsupported_event", "event": event, "supported": sorted(_SUPPORTED_EVENTS)}

    raw_items = payload.get("items")
    if not isinstance(raw_items, list):
        return 400, {"error": "items_must_be_array"}

    items = [x for x in raw_items if isinstance(x, dict)]
    allowed_bases = _resolved_outputs_root_allowlist_bases(cfg.outputs_root_allowlist)
    scopes = _resolve_scopes_from_payload(event=event, items=items, allowed_bases=allowed_bases)
    if not scopes:
        return 200, {"schema": "akc.living.webhook_result.v1", "processed": [], "message": "no_eligible_items"}

    ingest = cfg.ingest_state_path

    def _parse_pm(s: str) -> Any:
        v = str(s).strip()
        if v == "audit_only":
            return "audit_only"
        if v == "enforce":
            return "enforce"
        raise ValueError("policy_mode")

    def _parse_cam(s: str) -> Any:
        v = str(s).strip()
        if v in ("quick", "thorough"):
            return v
        raise ValueError("canary/accept mode")

    def _parse_ctm(s: str) -> Any:
        v = str(s).strip()
        if v in ("smoke", "full"):
            return v
        raise ValueError("canary_test_mode")

    try:
        policy_mode = _parse_pm(cfg.policy_mode)
        canary_mode = _parse_cam(cfg.canary_mode)
        accept_mode = _parse_cam(cfg.accept_mode)
        canary_test_mode = _parse_ctm(cfg.canary_test_mode)
    except ValueError as e:
        return 500, {"error": "invalid_dispatch_defaults", "detail": str(e)}

    processed: list[dict[str, Any]] = []
    for tenant_id, repo_id, outputs_root in scopes:
        if not _tenant_allowed(cfg.tenant_allowlist, tenant_id):
            processed.append(
                {
                    "tenant_id": tenant_id,
                    "repo_id": repo_id,
                    "outputs_root": str(outputs_root),
                    "skipped": True,
                    "reason": "tenant_not_allowlisted",
                }
            )
            continue

        code = living_recompile_execute(
            tenant_id=tenant_id,
            repo_id=repo_id,
            outputs_root=outputs_root,
            ingest_state_path=ingest,
            baseline_path=None,
            eval_suite_path=cfg.eval_suite_path,
            goal=cfg.goal,
            policy_mode=policy_mode,
            canary_mode=canary_mode,
            accept_mode=accept_mode,
            canary_test_mode=canary_test_mode,
            allow_network=cfg.allow_network,
            llm_backend=cfg.llm_backend,
            update_baseline_on_accept=cfg.update_baseline_on_accept,
            skip_other_pending=cfg.skip_other_pending,
            opa_policy_path=cfg.opa_policy_path,
            opa_decision_path=cfg.opa_decision_path,
            living_automation_profile=cfg.living_automation_profile,
        )
        processed.append(
            {
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "outputs_root": str(outputs_root),
                "exit_code": int(code),
            }
        )

    any_fail = any((not p.get("skipped")) and int(p.get("exit_code", 1)) != 0 for p in processed if isinstance(p, dict))
    body = {
        "schema": "akc.living.webhook_result.v1",
        "event": event,
        "processed": processed,
        "any_failure": bool(any_fail),
    }
    return 200, body


def _read_body(handler: BaseHTTPRequestHandler) -> bytes:
    length_s = handler.headers.get("Content-Length")
    if not length_s:
        return b""
    try:
        n = int(length_s)
    except ValueError:
        return b""
    if n < 0 or n > _MAX_BODY_BYTES:
        return b""
    return handler.rfile.read(n)


class _LivingWebhookHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.debug("%s - %s", self.address_string(), fmt % args)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == "/health":
            body = json.dumps({"ok": True, "schema": "akc.living.webhook_health.v1"}, sort_keys=True).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(404)
        self.end_headers()

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path not in ("/v1/trigger", "/"):
            self.send_response(404)
            self.end_headers()
            return

        cfg: LivingWebhookServerConfig = self.server.webhook_cfg  # type: ignore[attr-defined]
        body = _read_body(self)
        sig = self.headers.get("X-AKC-Signature")
        if not _verify_signature(body=body, secret=cfg.secret, header_val=sig):
            msg = json.dumps({"error": "invalid_signature"}, sort_keys=True).encode("utf-8")
            self.send_response(401)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return

        try:
            payload = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            msg = json.dumps({"error": "invalid_json"}, sort_keys=True).encode("utf-8")
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return

        if not isinstance(payload, dict):
            msg = json.dumps({"error": "payload_not_object"}, sort_keys=True).encode("utf-8")
            self.send_response(400)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(msg)))
            self.end_headers()
            self.wfile.write(msg)
            return

        status, out = process_fleet_webhook_payload(payload, cfg=cfg)
        raw = json.dumps(out, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)


class _ThreadingHTTPServerWithCfg(ThreadingHTTPServer):
    def __init__(self, server_address: Any, RequestHandlerClass: type, *, webhook_cfg: LivingWebhookServerConfig):
        super().__init__(server_address, RequestHandlerClass)
        self.webhook_cfg = webhook_cfg


def run_living_webhook_server(cfg: LivingWebhookServerConfig) -> None:
    """Listen until interrupted (KeyboardInterrupt)."""

    addr = (cfg.bind_host, int(cfg.port))
    httpd = _ThreadingHTTPServerWithCfg(addr, _LivingWebhookHandler, webhook_cfg=cfg)
    logger.info("living webhook listening on http://%s:%s (POST /v1/trigger or /)", cfg.bind_host, cfg.port)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


def run_living_webhook_server_thread(cfg: LivingWebhookServerConfig) -> tuple[threading.Thread, ThreadingHTTPServer]:
    """Start server in a daemon thread (for tests). Returns (thread, server)."""

    addr = (cfg.bind_host, int(cfg.port))
    httpd = _ThreadingHTTPServerWithCfg(addr, _LivingWebhookHandler, webhook_cfg=cfg)
    th = threading.Thread(target=httpd.serve_forever, name="akc-living-webhook", daemon=True)
    th.start()
    return th, httpd
