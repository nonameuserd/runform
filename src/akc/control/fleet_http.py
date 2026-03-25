"""Stdlib HTTP surface for cross-shard operations catalog (Phase 5 fleet).

Run listing merges per-shard ``operations.sqlite`` indexes; artifact bytes are not served.
Bounded writes (e.g. run labels) map to the same mutations as the control CLI and append
to :mod:`akc.control.control_audit`.

**Delivery sessions:** ``GET /v1/deliveries`` and ``GET /v1/deliveries/<tenant>/<repo>/<delivery_id>``
read merged ``delivery_sessions`` rows from the same per-tenant ``operations.sqlite`` indexes
(artifact paths only; no compile/run replacement).

For trace analytics, combine this discovery API with :mod:`akc.control.otel_export` files on
the same ``outputs_root`` trees (Grafana/Loki/OTel collectors).

**Track 6 — read-only operator dashboard**

The static UI under :mod:`akc.control.operator_dashboard` uses **GET only** against this API
(merged runs, run detail). It does not invoke compile, runtime, or viewers. Operators still
open local artifact trees and ``akc view export`` zips on the workstation; the API only
returns index metadata (including ``outputs_root`` and ``manifest_rel_path``) so the UI can
show copy-pastable filesystem paths.

**Deployment (summary)**

1. Run ``akc fleet serve --config /path/to/fleet.json`` (or bind behind a reverse proxy).
2. Serve the dashboard static files (``akc fleet dashboard-serve`` or any static host).
3. If the dashboard origin differs from the fleet origin, set
   ``AKC_FLEET_CORS_ALLOW_ORIGIN`` to the dashboard origin (exact string, e.g.
   ``http://127.0.0.1:9090``) so browsers can send ``Authorization: Bearer …`` on GET/POST.
   When unset, no CORS headers are added (default safe for server-to-server clients only).
4. Use a **read-scoped** token (``runs:read``) in the dashboard; HTTP label writes remain a
   separate capability (``runs:label``) per :mod:`akc.control.fleet_auth`.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
import uuid
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from akc.control.control_audit import append_control_audit_event
from akc.control.fleet_auth import auth_allows_tenant, fleet_read_auth_result, fleet_write_auth_result
from akc.control.fleet_catalog import (
    fleet_get_delivery,
    fleet_get_run,
    fleet_list_deliveries_merged,
    fleet_list_runs_merged,
    fleet_resolve_label_write_shard,
)
from akc.control.fleet_config import FleetConfig, fleet_config_summary
from akc.control.operations_index import OperationsIndex, operations_sqlite_path, validate_run_label_key_value

logger = logging.getLogger(__name__)

_MAX_JSON_POST_BYTES = 8192
_MAX_IDEMPOTENCY_KEY_BYTES = 128
_WRITE_RATE_WINDOW_SECONDS = 60
_WRITE_RATE_MAX_REQUESTS_PER_WINDOW = 30
_IDEMPOTENCY_CACHE_TTL_SECONDS = 600
_IDEMPOTENCY_CACHE_MAX_ENTRIES = 2000

# Characters that must not appear in a single logical header line (response splitting / injection).
_OUTBOUND_HEADER_STRIP = str.maketrans("", "", "\r\n\x00")


def _sanitize_outbound_header_token(name: str) -> str:
    """Strip CR/LF/NUL from header field-names before ``send_header``."""

    return str(name).translate(_OUTBOUND_HEADER_STRIP)


def _sanitize_outbound_header_value(value: str) -> str:
    """Strip CR/LF/NUL from header field-values before ``send_header``."""

    return str(value).translate(_OUTBOUND_HEADER_STRIP)


def _stable_labels_etag(*, tenant_id: str, repo_id: str, run_id: str, labels: dict[str, str]) -> str:
    payload = {
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "run_id": run_id,
        "labels": {k: labels[k] for k in sorted(labels)},
    }
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
    return f'"{digest}"'


def _fleet_cors_allow_origin() -> str | None:
    raw = os.environ.get("AKC_FLEET_CORS_ALLOW_ORIGIN", "").strip()
    return raw or None


def _headers_with_cors(headers: dict[str, str]) -> dict[str, str]:
    """Mirror dashboard origin when ``AKC_FLEET_CORS_ALLOW_ORIGIN`` is set."""

    origin = _fleet_cors_allow_origin()
    if not origin:
        return headers
    out = dict(headers)
    out["Access-Control-Allow-Origin"] = origin
    out["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    out["Access-Control-Allow-Headers"] = "Authorization, Content-Type, X-Request-ID, If-Match, Idempotency-Key"
    out["Vary"] = "Origin"
    return out


def _cors_preflight_path_allowed(path: str) -> bool:
    """Paths eligible for browser CORS preflight (OPTIONS)."""

    p = path.rstrip("/") or "/"
    if p == "/health":
        return True
    parts = [x for x in p.split("/") if x]
    if len(parts) == 2 and parts[0] == "v1" and parts[1] in ("runs", "deliveries"):
        return True
    if len(parts) == 5 and parts[0] == "v1" and parts[1] == "runs":
        return True
    if len(parts) == 5 and parts[0] == "v1" and parts[1] == "deliveries":
        return True
    return bool(len(parts) == 6 and parts[0] == "v1" and parts[1] == "runs" and parts[5] == "labels")


def _json_bytes(payload: dict[str, Any], *, status: int = 200) -> tuple[int, bytes, dict[str, str]]:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    headers = {"Content-Type": "application/json; charset=utf-8", "Content-Length": str(len(body))}
    return status, body, headers


class FleetHTTPRequestHandler(BaseHTTPRequestHandler):
    """``FleetConfig`` is set as ``server.fleet_config``."""

    protocol_version = "HTTP/1.1"
    _write_rl_lock = threading.Lock()
    _write_rl_hits: dict[str, list[float]] = {}
    _idempotency_lock = threading.Lock()
    _idempotency_cache: dict[str, dict[str, Any]] = {}

    def log_message(self, fmt: str, *args: Any) -> None:
        logger.debug("%s - %s", self.address_string(), fmt % args)

    def _send(self, status: int, body: bytes, headers: dict[str, str]) -> None:
        self.send_response(status)
        for k, v in _headers_with_cors(headers).items():
            self.send_header(_sanitize_outbound_header_token(k), _sanitize_outbound_header_value(v))
        self.end_headers()
        self.wfile.write(body)

    def _rate_limit_write(self, *, tenant_id: str, actor: str, action: str) -> bool:
        key = f"{tenant_id}|{actor}|{action}"
        now = time.time()
        floor = now - _WRITE_RATE_WINDOW_SECONDS
        with self._write_rl_lock:
            bucket = self._write_rl_hits.get(key, [])
            bucket = [ts for ts in bucket if ts >= floor]
            if len(bucket) >= _WRITE_RATE_MAX_REQUESTS_PER_WINDOW:
                self._write_rl_hits[key] = bucket
                return False
            bucket.append(now)
            self._write_rl_hits[key] = bucket
            return True

    def _idempotency_fetch(self, *, cache_key: str, payload_hash: str) -> dict[str, Any] | None:
        now = time.time()
        cutoff = now - _IDEMPOTENCY_CACHE_TTL_SECONDS
        with self._idempotency_lock:
            if self._idempotency_cache:
                stale = [k for k, rec in self._idempotency_cache.items() if float(rec.get("created_at", 0.0)) < cutoff]
                for k in stale:
                    self._idempotency_cache.pop(k, None)
            rec = self._idempotency_cache.get(cache_key)
            if rec is None:
                return None
            if str(rec.get("payload_hash")) != payload_hash:
                return {"conflict": True}
            return rec

    def _idempotency_store(
        self,
        *,
        cache_key: str,
        payload_hash: str,
        status: int,
        body: bytes,
        headers: dict[str, str],
    ) -> None:
        now = time.time()
        with self._idempotency_lock:
            self._idempotency_cache[cache_key] = {
                "created_at": now,
                "payload_hash": payload_hash,
                "status": status,
                "body": body,
                "headers": dict(headers),
            }
            if len(self._idempotency_cache) > _IDEMPOTENCY_CACHE_MAX_ENTRIES:
                oldest_key = min(
                    self._idempotency_cache,
                    key=lambda k: float(self._idempotency_cache[k].get("created_at", now)),
                )
                self._idempotency_cache.pop(oldest_key, None)

    def do_OPTIONS(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path or "/"
        if not _cors_preflight_path_allowed(path):
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(204)
        for k, v in _headers_with_cors({"Content-Length": "0"}).items():
            self.send_header(_sanitize_outbound_header_token(k), _sanitize_outbound_header_value(v))
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        cfg: FleetConfig = self.server.fleet_config  # type: ignore[attr-defined]
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        auth_header = self.headers.get("Authorization")

        if path == "/health":
            st, body, hdrs = _json_bytes({"status": "ok", "fleet": fleet_config_summary(cfg)})
            self._send(st, body, hdrs)
            return

        err, ctx = fleet_read_auth_result(cfg, auth_header)
        if err is not None:
            st, body, hdrs = _json_bytes({"error": "unauthorized" if err == 401 else "forbidden"}, status=err)
            self._send(st, body, hdrs)
            return

        if path == "/v1/runs":
            qs = parse_qs(parsed.query)
            tenant_list = qs.get("tenant_id", [])
            tenant_id = str(tenant_list[0]).strip() if tenant_list else ""
            if not tenant_id:
                st, body, hdrs = _json_bytes({"error": "tenant_id query parameter required"}, status=400)
                self._send(st, body, hdrs)
                return
            if not auth_allows_tenant(ctx, tenant_id=tenant_id, cfg=cfg, enforce_allowlist=False):
                st, body, hdrs = _json_bytes({"error": "tenant not allowed for this token"}, status=403)
                self._send(st, body, hdrs)
                return

            repo_list = qs.get("repo_id", [])
            repo_id = str(repo_list[0]).strip() if repo_list else None
            if repo_id == "":
                repo_id = None

            def _opt_int(key: str) -> int | None:
                v = qs.get(key, [])
                if not v:
                    return None
                try:
                    return int(str(v[0]).strip())
                except ValueError:
                    return None

            limit_raw = qs.get("limit", ["50"])
            try:
                limit = int(str(limit_raw[0]).strip()) if limit_raw else 50
            except ValueError:
                limit = 50

            has_trig = qs.get("has_recompile_triggers", [])
            trig_filter: bool | None = None
            if has_trig:
                hv = str(has_trig[0]).strip().lower()
                if hv == "yes":
                    trig_filter = True
                elif hv == "no":
                    trig_filter = False

            rt_ev = qs.get("runtime_evidence", [])
            ev_filter: bool | None = None
            if rt_ev:
                ev = str(rt_ev[0]).strip().lower()
                if ev == "yes":
                    ev_filter = True
                elif ev == "no":
                    ev_filter = False

            intent_list = qs.get("intent_sha256", [])
            intent_s = str(intent_list[0]).strip().lower() if intent_list else None
            if intent_s == "":
                intent_s = None

            runs = fleet_list_runs_merged(
                cfg.shards,
                tenant_id=tenant_id,
                repo_id=repo_id,
                since_ms=_opt_int("since_ms"),
                until_ms=_opt_int("until_ms"),
                stable_intent_sha256=intent_s,
                has_recompile_triggers=trig_filter,
                runtime_evidence_present=ev_filter,
                limit=limit,
            )
            st, body, hdrs = _json_bytes({"tenant_id": tenant_id, "runs": runs})
            self._send(st, body, hdrs)
            return

        if path == "/v1/deliveries":
            qs = parse_qs(parsed.query)
            tenant_list = qs.get("tenant_id", [])
            tenant_id = str(tenant_list[0]).strip() if tenant_list else ""
            if not tenant_id:
                st, body, hdrs = _json_bytes({"error": "tenant_id query parameter required"}, status=400)
                self._send(st, body, hdrs)
                return
            if not auth_allows_tenant(ctx, tenant_id=tenant_id, cfg=cfg, enforce_allowlist=False):
                st, body, hdrs = _json_bytes({"error": "tenant not allowed for this token"}, status=403)
                self._send(st, body, hdrs)
                return

            repo_list = qs.get("repo_id", [])
            repo_id = str(repo_list[0]).strip() if repo_list else None
            if repo_id == "":
                repo_id = None

            def _opt_int_d(key: str) -> int | None:
                v = qs.get(key, [])
                if not v:
                    return None
                try:
                    return int(str(v[0]).strip())
                except ValueError:
                    return None

            limit_raw = qs.get("limit", ["50"])
            try:
                dlim = int(str(limit_raw[0]).strip()) if limit_raw else 50
            except ValueError:
                dlim = 50

            deliveries = fleet_list_deliveries_merged(
                cfg.shards,
                tenant_id=tenant_id,
                repo_id=repo_id,
                since_ms=_opt_int_d("since_ms"),
                until_ms=_opt_int_d("until_ms"),
                limit=dlim,
            )
            st, body, hdrs = _json_bytes({"tenant_id": tenant_id, "deliveries": deliveries})
            self._send(st, body, hdrs)
            return

        parts = path.split("/")
        # /v1/deliveries/<tenant>/<repo>/<delivery_id>
        if len(parts) == 6 and parts[1] == "v1" and parts[2] == "deliveries":
            tenant_id = unquote(parts[3])
            repo_id = unquote(parts[4])
            delivery_id = unquote(parts[5])
            if not auth_allows_tenant(ctx, tenant_id=tenant_id, cfg=cfg, enforce_allowlist=False):
                st, body, hdrs = _json_bytes({"error": "tenant not allowed for this token"}, status=403)
                self._send(st, body, hdrs)
                return
            drow = fleet_get_delivery(cfg.shards, tenant_id=tenant_id, repo_id=repo_id, delivery_id=delivery_id)
            if drow is None:
                st, body, hdrs = _json_bytes({"error": "not_found"}, status=404)
            else:
                st, body, hdrs = _json_bytes({"delivery": drow})
            self._send(st, body, hdrs)
            return

        # /v1/runs/<tenant>/<repo>/<run_id>
        if len(parts) == 6 and parts[1] == "v1" and parts[2] == "runs":
            tenant_id = unquote(parts[3])
            repo_id = unquote(parts[4])
            run_id = unquote(parts[5])
            if not auth_allows_tenant(ctx, tenant_id=tenant_id, cfg=cfg, enforce_allowlist=False):
                st, body, hdrs = _json_bytes({"error": "tenant not allowed for this token"}, status=403)
                self._send(st, body, hdrs)
                return
            row = fleet_get_run(cfg.shards, tenant_id=tenant_id, repo_id=repo_id, run_id=run_id)
            if row is None:
                st, body, hdrs = _json_bytes({"error": "not_found"}, status=404)
            else:
                st, body, hdrs = _json_bytes({"run": row})
                labels_obj = row.get("labels")
                labels = labels_obj if isinstance(labels_obj, dict) else {}
                hdrs["ETag"] = _stable_labels_etag(
                    tenant_id=tenant_id,
                    repo_id=repo_id,
                    run_id=run_id,
                    labels={str(k): str(v) for k, v in labels.items()},
                )
            self._send(st, body, hdrs)
            return

        st, body, hdrs = _json_bytes({"error": "not_found"}, status=404)
        self._send(st, body, hdrs)

    def do_POST(self) -> None:  # noqa: N802
        cfg: FleetConfig = self.server.fleet_config  # type: ignore[attr-defined]
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        parts = path.split("/")

        if not (len(parts) == 7 and parts[1] == "v1" and parts[2] == "runs" and parts[6] == "labels"):
            st, body, hdrs = _json_bytes({"error": "not_found"}, status=404)
            self._send(st, body, hdrs)
            return

        auth_header = self.headers.get("Authorization")
        err, ctx = fleet_write_auth_result(
            cfg,
            auth_header,
            required_scope=("runs:metadata:write", "runs:label"),
        )
        if err is not None:
            st, body, hdrs = _json_bytes({"error": "unauthorized" if err == 401 else "forbidden"}, status=err)
            self._send(st, body, hdrs)
            return
        assert ctx is not None

        tenant_id = unquote(parts[3])
        repo_id = unquote(parts[4])
        run_id = unquote(parts[5])
        if not auth_allows_tenant(ctx, tenant_id=tenant_id, cfg=cfg, enforce_allowlist=True):
            st, body, hdrs = _json_bytes({"error": "tenant not allowed for this token"}, status=403)
            self._send(st, body, hdrs)
            return

        shard = fleet_resolve_label_write_shard(
            cfg.shards,
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
        )
        if shard is None:
            st, body, hdrs = _json_bytes({"error": "tenant not hosted on this fleet"}, status=403)
            self._send(st, body, hdrs)
            return

        actor = ctx.token_id.strip() if ctx.token_id and str(ctx.token_id).strip() else f"fleet-api:{ctx.role}"
        if not self._rate_limit_write(tenant_id=tenant_id, actor=actor, action="runs.label.set"):
            st, body, hdrs = _json_bytes(
                {
                    "error": "write_rate_limited",
                    "window_seconds": _WRITE_RATE_WINDOW_SECONDS,
                    "max_requests": _WRITE_RATE_MAX_REQUESTS_PER_WINDOW,
                },
                status=429,
            )
            self._send(st, body, hdrs)
            return

        content_type = (self.headers.get("Content-Type") or "").strip().lower()
        if content_type and "application/json" not in content_type:
            st, body, hdrs = _json_bytes({"error": "unsupported_content_type"}, status=415)
            self._send(st, body, hdrs)
            return

        length_hdr = self.headers.get("Content-Length")
        try:
            nbytes = int(str(length_hdr).strip()) if length_hdr else 0
        except ValueError:
            nbytes = -1
        if nbytes < 0:
            st, body, hdrs = _json_bytes({"error": "invalid Content-Length"}, status=400)
            self._send(st, body, hdrs)
            return
        if nbytes > _MAX_JSON_POST_BYTES:
            st, body, hdrs = _json_bytes(
                {"error": "payload_too_large", "max_bytes": _MAX_JSON_POST_BYTES},
                status=413,
            )
            self._send(st, body, hdrs)
            return
        raw = self.rfile.read(nbytes) if nbytes else b""
        if length_hdr is not None and len(raw) != nbytes:
            st, body, hdrs = _json_bytes({"error": "body size mismatch"}, status=400)
            self._send(st, body, hdrs)
            return
        try:
            body_obj = json.loads(raw.decode("utf-8") if raw else "{}")
        except (UnicodeDecodeError, json.JSONDecodeError):
            st, body, hdrs = _json_bytes({"error": "invalid JSON body"}, status=400)
            self._send(st, body, hdrs)
            return
        if not isinstance(body_obj, dict):
            st, body, hdrs = _json_bytes({"error": "JSON body must be an object"}, status=400)
            self._send(st, body, hdrs)
            return
        key_raw = body_obj.get("label_key", body_obj.get("key"))
        val_raw = body_obj.get("label_value", body_obj.get("value"))
        if key_raw is None or val_raw is None:
            st, body, hdrs = _json_bytes(
                {"error": "expected label_key/label_value or key/value strings"},
                status=400,
            )
            self._send(st, body, hdrs)
            return
        try:
            lk, lv = validate_run_label_key_value(label_key=str(key_raw), label_value=str(val_raw))
        except ValueError as e:
            st, body, hdrs = _json_bytes({"error": str(e)}, status=400)
            self._send(st, body, hdrs)
            return

        if_match = (self.headers.get("If-Match") or "").strip()
        if not if_match:
            st, body, hdrs = _json_bytes({"error": "if_match_required"}, status=428)
            self._send(st, body, hdrs)
            return

        idem_key_raw = (self.headers.get("Idempotency-Key") or "").strip()
        if idem_key_raw and len(idem_key_raw.encode("utf-8")) > _MAX_IDEMPOTENCY_KEY_BYTES:
            st, body, hdrs = _json_bytes({"error": "idempotency_key_too_large"}, status=400)
            self._send(st, body, hdrs)
            return

        payload_hash = sha256(
            json.dumps(
                {
                    "tenant_id": tenant_id,
                    "repo_id": repo_id,
                    "run_id": run_id,
                    "action": "runs.label.set",
                    "label_key": lk,
                    "label_value": lv,
                    "if_match": if_match,
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        if idem_key_raw:
            cache_key = f"{tenant_id}|{repo_id}|{run_id}|runs.label.set|{idem_key_raw}"
            cached = self._idempotency_fetch(cache_key=cache_key, payload_hash=payload_hash)
            if cached is not None:
                if cached.get("conflict"):
                    st, body, hdrs = _json_bytes({"error": "idempotency_conflict"}, status=409)
                    self._send(st, body, hdrs)
                    return
                cached_headers = dict(cached.get("headers") or {})
                cached_headers["Idempotent-Replay"] = "true"
                self._send(
                    int(cached.get("status", 200)),
                    bytes(cached.get("body", b"")),
                    cached_headers,
                )
                return

        req_header = self.headers.get("X-Request-ID")
        request_id = str(req_header).strip() if req_header and str(req_header).strip() else str(uuid.uuid4())

        sqlite_p = operations_sqlite_path(outputs_root=shard.outputs_root, tenant_id=tenant_id)
        idx = OperationsIndex(sqlite_path=sqlite_p)
        row = idx.get_run(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id)
        if row is None:
            st, body, hdrs = _json_bytes({"error": "not_found"}, status=404)
            self._send(st, body, hdrs)
            return
        labels_obj = row.get("labels")
        current_labels = labels_obj if isinstance(labels_obj, dict) else {}
        current_etag = _stable_labels_etag(
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
            labels={str(k): str(v) for k, v in current_labels.items()},
        )
        if if_match != "*" and if_match != current_etag:
            st, body, hdrs = _json_bytes(
                {
                    "error": "precondition_failed",
                    "resource": "run_labels",
                    "tenant_id": tenant_id,
                    "repo_id": repo_id,
                    "run_id": run_id,
                    "expected_etag": current_etag,
                    "if_match": if_match,
                },
                status=412,
            )
            hdrs["ETag"] = current_etag
            self._send(st, body, hdrs)
            return

        prior = idx.get_label_value(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id, label_key=lk)
        idx.upsert_label(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id, label_key=lk, label_value=lv)
        append_control_audit_event(
            outputs_root=shard.outputs_root,
            tenant_id=tenant_id,
            action="runs.label.set",
            actor=actor,
            request_id=request_id,
            details={
                "shard_id": shard.id,
                "repo_id": repo_id,
                "run_id": run_id,
                "label_key": lk,
                "before": {"label_value": prior},
                "after": {"label_value": lv},
            },
        )

        st, body, hdrs = _json_bytes(
            {
                "tenant_id": tenant_id,
                "repo_id": repo_id,
                "run_id": run_id,
                "label_key": lk,
                "label_value": lv,
                "shard_id": shard.id,
                "request_id": request_id,
            },
            status=200,
        )
        updated = idx.get_run(tenant_id=tenant_id, repo_id=repo_id, run_id=run_id)
        updated_labels_obj = updated.get("labels") if isinstance(updated, dict) else {}
        updated_labels = updated_labels_obj if isinstance(updated_labels_obj, dict) else {}
        hdrs["ETag"] = _stable_labels_etag(
            tenant_id=tenant_id,
            repo_id=repo_id,
            run_id=run_id,
            labels={str(k): str(v) for k, v in updated_labels.items()},
        )
        if idem_key_raw:
            self._idempotency_store(
                cache_key=f"{tenant_id}|{repo_id}|{run_id}|runs.label.set|{idem_key_raw}",
                payload_hash=payload_hash,
                status=st,
                body=body,
                headers=hdrs,
            )
        self._send(st, body, hdrs)


def serve_fleet_http(
    cfg: FleetConfig,
    *,
    host: str = "127.0.0.1",
    port: int = 0,
) -> ThreadingHTTPServer:
    """Start a threaded HTTP server. If ``port`` is 0, OS assigns a port.

    **Operator dashboard:** ship the static UI via ``akc fleet dashboard-serve`` (or copy
    ``akc/control/operator_dashboard/*`` to any static host). Point the UI at this server's
    origin. For cross-origin browser access, set ``AKC_FLEET_CORS_ALLOW_ORIGIN`` to the
    dashboard's origin; keep using read-scoped tokens in the dashboard and reserve
    ``runs:label`` for explicit automation or a future write-capable client.
    """

    httpd = ThreadingHTTPServer((host, port), FleetHTTPRequestHandler)
    httpd.fleet_config = cfg  # type: ignore[attr-defined]
    return httpd
