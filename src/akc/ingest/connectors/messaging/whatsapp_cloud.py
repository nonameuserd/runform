"""WhatsApp Cloud webhook payload connector (Phase 1).

WhatsApp Cloud API is primarily webhook-driven for inbound message events. This connector
ingests *stored* webhook payloads (JSON/JSONL) captured by your infrastructure.
"""

from __future__ import annotations

import hmac
import json
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

from akc.ingest.connectors.base import BaseConnector
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document


def _require_non_empty(value: str, *, name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be a non-empty string")


def _coerce_str(value: object) -> str | None:
    if isinstance(value, str):
        s = value.strip()
        return s if s else None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return None


def _coerce_int(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            return int(s)
        except Exception:
            return None
    return None


def _normalize_header_key(key: str) -> str:
    return key.strip().lower()


def verify_whatsapp_cloud_signature(*, app_secret: str, payload_body: bytes, signature_header: str) -> bool:
    """Verify Meta webhook signature.

    Args:
        app_secret: Your WhatsApp app secret (never store in state or metadata).
        payload_body: Raw request body bytes (exact bytes received).
        signature_header: Value of `X-Hub-Signature-256`, usually `sha256=<hex>`.

    Returns:
        True if signature matches; else False.
    """

    _require_non_empty(app_secret, name="app_secret")
    if not isinstance(payload_body, (bytes, bytearray)):
        raise TypeError("payload_body must be bytes")
    if not isinstance(signature_header, str) or not signature_header.strip():
        return False

    header = signature_header.strip()
    if header.lower().startswith("sha256="):
        header = header.split("=", 1)[1].strip()
    if not header:
        return False

    try:
        expected = hmac.new(app_secret.encode("utf-8"), payload_body, sha256).hexdigest()
    except Exception:
        return False
    try:
        return hmac.compare_digest(expected, header)
    except Exception:
        return False


class _WhatsAppDedupeState:
    """Tiny JSON state holding recent message ids to dedupe payload replays."""

    def __init__(self, *, path: str, tenant_id: str, max_ids: int) -> None:
        _require_non_empty(path, name="path")
        _require_non_empty(tenant_id, name="tenant_id")
        if max_ids <= 0:
            raise ValueError("max_ids must be > 0")
        self._path = Path(path)
        self._tenant_id = tenant_id
        self._max_ids = int(max_ids)

    def load_seen_ids(self) -> list[str]:
        try:
            raw = self._path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return []
        except OSError as e:
            raise ConnectorError(f"failed to read whatsapp state: {self._path}") from e
        try:
            data = json.loads(raw)
        except Exception as e:
            raise ConnectorError(f"whatsapp state is not valid JSON: {self._path}") from e
        if not isinstance(data, dict):
            raise ConnectorError(f"whatsapp state must be a JSON object: {self._path}")
        tid = data.get("tenant_id")
        if isinstance(tid, str) and tid and tid != self._tenant_id:
            raise ConnectorError("whatsapp state tenant_id mismatch (possible cross-tenant state)")
        ids = data.get("seen_message_ids")
        if not isinstance(ids, list):
            return []
        out: list[str] = []
        for x in ids:
            if isinstance(x, str) and x.strip():
                out.append(x.strip())
        return out[-self._max_ids :]

    def save_seen_ids(self, *, seen_ids: Sequence[str]) -> None:
        payload = {
            "tenant_id": self._tenant_id,
            "seen_message_ids": list(seen_ids)[-self._max_ids :],
        }
        try:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._path.with_suffix(self._path.suffix + ".tmp")
            tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
            tmp.replace(self._path)
        except OSError as e:
            raise ConnectorError(f"failed to write whatsapp state: {self._path}") from e


def _iter_payload_files(paths: Sequence[str]) -> list[Path]:
    files: list[Path] = []
    for p in paths:
        _require_non_empty(p, name="payload_paths item")
        path = Path(p)
        if path.is_dir():
            for ext in ("*.json", "*.jsonl"):
                files.extend(sorted(path.rglob(ext)))
        else:
            files.append(path)
    # Deterministic ordering, stable across runs for state updates.
    uniq = sorted({f.resolve() for f in files})
    return uniq


def _iter_json_objects_from_file(path: Path) -> Iterator[dict[str, Any]]:
    suffix = path.suffix.lower()
    try:
        raw_text = path.read_text(encoding="utf-8")
    except OSError as e:
        raise ConnectorError(f"failed to read whatsapp payload file: {path}") from e
    if suffix == ".jsonl":
        for line_no, line in enumerate(raw_text.splitlines(), start=1):
            s = line.strip()
            if not s:
                continue
            try:
                obj = json.loads(s)
            except Exception as e:
                raise ConnectorError(f"invalid JSONL in whatsapp payload file {path}:{line_no}") from e
            if isinstance(obj, dict):
                yield obj
            elif isinstance(obj, list):
                for item in obj:
                    if isinstance(item, dict):
                        yield item
            else:
                continue
        return

    # .json (or unknown): accept object or list.
    try:
        parsed = json.loads(raw_text)
    except Exception as e:
        raise ConnectorError(f"invalid JSON in whatsapp payload file {path}") from e
    if isinstance(parsed, dict):
        yield parsed
    elif isinstance(parsed, list):
        for item in parsed:
            if isinstance(item, dict):
                yield item


def _extract_text_from_message(message: Mapping[str, Any]) -> str:
    mtype = message.get("type")
    t = mtype if isinstance(mtype, str) and mtype else ""

    # Common WhatsApp payload shapes.
    text_obj = message.get("text")
    if isinstance(text_obj, dict):
        body = text_obj.get("body")
        if isinstance(body, str) and body.strip():
            return body

    # Interactive payloads often include user selections.
    interactive = message.get("interactive")
    if isinstance(interactive, dict):
        itype = interactive.get("type")
        if isinstance(itype, str) and itype.strip():
            # Try to extract the most useful selection fields.
            if itype == "button_reply":
                br = interactive.get("button_reply")
                if isinstance(br, dict):
                    title = br.get("title")
                    if isinstance(title, str) and title.strip():
                        return title
                    bid = br.get("id")
                    if isinstance(bid, str) and bid.strip():
                        return bid
            if itype == "list_reply":
                lr = interactive.get("list_reply")
                if isinstance(lr, dict):
                    title = lr.get("title")
                    if isinstance(title, str) and title.strip():
                        return title
                    desc = lr.get("description")
                    if isinstance(desc, str) and desc.strip():
                        return desc
                    lid = lr.get("id")
                    if isinstance(lid, str) and lid.strip():
                        return lid

    # Fallback fields (e.g. button messages, media captions).
    caption = None
    for k in ("image", "video", "document", "audio", "sticker"):
        obj = message.get(k)
        if isinstance(obj, dict):
            cap = obj.get("caption")
            if isinstance(cap, str) and cap.strip():
                caption = cap.strip()
                break
    if caption:
        return caption

    if t:
        return f"[{t}]"
    return ""


def _iter_envelope_messages(payload: Mapping[str, Any]) -> Iterator[tuple[dict[str, Any], dict[str, Any]]]:
    """Yield (message, envelope_metadata) tuples from a webhook payload.

    envelope_metadata may include phone_number_id and waba_id.
    """

    entry = payload.get("entry")
    if not isinstance(entry, list):
        return
    for e in entry:
        if not isinstance(e, dict):
            continue
        changes = e.get("changes")
        if not isinstance(changes, list):
            continue
        for ch in changes:
            if not isinstance(ch, dict):
                continue
            value = ch.get("value")
            if not isinstance(value, dict):
                continue
            metadata = value.get("metadata")
            phone_number_id: str | None = None
            if isinstance(metadata, dict):
                phone_number_id = _coerce_str(metadata.get("phone_number_id"))
            waba_id = _coerce_str(value.get("business_account_id")) or _coerce_str(e.get("id"))
            env_md: dict[str, Any] = {}
            if phone_number_id is not None:
                env_md["phone_number_id"] = phone_number_id
            if waba_id is not None:
                env_md["waba_id"] = waba_id

            messages = value.get("messages")
            if isinstance(messages, list):
                for m in messages:
                    if isinstance(m, dict):
                        yield m, env_md


@dataclass(frozen=True, slots=True)
class WhatsAppCloudWebhookConfig:
    """Configuration for WhatsApp Cloud webhook payload ingestion.

    Args:
        payload_paths: Files or directories containing JSON/JSONL webhook payloads.
        phone_number_id: Optional filter; only emit messages for this phone number id.
        waba_id: Optional filter; only emit messages for this WhatsApp Business Account id.
        state_path: Optional per-tenant JSON file used for dedupe across runs.
        max_seen_message_ids: Max message ids retained in state for dedupe.
        max_documents_per_run: Safety cap; stop emitting after this many docs in a run.
        verify_signatures: If True, enforce signature verification when payload envelope contains headers.
        app_secret: Required if verify_signatures is True. Do not hardcode; pass from env/CLI.
    """

    payload_paths: tuple[str, ...]
    phone_number_id: str | None = None
    waba_id: str | None = None
    state_path: str | None = None
    max_seen_message_ids: int = 5000
    max_documents_per_run: int = 5000
    verify_signatures: bool = False
    app_secret: str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.payload_paths, tuple) or not self.payload_paths:
            raise ValueError("payload_paths must be a non-empty tuple")
        for p in self.payload_paths:
            _require_non_empty(p, name="payload_paths item")
        if self.phone_number_id is not None:
            _require_non_empty(self.phone_number_id, name="phone_number_id")
        if self.waba_id is not None:
            _require_non_empty(self.waba_id, name="waba_id")
        if self.state_path is not None:
            _require_non_empty(self.state_path, name="state_path")
        if self.max_seen_message_ids <= 0:
            raise ValueError("max_seen_message_ids must be > 0")
        if self.max_documents_per_run <= 0:
            raise ValueError("max_documents_per_run must be > 0")
        if self.verify_signatures:
            if self.app_secret is None:
                raise ValueError("app_secret is required when verify_signatures is True")
            _require_non_empty(self.app_secret, name="app_secret")


class WhatsAppCloudWebhookConnector(BaseConnector):
    """Connector that ingests stored WhatsApp Cloud webhook payloads."""

    def __init__(self, *, tenant_id: str, config: WhatsAppCloudWebhookConfig) -> None:
        super().__init__(tenant_id=tenant_id, source_type="messaging")
        self._config = config
        self._state = (
            _WhatsAppDedupeState(path=config.state_path, tenant_id=tenant_id, max_ids=config.max_seen_message_ids)
            if config.state_path is not None
            else None
        )

    @property
    def config(self) -> WhatsAppCloudWebhookConfig:
        return self._config

    def list_sources(self) -> Sequence[str]:
        return ["webhook_payloads"]

    def _enforce_signature_if_present(self, *, envelope: Mapping[str, Any], body_obj: Mapping[str, Any]) -> None:
        if not self._config.verify_signatures:
            return
        # Envelope shape we accept:
        # - {"headers": {...}, "raw_body": "...", "body": {...}}
        headers = envelope.get("headers")
        if not isinstance(headers, dict):
            raise ConnectorError("signature verification enabled but payload envelope has no headers")

        sig: str | None = None
        for k, v in headers.items():
            if not isinstance(k, str):
                continue
            if _normalize_header_key(k) == "x-hub-signature-256":
                if isinstance(v, str):
                    sig = v
                break
        if sig is None:
            raise ConnectorError("signature verification enabled but missing X-Hub-Signature-256 header")

        raw_body = envelope.get("raw_body")
        if isinstance(raw_body, str):
            payload_bytes = raw_body.encode("utf-8")
        else:
            # Best-effort canonical encoding; callers should prefer storing raw_body.
            payload_bytes = json.dumps(body_obj, separators=(",", ":"), sort_keys=True).encode("utf-8")

        ok = verify_whatsapp_cloud_signature(
            app_secret=self._config.app_secret or "",
            payload_body=payload_bytes,
            signature_header=sig,
        )
        if not ok:
            raise ConnectorError("whatsapp webhook signature verification failed")

    def fetch(self, source_id: str) -> Iterable[Document]:
        if source_id != "webhook_payloads":
            raise ConnectorError("unknown source_id for WhatsAppCloudWebhookConnector")

        seen_ids_list = self._state.load_seen_ids() if self._state is not None else []
        seen: set[str] = set(seen_ids_list)
        emitted = 0

        for file_path in _iter_payload_files(self._config.payload_paths):
            for obj in _iter_json_objects_from_file(file_path):
                envelope: Mapping[str, Any] = obj
                body = obj.get("body") if isinstance(obj, dict) else None
                if isinstance(body, dict):
                    self._enforce_signature_if_present(envelope=envelope, body_obj=body)
                    payload = body
                else:
                    payload = obj

                if not isinstance(payload, dict):
                    continue

                for msg, env_md in _iter_envelope_messages(payload):
                    msg_id = _coerce_str(msg.get("id"))
                    if msg_id is None:
                        continue
                    if msg_id in seen:
                        continue

                    # Filters (tenant-safe; applied before state update).
                    phone_number_id = _coerce_str(env_md.get("phone_number_id"))
                    waba_id = _coerce_str(env_md.get("waba_id"))
                    if self._config.phone_number_id is not None and phone_number_id != self._config.phone_number_id:
                        continue
                    if self._config.waba_id is not None and waba_id != self._config.waba_id:
                        continue

                    wa_from = _coerce_str(msg.get("from")) or ""
                    timestamp = _coerce_str(msg.get("timestamp")) or ""
                    text = _extract_text_from_message(msg).strip()
                    if not text:
                        continue

                    context = msg.get("context")
                    thread_id = msg_id
                    if isinstance(context, dict):
                        ctx_id = _coerce_str(context.get("id"))
                        if ctx_id is not None:
                            thread_id = ctx_id

                    channel_id = phone_number_id or "unknown"
                    source = f"whatsapp_cloud:{channel_id}"
                    logical_locator = f"thread:{thread_id}/message:{msg_id}"

                    ts_int = _coerce_int(timestamp)
                    indexed_at_ms = int(ts_int) * 1000 if ts_int is not None else int(time.time() * 1000)

                    content = (
                        "\n".join(
                            [
                                f"WhatsApp message {msg_id} to phone_number_id {channel_id}",
                                "",
                                (f"From {wa_from} @ {timestamp}:" if wa_from or timestamp else "Message:"),
                                text,
                                "",
                            ]
                        ).strip()
                        + "\n"
                    )
                    metadata: dict[str, object] = {
                        "platform": "whatsapp_cloud",
                        "phone_number_id": channel_id,
                        "waba_id": waba_id or "",
                        "thread_id": thread_id,
                        "message_id": msg_id,
                        "timestamp": timestamp,
                        "user": wa_from,
                        "ingest_source_kind": "messaging",
                        "indexed_at_ms": indexed_at_ms,
                        "payload_file": str(file_path),
                    }

                    yield self._make_document(
                        source=source,
                        logical_locator=logical_locator,
                        content=content,
                        metadata=metadata,
                    )

                    emitted += 1
                    seen.add(msg_id)
                    seen_ids_list.append(msg_id)
                    if emitted >= self._config.max_documents_per_run:
                        break
                if emitted >= self._config.max_documents_per_run:
                    break
            if emitted >= self._config.max_documents_per_run:
                break

        if self._state is not None:
            self._state.save_seen_ids(seen_ids=seen_ids_list)


def build_whatsapp_cloud_connector(
    *,
    tenant_id: str,
    payload_paths: Sequence[str],
    phone_number_id: str | None = None,
    waba_id: str | None = None,
    state_path: str | None = None,
    max_seen_message_ids: int = 5000,
    max_documents_per_run: int = 5000,
    verify_signatures: bool = False,
    app_secret: str | None = None,
) -> WhatsAppCloudWebhookConnector:
    return WhatsAppCloudWebhookConnector(
        tenant_id=tenant_id,
        config=WhatsAppCloudWebhookConfig(
            payload_paths=tuple(str(p) for p in payload_paths),
            phone_number_id=phone_number_id,
            waba_id=waba_id,
            state_path=state_path,
            max_seen_message_ids=int(max_seen_message_ids),
            max_documents_per_run=int(max_documents_per_run),
            verify_signatures=bool(verify_signatures),
            app_secret=app_secret,
        ),
    )
