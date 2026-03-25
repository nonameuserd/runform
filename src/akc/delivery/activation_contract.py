"""Activation client contract artifact for generated apps (fields + reporting shape)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from akc.delivery import store as delivery_store

CONTRACT_SCHEMA_ID = "akc.delivery.activation_client_contract.v1"


def build_activation_client_contract(
    *,
    session: Mapping[str, Any],
    recipients_sidecar: Mapping[str, Any],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    """Return the v1 contract dict embedded into packaged app trees."""

    did = str(session.get("delivery_id") or "").strip()
    delivery_version = str(session.get("delivery_version") or "").strip()
    per = session.get("per_recipient")
    rmap = recipients_sidecar.get("recipients")
    named: list[dict[str, Any]] = []
    if isinstance(per, dict) and isinstance(rmap, dict):
        for email, prow in per.items():
            if not isinstance(prow, dict):
                continue
            tid = prow.get("invite_token_id")
            row = rmap.get(email)
            plats: list[str] = []
            if isinstance(row, dict) and isinstance(row.get("platforms"), list):
                plats = [str(p) for p in row["platforms"]]
            if tid:
                named.append(
                    {
                        "email": str(email).strip().lower(),
                        "recipient_token_id": str(tid),
                        "platforms": plats,
                    },
                )

    secrets = session.get("secrets")
    invite_hmac_key_present = isinstance(secrets, dict) and bool(str(secrets.get("invite_hmac_key") or "").strip())

    return {
        "schema_id": CONTRACT_SCHEMA_ID,
        "delivery_id": did,
        "delivery_version": delivery_version,
        "tenant_id": str(tenant_id).strip(),
        "repo_id": str(repo_id).strip(),
        "invite_signin_required": True,
        "invite_hmac_configured": invite_hmac_key_present,
        "client_report_fields": {
            "required": [
                "delivery_id",
                "recipient_token_id",
                "platform",
                "app_version",
                "first_run_at_unix_ms",
            ],
            "optional_heartbeat": [
                "heartbeat_at_unix_ms",
                "active",
            ],
        },
        "http_ingest": {
            "description": (
                "POST JSON to operator-controlled endpoint that forwards to "
                "`akc deliver activation-report` or fleet integration; "
                "include tenant/repo/delivery scope in URL or headers."
            ),
            "content_type": "application/json",
            "example_body": {
                "delivery_id": did,
                "recipient_token_id": "<invite_token_id from signed invite / distribution>",
                "platform": "web|ios|android",
                "app_version": delivery_version,
                "first_run_at_unix_ms": 0,
                "heartbeat_at_unix_ms": 0,
                "active": True,
            },
        },
        "named_recipients": named,
    }


def write_activation_client_contract(
    *,
    project_dir: Path,
    delivery_id: str,
    tenant_id: str,
    repo_id: str,
) -> Path:
    """Write ``activation_client_contract.v1.json`` next to packaging outputs."""

    delivery_store.assert_safe_delivery_id(delivery_id)
    session = delivery_store.load_session(project_dir, delivery_id)
    sidecar = delivery_store.load_recipients_sidecar(project_dir, delivery_id)
    doc = build_activation_client_contract(
        session=session,
        recipients_sidecar=sidecar,
        tenant_id=tenant_id,
        repo_id=repo_id,
    )
    out = project_dir.resolve() / ".akc" / "delivery" / delivery_id / "activation_client_contract.v1.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out
