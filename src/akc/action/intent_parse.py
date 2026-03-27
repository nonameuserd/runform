from __future__ import annotations

import re
from hashlib import sha256

from akc.action.models import ActionIntentV1
from akc.memory.models import normalize_repo_id, normalize_tenant_id


def parse_intent(
    *,
    text: str,
    tenant_id: str,
    repo_id: str,
    actor_id: str | None,
    channel: str | None,
) -> ActionIntentV1:
    utterance = text.strip()
    if not utterance:
        raise ValueError("text must be non-empty")

    normalized_tenant = normalize_tenant_id(tenant_id)
    normalized_repo = normalize_repo_id(repo_id)
    intent_material = f"{normalized_tenant}:{normalized_repo}:{utterance}"
    intent_id = f"intent_{sha256(intent_material.encode('utf-8')).hexdigest()[:16]}"
    lowered = utterance.lower()
    if any(k in lowered for k in ("book", "pay", "purchase", "wire", "transfer")):
        risk_summary = "high"
    elif any(k in lowered for k in ("call", "dial", "message", "text", "schedule", "calendar", "event")):
        risk_summary = "medium"
    else:
        risk_summary = "low"
    entities = _extract_entities(utterance)
    goal = utterance
    return ActionIntentV1(
        schema_kind="action_intent",
        schema_version=1,
        intent_id=intent_id,
        tenant_id=normalized_tenant,
        repo_id=normalized_repo,
        actor_id=actor_id.strip() if isinstance(actor_id, str) and actor_id.strip() else None,
        channel=channel.strip() if isinstance(channel, str) and channel.strip() else None,
        utterance=utterance,
        goal=goal,
        entities=entities,
        constraints={},
        risk_summary=risk_summary,
    )


def _extract_entities(text: str) -> dict[str, str]:
    email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    phone_match = re.search(r"\+?[0-9][0-9\-\s]{7,}[0-9]", text)
    date_match = re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
    relation_match = re.search(
        r"\b(?:my\s+)?(dad|father|mom|mother|wife|husband|partner|brother|sister|son|daughter)\b",
        text.lower(),
    )
    out: dict[str, str] = {}
    if email_match is not None:
        out["email"] = email_match.group(0)
    if phone_match is not None:
        out["phone"] = phone_match.group(0).strip()
    if date_match is not None:
        out["date"] = date_match.group(0)
    if relation_match is not None:
        out["contact_hint"] = relation_match.group(1)
    return out
