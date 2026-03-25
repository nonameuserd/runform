"""Per-recipient invite tokens and signed web invite links (v1).

Each named recipient receives a stable ``invite_token_id`` stored in the delivery session.
Web distribution uses HMAC-signed URLs so the control plane can treat link opens as
provider-side proof without trusting unauthenticated email alone.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import uuid
from urllib.parse import quote, urlencode


def new_invite_token_id() -> str:
    """Return a new opaque invite token id (UUID4)."""

    return str(uuid.uuid4())


def new_invite_hmac_key() -> str:
    """Return a high-entropy key for signing invite URLs (urlsafe)."""

    return secrets.token_urlsafe(48)


def sign_invite_query(*, delivery_id: str, invite_token_id: str, key: str) -> str:
    """Return hex digest HMAC-SHA256 over ``delivery_id|invite_token_id``."""

    msg = f"{delivery_id}|{invite_token_id}".encode()
    digest = hmac.new(key.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    return digest


def verify_invite_query(*, delivery_id: str, invite_token_id: str, key: str, signature: str) -> bool:
    """Constant-time compare of invite HMAC."""

    if not signature or not key:
        return False
    expect = sign_invite_query(delivery_id=delivery_id, invite_token_id=invite_token_id, key=key)
    return hmac.compare_digest(expect, signature)


def build_signed_web_invite_url(
    *,
    invite_base_url: str,
    delivery_id: str,
    invite_token_id: str,
    key: str,
) -> str:
    """Build ``invite_base_url`` with query params ``akc_did``, ``akc_tid``, ``akc_sig``."""

    base = invite_base_url.strip().rstrip("/?")
    q = urlencode(
        {
            "akc_did": delivery_id,
            "akc_tid": invite_token_id,
            "akc_sig": sign_invite_query(delivery_id=delivery_id, invite_token_id=invite_token_id, key=key),
        },
        quote_via=quote,
        safe="",
    )
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{q}"
