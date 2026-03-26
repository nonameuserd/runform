from __future__ import annotations

import binascii
import hashlib
import hmac
import time
from collections.abc import Mapping
from dataclasses import dataclass

from akc.control_bot.ingress_adapters import IngressError
from akc.memory.models import require_non_empty


@dataclass(frozen=True, slots=True)
class IngressAuthContext:
    """Normalized auth inputs for signature verification."""

    channel: str
    headers: Mapping[str, str]
    body: bytes
    now_s: int | None = None


def _h(headers: Mapping[str, str], name: str) -> str:
    # HTTP headers are case-insensitive; caller should provide lower-cased keys.
    return str(headers.get(name, "") or "").strip()


def _require_enabled(enabled: bool, *, channel: str) -> None:
    if not enabled:
        raise IngressError(f"channel disabled: {channel}")


def verify_slack_request(
    ctx: IngressAuthContext,
    *,
    enabled: bool,
    signing_secret: str | None,
    tolerance_s: int = 60 * 5,
) -> None:
    """Verify Slack request signing.

    Algorithm: compare `X-Slack-Signature` against
    `v0=` + HMAC_SHA256(signing_secret, "v0:{timestamp}:{raw_body}").
    """

    _require_enabled(enabled, channel="slack")
    secret = str(signing_secret or "").strip()
    require_non_empty(secret, name="channels.slack.signing_secret")

    ts = _h(ctx.headers, "x-slack-request-timestamp")
    sig = _h(ctx.headers, "x-slack-signature")
    if not ts or not sig:
        raise IngressError("missing Slack signature headers")

    try:
        ts_i = int(ts)
    except Exception as e:
        raise IngressError("invalid Slack request timestamp") from e

    now_s = int(ctx.now_s if ctx.now_s is not None else time.time())
    if abs(now_s - ts_i) > int(tolerance_s):
        raise IngressError("stale Slack request timestamp")

    base = b"v0:" + str(ts_i).encode("utf-8") + b":" + ctx.body
    mac = hmac.new(secret.encode("utf-8"), base, hashlib.sha256).hexdigest()
    expected = f"v0={mac}"
    if not hmac.compare_digest(expected, sig):
        raise IngressError("invalid Slack signature")


def verify_telegram_request(
    ctx: IngressAuthContext,
    *,
    enabled: bool,
    secret_token: str | None,
) -> None:
    _require_enabled(enabled, channel="telegram")
    secret = str(secret_token or "").strip()
    require_non_empty(secret, name="channels.telegram.secret_token")

    got = _h(ctx.headers, "x-telegram-bot-api-secret-token")
    if not got:
        raise IngressError("missing Telegram secret token header")
    if not hmac.compare_digest(got, secret):
        raise IngressError("invalid Telegram secret token")


def verify_whatsapp_request(
    ctx: IngressAuthContext,
    *,
    enabled: bool,
    app_secret: str | None,
) -> None:
    """Verify WhatsApp (Meta Graph) webhook signature header.

    Header: `X-Hub-Signature-256: sha256=<hexdigest>`
    Mac: HMAC_SHA256(app_secret, raw_body)
    """

    _require_enabled(enabled, channel="whatsapp")
    secret = str(app_secret or "").strip()
    require_non_empty(secret, name="channels.whatsapp.app_secret")

    sig = _h(ctx.headers, "x-hub-signature-256")
    if not sig:
        raise IngressError("missing WhatsApp signature header")
    if not sig.startswith("sha256="):
        raise IngressError("invalid WhatsApp signature format")
    got_hex = sig.removeprefix("sha256=").strip()
    try:
        _ = binascii.unhexlify(got_hex.encode("ascii"))
    except Exception as e:
        raise IngressError("invalid WhatsApp signature hex") from e

    mac = hmac.new(secret.encode("utf-8"), ctx.body, hashlib.sha256).hexdigest()
    expected = f"sha256={mac}"
    if not hmac.compare_digest(expected, sig):
        raise IngressError("invalid WhatsApp signature")


def verify_whatsapp_webhook_verification(
    *,
    enabled: bool,
    expected_verify_token: str | None,
    mode: str | None,
    verify_token: str | None,
    challenge: str | None,
) -> str:
    """Verify WhatsApp (Meta Cloud API) GET webhook handshake.

    Query params:
    - `hub.mode=subscribe`
    - `hub.verify_token=<token>`
    - `hub.challenge=<random>`

    Returns the `hub.challenge` string if verification succeeds.
    """

    _require_enabled(enabled, channel="whatsapp")
    expected = str(expected_verify_token or "").strip()
    require_non_empty(expected, name="channels.whatsapp.verify_token")

    m = str(mode or "").strip()
    tok = str(verify_token or "").strip()
    ch = str(challenge or "").strip()

    if m != "subscribe":
        raise IngressError("whatsapp verification failed: invalid hub.mode")
    if not hmac.compare_digest(tok, expected):
        raise IngressError("whatsapp verification failed: invalid hub.verify_token")
    if not ch:
        raise IngressError("whatsapp verification failed: missing hub.challenge")
    return ch


def verify_discord_request(
    ctx: IngressAuthContext,
    *,
    enabled: bool,
    public_key: str | None,
) -> None:
    """Verify Discord interactions request signature (Ed25519).

    Headers:
    - `X-Signature-Ed25519` hex signature
    - `X-Signature-Timestamp` timestamp string (included in signed message)
    Signed message: timestamp + raw_body
    """

    _require_enabled(enabled, channel="discord")
    pk_hex = str(public_key or "").strip()
    require_non_empty(pk_hex, name="channels.discord.public_key")

    sig_hex = _h(ctx.headers, "x-signature-ed25519")
    ts = _h(ctx.headers, "x-signature-timestamp")
    if not sig_hex or not ts:
        raise IngressError("missing Discord signature headers")

    try:
        sig_bytes = binascii.unhexlify(sig_hex.encode("ascii"))
    except Exception as e:
        raise IngressError("invalid Discord signature hex") from e

    try:
        pk_bytes = binascii.unhexlify(pk_hex.encode("ascii"))
    except Exception as e:
        raise IngressError("invalid Discord public key hex") from e

    msg = ts.encode("utf-8") + ctx.body

    try:
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
    except Exception as e:  # pragma: no cover
        raise IngressError("Discord signature verification requires 'cryptography' to be installed") from e

    try:
        Ed25519PublicKey.from_public_bytes(pk_bytes).verify(sig_bytes, msg)
    except Exception as e:
        raise IngressError("invalid Discord signature") from e
