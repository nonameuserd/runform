"""Recompute delivery activation proof from activation evidence + session channel state."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast


def resolve_recipient_email_for_token(per_recipient: Mapping[str, Any], invite_token_id: str) -> str | None:
    """Map ``invite_token_id`` to normalized email key in ``per_recipient``."""

    tid = str(invite_token_id).strip()
    if not tid:
        return None
    for email, row in per_recipient.items():
        if not isinstance(row, dict):
            continue
        if str(row.get("invite_token_id") or "").strip() == tid:
            return str(email).strip().lower()
    return None


def recipient_platforms_from_sidecar(recipients_sidecar: Mapping[str, Any] | None, email: str) -> tuple[str, ...]:
    if recipients_sidecar is None:
        return ()
    rmap = recipients_sidecar.get("recipients")
    if not isinstance(rmap, dict):
        return ()
    row = rmap.get(email)
    if not isinstance(row, dict):
        return ()
    plat = row.get("platforms")
    if not isinstance(plat, list):
        return ()
    return tuple(str(p) for p in plat if str(p).strip())


def _evidence_for_recipient(records: list[Mapping[str, Any]], *, email: str) -> list[Mapping[str, Any]]:
    em = email.strip().lower()
    return [r for r in records if str(r.get("recipient_email") or "").strip().lower() == em]


def _has_beta_provider_evidence(rows: list[Mapping[str, Any]], *, platform: str) -> bool:
    for r in rows:
        if str(r.get("platform") or "") != platform:
            continue
        kind = str(r.get("evidence_kind") or "")
        if kind in {"invite_opened", "provider_install"}:
            return True
    return False


def _store_lane_satisfied(session: Mapping[str, Any], *, platform: str) -> bool:
    """Provider publication: web store channel progressed, or mobile store_release lane live."""

    if platform == "web":
        pp = session.get("per_platform")
        if not isinstance(pp, dict):
            return False
        web = pp.get("web")
        if not isinstance(web, dict):
            return False
        channels = web.get("channels")
        if not isinstance(channels, dict):
            return False
        store = channels.get("store")
        if not isinstance(store, dict):
            return False
        return str(store.get("status") or "") in {"completed", "in_progress"}
    sr = session.get("store_release")
    if not isinstance(sr, dict):
        return False
    lane = sr.get(platform)
    if not isinstance(lane, dict):
        return False
    return str(lane.get("status") or "") == "live"


def _has_app_first_run(rows: list[Mapping[str, Any]], *, platform: str) -> bool:
    for r in rows:
        if str(r.get("platform") or "") != platform:
            continue
        if str(r.get("evidence_kind") or "") == "app_first_run":
            return True
    return False


def _provider_ok_for_platform(
    *,
    release_mode: str,
    session: Mapping[str, Any],
    erows: list[Mapping[str, Any]],
    platform: str,
) -> bool:
    beta_ok = _has_beta_provider_evidence(erows, platform=platform)
    store_ok = _store_lane_satisfied(session, platform=platform)
    if release_mode == "beta":
        return beta_ok
    if release_mode == "store":
        return store_ok
    return beta_ok or store_ok


def recompute_delivery_activation(
    session: dict[str, Any],
    *,
    evidence_records: list[dict[str, Any]],
    recipients_sidecar: Mapping[str, Any] | None,
) -> None:
    """Mutate ``session`` ``per_recipient`` activation_proof / status and session rollup."""

    per_raw = session.get("per_recipient")
    if not isinstance(per_raw, dict):
        return
    per_recipient: dict[str, Any] = cast(dict[str, Any], per_raw)
    release_mode = str(session.get("release_mode") or "")
    sess_platforms_raw = session.get("platforms")
    sess_platforms: tuple[str, ...] = (
        tuple(str(p) for p in sess_platforms_raw) if isinstance(sess_platforms_raw, list) else ()
    )

    total = len(per_recipient)
    prov_ok = 0
    full_ok = 0

    for email, row in per_recipient.items():
        if not isinstance(row, dict):
            continue
        proof = row.get("activation_proof")
        if not isinstance(proof, dict):
            proof = {"status": "pending", "provider_proof": "pending", "app_proof": "pending"}
            row["activation_proof"] = proof

        platforms = recipient_platforms_from_sidecar(recipients_sidecar, email)
        if not platforms:
            plat_row = row.get("platforms")
            if isinstance(plat_row, list):
                platforms = tuple(str(p) for p in plat_row)
        if not platforms:
            platforms = sess_platforms

        platforms = tuple(p for p in platforms if p in ("web", "ios", "android"))
        erows = _evidence_for_recipient(evidence_records, email=email)

        if not platforms:
            proof["provider_proof"] = "not_applicable"
            proof["app_proof"] = "pending"
            proof["status"] = "pending"
            if str(row.get("status") or "") not in {"failed", "blocked"}:
                row["status"] = "pending"
            continue

        prov_flags = [
            _provider_ok_for_platform(
                release_mode=release_mode,
                session=session,
                erows=erows,
                platform=plat,
            )
            for plat in platforms
        ]
        app_flags = [_has_app_first_run(erows, platform=plat) for plat in platforms]

        provider_ok = all(prov_flags)
        app_ok = all(app_flags)

        proof["provider_proof"] = "satisfied" if provider_ok else "pending"
        proof["app_proof"] = "satisfied" if app_ok else "pending"

        if provider_ok and app_ok:
            proof["status"] = "satisfied"
            if str(row.get("status") or "") not in {"failed", "blocked"}:
                row["status"] = "active"
        elif provider_ok or app_ok:
            proof["status"] = "partial"
            if str(row.get("status") or "") not in {"failed", "blocked"}:
                if provider_ok and not app_ok:
                    row["status"] = "installed"
                elif app_ok and not provider_ok:
                    row["status"] = "invited"
                else:
                    row["status"] = "pending"
        else:
            proof["status"] = "pending"
            if str(row.get("status") or "") not in {"failed", "blocked"}:
                row["status"] = "pending"

        if proof["provider_proof"] == "satisfied":
            prov_ok += 1
        if proof["status"] == "satisfied":
            full_ok += 1

    rollup = session.get("activation_proof")
    if not isinstance(rollup, dict):
        rollup = {
            "status": "pending",
            "recipients_total": total,
            "recipients_provider_satisfied": 0,
            "recipients_fully_satisfied": 0,
        }
        session["activation_proof"] = rollup
    rollup["recipients_total"] = total
    rollup["recipients_provider_satisfied"] = prov_ok
    rollup["recipients_fully_satisfied"] = full_ok
    if total == 0:
        rollup["status"] = "pending"
    elif full_ok == total:
        rollup["status"] = "complete"
    elif full_ok > 0 or prov_ok > 0:
        rollup["status"] = "partial"
    else:
        rollup["status"] = "pending"
