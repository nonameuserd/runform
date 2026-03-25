"""HTTP clients for TestFlight (App Store Connect), Firebase App Distribution, and Google Play.

Uses stdlib :mod:`urllib` for HTTP; iOS IPA upload uses :mod:`subprocess` (``xcrun altool``) on macOS.
Cryptographic signing for App Store Connect JWT
(ES256) and OAuth for Google APIs require the optional ``akc[delivery-providers]`` extra
(``PyJWT``, ``cryptography``, ``google-auth``).

Tenant isolation: callers must pass ``tenant_id`` / ``repo_id`` for audit context only; no
credentials are read from project files beyond explicit operator env paths.

Environment:
- ``AKC_DELIVERY_PROVIDER_DRY_RUN``: if truthy, skip outbound HTTP and return synthetic success.
- ``AKC_DELIVERY_EXECUTE_PROVIDERS``: if falsy (default when unset treats as **true** for
  real calls when credentials exist); set to ``0`` to force local stub responses without HTTP.
- iOS App Store **IPA upload** uses ``xcrun altool`` on **macOS** only; Linux hosts get a
  blocked error unless dry-run / execute off (same pattern as local Play stubs).
"""

from __future__ import annotations

import json
import os
import shutil
import ssl
import subprocess
import sys
import tempfile
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Final
from urllib.parse import quote, urlencode

ASC_API_BASE: Final[str] = "https://api.appstoreconnect.apple.com"
FIREBASE_APP_DIST_API: Final[str] = "https://firebaseappdistribution.googleapis.com"
PLAY_API_BASE: Final[str] = "https://androidpublisher.googleapis.com/androidpublisher/v3"
PLAY_UPLOAD_BASE: Final[str] = "https://androidpublisher.googleapis.com/upload/androidpublisher/v3"


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def provider_dry_run() -> bool:
    return _env_truthy("AKC_DELIVERY_PROVIDER_DRY_RUN")


def execute_providers_requested() -> bool:
    """When False, distribution dispatch returns stubbed provider results (no HTTP)."""

    raw = str(os.environ.get("AKC_DELIVERY_EXECUTE_PROVIDERS", "") or "").strip().lower()
    if not raw:
        return True
    return raw not in {"0", "false", "no", "off"}


def _read_json_response(resp: Any) -> Any:
    body = resp.read().decode("utf-8", errors="replace")
    if not body.strip():
        return None
    try:
        return json.loads(body)
    except json.JSONDecodeError:
        return {"_raw": body}


def http_request_json(
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    body: dict[str, Any] | None = None,
    timeout_s: float = 120.0,
) -> tuple[int, Any]:
    """Return ``(status_code, parsed_json_or_raw)``."""

    data: bytes | None = None
    hdrs = dict(headers)
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs.setdefault("Content-Type", "application/json")
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method.upper())
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s, context=ctx) as resp:
            return int(resp.getcode() or 200), _read_json_response(resp)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        parsed: Any
        try:
            parsed = json.loads(raw) if raw.strip() else {"error": exc.reason}
        except json.JSONDecodeError:
            parsed = {"_raw": raw, "error": exc.reason}
        return int(exc.code), parsed


# --- App Store Connect (TestFlight beta tester invitations) ---


def _asc_api_key_parts() -> tuple[str, str, Path]:
    """Resolve App Store Connect API key id, issuer id, and ``.p8`` path (operator env)."""

    key_id = (
        os.environ.get("APP_STORE_CONNECT_API_KEY_ID") or os.environ.get("APP_STORE_CONNECT_KEY_ID") or ""
    ).strip()
    issuer_id = (
        os.environ.get("APP_STORE_CONNECT_API_ISSUER_ID") or os.environ.get("APP_STORE_CONNECT_ISSUER_ID") or ""
    ).strip()
    key_path = (
        os.environ.get("APP_STORE_CONNECT_PRIVATE_KEY_PATH") or os.environ.get("APP_STORE_CONNECT_API_KEY_PATH") or ""
    ).strip()
    if not key_id or not issuer_id or not key_path:
        raise RuntimeError(
            "Missing App Store Connect API env: APP_STORE_CONNECT_API_KEY_ID (or KEY_ID), "
            "APP_STORE_CONNECT_API_ISSUER_ID (or ISSUER_ID), "
            "APP_STORE_CONNECT_PRIVATE_KEY_PATH (or API_KEY_PATH)",
        )
    p = Path(key_path).expanduser()
    if not p.is_file():
        raise RuntimeError(f"App Store Connect private key file not found: {p}")
    return key_id, issuer_id, p


def _asc_jwt_token() -> str:
    try:
        import jwt
    except ImportError as exc:
        raise RuntimeError(
            "App Store Connect API requires PyJWT; install `akc[delivery-providers]`",
        ) from exc

    key_id, issuer_id, p = _asc_api_key_parts()
    private_key = p.read_text(encoding="utf-8")
    now = int(__import__("time").time())
    payload = {"iss": issuer_id, "iat": now, "exp": now + 19 * 60, "aud": "appstoreconnect-v1"}
    return str(
        jwt.encode(payload, private_key, algorithm="ES256", headers={"alg": "ES256", "kid": key_id, "typ": "JWT"}),
    )


def asc_invite_emails_to_beta_group(
    *,
    emails: list[str],
    beta_group_id: str,
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    """Create or resolve beta testers and send ``betaTesterInvitations`` for each email.

    ``beta_group_id`` is the App Store Connect UUID for the *external* (or internal) beta group.
    """

    _ = (tenant_id, repo_id)
    if provider_dry_run() or not execute_providers_requested():
        return {"ok": True, "dry_run": True, "invited": list(emails)}
    token = _asc_jwt_token()
    headers = {"Authorization": f"Bearer {token}"}

    invited: list[str] = []
    errors: list[str] = []

    for email in emails:
        e = email.strip()
        if not e:
            continue
        q = urlencode({"filter[email]": e})
        st, data = http_request_json(method="GET", url=f"{ASC_API_BASE}/v1/betaTesters?{q}", headers=headers)
        tester_id: str | None = None
        if st == 200 and isinstance(data, dict):
            dta = data.get("data")
            if isinstance(dta, list) and dta:
                tid = dta[0].get("id")
                if isinstance(tid, str):
                    tester_id = tid
        if tester_id is None:
            create_body = {"data": {"type": "betaTesters", "attributes": {"email": e}}}
            st2, data2 = http_request_json(
                method="POST",
                url=f"{ASC_API_BASE}/v1/betaTesters",
                headers=headers,
                body=create_body,
            )
            if st2 not in {200, 201}:
                errors.append(f"{e}: create betaTester HTTP {st2}: {data2!s}"[:500])
                continue
            if isinstance(data2, dict):
                tid = data2.get("data", {}).get("id")
                if isinstance(tid, str):
                    tester_id = tid
        if not tester_id:
            errors.append(f"{e}: could not resolve betaTester id")
            continue

        invite_body = {
            "data": {
                "type": "betaTesterInvitations",
                "relationships": {
                    "betaTester": {"data": {"type": "betaTesters", "id": tester_id}},
                    "betaGroup": {"data": {"type": "betaGroups", "id": beta_group_id.strip()}},
                },
            },
        }
        st3, data3 = http_request_json(
            method="POST",
            url=f"{ASC_API_BASE}/v1/betaTesterInvitations",
            headers=headers,
            body=invite_body,
        )
        if st3 not in {200, 201, 204}:
            errors.append(f"{e}: betaTesterInvitation HTTP {st3}: {data3!s}"[:500])
            continue
        invited.append(e)

    ok = len(errors) == 0 and len(invited) == len([x for x in emails if x.strip()])
    return {"ok": ok, "invited": invited, "errors": errors}


# --- Firebase App Distribution ---


def _google_access_token(*, scopes: list[str]) -> str:
    try:
        from google.auth.transport import requests as gar  # type: ignore[import-not-found]
        from google.oauth2 import service_account  # type: ignore[import-not-found]
    except ImportError as exc:
        raise RuntimeError(
            "Google APIs require google-auth; install `akc[delivery-providers]`",
        ) from exc

    path = str(os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "") or "").strip()
    if not path:
        raise RuntimeError("GOOGLE_APPLICATION_CREDENTIALS is not set for Firebase / Play OAuth")
    creds = service_account.Credentials.from_service_account_file(path, scopes=scopes)
    creds.refresh(gar.Request())
    tok = creds.token
    if not tok:
        raise RuntimeError("Failed to refresh Google service account token")
    return str(tok)


def firebase_distribute_release(
    *,
    release_name: str,
    tester_emails: list[str],
    group_aliases: list[str],
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    """POST ``releases: distribute`` (full resource name includes project, app, release)."""

    _ = (tenant_id, repo_id)
    if provider_dry_run() or not execute_providers_requested():
        return {"ok": True, "dry_run": True, "release_name": release_name}
    token = _google_access_token(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    url = f"{FIREBASE_APP_DIST_API}/v1/{release_name}:distribute"
    body = {
        "testerEmails": [e.strip() for e in tester_emails if e.strip()],
        "groupAliases": [g.strip() for g in group_aliases if g.strip()],
    }
    st, data = http_request_json(
        method="POST",
        url=url,
        headers={"Authorization": f"Bearer {token}"},
        body=body,
    )
    if st not in {200, 201, 204}:
        return {"ok": False, "http_status": st, "error": data}
    return {"ok": True, "http_status": st, "response": data}


# --- Google Play Developer API ---


def play_validate_edits_session(
    *,
    package_name: str,
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    """Create a draft edit, read the production track, then discard the edit (no publication).

    When an ``aab_path`` is provided to dispatch, callers should use a separate upload path;
    this validates credentials and ``package_name`` only.
    """

    _ = (tenant_id, repo_id)
    if provider_dry_run() or not execute_providers_requested():
        return {"ok": True, "dry_run": True, "package_name": package_name}
    token = _google_access_token(scopes=["https://www.googleapis.com/auth/androidpublisher"])
    headers = {"Authorization": f"Bearer {token}"}
    st, data = http_request_json(
        method="POST",
        url=f"{PLAY_API_BASE}/applications/{quote(package_name, safe='')}/edits",
        headers=headers,
        body={},
    )
    if st not in {200, 201} or not isinstance(data, dict):
        return {"ok": False, "http_status": st, "error": data}
    edit_id = data.get("id")
    if not isinstance(edit_id, str):
        return {"ok": False, "error": "edits.insert missing id", "raw": data}
    tr_url = (
        f"{PLAY_API_BASE}/applications/{quote(package_name, safe='')}/edits/{quote(edit_id, safe='')}/tracks/production"
    )
    st2, tracks = http_request_json(method="GET", url=tr_url, headers=headers)
    del_url = f"{PLAY_API_BASE}/applications/{quote(package_name, safe='')}/edits/{quote(edit_id, safe='')}"
    http_request_json(method="DELETE", url=del_url, headers=headers)
    if st2 != 200:
        return {"ok": False, "http_status": st2, "edit_id": edit_id, "tracks_error": tracks}
    return {
        "ok": True,
        "package_name": package_name,
        "edit_insert_ok": True,
        "production_track_snapshot": tracks,
        "note": "Edit discarded; no new release was committed. Use bundletool / Play upload for artifacts.",
    }


def play_upload_aab_and_commit_production(
    *,
    package_name: str,
    aab_path: Path,
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    """Upload ``.aab`` to a new edit and rolls out to the production track (full rollout)."""

    _ = (tenant_id, repo_id)
    if provider_dry_run() or not execute_providers_requested():
        return {"ok": True, "dry_run": True, "aab_path": str(aab_path)}
    if not aab_path.is_file():
        return {"ok": False, "error": f"bundle not found: {aab_path}"}

    token = _google_access_token(scopes=["https://www.googleapis.com/auth/androidpublisher"])
    pkg_q = quote(package_name, safe="")
    st, data = http_request_json(
        method="POST",
        url=f"{PLAY_API_BASE}/applications/{pkg_q}/edits",
        headers={"Authorization": f"Bearer {token}"},
        body={},
    )
    if st not in {200, 201} or not isinstance(data, dict):
        return {"ok": False, "phase": "edits.insert", "http_status": st, "error": data}
    edit_id = data.get("id")
    if not isinstance(edit_id, str):
        return {"ok": False, "phase": "edits.insert", "error": "missing edit id"}

    bundle_url = f"{PLAY_UPLOAD_BASE}/applications/{pkg_q}/edits/{quote(edit_id, safe='')}/bundles?uploadType=media"
    st_u, up_data = _upload_binary(
        upload_url=bundle_url,
        headers_base={"Authorization": f"Bearer {token}"},
        file_path=aab_path,
        mime="application/octet-stream",
    )
    if st_u not in {200, 201}:
        http_request_json(
            method="DELETE",
            url=f"{PLAY_API_BASE}/applications/{pkg_q}/edits/{quote(edit_id, safe='')}",
            headers={"Authorization": f"Bearer {token}"},
        )
        return {"ok": False, "phase": "bundles.upload", "http_status": st_u, "error": up_data}

    version_code: int | None = None
    if isinstance(up_data, dict):
        vc = up_data.get("versionCode")
        if isinstance(vc, int):
            version_code = vc

    track_body = {
        "track": "production",
        "releases": [
            {
                "versionCodes": [version_code] if version_code is not None else [],
                "status": "completed",
            },
        ],
    }
    if version_code is None:
        http_request_json(
            method="DELETE",
            url=f"{PLAY_API_BASE}/applications/{pkg_q}/edits/{quote(edit_id, safe='')}",
            headers={"Authorization": f"Bearer {token}"},
        )
        return {
            "ok": False,
            "phase": "bundles.upload",
            "error": "versionCode missing from upload response",
            "raw": up_data,
        }

    tr_url = f"{PLAY_API_BASE}/applications/{pkg_q}/edits/{quote(edit_id, safe='')}/tracks/production"
    st_t, tr_data = http_request_json(
        method="PUT",
        url=tr_url,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        body=track_body,
    )
    if st_t != 200:
        http_request_json(
            method="DELETE",
            url=f"{PLAY_API_BASE}/applications/{pkg_q}/edits/{quote(edit_id, safe='')}",
            headers={"Authorization": f"Bearer {token}"},
        )
        return {"ok": False, "phase": "tracks.update", "http_status": st_t, "error": tr_data}

    cm_url = f"{PLAY_API_BASE}/applications/{pkg_q}/edits/{quote(edit_id, safe='')}:commit"
    st_c, cm_data = http_request_json(
        method="POST",
        url=cm_url,
        headers={"Authorization": f"Bearer {token}"},
        body={},
    )
    if st_c not in {200, 201}:
        return {"ok": False, "phase": "edits.commit", "http_status": st_c, "error": cm_data}
    return {
        "ok": True,
        "package_name": package_name,
        "edit_id": edit_id,
        "version_code": version_code,
        "commit": cm_data,
    }


def _upload_binary(
    *,
    upload_url: str,
    headers_base: dict[str, str],
    file_path: Path,
    mime: str,
) -> tuple[int, Any]:
    payload = file_path.read_bytes()
    headers = {**headers_base, "Content-Type": mime, "Content-Length": str(len(payload))}
    req = urllib.request.Request(upload_url, data=payload, headers=headers, method="POST")
    ctx = ssl.create_default_context()
    try:
        with urllib.request.urlopen(req, timeout=600.0, context=ctx) as resp:
            return int(resp.getcode() or 200), _read_json_response(resp)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        try:
            parsed = json.loads(raw) if raw.strip() else {"error": exc.reason}
        except json.JSONDecodeError:
            parsed = {"_raw": raw}
        return int(exc.code), parsed


# --- App Store (live): IPA upload via altool; review/release via App Store Connect UI or future API ---


def asc_upload_ipa_to_app_store_connect(
    *,
    ipa_path: Path,
    tenant_id: str,
    repo_id: str,
) -> dict[str, Any]:
    """Upload ``.ipa`` to App Store Connect using ``xcrun altool`` (macOS + Xcode CLT).

    ``altool`` resolves the ``AuthKey_<key_id>.p8`` file via ``API_PRIVATE_KEYS_DIR`` (temporary
    directory with a symlink/copy of the operator key). Team API keys are required (not individual
    keys) per Apple ``altool`` constraints.

    Tenant isolation: ``tenant_id`` / ``repo_id`` are audit context only.
    """

    _ = (tenant_id, repo_id)
    if provider_dry_run() or not execute_providers_requested():
        return {"ok": True, "dry_run": True, "ipa_path": str(ipa_path)}
    resolved = ipa_path.expanduser()
    if not resolved.is_file():
        return {"ok": False, "error": f"IPA not found: {resolved}"}
    if sys.platform != "darwin":
        return {
            "ok": False,
            "blocked": True,
            "error": (
                "iOS App Store IPA upload requires macOS with Xcode Command Line Tools (xcrun altool). "
                "Run distribution on a Mac build host, or set AKC_DELIVERY_PROVIDER_DRY_RUN=1 / "
                "AKC_DELIVERY_EXECUTE_PROVIDERS=0 for local simulation."
            ),
        }
    try:
        key_id, issuer_id, key_file = _asc_api_key_parts()
    except RuntimeError as exc:
        return {"ok": False, "error": str(exc)}

    tmp_dir = tempfile.mkdtemp(prefix="akc-asc-private-keys-")
    try:
        key_link = Path(tmp_dir) / f"AuthKey_{key_id}.p8"
        try:
            key_link.symlink_to(key_file.resolve())
        except OSError:
            shutil.copy2(key_file, key_link)
        env = {**os.environ, "API_PRIVATE_KEYS_DIR": tmp_dir}
        cmd = [
            "xcrun",
            "altool",
            "--upload-app",
            "--file",
            str(resolved.resolve()),
            "--type",
            "ios",
            "--apiKey",
            key_id,
            "--apiIssuer",
            issuer_id,
        ]
        proc = subprocess.run(
            cmd,
            env=env,
            capture_output=True,
            text=True,
            timeout=3600.0,
            check=False,
        )
        combined = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
        if proc.returncode != 0:
            return {
                "ok": False,
                "error": f"altool upload failed (exit {proc.returncode})",
                "altool_output": combined[:8000],
            }
        return {"ok": True, "ipa_path": str(resolved), "altool_output": combined[:4000]}
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def asc_verify_api_token(tenant_id: str, repo_id: str) -> dict[str, Any]:
    """Lightweight call to prove API credentials (lists visible apps page 1)."""

    _ = (tenant_id, repo_id)
    if provider_dry_run() or not execute_providers_requested():
        return {"ok": True, "dry_run": True}
    token = _asc_jwt_token()
    st, data = http_request_json(
        method="GET",
        url=f"{ASC_API_BASE}/v1/apps?limit=5",
        headers={"Authorization": f"Bearer {token}"},
    )
    if st != 200:
        return {"ok": False, "http_status": st, "error": data}
    return {"ok": True, "http_status": st, "sample": data}
