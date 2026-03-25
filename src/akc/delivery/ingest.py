"""Plain-language delivery request ingestion (structured fields; recipients stay explicit)."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Final, Literal, TypeAlias, cast

ReleaseMode: TypeAlias = Literal["beta", "store", "both"]
ReleaseLane: TypeAlias = Literal["beta", "store"]

_PLATFORM_TOKEN_RE = re.compile(
    r"\b(web|ios|iphone|ipad|android|play store|app store|testflight|firebase)\b",
    re.IGNORECASE,
)
_RECIPIENT_BOILERPLATE_RE = re.compile(
    r"(?i)\b(send|ship|deliver|distribute)\s+(it|this|the app)\s+to\b.*$",
)
_SEND_TO_TAIL_RE = re.compile(
    r"(?i)\b(and\s+)?send\s+(it\s+)?to\s+these\s+\d+\s+users\s*$",
)

# Operator-supplied answers (local-only; never committed with secrets). Satisfies probes when present.
_OPERATOR_MANIFEST_REL: Final[str] = ".akc/delivery/operator_prereqs.json"


def load_recipients_from_file(path: Path) -> list[str]:
    """Load recipient emails from JSON (``recipients`` / ``emails`` array) or one address per line."""

    raw_path = path.expanduser().resolve()
    if not raw_path.is_file():
        raise ValueError(f"recipients file not found: {raw_path}")
    suffix = raw_path.suffix.lower()
    if suffix == ".json":
        loaded = json.loads(raw_path.read_text(encoding="utf-8"))
        if not isinstance(loaded, dict):
            raise ValueError("recipients JSON must be an object")
        keys = ("recipients", "emails")
        arr: list[Any] | None = None
        for k in keys:
            v = loaded.get(k)
            if isinstance(v, list):
                arr = v
                break
        if arr is None:
            raise ValueError(f"recipients JSON must contain one of {keys} as a non-empty array")
        return [str(x).strip() for x in arr if str(x).strip()]
    lines_out: list[str] = []
    for line in raw_path.read_text(encoding="utf-8").splitlines():
        s = line.split("#", 1)[0].strip()
        if not s:
            continue
        lines_out.append(s)
    return lines_out


def extract_app_goal(request_text: str) -> str:
    """Strip common recipient/distribution boilerplate; keep the build/product intent."""

    s = " ".join(request_text.strip().split())
    if not s:
        return "Deliver application"
    s = _SEND_TO_TAIL_RE.sub("", s).strip()
    s = _RECIPIENT_BOILERPLATE_RE.sub("", s).strip()
    s = " ".join(s.split())
    return s if s else request_text.strip()


def platform_keywords_in_text(request_text: str) -> list[str]:
    """Non-authoritative platform mentions for diagnostics (does not select targets)."""

    found: set[str] = set()
    for m in _PLATFORM_TOKEN_RE.finditer(request_text):
        tok = m.group(1).lower()
        if tok in {"web"}:
            found.add("web")
        elif tok in {"ios", "iphone", "ipad", "testflight", "app store"}:
            found.add("ios")
        elif tok in {"android", "play store", "firebase"}:
            found.add("android")
    return sorted(found)


def release_lanes_for_mode(release_mode: ReleaseMode) -> list[ReleaseLane]:
    if release_mode == "beta":
        return ["beta"]
    if release_mode == "store":
        return ["store"]
    return ["beta", "store"]


def build_parsed_delivery_fields(
    *,
    request_text: str,
    cli_platforms: Sequence[str],
    release_mode: ReleaseMode,
    authoritative_recipients: Sequence[str],
) -> dict[str, Any]:
    """Structured parse of the operator request (CLI/file win for platforms, mode, recipients)."""

    mentions = platform_keywords_in_text(request_text)
    warnings: list[str] = []
    cli_set = set(cli_platforms)
    mention_set = set(mentions)
    if mention_set and not mention_set.issubset(cli_set):
        extra = sorted(mention_set - cli_set)
        if extra:
            warnings.append(
                f"request text mentions platform(s) {extra} but --platforms is authoritative; "
                "targets were not expanded from free text",
            )

    return {
        "app_goal": extract_app_goal(request_text),
        "requested_platforms": list(cli_platforms),
        "delivery_mode": release_mode,
        "recipient_set": list(authoritative_recipients),
        "release_lanes": release_lanes_for_mode(release_mode),
        "request_mentions_platforms": mentions,
        "warnings": warnings,
    }


def _load_operator_manifest(project_dir: Path) -> dict[str, Any]:
    p = (project_dir / _OPERATOR_MANIFEST_REL).resolve()
    if not p.is_file():
        return {}
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def load_operator_prereqs_manifest(project_dir: Path) -> dict[str, Any]:
    """Load ``.akc/delivery/operator_prereqs.json`` when present (local-only operator signals)."""

    return _load_operator_manifest(project_dir)


def _expo_doc(project_dir: Path) -> dict[str, Any] | None:
    for name in ("app.json", "app.config.json"):
        fp = project_dir / name
        if not fp.is_file():
            continue
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict):
            return data
    return None


def _expo_sub(doc: dict[str, Any], *keys: str) -> Any:
    cur: Any = doc
    for k in keys:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def probe_ios_bundle_id(project_dir: Path) -> str | None:
    doc = _expo_doc(project_dir)
    if not doc:
        return None
    expo = doc.get("expo")
    ios = _expo_sub(cast(dict[str, Any], expo) if isinstance(expo, dict) else doc, "ios")
    if isinstance(ios, dict):
        bid = ios.get("bundleIdentifier")
        if isinstance(bid, str) and bid.strip():
            return bid.strip()
    return None


def probe_android_application_id(project_dir: Path) -> str | None:
    doc = _expo_doc(project_dir)
    if doc:
        expo = doc.get("expo")
        android = _expo_sub(cast(dict[str, Any], expo) if isinstance(expo, dict) else doc, "android")
        if isinstance(android, dict):
            pkg = android.get("package")
            if isinstance(pkg, str) and pkg.strip():
                return pkg.strip()
    gradle = project_dir / "android" / "app" / "build.gradle"
    if gradle.is_file():
        try:
            text = gradle.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return None
        m = re.search(r'applicationId\s+["\']([^"\']+)["\']', text)
        if m:
            return m.group(1).strip()
    return None


def probe_apple_team_id(project_dir: Path) -> str | None:
    eas = project_dir / "eas.json"
    if eas.is_file():
        try:
            data = json.loads(eas.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = None
        if isinstance(data, dict):
            submit = data.get("submit")
            if isinstance(submit, dict):
                prod = submit.get("production")
                if isinstance(prod, dict):
                    ios = prod.get("ios")
                    if isinstance(ios, dict):
                        tid = ios.get("appleTeamId")
                        if isinstance(tid, str) and tid.strip():
                            return tid.strip()
    return None


def probe_google_services_present(project_dir: Path) -> bool:
    cands = [
        project_dir / "android" / "app" / "google-services.json",
        project_dir / "google-services.json",
    ]
    return any(p.is_file() for p in cands)


def probe_ios_firebase_plist(project_dir: Path) -> bool:
    root = project_dir / "ios"
    if not root.is_dir():
        return False
    try:
        return any(p.is_file() for p in root.rglob("GoogleService-Info.plist"))
    except OSError:
        return False


def probe_firebase_project_config(project_dir: Path) -> bool:
    return (project_dir / "firebase.json").is_file() or (project_dir / ".firebaserc").is_file()


def probe_web_hosting_endpoint(project_dir: Path) -> str | None:
    for rel in (
        "vercel.json",
        "netlify.toml",
        "wrangler.toml",
        "fly.toml",
    ):
        if (project_dir / rel).is_file():
            return f"detected:{rel}"
    return None


def probe_ios_signing_hint(project_dir: Path) -> bool:
    cred = project_dir / "credentials.json"
    if cred.is_file():
        return True
    ios_dir = project_dir / "ios"
    if ios_dir.is_dir():
        for p in ios_dir.rglob("*.mobileprovision"):
            if p.is_file():
                return True
    return False


def probe_ios_xcode_or_eas_build(project_dir: Path) -> bool:
    """True when a local iOS project or EAS Build config suggests archive/export is possible."""

    ios_dir = project_dir / "ios"
    if ios_dir.is_dir():
        try:
            if any(ios_dir.rglob("*.xcodeproj")) or any(ios_dir.rglob("*.xcworkspace")):
                return True
        except OSError:
            pass
    eas = project_dir / "eas.json"
    if eas.is_file():
        try:
            data = json.loads(eas.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            data = None
        if isinstance(data, dict) and isinstance(data.get("build"), dict) and data["build"]:
            return True
    exp = project_dir / "app.json"
    if exp.is_file():
        try:
            dj = json.loads(exp.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            dj = None
        if isinstance(dj, dict) and isinstance(dj.get("expo"), dict):
            ios = cast(dict[str, Any], dj["expo"]).get("ios")
            if isinstance(ios, dict) and str(ios.get("bundleIdentifier") or "").strip():
                # Expo-managed workflow may use EAS without checked-in ios/ — operator flag still required elsewhere.
                return (project_dir / "eas.json").is_file() or probe_ios_signing_hint(project_dir)
    return False


def probe_android_gradle_android_project(project_dir: Path) -> bool:
    gradle = project_dir / "android" / "app" / "build.gradle"
    gradle_kts = project_dir / "android" / "app" / "build.gradle.kts"
    return gradle.is_file() or gradle_kts.is_file()


def infer_required_accounts_from_human_inputs(rows: Sequence[Mapping[str, Any]]) -> list[str]:
    """Map ``required_human_inputs`` rows to stable provider account labels (``request.required_accounts``)."""

    id_to_account: dict[str, str] = {
        "ios_bundle_id": "apple_developer",
        "apple_team_registration": "apple_developer",
        "ios_signing_assets": "apple_developer",
        "apple_app_store_registration": "app_store_connect",
        "firebase_ios_app_id": "firebase",
        "firebase_android_app_registration": "firebase",
        "google_play_app_registration": "google_play",
        "google_play_publisher_credentials": "google_play",
        "android_signing_assets": "google_play",
        "web_hosting_endpoint": "web_hosting",
    }
    seen: set[str] = set()
    out: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        hid = str(row.get("id") or "").strip()
        acct = id_to_account.get(hid)
        if acct and acct not in seen:
            seen.add(acct)
            out.append(acct)
    return out


def collect_prerequisite_human_inputs(
    *,
    project_dir: Path,
    platforms: Sequence[str],
    release_mode: ReleaseMode,
) -> list[dict[str, Any]]:
    """Emit ``required_human_inputs`` rows for missing bundle IDs, registrations, signing, and web hosting."""

    op = _load_operator_manifest(project_dir)
    lanes = release_lanes_for_mode(release_mode)
    need_beta = "beta" in lanes
    need_store = "store" in lanes

    def _op(path: tuple[str, ...]) -> bool:
        cur: Any = op
        for key in path:
            if not isinstance(cur, dict):
                return False
            cur = cur.get(key)
        return bool(cur)

    def _row(
        *,
        id_: str,
        ask_order: int,
        reason: str,
        title: str,
        question: str,
        value_kind: str,
        prop: str,
    ) -> dict[str, Any]:
        return {
            "id": id_,
            "status": "missing",
            "blocking_for": list(lanes),
            "reason": reason,
            "ask_order": ask_order,
            "context": {},
            "ui_prompt": {
                "audience": "operator",
                "title": title,
                "question": question,
                "help_text": "",
                "value_kind": value_kind,
                "sensitive": "secret" in id_ or "signing" in id_,
            },
            "answer_binding": {
                "kind": "ir_node_property",
                "property": prop,
                "scope": "repo",
            },
        }

    rows: list[dict[str, Any]] = []
    order = 0

    if "ios" in platforms:
        order += 1
        if not probe_ios_bundle_id(project_dir) and not _op(("ios", "bundle_id")):
            rows.append(
                _row(
                    id_="ios_bundle_id",
                    ask_order=order,
                    reason="iOS builds require a bundle identifier registered with Apple.",
                    title="iOS bundle identifier",
                    question="What bundle identifier should Apple builds use (e.g. com.example.app)?",
                    value_kind="bundle_id",
                    prop="delivery.ios.bundle_id",
                ),
            )
        order += 1
        if not probe_apple_team_id(project_dir) and not _op(("apple", "team_id")):
            rows.append(
                _row(
                    id_="apple_team_registration",
                    ask_order=order,
                    reason="TestFlight/App Store automation needs an Apple Developer team id.",
                    title="Apple Developer team",
                    question="What is your Apple Developer Program team id for this app?",
                    value_kind="apple_team_id",
                    prop="delivery.ios.apple_team_id",
                ),
            )
        if need_beta or need_store:
            order += 1
            if not probe_ios_signing_hint(project_dir) and not _op(("ios", "signing_assets")):
                rows.append(
                    _row(
                        id_="ios_signing_assets",
                        ask_order=order,
                        reason=(
                            "iOS beta/store lanes require signing certificates and provisioning profiles "
                            "(or EAS credentials)."
                        ),
                        title="iOS signing assets",
                        question=(
                            "Provide signing credentials (e.g. EAS credentials.json or CI signing setup) for this repo."
                        ),
                        value_kind="signing_profile_bundle",
                        prop="delivery.ios.signing_assets",
                    ),
                )
        if need_store:
            order += 1
            if not _op(("ios", "app_store_connect_app")):
                rows.append(
                    _row(
                        id_="apple_app_store_registration",
                        ask_order=order,
                        reason="App Store release requires an App Store Connect app record for this bundle id.",
                        title="App Store Connect app",
                        question="Confirm the App Store Connect app id / SKU exists for this bundle identifier.",
                        value_kind="app_store_app_id",
                        prop="delivery.ios.app_store_connect_app",
                    ),
                )
        if need_beta:
            order += 1
            if (
                not probe_ios_firebase_plist(project_dir)
                and not _op(("ios", "firebase_app_id"))
                and not probe_firebase_project_config(project_dir)
            ):
                rows.append(
                    _row(
                        id_="firebase_ios_app_id",
                        ask_order=order,
                        reason="When using Firebase on iOS, register the app and add GoogleService-Info.plist "
                        "(or firebase project config).",
                        title="Firebase iOS app",
                        question="Provide the Firebase iOS app id or add GoogleService-Info.plist / firebase.json.",
                        value_kind="firebase_app_id",
                        prop="delivery.ios.firebase_app_id",
                    ),
                )

    if "android" in platforms and need_beta:
        order += 1
        if (
            not probe_google_services_present(project_dir)
            and not _op(("android", "firebase_app_id"))
            and not probe_firebase_project_config(project_dir)
        ):
            rows.append(
                _row(
                    id_="firebase_android_app_registration",
                    ask_order=order,
                    reason=(
                        "Firebase App Distribution needs a Firebase Android app "
                        "(google-services.json / project config)."
                    ),
                    title="Firebase Android configuration",
                    question=(
                        "Add google-services.json (or firebase.json + .firebaserc) or provide the "
                        "Firebase Android app id."
                    ),
                    value_kind="firebase_app_id",
                    prop="delivery.android.firebase_app_id",
                ),
            )

    if "android" in platforms and need_store:
        order += 1
        if not probe_android_application_id(project_dir) and not _op(("android", "play_package")):
            rows.append(
                _row(
                    id_="google_play_app_registration",
                    ask_order=order,
                    reason="Play store releases require a registered package name / app in Google Play Console.",
                    title="Google Play package name",
                    question="What Android applicationId / package name is registered in Play Console?",
                    value_kind="android_package",
                    prop="delivery.android.play_application_id",
                ),
            )
        order += 1
        if not _op(("android", "play_publisher_api")):
            rows.append(
                _row(
                    id_="google_play_publisher_credentials",
                    ask_order=order,
                    reason=(
                        "Play production automation needs Google Play Developer API credentials "
                        "with release permissions."
                    ),
                    title="Play publisher API credentials",
                    question=("Provide a service account JSON (or CI secret reference) authorized for this Play app."),
                    value_kind="service_account_json",
                    prop="delivery.android.play_publisher_credentials",
                ),
            )
        order += 1
        if not _op(("android", "signing_keystore")):
            rows.append(
                _row(
                    id_="android_signing_assets",
                    ask_order=order,
                    reason="Play/App bundle uploads require a signing keystore or Play App Signing configuration.",
                    title="Android signing configuration",
                    question="Provide keystore / Play App Signing details used for release builds.",
                    value_kind="android_signing",
                    prop="delivery.android.signing_assets",
                ),
            )

    if "web" in platforms and (need_beta or need_store):
        order += 1
        if not probe_web_hosting_endpoint(project_dir) and not _op(("web", "hosting_endpoint")):
            rows.append(
                _row(
                    id_="web_hosting_endpoint",
                    ask_order=order,
                    reason="Web delivery needs a deployed base URL or a configured hosting destination.",
                    title="Web hosting endpoint",
                    question="What HTTPS base URL (or hosting provider config) should beta/store web invites target?",
                    value_kind="url",
                    prop="delivery.web.hosting_endpoint",
                ),
            )

    rows.sort(key=lambda r: int(r.get("ask_order", 0)))
    return rows
