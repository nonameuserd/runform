"""Distribution adapters and scheduling for named-recipient delivery (v1).

Adapters map (platform, release lane) → provider-specific beta or store flows:

- **Web** — recipient-specific HTTPS invite links delivered by email (hosted PWA/web app).
- **iOS beta** — TestFlight email invites (no enterprise/MDM/ad-hoc sideload in v1).
- **Android beta** — Firebase App Distribution testers/groups + email invites.
- **iOS store** — App Store Connect submission and live-release configuration.
- **Android store** — Google Play production-track promotion.

:class:`DistributionAdapter.preflight` always enforces tenant/repo isolation. **Provider
lane prerequisites** (fail-closed) run unless relaxed via
:envvar:`AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT`, or by setting legacy
:envvar:`AKC_DELIVERY_ENFORCE_ADAPTER_PREFLIGHT` to ``0``/``false``/``off``.
Checks use environment variables, ``.akc/delivery/operator_prereqs.json``, and repo
fingerprints only — preflight does not call external APIs. **Execution** (invites, Play
edits, Firebase ``distribute``) lives in :mod:`akc.delivery.distribution_dispatch` and
:mod:`akc.delivery.provider_clients` (optional ``akc[delivery-providers]``).

For ``release_mode="both"``, :func:`iter_distribution_jobs` enumerates **all beta
lanes across platforms before any store lane**, matching the intended sequence:
beta delivery → human readiness gate → store promotion (see
:data:`BOTH_MODE_DISTRIBUTION_PHASES`).
"""

from __future__ import annotations

import os
from abc import abstractmethod
from collections.abc import Iterator, Sequence
from pathlib import Path
from typing import Any, Final, cast

from akc.delivery.adapter import DistributionAdapter
from akc.delivery.ingest import (
    load_operator_prereqs_manifest,
    probe_android_application_id,
    probe_android_gradle_android_project,
    probe_apple_team_id,
    probe_firebase_project_config,
    probe_google_services_present,
    probe_ios_bundle_id,
    probe_ios_signing_hint,
    probe_ios_xcode_or_eas_build,
    probe_web_hosting_endpoint,
)
from akc.delivery.types import DeliveryPlatform, PlatformBuildSpec, ReleaseLane, ReleaseMode

_RELAX_PREFLIGHT_ENV: Final[str] = "AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT"
_LEGACY_ENFORCE_ENV: Final[str] = "AKC_DELIVERY_ENFORCE_ADAPTER_PREFLIGHT"

BOTH_MODE_DISTRIBUTION_PHASES: Final[tuple[str, ...]] = (
    "beta_delivery",
    "human_readiness_gate",
    "store_promotion",
)


def enforce_adapter_preflight() -> bool:
    """Return True when missing provider prerequisites should block the delivery session.

    Default is **True** (fail closed). Set :envvar:`AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT` to
    ``1``/``true`` to skip strict probes (local tests / sandbox). Legacy opt-out: set
    :envvar:`AKC_DELIVERY_ENFORCE_ADAPTER_PREFLIGHT` to ``0``/``false``/``off``.
    """

    relax = os.environ.get(_RELAX_PREFLIGHT_ENV, "").strip().lower()
    if relax in {"1", "true", "yes", "on"}:
        return False
    legacy = os.environ.get(_LEGACY_ENFORCE_ENV, "").strip().lower()
    return legacy not in {"0", "false", "no", "off"}


def _validate_scope(*, tenant_id: str, repo_id: str, spec: PlatformBuildSpec) -> list[str]:
    if tenant_id.strip() != tenant_id or not tenant_id.strip():
        return ["invalid tenant_id for scoped preflight"]
    if repo_id.strip() != repo_id or not repo_id.strip():
        return ["invalid repo_id for scoped preflight"]
    if spec.tenant_id != tenant_id or spec.repo_id != repo_id:
        return ["PlatformBuildSpec tenant/repo mismatch (isolation check failed)"]
    return []


def _op_flag(op: dict[str, Any], path: tuple[str, ...]) -> bool:
    cur: Any = op
    for key in path:
        if not isinstance(cur, dict):
            return False
        cur = cur.get(key)
    return bool(cur)


def _env_strip(name: str) -> str:
    return str(os.environ.get(name, "") or "").strip()


def _metadata_str(spec: PlatformBuildSpec, key: str) -> str:
    raw = spec.metadata.get(key)
    return str(raw).strip() if raw is not None else ""


def _gac_path_configured() -> bool:
    p = _env_strip("GOOGLE_APPLICATION_CREDENTIALS")
    if not p:
        return False
    try:
        return Path(p).expanduser().is_file()
    except OSError:
        return False


def _firebase_cli_auth_hint() -> bool:
    return bool(_env_strip("FIREBASE_TOKEN") or _gac_path_configured())


def _firebase_app_distribution_groups_hint(*, op: dict[str, Any]) -> bool:
    if _op_flag(op, ("android", "firebase_app_distribution_groups")):
        return True
    if _env_strip("AKC_DELIVERY_FIREBASE_APP_DIST_GROUPS"):
        return True
    return bool(_env_strip("FIREBASE_APP_DISTRIBUTION_GROUPS"))


def _web_invite_base_resolved(*, project_dir: Path, spec: PlatformBuildSpec, op: dict[str, Any]) -> bool:
    if _metadata_str(spec, "web_invite_base_url"):
        return True
    if _env_strip("AKC_DELIVERY_WEB_INVITE_BASE_URL"):
        return True
    if probe_web_hosting_endpoint(project_dir):
        return True
    return bool(_op_flag(op, ("web", "hosting_endpoint")) or _op_flag(op, ("web", "invite_base_url")))


def _web_invite_email_resolved(*, op: dict[str, Any]) -> bool:
    if _op_flag(op, ("web", "invite_email_configured")):
        return True
    if _env_strip("AKC_DELIVERY_INVITE_EMAIL_FROM"):
        return True
    if _env_strip("SENDGRID_API_KEY"):
        return True
    if _env_strip("POSTMARK_API_TOKEN"):
        return True
    if _env_strip("AKC_DELIVERY_SMTP_URL") or (
        _env_strip("SMTP_HOST") and _env_strip("SMTP_USER") and _env_strip("SMTP_PASS")
    ):
        return True
    return bool(
        _env_strip("AWS_SES_REGION") and (_env_strip("AWS_ACCESS_KEY_ID") or _env_strip("AWS_PROFILE")),
    )


def _app_store_connect_api_resolved(*, op: dict[str, Any]) -> bool:
    if _op_flag(op, ("ios", "app_store_connect_api")):
        return True
    if _env_strip("APP_STORE_CONNECT_API_KEY_ID") and _env_strip("APP_STORE_CONNECT_API_ISSUER_ID"):
        return True
    if _env_strip("APP_STORE_CONNECT_KEY_ID") and _env_strip("APP_STORE_CONNECT_ISSUER_ID"):
        return True
    key_path = _env_strip("APP_STORE_CONNECT_PRIVATE_KEY_PATH") or _env_strip(
        "APP_STORE_CONNECT_API_KEY_PATH",
    )
    if key_path:
        try:
            return Path(key_path).expanduser().is_file()
        except OSError:
            return False
    return False


def _ios_beta_prereqs_met(*, project_dir: Path, op: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if not probe_ios_bundle_id(project_dir) and not _op_flag(op, ("ios", "bundle_id")):
        issues.append(
            "Apple / iOS lane: bundle identifier missing (expected expo/ios config or operator_prereqs ios.bundle_id)",
        )
    if not probe_apple_team_id(project_dir) and not _op_flag(op, ("apple", "team_id")):
        issues.append(
            "Apple / iOS lane: Apple Developer team id missing "
            "(expected eas.json submit.production.ios.appleTeamId or operator_prereqs apple.team_id)",
        )
    if not probe_ios_xcode_or_eas_build(project_dir) and not _op_flag(
        op,
        ("ios", "xcode_or_eas_export"),
    ):
        issues.append(
            "Apple / iOS lane: Xcode workspace / xcworkspace or EAS Build config not detected "
            "(check in ios/*.xcodeproj, ios/*.xcworkspace, eas.json build profiles, or "
            "operator_prereqs ios.xcode_or_eas_export)",
        )
    if not probe_ios_signing_hint(project_dir) and not _op_flag(op, ("ios", "signing_assets")):
        issues.append(
            "Apple / iOS lane: signing assets not detected for archive/export "
            "(credentials.json, *.mobileprovision, or operator_prereqs ios.signing_assets)",
        )
    if not _app_store_connect_api_resolved(op=op):
        issues.append(
            "Apple / iOS lane: App Store Connect API auth not configured "
            "(issuer + key id + private key path env vars, or operator_prereqs ios.app_store_connect_api)",
        )
    return issues


def _ios_asc_app_registration_issues(*, op: dict[str, Any], context: str) -> list[str]:
    if _op_flag(op, ("ios", "app_store_connect_app")):
        return []
    return [
        f"Apple / iOS ({context}): App Store Connect app registration not acknowledged "
        "(operator_prereqs ios.app_store_connect_app)",
    ]


def _android_firebase_app_signal(*, project_dir: Path, op: dict[str, Any]) -> bool:
    return bool(
        probe_google_services_present(project_dir)
        or probe_firebase_project_config(project_dir)
        or _op_flag(op, ("android", "firebase_app_id")),
    )


class _ConfiguredDistributionAdapter(DistributionAdapter):
    """Isolation checks always; strict probes when :func:`enforce_adapter_preflight` is on."""

    def preflight(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
    ) -> list[str]:
        scoped = _validate_scope(tenant_id=tenant_id, repo_id=repo_id, spec=spec)
        if scoped:
            return scoped
        if not enforce_adapter_preflight():
            return []
        return self._strict_issues(project_dir=project_dir.resolve(), spec=spec)

    @abstractmethod
    def _strict_issues(self, *, project_dir: Path, spec: PlatformBuildSpec) -> list[str]:
        raise NotImplementedError


class WebInviteAdapter(_ConfiguredDistributionAdapter):
    """Web / PWA beta and store: recipient-specific invite URLs sent by email."""

    @property
    def kind(self) -> str:
        return "web_invite"

    def supported_platforms(self) -> frozenset[DeliveryPlatform]:
        return frozenset({"web"})

    def supported_lanes(self) -> frozenset[ReleaseLane]:
        return frozenset({"beta", "store"})

    def _strict_issues(self, *, project_dir: Path, spec: PlatformBuildSpec) -> list[str]:
        op = load_operator_prereqs_manifest(project_dir)
        issues: list[str] = []
        if not _web_invite_base_resolved(project_dir=project_dir, spec=spec, op=op):
            issues.append(
                "web distribution: invite base URL/hosting signal missing "
                "(AKC_DELIVERY_WEB_INVITE_BASE_URL, hosting provider config in repo, "
                "operator_prereqs web.hosting_endpoint / web.invite_base_url, "
                "or PlatformBuildSpec.metadata web_invite_base_url)",
            )
        if not _web_invite_email_resolved(op=op):
            issues.append(
                "web distribution: outbound email transport not configured "
                "(operator_prereqs web.invite_email_configured, "
                "AKC_DELIVERY_INVITE_EMAIL_FROM + SMTP/SendGrid/Postmark/SES env as applicable)",
            )
        return issues


class TestFlightAdapter(_ConfiguredDistributionAdapter):
    """iOS beta: TestFlight external testing invites by email (no sideload/MDM v1)."""

    @property
    def kind(self) -> str:
        return "testflight"

    def supported_platforms(self) -> frozenset[DeliveryPlatform]:
        return frozenset({"ios"})

    def supported_lanes(self) -> frozenset[ReleaseLane]:
        return frozenset({"beta"})

    def _strict_issues(self, *, project_dir: Path, spec: PlatformBuildSpec) -> list[str]:
        _ = spec.platform
        op = load_operator_prereqs_manifest(project_dir)
        return [
            *_ios_beta_prereqs_met(project_dir=project_dir, op=op),
            *_ios_asc_app_registration_issues(op=op, context="TestFlight beta"),
        ]


class FirebaseAppDistributionAdapter(_ConfiguredDistributionAdapter):
    """Android beta: Firebase App Distribution tester/group invites by email."""

    @property
    def kind(self) -> str:
        return "firebase_app_distribution"

    def supported_platforms(self) -> frozenset[DeliveryPlatform]:
        return frozenset({"android"})

    def supported_lanes(self) -> frozenset[ReleaseLane]:
        return frozenset({"beta"})

    def _strict_issues(self, *, project_dir: Path, spec: PlatformBuildSpec) -> list[str]:
        _ = spec.platform
        op = load_operator_prereqs_manifest(project_dir)
        issues: list[str] = []
        if not _android_firebase_app_signal(project_dir=project_dir, op=op):
            issues.append(
                "Android beta (Firebase App Distribution): Firebase Android app signal missing "
                "(google-services.json / firebase.json, or operator_prereqs android.firebase_app_id)",
            )
        if not _op_flag(op, ("android", "firebase_distribution_auth")) and not _firebase_cli_auth_hint():
            issues.append(
                "Android beta (Firebase App Distribution): Firebase CLI/API auth not configured "
                "(FIREBASE_TOKEN, GOOGLE_APPLICATION_CREDENTIALS, or "
                "operator_prereqs android.firebase_distribution_auth)",
            )
        if not _firebase_app_distribution_groups_hint(op=op):
            issues.append(
                "Android beta (Firebase App Distribution): tester group / allowlist not configured "
                "(operator_prereqs android.firebase_app_distribution_groups, or "
                "AKC_DELIVERY_FIREBASE_APP_DIST_GROUPS / FIREBASE_APP_DISTRIBUTION_GROUPS)",
            )
        return issues


class AppStoreReleaseAdapter(_ConfiguredDistributionAdapter):
    """iOS store: App Store Connect submission and live-release (production)."""

    @property
    def kind(self) -> str:
        return "app_store_release"

    def supported_platforms(self) -> frozenset[DeliveryPlatform]:
        return frozenset({"ios"})

    def supported_lanes(self) -> frozenset[ReleaseLane]:
        return frozenset({"store"})

    def _strict_issues(self, *, project_dir: Path, spec: PlatformBuildSpec) -> list[str]:
        _ = spec.platform
        op = load_operator_prereqs_manifest(project_dir)
        return [
            *_ios_beta_prereqs_met(project_dir=project_dir, op=op),
            *_ios_asc_app_registration_issues(op=op, context="App Store release"),
        ]


class GooglePlayReleaseAdapter(_ConfiguredDistributionAdapter):
    """Android store: Google Play production-track promotion."""

    @property
    def kind(self) -> str:
        return "google_play_release"

    def supported_platforms(self) -> frozenset[DeliveryPlatform]:
        return frozenset({"android"})

    def supported_lanes(self) -> frozenset[ReleaseLane]:
        return frozenset({"store"})

    def _strict_issues(self, *, project_dir: Path, spec: PlatformBuildSpec) -> list[str]:
        _ = spec.platform
        op = load_operator_prereqs_manifest(project_dir)
        issues: list[str] = []
        if not probe_android_application_id(project_dir) and not _op_flag(op, ("android", "play_package")):
            issues.append(
                "Android store: applicationId / package not found "
                "(expo `android.package`, Gradle applicationId, or operator_prereqs android.play_package)",
            )
        if not _op_flag(op, ("android", "play_publisher_api")):
            issues.append(
                "Google Play store lane: Play Developer Publishing API credentials not acknowledged "
                "(operator_prereqs android.play_publisher_api)",
            )
        if not probe_android_gradle_android_project(project_dir) and not _op_flag(
            op,
            ("android", "gradle_project"),
        ):
            issues.append(
                "Google Play store lane: Android Gradle project not detected "
                "(android/app/build.gradle[.kts], or operator_prereqs android.gradle_project)",
            )
        if not _op_flag(op, ("android", "signing_keystore")):
            issues.append(
                "Google Play store lane: release signing config not acknowledged for uploaded artifacts "
                "(operator_prereqs android.signing_keystore)",
            )
        return issues


_WEB_INVITE = WebInviteAdapter()
_TESTFLIGHT = TestFlightAdapter()
_FIREBASE = FirebaseAppDistributionAdapter()
_APP_STORE = AppStoreReleaseAdapter()
_GOOGLE_PLAY = GooglePlayReleaseAdapter()

_ADAPTER_MATRIX: Final[dict[tuple[DeliveryPlatform, ReleaseLane], DistributionAdapter]] = {
    ("web", "beta"): _WEB_INVITE,
    ("web", "store"): _WEB_INVITE,
    ("ios", "beta"): _TESTFLIGHT,
    ("ios", "store"): _APP_STORE,
    ("android", "beta"): _FIREBASE,
    ("android", "store"): _GOOGLE_PLAY,
}

ALL_STUB_ADAPTERS: Final[tuple[DistributionAdapter, ...]] = (
    _WEB_INVITE,
    _TESTFLIGHT,
    _FIREBASE,
    _APP_STORE,
    _GOOGLE_PLAY,
)


def release_lanes_for_mode(release_mode: ReleaseMode) -> tuple[ReleaseLane, ...]:
    if release_mode == "beta":
        return ("beta",)
    if release_mode == "store":
        return ("store",)
    return ("beta", "store")


def iter_distribution_jobs(
    *,
    platforms: Sequence[str],
    release_mode: ReleaseMode,
) -> Iterator[tuple[DeliveryPlatform, ReleaseLane, DistributionAdapter]]:
    """Yield ``(platform, lane, adapter)`` for scheduled distribution.

    Lanes are outer-most so that for ``release_mode="both"`` all **beta** jobs run
    across platforms before any **store** job, aligning with beta → human gate →
    store promotion.
    """

    lanes = release_lanes_for_mode(release_mode)
    for lane in lanes:
        for plat_raw in platforms:
            if plat_raw not in ("web", "ios", "android"):
                continue
            platform = cast(DeliveryPlatform, plat_raw)
            adapter = _ADAPTER_MATRIX.get((platform, lane))
            if adapter is None:
                continue
            yield platform, lane, adapter


def collect_distribution_preflight_issues(
    *,
    project_dir: Path,
    tenant_id: str,
    repo_id: str,
    delivery_id: str,
    delivery_version: str,
    platforms: Sequence[str],
    release_mode: ReleaseMode,
) -> list[dict[str, Any]]:
    """Run :meth:`DistributionAdapter.preflight` for each scheduled lane; return issue rows."""

    issues: list[dict[str, Any]] = []
    for platform, lane, adapter in iter_distribution_jobs(
        platforms=platforms,
        release_mode=release_mode,
    ):
        spec = PlatformBuildSpec(
            tenant_id=tenant_id,
            repo_id=repo_id,
            delivery_id=delivery_id,
            platform=platform,
            delivery_version=delivery_version,
            release_lanes=(lane,),
        )
        for reason in adapter.preflight(
            project_dir=project_dir,
            tenant_id=tenant_id,
            repo_id=repo_id,
            spec=spec,
        ):
            issues.append(
                {
                    "platform": platform,
                    "lane": lane,
                    "adapter_kind": adapter.kind,
                    "reason": reason,
                },
            )
    return issues
