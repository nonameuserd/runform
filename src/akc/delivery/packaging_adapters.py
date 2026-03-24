"""Concrete packaging lanes: ``web_bundle``, ``ios_build``, ``android_build`` (v1 stubs)."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Final, cast

from akc.delivery.packaging_adapter import PackagingAdapter, PackagingResult
from akc.delivery.types import DeliveryPlatform, PlatformBuildSpec, ReleaseMode
from akc.delivery.versioning import PlatformProviderVersions

_PREFLIGHT_STRICT_ENV: Final[str] = "AKC_PACKAGING_ENFORCE_PREFLIGHT"
_TRUTHY: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})
_FALSY: Final[frozenset[str]] = frozenset({"0", "false", "no", "off"})


def enforce_packaging_preflight(*, release_mode: ReleaseMode | str | None = None) -> bool:
    """Return True when stub adapters should report blocking packaging preflight failures.

    Environment override:
    - ``AKC_PACKAGING_ENFORCE_PREFLIGHT=1|true|yes|on`` → force strict
    - ``AKC_PACKAGING_ENFORCE_PREFLIGHT=0|false|no|off`` → force relaxed

    Default behavior:
    - ``store`` / ``both`` are strict by default (fail-closed for release lanes).
    - ``beta`` defaults relaxed to preserve local iteration on v1 stubs.
    """

    v = os.environ.get(_PREFLIGHT_STRICT_ENV, "").strip().lower()
    if v in _TRUTHY:
        return True
    if v in _FALSY:
        return False
    rm = str(release_mode or "beta").strip().lower()
    return rm in {"store", "both"}


def _release_mode_for_spec(spec: PlatformBuildSpec) -> ReleaseMode:
    lanes = tuple(str(x).strip().lower() for x in spec.release_lanes if str(x).strip())
    lane_set = set(lanes)
    if lane_set == {"store"}:
        return "store"
    if lane_set == {"beta"}:
        return "beta"
    return "both"


class BaseStubPackagingAdapter(PackagingAdapter):
    """Stub base with strict preflight for store-capable release modes."""

    _strict_reason: str = "packaging: adapter not configured (v1 stub)"

    def preflight(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
    ) -> list[str]:
        if tenant_id.strip() != tenant_id or not tenant_id.strip():
            return ["invalid tenant_id for scoped packaging preflight"]
        if repo_id.strip() != repo_id or not repo_id.strip():
            return ["invalid repo_id for scoped packaging preflight"]
        if spec.tenant_id != tenant_id or spec.repo_id != repo_id:
            return ["PlatformBuildSpec tenant/repo mismatch (isolation check failed)"]
        _ = project_dir
        if not enforce_packaging_preflight(release_mode=_release_mode_for_spec(spec)):
            return []
        return [self._strict_reason]


class WebBundlePackagingAdapter(BaseStubPackagingAdapter):
    """Deployable web / PWA output; URL binding happens in distribution/runtime substrate."""

    _strict_reason = (
        "web packaging: deploy target and static hosting binding are not configured (v1 stub)"
    )

    @property
    def lane(self) -> str:
        return "web_bundle"

    @property
    def platform(self) -> DeliveryPlatform:
        return "web"

    def package(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
        compile_run_id: str | None,
        provider_versions: PlatformProviderVersions,
    ) -> PackagingResult:
        _ = (tenant_id, repo_id)
        root = project_dir.resolve()
        staging = root / ".akc" / "delivery" / spec.delivery_id / "packaging" / "web"
        deployed_url: str | None = None
        raw_meta = spec.metadata.get("web_invite_base_url")
        if isinstance(raw_meta, str) and raw_meta.strip():
            deployed_url = raw_meta.strip()
        return PackagingResult(
            ok=True,
            lane=self.lane,
            error=None,
            outputs={
                "app_stack": "react_expo_default",
                "compile_run_id": compile_run_id,
                "staging_dir": str(staging),
                "deployed_url": deployed_url,
                "delivery_plan_bound": bool((spec.metadata.get("compile_handoff") or {}).get("delivery_plan_loaded")),
                "provider_versions": {
                    "delivery_version": provider_versions.delivery_version,
                    "web_pwa_version": provider_versions.web_pwa_version,
                },
            },
        )


class IosBuildPackagingAdapter(BaseStubPackagingAdapter):
    """Archive, sign, export; upload is handled by distribution adapters (TestFlight / ASC)."""

    _strict_reason = (
        "iOS packaging: Xcode workspace, signing, and export options are not configured (v1 stub)"
    )

    @property
    def lane(self) -> str:
        return "ios_build"

    @property
    def platform(self) -> DeliveryPlatform:
        return "ios"

    def package(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
        compile_run_id: str | None,
        provider_versions: PlatformProviderVersions,
    ) -> PackagingResult:
        _ = (tenant_id, repo_id)
        root = project_dir.resolve()
        staging = root / ".akc" / "delivery" / spec.delivery_id / "packaging" / "ios"
        return PackagingResult(
            ok=True,
            lane=self.lane,
            error=None,
            outputs={
                "app_stack": "react_expo_default",
                "compile_run_id": compile_run_id,
                "staging_dir": str(staging),
                "ipa_path": None,
                "provider_versions": {
                    "delivery_version": provider_versions.delivery_version,
                    "ios_marketing_version": provider_versions.ios_marketing_version,
                    "ios_build_number": provider_versions.ios_build_number,
                },
            },
        )


class AndroidBuildPackagingAdapter(BaseStubPackagingAdapter):
    """Sign APK/AAB; upload targets Firebase App Distribution / Play in distribution phase."""

    _strict_reason = (
        "Android packaging: Gradle bundle, signing, and artifact outputs are not configured (v1 stub)"
    )

    @property
    def lane(self) -> str:
        return "android_build"

    @property
    def platform(self) -> DeliveryPlatform:
        return "android"

    def package(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
        compile_run_id: str | None,
        provider_versions: PlatformProviderVersions,
    ) -> PackagingResult:
        _ = (tenant_id, repo_id)
        root = project_dir.resolve()
        staging = root / ".akc" / "delivery" / spec.delivery_id / "packaging" / "android"
        return PackagingResult(
            ok=True,
            lane=self.lane,
            error=None,
            outputs={
                "app_stack": "react_expo_default",
                "compile_run_id": compile_run_id,
                "staging_dir": str(staging),
                "aab_path": None,
                "apk_path": None,
                "provider_versions": {
                    "delivery_version": provider_versions.delivery_version,
                    "android_version_name": provider_versions.android_version_name,
                    "android_version_code": provider_versions.android_version_code,
                },
            },
        )


_WEB = WebBundlePackagingAdapter()
_IOS = IosBuildPackagingAdapter()
_ANDROID = AndroidBuildPackagingAdapter()

_PACKAGING_BY_PLATFORM: Final[dict[DeliveryPlatform, PackagingAdapter]] = {
    "web": _WEB,
    "ios": _IOS,
    "android": _ANDROID,
}

ALL_PACKAGING_ADAPTERS: Final[tuple[PackagingAdapter, ...]] = (_WEB, _IOS, _ANDROID)


def packaging_adapter_for(platform: DeliveryPlatform) -> PackagingAdapter:
    """Return the packaging lane for ``platform``."""

    return _PACKAGING_BY_PLATFORM[platform]


def collect_packaging_preflight_issues(
    *,
    project_dir: Path,
    tenant_id: str,
    repo_id: str,
    delivery_id: str,
    delivery_version: str,
    platforms: list[str],
    release_mode: str,
) -> list[dict[str, Any]]:
    """Run :meth:`PackagingAdapter.preflight` for each selected platform."""

    from akc.delivery import adapters as distribution_adapters

    issues: list[dict[str, Any]] = []
    if release_mode not in ("beta", "store", "both"):
        raise ValueError(f"invalid release_mode: {release_mode!r}")
    lanes = distribution_adapters.release_lanes_for_mode(cast(ReleaseMode, release_mode))
    for plat_raw in platforms:
        if plat_raw not in ("web", "ios", "android"):
            continue
        platform = cast(DeliveryPlatform, plat_raw)
        adapter = packaging_adapter_for(platform)
        spec = PlatformBuildSpec(
            tenant_id=tenant_id,
            repo_id=repo_id,
            delivery_id=delivery_id,
            platform=platform,
            delivery_version=delivery_version,
            release_lanes=lanes,
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
                    "lane": adapter.lane,
                    "reason": reason,
                },
            )
    return issues
