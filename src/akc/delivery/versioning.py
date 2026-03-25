"""Deterministic mapping from ``delivery_version`` to provider-specific build metadata."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass

# Google Play versionCode must be a positive 32-bit integer (docs cap at 2100000000).
_MAX_PLAY_VERSION_CODE: int = 2_100_000_000


@dataclass(frozen=True, slots=True)
class PlatformProviderVersions:
    """Per-session logical version string plus derived provider fields (no secrets)."""

    delivery_version: str
    """Logical semver (or semver-like) string for the delivery session."""

    ios_marketing_version: str
    """CFBundleShortVersionString-style value (major.minor.patch)."""

    ios_build_number: str
    """CFBundleVersion: monotonic-friendly string derived from ``delivery_version``."""

    android_version_name: str
    """versionName for Gradle / Play (same family as marketing version)."""

    android_version_code: int
    """versionCode for Play; deterministic positive integer."""

    web_pwa_version: str
    """Manifest / service-worker cache token friendly string."""


_SEMVER_PREFIX_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)")


def parse_marketing_version(delivery_version: str) -> str:
    """Best-effort ``major.minor.patch`` for iOS/Android display fields."""

    s = delivery_version.strip()
    m = _SEMVER_PREFIX_RE.match(s)
    if m:
        return f"{int(m.group(1))}.{int(m.group(2))}.{int(m.group(3))}"
    return "0.0.0"


def derive_android_version_code(delivery_version: str) -> int:
    """Stable positive ``versionCode`` from the logical delivery version string."""

    h = hashlib.sha256(delivery_version.strip().encode("utf-8")).digest()
    n = int.from_bytes(h[:4], "big") % (_MAX_PLAY_VERSION_CODE - 1)
    return max(1, int(n) + 1)


def derive_ios_build_number(delivery_version: str) -> str:
    """Stable numeric build string for ``CFBundleVersion`` (TestFlight / App Store Connect)."""

    return str(derive_android_version_code(delivery_version))


def derive_platform_provider_versions(delivery_version: str) -> PlatformProviderVersions:
    """Derive all platform-specific version fields from one logical ``delivery_version``."""

    dv = delivery_version.strip()
    if not dv:
        dv = "0.0.0"
    marketing = parse_marketing_version(dv)
    code = derive_android_version_code(dv)
    web_token = hashlib.sha256(dv.encode("utf-8")).hexdigest()[:12]
    return PlatformProviderVersions(
        delivery_version=dv,
        ios_marketing_version=marketing,
        ios_build_number=derive_ios_build_number(dv),
        android_version_name=marketing,
        android_version_code=code,
        web_pwa_version=f"{marketing}+{web_token}",
    )
