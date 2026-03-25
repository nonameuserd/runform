from __future__ import annotations

from akc.delivery.versioning import (
    derive_android_version_code,
    derive_platform_provider_versions,
    parse_marketing_version,
)


def test_derive_platform_provider_versions_is_deterministic() -> None:
    a = derive_platform_provider_versions("1.2.3")
    b = derive_platform_provider_versions("1.2.3")
    assert a == b
    assert a.android_version_code == b.android_version_code
    assert a.ios_build_number == b.ios_build_number


def test_android_version_code_within_play_bounds() -> None:
    v = derive_platform_provider_versions("1.0.0")
    assert 1 <= v.android_version_code < 2_100_000_000
    assert derive_android_version_code("1.0.0") == v.android_version_code


def test_marketing_version_from_semver_prefix() -> None:
    v = derive_platform_provider_versions("2.5.7+build.1")
    assert v.ios_marketing_version == "2.5.7"
    assert v.android_version_name == "2.5.7"
    assert parse_marketing_version("not-semver") == "0.0.0"


def test_different_delivery_versions_yield_different_codes() -> None:
    a = derive_platform_provider_versions("1.0.0")
    b = derive_platform_provider_versions("1.0.1")
    assert a.android_version_code != b.android_version_code
