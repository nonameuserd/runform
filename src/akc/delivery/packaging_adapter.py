"""Packaging adapter protocol (web PWA bundle, iOS archive/export, Android APK/AAB)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from akc.delivery.types import DeliveryPlatform, PlatformBuildSpec
from akc.delivery.versioning import PlatformProviderVersions


@dataclass(frozen=True, slots=True)
class PackagingResult:
    """Outcome of a single packaging lane (non-secret metadata and paths)."""

    ok: bool
    lane: str
    error: str | None
    outputs: dict[str, Any] = field(default_factory=dict)


class PackagingAdapter(ABC):
    """Platform-specific packaging after compile (shared React/Expo app base by default)."""

    @property
    @abstractmethod
    def lane(self) -> str:
        """Stable lane id: ``web_bundle`` | ``ios_build`` | ``android_build``."""

    @property
    @abstractmethod
    def platform(self) -> DeliveryPlatform:
        """Target platform for this adapter."""

    def preflight(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
    ) -> list[str]:
        """Return blocking reasons if packaging cannot run; empty means ok."""

        _ = (project_dir, tenant_id, repo_id, spec)
        return []

    @abstractmethod
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
        """Produce or stage deployable artifacts for this platform (v1 stubs record intent only)."""
