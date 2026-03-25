"""Distribution adapter protocol for named-recipient delivery (beta / store lanes)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from akc.delivery.types import DeliveryPlatform, PlatformBuildSpec, ReleaseLane


class DistributionAdapter(ABC):
    """Provider-specific beta or store distribution (TestFlight, Play, web invite, etc.)."""

    @property
    @abstractmethod
    def kind(self) -> str:
        """Stable adapter identifier (e.g. ``web_invite``, ``testflight``)."""

    @abstractmethod
    def supported_platforms(self) -> frozenset[DeliveryPlatform]:
        """Platforms this adapter can target."""

    @abstractmethod
    def supported_lanes(self) -> frozenset[ReleaseLane]:
        """Release lanes this adapter implements."""

    def preflight(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
    ) -> list[str]:
        """Return blocking reasons if this adapter cannot run; empty means ok.

        Must not log or persist credentials. ``tenant_id`` / ``repo_id`` scope
        checks to the active workspace for isolation across tenants.
        """

        _ = (project_dir, tenant_id, repo_id, spec)
        return []
