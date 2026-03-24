"""Optional deployment provider adapters (read-only observe first, feature-flagged)."""

from akc.runtime.providers.factory import create_deployment_provider

__all__ = ["create_deployment_provider"]
