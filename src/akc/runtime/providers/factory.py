from __future__ import annotations

import os
from collections.abc import Mapping

from akc.path_security import safe_resolve_path
from akc.runtime.models import RuntimeBundle
from akc.runtime.providers.compose import DockerComposeApplyProvider, DockerComposeObserveProvider
from akc.runtime.providers.kubernetes import KubernetesApplyProvider, KubernetesObserveProvider
from akc.runtime.reconciler import DeploymentProviderClient, InMemoryDeploymentProviderClient


def full_layer_replacement_enabled(bundle: RuntimeBundle) -> bool:
    """Enable full replacement mode from bundle contract metadata.

    This is tenant-scoped by runtime bundle loading and is explicit opt-in.
    """
    raw = bundle.metadata.get("layer_replacement_mode")
    if isinstance(raw, str) and raw.strip().lower() == "full":
        return True
    contract = bundle.metadata.get("deployment_provider_contract")
    if isinstance(contract, Mapping):
        mode = contract.get("mutation_mode")
        if isinstance(mode, str) and mode.strip().lower() == "full":
            return True
    return False


def external_deployment_providers_enabled() -> bool:
    """Gate real cluster/compose observation behind an explicit env flag (tenant ops control)."""
    return os.environ.get("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", "").strip() == "1"


def mutating_deployment_providers_enabled() -> bool:
    """Gate ``docker compose up`` / ``kubectl apply`` providers (OSS safety; opt-in)."""
    return os.environ.get("AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER", "").strip() == "1"


def create_deployment_provider(bundle: RuntimeBundle) -> DeploymentProviderClient:
    """Resolve DeploymentProviderClient from bundle metadata when flags allow external infra."""
    raw = bundle.metadata.get("deployment_provider")
    if not isinstance(raw, Mapping):
        return InMemoryDeploymentProviderClient()
    kind = str(raw.get("kind", "")).strip()
    bundle_path = safe_resolve_path(str(bundle.ref.bundle_path))
    full_replace = full_layer_replacement_enabled(bundle)

    if kind in {"docker_compose_apply", "kubernetes_apply"}:
        if not full_replace and not mutating_deployment_providers_enabled():
            return InMemoryDeploymentProviderClient()
        if kind == "docker_compose_apply":
            return DockerComposeApplyProvider.from_bundle_metadata(bundle.metadata, bundle_path=bundle_path)
        return KubernetesApplyProvider.from_bundle_metadata(bundle.metadata, bundle_path=bundle_path)

    if not full_replace and not external_deployment_providers_enabled():
        return InMemoryDeploymentProviderClient()
    if kind == "docker_compose_observe":
        return DockerComposeObserveProvider.from_bundle_metadata(bundle.metadata, bundle_path=bundle_path)
    if kind == "kubernetes_observe":
        return KubernetesObserveProvider.from_bundle_metadata(bundle.metadata, bundle_path=bundle_path)
    return InMemoryDeploymentProviderClient()
