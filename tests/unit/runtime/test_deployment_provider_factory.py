from __future__ import annotations

from pathlib import Path

from akc.runtime.models import RuntimeBundle, RuntimeBundleRef, RuntimeContext
from akc.runtime.providers.compose import DockerComposeApplyProvider, DockerComposeObserveProvider
from akc.runtime.providers.factory import (
    create_deployment_provider,
    external_deployment_providers_enabled,
    mutating_deployment_providers_enabled,
)
from akc.runtime.providers.kubernetes import KubernetesApplyProvider, KubernetesObserveProvider
from akc.runtime.reconciler import InMemoryDeploymentProviderClient


def _bundle(*, path: Path, metadata: dict) -> RuntimeBundle:
    ctx = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="run-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="native",
    )
    ref = RuntimeBundleRef(
        bundle_path=str(path),
        manifest_hash="a" * 64,
        created_at=1,
        source_compile_run_id="run-1",
    )
    return RuntimeBundle(context=ctx, ref=ref, nodes=(), contract_ids=(), metadata=metadata)


def test_external_flag_default_off(monkeypatch: object) -> None:
    monkeypatch.delenv("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", raising=False)
    assert external_deployment_providers_enabled() is False


def test_factory_in_memory_without_env(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.delenv("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", raising=False)
    p = tmp_path / "repo" / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    bundle = _bundle(
        path=p,
        metadata={
            "deployment_provider": {
                "kind": "docker_compose_observe",
                "project": "demo",
                "service_map": {"svc": "api"},
            }
        },
    )
    client = create_deployment_provider(bundle)
    assert isinstance(client, InMemoryDeploymentProviderClient)


def test_factory_compose_when_enabled(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setenv("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", "1")
    p = tmp_path / "repo" / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    bundle = _bundle(
        path=p,
        metadata={
            "deployment_provider": {
                "kind": "docker_compose_observe",
                "project": "demo",
                "service_map": {"svc": "api"},
            },
            "deployment_observe_hash_contract": {"version": 1, "algorithm": "stable_json_sha256_v1"},
        },
    )
    client = create_deployment_provider(bundle)
    assert isinstance(client, DockerComposeObserveProvider)
    assert client.observe_only is True


def test_factory_kubernetes_when_enabled(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setenv("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", "1")
    p = tmp_path / "repo" / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    bundle = _bundle(
        path=p,
        metadata={
            "deployment_provider": {
                "kind": "kubernetes_observe",
                "namespace": "default",
                "resource_map": {"svc": "pod-1"},
                "resource_kind": "pod",
            },
        },
    )
    client = create_deployment_provider(bundle)
    assert isinstance(client, KubernetesObserveProvider)
    assert client.observe_only is True


def test_mutating_flag_default_off(monkeypatch: object) -> None:
    monkeypatch.delenv("AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER", raising=False)
    assert mutating_deployment_providers_enabled() is False


def test_factory_compose_apply_in_memory_without_mutating_env(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.delenv("AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER", raising=False)
    p = tmp_path / "repo" / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    manifest = tmp_path / "repo" / "compose.yml"
    manifest.write_text("services: {}\n", encoding="utf-8")
    import hashlib

    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    bundle = _bundle(
        path=p,
        metadata={
            "deployment_provider": {
                "kind": "docker_compose_apply",
                "project": "demo",
                "service_map": {"svc": "api"},
                "apply_manifest_path": "compose.yml",
                "apply_manifest_sha256": digest,
            }
        },
    )
    assert isinstance(create_deployment_provider(bundle), InMemoryDeploymentProviderClient)


def test_factory_compose_apply_when_mutating_enabled(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setenv("AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER", "1")
    repo = tmp_path / "repo"
    p = repo / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    manifest = repo / "compose.yml"
    manifest.write_text("services: {}\n", encoding="utf-8")
    import hashlib

    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    bundle = _bundle(
        path=p,
        metadata={
            "deployment_provider": {
                "kind": "docker_compose_apply",
                "project": "demo",
                "service_map": {"svc": "api"},
                "apply_manifest_path": "compose.yml",
                "apply_manifest_sha256": digest,
            },
            "deployment_observe_hash_contract": {"version": 1},
        },
    )
    client = create_deployment_provider(bundle)
    assert isinstance(client, DockerComposeApplyProvider)
    assert client.observe_only is False


def test_factory_kubernetes_apply_when_mutating_enabled(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setenv("AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER", "1")
    repo = tmp_path / "repo"
    p = repo / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    manifest = repo / "app.yaml"
    manifest.write_text("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: akc-test-ns\n", encoding="utf-8")
    import hashlib

    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    bundle = _bundle(
        path=p,
        metadata={
            "deployment_provider": {
                "kind": "kubernetes_apply",
                "namespace": "default",
                "resource_map": {"svc": "pod-1"},
                "resource_kind": "pod",
                "apply_manifest_path": "app.yaml",
                "apply_manifest_sha256": digest,
            },
        },
    )
    client = create_deployment_provider(bundle)
    assert isinstance(client, KubernetesApplyProvider)
    assert client.observe_only is False


def test_factory_unknown_kind_falls_back(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.setenv("AKC_ENABLE_EXTERNAL_DEPLOYMENT_PROVIDER", "1")
    p = tmp_path / "repo" / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    bundle = _bundle(
        path=p,
        metadata={"deployment_provider": {"kind": "unknown_provider"}},
    )
    assert isinstance(create_deployment_provider(bundle), InMemoryDeploymentProviderClient)


def test_factory_full_layer_replacement_enables_compose_apply_without_env(tmp_path: Path, monkeypatch: object) -> None:
    monkeypatch.delenv("AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER", raising=False)
    repo = tmp_path / "repo"
    p = repo / ".akc" / "runtime" / "b.json"
    p.parent.mkdir(parents=True)
    p.write_text("{}", encoding="utf-8")
    manifest = repo / "compose.yml"
    manifest.write_text("services: {}\n", encoding="utf-8")
    import hashlib

    digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
    bundle = _bundle(
        path=p,
        metadata={
            "layer_replacement_mode": "full",
            "deployment_provider": {
                "kind": "docker_compose_apply",
                "project": "demo",
                "service_map": {"svc": "api"},
                "apply_manifest_path": "compose.yml",
                "apply_manifest_sha256": digest,
            },
        },
    )
    client = create_deployment_provider(bundle)
    assert isinstance(client, DockerComposeApplyProvider)
