from __future__ import annotations

import subprocess
from pathlib import Path

from akc.runtime.providers.compose import DockerComposeApplyProvider
from akc.runtime.providers.kubernetes import KubernetesApplyProvider
from akc.runtime.reconciler import ObservedResource


def test_compose_apply_rollback_without_manifest_snapshot_is_unsupported(tmp_path: Path) -> None:
    provider = DockerComposeApplyProvider(
        bundle_path=tmp_path / ".akc" / "runtime" / "bundle.json",
        compose_project="demo",
        compose_files=(),
        working_dir=tmp_path,
        service_map={},
        hash_contract=None,
        apply_manifest_path="compose.yml",
        apply_manifest_sha256_hex="a" * 64,
        rollback_apply_manifest_by_desired_hash={},
    )
    res = provider.rollback(resource_id="svc-api", resource_class="service", target_hash="hash-prev")
    assert res.applied is False
    assert str(res.error or "").startswith("rollback_unsupported:")
    assert res.evidence.get("rollback_outcome") == "rollback_unsupported"


def test_kubernetes_apply_rollback_hash_mismatch_is_failure(monkeypatch: object, tmp_path: Path) -> None:
    provider = KubernetesApplyProvider(
        bundle_path=tmp_path / ".akc" / "runtime" / "bundle.json",
        namespace="default",
        resource_map={"svc-api": "svc-api"},
        kube_context=None,
        kind="deployment",
        hash_contract=None,
        apply_manifest_path="app.yaml",
        apply_manifest_sha256_hex="a" * 64,
    )

    def _fake_run_checked(argv: list[str], *, cwd: Path, timeout_sec: float) -> subprocess.CompletedProcess[str]:
        _ = cwd, timeout_sec
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr("akc.runtime.providers.kubernetes.run_checked", _fake_run_checked)
    monkeypatch.setattr(
        provider,
        "observe",
        lambda *, resource_id, resource_class: ObservedResource(
            resource_id=resource_id,
            resource_class=resource_class,
            observed_hash="hash-observed",
            health_status="healthy",
        ),
    )
    res = provider.rollback(resource_id="svc-api", resource_class="service", target_hash="hash-target")
    assert res.applied is False
    assert str(res.error or "").startswith("rollback_failed:target_hash_mismatch_after_rollout_undo")
    assert res.evidence.get("rollback_outcome") == "rollback_failed"


def test_compose_apply_rollback_hash_mismatch_is_failure(monkeypatch: object, tmp_path: Path) -> None:
    provider = DockerComposeApplyProvider(
        bundle_path=tmp_path / ".akc" / "runtime" / "bundle.json",
        compose_project="demo",
        compose_files=(),
        working_dir=tmp_path,
        service_map={"svc-api": "api"},
        hash_contract=None,
        apply_manifest_path="compose.yml",
        apply_manifest_sha256_hex="a" * 64,
        rollback_apply_manifest_by_desired_hash={
            "hash-target": {
                "apply_manifest_path": "rb.yml",
                "apply_manifest_sha256": "b" * 64,
            }
        },
    )

    def _fake_run_checked(argv: list[str], *, cwd: Path, timeout_sec: float) -> subprocess.CompletedProcess[str]:
        _ = argv, cwd, timeout_sec
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr("akc.runtime.providers.compose.run_checked", _fake_run_checked)

    def _validated_rb_compose(*, target_hash: str) -> tuple[Path, None]:
        _ = target_hash
        return (tmp_path / "rb.yml", None)

    monkeypatch.setattr(provider, "_validated_rollback_manifest_file", _validated_rb_compose)
    monkeypatch.setattr(
        provider,
        "observe",
        lambda *, resource_id, resource_class: ObservedResource(
            resource_id=resource_id,
            resource_class=resource_class,
            observed_hash="hash-observed",
            health_status="healthy",
        ),
    )
    res = provider.rollback(resource_id="svc-api", resource_class="service", target_hash="hash-target")
    assert res.applied is False
    assert str(res.error or "").startswith("rollback_failed:target_hash_mismatch_after_manifest_apply")
    assert res.evidence.get("rollback_outcome") == "rollback_failed"


def test_kubernetes_apply_rollback_uses_manifest_snapshot_when_present(monkeypatch: object, tmp_path: Path) -> None:
    provider = KubernetesApplyProvider(
        bundle_path=tmp_path / ".akc" / "runtime" / "bundle.json",
        namespace="default",
        resource_map={"svc-api": "svc-api"},
        kube_context=None,
        kind="pod",
        hash_contract=None,
        apply_manifest_path="app.yaml",
        apply_manifest_sha256_hex="a" * 64,
        rollback_apply_manifest_by_desired_hash={
            "hash-target": {
                "apply_manifest_path": "rb.yaml",
                "apply_manifest_sha256": "b" * 64,
            }
        },
    )

    calls: list[list[str]] = []

    def _fake_run_checked(argv: list[str], *, cwd: Path, timeout_sec: float) -> subprocess.CompletedProcess[str]:
        _ = cwd, timeout_sec
        calls.append(list(argv))
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr("akc.runtime.providers.kubernetes.run_checked", _fake_run_checked)

    def _validated_rb_k8s(*, target_hash: str) -> tuple[Path, None]:
        _ = target_hash
        return (tmp_path / "rb.yaml", None)

    monkeypatch.setattr(provider, "_validated_rollback_manifest_file", _validated_rb_k8s)
    monkeypatch.setattr(
        provider,
        "observe",
        lambda *, resource_id, resource_class: ObservedResource(
            resource_id=resource_id,
            resource_class=resource_class,
            observed_hash="hash-target",
            health_status="healthy",
        ),
    )
    res = provider.rollback(resource_id="svc-api", resource_class="service", target_hash="hash-target")
    assert res.applied is True
    assert res.error is None
    assert res.evidence.get("rollback_method") == "manifest_snapshot"
    assert any("apply" in argv for argv in calls)
