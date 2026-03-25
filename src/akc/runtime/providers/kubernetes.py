from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, cast

from akc.memory.models import JSONValue
from akc.runtime.bundle_delivery import build_delivery_handoff_context
from akc.runtime.models import (
    HealthStatus,
    ObservedHealthCondition,
    ObservedHealthConditionStatus,
    ReconcileOperation,
)
from akc.runtime.observe_probes import evaluate_observe_probes, parse_observe_probe_specs
from akc.runtime.providers._subprocess import run_checked
from akc.runtime.reconciler import (
    DeploymentProviderClient,
    ObservedResource,
    ProviderOperationResult,
)
from akc.utils.fingerprint import stable_json_fingerprint


def _repo_root_from_bundle_path(bundle_path: Path) -> Path:
    norm = bundle_path.resolve().as_posix()
    if "/.akc/runtime/" in norm:
        return bundle_path.resolve().parent.parent.parent
    return bundle_path.resolve().parent


_OBSERVE_ONLY = (
    "observe-only deployment provider: apply/rollback is disabled; "
    "use simulate mode or an in-memory provider for mutating tests"
)


def _pod_status_conditions(status: Mapping[str, Any]) -> tuple[ObservedHealthCondition, ...]:
    """Map pod ``status.conditions`` to :class:`ObservedHealthCondition` rows (replay-stable)."""
    raw = status.get("conditions") if isinstance(status, dict) else None
    if not isinstance(raw, list):
        return ()
    out: list[ObservedHealthCondition] = []
    for c in raw:
        if not isinstance(c, dict):
            continue
        t = str(c.get("type", "")).strip()
        if not t:
            continue
        st_raw = str(c.get("status", "")).strip()
        st: ObservedHealthConditionStatus
        if st_raw == "True":
            st = "true"
        elif st_raw == "False":
            st = "false"
        elif st_raw == "Unknown":
            st = "unknown"
        elif st_raw.lower() in {"true", "false", "unknown"}:
            st = cast(ObservedHealthConditionStatus, st_raw.lower())
        else:
            st = "unknown"
        reason = str(c.get("reason", "")).strip() or None
        msg = str(c.get("message", "")).strip() or None
        ltt = str(c.get("lastTransitionTime", "")).strip() or None
        out.append(
            ObservedHealthCondition(
                type=t,
                status=st,
                reason=reason,
                message=msg[:2048] if msg else None,
                last_transition_time=ltt,
            )
        )
    return tuple(out)


def _hash_body(
    *,
    contract: Mapping[str, JSONValue] | None,
    resource_id: str,
    body: dict[str, JSONValue],
) -> str:
    version = 1
    if contract is not None:
        raw_v = contract.get("version")
        if isinstance(raw_v, int) and not isinstance(raw_v, bool):
            version = int(raw_v)
    envelope: dict[str, JSONValue] = {
        "contract_version": version,
        "resource_id": resource_id,
        "body": body,
    }
    if contract is not None:
        envelope["contract"] = dict(contract)
    return stable_json_fingerprint(envelope)


@dataclass(slots=True)
class KubernetesObserveProvider(DeploymentProviderClient):
    """Read-only kubectl observer: hashes pod phase + container image IDs + uid.

    Use ``bundle.metadata["intent_ref"]`` (handoff correlation from compile) for intent
    identity; do not infer it from IR-only labels or Kubernetes resource names.

    **Health mapping**

    - When pod ``status.conditions`` includes **Ready**, that row drives readiness for
      ``health_status``: ``Ready=False`` → ``degraded`` (or ``failed`` if phase is Failed);
      ``Ready=Unknown`` → ``unknown``; ``Ready=True`` uses phase (``Running`` → ``healthy``, etc.).
    - If **Ready** is absent, legacy **phase-only** mapping applies (``Running`` → ``healthy``, …).

    **Extension points**

    - Optional ``deployment_provider.observe_probes`` adds ``ProbeHttp`` / ``ProbeTcp`` conditions;
      a failing probe downgrades ``healthy`` → ``degraded``.
    - Subclass ``observe`` / ``_observe_pod_object`` for custom clusters; keep ``namespace`` scoped.
    """

    observe_only: ClassVar[bool] = True
    bundle_path: Path
    namespace: str
    resource_map: dict[str, str]
    kube_context: str | None
    kind: str
    hash_contract: dict[str, JSONValue] | None
    observe_probe_specs: tuple[dict[str, JSONValue], ...] = ()
    timeout_sec: float = 45.0
    delivery_handoff_context: dict[str, JSONValue] = field(default_factory=dict)

    @classmethod
    def from_bundle_metadata(
        cls,
        metadata: Mapping[str, JSONValue],
        *,
        bundle_path: Path,
    ) -> KubernetesObserveProvider:
        raw = metadata.get("deployment_provider")
        if not isinstance(raw, Mapping):
            raise ValueError("deployment_provider metadata missing for kubernetes provider")
        ns = str(raw.get("namespace", "")).strip()
        if not ns:
            raise ValueError("deployment_provider.namespace is required for kubernetes_observe")
        sm_raw = raw.get("resource_map", raw.get("service_map", {}))
        resource_map: dict[str, str] = {}
        if isinstance(sm_raw, Mapping):
            for rid, name in sm_raw.items():
                if isinstance(rid, str) and rid.strip() and isinstance(name, str) and name.strip():
                    resource_map[rid.strip()] = name.strip()
        ctx_raw = raw.get("kube_context")
        kube_context = str(ctx_raw).strip() if isinstance(ctx_raw, str) and ctx_raw.strip() else None
        kind = str(raw.get("resource_kind", "pod")).strip().lower() or "pod"
        if kind not in {"pod", "deployment"}:
            raise ValueError("deployment_provider.resource_kind must be 'pod' or 'deployment'")
        contract = metadata.get("deployment_observe_hash_contract")
        hash_contract: dict[str, JSONValue] | None = None
        if isinstance(contract, Mapping):
            hash_contract = dict(contract)
        probes = parse_observe_probe_specs(raw.get("observe_probes"))
        return cls(
            bundle_path=bundle_path,
            namespace=ns,
            resource_map=resource_map,
            kube_context=kube_context,
            kind=kind,
            hash_contract=hash_contract,
            observe_probe_specs=probes,
            delivery_handoff_context=build_delivery_handoff_context(metadata),
        )

    def _kubectl_base(self) -> list[str]:
        argv = ["kubectl"]
        if self.kube_context:
            argv.extend(["--context", self.kube_context])
        argv.extend(["-n", self.namespace])
        return argv

    def _get_pod_json(self, name: str) -> dict[str, Any] | None:
        argv = self._kubectl_base() + ["get", "pod", name, "-o", "json"]
        proc = run_checked(argv, cwd=_repo_root_from_bundle_path(self.bundle_path), timeout_sec=self.timeout_sec)
        if proc.returncode != 0 or not proc.stdout.strip():
            return None
        try:
            data = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    def _get_deployment_json(self, name: str) -> dict[str, Any] | None:
        argv = self._kubectl_base() + ["get", "deployment", name, "-o", "json"]
        proc = run_checked(argv, cwd=_repo_root_from_bundle_path(self.bundle_path), timeout_sec=self.timeout_sec)
        if proc.returncode != 0 or not proc.stdout.strip():
            return None
        try:
            data = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return None
        return data if isinstance(data, dict) else None

    def _pod_from_deployment(self, dep: Mapping[str, Any]) -> dict[str, Any] | None:
        labels = dep.get("spec", {})
        if not isinstance(labels, dict):
            return None
        sel = labels.get("selector", {})
        if not isinstance(sel, dict):
            return None
        match_labels = sel.get("matchLabels")
        if not isinstance(match_labels, dict) or not match_labels:
            return None
        parts = [f"{k}={v}" for k, v in match_labels.items() if isinstance(k, str) and isinstance(v, str)]
        if not parts:
            return None
        argv = self._kubectl_base() + ["get", "pods", "-l", ",".join(parts), "-o", "json"]
        proc = run_checked(argv, cwd=_repo_root_from_bundle_path(self.bundle_path), timeout_sec=self.timeout_sec)
        if proc.returncode != 0 or not proc.stdout.strip():
            return None
        try:
            lst = json.loads(proc.stdout)
        except json.JSONDecodeError:
            return None
        items = lst.get("items") if isinstance(lst, dict) else None
        if not isinstance(items, list) or not items:
            return None
        first = items[0]
        return first if isinstance(first, dict) else None

    def _observe_pod_object(self, *, resource_id: str, resource_class: str, pod: Mapping[str, Any]) -> ObservedResource:
        meta = pod.get("metadata")
        uid = ""
        if isinstance(meta, dict):
            u = meta.get("uid")
            if isinstance(u, str) and u.strip():
                uid = u.strip()
        status = pod.get("status", {})
        phase = "unknown"
        if isinstance(status, dict):
            ph = status.get("phase")
            if isinstance(ph, str) and ph.strip():
                phase = ph.strip()
        k8s_conds: tuple[ObservedHealthCondition, ...] = ()
        if isinstance(status, dict):
            k8s_conds = _pod_status_conditions(status)
        ready_row = next((c for c in k8s_conds if c.type == "Ready"), None)
        health_status: HealthStatus = "unknown"
        if ready_row is not None:
            if ready_row.status == "false":
                health_status = "failed" if phase == "Failed" else "degraded"
            elif ready_row.status == "unknown":
                health_status = "unknown"
            elif ready_row.status == "true":
                if phase == "Running":
                    health_status = "healthy"
                elif phase in {"Failed", "Unknown"}:
                    health_status = "failed"
                elif phase == "Pending":
                    health_status = "degraded"
                else:
                    health_status = "unknown"
        else:
            if phase == "Running":
                health_status = "healthy"
            elif phase in {"Failed", "Unknown"}:
                health_status = "failed"
            elif phase == "Pending":
                health_status = "degraded"

        container_ids: list[str] = []
        if isinstance(status, dict):
            ids = status.get("containerStatuses")
            if isinstance(ids, list):
                for cs in ids:
                    if not isinstance(cs, dict):
                        continue
                    iid = cs.get("imageID")
                    if isinstance(iid, str) and iid.strip():
                        container_ids.append(iid.strip())
        container_ids.sort()

        body: dict[str, JSONValue] = {
            "provider": "kubernetes_observe",
            "kind": self.kind,
            "namespace": self.namespace,
            "phase": phase,
            "uid": uid or None,
            "container_image_ids": cast(JSONValue, container_ids),
        }
        observed_hash = _hash_body(contract=self.hash_contract, resource_id=resource_id, body=body)
        name = ""
        if isinstance(meta, dict):
            n = meta.get("name")
            if isinstance(n, str):
                name = n.strip()
        probe_conds = evaluate_observe_probes(self.observe_probe_specs)
        merged_conds = k8s_conds + probe_conds
        if health_status == "healthy" and any(p.type.startswith("Probe") and p.status == "false" for p in probe_conds):
            health_status = "degraded"

        payload: dict[str, JSONValue] = {
            "namespace": self.namespace,
            "pod_name": name,
            "phase": phase,
            "health_conditions": cast(JSONValue, [c.to_json_obj() for c in merged_conds]),
        }
        rc = str(resource_class).strip() or "service"
        return ObservedResource(
            resource_id=resource_id,
            resource_class=rc,
            observed_hash=observed_hash,
            health_status=health_status,
            payload=payload,
            health_conditions=merged_conds,
        )

    def observe(self, *, resource_id: str, resource_class: str) -> ObservedResource | None:
        name = self.resource_map.get(resource_id, resource_id)
        pod: dict[str, Any] | None
        if self.kind == "pod":
            pod = self._get_pod_json(name)
        else:
            dep = self._get_deployment_json(name)
            pod = self._pod_from_deployment(dep) if dep is not None else None
        if pod is None:
            return None
        return self._observe_pod_object(resource_id=resource_id, resource_class=resource_class, pod=pod)

    def list_observed(self) -> tuple[ObservedResource, ...]:
        out: list[ObservedResource] = []
        for resource_id in sorted(self.resource_map):
            obs = self.observe(resource_id=resource_id, resource_class="service")
            if obs is not None:
                out.append(obs)
        return tuple(out)

    def apply(self, *, operation: ReconcileOperation) -> ProviderOperationResult:
        return ProviderOperationResult(
            operation=operation,
            applied=False,
            observed_hash="",
            health_status="unknown",
            error=_OBSERVE_ONLY,
            evidence={"observe_only": True},
        )

    def rollback(self, *, resource_id: str, resource_class: str, target_hash: str) -> ProviderOperationResult:
        operation = ReconcileOperation(
            operation_id=f"{resource_id}:rollback:{target_hash}",
            operation_type="update",
            target=resource_id,
            payload={"resource_id": resource_id, "resource_class": resource_class, "desired_hash": target_hash},
        )
        return ProviderOperationResult(
            operation=operation,
            applied=False,
            observed_hash="",
            health_status="unknown",
            error=_OBSERVE_ONLY,
            evidence={"observe_only": True, "rollback_target_hash": target_hash},
        )


_K8S_ROLLBACK_ERR_UNSUPPORTED = "rollback_unsupported:rollout_undo_requires_resource_kind_deployment"
_K8S_ROLLBACK_ERR_FAILED = "rollback_failed:kubectl_rollout_undo_failed"
_K8S_ROLLBACK_ERR_HASH_MISMATCH = "rollback_failed:target_hash_mismatch_after_rollout_undo"
_K8S_ROLLBACK_ERR_HASH_MISMATCH_MANIFEST = "rollback_failed:target_hash_mismatch_after_manifest_apply"


@dataclass(slots=True)
class KubernetesApplyProvider(KubernetesObserveProvider):
    """Opt-in mutating kubectl with pinned manifest SHA-256 gates.

    Rollback precedence:
    1. If ``rollback_apply_manifest_by_desired_hash[target_hash]`` exists and validates,
       execute ``kubectl apply -f`` on that pinned rollback snapshot and verify observed hash.
    2. Else, fallback to ``kubectl rollout undo deployment/<name>`` for ``resource_kind=deployment``.
    """

    observe_only: ClassVar[bool] = False
    apply_manifest_path: str = ""
    apply_manifest_sha256_hex: str = ""
    # Optional deterministic rollback mapping: desired_hash -> pinned rollback manifest snapshot.
    rollback_apply_manifest_by_desired_hash: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_bundle_metadata(
        cls,
        metadata: Mapping[str, JSONValue],
        *,
        bundle_path: Path,
    ) -> KubernetesApplyProvider:
        base = KubernetesObserveProvider.from_bundle_metadata(metadata, bundle_path=bundle_path)
        raw = metadata.get("deployment_provider")
        if not isinstance(raw, Mapping):
            raise ValueError("deployment_provider metadata missing for kubernetes apply provider")
        rel = str(raw.get("apply_manifest_path", "")).strip()
        digest = str(raw.get("apply_manifest_sha256", "")).strip().lower()
        if not rel or not digest:
            raise ValueError(
                "kubernetes_apply requires deployment_provider.apply_manifest_path and apply_manifest_sha256"
            )
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("apply_manifest_sha256 must be a 64-char lowercase hex sha256")
        rollback_map: dict[str, dict[str, str]] = {}
        raw_rb = raw.get("rollback_apply_manifest_by_desired_hash")
        if isinstance(raw_rb, Mapping):
            for desired_hash, row in raw_rb.items():
                if not isinstance(row, Mapping):
                    continue
                dh = str(desired_hash).strip().lower()
                rb_rel = str(row.get("apply_manifest_path", "")).strip()
                rb_sha = str(row.get("apply_manifest_sha256", "")).strip().lower()
                if not dh or not rb_rel or not rb_sha:
                    continue
                if len(rb_sha) != 64 or any(c not in "0123456789abcdef" for c in rb_sha):
                    continue
                rollback_map[dh] = {"apply_manifest_path": rb_rel, "apply_manifest_sha256": rb_sha}
        return cls(
            bundle_path=base.bundle_path,
            namespace=base.namespace,
            resource_map=base.resource_map,
            kube_context=base.kube_context,
            kind=base.kind,
            hash_contract=base.hash_contract,
            observe_probe_specs=base.observe_probe_specs,
            timeout_sec=base.timeout_sec,
            delivery_handoff_context=base.delivery_handoff_context,
            apply_manifest_path=rel,
            apply_manifest_sha256_hex=digest,
            rollback_apply_manifest_by_desired_hash=rollback_map,
        )

    def _validated_manifest_file(self) -> tuple[Path | None, str | None]:
        repo = _repo_root_from_bundle_path(self.bundle_path)
        manifest = (repo / self.apply_manifest_path).resolve()
        try:
            manifest.relative_to(repo.resolve())
        except ValueError:
            return None, "apply_manifest_path escapes repository root"
        if not manifest.is_file():
            return None, "apply manifest file is missing"
        digest = hashlib.sha256(manifest.read_bytes()).hexdigest()
        if digest != self.apply_manifest_sha256_hex:
            return (
                None,
                f"apply manifest sha256 mismatch (expected {self.apply_manifest_sha256_hex}, observed {digest})",
            )
        return manifest, None

    def _validated_rollback_manifest_file(self, *, target_hash: str) -> tuple[Path | None, str | None]:
        row = self.rollback_apply_manifest_by_desired_hash.get(target_hash)
        if not row:
            return None, "rollback_unsupported:manifest_snapshot_missing_for_target_hash"
        repo = _repo_root_from_bundle_path(self.bundle_path)
        rel = str(row.get("apply_manifest_path", "")).strip()
        digest = str(row.get("apply_manifest_sha256", "")).strip().lower()
        if not rel or not digest:
            return None, "rollback_unsupported:manifest_snapshot_missing_for_target_hash"
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            return None, "rollback apply_manifest_sha256 must be a 64-char lowercase hex sha256"
        manifest = (repo / rel).resolve()
        try:
            manifest.relative_to(repo.resolve())
        except ValueError:
            return None, "rollback apply_manifest_path escapes repository root"
        if not manifest.is_file():
            return None, "rollback apply manifest file is missing"
        observed = hashlib.sha256(manifest.read_bytes()).hexdigest()
        if observed != digest:
            return None, f"rollback manifest sha256 mismatch (expected {digest}, observed {observed})"
        return manifest, None

    def apply(self, *, operation: ReconcileOperation) -> ProviderOperationResult:
        manifest, err = self._validated_manifest_file()
        if manifest is None:
            return ProviderOperationResult(
                operation=operation,
                applied=False,
                observed_hash="",
                health_status="unknown",
                error=err or "apply manifest validation failed",
                evidence={"mutating": False, "gate": "manifest_sha256"},
            )
        argv = self._kubectl_base() + ["apply", "-f", str(manifest)]
        repo = _repo_root_from_bundle_path(self.bundle_path)
        proc = run_checked(argv, cwd=repo, timeout_sec=self.timeout_sec)
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "").strip()[-4096:]
            return ProviderOperationResult(
                operation=operation,
                applied=False,
                observed_hash="",
                health_status="unknown",
                error=f"kubectl apply failed (exit {proc.returncode}): {tail}",
                evidence={"mutating": True},
            )
        observed = self.observe(
            resource_id=str(operation.payload.get("resource_id", operation.target)),
            resource_class=str(operation.payload.get("resource_class", "service")),
        )
        return ProviderOperationResult(
            operation=operation,
            applied=True,
            observed_hash=observed.observed_hash if observed is not None else "",
            health_status=observed.health_status if observed is not None else "unknown",
            evidence={"mutating": True, "provider": "kubernetes_apply"},
        )

    def rollback(self, *, resource_id: str, resource_class: str, target_hash: str) -> ProviderOperationResult:
        operation = ReconcileOperation(
            operation_id=f"{resource_id}:rollback:{target_hash}",
            operation_type="update",
            target=resource_id,
            payload={"resource_id": resource_id, "resource_class": resource_class, "desired_hash": target_hash},
        )
        repo = _repo_root_from_bundle_path(self.bundle_path)
        rollback_manifest, rollback_manifest_err = self._validated_rollback_manifest_file(target_hash=target_hash)
        if rollback_manifest is not None:
            argv = self._kubectl_base() + ["apply", "-f", str(rollback_manifest)]
            proc = run_checked(argv, cwd=repo, timeout_sec=self.timeout_sec)
            if proc.returncode != 0:
                tail = (proc.stderr or proc.stdout or "").strip()[-4096:]
                return ProviderOperationResult(
                    operation=operation,
                    applied=False,
                    observed_hash="",
                    health_status="unknown",
                    error=f"rollback_failed:kubectl_apply_failed (exit {proc.returncode}): {tail}",
                    evidence={
                        "mutating": True,
                        "rollback_target_hash": target_hash,
                        "argv_tail": cast(JSONValue, list(argv[-6:])),
                        "rollback_outcome": "rollback_failed",
                    },
                )
            observed = self.observe(resource_id=resource_id, resource_class=resource_class)
            observed_hash = observed.observed_hash if observed is not None else ""
            observed_health = observed.health_status if observed is not None else "unknown"
            mismatch = bool(str(observed_hash).strip() != str(target_hash).strip())
            err = (
                f"{_K8S_ROLLBACK_ERR_HASH_MISMATCH_MANIFEST} (expected {target_hash}, observed {observed_hash})"
                if mismatch
                else None
            )
            return ProviderOperationResult(
                operation=operation,
                applied=not mismatch,
                observed_hash=observed_hash,
                health_status=observed_health,
                error=err,
                evidence={
                    "mutating": True,
                    "provider": "kubernetes_apply",
                    "rollback_target_hash": target_hash,
                    "rollback_outcome": ("rollback_failed" if mismatch else "rollback_applied"),
                    "rollback_method": "manifest_snapshot",
                },
            )

        if self.kind != "deployment":
            return ProviderOperationResult(
                operation=operation,
                applied=False,
                observed_hash="",
                health_status="unknown",
                error=rollback_manifest_err or _K8S_ROLLBACK_ERR_UNSUPPORTED,
                evidence={
                    "mutating": True,
                    "rollback_target_hash": target_hash,
                    "rollback_supported": False,
                    "rollback_outcome": "rollback_unsupported",
                },
            )

        name = self.resource_map.get(resource_id, resource_id)
        argv = self._kubectl_base() + ["rollout", "undo", "deployment", name]
        proc = run_checked(argv, cwd=repo, timeout_sec=self.timeout_sec)
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "").strip()[-4096:]
            return ProviderOperationResult(
                operation=operation,
                applied=False,
                observed_hash="",
                health_status="unknown",
                error=f"{_K8S_ROLLBACK_ERR_FAILED} (exit {proc.returncode}): {tail}",
                evidence={
                    "mutating": True,
                    "rollback_target_hash": target_hash,
                    "argv_tail": cast(JSONValue, list(argv[-6:])),
                    "rollback_outcome": "rollback_failed",
                },
            )

        observed = self.observe(resource_id=resource_id, resource_class=resource_class)
        observed_hash = observed.observed_hash if observed is not None else ""
        observed_health = observed.health_status if observed is not None else "unknown"
        mismatch = bool(
            observed is not None and str(observed_hash).strip() and str(observed_hash).strip() != target_hash
        )
        err = (
            f"{_K8S_ROLLBACK_ERR_HASH_MISMATCH} (expected {target_hash}, observed {observed_hash})"
            if mismatch
            else None
        )
        return ProviderOperationResult(
            operation=operation,
            applied=not mismatch,
            observed_hash=observed_hash,
            health_status=observed_health,
            error=err,
            evidence={
                "mutating": True,
                "provider": "kubernetes_apply",
                "rollback_target_hash": target_hash,
                "rollback_outcome": ("rollback_failed" if mismatch else "rollback_applied"),
                "rollback_method": "rollout_undo",
            },
        )
