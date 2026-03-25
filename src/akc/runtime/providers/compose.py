from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, cast

from akc.memory.models import JSONValue
from akc.runtime.bundle_delivery import build_delivery_handoff_context
from akc.runtime.models import HealthStatus, ObservedHealthCondition, ObservedHealthConditionStatus, ReconcileOperation
from akc.runtime.observe_probes import evaluate_observe_probes, parse_observe_probe_specs
from akc.runtime.providers._subprocess import run_checked
from akc.runtime.reconciler import (
    DeploymentProviderClient,
    ObservedResource,
    ProviderOperationResult,
)
from akc.utils.fingerprint import stable_json_fingerprint

_OBSERVE_ONLY = (
    "observe-only deployment provider: apply/rollback is disabled; "
    "use simulate mode or an in-memory provider for mutating tests"
)


def _repo_root_from_bundle_path(bundle_path: Path) -> Path:
    norm = bundle_path.resolve().as_posix()
    if "/.akc/runtime/" in norm:
        return bundle_path.resolve().parent.parent.parent
    return bundle_path.resolve().parent


def _resolve_compose_cwd(bundle_path: Path, raw_cwd: str | None) -> Path:
    repo = _repo_root_from_bundle_path(bundle_path)
    if raw_cwd is None or not str(raw_cwd).strip():
        return repo
    p = Path(str(raw_cwd).strip())
    resolved = (p if p.is_absolute() else (repo / p)).resolve()
    try:
        resolved.relative_to(repo)
    except ValueError as exc:
        raise ValueError("compose working directory must stay inside the bundle repo root") from exc
    return resolved


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


def _parse_compose_ps_json(stdout: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            rows.append(parsed)
    return rows


def _container_id_from_ps_row(row: Mapping[str, Any]) -> str | None:
    for key in ("ID", "Id", "ContainerID", "container_id"):
        raw = row.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip().split()[0]
    return None


def _inspect_image_digest(*, container_id: str, cwd: Path, timeout_sec: float) -> tuple[str, str]:
    proc = run_checked(
        ["docker", "inspect", container_id],
        cwd=cwd,
        timeout_sec=timeout_sec,
    )
    if proc.returncode != 0 or not proc.stdout.strip():
        return "", ""
    try:
        data = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return "", ""
    if not isinstance(data, list) or not data:
        return "", ""
    first = data[0]
    if not isinstance(first, dict):
        return "", ""
    cfg = first.get("Config")
    image_ref = ""
    if isinstance(cfg, dict):
        ref = cfg.get("Image")
        if isinstance(ref, str) and ref.strip():
            image_ref = ref.strip()
    digests = first.get("RepoDigests")
    digest = ""
    if isinstance(digests, list) and digests:
        d0 = digests[0]
        if isinstance(d0, str) and d0.strip():
            digest = d0.strip()
    image_id = ""
    iid = first.get("Image")
    if isinstance(iid, str) and iid.strip():
        image_id = iid.strip()
    return digest or image_id, image_ref


@dataclass(slots=True)
class DockerComposeObserveProvider(DeploymentProviderClient):
    """Read-only Docker Compose observer: hashes stable spec fields + image digest.

    Correlate with compile output via ``bundle.metadata["intent_ref"]`` (same object as
    ``build_handoff_intent_ref`` on the runtime bundle), not IR ``deployment_intents``
    or orchestration step ids alone.

    **Health mapping (aggregate ``health_status``)**

    - ``failed``: container state suggests exited / dead / removing.
    - ``degraded``: Docker healthcheck **unhealthy**, or ``running`` with non-healthy health field,
      or optional ``observe_probes`` reported false.
    - ``healthy``: ``running`` and (no healthcheck output, or health contains **healthy**).
    - ``unknown``: otherwise (for example state not recognized).

    **Structured conditions** (``ObservedResource.health_conditions``), extension points for strict gates:

    - ``RuntimeRunning``: compose **ps** state is running.
    - ``ContainerHealth``: docker health string when present (**true** / **false** / **unknown**).
    - ``ProbeTcp`` / ``ProbeHttp``: from ``deployment_provider.observe_probes`` (read-only GET / TCP connect).

    Override by subclassing ``observe`` / ``_observe_service``; keep tenant-scoped ``working_dir`` inside repo root.
    """

    observe_only: ClassVar[bool] = True
    bundle_path: Path
    compose_project: str
    compose_files: tuple[str, ...]
    working_dir: Path
    service_map: dict[str, str]
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
    ) -> DockerComposeObserveProvider:
        raw = metadata.get("deployment_provider")
        if not isinstance(raw, Mapping):
            raise ValueError("deployment_provider metadata missing for compose provider")
        project = str(raw.get("project", "")).strip()
        if not project:
            raise ValueError("deployment_provider.project is required for docker_compose_observe")
        files_raw = raw.get("compose_files", [])
        files: tuple[str, ...] = ()
        if isinstance(files_raw, list) and files_raw:
            files = tuple(str(f).strip() for f in files_raw if str(f).strip())
        sm_raw = raw.get("service_map", {})
        service_map: dict[str, str] = {}
        if isinstance(sm_raw, Mapping):
            for rid, svc in sm_raw.items():
                if isinstance(rid, str) and rid.strip() and isinstance(svc, str) and svc.strip():
                    service_map[rid.strip()] = svc.strip()
        cwd_raw = raw.get("compose_working_dir")
        cwd_str = str(cwd_raw).strip() if isinstance(cwd_raw, str) else None
        working_dir = _resolve_compose_cwd(bundle_path, cwd_str)
        contract = metadata.get("deployment_observe_hash_contract")
        hash_contract: dict[str, JSONValue] | None = None
        if isinstance(contract, Mapping):
            hash_contract = dict(contract)
        probes = parse_observe_probe_specs(raw.get("observe_probes"))
        return cls(
            bundle_path=bundle_path,
            compose_project=project,
            compose_files=files,
            working_dir=working_dir,
            service_map=service_map,
            hash_contract=hash_contract,
            observe_probe_specs=probes,
            delivery_handoff_context=build_delivery_handoff_context(metadata),
        )

    def _compose_base_argv(self) -> list[str]:
        argv = ["docker", "compose", "-p", self.compose_project]
        for cf in self.compose_files:
            argv.extend(["-f", cf])
        return argv

    def _observe_service(self, *, resource_id: str, resource_class: str, service: str) -> ObservedResource | None:
        argv = self._compose_base_argv() + ["ps", "-a", "--format", "json"]
        proc = run_checked(argv, cwd=self.working_dir, timeout_sec=self.timeout_sec)
        if proc.returncode != 0:
            return None
        rows = _parse_compose_ps_json(proc.stdout)
        match: dict[str, Any] | None = None
        for row in rows:
            name = str(row.get("Service", row.get("service", ""))).strip()
            if name == service:
                match = row
                break
        if match is None:
            return None
        state = str(match.get("State", match.get("Status", "unknown"))).strip() or "unknown"
        health_raw = match.get("Health", match.get("health", ""))
        health = str(health_raw).strip().lower() if isinstance(health_raw, str) else ""
        health_status: HealthStatus = "unknown"
        if "unhealthy" in health or "unhealthy" in state.lower():
            health_status = "degraded"
        elif state.lower() == "running":
            health_status = "healthy" if not health or "healthy" in health else "degraded"
        elif "exited" in state.lower() or state.lower() in {"dead", "removing"}:
            health_status = "failed"

        conds: list[ObservedHealthCondition] = [
            ObservedHealthCondition(
                type="RuntimeRunning",
                status="true" if state.lower() == "running" else "false",
                reason="compose_ps_state",
                message=state[:512] if state else None,
                last_transition_time=None,
            )
        ]
        if health:
            ch_status: ObservedHealthConditionStatus
            if "healthy" in health and "unhealthy" not in health:
                ch_status = "true"
            elif "unhealthy" in health:
                ch_status = "false"
            else:
                ch_status = "unknown"
            conds.append(
                ObservedHealthCondition(
                    type="ContainerHealth",
                    status=ch_status,
                    reason="compose_ps_health",
                    message=health[:512] if health else None,
                    last_transition_time=None,
                )
            )

        cid = _container_id_from_ps_row(match)
        image_digest = ""
        image_ref = ""
        if cid:
            image_digest, image_ref = _inspect_image_digest(
                container_id=cid, cwd=self.working_dir, timeout_sec=self.timeout_sec
            )

        config_hash = ""
        cfg_proc = run_checked(
            self._compose_base_argv() + ["config", "--hash", service],
            cwd=self.working_dir,
            timeout_sec=self.timeout_sec,
        )
        if cfg_proc.returncode == 0 and cfg_proc.stdout.strip():
            config_hash = cfg_proc.stdout.strip().splitlines()[-1].strip()

        body: dict[str, JSONValue] = {
            "provider": "docker_compose_observe",
            "service": service,
            "state": state,
            "image_digest": image_digest or None,
            "image_ref": image_ref or None,
            "config_hash": config_hash or None,
        }
        observed_hash = _hash_body(contract=self.hash_contract, resource_id=resource_id, body=body)
        probe_conds = evaluate_observe_probes(self.observe_probe_specs)
        conds.extend(probe_conds)
        if health_status == "healthy" and any(p.type.startswith("Probe") and p.status == "false" for p in probe_conds):
            health_status = "degraded"

        payload: dict[str, JSONValue] = {
            "compose_project": self.compose_project,
            "service": service,
            "state": state,
            "container_id": cid,
            "health_conditions": cast(JSONValue, [c.to_json_obj() for c in conds]),
        }
        return ObservedResource(
            resource_id=resource_id,
            resource_class=resource_class,
            observed_hash=observed_hash,
            health_status=health_status,
            payload=payload,
            health_conditions=tuple(conds),
        )

    def observe(self, *, resource_id: str, resource_class: str) -> ObservedResource | None:
        service = self.service_map.get(resource_id, resource_id)
        rc = str(resource_class).strip() or "service"
        return self._observe_service(resource_id=resource_id, resource_class=rc, service=service)

    def list_observed(self) -> tuple[ObservedResource, ...]:
        out: list[ObservedResource] = []
        for resource_id in sorted(self.service_map):
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


_ROLLBACK_UNSUPPORTED = "unsupported_capability:mutating_compose_provider.rollback_manifest_snapshot_by_desired_hash"
_ROLLBACK_ERR_UNSUPPORTED = "rollback_unsupported:manifest_snapshot_missing_for_target_hash"
_ROLLBACK_ERR_FAILED = "rollback_failed:docker_compose_up_failed"
_ROLLBACK_ERR_HASH_MISMATCH = "rollback_failed:target_hash_mismatch_after_manifest_apply"


@dataclass(slots=True)
class DockerComposeApplyProvider(DockerComposeObserveProvider):
    """Opt-in mutating compose: ``docker compose up -d`` after pinned manifest SHA-256 gate.

    Requires env ``AKC_ENABLE_MUTATING_DEPLOYMENT_PROVIDER=1`` and reconciler policy
    ``service.reconcile.apply`` (same authorization path as other providers).
    """

    observe_only: ClassVar[bool] = False
    apply_manifest_path: str = ""
    apply_manifest_sha256_hex: str = ""
    # Optional mapping for deterministic rollback: desired_hash -> pinned compose manifest snapshot.
    # Expected shape under `deployment_provider`:
    #   rollback_apply_manifest_by_desired_hash: {
    #     "<desired_hash>": {"apply_manifest_path": "...", "apply_manifest_sha256": "<sha256_hex>"},
    #   }
    rollback_apply_manifest_by_desired_hash: dict[str, dict[str, str]] = field(default_factory=dict)

    @classmethod
    def from_bundle_metadata(
        cls,
        metadata: Mapping[str, JSONValue],
        *,
        bundle_path: Path,
    ) -> DockerComposeApplyProvider:
        base = DockerComposeObserveProvider.from_bundle_metadata(metadata, bundle_path=bundle_path)
        raw = metadata.get("deployment_provider")
        if not isinstance(raw, Mapping):
            raise ValueError("deployment_provider metadata missing for compose apply provider")
        rel = str(raw.get("apply_manifest_path", "")).strip()
        digest = str(raw.get("apply_manifest_sha256", "")).strip().lower()
        if not rel or not digest:
            raise ValueError(
                "docker_compose_apply requires deployment_provider.apply_manifest_path and apply_manifest_sha256"
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
            compose_project=base.compose_project,
            compose_files=base.compose_files,
            working_dir=base.working_dir,
            service_map=base.service_map,
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
            return None, _ROLLBACK_ERR_UNSUPPORTED
        repo = _repo_root_from_bundle_path(self.bundle_path)
        rel = str(row.get("apply_manifest_path", "")).strip()
        digest = str(row.get("apply_manifest_sha256", "")).strip().lower()
        if not rel or not digest:
            return None, _ROLLBACK_ERR_UNSUPPORTED
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
        wd = self.working_dir.resolve()
        try:
            f_primary = manifest.resolve().relative_to(wd).as_posix()
        except ValueError:
            f_primary = str(manifest.resolve())
        argv: list[str] = ["docker", "compose", "-p", self.compose_project, "-f", f_primary]
        seen: set[str] = {f_primary}
        for cf in self.compose_files:
            c = str(cf).strip().replace("\\", "/")
            if not c or c in seen:
                continue
            argv.extend(["-f", c])
            seen.add(c)
        argv.extend(["up", "-d", "--remove-orphans"])
        proc = run_checked(argv, cwd=self.working_dir, timeout_sec=self.timeout_sec)
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "").strip()[-4096:]
            return ProviderOperationResult(
                operation=operation,
                applied=False,
                observed_hash="",
                health_status="unknown",
                error=f"docker compose up failed (exit {proc.returncode}): {tail}",
                evidence={"mutating": True, "argv_tail": cast(JSONValue, list(argv[-6:]))},
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
            evidence={
                "mutating": True,
                "provider": "docker_compose_apply",
                "compose_project": self.compose_project,
            },
        )

    def rollback(self, *, resource_id: str, resource_class: str, target_hash: str) -> ProviderOperationResult:
        operation = ReconcileOperation(
            operation_id=f"{resource_id}:rollback:{target_hash}",
            operation_type="update",
            target=resource_id,
            payload={"resource_id": resource_id, "resource_class": resource_class, "desired_hash": target_hash},
        )
        manifest, err = self._validated_rollback_manifest_file(target_hash=target_hash)
        if manifest is None:
            return ProviderOperationResult(
                operation=operation,
                applied=False,
                observed_hash="",
                health_status="unknown",
                error=err or _ROLLBACK_ERR_UNSUPPORTED,
                evidence={
                    "mutating": True,
                    "rollback_target_hash": target_hash,
                    "rollback_supported": False,
                    "rollback_outcome": "rollback_unsupported",
                },
            )

        wd = self.working_dir.resolve()
        try:
            f_primary = manifest.resolve().relative_to(wd).as_posix()
        except ValueError:
            f_primary = str(manifest.resolve())
        argv: list[str] = ["docker", "compose", "-p", self.compose_project, "-f", f_primary]
        seen: set[str] = {f_primary}
        for cf in self.compose_files:
            c = str(cf).strip().replace("\\", "/")
            if not c or c in seen:
                continue
            argv.extend(["-f", c])
            seen.add(c)
        argv.extend(["up", "-d", "--remove-orphans"])
        proc = run_checked(argv, cwd=self.working_dir, timeout_sec=self.timeout_sec)
        if proc.returncode != 0:
            tail = (proc.stderr or proc.stdout or "").strip()[-4096:]
            return ProviderOperationResult(
                operation=operation,
                applied=False,
                observed_hash="",
                health_status="unknown",
                error=f"{_ROLLBACK_ERR_FAILED} (exit {proc.returncode}): {tail}",
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
        err = f"{_ROLLBACK_ERR_HASH_MISMATCH} (expected {target_hash}, observed {observed_hash})" if mismatch else None
        return ProviderOperationResult(
            operation=operation,
            applied=not mismatch,
            observed_hash=observed_hash,
            health_status=observed_health,
            error=err,
            evidence={
                "mutating": True,
                "provider": "docker_compose_apply",
                "rollback_target_hash": target_hash,
                "rollback_outcome": ("rollback_failed" if mismatch else "rollback_applied"),
            },
        )
