"""Optional tenant/repo policy bundle artifact (``.akc/control/policy_bundle.json``).

v1 is governance visibility only: compiler/runtime do not consume this file.
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator

from akc.control.control_audit import append_control_audit_event
from akc.utils.fingerprint import stable_json_fingerprint

POLICY_BUNDLE_REL_PATH = ".akc/control/policy_bundle.json"
POLICY_BUNDLE_ACTIVATION_REL_PATH = ".akc/control/policy_bundle.activation.json"
GOVERNANCE_PROFILE_RESOLVED_REL_PATH = ".akc/control/governance_profile.resolved.json"


_GOVERNANCE_COMPILE_DEFAULT_KEYS = frozenset(
    {
        "sandbox",
        "strong_lane_preference",
        "policy_mode",
        "replay_mode",
        "promotion_mode",
        "stored_assertion_index",
        "quality_contract_rollout_stage",
        "quality_domain_id",
        "quality_domain_matrix_path",
        "quality_evidence_expectations",
    }
)


def _compile_defaults_tuple_from_governance_mapping(gp: Mapping[str, Any]) -> tuple[tuple[str, Any], ...]:
    """Parse optional org-wide compile defaults from ``governance_profile`` in a policy bundle."""

    raw = gp.get("compile_defaults")
    if not isinstance(raw, dict):
        return ()
    out: list[tuple[str, Any]] = []
    for k, v in raw.items():
        key = str(k).strip()
        if key not in _GOVERNANCE_COMPILE_DEFAULT_KEYS:
            continue
        if v is None:
            continue
        if isinstance(v, str):
            val = v.strip()
            if not val:
                continue
            out.append((key, val))
            continue
        out.append((key, v))
    return tuple(sorted(out))


@dataclass(frozen=True, slots=True)
class GovernanceProfile:
    version: int
    assurance_mode: str
    verifier_coupling_default: bool
    verifier_enforcement: str
    provider_allowlist: tuple[str, ...]
    max_errors_before_block: int
    rollout_stage: str | None
    # Org-wide compile defaults (optional); overlays rollout_stage-derived keys in profile resolution.
    compile_defaults: tuple[tuple[str, Any], ...]
    source: str

    def to_json_obj(self) -> dict[str, Any]:
        compile_defaults_obj = dict(sorted(self.compile_defaults)) if self.compile_defaults else {}
        fp_payload = {
            "version": int(self.version),
            "assurance_mode": self.assurance_mode,
            "verifier_coupling_default": bool(self.verifier_coupling_default),
            "verifier_enforcement": self.verifier_enforcement,
            "provider_allowlist": list(self.provider_allowlist),
            "max_errors_before_block": int(self.max_errors_before_block),
            "rollout_stage": self.rollout_stage,
            "compile_defaults": compile_defaults_obj,
        }
        return {
            "version": int(self.version),
            "assurance_mode": self.assurance_mode,
            "verifier_coupling_default": bool(self.verifier_coupling_default),
            "verifier_enforcement": self.verifier_enforcement,
            "provider_allowlist": list(self.provider_allowlist),
            "escalation_thresholds": {"max_errors_before_block": int(self.max_errors_before_block)},
            "rollout_stage": self.rollout_stage,
            "compile_defaults": compile_defaults_obj,
            "source": self.source,
            "fingerprint_sha256": stable_json_fingerprint(fp_payload),
        }


@dataclass(frozen=True, slots=True)
class PolicyBundleProvenance:
    revision: str | None
    root_owner: str | None
    signature_key_id: str | None
    signature_algorithm: str | None
    signature_value: str | None


@dataclass(frozen=True, slots=True)
class PolicyBundleShardState:
    shard_id: str
    outputs_root: str
    tenant_id: str
    repo_id: str
    bundle_rel_path: str
    bundle_exists: bool
    fingerprint_sha256: str | None
    rollout_stage: str | None
    revision_id: str | None
    provenance: PolicyBundleProvenance
    activation: dict[str, Any] | None
    diverged: bool
    drift_reasons: tuple[str, ...]

    def to_json_obj(self) -> dict[str, Any]:
        return {
            "shard_id": self.shard_id,
            "outputs_root": self.outputs_root,
            "tenant_id": self.tenant_id,
            "repo_id": self.repo_id,
            "bundle_rel_path": self.bundle_rel_path,
            "bundle_exists": self.bundle_exists,
            "fingerprint_sha256": self.fingerprint_sha256,
            "rollout_stage": self.rollout_stage,
            "revision_id": self.revision_id,
            "provenance": {
                "revision": self.provenance.revision,
                "root_owner": self.provenance.root_owner,
                "signature_key_id": self.provenance.signature_key_id,
                "signature_algorithm": self.provenance.signature_algorithm,
                "signature_value": self.provenance.signature_value,
            },
            "activation": self.activation,
            "diverged": self.diverged,
            "drift_reasons": list(self.drift_reasons),
        }


def policy_bundle_schema() -> dict[str, Any]:
    """Load the frozen JSON Schema for :func:`validate_policy_bundle_document`."""

    path = Path(__file__).resolve().parent / "schemas" / "policy_bundle.v1.schema.json"
    loaded = json.loads(path.read_text(encoding="utf-8"))
    return cast(dict[str, Any], loaded)


def validate_policy_bundle_document(obj: dict[str, Any]) -> list[str]:
    """Return human-readable validation messages (empty if valid)."""

    schema = policy_bundle_schema()
    v = Draft202012Validator(schema)
    return [f"{list(e.path)}: {e.message}" for e in v.iter_errors(obj)]


def canonical_policy_bundle_bytes(obj: dict[str, Any]) -> bytes:
    """Canonical on-disk bytes used for stable fingerprinting and shard writes."""

    return (json.dumps(obj, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8")


def fingerprint_policy_bundle_bytes(data: bytes) -> str:
    """SHA-256 hex digest of the canonical on-disk bytes."""

    return hashlib.sha256(data).hexdigest()


def index_fields_from_document(obj: dict[str, Any]) -> tuple[str | None, str | None]:
    """Rollout stage and optional revision label for the operations index."""

    rs = obj.get("rollout_stage")
    rollout = str(rs).strip() if isinstance(rs, str) else None
    rev_raw = obj.get("revision_id")
    revision = str(rev_raw).strip() if rev_raw is not None else None
    if revision == "":
        revision = None
    if revision is not None and len(revision) > 256:
        revision = revision[:256]
    return rollout, revision


def provenance_from_document(obj: dict[str, Any]) -> PolicyBundleProvenance:
    """Extract OPA-like provenance cells from the bundle document."""

    prov = obj.get("provenance")
    if not isinstance(prov, dict):
        prov = {}
    revision_raw = prov.get("revision", obj.get("revision_id"))
    revision = str(revision_raw).strip() if revision_raw is not None else None
    if revision == "":
        revision = None
    owner_raw = prov.get("root_owner")
    root_owner = str(owner_raw).strip() if owner_raw is not None else None
    if root_owner == "":
        root_owner = None
    sig = prov.get("signature") if isinstance(prov.get("signature"), dict) else {}
    key_raw = sig.get("key_id") if isinstance(sig, dict) else None
    alg_raw = sig.get("algorithm") if isinstance(sig, dict) else None
    val_raw = sig.get("value") if isinstance(sig, dict) else None
    key_id = str(key_raw).strip() if key_raw is not None else None
    signature_algorithm = str(alg_raw).strip() if alg_raw is not None else None
    signature_value = str(val_raw).strip() if val_raw is not None else None
    if key_id == "":
        key_id = None
    if signature_algorithm == "":
        signature_algorithm = None
    if signature_value == "":
        signature_value = None
    return PolicyBundleProvenance(
        revision=revision,
        root_owner=root_owner,
        signature_key_id=key_id,
        signature_algorithm=signature_algorithm,
        signature_value=signature_value,
    )


def load_policy_bundle_json_bytes(data: bytes) -> dict[str, Any]:
    """Parse JSON object from bytes; raises ``json.JSONDecodeError``, ``UnicodeDecodeError``."""

    text = data.decode("utf-8")
    loaded = json.loads(text)
    if not isinstance(loaded, dict):
        raise ValueError("policy bundle root must be a JSON object")
    return cast(dict[str, Any], loaded)


def default_policy_bundle_path_for_scope(scope_root: Path) -> Path:
    """Resolved path under a tenant/repo outputs scope."""

    return (Path(scope_root).expanduser().resolve() / ".akc" / "control" / "policy_bundle.json").resolve()


def default_policy_bundle_activation_path_for_scope(scope_root: Path) -> Path:
    return (Path(scope_root).expanduser().resolve() / ".akc" / "control" / "policy_bundle.activation.json").resolve()


def default_governance_profile_path_for_scope(scope_root: Path) -> Path:
    return (Path(scope_root).expanduser().resolve() / GOVERNANCE_PROFILE_RESOLVED_REL_PATH).resolve()


def governance_profile_from_document(obj: dict[str, Any]) -> GovernanceProfile:
    gp_raw = obj.get("governance_profile")
    gp = gp_raw if isinstance(gp_raw, dict) else {}
    allow_raw = gp.get("provider_allowlist")
    allow: tuple[str, ...] = (
        tuple(sorted({str(x).strip() for x in allow_raw if str(x).strip()})) if isinstance(allow_raw, list) else ()
    )
    esc = gp.get("escalation_thresholds")
    esc_obj = esc if isinstance(esc, dict) else {}
    raw_threshold = esc_obj.get("max_errors_before_block", 1)
    if isinstance(raw_threshold, bool):
        raw_threshold = 1
    threshold = int(raw_threshold) if isinstance(raw_threshold, (int, float)) else 1
    if threshold < 0:
        threshold = 0
    rollout_raw = obj.get("rollout_stage")
    rollout = str(rollout_raw).strip() if isinstance(rollout_raw, str) else None
    source = "policy_bundle.governance_profile" if isinstance(gp_raw, dict) else "policy_bundle.defaults"
    compile_defaults = _compile_defaults_tuple_from_governance_mapping(gp)
    return GovernanceProfile(
        version=int(gp.get("version", 1)) if isinstance(gp.get("version"), (int, float)) else 1,
        assurance_mode=str(gp.get("assurance_mode", "hybrid")).strip() or "hybrid",
        verifier_coupling_default=bool(gp.get("verifier_coupling_default", True)),
        verifier_enforcement=str(gp.get("verifier_enforcement", "auto")).strip() or "auto",
        provider_allowlist=allow,
        max_errors_before_block=threshold,
        rollout_stage=rollout,
        compile_defaults=compile_defaults,
        source=source,
    )


def resolve_governance_profile_for_scope(scope_root: Path) -> GovernanceProfile | None:
    bundle_path = default_policy_bundle_path_for_scope(scope_root)
    if not bundle_path.is_file():
        return None
    try:
        doc = load_policy_bundle_json_bytes(bundle_path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
        return None
    errs = validate_policy_bundle_document(doc)
    if errs:
        return None
    return governance_profile_from_document(doc)


def write_resolved_governance_profile_for_scope(
    *,
    scope_root: Path,
    profile: GovernanceProfile,
    now_ms: int,
    source_bundle_fingerprint_sha256: str,
) -> dict[str, Any]:
    path = default_governance_profile_path_for_scope(scope_root)
    payload = {
        "schema_kind": "akc_governance_profile_resolved",
        "version": 1,
        "resolved_at_ms": int(now_ms),
        "source_bundle_rel_path": POLICY_BUNDLE_REL_PATH,
        "source_bundle_fingerprint_sha256": source_bundle_fingerprint_sha256,
        "profile": profile.to_json_obj(),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes((json.dumps(payload, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8"))
    tmp.replace(path)
    return payload


def _scope_root(*, outputs_root: Path, tenant_id: str, repo_id: str) -> Path:
    return (outputs_root / tenant_id.strip() / str(repo_id).strip()).resolve()


def _shard_accepts_tenant(shard: Any, *, tenant_id: str) -> bool:
    allowlist = tuple(str(x).strip() for x in getattr(shard, "tenant_allowlist", ()) if str(x).strip())
    return "*" in allowlist or tenant_id.strip() in allowlist


def _load_json_obj(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(raw, dict):
        return None
    return cast(dict[str, Any], raw)


def _load_activation_marker_for_scope(scope_root: Path) -> dict[str, Any] | None:
    path = default_policy_bundle_activation_path_for_scope(scope_root)
    if not path.is_file():
        return None
    return _load_json_obj(path)


def _write_activation_marker_for_scope(
    *,
    scope_root: Path,
    tenant_id: str,
    repo_id: str,
    actor: str,
    fingerprint_sha256: str,
    revision_id: str | None,
    rollout_stage: str | None,
    provenance: PolicyBundleProvenance,
    request_id: str,
    now_ms: int,
) -> tuple[dict[str, Any], bool, str | None]:
    path = default_policy_bundle_activation_path_for_scope(scope_root)
    prev = _load_activation_marker_for_scope(scope_root)
    previous_revision = (
        str(prev.get("bundle_revision_id")).strip()
        if isinstance(prev, dict) and prev.get("bundle_revision_id")
        else None
    )
    rollback_from = previous_revision if previous_revision and previous_revision != revision_id else None
    marker = {
        "schema_kind": "akc_policy_bundle_activation",
        "version": 1,
        "tenant_id": tenant_id.strip(),
        "repo_id": repo_id.strip(),
        "activated_at_ms": int(now_ms),
        "activated_by": actor,
        "request_id": request_id,
        "bundle_rel_path": POLICY_BUNDLE_REL_PATH,
        "bundle_fingerprint_sha256": fingerprint_sha256,
        "bundle_revision_id": revision_id,
        "rollout_stage": rollout_stage,
        "rollback_marker": bool(rollback_from),
        "rollback_from_revision_id": rollback_from,
        "provenance": {
            "revision": provenance.revision,
            "root_owner": provenance.root_owner,
            "signature_key_id": provenance.signature_key_id,
            "signature_algorithm": provenance.signature_algorithm,
            "signature_value": provenance.signature_value,
        },
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes((json.dumps(marker, sort_keys=True, ensure_ascii=False) + "\n").encode("utf-8"))
    tmp.replace(path)
    return marker, bool(rollback_from), rollback_from


def distribute_policy_bundle_document(
    *,
    shards: Sequence[Any],
    tenant_id: str,
    repo_id: str,
    document: dict[str, Any],
    actor: str | None = None,
    activate: bool = False,
    request_id: str | None = None,
    now_ms: int | None = None,
) -> list[dict[str, Any]]:
    """Write one validated bundle revision to every tenant-eligible shard."""

    errs = validate_policy_bundle_document(document)
    if errs:
        raise ValueError("; ".join(errs))
    canonical = canonical_policy_bundle_bytes(document)
    fingerprint = fingerprint_policy_bundle_bytes(canonical)
    rollout_stage, revision_id = index_fields_from_document(document)
    provenance = provenance_from_document(document)
    who = str(actor or os.environ.get("USER") or "unknown").strip() or "unknown"
    rid = str(request_id or uuid.uuid4())
    ts_ms = int(time.time() * 1000) if now_ms is None else int(now_ms)

    writes: list[dict[str, Any]] = []
    for shard in shards:
        if not _shard_accepts_tenant(shard, tenant_id=tenant_id):
            continue
        outputs_root = Path(shard.outputs_root).expanduser().resolve()
        scope = _scope_root(outputs_root=outputs_root, tenant_id=tenant_id, repo_id=repo_id)
        dest = default_policy_bundle_path_for_scope(scope)
        dest.parent.mkdir(parents=True, exist_ok=True)
        tmp = dest.with_suffix(dest.suffix + ".tmp")
        tmp.write_bytes(canonical)
        tmp.replace(dest)
        profile = governance_profile_from_document(document)
        resolved_profile = write_resolved_governance_profile_for_scope(
            scope_root=scope,
            profile=profile,
            now_ms=ts_ms,
            source_bundle_fingerprint_sha256=fingerprint,
        )

        activation_marker: dict[str, Any] | None = None
        rollback_marker = False
        rollback_from: str | None = None
        if activate:
            activation_marker, rollback_marker, rollback_from = _write_activation_marker_for_scope(
                scope_root=scope,
                tenant_id=tenant_id,
                repo_id=repo_id,
                actor=who,
                fingerprint_sha256=fingerprint,
                revision_id=revision_id,
                rollout_stage=rollout_stage,
                provenance=provenance,
                request_id=rid,
                now_ms=ts_ms,
            )

        append_control_audit_event(
            outputs_root=outputs_root,
            tenant_id=tenant_id,
            action="policy_bundle.distribute",
            actor=who,
            request_id=rid,
            details={
                "shard_id": str(shard.id).strip(),
                "repo_id": str(repo_id).strip(),
                "rel_path": POLICY_BUNDLE_REL_PATH,
                "fingerprint_sha256": fingerprint,
                "revision_id": revision_id,
                "rollout_stage": rollout_stage,
                "governance_profile_fingerprint_sha256": str(
                    cast(dict[str, Any], resolved_profile.get("profile", {})).get("fingerprint_sha256", "")
                ).strip()
                or None,
                "provenance": {
                    "revision": provenance.revision,
                    "root_owner": provenance.root_owner,
                    "signature_key_id": provenance.signature_key_id,
                    "signature_algorithm": provenance.signature_algorithm,
                },
                "activation_requested": activate,
                "rollback_marker": rollback_marker,
                "rollback_from_revision_id": rollback_from,
            },
        )
        if activation_marker is not None:
            append_control_audit_event(
                outputs_root=outputs_root,
                tenant_id=tenant_id,
                action="policy_bundle.activate",
                actor=who,
                request_id=rid,
                details={
                    "shard_id": str(shard.id).strip(),
                    "repo_id": str(repo_id).strip(),
                    "bundle_fingerprint_sha256": fingerprint,
                    "bundle_revision_id": revision_id,
                    "rollout_stage": rollout_stage,
                    "rollback_marker": rollback_marker,
                    "rollback_from_revision_id": rollback_from,
                    "activation_rel_path": POLICY_BUNDLE_ACTIVATION_REL_PATH,
                },
            )

        writes.append(
            {
                "shard_id": str(shard.id).strip(),
                "outputs_root": str(outputs_root),
                "tenant_id": tenant_id.strip(),
                "repo_id": repo_id.strip(),
                "bundle_path": str(dest),
                "fingerprint_sha256": fingerprint,
                "revision_id": revision_id,
                "rollout_stage": rollout_stage,
                "governance_profile_path": str(default_governance_profile_path_for_scope(scope)),
                "governance_profile_fingerprint_sha256": str(
                    cast(dict[str, Any], resolved_profile.get("profile", {})).get("fingerprint_sha256", "")
                ).strip()
                or None,
                "activation_requested": activate,
                "rollback_marker": rollback_marker,
                "rollback_from_revision_id": rollback_from,
                "request_id": rid,
            }
        )
    return writes


def policy_bundle_drift_report(
    *,
    shards: Sequence[Any],
    tenant_id: str,
    repo_id: str,
    generated_at_ms: int | None = None,
) -> dict[str, Any]:
    """Cross-shard drift view over policy bundle revision/fingerprint and activation markers."""

    ts_ms = int(time.time() * 1000) if generated_at_ms is None else int(generated_at_ms)
    present_versions: list[str] = []
    candidates: list[
        tuple[str, str | None, str | None, str | None, PolicyBundleProvenance, dict[str, Any] | None, str]
    ] = []
    for shard in shards:
        if not _shard_accepts_tenant(shard, tenant_id=tenant_id):
            continue
        shard_id = str(shard.id).strip()
        outputs_root = Path(shard.outputs_root).expanduser().resolve()
        scope = _scope_root(outputs_root=outputs_root, tenant_id=tenant_id, repo_id=repo_id)
        bundle_path = default_policy_bundle_path_for_scope(scope)
        activation = _load_activation_marker_for_scope(scope)
        if not bundle_path.is_file():
            candidates.append(
                (
                    shard_id,
                    None,
                    None,
                    None,
                    PolicyBundleProvenance(None, None, None, None, None),
                    activation,
                    str(outputs_root),
                )
            )
            continue
        try:
            data = bundle_path.read_bytes()
            doc = load_policy_bundle_json_bytes(data)
        except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError):
            candidates.append(
                (
                    shard_id,
                    None,
                    None,
                    None,
                    PolicyBundleProvenance(None, None, None, None, None),
                    activation,
                    str(outputs_root),
                )
            )
            continue
        errs = validate_policy_bundle_document(doc)
        if errs:
            candidates.append(
                (
                    shard_id,
                    None,
                    None,
                    None,
                    PolicyBundleProvenance(None, None, None, None, None),
                    activation,
                    str(outputs_root),
                )
            )
            continue
        rollout, revision = index_fields_from_document(doc)
        fp = fingerprint_policy_bundle_bytes(data)
        prov = provenance_from_document(doc)
        version_key = revision or fp
        if version_key is not None:
            present_versions.append(version_key)
        candidates.append((shard_id, fp, rollout, revision, prov, activation, str(outputs_root)))

    majority: str | None = None
    if present_versions:
        majority = Counter(present_versions).most_common(1)[0][0]

    states: list[PolicyBundleShardState] = []
    distinct_versions = sorted(set(present_versions))
    for shard_id, fp_opt, rollout_opt, revision_opt, prov, activation, outputs_root_str in candidates:
        reasons: list[str] = []
        exists = fp_opt is not None
        key: str | None = revision_opt or fp_opt
        if not exists:
            reasons.append("missing_bundle")
        if majority is not None and key is not None and key != majority:
            reasons.append("version_mismatch")
        if isinstance(activation, dict):
            act_rev = (
                str(activation.get("bundle_revision_id")).strip() if activation.get("bundle_revision_id") else None
            )
            act_fp = (
                str(activation.get("bundle_fingerprint_sha256")).strip()
                if activation.get("bundle_fingerprint_sha256")
                else None
            )
            if act_rev and revision_opt and act_rev != revision_opt:
                reasons.append("activation_revision_mismatch")
            if act_fp and fp_opt and act_fp != fp_opt:
                reasons.append("activation_fingerprint_mismatch")
        states.append(
            PolicyBundleShardState(
                shard_id=shard_id,
                outputs_root=outputs_root_str,
                tenant_id=tenant_id.strip(),
                repo_id=repo_id.strip(),
                bundle_rel_path=POLICY_BUNDLE_REL_PATH,
                bundle_exists=exists,
                fingerprint_sha256=fp_opt,
                rollout_stage=rollout_opt,
                revision_id=revision_opt,
                provenance=prov,
                activation=activation,
                diverged=bool(reasons),
                drift_reasons=tuple(reasons),
            )
        )

    report = {
        "schema_kind": "akc_policy_bundle_drift_report",
        "version": 1,
        "generated_at_ms": ts_ms,
        "tenant_id": tenant_id.strip(),
        "repo_id": repo_id.strip(),
        "shard_count": len(states),
        "distinct_versions": distinct_versions,
        "reference_version": majority,
        "drift_detected": any(s.diverged for s in states),
        "shards": [s.to_json_obj() for s in states],
    }
    return report
