"""Load coordination JSON from bundle payloads and validate fingerprints (fail-closed)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.compile.artifact_consistency import validate_coordination_orchestration_consistency
from akc.coordination.models import CoordinationParseError, ParsedCoordinationSpec, parse_coordination_obj
from akc.memory.models import JSONValue
from akc.utils.fingerprint import stable_json_fingerprint


def resolve_coordination_artifact_path(*, bundle_path: Path, ref_path: str) -> Path:
    """Resolve ``coordination_ref.path`` the same way as ``system_ir_ref`` (repo-root ``.akc/...``)."""

    raw = str(ref_path).strip()
    if not raw:
        return Path()
    p = Path(raw)
    if p.is_absolute():
        return p.resolve()
    norm = raw.replace("\\", "/")
    if norm.startswith(".akc/"):
        repo_root = bundle_path.parent.parent.parent
        return (repo_root / Path(norm)).resolve()
    return (bundle_path.parent / Path(raw)).resolve()


def _read_json_object(path: Path) -> dict[str, Any]:
    raw = path.read_text(encoding="utf-8")
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        raise CoordinationParseError("coordination file must decode to a JSON object")
    return obj


def _maybe_load_orchestration(*, bundle_path: Path, run_id: str) -> dict[str, Any] | None:
    """Best-effort load of compile-emitted orchestration JSON (same path convention as artifact passes)."""

    rel = f".akc/orchestration/{run_id}.orchestration.json"
    repo_root = bundle_path.parent.parent.parent
    path = (repo_root / rel).resolve()
    if not path.is_file():
        return None
    raw = path.read_text(encoding="utf-8")
    obj = json.loads(raw)
    if not isinstance(obj, dict):
        return None
    return obj


@dataclass(frozen=True, slots=True)
class LoadedCoordination:
    parsed: ParsedCoordinationSpec
    orchestration: dict[str, Any] | None
    fingerprint_sha256: str


def load_coordination_for_bundle(
    *,
    bundle_path: Path,
    payload: Mapping[str, Any],
    enforce_fingerprint: bool = True,
) -> LoadedCoordination | None:
    """Load coordination spec when ``coordination_spec`` or ``coordination_ref`` is present.

    When ``spec_hashes.coordination_spec_sha256`` is set, the loaded bytes must match (fail-closed).
    Optionally validates orchestration ↔ coordination step references when orchestration JSON exists.
    """

    embedded = payload.get("coordination_spec")
    obj: dict[str, Any] | None = None
    if isinstance(embedded, Mapping):
        obj = dict(embedded)

    ref_raw = payload.get("coordination_ref")
    if obj is None and isinstance(ref_raw, Mapping):
        path = str(ref_raw.get("path", "")).strip()
        if not path:
            return None
        resolved = resolve_coordination_artifact_path(bundle_path=bundle_path.expanduser().resolve(), ref_path=path)
        if not resolved.is_file():
            raise CoordinationParseError(f"coordination_ref path {path!r} resolved to missing file {resolved}")
        obj = _read_json_object(resolved)
        ref_fp = ref_raw.get("fingerprint")
        if isinstance(ref_fp, str) and ref_fp.strip():
            got_fp = stable_json_fingerprint(obj)
            if ref_fp.strip().lower() != got_fp:
                raise CoordinationParseError(
                    "coordination_ref.fingerprint does not match coordination file contents "
                    f"(declared {ref_fp.strip().lower()!r}, file {got_fp!r})"
                )

    if obj is None:
        return None

    want_fp = ""
    if enforce_fingerprint:
        specs = payload.get("spec_hashes")
        if isinstance(specs, Mapping):
            raw = specs.get("coordination_spec_sha256")
            if isinstance(raw, str) and raw.strip():
                want_fp = raw.strip().lower()
        got_fp = stable_json_fingerprint(obj)
        if want_fp and got_fp != want_fp:
            raise CoordinationParseError(
                "coordination_spec_sha256 fingerprint mismatch vs loaded coordination JSON "
                f"(expected {want_fp!r}, got {got_fp!r})"
            )

    parsed = parse_coordination_obj(obj)
    run_id = str(payload.get("run_id", parsed.run_id)).strip()
    orch = _maybe_load_orchestration(bundle_path=bundle_path.expanduser().resolve(), run_id=run_id)
    if orch is not None:
        issues = validate_coordination_orchestration_consistency(
            orchestration_obj=orch,
            coordination_obj=obj,
        )
        if issues:
            raise CoordinationParseError("; ".join(issues))

    return LoadedCoordination(
        parsed=parsed,
        orchestration=orch,
        fingerprint_sha256=stable_json_fingerprint(obj),
    )


def coordination_plan_metadata(
    *,
    schedule_layers: tuple[tuple[str, ...], ...],
) -> dict[str, JSONValue]:
    """Stable JSON fragment for bundle/runtime metadata (tests and evidence)."""

    return {
        "coordination_schedule_layer_count": len(schedule_layers),
        "coordination_schedule_layers": [[str(s) for s in layer] for layer in schedule_layers],
    }
