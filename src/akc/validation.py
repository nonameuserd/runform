from __future__ import annotations

import hashlib
import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, cast

from jsonschema import Draft202012Validator

from akc.artifacts.contracts import apply_schema_envelope
from akc.artifacts.validate import validate_artifact_json
from akc.intent.models import OperationalValidityParams
from akc.memory.models import JSONValue
from akc.run.manifest import RuntimeEvidenceRecord
from akc.utils.fingerprint import stable_json_fingerprint

if TYPE_CHECKING:
    from akc.cli.project_config import AkcProjectConfig

VALIDATOR_BINDINGS_SCHEMA_KIND = "validator_bindings"
VALIDATOR_BINDINGS_SCHEMA_VERSION = 1
DEFAULT_VALIDATOR_BINDINGS_REL_PATH = Path("configs") / "validation" / "validator_bindings.v1.yaml"

OBSERVABILITY_QUERY_RESULT_EVIDENCE_TYPE = "akc_observability_query_result"
MOBILE_JOURNEY_RESULT_EVIDENCE_TYPE = "akc_mobile_journey_result"
DEVICE_CAPTURE_RESULT_EVIDENCE_TYPE = "akc_device_capture_result"
SUPPORTED_VALIDATOR_ADAPTER_ID = "local_depth"

ValidatorBindingKind = Literal[
    "logql_query",
    "promql_query",
    "traceql_query",
    "maestro_flow",
    "android_helper",
    "ios_simulator_helper",
]

ValidatorResultArtifactKind = Literal["observability_query_result", "mobile_journey_result", "device_capture_result"]

_OBS_QUERY_KINDS: frozenset[str] = frozenset({"logql_query", "promql_query", "traceql_query"})
_BINDING_READ_PATH_KEYS: frozenset[str] = frozenset({"flow_path", "apk_path", "app_path"})
_HELPER_CAPTURE_OPS: dict[str, frozenset[str]] = {
    "android_helper": frozenset({"screenshot", "screenrecord", "logcat_export"}),
    "ios_simulator_helper": frozenset({"screenshot", "record_video"}),
}
_VALIDATOR_ID_PATTERN = re.compile(r"[^A-Za-z0-9_.-]+")


class ValidatorBindingsConfigError(ValueError):
    """Raised when a validator binding registry cannot be loaded or validated."""


@dataclass(frozen=True, slots=True)
class ValidatorArtifactRef:
    path: str
    media_type: str
    sha256: str

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "path": self.path,
            "media_type": self.media_type,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class ValidatorBinding:
    binding_id: str
    kind: ValidatorBindingKind
    config: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ValidatorExecutionResult:
    evidence: tuple[RuntimeEvidenceRecord, ...]
    artifact_paths: tuple[str, ...]
    binding_results: tuple[dict[str, JSONValue], ...]


def validator_bindings_schema() -> dict[str, Any]:
    path = Path(__file__).resolve().parent / "control" / "schemas" / "validator_bindings.v1.schema.json"
    return cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))


def resolve_validator_bindings_path(
    *,
    cwd: Path,
    project: AkcProjectConfig | None,
    cli_value: str | None,
) -> Path | None:
    if cli_value:
        return (
            (cwd / cli_value).expanduser().resolve()
            if not Path(cli_value).expanduser().is_absolute()
            else Path(cli_value).expanduser().resolve()
        )
    if project is not None and project.validation_bindings_path:
        candidate = Path(project.validation_bindings_path).expanduser()
        if not candidate.is_absolute():
            candidate = (cwd / candidate).resolve()
        return candidate
    candidate = (cwd / DEFAULT_VALIDATOR_BINDINGS_REL_PATH).resolve()
    return candidate if candidate.is_file() else None


def _read_config_obj(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    text = path.read_text(encoding="utf-8")
    if suffix in {".yaml", ".yml"}:
        try:
            import yaml
        except ImportError:
            try:
                loaded = json.loads(text)
            except json.JSONDecodeError as exc:  # pragma: no cover - optional dependency guard
                raise RuntimeError("validator bindings YAML requires PyYAML") from exc
        else:
            loaded = yaml.safe_load(text)
    else:
        loaded = json.loads(text)
    if not isinstance(loaded, dict):
        raise ValidatorBindingsConfigError("validator bindings config must be an object")
    return loaded


def _normalize_headers(raw: Any, *, context: str) -> dict[str, str]:
    if raw is None:
        return {}
    if not isinstance(raw, Mapping):
        raise ValidatorBindingsConfigError(f"{context} must be an object when set")
    out: dict[str, str] = {}
    for key, value in raw.items():
        ks = str(key).strip()
        vs = str(value).strip()
        if not ks or any(ch in ks for ch in ("\r", "\n")) or any(ch in vs for ch in ("\r", "\n")):
            raise ValidatorBindingsConfigError(f"{context} contains an invalid header entry")
        out[ks] = vs
    return out


def _normalize_path_value(raw: Any, *, base_dir: Path, context: str) -> str:
    s = str(raw or "").strip()
    if not s:
        raise ValidatorBindingsConfigError(f"{context} must be a non-empty path")
    p = Path(s).expanduser()
    p = (base_dir / p).resolve() if not p.is_absolute() else p.resolve()
    return str(p)


def _normalize_int(raw: Any, *, default: int, minimum: int, context: str) -> int:
    if raw is None:
        return default
    if isinstance(raw, bool) or not isinstance(raw, (int, float)):
        raise ValidatorBindingsConfigError(f"{context} must be an integer >= {minimum}")
    out = int(raw)
    if out < minimum:
        raise ValidatorBindingsConfigError(f"{context} must be >= {minimum}")
    return out


def _normalize_str_list(raw: Any, *, context: str) -> tuple[str, ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValidatorBindingsConfigError(f"{context} must be a non-empty array of strings")
    out = tuple(str(item).strip() for item in raw if str(item).strip())
    if not out:
        raise ValidatorBindingsConfigError(f"{context} must be a non-empty array of strings")
    return out


def _normalize_binding(*, binding_id: str, raw: Mapping[str, Any], base_dir: Path) -> ValidatorBinding:
    kind_raw = str(raw.get("kind", "")).strip()
    if kind_raw not in _OBS_QUERY_KINDS | {"maestro_flow", "android_helper", "ios_simulator_helper"}:
        raise ValidatorBindingsConfigError(f"binding {binding_id!r} has unsupported kind {kind_raw!r}")
    kind = cast(ValidatorBindingKind, kind_raw)
    cfg = dict(raw)
    cfg["kind"] = kind
    if kind in _OBS_QUERY_KINDS:
        url = str(cfg.get("url", "")).strip()
        query = str(cfg.get("query", "")).strip()
        target = str(cfg.get("target", "")).strip() or binding_id
        method = str(cfg.get("method", "GET")).strip().upper() or "GET"
        if method not in {"GET", "POST"}:
            raise ValidatorBindingsConfigError(f"binding {binding_id!r} method must be GET or POST")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValidatorBindingsConfigError(f"binding {binding_id!r} url must be http(s)")
        if not query:
            raise ValidatorBindingsConfigError(f"binding {binding_id!r} query must be non-empty")
        cfg = {
            "kind": kind,
            "url": url,
            "query": query,
            "target": target,
            "method": method,
            "timeout_ms": _normalize_int(
                cfg.get("timeout_ms"),
                default=5000,
                minimum=1,
                context=f"{binding_id}.timeout_ms",
            ),
            "query_param": str(cfg.get("query_param", "query")).strip() or "query",
            "headers": _normalize_headers(cfg.get("headers"), context=f"{binding_id}.headers"),
        }
        return ValidatorBinding(binding_id=binding_id, kind=kind, config=cfg)
    if kind == "maestro_flow":
        platform = str(cfg.get("platform", "")).strip().lower()
        if platform not in {"android", "ios"}:
            raise ValidatorBindingsConfigError(f"binding {binding_id!r} platform must be android or ios")
        cfg = {
            "kind": kind,
            "platform": platform,
            "journey_id": str(cfg.get("journey_id", "")).strip() or binding_id,
            "flow_path": _normalize_path_value(
                cfg.get("flow_path"),
                base_dir=base_dir,
                context=f"{binding_id}.flow_path",
            ),
            "device_id": str(cfg.get("device_id", "")).strip() or None,
            "app_id": str(cfg.get("app_id", "")).strip() or None,
            "timeout_ms": _normalize_int(
                cfg.get("timeout_ms"),
                default=300_000,
                minimum=1,
                context=f"{binding_id}.timeout_ms",
            ),
        }
        return ValidatorBinding(binding_id=binding_id, kind=kind, config=cfg)
    if kind == "android_helper":
        operation = str(cfg.get("operation", "")).strip()
        if operation not in {
            "install",
            "clear_data",
            "grant_permissions",
            "start_activity",
            "screenshot",
            "screenrecord",
            "logcat_export",
        }:
            raise ValidatorBindingsConfigError(f"binding {binding_id!r} android_helper operation is invalid")
        normalized: dict[str, Any] = {
            "kind": kind,
            "platform": "android",
            "operation": operation,
            "device_id": str(cfg.get("device_id", "")).strip() or None,
            "journey_id": str(cfg.get("journey_id", "")).strip() or None,
        }
        if operation == "install":
            normalized["apk_path"] = _normalize_path_value(
                cfg.get("apk_path"),
                base_dir=base_dir,
                context=f"{binding_id}.apk_path",
            )
        elif operation == "clear_data":
            normalized["package_name"] = str(cfg.get("package_name", "")).strip()
        elif operation == "grant_permissions":
            normalized["package_name"] = str(cfg.get("package_name", "")).strip()
            normalized["permissions"] = list(
                _normalize_str_list(cfg.get("permissions"), context=f"{binding_id}.permissions")
            )
        elif operation == "start_activity":
            normalized["activity"] = str(cfg.get("activity", "")).strip()
        elif operation in {"screenshot", "screenrecord", "logcat_export"}:
            name_raw = str(cfg.get("artifact_name", "")).strip() or operation
            normalized["artifact_name"] = _safe_artifact_segment(name_raw, context=f"{binding_id}.artifact_name")
            if operation == "screenrecord":
                normalized["duration_s"] = _normalize_int(
                    cfg.get("duration_s"), default=15, minimum=1, context=f"{binding_id}.duration_s"
                )
        for required in ("package_name", "activity"):
            if required in normalized and not str(normalized[required]).strip():
                raise ValidatorBindingsConfigError(f"binding {binding_id!r} {required} must be non-empty")
        return ValidatorBinding(binding_id=binding_id, kind=kind, config=normalized)
    operation = str(cfg.get("operation", "")).strip()
    if operation not in {"boot", "erase", "install_app", "launch_app", "screenshot", "record_video"}:
        raise ValidatorBindingsConfigError(f"binding {binding_id!r} ios_simulator_helper operation is invalid")
    device_id = str(cfg.get("device_id", "")).strip() or "booted"
    normalized = {
        "kind": kind,
        "platform": "ios",
        "operation": operation,
        "device_id": device_id,
        "journey_id": str(cfg.get("journey_id", "")).strip() or None,
    }
    if operation == "install_app":
        normalized["app_path"] = _normalize_path_value(
            cfg.get("app_path"),
            base_dir=base_dir,
            context=f"{binding_id}.app_path",
        )
    elif operation == "launch_app":
        bundle_id = str(cfg.get("bundle_id", "")).strip()
        if not bundle_id:
            raise ValidatorBindingsConfigError(f"binding {binding_id!r} bundle_id must be non-empty")
        normalized["bundle_id"] = bundle_id
    elif operation in {"screenshot", "record_video"}:
        name_raw = str(cfg.get("artifact_name", "")).strip() or operation
        normalized["artifact_name"] = _safe_artifact_segment(name_raw, context=f"{binding_id}.artifact_name")
        if operation == "record_video":
            normalized["duration_s"] = _normalize_int(
                cfg.get("duration_s"), default=15, minimum=1, context=f"{binding_id}.duration_s"
            )
    return ValidatorBinding(binding_id=binding_id, kind=kind, config=normalized)


def load_validator_bindings(*, path: Path | None) -> dict[str, ValidatorBinding]:
    if path is None:
        return {}
    if not path.is_file():
        raise ValidatorBindingsConfigError(f"validator bindings file does not exist: {path}")
    loaded = _read_config_obj(path)
    issues = list(Draft202012Validator(validator_bindings_schema()).iter_errors(loaded))
    if issues:
        first = sorted(issues, key=lambda err: (list(err.path), err.message))[0]
        loc = "/" + "/".join(str(p) for p in first.path) if first.path else "/"
        raise ValidatorBindingsConfigError(f"validator bindings schema invalid at {loc}: {first.message}")
    raw_bindings = loaded.get("bindings", {})
    if not isinstance(raw_bindings, Mapping):
        raise ValidatorBindingsConfigError("validator bindings config bindings must be an object")
    out: dict[str, ValidatorBinding] = {}
    base_dir = path.parent.resolve()
    for raw_id, binding_raw in raw_bindings.items():
        binding_id = str(raw_id).strip()
        if not binding_id or not isinstance(binding_raw, Mapping):
            raise ValidatorBindingsConfigError("validator binding ids must map to objects")
        out[binding_id] = _normalize_binding(binding_id=binding_id, raw=binding_raw, base_dir=base_dir)
    return out


def collect_binding_ids_from_specs(
    specs: Sequence[tuple[str, OperationalValidityParams]],
) -> tuple[str, ...]:
    out: set[str] = set()
    for _sc_id, params in specs:
        for sig in params.signals:
            stub = sig.binding_stub
            if stub:
                out.add(stub)
    return tuple(sorted(out))


def _stub_usage_map(
    specs: Sequence[tuple[str, OperationalValidityParams]],
) -> dict[str, dict[str, Any]]:
    usage: dict[str, dict[str, Any]] = {}
    for sc_id, params in specs:
        for sig in params.signals:
            stub = sig.binding_stub
            if not stub:
                continue
            current = usage.setdefault(
                stub,
                {
                    "success_criterion_ids": [],
                    "evidence_type": sig.evidence_type,
                },
            )
            current["success_criterion_ids"].append(sc_id)
    for value in usage.values():
        value["success_criterion_ids"] = sorted({str(x) for x in value["success_criterion_ids"]})
    return usage


def _safe_binding_slug(binding_id: str) -> str:
    return _VALIDATOR_ID_PATTERN.sub("_", binding_id).strip("._-") or "binding"


def _safe_artifact_segment(raw: Any, *, context: str) -> str:
    """Reject path traversal in binder-controlled filename segments (screenshots, videos, logs)."""

    s = str(raw or "").strip()
    if not s:
        raise ValidatorBindingsConfigError(f"{context} must be non-empty")
    if any(sep in s for sep in ("/", "\\")) or s in {".", ".."}:
        raise ValidatorBindingsConfigError(f"{context} must not contain path separators or '.' components")
    return s


def _iter_binding_read_paths(binding: ValidatorBinding) -> tuple[tuple[str, Path], ...]:
    """Filesystem inputs executed or handed to device tooling (Maestro flow, APK, iOS app bundle)."""

    cfg = binding.config
    out: list[tuple[str, Path]] = []
    for key in _BINDING_READ_PATH_KEYS:
        raw = cfg.get(key)
        if raw is None:
            continue
        s = str(raw).strip()
        if not s:
            continue
        out.append((key, Path(s).expanduser().resolve()))
    return tuple(out)


def _require_read_paths_under_scope_root(
    *,
    binding_id: str,
    paths: Sequence[tuple[str, Path]],
    scope_root: Path,
) -> None:
    root = scope_root.expanduser().resolve()
    for label, resolved in paths:
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValidatorBindingsConfigError(
                f"binding {binding_id!r} {label} ({resolved}) must be under scope_root ({root})"
            ) from exc


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _artifact_rel_path(*, scope_root: Path, path: Path) -> str:
    return path.resolve().relative_to(scope_root.resolve()).as_posix()


def _write_attachment_bytes(
    *,
    attachments_dir: Path,
    scope_root: Path,
    binding_id: str,
    suffix: str,
    data: bytes,
    media_type: str,
) -> ValidatorArtifactRef:
    attachments_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{_safe_binding_slug(binding_id)}{suffix}"
    path = attachments_dir / filename
    path.write_bytes(data)
    try:
        path.resolve().relative_to(attachments_dir.resolve())
    except ValueError as exc:
        path.unlink(missing_ok=True)
        raise ValidatorBindingsConfigError("attachment path escapes attachments directory") from exc
    return ValidatorArtifactRef(
        path=_artifact_rel_path(scope_root=scope_root, path=path),
        media_type=media_type,
        sha256=_sha256_bytes(data),
    )


def _write_attachment_text(
    *,
    attachments_dir: Path,
    scope_root: Path,
    binding_id: str,
    suffix: str,
    text: str,
    media_type: str = "text/plain",
) -> ValidatorArtifactRef:
    return _write_attachment_bytes(
        attachments_dir=attachments_dir,
        scope_root=scope_root,
        binding_id=binding_id,
        suffix=suffix,
        data=text.encode("utf-8"),
        media_type=media_type,
    )


def _run_command(
    *,
    argv: Sequence[str],
    cwd: Path,
    timeout_s: float,
    env: Mapping[str, str] | None = None,
    capture_text: bool = True,
) -> tuple[int, str, str]:
    proc = subprocess.run(
        list(argv),
        cwd=str(cwd),
        capture_output=True,
        text=capture_text,
        timeout=timeout_s,
        env=dict(env) if env is not None else {"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
        check=False,
    )
    stdout = (
        proc.stdout
        if isinstance(proc.stdout, str)
        else (proc.stdout.decode("utf-8", errors="replace") if proc.stdout else "")
    )
    stderr = (
        proc.stderr
        if isinstance(proc.stderr, str)
        else (proc.stderr.decode("utf-8", errors="replace") if proc.stderr else "")
    )
    return int(proc.returncode), stdout or "", stderr or ""


def _normalize_summary_value(value: Any) -> JSONValue:
    if value is None or isinstance(value, (bool, int, float, str)):
        return cast(JSONValue, value)
    if isinstance(value, list):
        return cast(JSONValue, [_normalize_summary_value(item) for item in value[:64]])
    if isinstance(value, Mapping):
        return cast(
            JSONValue,
            {str(key): _normalize_summary_value(item) for key, item in list(value.items())[:128] if str(key).strip()},
        )
    return str(value)


def _compute_fingerprint(payload: Mapping[str, Any]) -> str:
    return stable_json_fingerprint(dict(payload))


def merge_validator_evidence(
    *,
    existing: Sequence[RuntimeEvidenceRecord],
    updates: Sequence[RuntimeEvidenceRecord],
) -> tuple[RuntimeEvidenceRecord, ...]:
    if not updates:
        return tuple(existing)
    replace_keys = {
        (rec.evidence_type, str(rec.payload.get("binding_id", "")).strip())
        for rec in updates
        if str(rec.payload.get("binding_id", "")).strip()
    }
    kept = [
        rec
        for rec in existing
        if (rec.evidence_type, str(rec.payload.get("binding_id", "")).strip()) not in replace_keys
    ]
    return tuple(kept + list(updates))


def _write_result_artifact(
    *,
    validation_dir: Path,
    scope_root: Path,
    binding_id: str,
    artifact_kind: ValidatorResultArtifactKind,
    payload: dict[str, Any],
) -> str:
    validation_dir.mkdir(parents=True, exist_ok=True)
    out = apply_schema_envelope(obj=dict(payload), kind=artifact_kind, version=1)
    validate_artifact_json(obj=out, kind=artifact_kind, version=1)
    path = validation_dir / f"{_safe_binding_slug(binding_id)}.{artifact_kind}.json"
    path.write_text(json.dumps(out, indent=2, sort_keys=True, ensure_ascii=False) + "\n", encoding="utf-8")
    return _artifact_rel_path(scope_root=scope_root, path=path)


def _default_device_id_for_platform(platform: str, configured: str | None) -> str:
    if configured:
        return configured
    return "booted" if platform == "ios" else "default"


def _error_result_payload_for_missing_or_blocked(
    *,
    binding_id: str,
    binding_kind: str,
    success_criterion_ids: Sequence[str],
    error_message: str,
) -> tuple[ValidatorResultArtifactKind, dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    if binding_kind == "maestro_flow":
        payload: dict[str, Any] = {
            "binding_id": binding_id,
            "binding_kind": binding_kind,
            "platform": "android",
            "device_id": "default",
            "journey_id": binding_id,
            "status": "error",
            "started_at_ms": now_ms,
            "ended_at_ms": now_ms,
            "assertions_passed": 0,
            "assertions_failed": 1,
            "artifacts": [],
            "summary": {"error": error_message},
            "success_criterion_ids": list(success_criterion_ids),
        }
        payload["fingerprint_sha256"] = _compute_fingerprint(payload)
        return ("mobile_journey_result", payload)
    if binding_kind in {"android_helper", "ios_simulator_helper"}:
        payload = {
            "binding_id": binding_id,
            "binding_kind": binding_kind,
            "platform": "ios" if binding_kind == "ios_simulator_helper" else "android",
            "capture_kind": "blocked",
            "status": "error",
            "artifact_path": None,
            "metadata": {
                "error": error_message,
                "success_criterion_ids": list(success_criterion_ids),
            },
        }
        payload["fingerprint_sha256"] = _compute_fingerprint(payload)
        return ("device_capture_result", payload)
    payload = {
        "binding_id": binding_id,
        "binding_kind": binding_kind,
        "query_kind": binding_kind if binding_kind in _OBS_QUERY_KINDS else "logql_query",
        "target": binding_id,
        "window_start_ms": now_ms,
        "window_end_ms": now_ms,
        "status": "error",
        "summary": {"error": error_message},
        "attachments": [],
        "success_criterion_ids": list(success_criterion_ids),
    }
    payload["fingerprint_sha256"] = _compute_fingerprint(payload)
    return ("observability_query_result", payload)


def _error_evidence_type_for_artifact_kind(artifact_kind: ValidatorResultArtifactKind) -> str:
    if artifact_kind == "mobile_journey_result":
        return MOBILE_JOURNEY_RESULT_EVIDENCE_TYPE
    if artifact_kind == "device_capture_result":
        return DEVICE_CAPTURE_RESULT_EVIDENCE_TYPE
    return OBSERVABILITY_QUERY_RESULT_EVIDENCE_TYPE


def _execute_observability_binding(
    *,
    binding: ValidatorBinding,
    attachments_dir: Path,
    validation_dir: Path,
    scope_root: Path,
    runtime_run_id: str,
    success_criterion_ids: Sequence[str],
) -> tuple[RuntimeEvidenceRecord, str, dict[str, JSONValue]]:
    cfg = binding.config
    started_ms = int(time.time() * 1000)
    request_url = str(cfg["url"])
    method = str(cfg["method"])
    headers = dict(cast(dict[str, str], cfg["headers"]))
    data: bytes | None = None
    if method == "GET":
        parts = urllib.parse.urlsplit(request_url)
        query = dict(urllib.parse.parse_qsl(parts.query, keep_blank_values=True))
        query[str(cfg["query_param"])] = str(cfg["query"])
        request_url = urllib.parse.urlunsplit(parts._replace(query=urllib.parse.urlencode(query)))
    else:
        headers.setdefault("Content-Type", "application/json")
        data = json.dumps({str(cfg["query_param"]): str(cfg["query"])}).encode("utf-8")
    attachments: list[ValidatorArtifactRef] = []
    status = "ok"
    summary: dict[str, JSONValue] = {}
    error_text: str | None = None
    try:
        req = urllib.request.Request(url=request_url, data=data, method=method, headers=headers)
        with urllib.request.urlopen(req, timeout=max(0.1, float(int(cfg["timeout_ms"])) / 1000.0)) as resp:
            body = resp.read()
            media_type = (
                resp.headers.get_content_type()
                if hasattr(resp.headers, "get_content_type")
                else "application/octet-stream"
            )
        attachment = _write_attachment_bytes(
            attachments_dir=attachments_dir,
            scope_root=scope_root,
            binding_id=binding.binding_id,
            suffix=".response.json" if media_type == "application/json" else ".response.txt",
            data=body,
            media_type=media_type,
        )
        attachments.append(attachment)
        try:
            parsed = json.loads(body.decode("utf-8"))
            normalized = _normalize_summary_value(parsed)
            summary = normalized if isinstance(normalized, dict) else {"value": cast(JSONValue, normalized)}
        except Exception:
            summary = {"text": body.decode("utf-8", errors="replace")[:16384]}
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError, ValueError) as exc:
        status = "error"
        error_text = str(exc)
        attachments.append(
            _write_attachment_text(
                attachments_dir=attachments_dir,
                scope_root=scope_root,
                binding_id=binding.binding_id,
                suffix=".error.txt",
                text=error_text,
            )
        )
        summary = {"error": error_text}
    ended_ms = int(time.time() * 1000)
    payload: dict[str, Any] = {
        "binding_id": binding.binding_id,
        "binding_kind": binding.kind,
        "query_kind": binding.kind,
        "target": str(cfg["target"]),
        "window_start_ms": started_ms,
        "window_end_ms": ended_ms,
        "status": status,
        "summary": summary,
        "attachments": [item.to_json_obj() for item in attachments],
        "success_criterion_ids": list(success_criterion_ids),
    }
    payload["fingerprint_sha256"] = _compute_fingerprint(payload)
    artifact_path = _write_result_artifact(
        validation_dir=validation_dir,
        scope_root=scope_root,
        binding_id=binding.binding_id,
        artifact_kind="observability_query_result",
        payload=payload,
    )
    evidence = RuntimeEvidenceRecord(
        evidence_type=cast(Any, OBSERVABILITY_QUERY_RESULT_EVIDENCE_TYPE),
        timestamp=ended_ms,
        runtime_run_id=runtime_run_id,
        payload=cast(dict[str, JSONValue], dict(payload)),
    )
    return evidence, artifact_path, {"binding_id": binding.binding_id, "kind": binding.kind, "status": status}


def _execute_maestro_binding(
    *,
    binding: ValidatorBinding,
    attachments_dir: Path,
    validation_dir: Path,
    scope_root: Path,
    runtime_run_id: str,
    success_criterion_ids: Sequence[str],
) -> tuple[RuntimeEvidenceRecord, str, dict[str, JSONValue]]:
    cfg = binding.config
    started_ms = int(time.time() * 1000)
    argv = ["maestro", "test", str(cfg["flow_path"])]
    if cfg.get("device_id"):
        argv.extend(["--device", str(cfg["device_id"])])
    env = {"PATH": os.environ.get("PATH", "/usr/bin:/bin")}
    if cfg.get("app_id"):
        env["AKC_VALIDATION_APP_ID"] = str(cfg["app_id"])
    rc, stdout, stderr = _run_command(
        argv=argv,
        cwd=scope_root,
        timeout_s=max(float(int(cfg["timeout_ms"])) / 1000.0, 0.001),
        env=env,
    )
    attachments: list[ValidatorArtifactRef] = []
    log_text = "\n".join(part for part in (stdout.strip(), stderr.strip()) if part)
    if log_text:
        attachments.append(
            _write_attachment_text(
                attachments_dir=attachments_dir,
                scope_root=scope_root,
                binding_id=binding.binding_id,
                suffix=".maestro.log",
                text=log_text,
            )
        )
    status = "passed" if rc == 0 else "failed"
    ended_ms = int(time.time() * 1000)
    payload: dict[str, Any] = {
        "binding_id": binding.binding_id,
        "binding_kind": binding.kind,
        "platform": str(cfg["platform"]),
        "device_id": _default_device_id_for_platform(str(cfg["platform"]), cast(str | None, cfg.get("device_id"))),
        "journey_id": str(cfg["journey_id"]),
        "status": status,
        "started_at_ms": started_ms,
        "ended_at_ms": ended_ms,
        "assertions_passed": 1 if rc == 0 else 0,
        "assertions_failed": 0 if rc == 0 else 1,
        "artifacts": [item.to_json_obj() for item in attachments],
        "success_criterion_ids": list(success_criterion_ids),
    }
    payload["fingerprint_sha256"] = _compute_fingerprint(payload)
    artifact_path = _write_result_artifact(
        validation_dir=validation_dir,
        scope_root=scope_root,
        binding_id=binding.binding_id,
        artifact_kind="mobile_journey_result",
        payload=payload,
    )
    evidence = RuntimeEvidenceRecord(
        evidence_type=cast(Any, MOBILE_JOURNEY_RESULT_EVIDENCE_TYPE),
        timestamp=ended_ms,
        runtime_run_id=runtime_run_id,
        payload=cast(dict[str, JSONValue], dict(payload)),
    )
    return evidence, artifact_path, {"binding_id": binding.binding_id, "kind": binding.kind, "status": status}


def _adb_argv(*, device_id: str | None, extra: Sequence[str]) -> list[str]:
    argv = ["adb"]
    if device_id:
        argv.extend(["-s", device_id])
    argv.extend(list(extra))
    return argv


def _xcrun_argv(extra: Sequence[str]) -> list[str]:
    return ["xcrun", *list(extra)]


def _execute_android_helper_binding(
    *,
    binding: ValidatorBinding,
    attachments_dir: Path,
    validation_dir: Path,
    scope_root: Path,
    runtime_run_id: str,
    success_criterion_ids: Sequence[str],
) -> tuple[RuntimeEvidenceRecord, str, dict[str, JSONValue]]:
    cfg = binding.config
    operation = str(cfg["operation"])
    device_id = cast(str | None, cfg.get("device_id"))
    metadata: dict[str, JSONValue] = {
        "binding_kind": binding.kind,
        "operation": operation,
        "device_id": cast(JSONValue, device_id),
        "success_criterion_ids": list(success_criterion_ids),
    }
    status = "ok"
    artifact_path: str | None = None
    try:
        if operation == "install":
            rc, stdout, stderr = _run_command(
                argv=_adb_argv(device_id=device_id, extra=["install", "-r", str(cfg["apk_path"])]),
                cwd=scope_root,
                timeout_s=300.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:8192], "stderr": stderr[:8192]})
            if rc != 0:
                status = "error"
        elif operation == "clear_data":
            rc, stdout, stderr = _run_command(
                argv=_adb_argv(device_id=device_id, extra=["shell", "pm", "clear", str(cfg["package_name"])]),
                cwd=scope_root,
                timeout_s=60.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:8192], "stderr": stderr[:8192]})
            if rc != 0:
                status = "error"
        elif operation == "grant_permissions":
            perms = cast(list[str], cfg["permissions"])
            rows: list[dict[str, JSONValue]] = []
            for perm in perms:
                rc, stdout, stderr = _run_command(
                    argv=_adb_argv(
                        device_id=device_id,
                        extra=["shell", "pm", "grant", str(cfg["package_name"]), perm],
                    ),
                    cwd=scope_root,
                    timeout_s=60.0,
                )
                rows.append({"permission": perm, "returncode": rc, "stdout": stdout[:1024], "stderr": stderr[:1024]})
                if rc != 0:
                    status = "error"
            metadata["permission_results"] = cast(JSONValue, rows)
        elif operation == "start_activity":
            rc, stdout, stderr = _run_command(
                argv=_adb_argv(device_id=device_id, extra=["shell", "am", "start", "-n", str(cfg["activity"])]),
                cwd=scope_root,
                timeout_s=60.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:8192], "stderr": stderr[:8192]})
            if rc != 0:
                status = "error"
        elif operation == "screenshot":
            proc = subprocess.run(
                _adb_argv(device_id=device_id, extra=["exec-out", "screencap", "-p"]),
                cwd=str(scope_root),
                capture_output=True,
                timeout=60.0,
                check=False,
            )
            metadata["returncode"] = int(proc.returncode)
            shot = proc.stdout
            if int(proc.returncode) == 0 and shot:
                ref = _write_attachment_bytes(
                    attachments_dir=attachments_dir,
                    scope_root=scope_root,
                    binding_id=binding.binding_id,
                    suffix=f".{str(cfg['artifact_name'])}.png",
                    data=shot,
                    media_type="image/png",
                )
                artifact_path = ref.path
            else:
                status = "error"
                metadata["stderr"] = (proc.stderr.decode("utf-8", errors="replace") if proc.stderr else "")[:8192]
        elif operation == "screenrecord":
            attachments_dir.mkdir(parents=True, exist_ok=True)
            local_path = attachments_dir / f"{_safe_binding_slug(binding.binding_id)}.{str(cfg['artifact_name'])}.mp4"
            remote_path = f"/sdcard/{local_path.name}"
            rc, stdout, stderr = _run_command(
                argv=_adb_argv(
                    device_id=device_id,
                    extra=["shell", "screenrecord", "--time-limit", str(int(cfg["duration_s"])), remote_path],
                ),
                cwd=scope_root,
                timeout_s=float(int(cfg["duration_s"])) + 30.0,
            )
            metadata.update({"record_returncode": rc, "stdout": stdout[:4096], "stderr": stderr[:4096]})
            if rc == 0:
                prc, pout, perr = _run_command(
                    argv=_adb_argv(device_id=device_id, extra=["pull", remote_path, str(local_path)]),
                    cwd=scope_root,
                    timeout_s=120.0,
                )
                metadata.update({"pull_returncode": prc, "pull_stdout": pout[:1024], "pull_stderr": perr[:1024]})
                if prc == 0 and local_path.is_file():
                    artifact_path = _artifact_rel_path(scope_root=scope_root, path=local_path)
                else:
                    status = "error"
            else:
                status = "error"
        else:
            rc, stdout, stderr = _run_command(
                argv=_adb_argv(device_id=device_id, extra=["logcat", "-d"]),
                cwd=scope_root,
                timeout_s=60.0,
            )
            metadata["returncode"] = rc
            if rc == 0:
                ref = _write_attachment_text(
                    attachments_dir=attachments_dir,
                    scope_root=scope_root,
                    binding_id=binding.binding_id,
                    suffix=f".{str(cfg['artifact_name'])}.log",
                    text=stdout,
                )
                artifact_path = ref.path
            else:
                status = "error"
                metadata["stderr"] = stderr[:8192]
    except (OSError, subprocess.TimeoutExpired) as exc:
        status = "error"
        metadata["error"] = str(exc)
    payload: dict[str, Any] = {
        "binding_id": binding.binding_id,
        "binding_kind": binding.kind,
        "platform": "android",
        "capture_kind": operation,
        "status": status,
        "artifact_path": artifact_path,
        "metadata": metadata,
    }
    payload["fingerprint_sha256"] = _compute_fingerprint(payload)
    ts_ms = int(time.time() * 1000)
    artifact_file = _write_result_artifact(
        validation_dir=validation_dir,
        scope_root=scope_root,
        binding_id=binding.binding_id,
        artifact_kind="device_capture_result",
        payload=payload,
    )
    evidence = RuntimeEvidenceRecord(
        evidence_type=cast(Any, DEVICE_CAPTURE_RESULT_EVIDENCE_TYPE),
        timestamp=ts_ms,
        runtime_run_id=runtime_run_id,
        payload=cast(dict[str, JSONValue], dict(payload)),
    )
    return evidence, artifact_file, {"binding_id": binding.binding_id, "kind": binding.kind, "status": status}


def _execute_ios_helper_binding(
    *,
    binding: ValidatorBinding,
    attachments_dir: Path,
    validation_dir: Path,
    scope_root: Path,
    runtime_run_id: str,
    success_criterion_ids: Sequence[str],
) -> tuple[RuntimeEvidenceRecord, str, dict[str, JSONValue]]:
    cfg = binding.config
    operation = str(cfg["operation"])
    device_id = str(cfg["device_id"])
    metadata: dict[str, JSONValue] = {
        "binding_kind": binding.kind,
        "operation": operation,
        "device_id": device_id,
        "success_criterion_ids": list(success_criterion_ids),
    }
    status = "ok"
    artifact_path: str | None = None
    try:
        if operation == "boot":
            rc, stdout, stderr = _run_command(
                argv=_xcrun_argv(["simctl", "boot", device_id]),
                cwd=scope_root,
                timeout_s=120.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:4096], "stderr": stderr[:4096]})
            if rc != 0 and "Unable to boot device in current state: Booted" not in stderr:
                status = "error"
        elif operation == "erase":
            rc, stdout, stderr = _run_command(
                argv=_xcrun_argv(["simctl", "erase", device_id]),
                cwd=scope_root,
                timeout_s=120.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:4096], "stderr": stderr[:4096]})
            if rc != 0:
                status = "error"
        elif operation == "install_app":
            rc, stdout, stderr = _run_command(
                argv=_xcrun_argv(["simctl", "install", device_id, str(cfg["app_path"])]),
                cwd=scope_root,
                timeout_s=300.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:4096], "stderr": stderr[:4096]})
            if rc != 0:
                status = "error"
        elif operation == "launch_app":
            rc, stdout, stderr = _run_command(
                argv=_xcrun_argv(["simctl", "launch", device_id, str(cfg["bundle_id"])]),
                cwd=scope_root,
                timeout_s=120.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:4096], "stderr": stderr[:4096]})
            if rc != 0:
                status = "error"
        elif operation == "screenshot":
            attachments_dir.mkdir(parents=True, exist_ok=True)
            local_path = attachments_dir / f"{_safe_binding_slug(binding.binding_id)}.{str(cfg['artifact_name'])}.png"
            rc, stdout, stderr = _run_command(
                argv=_xcrun_argv(["simctl", "io", device_id, "screenshot", str(local_path)]),
                cwd=scope_root,
                timeout_s=120.0,
            )
            metadata.update({"returncode": rc, "stdout": stdout[:4096], "stderr": stderr[:4096]})
            if rc == 0 and local_path.is_file():
                artifact_path = _artifact_rel_path(scope_root=scope_root, path=local_path)
            else:
                status = "error"
        else:
            attachments_dir.mkdir(parents=True, exist_ok=True)
            local_path = attachments_dir / f"{_safe_binding_slug(binding.binding_id)}.{str(cfg['artifact_name'])}.mp4"
            proc = subprocess.Popen(
                _xcrun_argv(["simctl", "io", device_id, "recordVideo", "--force", str(local_path)]),
                cwd=str(scope_root),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env={"PATH": os.environ.get("PATH", "/usr/bin:/bin")},
            )
            try:
                stdout, stderr = proc.communicate(timeout=float(int(cfg["duration_s"])))
                rc = int(proc.returncode)
            except subprocess.TimeoutExpired:
                proc.terminate()
                try:
                    stdout, stderr = proc.communicate(timeout=5.0)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    stdout, stderr = proc.communicate()
                rc = int(proc.returncode)
            metadata.update({"returncode": rc, "stdout": stdout[:4096], "stderr": stderr[:4096]})
            if local_path.is_file():
                artifact_path = _artifact_rel_path(scope_root=scope_root, path=local_path)
            else:
                status = "error"
    except (OSError, subprocess.TimeoutExpired) as exc:
        status = "error"
        metadata["error"] = str(exc)
    payload: dict[str, Any] = {
        "binding_id": binding.binding_id,
        "binding_kind": binding.kind,
        "platform": "ios",
        "capture_kind": operation,
        "status": status,
        "artifact_path": artifact_path,
        "metadata": metadata,
    }
    payload["fingerprint_sha256"] = _compute_fingerprint(payload)
    ts_ms = int(time.time() * 1000)
    artifact_file = _write_result_artifact(
        validation_dir=validation_dir,
        scope_root=scope_root,
        binding_id=binding.binding_id,
        artifact_kind="device_capture_result",
        payload=payload,
    )
    evidence = RuntimeEvidenceRecord(
        evidence_type=cast(Any, DEVICE_CAPTURE_RESULT_EVIDENCE_TYPE),
        timestamp=ts_ms,
        runtime_run_id=runtime_run_id,
        payload=cast(dict[str, JSONValue], dict(payload)),
    )
    return evidence, artifact_file, {"binding_id": binding.binding_id, "kind": binding.kind, "status": status}


def _ordered_bindings(bindings: Sequence[ValidatorBinding]) -> list[ValidatorBinding]:
    def _key(binding: ValidatorBinding) -> tuple[int, str]:
        if binding.kind in {"android_helper", "ios_simulator_helper"}:
            operation = str(binding.config.get("operation", ""))
            group = 0 if operation not in _HELPER_CAPTURE_OPS[binding.kind] else 2
            return (group, binding.binding_id)
        if binding.kind == "maestro_flow":
            return (1, binding.binding_id)
        return (3, binding.binding_id)

    return sorted(bindings, key=_key)


def execute_validator_bindings(
    *,
    scope_root: Path,
    run_id: str,
    runtime_run_id: str,
    specs: Sequence[tuple[str, OperationalValidityParams]],
    bindings_path: Path | None,
    adapter_id: str = SUPPORTED_VALIDATOR_ADAPTER_ID,
) -> ValidatorExecutionResult:
    """Run validators, writing artifacts under ``scope_root`` / ``.akc/verification/validators/``.

    Resolved ``flow_path``, ``apk_path``, and ``app_path`` must lie under
    ``scope_root`` (typically ``<outputs_root>/<tenant_id>/<repo_id>``); otherwise
    :class:`ValidatorBindingsConfigError` is raised. Paths are normalized against
    the registry directory when the registry is loaded, then re-checked here.
    """

    binding_ids = collect_binding_ids_from_specs(specs)
    if not binding_ids:
        return ValidatorExecutionResult(evidence=(), artifact_paths=(), binding_results=())
    bindings = load_validator_bindings(path=bindings_path)
    usage = _stub_usage_map(specs)
    validation_dir = scope_root / ".akc" / "verification" / "validators" / run_id
    attachments_dir = validation_dir / "attachments"
    evidence: list[RuntimeEvidenceRecord] = []
    artifact_paths: list[str] = []
    binding_results: list[dict[str, JSONValue]] = []
    selected: list[ValidatorBinding] = []
    for binding_id in binding_ids:
        binding = bindings.get(binding_id)
        if binding is None:
            expected_type = str(
                usage.get(binding_id, {}).get("evidence_type", OBSERVABILITY_QUERY_RESULT_EVIDENCE_TYPE)
            )
            if expected_type == MOBILE_JOURNEY_RESULT_EVIDENCE_TYPE:
                kind_name = "maestro_flow"
            elif expected_type == DEVICE_CAPTURE_RESULT_EVIDENCE_TYPE:
                kind_name = "android_helper"
            else:
                kind_name = "logql_query"
            artifact_kind, payload = _error_result_payload_for_missing_or_blocked(
                binding_id=binding_id,
                binding_kind=kind_name,
                success_criterion_ids=cast(Sequence[str], usage.get(binding_id, {}).get("success_criterion_ids", ())),
                error_message="validator binding missing",
            )
            artifact_path = _write_result_artifact(
                validation_dir=validation_dir,
                scope_root=scope_root,
                binding_id=binding_id,
                artifact_kind=artifact_kind,
                payload=payload,
            )
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type=cast(Any, _error_evidence_type_for_artifact_kind(artifact_kind)),
                    timestamp=int(time.time() * 1000),
                    runtime_run_id=runtime_run_id,
                    payload=cast(dict[str, JSONValue], dict(payload)),
                )
            )
            artifact_paths.append(artifact_path)
            binding_results.append({"binding_id": binding_id, "kind": "missing", "status": "error"})
            continue
        selected.append(binding)
    for binding in selected:
        _require_read_paths_under_scope_root(
            binding_id=binding.binding_id,
            paths=_iter_binding_read_paths(binding),
            scope_root=scope_root,
        )
    if selected and adapter_id != SUPPORTED_VALIDATOR_ADAPTER_ID:
        for binding in _ordered_bindings(selected):
            success_ids = cast(Sequence[str], usage.get(binding.binding_id, {}).get("success_criterion_ids", ()))
            artifact_kind, payload = _error_result_payload_for_missing_or_blocked(
                binding_id=binding.binding_id,
                binding_kind=binding.kind,
                success_criterion_ids=success_ids,
                error_message=f"validator execution requires adapter_id={SUPPORTED_VALIDATOR_ADAPTER_ID}",
            )
            artifact_path = _write_result_artifact(
                validation_dir=validation_dir,
                scope_root=scope_root,
                binding_id=binding.binding_id,
                artifact_kind=artifact_kind,
                payload=payload,
            )
            evidence.append(
                RuntimeEvidenceRecord(
                    evidence_type=cast(Any, _error_evidence_type_for_artifact_kind(artifact_kind)),
                    timestamp=int(time.time() * 1000),
                    runtime_run_id=runtime_run_id,
                    payload=cast(dict[str, JSONValue], dict(payload)),
                )
            )
            artifact_paths.append(artifact_path)
            binding_results.append(
                {
                    "binding_id": binding.binding_id,
                    "kind": binding.kind,
                    "status": "blocked",
                    "adapter_id": adapter_id,
                }
            )
        return ValidatorExecutionResult(
            evidence=tuple(evidence),
            artifact_paths=tuple(artifact_paths),
            binding_results=tuple(binding_results),
        )
    for binding in _ordered_bindings(selected):
        success_ids = cast(Sequence[str], usage.get(binding.binding_id, {}).get("success_criterion_ids", ()))
        if binding.kind in _OBS_QUERY_KINDS:
            rec, artifact_path, row = _execute_observability_binding(
                binding=binding,
                attachments_dir=attachments_dir,
                validation_dir=validation_dir,
                scope_root=scope_root,
                runtime_run_id=runtime_run_id,
                success_criterion_ids=success_ids,
            )
        elif binding.kind == "maestro_flow":
            rec, artifact_path, row = _execute_maestro_binding(
                binding=binding,
                attachments_dir=attachments_dir,
                validation_dir=validation_dir,
                scope_root=scope_root,
                runtime_run_id=runtime_run_id,
                success_criterion_ids=success_ids,
            )
        elif binding.kind == "android_helper":
            rec, artifact_path, row = _execute_android_helper_binding(
                binding=binding,
                attachments_dir=attachments_dir,
                validation_dir=validation_dir,
                scope_root=scope_root,
                runtime_run_id=runtime_run_id,
                success_criterion_ids=success_ids,
            )
        else:
            rec, artifact_path, row = _execute_ios_helper_binding(
                binding=binding,
                attachments_dir=attachments_dir,
                validation_dir=validation_dir,
                scope_root=scope_root,
                runtime_run_id=runtime_run_id,
                success_criterion_ids=success_ids,
            )
        evidence.append(rec)
        artifact_paths.append(artifact_path)
        binding_results.append(row)
    return ValidatorExecutionResult(
        evidence=tuple(evidence),
        artifact_paths=tuple(artifact_paths),
        binding_results=tuple(binding_results),
    )
