from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from akc.memory.models import JSONValue, require_non_empty


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def stable_json_fingerprint(obj: Mapping[str, Any]) -> str:
    """Compute a stable SHA256 fingerprint for a JSON object.

    The object is serialized with sorted keys and compact separators to ensure
    deterministic hashing across runs/platforms.
    """

    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return _sha256_hex(raw)


@dataclass(frozen=True, slots=True)
class IngestStateFingerprint:
    """Fingerprint of an ingestion state file, optionally filtered by tenant_id."""

    tenant_id: str
    state_path: str
    sha256: str
    keys_included: int

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "tenant_id": self.tenant_id,
            "state_path": self.state_path,
            "sha256": self.sha256,
            "keys_included": int(self.keys_included),
        }


def fingerprint_ingestion_state(
    *,
    tenant_id: str,
    state_path: str | Path,
) -> IngestStateFingerprint:
    """Fingerprint `IngestionStateStore` JSON (best-effort source set fingerprint).

    The state file uses keys of the form: `<tenant_id>::<connector>::<source_id>`.
    For drift detection, we only include keys that match the provided `tenant_id`.
    """

    require_non_empty(tenant_id, name="tenant_id")
    p = Path(state_path).expanduser()
    raw = p.read_text(encoding="utf-8")
    loaded = json.loads(raw)
    if not isinstance(loaded, dict):
        raise ValueError("ingestion state must be a JSON object")

    prefix = f"{tenant_id}::"
    filtered: dict[str, Any] = {}
    for k, v in loaded.items():
        if isinstance(k, str) and k.startswith(prefix):
            filtered[k] = v

    return IngestStateFingerprint(
        tenant_id=tenant_id.strip(),
        state_path=str(p),
        sha256=stable_json_fingerprint(filtered),
        keys_included=len(filtered),
    )


def fingerprint_file_bytes(*, path: str | Path) -> str:
    p = Path(path)
    return _sha256_hex(p.read_bytes())
