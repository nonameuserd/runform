from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from typing import Any


def stable_json_fingerprint(obj: Mapping[str, Any]) -> str:
    """Compute a stable SHA256 fingerprint for a JSON object."""
    raw = json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
