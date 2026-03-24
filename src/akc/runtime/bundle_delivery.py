from __future__ import annotations

from collections.abc import Mapping

from akc.memory.models import JSONValue


def build_delivery_handoff_context(metadata: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
    """Slim, tenant-safe projection of compile-time delivery handoff for runtime providers.

    Used for correlating observation/reconcile runs with ``delivery_plan_ref`` and enriched
    ``deployment_intents`` without copying full plan payloads onto provider hot paths.
    """

    out: dict[str, JSONValue] = {}
    raw_ref = metadata.get("delivery_plan_ref")
    if isinstance(raw_ref, Mapping):
        path = raw_ref.get("path")
        fp = raw_ref.get("fingerprint")
        if fp is None and "sha256" in raw_ref:
            fp = raw_ref.get("sha256")
        row: dict[str, JSONValue] = {}
        if isinstance(path, str) and path.strip():
            row["path"] = path.strip()
        if isinstance(fp, str) and fp.strip():
            row["fingerprint"] = fp.strip()
        if row:
            out["delivery_plan_ref"] = row
    intents = metadata.get("deployment_intents")
    if isinstance(intents, list):
        out["deployment_intent_count"] = len(intents)
        enriched = 0
        for item in intents:
            if not isinstance(item, Mapping):
                continue
            tc = item.get("target_class")
            if isinstance(tc, str) and tc.strip() and str(tc).strip().lower() != "unknown":
                enriched += 1
        out["deployment_intents_enriched_rows"] = enriched
    readiness = metadata.get("promotion_readiness")
    if isinstance(readiness, Mapping):
        status = readiness.get("status")
        if isinstance(status, str) and status.strip():
            out["promotion_readiness_status"] = status.strip()
    return out
