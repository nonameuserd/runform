"""Per-step coordination metadata merged into kernel ``policy_context`` / fingerprints."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from akc.coordination.models import CoordinationGraphEdge, ParsedCoordinationSpec
from akc.memory.models import JSONValue

_V1_SCHEDULABLE_KINDS = frozenset({"depends_on"})
_V2_SCHEDULABLE_KINDS = frozenset({"depends_on", "parallel", "barrier", "delegate", "handoff"})


def _sorted_edge_records(records: list[dict[str, JSONValue]]) -> list[dict[str, JSONValue]]:
    """Stable ordering for JSON serialization (matches fingerprint style)."""

    return sorted(records, key=lambda r: (str(r.get("from_step_id", "")), str(r.get("edge_id", ""))))


def _incoming_edges(*, graph_edges: tuple[CoordinationGraphEdge, ...], step_id: str) -> list[CoordinationGraphEdge]:
    return [e for e in graph_edges if e.dst_step_id == step_id]


def _metadata_str(meta: Mapping[str, Any] | None, key: str) -> str | None:
    if meta is None:
        return None
    raw = meta.get(key)
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    return None


def _delegate_kind_from_edge(e: CoordinationGraphEdge) -> str:
    meta = e.metadata or {}
    dk = _metadata_str(meta, "delegate_kind")
    if dk is not None:
        return dk
    target = _metadata_str(meta, "delegate_target")
    if target is None:
        return "opaque"
    tl = target.lower()
    if tl.startswith("https://") or tl.startswith("http://"):
        return "http"
    return "opaque"


def normalize_agent_output_sha256_hex(digest: str) -> str:
    """Normalize worker output digest to lowercase hex for stable comparisons."""

    s = str(digest).strip().lower()
    if len(s) != 64 or any(c not in "0123456789abcdef" for c in s):
        raise ValueError(f"agent_worker_output_sha256 must be a 64-char hex string, got {digest!r}")
    return s


def coordination_step_policy_extras(
    *,
    parsed: ParsedCoordinationSpec,
    step_id: str,
    coordination_step_outputs: Mapping[str, str],
) -> dict[str, JSONValue]:
    """Return fields to merge into coordination ``policy_context`` / ``inputs_fingerprint`` payload.

    Collects deterministic slices for incoming ``handoff``, ``delegate``, and ``parallel`` edges.
    For each distinct handoff predecessor (``src_step_id``), requires a recorded successful output
    digest in ``coordination_step_outputs`` and exposes it as
    ``coordination_handoff_predecessor_output_sha256s`` (sorted by ``predecessor_step_id``).

    Raises:
        ValueError: If a handoff predecessor has no recorded output digest (fail-closed).
    """

    sid = str(step_id).strip()
    incoming = _incoming_edges(graph_edges=parsed.graph.edges, step_id=sid)
    sched_kinds = _V2_SCHEDULABLE_KINDS if int(parsed.spec_version) >= 2 else _V1_SCHEDULABLE_KINDS
    incoming_kind_set: set[str] = {e.kind for e in incoming if e.kind in sched_kinds}

    handoff_edges: list[dict[str, JSONValue]] = []
    delegate_edges: list[dict[str, JSONValue]] = []
    parallel_edges: list[dict[str, JSONValue]] = []

    handoff_predecessors: set[str] = set()
    handoff_id_set: set[str] = set()
    delegate_kind_set: set[str] = set()
    for e in incoming:
        if e.kind == "handoff":
            hid = _metadata_str(e.metadata, "handoff_id")
            if not hid:
                raise ValueError(f"coordination edge {e.edge_id!r} (handoff): missing metadata.handoff_id")
            rec: dict[str, JSONValue] = {
                "edge_id": e.edge_id,
                "from_step_id": e.src_step_id,
                "handoff_id": hid,
            }
            art = _metadata_str(e.metadata, "artifact_ref")
            if art is not None:
                rec["artifact_ref"] = art
            handoff_edges.append(rec)
            handoff_predecessors.add(e.src_step_id)
            handoff_id_set.add(hid)
        elif e.kind == "delegate":
            target = _metadata_str(e.metadata, "delegate_target")
            if not target:
                raise ValueError(f"coordination edge {e.edge_id!r} (delegate): missing metadata.delegate_target")
            delegate_kind_set.add(_delegate_kind_from_edge(e))
            delegate_edges.append(
                {
                    "edge_id": e.edge_id,
                    "from_step_id": e.src_step_id,
                    "delegate_target": target,
                }
            )
        elif e.kind == "parallel":
            rec_p: dict[str, JSONValue] = {
                "edge_id": e.edge_id,
                "from_step_id": e.src_step_id,
            }
            pg = _metadata_str(e.metadata, "parallel_group_id")
            if pg is not None:
                rec_p["parallel_group_id"] = pg
            parallel_edges.append(rec_p)

    predecessor_shas: list[dict[str, JSONValue]] = []
    for pred in sorted(handoff_predecessors):
        raw = coordination_step_outputs.get(pred)
        if raw is None or not str(raw).strip():
            raise ValueError(
                f"coordination step {sid!r} has handoff from predecessor {pred!r} "
                "but no recorded agent_worker_output_sha256 for that step in this run"
            )
        predecessor_shas.append(
            {
                "predecessor_step_id": pred,
                "agent_worker_output_sha256": normalize_agent_output_sha256_hex(str(raw)),
            }
        )

    out: dict[str, JSONValue] = {
        "coordination_handoff_edges": cast(JSONValue, _sorted_edge_records(handoff_edges)),
        "coordination_delegate_edges": cast(JSONValue, _sorted_edge_records(delegate_edges)),
        "coordination_parallel_edges": cast(JSONValue, _sorted_edge_records(parallel_edges)),
        "coordination_handoff_predecessor_output_sha256s": cast(JSONValue, predecessor_shas),
    }
    if incoming_kind_set:
        out["coordination_edge_kind"] = ",".join(sorted(incoming_kind_set))
    if handoff_id_set:
        out["coordination_handoff_id"] = ",".join(sorted(handoff_id_set))
    if delegate_kind_set:
        out["coordination_delegate_kind"] = ",".join(sorted(delegate_kind_set))
    return out
