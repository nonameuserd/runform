from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from typing import Any

from akc.compile.interfaces import ExecutionResult
from akc.memory.models import (
    PlanState,
    PlanStep,
    PlanStepStatus,
    now_ms,
    require_non_empty,
)
from akc.run.manifest import McpReplayEvent, RunManifest
from akc.utils.fingerprint import stable_json_fingerprint


def _update_step(
    *,
    plan: PlanState,
    step_id: str,
    mutate: Callable[[PlanStep], PlanStep],
) -> PlanState:
    require_non_empty(step_id, name="step_id")
    steps2: list[PlanStep] = []
    found = False
    for s in plan.steps:
        if s.id != step_id:
            steps2.append(s)
            continue
        steps2.append(mutate(s))
        found = True
    if not found:
        raise ValueError("step not found")
    return replace(plan, steps=tuple(steps2), updated_at_ms=now_ms())


def _set_step_outputs(
    *,
    plan: PlanState,
    step_id: str,
    outputs_patch: dict[str, Any],
) -> PlanState:
    def _mutate(s: PlanStep) -> PlanStep:
        out = dict(s.outputs or {})
        out.update(dict(outputs_patch))
        return replace(s, outputs=out)

    return _update_step(plan=plan, step_id=step_id, mutate=_mutate)


def _set_step_status(
    *,
    plan: PlanState,
    step_id: str,
    status: PlanStepStatus,
    notes: str | None = None,
) -> PlanState:
    t = now_ms()

    def _mutate(s: PlanStep) -> PlanStep:
        started = s.started_at_ms
        finished = s.finished_at_ms
        if status == "in_progress" and started is None:
            started = t
        if status in {"done", "failed", "skipped"} and finished is None:
            finished = t
        return PlanStep(
            id=s.id,
            title=s.title,
            status=status,
            order_idx=s.order_idx,
            started_at_ms=started,
            finished_at_ms=finished,
            notes=notes if notes is not None else s.notes,
            inputs=s.inputs,
            outputs=s.outputs,
        )

    return _update_step(plan=plan, step_id=step_id, mutate=_mutate)


def persist_mcp_manifest_events_to_retrieval_snapshot(
    *,
    plan: PlanState,
    step_id: str,
    mcp_events: list[dict[str, Any]],
) -> PlanState:
    """Rewrite ``outputs.retrieval_snapshot.mcp_events`` (retrieve + generate/repair MCP audit)."""

    step = next((s for s in plan.steps if s.id == step_id), None)
    if step is None:
        return plan
    out_prev = dict(step.outputs or {})
    snap = dict(out_prev.get("retrieval_snapshot") or {})
    snap["mcp_events"] = list(mcp_events)
    return _set_step_outputs(plan=plan, step_id=step_id, outputs_patch={"retrieval_snapshot": snap})


def _manifest_pass_metadata(*, replay_manifest: RunManifest | None, pass_name: str) -> dict[str, Any] | None:
    if replay_manifest is None:
        return None
    for rec in replay_manifest.passes:
        if rec.name == pass_name and isinstance(rec.metadata, dict):
            return dict(rec.metadata)
    return None


def _cached_execution_stage(*, plan: PlanState, step_id: str) -> tuple[str, ExecutionResult, list[str]] | None:
    step = next((s for s in plan.steps if s.id == step_id), None)
    if step is None:
        return None
    outputs = dict(step.outputs or {})
    for key in ("last_tests_full", "last_tests_smoke"):
        raw = outputs.get(key)
        if not isinstance(raw, dict):
            continue
        stage = str(raw.get("stage") or "")
        command_raw = raw.get("command")
        command = [str(x) for x in command_raw] if isinstance(command_raw, list) else []
        try:
            exit_code_raw = raw.get("exit_code")
            if exit_code_raw is None:
                continue
            duration_raw = raw.get("duration_ms")
            result = ExecutionResult(
                exit_code=int(exit_code_raw),
                stdout=str(raw.get("stdout") or ""),
                stderr=str(raw.get("stderr") or ""),
                duration_ms=(int(duration_raw) if duration_raw is not None else None),
            )
        except Exception:
            continue
        return stage, result, command
    return None


def _replayed_execution_stage(
    *, plan: PlanState, step_id: str, replay_manifest: RunManifest | None
) -> tuple[str, ExecutionResult, list[str]] | None:
    cached = _cached_execution_stage(plan=plan, step_id=step_id)
    if cached is not None:
        return cached
    md = _manifest_pass_metadata(replay_manifest=replay_manifest, pass_name="execute")
    if md is None:
        return None
    stage = str(md.get("stage") or "tests_full")
    cmd_raw = md.get("command")
    command = [str(x) for x in cmd_raw] if isinstance(cmd_raw, list) else []
    try:
        exit_code_raw = md.get("exit_code")
        if exit_code_raw is None:
            return None
        duration_raw = md.get("duration_ms")
        result = ExecutionResult(
            exit_code=int(exit_code_raw),
            stdout=str(md.get("stdout") or ""),
            stderr=str(md.get("stderr") or ""),
            duration_ms=(int(duration_raw) if duration_raw is not None else None),
        )
    except Exception:
        return None
    return stage, result, command


def _mcp_replay_doc_id(ev: McpReplayEvent) -> str:
    return f"mcp-replay:{stable_json_fingerprint(ev.to_json_obj())[:16]}"


def _synthetic_mcp_replay_document(*, ev: McpReplayEvent, snap_idx: int) -> dict[str, Any]:
    doc_id = _mcp_replay_doc_id(ev)
    title = f"replayed:mcp:{ev.server}:{ev.kind}"
    lines = [
        "[Compile-time MCP replay — deterministic placeholder; live payload not re-fetched]",
        f"kind={ev.kind}",
        f"server={ev.server}",
        f"action={ev.action}",
    ]
    if ev.uri:
        lines.append(f"uri={ev.uri}")
    if ev.tool_name:
        lines.append(f"tool_name={ev.tool_name}")
    if ev.arguments_sha256:
        lines.append(f"arguments_sha256={ev.arguments_sha256}")
    if ev.payload_sha256:
        lines.append(f"payload_sha256={ev.payload_sha256}")
    if ev.reason:
        lines.append(f"reason={ev.reason}")
    content = "\n".join(lines)
    return {
        "doc_id": doc_id,
        "title": title,
        "content": content,
        "score": 0.05,
        "metadata": {
            "source_type": "mcp_compile_replay",
            "mcp_replay": True,
            "snapshot_index": snap_idx,
            "replay_event": ev.to_json_obj(),
        },
    }


def _replayed_retrieval_context(
    *,
    replay_manifest: RunManifest | None,
    goal: str,
) -> dict[str, Any] | None:
    if replay_manifest is None:
        return None
    snapshots = tuple(replay_manifest.retrieval_snapshots or ())
    if not snapshots:
        return None

    documents: list[dict[str, Any]] = []
    item_ids: list[str] = []
    for idx, snap in enumerate(snapshots):
        snap_item_ids = [str(x).strip() for x in snap.item_ids if str(x).strip()]
        item_ids.extend(snap_item_ids)
        for rank, item_id in enumerate(snap_item_ids):
            documents.append(
                {
                    "doc_id": item_id,
                    "title": f"replayed:{snap.source}:{idx}:{rank}",
                    # Keep replay prompt construction manifest-self-contained.
                    "content": (f"replayed retrieval snapshot source={snap.source} query={snap.query} rank={rank}"),
                    "score": float(max(int(snap.top_k) - rank, 0)),
                    "metadata": {
                        "source_type": "retrieval_snapshot",
                        "source": snap.source,
                        "query": snap.query,
                        "rank": rank,
                        "replay": True,
                    },
                }
            )
        for ev in snap.mcp_events:
            doc = _synthetic_mcp_replay_document(ev=ev, snap_idx=idx)
            documents.append(doc)
            item_ids.append(doc["doc_id"])

    return {
        "code_memory_items": [
            {
                "item_id": item_id,
                "kind": "retrieval_snapshot",
                "artifact_id": replay_manifest.run_id,
                "payload": {"goal": goal, "replay_mode": replay_manifest.replay_mode},
            }
            for item_id in item_ids
        ],
        "documents": documents,
        "why_graph": {
            "replayed_from_manifest": True,
            "run_id": replay_manifest.run_id,
            "query_count": len(snapshots),
        },
    }
