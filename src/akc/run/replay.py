from __future__ import annotations

from dataclasses import dataclass

from akc.memory.models import require_non_empty
from akc.run.manifest import ReplayMode, RunManifest


@dataclass(frozen=True, slots=True)
class ReplayDecision:
    """Resolved replay behavior for a pass within a run."""

    pass_name: str
    mode: ReplayMode
    should_call_model: bool
    should_call_tools: bool


def decide_replay_for_pass(*, manifest: RunManifest, pass_name: str) -> ReplayDecision:
    require_non_empty(pass_name, name="replay.pass_name")
    mode = manifest.replay_mode
    if mode == "live":
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=True,
            should_call_tools=True,
        )
    if mode == "llm_vcr":
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=False,
            should_call_tools=True,
        )
    if mode == "full_replay":
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=False,
            should_call_tools=False,
        )
    # partial_replay: rerun only selected passes from manifest.partial_replay_passes.
    selected = set(manifest.partial_replay_passes)
    rerun_execute = "execute" in selected
    if pass_name == "execute":
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=False,
            should_call_tools=rerun_execute,
        )
    if pass_name in {"generate", "repair"}:
        # In controller flow, "execute" is coupled to generate/repair attempts; avoid
        # re-running tools unless execute is explicitly selected.
        should_call_model = pass_name in selected
        return ReplayDecision(
            pass_name=pass_name,
            mode=mode,
            should_call_model=should_call_model,
            should_call_tools=rerun_execute,
        )
    return ReplayDecision(
        pass_name=pass_name,
        mode=mode,
        should_call_model=False,
        should_call_tools=True,
    )
