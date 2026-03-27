from __future__ import annotations

from pathlib import Path

from akc.action.models import ActionPlanStepV1


def approval_path(*, action_dir: Path, step_id: str) -> Path:
    return action_dir / "approvals" / f"{step_id}.approved"


def requires_approval(step: ActionPlanStepV1) -> bool:
    return bool(step.requires_approval)


def is_approved(*, action_dir: Path, step_id: str) -> bool:
    return approval_path(action_dir=action_dir, step_id=step_id).exists()


def approve_step(*, action_dir: Path, step_id: str) -> None:
    marker = approval_path(action_dir=action_dir, step_id=step_id)
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text("approved\n", encoding="utf-8")
