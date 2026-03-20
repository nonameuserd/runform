from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from akc.ir.schema import IRDocument, IRNode
from akc.utils.fingerprint import stable_json_fingerprint


@dataclass(frozen=True, slots=True)
class IRDiff:
    """Diff between two IR documents with stable, audit-friendly shape."""

    added: tuple[str, ...]
    removed: tuple[str, ...]
    changed: tuple[str, ...]

    def is_empty(self) -> bool:
        return not self.added and not self.removed and not self.changed

    def to_json_obj(self) -> dict[str, list[str]]:
        return {
            "added": list(self.added),
            "removed": list(self.removed),
            "changed": list(self.changed),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> IRDiff:
        added_raw = obj.get("added") or []
        removed_raw = obj.get("removed") or []
        changed_raw = obj.get("changed") or []

        if not isinstance(added_raw, list):
            raise ValueError("ir_diff.added must be a JSON array")
        if not isinstance(removed_raw, list):
            raise ValueError("ir_diff.removed must be a JSON array")
        if not isinstance(changed_raw, list):
            raise ValueError("ir_diff.changed must be a JSON array")

        return IRDiff(
            added=tuple(str(x) for x in added_raw),
            removed=tuple(str(x) for x in removed_raw),
            changed=tuple(str(x) for x in changed_raw),
        )


def _node_fingerprint(node: IRNode) -> str:
    return stable_json_fingerprint(node.to_json_obj())


def diff_ir(*, before: IRDocument, after: IRDocument) -> IRDiff:
    if before.tenant_id != after.tenant_id or before.repo_id != after.repo_id:
        raise ValueError("cannot diff IR documents across tenant/repo scopes")

    left = {n.id: n for n in before.nodes}
    right = {n.id: n for n in after.nodes}

    left_ids = set(left)
    right_ids = set(right)

    added = tuple(sorted(right_ids - left_ids))
    removed = tuple(sorted(left_ids - right_ids))

    changed_ids: list[str] = []
    for node_id in sorted(left_ids & right_ids):
        if _node_fingerprint(left[node_id]) != _node_fingerprint(right[node_id]):
            changed_ids.append(node_id)

    return IRDiff(added=added, removed=removed, changed=tuple(changed_ids))
