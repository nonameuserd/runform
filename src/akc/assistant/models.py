from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal, cast

from akc.memory.models import JSONValue, require_non_empty

AssistantMode = Literal["plan", "execute"]
AssistantMemorySource = Literal["assistant_turn", "assistant_command"]
PendingActionStatus = Literal["pending", "approved", "denied", "executed", "error"]
AssistantResponseStatus = Literal[
    "planned",
    "approval_required",
    "executed",
    "error",
    "no_action",
    "session_updated",
    "exit",
]


@dataclass(frozen=True, slots=True)
class AssistantScope:
    tenant_id: str | None = None
    repo_id: str | None = None
    outputs_root: str | None = None

    def normalized(self) -> AssistantScope:
        tid = str(self.tenant_id or "").strip() or None
        rid = str(self.repo_id or "").strip() or None
        out = str(self.outputs_root or "").strip() or None
        return AssistantScope(tenant_id=tid, repo_id=rid, outputs_root=out)

    def to_json_obj(self) -> dict[str, JSONValue]:
        n = self.normalized()
        return {
            "tenant_id": n.tenant_id,
            "repo_id": n.repo_id,
            "outputs_root": n.outputs_root,
        }

    @staticmethod
    def from_json_obj(obj: object) -> AssistantScope:
        if not isinstance(obj, dict):
            return AssistantScope()
        return AssistantScope(
            tenant_id=str(obj.get("tenant_id") or "").strip() or None,
            repo_id=str(obj.get("repo_id") or "").strip() or None,
            outputs_root=str(obj.get("outputs_root") or "").strip() or None,
        ).normalized()


@dataclass(frozen=True, slots=True)
class AssistantTurn:
    role: Literal["user", "assistant", "system"]
    text: str
    created_at_ms: int

    def validate(self) -> None:
        if self.role not in {"user", "assistant", "system"}:
            raise ValueError("invalid assistant turn role")
        require_non_empty(self.text, name="turn.text")
        if int(self.created_at_ms) < 0:
            raise ValueError("turn.created_at_ms must be >= 0")

    def to_json_obj(self) -> dict[str, JSONValue]:
        self.validate()
        return {
            "role": self.role,
            "text": self.text,
            "created_at_ms": int(self.created_at_ms),
        }

    @staticmethod
    def from_json_obj(obj: object) -> AssistantTurn:
        if not isinstance(obj, dict):
            raise ValueError("assistant turn must be a JSON object")
        role = str(obj.get("role") or "").strip()
        text = str(obj.get("text") or "").strip()
        created = obj.get("created_at_ms")
        if isinstance(created, bool):
            raise ValueError("assistant turn created_at_ms must be an integer")
        if isinstance(created, int):
            created_ms = int(created)
        elif isinstance(created, float):
            if not float(created).is_integer():
                raise ValueError("assistant turn created_at_ms must be an integer")
            created_ms = int(created)
        else:
            raise ValueError("assistant turn created_at_ms must be an integer")
        turn = AssistantTurn(
            role="system" if role not in {"user", "assistant", "system"} else role,  # type: ignore[arg-type]
            text=text,
            created_at_ms=created_ms,
        )
        turn.validate()
        return turn


@dataclass(frozen=True, slots=True)
class PendingAction:
    request_id: str
    action_id: str
    argv: tuple[str, ...]
    risk: Literal["read_only", "mutating"]
    status: PendingActionStatus
    created_at_ms: int
    updated_at_ms: int
    command_exit_code: int | None = None
    command_stdout: str | None = None
    command_stderr: str | None = None

    def validate(self) -> None:
        require_non_empty(self.request_id, name="pending_action.request_id")
        require_non_empty(self.action_id, name="pending_action.action_id")
        if not self.argv:
            raise ValueError("pending_action.argv must be non-empty")
        if self.risk not in {"read_only", "mutating"}:
            raise ValueError("pending_action.risk must be read_only|mutating")
        if self.status not in {"pending", "approved", "denied", "executed", "error"}:
            raise ValueError("pending_action.status is invalid")

    def to_json_obj(self) -> dict[str, JSONValue]:
        self.validate()
        return {
            "request_id": self.request_id,
            "action_id": self.action_id,
            "argv": list(self.argv),
            "risk": self.risk,
            "status": self.status,
            "created_at_ms": int(self.created_at_ms),
            "updated_at_ms": int(self.updated_at_ms),
            "command_exit_code": self.command_exit_code,
            "command_stdout": self.command_stdout,
            "command_stderr": self.command_stderr,
        }

    @staticmethod
    def from_json_obj(obj: object) -> PendingAction:
        if not isinstance(obj, dict):
            raise ValueError("pending action must be a JSON object")
        argv_raw = obj.get("argv")
        if not isinstance(argv_raw, list):
            raise ValueError("pending action argv must be an array")
        argv = tuple(str(x).strip() for x in argv_raw if str(x).strip())
        risk = str(obj.get("risk") or "").strip()
        status = str(obj.get("status") or "").strip()
        created_raw = obj.get("created_at_ms")
        updated_raw = obj.get("updated_at_ms")
        if isinstance(created_raw, bool) or isinstance(updated_raw, bool):
            raise ValueError("pending action timestamps must be integers")
        created_at_ms = int(created_raw) if isinstance(created_raw, (int, float)) else 0
        updated_at_ms = int(updated_raw) if isinstance(updated_raw, (int, float)) else 0

        exit_raw = obj.get("command_exit_code")
        if isinstance(exit_raw, bool):
            command_exit_code = None
        elif isinstance(exit_raw, int):
            command_exit_code = int(exit_raw)
        elif isinstance(exit_raw, float):
            command_exit_code = int(exit_raw) if float(exit_raw).is_integer() else None
        else:
            command_exit_code = None

        out = PendingAction(
            request_id=str(obj.get("request_id") or "").strip(),
            action_id=str(obj.get("action_id") or "").strip(),
            argv=argv,
            risk="mutating" if risk == "mutating" else "read_only",
            status=status if status in {"pending", "approved", "denied", "executed", "error"} else "error",  # type: ignore[arg-type]
            created_at_ms=created_at_ms,
            updated_at_ms=updated_at_ms,
            command_exit_code=command_exit_code,
            command_stdout=str(obj.get("command_stdout")) if obj.get("command_stdout") is not None else None,
            command_stderr=str(obj.get("command_stderr")) if obj.get("command_stderr") is not None else None,
        )
        out.validate()
        return out


@dataclass(frozen=True, slots=True)
class AssistantMemoryEntry:
    memory_id: str
    source: AssistantMemorySource
    text: str
    created_at_ms: int
    last_used_at_ms: int
    use_count: int = 0
    pinned: bool = False
    importance: float = 0.5
    reliability: float = 0.5
    explicit_boost: float = 0.0
    metadata: Mapping[str, JSONValue] | None = None

    def validate(self) -> None:
        require_non_empty(self.memory_id, name="assistant_memory_entry.memory_id")
        if self.source not in {"assistant_turn", "assistant_command"}:
            raise ValueError("assistant_memory_entry.source must be assistant_turn|assistant_command")
        require_non_empty(self.text, name="assistant_memory_entry.text")
        if int(self.created_at_ms) < 0:
            raise ValueError("assistant_memory_entry.created_at_ms must be >= 0")
        if int(self.last_used_at_ms) < 0:
            raise ValueError("assistant_memory_entry.last_used_at_ms must be >= 0")
        if int(self.use_count) < 0:
            raise ValueError("assistant_memory_entry.use_count must be >= 0")

    def to_json_obj(self) -> dict[str, JSONValue]:
        self.validate()
        return {
            "memory_id": self.memory_id,
            "source": self.source,
            "text": self.text,
            "created_at_ms": int(self.created_at_ms),
            "last_used_at_ms": int(self.last_used_at_ms),
            "use_count": int(self.use_count),
            "pinned": bool(self.pinned),
            "importance": float(self.importance),
            "reliability": float(self.reliability),
            "explicit_boost": float(self.explicit_boost),
            "metadata": dict(self.metadata) if isinstance(self.metadata, Mapping) else None,
        }

    @staticmethod
    def from_json_obj(obj: object) -> AssistantMemoryEntry:
        if not isinstance(obj, dict):
            raise ValueError("assistant memory entry must be a JSON object")
        memory_id = str(obj.get("memory_id") or "").strip()
        source = str(obj.get("source") or "").strip()
        text = str(obj.get("text") or "").strip()
        created_raw = obj.get("created_at_ms")
        used_raw = obj.get("last_used_at_ms")
        use_raw = obj.get("use_count")
        imp_raw = obj.get("importance")
        rel_raw = obj.get("reliability")
        boost_raw = obj.get("explicit_boost")
        meta_raw = obj.get("metadata")
        entry = AssistantMemoryEntry(
            memory_id=memory_id,
            source=("assistant_command" if source == "assistant_command" else "assistant_turn"),
            text=text,
            created_at_ms=int(created_raw)
            if isinstance(created_raw, (int, float)) and not isinstance(created_raw, bool)
            else 0,
            last_used_at_ms=int(used_raw)
            if isinstance(used_raw, (int, float)) and not isinstance(used_raw, bool)
            else 0,
            use_count=int(use_raw) if isinstance(use_raw, (int, float)) and not isinstance(use_raw, bool) else 0,
            pinned=bool(obj.get("pinned") is True),
            importance=float(imp_raw) if isinstance(imp_raw, (int, float)) and not isinstance(imp_raw, bool) else 0.5,
            reliability=float(rel_raw) if isinstance(rel_raw, (int, float)) and not isinstance(rel_raw, bool) else 0.5,
            explicit_boost=float(boost_raw)
            if isinstance(boost_raw, (int, float)) and not isinstance(boost_raw, bool)
            else 0.0,
            metadata=cast(Mapping[str, JSONValue], meta_raw) if isinstance(meta_raw, dict) else None,
        )
        entry.validate()
        return entry


@dataclass(frozen=True, slots=True)
class AssistantCompactedTurnRef:
    compact_id: str
    summary: str
    citation_memory_ids: tuple[str, ...]
    created_at_ms: int

    def validate(self) -> None:
        require_non_empty(self.compact_id, name="assistant_compacted_turn_ref.compact_id")
        require_non_empty(self.summary, name="assistant_compacted_turn_ref.summary")
        if int(self.created_at_ms) < 0:
            raise ValueError("assistant_compacted_turn_ref.created_at_ms must be >= 0")

    def to_json_obj(self) -> dict[str, JSONValue]:
        self.validate()
        return {
            "compact_id": self.compact_id,
            "summary": self.summary,
            "citation_memory_ids": list(self.citation_memory_ids),
            "created_at_ms": int(self.created_at_ms),
        }

    @staticmethod
    def from_json_obj(obj: object) -> AssistantCompactedTurnRef:
        if not isinstance(obj, dict):
            raise ValueError("assistant compacted turn ref must be a JSON object")
        ids_raw = obj.get("citation_memory_ids")
        ids = tuple(str(x).strip() for x in ids_raw if str(x).strip()) if isinstance(ids_raw, list) else ()
        created_ms_raw = obj.get("created_at_ms")
        out = AssistantCompactedTurnRef(
            compact_id=str(obj.get("compact_id") or "").strip(),
            summary=str(obj.get("summary") or "").strip(),
            citation_memory_ids=ids,
            created_at_ms=(
                int(created_ms_raw)
                if isinstance(created_ms_raw, (int, float)) and not isinstance(created_ms_raw, bool)
                else 0
            ),
        )
        out.validate()
        return out


@dataclass(frozen=True, slots=True)
class AssistantSession:
    schema_version: int
    session_id: str
    created_at_ms: int
    updated_at_ms: int
    mode: AssistantMode
    scope: AssistantScope
    turns: tuple[AssistantTurn, ...] = ()
    pending_actions: dict[str, PendingAction] = field(default_factory=dict)
    last_suggested_command: tuple[str, ...] | None = None
    memory_index: dict[str, AssistantMemoryEntry] = field(default_factory=dict)
    compacted_turn_refs: tuple[AssistantCompactedTurnRef, ...] = ()
    pin_set: tuple[str, ...] = ()
    last_memory_trace_ref: str | None = None

    def validate(self) -> None:
        if int(self.schema_version) not in {1, 2}:
            raise ValueError("assistant session schema_version must be 1 or 2")
        require_non_empty(self.session_id, name="assistant_session.session_id")
        if self.mode not in {"plan", "execute"}:
            raise ValueError("assistant session mode must be plan|execute")
        _ = self.scope.normalized()
        for t in self.turns:
            t.validate()
        for req_id, pending in self.pending_actions.items():
            if req_id.strip() != pending.request_id.strip():
                raise ValueError("pending action key must match request_id")
            pending.validate()
        if self.last_suggested_command is not None and not tuple(self.last_suggested_command):
            raise ValueError("last_suggested_command must be non-empty when set")
        for mid, m in self.memory_index.items():
            if str(mid).strip() != m.memory_id.strip():
                raise ValueError("memory_index key must match memory_id")
            m.validate()
        for ref in self.compacted_turn_refs:
            ref.validate()
        for p in self.pin_set:
            if not str(p).strip():
                raise ValueError("pin_set entries must be non-empty")
        if self.last_memory_trace_ref is not None and not str(self.last_memory_trace_ref).strip():
            raise ValueError("last_memory_trace_ref must be non-empty when set")

    def to_json_obj(self) -> dict[str, JSONValue]:
        self.validate()
        return {
            "schema_version": int(self.schema_version),
            "session_id": self.session_id,
            "created_at_ms": int(self.created_at_ms),
            "updated_at_ms": int(self.updated_at_ms),
            "mode": self.mode,
            "scope": self.scope.to_json_obj(),
            "turns": [t.to_json_obj() for t in self.turns],
            "pending_actions": {k: v.to_json_obj() for k, v in sorted(self.pending_actions.items())},
            "last_suggested_command": (
                list(self.last_suggested_command) if self.last_suggested_command is not None else None
            ),
            "memory_index": {k: v.to_json_obj() for k, v in sorted(self.memory_index.items())},
            "compacted_turn_refs": [x.to_json_obj() for x in self.compacted_turn_refs],
            "pin_set": list(self.pin_set),
            "last_memory_trace_ref": self.last_memory_trace_ref,
        }

    @staticmethod
    def from_json_obj(obj: object) -> AssistantSession:
        if not isinstance(obj, dict):
            raise ValueError("assistant session must be a JSON object")
        turns_raw = obj.get("turns")
        if not isinstance(turns_raw, list):
            raise ValueError("assistant session turns must be an array")
        pending_raw = obj.get("pending_actions")
        if pending_raw is None:
            pending_raw = {}
        if not isinstance(pending_raw, dict):
            raise ValueError("assistant session pending_actions must be an object")
        lsc_raw = obj.get("last_suggested_command")
        lsc: tuple[str, ...] | None = None
        if isinstance(lsc_raw, list):
            vals = tuple(str(x).strip() for x in lsc_raw if str(x).strip())
            lsc = vals if vals else None
        mem_raw = obj.get("memory_index")
        if mem_raw is None:
            mem_raw = {}
        if not isinstance(mem_raw, dict):
            raise ValueError("assistant session memory_index must be an object")
        compacted_raw = obj.get("compacted_turn_refs")
        if compacted_raw is None:
            compacted_raw = []
        if not isinstance(compacted_raw, list):
            raise ValueError("assistant session compacted_turn_refs must be an array")
        pin_raw = obj.get("pin_set")
        if pin_raw is None:
            pin_raw = []
        if not isinstance(pin_raw, list):
            raise ValueError("assistant session pin_set must be an array")
        session = AssistantSession(
            schema_version=int(obj.get("schema_version") or 1),
            session_id=str(obj.get("session_id") or "").strip(),
            created_at_ms=int(obj.get("created_at_ms") or 0),
            updated_at_ms=int(obj.get("updated_at_ms") or 0),
            mode="execute" if str(obj.get("mode") or "").strip() == "execute" else "plan",
            scope=AssistantScope.from_json_obj(obj.get("scope")),
            turns=tuple(AssistantTurn.from_json_obj(x) for x in turns_raw),
            pending_actions={
                str(k).strip(): PendingAction.from_json_obj(v) for k, v in pending_raw.items() if str(k).strip()
            },
            last_suggested_command=lsc,
            memory_index={
                str(k).strip(): AssistantMemoryEntry.from_json_obj(v) for k, v in mem_raw.items() if str(k).strip()
            },
            compacted_turn_refs=tuple(AssistantCompactedTurnRef.from_json_obj(x) for x in compacted_raw),
            pin_set=tuple(str(x).strip() for x in pin_raw if str(x).strip()),
            last_memory_trace_ref=(
                str(obj.get("last_memory_trace_ref")).strip() if obj.get("last_memory_trace_ref") is not None else None
            ),
        )
        session.validate()
        return session


@dataclass(frozen=True, slots=True)
class AssistantResponse:
    status: AssistantResponseStatus
    message: str
    session: AssistantSession
    llm_mode: str | None = None
    suggested_command: tuple[str, ...] | None = None
    request_id: str | None = None
    command_exit_code: int | None = None
    command_stdout: str | None = None
    command_stderr: str | None = None
    memory_trace: dict[str, JSONValue] | None = None

    def to_json_obj(self) -> dict[str, JSONValue]:
        suggested: list[JSONValue] | None = None
        if self.suggested_command is not None:
            suggested = [cast(JSONValue, str(x)) for x in self.suggested_command]
        pending_ids: list[JSONValue] = [cast(JSONValue, str(x)) for x in sorted(self.session.pending_actions.keys())]
        out: dict[str, JSONValue] = {
            "status": self.status,
            "message": self.message,
            "session_id": self.session.session_id,
            "mode": self.session.mode,
            "llm_mode": self.llm_mode,
            "suggested_command": suggested,
            "request_id": self.request_id,
            "command_exit_code": self.command_exit_code,
            "command_stdout": self.command_stdout,
            "command_stderr": self.command_stderr,
            "pending_request_ids": pending_ids,
            "memory_trace": self.memory_trace,
        }
        scope_obj = self.session.scope.to_json_obj()
        out["scope"] = scope_obj
        return out
