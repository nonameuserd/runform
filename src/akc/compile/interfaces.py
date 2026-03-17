"""Phase 3 compile loop interfaces (ports).

These interfaces define the boundaries for the Plan → Retrieve → Generate → Execute → Repair loop:
- Index adapter (structured index / retrieval)
- LLM backend (generation)
- Executor (sandboxed execution)

Interfaces are intentionally small and explicit about tenant+repo scoping to preserve isolation.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal, Protocol, TypeAlias, runtime_checkable

from akc.memory.models import JSONValue, normalize_repo_id, require_non_empty


Stage: TypeAlias = Literal["plan", "retrieve", "generate", "execute", "repair"]


@dataclass(frozen=True, slots=True)
class TenantRepoScope:
    """Normalized tenant+repo scope for compile loop operations."""

    tenant_id: str
    repo_id: str

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="tenant_id")
        require_non_empty(self.repo_id, name="repo_id")
        object.__setattr__(self, "tenant_id", self.tenant_id.strip())
        object.__setattr__(self, "repo_id", normalize_repo_id(self.repo_id))


@dataclass(frozen=True, slots=True)
class IndexQuery:
    """A retrieval request against a structured index."""

    text: str
    k: int = 10
    filters: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.text, name="query.text")
        if int(self.k) <= 0:
            raise ValueError("query.k must be > 0")


@dataclass(frozen=True, slots=True)
class IndexDocument:
    """A retrieved document snippet suitable for prompt/context injection."""

    doc_id: str
    title: str | None
    content: str
    score: float | None = None
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.doc_id, name="doc_id")
        require_non_empty(self.content, name="content")


@runtime_checkable
class Index(Protocol):
    """Structured index adapter used during Retrieve.

    Implementations must enforce tenant isolation using the provided scope.
    """

    def query(self, *, scope: TenantRepoScope, query: IndexQuery) -> Sequence[IndexDocument]:
        """Return up to k results ordered by relevance."""


@dataclass(frozen=True, slots=True)
class LLMMessage:
    role: Literal["system", "user", "assistant", "tool"]
    content: str

    def __post_init__(self) -> None:
        require_non_empty(self.role, name="role")
        require_non_empty(self.content, name="content")


@dataclass(frozen=True, slots=True)
class LLMRequest:
    """A backend-agnostic LLM request."""

    messages: Sequence[LLMMessage]
    temperature: float = 0.2
    max_output_tokens: int | None = None
    stop: Sequence[str] | None = None
    metadata: Mapping[str, JSONValue] | None = None

    def __post_init__(self) -> None:
        if not self.messages:
            raise ValueError("messages must be non-empty")
        if self.max_output_tokens is not None and int(self.max_output_tokens) <= 0:
            raise ValueError("max_output_tokens must be > 0 when set")
        t = float(self.temperature)
        if t < 0.0 or t > 2.0:
            raise ValueError("temperature must be within [0.0, 2.0]")


@dataclass(frozen=True, slots=True)
class LLMResponse:
    """A backend-agnostic LLM response."""

    text: str
    raw: Mapping[str, Any] | None = None
    usage: Mapping[str, int] | None = None

    def __post_init__(self) -> None:
        require_non_empty(self.text, name="text")


@runtime_checkable
class LLMBackend(Protocol):
    """LLM backend used during Generate (and potentially Repair analysis)."""

    def complete(
        self,
        *,
        scope: TenantRepoScope,
        stage: Stage,
        request: LLMRequest,
    ) -> LLMResponse:
        """Perform a single completion call under the given scope and stage."""


@dataclass(frozen=True, slots=True)
class ExecutionRequest:
    """Request to execute a generated artifact in a sandbox."""

    command: Sequence[str]
    cwd: str | None = None
    env: Mapping[str, str] | None = None
    timeout_s: float | None = None
    stdin_text: str | None = None

    def __post_init__(self) -> None:
        if not self.command:
            raise ValueError("command must be non-empty")
        if self.timeout_s is not None and float(self.timeout_s) <= 0:
            raise ValueError("timeout_s must be > 0 when set")


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    exit_code: int
    stdout: str
    stderr: str
    duration_ms: int | None = None


@runtime_checkable
class Executor(Protocol):
    """Sandboxed executor used during Execute."""

    def run(
        self,
        *,
        scope: TenantRepoScope,
        request: ExecutionRequest,
    ) -> ExecutionResult:
        """Run a command in a sandbox and return captured output."""

