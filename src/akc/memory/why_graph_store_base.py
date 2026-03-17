"""Why graph store base types (Phase 2)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from akc.memory.models import WhyEdge, WhyNode, WhyNodeType, normalize_repo_id, require_non_empty


class WhyGraphError(Exception):
    """Raised when a why-graph operation fails."""


def require_scope(*, tenant_id: str, repo_id: str) -> str:
    """Validate tenant_id and normalize repo_id for scoping."""

    require_non_empty(tenant_id, name="tenant_id")
    return normalize_repo_id(repo_id)


class WhyGraphStore(ABC):
    @abstractmethod
    def upsert_nodes(self, *, tenant_id: str, repo_id: str, nodes: Iterable[WhyNode]) -> int: ...

    @abstractmethod
    def add_edges(self, *, tenant_id: str, repo_id: str, edges: Iterable[WhyEdge]) -> int: ...

    @abstractmethod
    def get_node(self, *, tenant_id: str, repo_id: str, node_id: str) -> WhyNode | None: ...

    @abstractmethod
    def iter_out_edges(self, *, tenant_id: str, repo_id: str, src: str) -> Iterator[WhyEdge]: ...

    @abstractmethod
    def list_nodes_by_type(
        self, *, tenant_id: str, repo_id: str, node_type: WhyNodeType
    ) -> list[WhyNode]: ...


@dataclass(frozen=True, slots=True)
class ConstraintKey:
    subject: str
    predicate: str
    object: str | None
    polarity: int
    scope: str
