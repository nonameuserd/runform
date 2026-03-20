from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any, Literal, cast

from akc.ir.provenance import ProvenancePointer
from akc.ir.versioning import (
    IR_FORMAT_VERSION,
    IR_SCHEMA_KIND,
    IR_SCHEMA_VERSION,
    require_supported_ir_version,
)
from akc.memory.models import JSONValue, require_non_empty
from akc.utils.fingerprint import stable_json_fingerprint

NodeKind = Literal["service", "workflow", "entity", "integration", "policy", "other"]
ALLOWED_NODE_KINDS: tuple[str, ...] = (
    "service",
    "workflow",
    "entity",
    "integration",
    "policy",
    "other",
)


def stable_node_id(*, kind: str, name: str) -> str:
    require_non_empty(kind, name="kind")
    require_non_empty(name, name="name")
    raw = f"{kind.strip()}::{name.strip()}".encode()
    return f"irn_{sha256(raw).hexdigest()[:16]}"


@dataclass(frozen=True, slots=True)
class EffectAnnotation:
    """Effect declaration for a node, used by policy/runtime gates."""

    network: bool = False
    fs_read: tuple[str, ...] = ()
    fs_write: tuple[str, ...] = ()
    secrets: tuple[str, ...] = ()
    tools: tuple[str, ...] = ()

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "network": bool(self.network),
            "fs_read": list(self.fs_read),
            "fs_write": list(self.fs_write),
            "secrets": list(self.secrets),
            "tools": list(self.tools),
        }

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> EffectAnnotation:
        return EffectAnnotation(
            network=bool(obj.get("network", False)),
            fs_read=tuple(str(x) for x in (obj.get("fs_read") or [])),
            fs_write=tuple(str(x) for x in (obj.get("fs_write") or [])),
            secrets=tuple(str(x) for x in (obj.get("secrets") or [])),
            tools=tuple(str(x) for x in (obj.get("tools") or [])),
        )


@dataclass(frozen=True, slots=True)
class IRNode:
    """Typed node inside the compiler IR graph."""

    id: str
    tenant_id: str
    kind: NodeKind
    name: str
    properties: Mapping[str, JSONValue]
    depends_on: tuple[str, ...] = ()
    effects: EffectAnnotation | None = None
    provenance: tuple[ProvenancePointer, ...] = ()

    def __post_init__(self) -> None:
        require_non_empty(self.id, name="ir_node.id")
        require_non_empty(self.tenant_id, name="ir_node.tenant_id")
        require_non_empty(self.kind, name="ir_node.kind")
        require_non_empty(self.name, name="ir_node.name")
        if self.kind not in ALLOWED_NODE_KINDS:
            raise ValueError(f"ir_node.kind must be one of {ALLOWED_NODE_KINDS}; got {self.kind!r}")
        for pointer in self.provenance:
            if pointer.tenant_id.strip() != self.tenant_id.strip():
                raise ValueError("all provenance pointers must match ir_node.tenant_id")

    def to_json_obj(self) -> dict[str, JSONValue]:
        deps_str = sorted({str(d).strip() for d in self.depends_on if str(d).strip()})
        # NOTE: JSONValue uses `list[JSONValue]`; `list[str]` is not compatible due to invariance.
        deps: list[JSONValue] = [cast(JSONValue, s) for s in deps_str]

        provenance_sorted_raw = sorted(
            (p.to_json_obj() for p in self.provenance),
            key=lambda x: (
                str(x.get("kind", "")),
                str(x.get("source_id", "")),
                str(x.get("locator", "")),
            ),
        )
        # NOTE: JSONValue uses `list[JSONValue]`; `list[dict[str, JSONValue]]`
        # is not compatible due to invariance.
        provenance_sorted: list[JSONValue] = [cast(JSONValue, x) for x in provenance_sorted_raw]
        out: dict[str, JSONValue] = {
            "id": self.id.strip(),
            "tenant_id": self.tenant_id.strip(),
            "kind": self.kind,
            "name": self.name.strip(),
            "properties": dict(self.properties),
            "depends_on": deps,
            "effects": self.effects.to_json_obj() if self.effects is not None else None,
            "provenance": provenance_sorted,
        }
        return {k: v for k, v in out.items() if v is not None}

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> IRNode:
        props = obj.get("properties")
        if not isinstance(props, dict):
            raise ValueError("ir_node.properties must be an object")

        depends_raw = obj.get("depends_on") or []
        if not isinstance(depends_raw, Sequence) or isinstance(depends_raw, (str, bytes)):
            raise ValueError("ir_node.depends_on must be an array")

        effects_raw = obj.get("effects")
        if effects_raw is not None and not isinstance(effects_raw, dict):
            raise ValueError("ir_node.effects must be an object when set")

        prov_raw = obj.get("provenance") or []
        if not isinstance(prov_raw, Sequence) or isinstance(prov_raw, (str, bytes)):
            raise ValueError("ir_node.provenance must be an array")

        provenance: list[ProvenancePointer] = []
        for p in prov_raw:
            if not isinstance(p, dict):
                raise ValueError("ir_node.provenance[] must be an object")
            provenance.append(ProvenancePointer.from_json_obj(dict(p)))

        return IRNode(
            id=str(obj.get("id", "")),
            tenant_id=str(obj.get("tenant_id", "")),
            kind=str(obj.get("kind", "other")),  # type: ignore[arg-type]
            name=str(obj.get("name", "")),
            properties=props,
            depends_on=tuple(str(x) for x in depends_raw),
            effects=(
                EffectAnnotation.from_json_obj(effects_raw)
                if isinstance(effects_raw, dict)
                else None
            ),
            provenance=tuple(provenance),
        )


@dataclass(frozen=True, slots=True)
class IRDocument:
    """Versioned intermediate representation for one tenant+repo scope."""

    tenant_id: str
    repo_id: str
    nodes: tuple[IRNode, ...]
    schema_version: int = IR_SCHEMA_VERSION
    format_version: str = IR_FORMAT_VERSION

    def __post_init__(self) -> None:
        require_non_empty(self.tenant_id, name="ir.tenant_id")
        require_non_empty(self.repo_id, name="ir.repo_id")
        require_supported_ir_version(
            schema_version=int(self.schema_version),
            format_version=self.format_version,
        )
        seen: set[str] = set()
        for n in self.nodes:
            if n.id in seen:
                raise ValueError(f"duplicate ir node id: {n.id}")
            if n.tenant_id.strip() != self.tenant_id.strip():
                raise ValueError("all ir nodes must match ir.tenant_id")
            seen.add(n.id)

    def to_json_obj(self) -> dict[str, JSONValue]:
        nodes = sorted((n.to_json_obj() for n in self.nodes), key=lambda x: str(x["id"]))
        nodes_value = cast(JSONValue, nodes)
        return {
            "schema_kind": IR_SCHEMA_KIND,
            "schema_version": int(self.schema_version),
            "format_version": self.format_version,
            "tenant_id": self.tenant_id.strip(),
            "repo_id": self.repo_id.strip(),
            "nodes": nodes_value,
        }

    def to_json_file(self, path: str | Path) -> None:
        """Write this IRDocument to a JSON file (deterministic key order)."""
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = self.to_json_obj()
        p.write_text(
            json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=False),
            encoding="utf-8",
        )

    def fingerprint(self) -> str:
        return stable_json_fingerprint(self.to_json_obj())

    @staticmethod
    def from_json_obj(obj: Mapping[str, Any]) -> IRDocument:
        if str(obj.get("schema_kind", "")) != IR_SCHEMA_KIND:
            raise ValueError(f"ir.schema_kind must be {IR_SCHEMA_KIND}")
        nodes_raw = obj.get("nodes")
        if not isinstance(nodes_raw, Sequence) or isinstance(nodes_raw, (str, bytes)):
            raise ValueError("ir.nodes must be an array")
        nodes: list[IRNode] = []
        for n in nodes_raw:
            if not isinstance(n, dict):
                raise ValueError("ir.nodes[] must be objects")
            nodes.append(IRNode.from_json_obj(n))
        return IRDocument(
            tenant_id=str(obj.get("tenant_id", "")),
            repo_id=str(obj.get("repo_id", "")),
            nodes=tuple(nodes),
            schema_version=int(obj.get("schema_version", IR_SCHEMA_VERSION)),
            format_version=str(obj.get("format_version", IR_FORMAT_VERSION)),
        )

    @staticmethod
    def from_json_file(path: str | Path) -> IRDocument:
        """Load an IRDocument from a JSON file."""
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, Mapping):
            raise ValueError("ir file must contain a JSON object")
        return IRDocument.from_json_obj(raw)
