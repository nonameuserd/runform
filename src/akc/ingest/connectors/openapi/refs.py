from __future__ import annotations

import urllib.parse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from akc.ingest.connectors.openapi.loader import load_spec_bytes, parse_spec


class RefResolver:
    def __init__(
        self,
        *,
        root: Mapping[str, Any],
        base_path: Path | None,
        resolve_external: bool,
        max_bytes: int,
        allow_urls: bool,
        user_agent: str,
        timeout_seconds: float,
        max_depth: int,
    ) -> None:
        self._root = root
        self._base_path = base_path
        self._resolve_external = resolve_external
        self._max_bytes = max_bytes
        self._allow_urls = allow_urls
        self._user_agent = user_agent
        self._timeout_seconds = timeout_seconds
        self._max_depth = max_depth
        self._cache: dict[str, Any] = {}

    def resolve(self, obj: Any) -> Any:
        return self._resolve(obj, depth=0, seen=set())

    def _resolve(self, obj: Any, *, depth: int, seen: set[str]) -> Any:
        if depth > self._max_depth:
            return obj
        if isinstance(obj, dict):
            ref = obj.get("$ref")
            if isinstance(ref, str) and ref.strip():
                target = self._resolve_ref(ref.strip(), seen=seen)
                if isinstance(target, dict):
                    merged = dict(target)
                    for k, v in obj.items():
                        if k == "$ref":
                            continue
                        merged[k] = v
                    return self._resolve(merged, depth=depth + 1, seen=seen)
                return target
            return {k: self._resolve(v, depth=depth + 1, seen=seen) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._resolve(x, depth=depth + 1, seen=seen) for x in obj]
        return obj

    def _resolve_ref(self, ref: str, *, seen: set[str]) -> Any:
        if ref in seen:
            return {"$ref": ref, "note": "cycle-detected"}
        seen.add(ref)
        try:
            if ref.startswith("#/"):
                return self._resolve_pointer(self._root, ref)
            if not self._resolve_external:
                return {"$ref": ref, "note": "external-ref-not-resolved"}
            return self._resolve_external_ref(ref)
        finally:
            seen.discard(ref)

    def _resolve_pointer(self, root: Mapping[str, Any], pointer: str) -> Any:
        parts = pointer.lstrip("#/").split("/")
        cur: Any = root
        for part in parts:
            part = part.replace("~1", "/").replace("~0", "~")
            if isinstance(cur, dict) and part in cur:
                cur = cur[part]
                continue
            return {"$ref": pointer, "note": "unresolvable-pointer"}
        return cur

    def _resolve_external_ref(self, ref: str) -> Any:
        if ref in self._cache:
            return self._cache[ref]

        parsed = urllib.parse.urlparse(ref)
        if parsed.scheme in {"http", "https"}:
            if not self._allow_urls:
                val: Any = {"$ref": ref, "note": "external-url-disabled"}
                self._cache[ref] = val
                return val
            raw, canonical, _ = load_spec_bytes(
                ref,
                allow_urls=True,
                max_bytes=self._max_bytes,
                user_agent=self._user_agent,
                timeout_seconds=self._timeout_seconds,
            )
            ext = parse_spec(raw, source_hint=canonical)
            fragment = parsed.fragment
            val2: Any = (
                ext if not fragment else self._resolve_pointer(ext, "#/" + fragment.lstrip("/"))
            )
            self._cache[ref] = val2
            return val2

        if self._base_path is None:
            val3: Any = {"$ref": ref, "note": "no-base-path-for-external-ref"}
            self._cache[ref] = val3
            return val3

        file_part, frag = (ref.split("#", 1) + [""])[:2]
        candidate = (self._base_path / file_part).resolve()
        try:
            candidate.relative_to(self._base_path.resolve())
        except ValueError:
            val4: Any = {"$ref": ref, "note": "external-ref-outside-base-dir"}
            self._cache[ref] = val4
            return val4

        raw2, canonical2, base2 = load_spec_bytes(
            str(candidate),
            allow_urls=False,
            max_bytes=self._max_bytes,
            user_agent=self._user_agent,
            timeout_seconds=self._timeout_seconds,
        )
        ext2 = parse_spec(raw2, source_hint=canonical2)
        fragment2 = frag

        prev = self._base_path
        self._base_path = base2
        try:
            val5: Any = (
                ext2 if not fragment2 else self._resolve_pointer(ext2, "#/" + fragment2.lstrip("/"))
            )
        finally:
            self._base_path = prev
        self._cache[ref] = val5
        return val5
