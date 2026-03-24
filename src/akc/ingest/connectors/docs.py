"""Docs connector (Markdown + HTML) for local filesystem ingestion.

Phase 1 scope: local files/directories only (no URL crawling yet).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from html import unescape
from html.parser import HTMLParser
from pathlib import Path, PurePosixPath

from akc.ingest.connectors.base import BaseConnector
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document


class _HTMLTextExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._chunks: list[str] = []
        self._skip_depth: int = 0  # nested <script>/<style>

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:  # noqa: ARG002
        if tag in {"script", "style", "noscript"}:
            self._skip_depth += 1
            return
        if self._skip_depth:
            return
        # Add lightweight separators around block-ish elements to avoid word-joins.
        if tag in {"p", "div", "br", "hr", "li", "ul", "ol", "section", "article"}:
            self._chunks.append("\n")
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._chunks.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"script", "style", "noscript"}:
            self._skip_depth = max(0, self._skip_depth - 1)
            return
        if self._skip_depth:
            return
        if tag in {"p", "div", "li", "section", "article"}:
            self._chunks.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth:
            return
        if data.strip():
            self._chunks.append(data)

    def text(self) -> str:
        raw = "".join(self._chunks)
        # Normalize whitespace while keeping paragraph breaks.
        raw = re.sub(r"[ \t]+\n", "\n", raw)
        raw = re.sub(r"\n{3,}", "\n\n", raw)
        raw = re.sub(r"[ \t]{2,}", " ", raw)
        return raw.strip()


_FRONTMATTER_RE = re.compile(r"\A---\s*\n.*?\n---\s*\n", re.DOTALL)
_CODE_FENCE_RE = re.compile(r"```.*?```", re.DOTALL)
_INLINE_CODE_RE = re.compile(r"`([^`]+)`")
_IMAGE_RE = re.compile(r"!\[([^\]]*)\]\(([^)]+)\)")
_LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")
_ATX_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+", re.MULTILINE)
_SETEXT_HEADING_RE = re.compile(r"^\s*(=+|-+)\s*$", re.MULTILINE)
_BLOCKQUOTE_RE = re.compile(r"^\s{0,3}>\s?", re.MULTILINE)
_LIST_MARKER_RE = re.compile(r"^\s*([-*+]|\d+\.)\s+", re.MULTILINE)


def _markdown_to_text(markdown: str) -> str:
    text = markdown.replace("\r\n", "\n")
    text = _FRONTMATTER_RE.sub("", text)
    text = _CODE_FENCE_RE.sub("\n", text)
    text = _ATX_HEADING_RE.sub("", text)
    text = _SETEXT_HEADING_RE.sub("", text)
    text = _BLOCKQUOTE_RE.sub("", text)
    text = _LIST_MARKER_RE.sub("", text)
    text = _IMAGE_RE.sub(r"\1", text)
    text = _LINK_RE.sub(r"\1", text)
    text = _INLINE_CODE_RE.sub(r"\1", text)
    # Remove common emphasis markers without trying to fully parse markdown.
    text = text.replace("**", "").replace("__", "").replace("*", "").replace("_", "")
    text = unescape(text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _html_to_text(html: str) -> str:
    parser = _HTMLTextExtractor()
    parser.feed(html)
    parser.close()
    return parser.text()


DEFAULT_INCLUDE_GLOBS: tuple[str, ...] = ("**/*.md", "**/*.markdown", "**/*.html", "**/*.htm")
DEFAULT_EXCLUDE_GLOBS: tuple[str, ...] = ("**/.git/**", "**/.venv/**", "**/node_modules/**")


@dataclass(frozen=True, slots=True)
class DocsConnectorConfig:
    root_path: Path
    include_globs: tuple[str, ...] = DEFAULT_INCLUDE_GLOBS
    exclude_globs: tuple[str, ...] = DEFAULT_EXCLUDE_GLOBS
    encoding: str = "utf-8"
    errors: str = "replace"
    max_bytes: int = 2_000_000
    reject_binary: bool = True


class DocsConnector(BaseConnector):
    """Ingest local Markdown/HTML docs from a filesystem root."""

    def __init__(self, *, tenant_id: str, config: DocsConnectorConfig) -> None:
        super().__init__(tenant_id=tenant_id, source_type="docs")
        root = config.root_path
        if not isinstance(root, Path):
            raise TypeError("config.root_path must be a pathlib.Path")
        root = root.expanduser()
        if not root.exists():
            raise ConnectorError(f"root path does not exist: {root}")
        self._config = DocsConnectorConfig(
            root_path=root,
            include_globs=tuple(config.include_globs),
            exclude_globs=tuple(config.exclude_globs),
            encoding=config.encoding,
            errors=config.errors,
            max_bytes=config.max_bytes,
            reject_binary=config.reject_binary,
        )

    def list_sources(self) -> Iterable[str]:
        return (str(p) for p in self._iter_files())

    def fetch(self, source_id: str) -> Iterable[Document]:
        try:
            path = Path(source_id).expanduser()
        except Exception as e:  # pragma: no cover
            raise ConnectorError("invalid source_id path") from e
        root = self._config.root_path.resolve()
        try:
            resolved = path.resolve()
        except FileNotFoundError as e:
            raise ConnectorError(f"source file not found: {path}") from e
        if not _is_within_root(resolved, root):
            raise ConnectorError("source_id is outside root_path")
        if not resolved.is_file():
            raise ConnectorError("source_id must be a file")

        content = _read_text(
            resolved,
            encoding=self._config.encoding,
            errors=self._config.errors,
            max_bytes=self._config.max_bytes,
            reject_binary=self._config.reject_binary,
        )
        extracted = _extract_text_for_path(resolved, content)
        if not extracted.strip():
            raise ConnectorError(f"extracted document content is empty: {resolved}")

        source = str(resolved)
        logical_locator = source
        return [
            self._make_document(
                source=source,
                logical_locator=logical_locator,
                content=extracted,
                metadata={"path": source},
            )
        ]

    def _iter_files(self) -> Iterator[Path]:
        root = self._config.root_path.resolve()
        candidates: set[Path] = set()

        if root.is_file():
            candidates.add(root)
        else:
            for pat in self._config.include_globs:
                for p in root.glob(pat):
                    candidates.add(p)

        for p in sorted(candidates):
            try:
                rp = p.resolve()
            except FileNotFoundError:
                continue
            if not _is_within_root(rp, root):
                continue
            if not rp.is_file():
                continue
            if not root.is_file():
                rel = rp.relative_to(root)
                if _matches_any_glob(rel, self._config.exclude_globs):
                    continue
            if rp.suffix.lower() not in {".md", ".markdown", ".html", ".htm"}:
                continue
            yield rp


def _read_text(path: Path, *, encoding: str, errors: str, max_bytes: int, reject_binary: bool) -> str:
    if max_bytes <= 0:
        raise ConnectorError("max_bytes must be > 0")
    try:
        with path.open("rb") as f:
            raw = f.read(max_bytes + 1)
    except OSError as e:
        raise ConnectorError(f"failed to read file: {path}") from e
    if len(raw) > max_bytes:
        raise ConnectorError(f"file exceeds max_bytes ({max_bytes}): {path}")
    if reject_binary and _looks_binary(raw):
        raise ConnectorError(f"file appears to be binary: {path}")
    try:
        return raw.decode(encoding, errors=errors)
    except LookupError as e:
        raise ConnectorError(f"unknown encoding '{encoding}'") from e
    except UnicodeDecodeError as e:  # pragma: no cover (errors may not be strict)
        raise ConnectorError(f"failed to decode text with encoding '{encoding}': {path}") from e


def _extract_text_for_path(path: Path, raw: str) -> str:
    suffix = path.suffix.lower()
    if suffix in {".md", ".markdown"}:
        return _markdown_to_text(raw)
    if suffix in {".html", ".htm"}:
        return _html_to_text(raw)
    raise ConnectorError(f"unsupported docs file type: {suffix}")


def _is_within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _matches_any_glob(rel_path: Path, globs: Sequence[str]) -> bool:
    # Path.match() is OS-path-flavor-dependent and can behave surprisingly when
    # patterns contain forward slashes on Windows. We normalize to posix here to
    # keep behavior deterministic across platforms.
    posix = PurePosixPath(rel_path.as_posix())
    return any(posix.match(pat) for pat in globs)


def _looks_binary(sample: bytes) -> bool:
    # Heuristic: NUL byte is a strong binary signal.
    if b"\x00" in sample:
        return True
    if not sample:
        return False
    # If most bytes are non-text-like, treat as binary. Keep permissive since
    # docs can include Unicode; we only flag very "noisy" content.
    # Count bytes outside common whitespace and printable ASCII range.
    noisy = 0
    for b in sample:
        if b in (9, 10, 13):  # \t \n \r
            continue
        if 32 <= b <= 126:
            continue
        noisy += 1
    return (noisy / len(sample)) > 0.30


def build_docs_connector(
    *,
    tenant_id: str,
    root_path: str | Path,
    include_globs: Sequence[str] | None = None,
    exclude_globs: Sequence[str] | None = None,
    encoding: str = "utf-8",
    errors: str = "replace",
    max_bytes: int = 2_000_000,
    reject_binary: bool = True,
) -> DocsConnector:
    """Convenience factory for creating a docs connector."""

    root = root_path if isinstance(root_path, Path) else Path(root_path)
    config = DocsConnectorConfig(
        root_path=root,
        include_globs=(tuple(include_globs) if include_globs is not None else DEFAULT_INCLUDE_GLOBS),
        exclude_globs=(tuple(exclude_globs) if exclude_globs is not None else DEFAULT_EXCLUDE_GLOBS),
        encoding=encoding,
        errors=errors,
        max_bytes=max_bytes,
        reject_binary=reject_binary,
    )
    return DocsConnector(tenant_id=tenant_id, config=config)
