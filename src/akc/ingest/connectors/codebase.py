"""Codebase connector for local filesystem ingestion.

This connector is intended for "Progressive Takeover" Level 0 (Observer):
it indexes an existing repository's source files so retrieval can ground
future compile runs.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path, PurePosixPath

from akc.ingest.connectors.base import BaseConnector
from akc.ingest.exceptions import ConnectorError
from akc.ingest.models import Document

DEFAULT_INCLUDE_GLOBS: tuple[str, ...] = (
    "**/*.py",
    "**/*.pyi",
    "**/*.md",
    "**/*.rst",
    "**/*.txt",
    "**/*.toml",
    "**/*.yaml",
    "**/*.yml",
    "**/*.json",
    "**/*.js",
    "**/*.jsx",
    "**/*.ts",
    "**/*.tsx",
    "**/*.go",
    "**/*.rs",
    "**/*.java",
    "**/*.kt",
    "**/*.kts",
    "**/*.sh",
    "**/*.sql",
)

DEFAULT_EXCLUDE_GLOBS: tuple[str, ...] = (
    "**/.git/**",
    "**/.akc/**",
    "**/.venv/**",
    "**/node_modules/**",
    "**/__pycache__/**",
    "**/.mypy_cache/**",
    "**/.pytest_cache/**",
    "**/dist/**",
    "**/build/**",
    "**/out/**",
    "**/target/**",
)


@dataclass(frozen=True, slots=True)
class CodebaseConnectorConfig:
    root_path: Path
    include_globs: tuple[str, ...] = DEFAULT_INCLUDE_GLOBS
    exclude_globs: tuple[str, ...] = DEFAULT_EXCLUDE_GLOBS
    encoding: str = "utf-8"
    errors: str = "replace"
    max_bytes: int = 2_000_000
    reject_binary: bool = True


class CodebaseConnector(BaseConnector):
    """Ingest source files from a filesystem root."""

    def __init__(self, *, tenant_id: str, config: CodebaseConnectorConfig) -> None:
        super().__init__(tenant_id=tenant_id, source_type="codebase")
        root = config.root_path
        if not isinstance(root, Path):
            raise TypeError("config.root_path must be a pathlib.Path")
        root = root.expanduser()
        if not root.exists():
            raise ConnectorError(f"root path does not exist: {root}")
        self._config = CodebaseConnectorConfig(
            root_path=root,
            include_globs=tuple(config.include_globs),
            exclude_globs=tuple(config.exclude_globs),
            encoding=config.encoding,
            errors=config.errors,
            max_bytes=int(config.max_bytes),
            reject_binary=bool(config.reject_binary),
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
        if not content.strip():
            raise ConnectorError(f"file content is empty after decode: {resolved}")

        source = str(resolved)
        logical_locator = _logical_locator(root=root, path=resolved)
        rel = _relposix(root, resolved)
        lang = _language_hint_from_suffix(resolved.suffix.lower())
        return [
            self._make_document(
                source=source,
                logical_locator=logical_locator,
                content=content,
                metadata={
                    "path": source,
                    "relpath": rel,
                    "language_hint": lang,
                },
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
            yield rp


def _relposix(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return str(path)


def _logical_locator(*, root: Path, path: Path) -> str:
    # Stable, human-friendly locator used for downstream provenance and
    # to avoid leaking absolute paths into prompts when unnecessary.
    rel = _relposix(root, path)
    return rel if rel else str(path)


def _is_within_root(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def _matches_any_glob(rel_path: Path, globs: Sequence[str]) -> bool:
    posix = PurePosixPath(rel_path.as_posix())
    return any(posix.match(pat) for pat in globs)


def _looks_binary(sample: bytes) -> bool:
    if b"\x00" in sample:
        return True
    if not sample:
        return False
    noisy = 0
    for b in sample:
        if b in (9, 10, 13):  # \t \n \r
            continue
        if 32 <= b <= 126:
            continue
        noisy += 1
    return (noisy / len(sample)) > 0.30


def _read_text(path: Path, *, encoding: str, errors: str, max_bytes: int, reject_binary: bool) -> str:
    if max_bytes <= 0:
        raise ConnectorError("max_bytes must be > 0")
    try:
        with path.open("rb") as f:
            raw = f.read(int(max_bytes) + 1)
    except OSError as e:
        raise ConnectorError(f"failed to read file: {path}") from e
    if len(raw) > int(max_bytes):
        raise ConnectorError(f"file exceeds max_bytes ({max_bytes}): {path}")
    if reject_binary and _looks_binary(raw):
        raise ConnectorError(f"file appears to be binary: {path}")
    try:
        return raw.decode(encoding, errors=errors)
    except LookupError as e:
        raise ConnectorError(f"unknown encoding '{encoding}'") from e
    except UnicodeDecodeError as e:  # pragma: no cover
        raise ConnectorError(f"failed to decode text with encoding '{encoding}': {path}") from e


def _language_hint_from_suffix(suffix: str) -> str:
    s = suffix.lower()
    return {
        ".py": "python",
        ".pyi": "python",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".js": "javascript",
        ".jsx": "javascript",
        ".rs": "rust",
        ".go": "go",
        ".java": "java",
        ".kt": "kotlin",
        ".kts": "kotlin",
        ".sh": "shell",
        ".sql": "sql",
        ".md": "markdown",
        ".rst": "rst",
        ".toml": "toml",
        ".yml": "yaml",
        ".yaml": "yaml",
        ".json": "json",
        ".txt": "text",
    }.get(s, "text")


def build_codebase_connector(
    *,
    tenant_id: str,
    root_path: str | Path,
    include_globs: Sequence[str] | None = None,
    exclude_globs: Sequence[str] | None = None,
    encoding: str = "utf-8",
    errors: str = "replace",
    max_bytes: int = 2_000_000,
    reject_binary: bool = True,
) -> CodebaseConnector:
    root = root_path if isinstance(root_path, Path) else Path(root_path)
    config = CodebaseConnectorConfig(
        root_path=root,
        include_globs=(tuple(include_globs) if include_globs is not None else DEFAULT_INCLUDE_GLOBS),
        exclude_globs=(tuple(exclude_globs) if exclude_globs is not None else DEFAULT_EXCLUDE_GLOBS),
        encoding=encoding,
        errors=errors,
        max_bytes=max_bytes,
        reject_binary=reject_binary,
    )
    return CodebaseConnector(tenant_id=tenant_id, config=config)
