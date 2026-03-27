from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Literal

try:
    # Optional dependency: activated only when installed.
    from tree_sitter import Parser  # type: ignore[import-not-found]
    from tree_sitter_languages import get_language  # type: ignore[import-not-found]

    _TS_AVAILABLE = True
except Exception:  # pragma: no cover (optional dependency)
    Parser = None
    get_language = None
    _TS_AVAILABLE = False


LanguageKey = Literal["python", "javascript", "typescript"]


def tree_sitter_available() -> bool:
    """Return whether tree-sitter parsing is available in this environment."""

    return bool(_TS_AVAILABLE)


_SKIP_DIRS = {
    ".git",
    ".akc",
    ".venv",
    "node_modules",
    "__pycache__",
    ".mypy_cache",
    ".pytest_cache",
    "dist",
    "build",
    "out",
    "target",
}


def _is_probably_text_file(path: Path) -> bool:
    try:
        return path.stat().st_size <= 2_000_000
    except OSError:
        return False


def _iter_files_under(root: Path, *, ext_allow: set[str], max_files: int) -> list[Path]:
    out: list[Path] = []
    if not root.is_dir():
        return out
    try:
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = sorted([d for d in dirnames if d not in _SKIP_DIRS])
            for name in sorted(filenames):
                fp = Path(dirpath) / name
                if fp.is_symlink() or not fp.is_file():
                    continue
                if fp.suffix.lower() not in ext_allow:
                    continue
                if not _is_probably_text_file(fp):
                    continue
                out.append(fp)
                if len(out) >= max_files:
                    return out
    except OSError:
        return out
    return out


def _walk_nodes(root_node: Any) -> list[Any]:
    # Iterative DFS walker to avoid recursion depth issues.
    out: list[Any] = []
    stack = [root_node]
    while stack:
        node = stack.pop()
        out.append(node)
        try:
            children = node.children  # tree_sitter Node exposes `.children`
        except Exception:
            children = []
        # Preserve a stable traversal order.
        for ch in reversed(children or []):
            stack.append(ch)
    return out


def _get_slice_text(*, source: bytes, node: Any) -> str:
    try:
        start = int(node.start_byte)
        end = int(node.end_byte)
    except Exception:
        return ""
    if start < 0 or end < start:
        return ""
    try:
        return source[start:end].decode("utf-8", errors="replace")
    except Exception:
        return ""


def _load_parsers() -> dict[LanguageKey, Any] | None:
    if not tree_sitter_available():
        return None
    assert Parser is not None and get_language is not None

    def _make(lang: LanguageKey) -> Any:
        parser = Parser()
        parser.set_language(get_language(lang))
        return parser

    return {
        "python": _make("python"),
        "javascript": _make("javascript"),
        "typescript": _make("typescript"),
    }


def extract_import_and_test_conventions_from_tree_sitter(
    *,
    code_root: Path,
    tests_dir: Path,
    max_files_per_language: int = 250,
    max_bytes_per_file: int = 250_000,
) -> tuple[dict[str, str], dict[str, str], dict[str, str]]:
    """Extract import style ratios + AST-detected test file org.

    Returns:
      - imports_conventions: dict for `ConventionSnapshot.imports` (ratio strings).
      - tests_conventions: dict for `ConventionSnapshot.tests` (count strings + ratios).
      - naming_conventions: dict for `ConventionSnapshot.naming` (identifier-based ratios).
    """

    parsers = _load_parsers()
    if parsers is None:
        return {}, {}, {}

    # Language file selection.
    py_test_files = _iter_files_under(tests_dir, ext_allow={".py"}, max_files=max_files_per_language)
    ts_test_files = _iter_files_under(tests_dir, ext_allow={".ts", ".tsx"}, max_files=max_files_per_language)
    js_test_files = _iter_files_under(tests_dir, ext_allow={".js", ".jsx"}, max_files=max_files_per_language)

    # Include colocated code-area tests too (files under code_root).
    py_test_files = py_test_files + _iter_files_under(code_root, ext_allow={".py"}, max_files=max_files_per_language)
    ts_test_files = ts_test_files + _iter_files_under(
        code_root, ext_allow={".ts", ".tsx"}, max_files=max_files_per_language
    )
    js_test_files = js_test_files + _iter_files_under(
        code_root, ext_allow={".js", ".jsx"}, max_files=max_files_per_language
    )

    # Deduplicate file paths while keeping order.
    def _dedupe(paths: list[Path]) -> list[Path]:
        seen: set[Path] = set()
        out: list[Path] = []
        for p in paths:
            if p in seen:
                continue
            seen.add(p)
            out.append(p)
        return out

    py_test_files = _dedupe(py_test_files)
    ts_test_files = _dedupe(ts_test_files)
    js_test_files = _dedupe(js_test_files)

    test_files_by_lang: list[tuple[LanguageKey, list[Path]]] = [
        ("python", py_test_files),
        ("typescript", ts_test_files),
        ("javascript", js_test_files),
    ]

    # Imports stats.
    total_imports = 0
    rel_imports = 0
    abs_imports = 0
    alias_imports = 0

    # Identifier naming stats.
    identifier_total = 0
    identifier_snake = 0
    identifier_camel = 0
    identifier_kebab = 0

    # Test file stats.
    separate_tests = 0
    colocated_tests = 0
    total_tests = 0

    # Node-type sets vary slightly across grammar versions; keep broad.
    py_import_node_types = {"import_statement", "import_from_statement"}
    js_ts_import_node_types = {"import_statement", "import_declaration"}
    py_func_node_type = "function_definition"
    js_ts_call_node_type = "call_expression"

    # Regexes on node slices (still AST-scoped).
    python_from_rel_rx = re.compile(r"^\s*from\s+\.+\s*[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", re.M)
    python_from_abs_rx = re.compile(r"^\s*from\s+[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", re.M)
    python_import_rel_rx = re.compile(r"^\s*import\s+\.+\s*[A-Za-z_]\w*", re.M)
    python_import_abs_rx = re.compile(r"^\s*import\s+[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*", re.M)
    js_import_from_path_rx = re.compile(r"""from\s+['"]([^'"]+)['"]""")

    # Test detection.
    py_test_def_rx = re.compile(r"\bdef\s+(test_[A-Za-z0-9_]+|[A-Za-z0-9_]+_test)\s*\(")
    js_ts_test_call_rx = re.compile(r"\b(test|it|describe)\s*\(")

    def _is_under_tests_dir(fp: Path) -> bool:
        try:
            return fp.resolve().is_relative_to(tests_dir.resolve())
        except Exception:
            # Best-effort fallback.
            try:
                return str(fp.resolve()).startswith(str(tests_dir.resolve()) + "/")
            except Exception:
                return False

    def _is_under_code_root(fp: Path) -> bool:
        try:
            return fp.resolve().is_relative_to(code_root.resolve())
        except Exception:
            try:
                return str(fp.resolve()).startswith(str(code_root.resolve()) + "/")
            except Exception:
                return False

    def _mark_test_file(fp: Path) -> None:
        nonlocal separate_tests, colocated_tests, total_tests
        if _is_under_tests_dir(fp):
            separate_tests += 1
        else:
            colocated_tests += 1
        total_tests += 1

    for lang, test_files in test_files_by_lang:
        parser = parsers.get(lang)
        if parser is None:
            continue
        node_import_types = py_import_node_types if lang == "python" else js_ts_import_node_types

        for fp in test_files:
            try:
                raw = fp.read_bytes()[:max_bytes_per_file]
            except OSError:
                continue
            if not raw:
                continue
            try:
                tree = parser.parse(raw)
            except Exception:
                continue
            root = tree.root_node
            nodes = _walk_nodes(root)

            # Identifier naming.
            # Keep this conservative: identifiers are abundant; cap for speed.
            identifiers_seen_in_file = 0
            max_identifiers_per_file = 2000
            for n in nodes:
                if identifiers_seen_in_file >= max_identifiers_per_file:
                    break
                if getattr(n, "type", None) != "identifier":
                    continue
                ident = _get_slice_text(source=raw, node=n).strip()
                if not ident:
                    continue
                identifier_total += 1
                identifiers_seen_in_file += 1
                if "_" in ident:
                    identifier_snake += 1
                if any(c.isupper() for c in ident):
                    identifier_camel += 1
                if "-" in ident:
                    identifier_kebab += 1

            # Imports.
            for n in nodes:
                ntype = getattr(n, "type", None)
                if ntype not in node_import_types:
                    continue
                stmt = _get_slice_text(source=raw, node=n)
                if not stmt.strip():
                    continue

                # Alias detection (`as` / `* as`).
                if re.search(r"\bas\s+", stmt):
                    alias_imports += 1

                # Import ratios should be computed from the primary code root only.
                # Test directories may include imports that skew project-wide conventions.
                if not _is_under_code_root(fp):
                    continue

                if lang == "python":
                    if python_from_rel_rx.search(stmt) or python_import_rel_rx.search(stmt):
                        rel_imports += 1
                        total_imports += 1
                    elif python_from_abs_rx.search(stmt) or python_import_abs_rx.search(stmt):
                        abs_imports += 1
                        total_imports += 1
                    else:
                        # Unknown python import; ignore to keep ratios sane.
                        continue
                else:
                    m = js_import_from_path_rx.search(stmt)
                    if not m:
                        continue
                    spec = m.group(1).strip()
                    total_imports += 1
                    if spec.startswith("."):
                        rel_imports += 1
                    else:
                        abs_imports += 1

            # Tests (AST-driven file classification).
            is_test_file = False
            if lang == "python":
                for n in nodes:
                    if getattr(n, "type", None) != py_func_node_type:
                        continue
                    stmt = _get_slice_text(source=raw, node=n)
                    if py_test_def_rx.search(stmt):
                        is_test_file = True
                        break
            else:
                for n in nodes:
                    if getattr(n, "type", None) != js_ts_call_node_type:
                        continue
                    stmt = _get_slice_text(source=raw, node=n)
                    if js_ts_test_call_rx.search(stmt):
                        is_test_file = True
                        break

            if is_test_file:
                _mark_test_file(fp)

    imports_conventions: dict[str, str] = {}
    if total_imports > 0:
        imports_conventions["relative_import_ratio"] = f"{rel_imports}/{total_imports}"
        imports_conventions["absolute_import_ratio"] = f"{abs_imports}/{total_imports}"
        imports_conventions["aliasing_ratio"] = f"{alias_imports}/{total_imports}"
        imports_conventions["import_preference"] = "relative" if rel_imports >= abs_imports else "absolute"

    tests_conventions: dict[str, str] = {}
    if total_tests > 0:
        tests_conventions["tests_separate_count"] = str(separate_tests)
        tests_conventions["tests_colocated_count"] = str(colocated_tests)
        tests_conventions["tests_total_count"] = str(total_tests)
        tests_conventions["tests_colocated_ratio"] = f"{colocated_tests}/{total_tests}"
        tests_conventions["test_org_preference"] = "colocated" if colocated_tests >= separate_tests else "separate"

    naming_conventions: dict[str, str] = {}
    if identifier_total > 0:
        naming_conventions["identifier_snake_case_ratio"] = f"{identifier_snake}/{identifier_total}"
        naming_conventions["identifier_camelcase_ratio"] = f"{identifier_camel}/{identifier_total}"
        naming_conventions["identifier_kebab_case_ratio"] = f"{identifier_kebab}/{identifier_total}"

    return imports_conventions, tests_conventions, naming_conventions
