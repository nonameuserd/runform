from __future__ import annotations

import json
import os
import re
import shlex
import tomllib
from pathlib import Path
from typing import Any

from .profile import (
    BuildCommand,
    BuildCommandKind,
    CISystem,
    ConventionSnapshot,
    LanguageEntry,
    ProjectProfile,
)


def _is_probably_text_file(path: Path) -> bool:
    try:
        # Size-only gate; content sampling is optional and would add overhead.
        return path.stat().st_size <= 2_000_000
    except OSError:
        return False


def _walk_files(root: Path) -> list[Path]:
    # Note: we keep detection lightweight and deterministic. Full .gitignore parsing
    # is intentionally out of scope for the initial `--detect` profile emitter.
    skip_dirs = {
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
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        # Prune in-place for os.walk.
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        for name in filenames:
            fp = Path(dirpath) / name
            if fp.is_symlink():
                continue
            if not _is_probably_text_file(fp):
                continue
            out.append(fp)
    # Determinism: os.walk traversal order can vary by filesystem. Sort paths to keep
    # evidence selection stable across runs.
    return sorted(out)


def _relposix(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        # If `path` is somehow outside `root`, fall back to the absolute path.
        return str(path)


def _detect_ci_systems(root: Path) -> list[CISystem]:
    systems: list[CISystem] = []
    if (root / ".github" / "workflows").is_dir():
        systems.append(CISystem(name="github_actions", evidence=(".github/workflows",)))
    if (root / ".gitlab-ci.yml").is_file():
        systems.append(CISystem(name="gitlab_ci", evidence=(".gitlab-ci.yml",)))
    if (root / "Jenkinsfile").is_file():
        systems.append(CISystem(name="jenkins", evidence=("Jenkinsfile",)))
    return systems


_COMMAND_KIND_REGEXES: dict[BuildCommandKind, tuple[re.Pattern[str], ...]] = {
    "test": (
        re.compile(r"\bpytest\b"),
        re.compile(r"\bvitest\b"),
        re.compile(r"\bjest\b"),
        re.compile(r"\bplaywright\s+test\b"),
        re.compile(r"\bmocha\b"),
        re.compile(r"\bava\b"),
        re.compile(r"\btox\b"),
        re.compile(r"\bnox\b"),
        re.compile(r"\bcargo\s+test\b"),
        re.compile(r"\bgo\s+test\b"),
        re.compile(r"\bctest\b"),
        re.compile(r"\b(?:pnpm|npm|yarn|bun)\s+(?:run\s+)?test\b"),
        re.compile(r"\b(?:nx|turbo)\b.*\btest\b"),
        re.compile(r"\bmake\s+(?:ci[-_:])?test\b"),
    ),
    "lint": (
        re.compile(r"\beslint\b"),
        re.compile(r"\bruff\s+check\b"),
        re.compile(r"\bflake8\b"),
        re.compile(r"\bgolangci-lint\b"),
        re.compile(r"\bcargo\s+clippy\b"),
        re.compile(r"\b(?:pnpm|npm|yarn|bun)\s+(?:run\s+)?lint\b"),
        re.compile(r"\b(?:nx|turbo)\b.*\blint\b"),
        re.compile(r"\bmake\s+lint\b"),
    ),
    "typecheck": (
        re.compile(r"\bmypy\b"),
        re.compile(r"\bpyright\b"),
        re.compile(r"\bbasedpyright\b"),
        re.compile(r"\bvue-tsc\b"),
        re.compile(r"\btsc\b"),
        re.compile(r"\bcargo\s+check\b"),
        re.compile(r"\bgo\s+vet\b"),
        re.compile(r"\b(?:pnpm|npm|yarn|bun)\s+(?:run\s+)?type-?check\b"),
        re.compile(r"\b(?:nx|turbo)\b.*\btype-?check\b"),
        re.compile(r"\bmake\s+(?:type-?check|check-types)\b"),
    ),
    "format": (
        re.compile(r"\bprettier\b"),
        re.compile(r"\bruff\s+format\b"),
        re.compile(r"\bblack\b"),
        re.compile(r"\brustfmt\b"),
        re.compile(r"\bcargo\s+fmt\b"),
        re.compile(r"\bgofmt\b"),
        re.compile(r"\b(?:pnpm|npm|yarn|bun)\s+(?:run\s+)?format\b"),
        re.compile(r"\b(?:nx|turbo)\b.*\bformat\b"),
        re.compile(r"\bmake\s+format\b"),
    ),
    "build": (
        re.compile(r"\bcargo\s+build\b"),
        re.compile(r"\bgo\s+build\b"),
        re.compile(r"\bmvn\b.*\b(?:package|verify|install|compile)\b"),
        re.compile(r"\b(?:gradle|gradlew)\b.*\bbuild\b"),
        re.compile(r"\bvite\s+build\b"),
        re.compile(r"\bwebpack\b"),
        re.compile(r"\bnext\s+build\b"),
        re.compile(r"\b(?:pnpm|npm|yarn|bun)\s+(?:run\s+)?build\b"),
        re.compile(r"\b(?:nx|turbo)\b.*\bbuild\b"),
        re.compile(r"\bmake\s+build\b"),
    ),
}

_SCRIPT_NAME_EXACT_KIND: dict[str, BuildCommandKind] = {
    "build": "build",
    "compile": "build",
    "bundle": "build",
    "test": "test",
    "tests": "test",
    "lint": "lint",
    "format": "format",
    "fmt": "format",
    "typecheck": "typecheck",
    "type_check": "typecheck",
    "check_types": "typecheck",
}

_SCRIPT_NAME_TOKEN_KIND: tuple[tuple[str, BuildCommandKind], ...] = (
    ("typecheck", "typecheck"),
    ("type_check", "typecheck"),
    ("check_types", "typecheck"),
    ("lint", "lint"),
    ("format", "format"),
    ("fmt", "format"),
    ("build", "build"),
    ("compile", "build"),
    ("bundle", "build"),
    ("test", "test"),
    ("tests", "test"),
    ("spec", "test"),
    ("e2e", "test"),
    ("integration", "test"),
)


def _maybe_parse_command_tokens(command_text: str) -> tuple[str, ...]:
    s = command_text.strip()
    if not s:
        return ()
    try:
        return tuple(shlex.split(s))
    except ValueError:
        # Best-effort fallback when Makefiles include non-shell syntax.
        return (s,)


def _dedupe_build_commands(cmds: list[BuildCommand]) -> list[BuildCommand]:
    seen: set[tuple[str, tuple[str, ...]]] = set()
    out: list[BuildCommand] = []
    for cmd in cmds:
        key = (cmd.kind, cmd.command)
        if key in seen:
            continue
        seen.add(key)
        out.append(cmd)
    return out


def _normalize_hint_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")


def _kind_from_name_hint(name: str) -> BuildCommandKind | None:
    normalized = _normalize_hint_name(name)
    if not normalized:
        return None
    exact = _SCRIPT_NAME_EXACT_KIND.get(normalized)
    if exact is not None:
        return exact
    parts = tuple(part for part in normalized.split("_") if part)
    for token, kind in _SCRIPT_NAME_TOKEN_KIND:
        if token in parts:
            return kind
    return None


def _split_shell_like_command_text(command_text: str) -> list[str]:
    parts: list[str] = []
    for raw_line in str(command_text).splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        line = line.lstrip("-").strip()
        if not line:
            continue
        for fragment in re.split(r"\s*(?:&&|\|\||;)\s*", line):
            candidate = fragment.strip()
            if candidate:
                parts.append(candidate)
    return parts


def _classify_command_kind(
    *,
    command_text: str,
    name_hint: str | None = None,
    context_hint: str | None = None,
) -> BuildCommandKind | None:
    for hint in (name_hint, context_hint):
        if hint is None:
            continue
        kind = _kind_from_name_hint(hint)
        if kind is not None:
            return kind

    for pattern_kind in ("test", "typecheck", "lint", "format", "build"):
        regexes = _COMMAND_KIND_REGEXES.get(pattern_kind, ())
        if any(rx.search(command_text) for rx in regexes):
            return pattern_kind
    return None


def _extract_build_commands_from_command_text(
    *,
    command_text: str,
    source: str,
    name_hint: str | None = None,
    context_hint: str | None = None,
) -> list[BuildCommand]:
    out: list[BuildCommand] = []
    for fragment in _split_shell_like_command_text(command_text):
        kind = _classify_command_kind(command_text=fragment, name_hint=name_hint, context_hint=context_hint)
        if kind is None:
            continue
        tokens = _maybe_parse_command_tokens(fragment)
        if not tokens:
            continue
        out.append(BuildCommand(command=tokens, kind=kind, source=source))
    return _dedupe_build_commands(out)


def _read_json_object(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _parse_package_json_scripts(pkg_json: Path) -> list[BuildCommand]:
    raw = _read_json_object(pkg_json)
    if raw is None:
        return []
    scripts = raw.get("scripts")
    if not isinstance(scripts, dict):
        return []

    out: list[BuildCommand] = []
    for name, value in sorted(scripts.items(), key=lambda item: str(item[0])):
        if not isinstance(value, str):
            continue
        script_name = str(name).strip()
        if not script_name or not value.strip():
            continue
        out.extend(
            _extract_build_commands_from_command_text(
                command_text=value,
                source=f"{pkg_json}#scripts.{script_name}",
                name_hint=script_name,
            )
        )
    return _dedupe_build_commands(out)


def _parse_makefile_test_commands(makefile: Path) -> list[BuildCommand]:
    try:
        text = makefile.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    out: list[BuildCommand] = []
    current_target: str | None = None
    for line in text.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if not line.startswith((" ", "\t")) and ":" in line:
            target = line.split(":", 1)[0].strip()
            current_target = target if target else None
            continue
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        out.extend(
            _extract_build_commands_from_command_text(
                command_text=stripped,
                source=f"{makefile}#{current_target}" if current_target else str(makefile),
                name_hint=current_target,
            )
        )
    return _dedupe_build_commands(out)


def _parse_pyproject_test_commands(pyproject: Path) -> list[BuildCommand]:
    # We keep TOML parsing deterministic via stdlib `tomllib`. If parsing fails,
    # fall back to minimal substring heuristics.
    raw_text: str
    try:
        raw_text = pyproject.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    try:
        data = tomllib.loads(raw_text)
    except tomllib.TOMLDecodeError:
        data = None

    out: list[BuildCommand] = []

    if isinstance(data, dict):
        tool = data.get("tool")
        if isinstance(tool, dict):
            pytest_tool = tool.get("pytest")
            if isinstance(pytest_tool, dict):
                ini_options = pytest_tool.get("ini_options")
                if isinstance(ini_options, dict):
                    addopts = ini_options.get("addopts")
                    if isinstance(addopts, str) and addopts.strip():
                        out.extend(
                            _extract_build_commands_from_command_text(
                                command_text=f"pytest {addopts}",
                                source=f"{pyproject}#tool.pytest.ini_options",
                                name_hint="test",
                            )
                        )
                    else:
                        out.append(
                            BuildCommand(
                                command=("pytest",),
                                kind="test",
                                source=f"{pyproject}#tool.pytest",
                            ),
                        )

        # Poetry scripts and PEP-621 project.scripts: deterministic extraction for
        # project-defined command wrappers such as `test:ci`, `check-types`, etc.
        for scripts_path in (("tool", "poetry", "scripts"), ("project", "scripts")):
            cursor: Any = data
            for part in scripts_path:
                if not isinstance(cursor, dict):
                    cursor = None
                    break
                cursor = cursor.get(part)
            if not isinstance(cursor, dict):
                continue
            for name, value in cursor.items():
                if not isinstance(value, str):
                    continue
                out.extend(
                    _extract_build_commands_from_command_text(
                        command_text=value,
                        source=f"{pyproject}#{'.'.join(scripts_path)}.{name}",
                        name_hint=str(name),
                    ),
                )

    # Minimal fallback: ensure we emit at least one pytest command if the file clearly
    # references pytest in tooling.
    if not out and re.search(r"\bpytest\b", raw_text):
        out.append(
            BuildCommand(
                command=("python", "-m", "pytest", "-q"),
                kind="test",
                source=str(pyproject),
            ),
        )

    return _dedupe_build_commands(out)


def _discover_repo_files(
    root: Path,
    *,
    names: tuple[str, ...],
    respect_gitignore: bool = False,
    max_files: int = 64,
) -> list[Path]:
    skip_dirs = {
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
    if respect_gitignore:
        skip_dirs = set(skip_dirs) | set(_gitignore_simple_skip_dir_names(root))

    wanted = set(names)
    found: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted([d for d in dirnames if d not in skip_dirs])
        for filename in sorted(filenames):
            if filename not in wanted:
                continue
            found.append(Path(dirpath) / filename)
            if len(found) >= int(max_files):
                return found
    return found


def _package_json_package_manager(pkg_json: Path) -> str | None:
    raw = _read_json_object(pkg_json)
    if raw is None:
        return None
    package_manager = str(raw.get("packageManager") or "").strip().lower()
    if package_manager.startswith("pnpm@"):
        return "pnpm"
    if package_manager.startswith("yarn@"):
        return "yarn"
    if package_manager.startswith("npm@"):
        return "npm"
    return None


def _detect_package_managers(root: Path) -> list[str]:
    out: list[str] = []
    package_json_paths = _discover_repo_files(root, names=("package.json",), max_files=24)
    if package_json_paths:
        out.append("npm_or_node")
        if any(_package_json_package_manager(path) == "pnpm" for path in package_json_paths):
            out.append("pnpm")
        if any(_package_json_package_manager(path) == "yarn" for path in package_json_paths):
            out.append("yarn")
        if any(_package_json_package_manager(path) == "npm" for path in package_json_paths):
            out.append("npm")
        if (root / "pnpm-lock.yaml").is_file():
            out.append("pnpm")
        if (root / "pnpm-workspace.yaml").is_file():
            out.append("pnpm")
        if (root / "yarn.lock").is_file():
            out.append("yarn")
        if (root / "package-lock.json").is_file():
            out.append("npm")
    if _discover_repo_files(root, names=("pyproject.toml", "setup.py"), max_files=24):
        out.append("pip_or_py")
        if _discover_repo_files(root, names=("uv.lock",), max_files=8):
            out.append("uv")
    if _discover_repo_files(root, names=("Cargo.toml",), max_files=24):
        out.append("cargo")
    if _discover_repo_files(root, names=("go.mod",), max_files=24):
        out.append("go")
    # De-duplicate while keeping order.
    seen: set[str] = set()
    deduped: list[str] = []
    for x in out:
        if x in seen:
            continue
        seen.add(x)
        deduped.append(x)
    return deduped


_EXTENSION_TO_LANGUAGE: dict[str, str] = {
    ".py": "python",
    ".rs": "rust",
    ".go": "go",
    ".java": "java",
    ".kt": "kotlin",
    ".ts": "typescript",
    ".tsx": "typescript",
    ".js": "javascript",
    ".jsx": "javascript",
}


def _detect_languages(root: Path) -> list[LanguageEntry]:
    # Step 1: manifest files -> definitive stack signals.
    manifest_bytes_by_lang: dict[str, int] = {}
    manifest_evidence_by_lang: dict[str, list[str]] = {}

    def _add_manifest_lang(lang: str, evidence_path: Path) -> None:
        manifest_bytes_by_lang[lang] = max(1, manifest_bytes_by_lang.get(lang, 0) + 1)
        manifest_evidence_by_lang.setdefault(lang, [])
        rel = _relposix(root, evidence_path)
        if rel not in manifest_evidence_by_lang[lang]:
            manifest_evidence_by_lang[lang].append(rel)

    pyproject_paths = _discover_repo_files(root, names=("pyproject.toml", "setup.py"), max_files=24)
    cargo_paths = _discover_repo_files(root, names=("Cargo.toml",), max_files=24)
    go_paths = _discover_repo_files(root, names=("go.mod",), max_files=24)
    package_json_paths = _discover_repo_files(root, names=("package.json",), max_files=24)
    tsconfig_paths = _discover_repo_files(root, names=("tsconfig.json",), max_files=24)
    pom_paths = _discover_repo_files(root, names=("pom.xml",), max_files=24)
    build_gradle_paths = _discover_repo_files(root, names=("build.gradle",), max_files=24)
    build_gradle_kts_paths = _discover_repo_files(root, names=("build.gradle.kts",), max_files=24)

    for path in pyproject_paths:
        _add_manifest_lang("python", path)
    for path in cargo_paths:
        _add_manifest_lang("rust", path)
    for path in go_paths:
        _add_manifest_lang("go", path)

    # Node/JS/TS refinement: any `tsconfig.json` upgrades the sibling package scope to TypeScript.
    if tsconfig_paths:
        tsconfig_dirs = {path.parent for path in tsconfig_paths}
        for path in tsconfig_paths:
            _add_manifest_lang("typescript", path)
        for path in package_json_paths:
            if path.parent in tsconfig_dirs:
                _add_manifest_lang("typescript", path)
        for path in package_json_paths:
            if path.parent not in tsconfig_dirs:
                _add_manifest_lang("javascript", path)
    else:
        for path in package_json_paths:
            _add_manifest_lang("javascript", path)

    for path in build_gradle_kts_paths:
        _add_manifest_lang("kotlin", path)
    for path in build_gradle_paths:
        if path.parent not in {kts.parent for kts in build_gradle_kts_paths}:
            _add_manifest_lang("java", path)
    for path in pom_paths:
        _add_manifest_lang("java", path)

    # Step 2: extension census -> language percentages.
    files = _walk_files(root)
    bytes_by_lang: dict[str, int] = dict(manifest_bytes_by_lang)
    count_by_lang: dict[str, int] = {}
    first_ext_evidence_by_lang: dict[str, str] = {}

    for fp in files:
        ext = fp.suffix.lower()
        lang = _EXTENSION_TO_LANGUAGE.get(ext)
        if lang is None:
            continue
        try:
            size = fp.stat().st_size
        except OSError:
            continue

        bytes_by_lang[lang] = bytes_by_lang.get(lang, 0) + int(size)
        count_by_lang[lang] = count_by_lang.get(lang, 0) + 1
        if lang not in first_ext_evidence_by_lang:
            first_ext_evidence_by_lang[lang] = _relposix(root, fp)

    if not bytes_by_lang:
        # If neither manifests nor extensions exist, return empty.
        # (Callers should treat empty as "unknown".)
        return []

    # Merge evidence deterministically: manifest evidence first, then one extension example.
    for lang, ext_evidence in first_ext_evidence_by_lang.items():
        if lang not in manifest_evidence_by_lang:
            manifest_evidence_by_lang[lang] = []
        if ext_evidence not in manifest_evidence_by_lang[lang]:
            manifest_evidence_by_lang[lang].append(ext_evidence)

    total = sum(bytes_by_lang.values()) or 1
    langs_sorted = sorted(bytes_by_lang.items(), key=lambda kv: kv[1], reverse=True)
    entries: list[LanguageEntry] = []
    for lang, b in langs_sorted:
        pct = 100.0 * float(b) / float(total)
        entries.append(
            LanguageEntry(
                language=lang,
                percent=pct,
                bytes=int(b),
                files=int(count_by_lang.get(lang, 0)),
                evidence=tuple(manifest_evidence_by_lang.get(lang, [])),
            ),
        )
    return entries


def _detect_conventions(root: Path) -> ConventionSnapshot:
    conventions = ConventionSnapshot()

    has_src = (root / "src").is_dir()
    has_lib = (root / "lib").is_dir()
    has_tests_dir = (root / "tests").is_dir()

    conventions.layout["has_src"] = "yes" if has_src else "no"
    conventions.layout["has_lib"] = "yes" if has_lib else "no"
    conventions.layout["has_tests_dir"] = "yes" if has_tests_dir else "no"

    primary_code_dir = "src" if has_src else ("lib" if has_lib else "root")
    conventions.layout["primary_code_dir"] = primary_code_dir

    code_root = root / primary_code_dir if primary_code_dir in ("src", "lib") else root

    skip_dirs = {
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

    def _iter_candidate_files(
        *,
        scan_root: Path,
        ext_allow: set[str] | None,
        max_files: int,
    ) -> list[Path]:
        """Deterministically select a capped set of candidate text files."""
        out: list[Path] = []
        if not scan_root.is_dir():
            return out

        try:
            for dirpath, dirnames, filenames in os.walk(scan_root):
                dirnames[:] = sorted([d for d in dirnames if d not in skip_dirs])
                for name in sorted(filenames):
                    fp = Path(dirpath) / name
                    if fp.is_symlink() or not fp.is_file():
                        continue
                    ext = fp.suffix.lower()
                    if ext_allow is not None and ext not in ext_allow:
                        continue
                    if not _is_probably_text_file(fp):
                        continue
                    out.append(fp)
                    if len(out) >= max_files:
                        return out
        except OSError:
            return out
        return out

    # 1) Naming + file naming patterns.
    # Inspect file stems under the primary code dir for underscores vs camelCase (and kebab-case).
    stems: list[str] = []
    stems_files = _iter_candidate_files(
        scan_root=code_root,
        ext_allow=set(_EXTENSION_TO_LANGUAGE.keys()),
        max_files=800,
    )
    for fp in stems_files:
        stems.append(fp.stem)

    if stems:
        snake = sum(1 for s in stems if "_" in s)
        camel = sum(1 for s in stems if any(c.isupper() for c in s))
        kebab = sum(1 for s in stems if "-" in s)
        conventions.naming["snake_case_ratio"] = f"{snake}/{len(stems)}"
        conventions.naming["camelcase_ratio"] = f"{camel}/{len(stems)}"
        conventions.naming["kebab_case_ratio"] = f"{kebab}/{len(stems)}"

    tests_dir = root / "tests"

    # 2) Import style (relative vs absolute) + aliasing.
    # Prefer tree-sitter when available; otherwise use best-effort line-based heuristics.
    # For Python:
    # - relative imports: `from .foo import ...`
    # - absolute imports: `from pkg.sub import ...` and `import pkg.sub`
    # For TS/JS:
    # - relative imports: `from "./..."` / `from '../...'`
    # - absolute imports: `from 'pkg/...` (non-dot)
    ts_imports: dict[str, str] = {}
    ts_tests: dict[str, str] = {}
    ts_naming: dict[str, str] = {}
    try:
        from .tree_sitter_conventions import (
            extract_import_and_test_conventions_from_tree_sitter,
            tree_sitter_available,
        )

        if tree_sitter_available():
            ts_imports, ts_tests, ts_naming = extract_import_and_test_conventions_from_tree_sitter(
                code_root=code_root,
                tests_dir=tests_dir,
            )
    except Exception:
        # Defensive: convention extraction must remain best-effort and
        # never fail whole adoption/detection.
        ts_imports, ts_tests, ts_naming = {}, {}, {}

    if ts_imports:
        conventions.imports.update(ts_imports)
    if ts_naming:
        conventions.naming.update(ts_naming)
    else:
        # Best-effort line-based heuristics fallback.
        _read_limited_text_bytes = 100_000
        _max_import_lines_per_file = 400

        def _read_prefix_text(path: Path) -> list[str]:
            try:
                raw = path.read_bytes()[:_read_limited_text_bytes]
            except OSError:
                return []
            text = raw.decode("utf-8", errors="replace")
            return text.splitlines()[:_max_import_lines_per_file]

        rel_imports = 0
        abs_imports = 0
        alias_imports = 0
        total_imports = 0

        python_rx_from_rel = re.compile(r"^\s*from\s+\.+\w*\s+import\s+")
        python_rx_from_abs = re.compile(r"^\s*from\s+[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\s+import\s+")
        python_rx_import_abs = re.compile(r"^\s*import\s+(?!\.)[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*")

        js_rx_from_rel = re.compile(r"""^\s*import\s+.*?\s+from\s+['"]\.\.?(?:/|\\)""")
        js_rx_from_abs = re.compile(r"""^\s*import\s+.*?\s+from\s+['"](?!\.)""")
        js_rx_any_alias = re.compile(r"""\bas\s+""")

        import_files = _iter_candidate_files(
            scan_root=code_root,
            ext_allow={".py", ".js", ".jsx", ".ts", ".tsx"},
            max_files=500,
        )

        for fp in import_files:
            ext = fp.suffix.lower()
            lines = _read_prefix_text(fp)
            if not lines:
                continue
            for ln in lines:
                s = ln.strip()
                if not s:
                    continue
                if ext == ".py":
                    is_import_stmt = s.startswith("from ") or s.startswith("import ")
                    if not is_import_stmt or "import" not in s:
                        continue
                    # Count each qualifying import statement once.
                    total_imports += 1
                    if python_rx_from_rel.match(s):
                        rel_imports += 1
                    elif python_rx_from_abs.match(s) or python_rx_import_abs.match(s):
                        abs_imports += 1
                    # aliasing
                    if " as " in s and "import" in s:
                        alias_imports += 1
                else:
                    # JS/TS line heuristics.
                    if not s.startswith("import "):
                        continue
                    if " from " not in s:
                        continue
                    # Only treat single-line imports; multi-line ones will be missed (best-effort).
                    total_imports += 1
                    if js_rx_from_rel.match(s):
                        rel_imports += 1
                    elif js_rx_from_abs.match(s):
                        abs_imports += 1
                    if js_rx_any_alias.search(s) is not None:
                        alias_imports += 1

        if total_imports > 0:
            conventions.imports["relative_import_ratio"] = f"{rel_imports}/{total_imports}"
            conventions.imports["absolute_import_ratio"] = f"{abs_imports}/{total_imports}"
            conventions.imports["aliasing_ratio"] = f"{alias_imports}/{total_imports}"
            conventions.imports["import_preference"] = "relative" if rel_imports >= abs_imports else "absolute"

    # 3) Directory structure + module nesting (Python-focused best-effort).
    # Determine whether the project tends to be flat or nested via python package depth.
    init_files: list[Path] = []
    for fp in _iter_candidate_files(
        scan_root=code_root,
        ext_allow={".py"},
        max_files=2000,
    ):
        if fp.name == "__init__.py":
            init_files.append(fp)

    if init_files:
        depths: list[int] = []
        for fp in init_files:
            try:
                # Depth of the containing package directory relative to the primary code root.
                d = len(fp.parent.relative_to(code_root).parts)
            except ValueError:
                continue
            depths.append(int(d))
        if depths:
            nested = sum(1 for d in depths if d >= 2)
            total = len(depths)
            ratio = f"{nested}/{total}"
            conventions.layout["python_package_depth_mode"] = "nested" if nested >= max(1, total // 4) else "flat"
            conventions.layout["python_package_depth_ratio"] = ratio
            conventions.layout["python_package_depth_max"] = str(max(depths))

    # 4) Test organization.
    def _is_test_filename(name: str, ext: str) -> bool:
        lower = name.lower()
        if ext == ".py":
            return (lower.startswith("test_") and lower.endswith(".py")) or lower.endswith("_test.py")
        if ext in {".ts", ".tsx", ".js", ".jsx"}:
            return (
                lower.endswith(".test.ts")
                or lower.endswith(".test.tsx")
                or lower.endswith(".spec.ts")
                or lower.endswith(".spec.tsx")
                or lower.endswith(".test.js")
                or lower.endswith(".spec.js")
                or lower.endswith(".test.jsx")
                or lower.endswith(".spec.jsx")
            )
        return False

    # If tree-sitter extracted test organization, prefer it.
    if ts_tests:
        conventions.tests.update(ts_tests)
        return conventions

    separate_tests = 0
    colocated_tests = 0
    total_tests = 0

    # Count test files under the conventional `tests/` directory.
    if tests_dir.is_dir():
        for dirpath, dirnames, filenames in os.walk(tests_dir):
            dirnames[:] = sorted([d for d in dirnames if d not in skip_dirs])
            for name in sorted(filenames):
                fp = Path(dirpath) / name
                if fp.is_symlink() or not fp.is_file():
                    continue
                ext = fp.suffix.lower()
                if not ext:
                    continue
                if not _is_test_filename(name=name, ext=ext):
                    continue
                separate_tests += 1
                total_tests += 1

    # Count colocated test files under primary code root, excluding `tests/`.
    # This captures patterns like `src/**/test_*.py` and `src/**/*.test.ts`.
    for dirpath, dirnames, filenames in os.walk(code_root):
        dirnames[:] = sorted([d for d in dirnames if d not in skip_dirs])
        for name in sorted(filenames):
            fp = Path(dirpath) / name
            if fp.is_symlink() or not fp.is_file():
                continue
            # Exclude test files living under `root/tests`.
            try:
                if tests_dir.is_dir() and fp.is_relative_to(tests_dir):
                    continue
            except Exception:
                # Python<3.9 fallback for is_relative_to; best-effort only.
                try:
                    if tests_dir.is_dir() and str(fp.resolve()).startswith(str(tests_dir.resolve()) + "/"):
                        continue
                except Exception:
                    pass
            ext = fp.suffix.lower()
            if not ext:
                continue
            if not _is_test_filename(name=name, ext=ext):
                continue
            colocated_tests += 1
            total_tests += 1

    if total_tests > 0:
        conventions.tests["tests_separate_count"] = str(separate_tests)
        conventions.tests["tests_colocated_count"] = str(colocated_tests)
        conventions.tests["tests_total_count"] = str(total_tests)
        conventions.tests["tests_colocated_ratio"] = f"{colocated_tests}/{total_tests}"
        conventions.tests["test_org_preference"] = "colocated" if colocated_tests >= separate_tests else "separate"

    return conventions


def _detect_entry_points(root: Path) -> list[str]:
    candidates: list[str] = []
    for name in ("main.py", "app.py", "server.py", "manage.py", "index.ts", "index.js", "cli.py"):
        fp = root / name
        if fp.is_file():
            candidates.append(name)
    src = root / "src"
    if src.is_dir():
        for name in ("__main__.py", "main.py", "index.ts", "index.js", "cli.py"):
            fp = src / name
            if fp.is_file():
                candidates.append(f"src/{name}")
    # De-duplicate while keeping order.
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        out.append(c)
    return out


def _gitignore_simple_skip_dir_names(root: Path) -> frozenset[str]:
    """Best-effort directory tokens from root ``.gitignore`` (no full gitignore grammar)."""

    gi = root / ".gitignore"
    if not gi.is_file():
        return frozenset()
    out: set[str] = set()
    try:
        text = gi.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return frozenset()
    for line in text.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        s = s.split("#", 1)[0].strip()
        if not s or s.startswith("!"):
            continue
        s = s.rstrip("/")
        if not s or any(ch in s for ch in "*?[]"):
            continue
        if "/" in s:
            continue
        out.add(s)
    return frozenset(out)


def _suggested_mutation_path_prefixes(root: Path) -> list[str]:
    """Stable directory prefixes for ``mutation_paths`` (scoped_apply)."""

    out: list[str] = []
    for name in ("apps", "packages", "src", "lib", "services", "workers"):
        p = root / name
        if p.is_dir():
            out.append(f"{name}/")
    return sorted(out)


def _discover_package_json_paths(
    root: Path,
    *,
    respect_gitignore: bool,
    max_files: int = 8,
) -> list[Path]:
    """Return sorted ``package.json`` paths (workspace/monorepo), capped for determinism."""

    try:
        root_resolved = root.expanduser().resolve()
    except OSError:
        root_resolved = root.expanduser()
    found = _discover_repo_files(
        root_resolved,
        names=("package.json",),
        respect_gitignore=respect_gitignore,
        max_files=max_files,
    )
    return sorted(found)


def _extract_github_actions_commands(workflow: Path) -> list[BuildCommand]:
    try:
        lines = workflow.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []

    out: list[BuildCommand] = []
    current_step_name: str | None = None
    idx = 0
    while idx < len(lines):
        raw = lines[idx]
        stripped = raw.strip()
        if stripped.startswith("name:"):
            current_step_name = stripped.split(":", 1)[1].strip().strip("'\"")
        if not stripped.startswith("run:"):
            idx += 1
            continue

        indent = len(raw) - len(raw.lstrip(" "))
        after = stripped.split(":", 1)[1].strip()
        source = f"{workflow}#{current_step_name or 'run'}"
        if after and after not in {"|", ">", "|-", ">-"}:
            out.extend(
                _extract_build_commands_from_command_text(
                    command_text=after,
                    source=source,
                    context_hint=current_step_name,
                )
            )
            idx += 1
            continue

        block: list[str] = []
        idx += 1
        while idx < len(lines):
            block_raw = lines[idx]
            block_stripped = block_raw.strip()
            if not block_stripped:
                idx += 1
                continue
            block_indent = len(block_raw) - len(block_raw.lstrip(" "))
            if block_indent <= indent:
                break
            block.append(block_stripped)
            idx += 1
        if block:
            out.extend(
                _extract_build_commands_from_command_text(
                    command_text="\n".join(block),
                    source=source,
                    context_hint=current_step_name,
                )
            )
    return _dedupe_build_commands(out)


def _extract_gitlab_ci_commands(ci_path: Path) -> list[BuildCommand]:
    try:
        lines = ci_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []

    out: list[BuildCommand] = []
    idx = 0
    while idx < len(lines):
        raw = lines[idx]
        stripped = raw.strip()
        if not stripped.startswith("script:"):
            idx += 1
            continue

        indent = len(raw) - len(raw.lstrip(" "))
        after = stripped.split(":", 1)[1].strip()
        if after:
            out.extend(
                _extract_build_commands_from_command_text(
                    command_text=after,
                    source=f"{ci_path}#script",
                )
            )
            idx += 1
            continue

        idx += 1
        block: list[str] = []
        while idx < len(lines):
            block_raw = lines[idx]
            block_stripped = block_raw.strip()
            if not block_stripped:
                idx += 1
                continue
            block_indent = len(block_raw) - len(block_raw.lstrip(" "))
            if block_indent <= indent:
                break
            block.append(block_stripped)
            idx += 1
        if block:
            out.extend(
                _extract_build_commands_from_command_text(
                    command_text="\n".join(block),
                    source=f"{ci_path}#script",
                )
            )
    return _dedupe_build_commands(out)


def _extract_jenkins_commands(jenkinsfile: Path) -> list[BuildCommand]:
    try:
        lines = jenkinsfile.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return []

    stage_name: str | None = None
    out: list[BuildCommand] = []
    for line in lines:
        stripped = line.strip()
        stage_match = re.search(r"""stage\(\s*['"]([^'"]+)['"]\s*\)""", stripped)
        if stage_match:
            stage_name = stage_match.group(1)
        for pattern in (
            r"""(?:sh|bat)\s+['"]([^'"]+)['"]""",
            r"""(?:sh|bat)\s*\(\s*['"]([^'"]+)['"]\s*\)""",
        ):
            for match in re.finditer(pattern, stripped):
                out.extend(
                    _extract_build_commands_from_command_text(
                        command_text=match.group(1),
                        source=f"{jenkinsfile}#{stage_name or 'stage'}",
                        context_hint=stage_name,
                    )
                )
    return _dedupe_build_commands(out)


def _extract_ci_build_commands(root: Path) -> list[BuildCommand]:
    out: list[BuildCommand] = []
    workflows_dir = root / ".github" / "workflows"
    if workflows_dir.is_dir():
        for workflow in sorted(workflows_dir.glob("*.y*ml")):
            out.extend(_extract_github_actions_commands(workflow))
    gitlab = root / ".gitlab-ci.yml"
    if gitlab.is_file():
        out.extend(_extract_gitlab_ci_commands(gitlab))
    jenkinsfile = root / "Jenkinsfile"
    if jenkinsfile.is_file():
        out.extend(_extract_jenkins_commands(jenkinsfile))
    return _dedupe_build_commands(out)


def _root_package_uses_workspaces(root: Path) -> bool:
    pkg_json = root / "package.json"
    raw = _read_json_object(pkg_json)
    if raw is None:
        return False
    workspaces = raw.get("workspaces")
    if isinstance(workspaces, list):
        return len(workspaces) > 0
    if isinstance(workspaces, dict):
        packages = workspaces.get("packages")
        return isinstance(packages, list) and len(packages) > 0
    return False


def _detect_architecture_hints(root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    workspace_tools: list[str] = []
    if (root / "nx.json").is_file():
        workspace_tools.append("nx")
    if (root / "turbo.json").is_file():
        workspace_tools.append("turbo")
    if (root / "pnpm-workspace.yaml").is_file():
        workspace_tools.append("pnpm_workspaces")
    if _root_package_uses_workspaces(root):
        workspace_tools.append("npm_workspaces")
    if (root / "lerna.json").is_file():
        workspace_tools.append("lerna")

    pkg_json_count = len(_discover_repo_files(root, names=("package.json",), max_files=64))
    out["workspace_tools"] = workspace_tools
    out["monorepo"] = bool(workspace_tools) or pkg_json_count >= 2
    out["package_json_count"] = pkg_json_count
    out["make_based"] = "yes" if (root / "Makefile").is_file() else "no"
    out["suggested_mutation_paths"] = _suggested_mutation_path_prefixes(root)
    return out


def detect_project_profile(*, root: Path, respect_gitignore: bool = False) -> ProjectProfile:
    """Detect a project's stack/conventions into a `ProjectProfile`."""

    root = root.expanduser()
    try:
        root_resolved = root.resolve()
    except OSError:
        root_resolved = root

    languages = _detect_languages(root_resolved)
    package_managers = _detect_package_managers(root_resolved)
    ci_systems = _detect_ci_systems(root_resolved)
    conventions = _detect_conventions(root_resolved)
    entry_points = _detect_entry_points(root_resolved)
    architecture_hints = _detect_architecture_hints(root_resolved)
    architecture_hints["mixed_language"] = len(languages) >= 2

    build_commands: list[BuildCommand] = []

    # Step 4: build/test command extraction (root + additional workspace packages).
    pkg_paths = _discover_package_json_paths(
        root_resolved,
        respect_gitignore=respect_gitignore,
        max_files=8,
    )
    for pkg_json in pkg_paths:
        build_commands.extend(_parse_package_json_scripts(pkg_json))

    makefile = root_resolved / "Makefile"
    if makefile.is_file():
        build_commands.extend(_parse_makefile_test_commands(makefile))

    pyproject = root_resolved / "pyproject.toml"
    if pyproject.is_file():
        build_commands.extend(_parse_pyproject_test_commands(pyproject))
    build_commands.extend(_extract_ci_build_commands(root_resolved))

    # Lightweight python fallback: if python manifests exist but we couldn't extract any test commands,
    # emit a deterministic default.
    if (pyproject.is_file() or (root_resolved / "setup.py").is_file()) and not any(
        c.kind == "test" for c in build_commands
    ):
        build_commands.append(
            BuildCommand(
                command=("python", "-m", "pytest", "-q"),
                kind="test",
                source="heuristic",
            ),
        )

    build_commands = _dedupe_build_commands(build_commands)

    return ProjectProfile(
        root=root_resolved,
        languages=languages,
        package_managers=package_managers,
        build_commands=build_commands,
        ci_systems=ci_systems,
        conventions=conventions,
        entry_points=entry_points,
        architecture_hints=architecture_hints,
    )
