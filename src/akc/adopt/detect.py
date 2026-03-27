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


_TEST_RUNNER_REGEXES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("pytest", re.compile(r"\bpytest\b")),
    ("jest", re.compile(r"\bjest\b")),
    ("cargo test", re.compile(r"\bcargo\s+test\b")),
    ("go test", re.compile(r"\bgo\s+test\b")),
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


def _extract_test_runners_from_command_text(*, command_text: str, source: str) -> list[BuildCommand]:
    # Extractive heuristic: if the command contains a known test runner, keep the full
    # command tokens (not only the runner) so downstream agents can interpret args.
    matched: list[BuildCommand] = []
    if not command_text.strip():
        return matched
    for _runner_label, rx in _TEST_RUNNER_REGEXES:
        if rx.search(command_text):
            tokens = _maybe_parse_command_tokens(command_text)
            if not tokens:
                continue
            matched.append(BuildCommand(command=tokens, kind="test", source=source))
            break
    return matched


def _parse_package_json_scripts(pkg_json: Path) -> list[BuildCommand]:
    try:
        raw = json.loads(pkg_json.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return []
    if not isinstance(raw, dict):
        return []
    scripts = raw.get("scripts")
    if not isinstance(scripts, dict):
        return []

    def _maybe_add(key: str, kind_guess: BuildCommandKind) -> BuildCommand | None:
        v = scripts.get(key)
        if v is None:
            return None
        if not isinstance(v, str):
            return None
        s = v.strip()
        if not s:
            return None
        try:
            command = tuple(shlex.split(s))
        except ValueError:
            # Fallback: keep entire string as one arg.
            command = (s,)
        return BuildCommand(command=command, kind=kind_guess, source=str(pkg_json))

    out: list[BuildCommand] = []

    # Common JS/TS scripts.
    key_map: dict[str, BuildCommandKind] = {
        "build": "build",
        "test": "test",
        "lint": "lint",
        "typecheck": "typecheck",
        "format": "format",
    }
    for key, kind in key_map.items():
        cmd = _maybe_add(key, kind)
        if cmd is not None:
            out.append(cmd)
    # Additional deterministic extraction: scan all `scripts` values for known test runners.
    for _name, value in scripts.items():
        if not isinstance(value, str):
            continue
        out.extend(
            _extract_test_runners_from_command_text(
                command_text=value,
                source=str(pkg_json),
            ),
        )
    return _dedupe_build_commands(out)


def _parse_makefile_test_commands(makefile: Path) -> list[BuildCommand]:
    try:
        text = makefile.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return []

    out: list[BuildCommand] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        # Makefile recipes typically indent commands with tabs/spaces; scanning the full line
        # is deterministic and avoids needing a full Make grammar.
        if any(rx.search(stripped) for _label, rx in _TEST_RUNNER_REGEXES):
            out.extend(
                _extract_test_runners_from_command_text(
                    command_text=stripped,
                    source=str(makefile),
                ),
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
                        tokens = _maybe_parse_command_tokens(f"pytest {addopts}")
                        if tokens:
                            out.append(BuildCommand(command=tokens, kind="test", source=str(pyproject)))
                    else:
                        out.append(
                            BuildCommand(
                                command=("pytest",),
                                kind="test",
                                source=str(pyproject),
                            ),
                        )

        # Poetry scripts and PEP-621 project.scripts: deterministic extraction for `pytest`-like entries.
        for scripts_path in (("tool", "poetry", "scripts"), ("project", "scripts")):
            cursor: Any = data
            for part in scripts_path:
                if not isinstance(cursor, dict):
                    cursor = None
                    break
                cursor = cursor.get(part)
            if not isinstance(cursor, dict):
                continue
            for _name, value in cursor.items():
                if not isinstance(value, str):
                    continue
                out.extend(
                    _extract_test_runners_from_command_text(
                        command_text=value,
                        source=str(pyproject),
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


def _detect_package_managers(root: Path) -> list[str]:
    out: list[str] = []
    if (root / "package.json").is_file():
        out.append("npm_or_node")
        if (root / "pnpm-lock.yaml").is_file():
            out.append("pnpm")
        if (root / "yarn.lock").is_file():
            out.append("yarn")
        if (root / "package-lock.json").is_file():
            out.append("npm")
    if (root / "pyproject.toml").is_file() or (root / "setup.py").is_file():
        out.append("pip_or_py")
        if (root / "uv.lock").is_file():
            out.append("uv")
    if (root / "Cargo.toml").is_file():
        out.append("cargo")
    if (root / "go.mod").is_file():
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

    has_pyproject = (root / "pyproject.toml").is_file()
    has_setup = (root / "setup.py").is_file()
    has_cargo = (root / "Cargo.toml").is_file()
    has_go = (root / "go.mod").is_file()
    has_pkg_json = (root / "package.json").is_file()
    has_tsconfig = (root / "tsconfig.json").is_file()
    has_pom = (root / "pom.xml").is_file()
    has_build_gradle = (root / "build.gradle").is_file()
    has_build_gradle_kts = (root / "build.gradle.kts").is_file()

    if has_pyproject or has_setup:
        if has_pyproject:
            _add_manifest_lang("python", root / "pyproject.toml")
        if has_setup:
            _add_manifest_lang("python", root / "setup.py")
    if has_cargo:
        _add_manifest_lang("rust", root / "Cargo.toml")
    if has_go:
        _add_manifest_lang("go", root / "go.mod")

    # Node/JS/TS refinement: `tsconfig.json` refines JS classification.
    if has_tsconfig:
        _add_manifest_lang("typescript", root / "tsconfig.json")
        if has_pkg_json:
            _add_manifest_lang("typescript", root / "package.json")
    elif has_pkg_json:
        _add_manifest_lang("javascript", root / "package.json")

    # Java/Kotlin: `build.gradle.kts` is a Kotlin-specific manifest signal.
    if has_build_gradle_kts:
        _add_manifest_lang("kotlin", root / "build.gradle.kts")
    else:
        if has_build_gradle:
            _add_manifest_lang("java", root / "build.gradle")
    if has_pom:
        _add_manifest_lang("java", root / "pom.xml")

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


def _detect_architecture_hints(root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {}
    out["monorepo"] = any((root / f).is_file() for f in ("lerna.json", "nx.json", "pnpm-workspace.yaml", "turbo.json"))
    # Heuristic: multiple package.json implies a workspace/monorepo.
    pkg_json_count = 0
    for fp in root.glob("**/package.json"):
        if ".git" in fp.parts or "node_modules" in fp.parts or ".akc" in fp.parts:
            continue
        pkg_json_count += 1
        if pkg_json_count >= 2:
            break
    out["package_json_count"] = pkg_json_count
    out["make_based"] = "yes" if (root / "Makefile").is_file() else "no"
    return out


def detect_project_profile(*, root: Path) -> ProjectProfile:
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

    build_commands: list[BuildCommand] = []

    # Step 4: build/test command extraction.
    pkg_json = root_resolved / "package.json"
    if pkg_json.is_file():
        build_commands.extend(_parse_package_json_scripts(pkg_json))

    makefile = root_resolved / "Makefile"
    if makefile.is_file():
        build_commands.extend(_parse_makefile_test_commands(makefile))

    pyproject = root_resolved / "pyproject.toml"
    if pyproject.is_file():
        build_commands.extend(_parse_pyproject_test_commands(pyproject))

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
