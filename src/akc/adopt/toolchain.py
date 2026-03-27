from __future__ import annotations

import shlex
import shutil
import subprocess
from collections.abc import Mapping
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

from .profile import ProjectProfile


@dataclass(frozen=True, slots=True)
class ToolchainProfile:
    language: str
    # npm, cargo, pip/uv (best-effort; can be None)
    package_manager: str | None
    # Example: ["pytest", "-x"]
    test_command: list[str]
    # Example: ["tsc", "--noEmit"]
    typecheck_command: list[str] | None
    # Example: ["cargo", "build"]
    build_command: list[str] | None
    # Example: ["ruff", "check", "."]
    lint_command: list[str] | None
    # Example: ["prettier", "--check", "."]
    format_command: list[str] | None
    install_command: list[str] | None
    # Example: ["python3", "node", "cargo"]
    required_binaries: list[str]


@dataclass(frozen=True, slots=True)
class _ToolchainPatch:
    language: str | None = None
    package_manager: str | None = None
    test_command: list[str] | None = None
    typecheck_command: list[str] | None = None
    build_command: list[str] | None = None
    lint_command: list[str] | None = None
    format_command: list[str] | None = None
    install_command: list[str] | None = None
    required_binaries: list[str] | None = None


@dataclass(frozen=True, slots=True)
class PreflightResult:
    """Result of a toolchain environment preflight.

    Fail-closed: ``ok`` is False when any required binary is missing or does not
    report a version string.
    """

    ok: bool
    missing: tuple[str, ...] = ()
    versions: dict[str, str] | None = None
    version_errors: tuple[str, ...] = ()


class ToolchainPreflightError(RuntimeError):
    def __init__(self, message: str, *, result: PreflightResult) -> None:
        super().__init__(message)
        self.result = result


def _normalize_language(language: str) -> str:
    raw = str(language).strip().lower()
    if raw in {"py", "python3", "python"}:
        return "python"
    if raw in {"ts", "typescript"}:
        return "typescript"
    if raw in {"js", "javascript"}:
        return "javascript"
    if raw in {"rs", "rust"}:
        return "rust"
    if raw in {"go", "golang"}:
        return "go"
    if raw in {"node"}:
        return "javascript"
    return raw or "python"


def _coerce_command(value: object) -> list[str]:
    """Coerce an explicit command into a list of args.

    Supports either:
    - JSON arrays: ["pytest", "-x"]
    - Single strings: "pytest -x"
    """
    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            s = str(item).strip()
            if s:
                out.append(s)
        return out
    if isinstance(value, str):
        return [x for x in shlex.split(value.strip()) if x]
    raise ValueError(f"command must be a list[str] or a string, not {type(value).__name__}")


def _maybe_coerce_command(value: object | None) -> list[str] | None:
    if value is None:
        return None
    v = str(value).strip() if not isinstance(value, list) else value
    if isinstance(v, str) and v == "":
        return None
    if isinstance(value, list) and len(value) == 0:
        return None
    return _coerce_command(value)


def _coerce_optional_patch(explicit_toolchain: Any) -> _ToolchainPatch | None:
    """Coerce explicit config input into a patch.

    Accepts either:
    - A `ToolchainProfile` (full values)
    - A mapping (JSON object / CLI-derived dict) with optional keys
    """
    if explicit_toolchain is None:
        return None
    if isinstance(explicit_toolchain, ToolchainProfile):
        return _ToolchainPatch(
            language=explicit_toolchain.language,
            package_manager=explicit_toolchain.package_manager,
            test_command=list(explicit_toolchain.test_command),
            typecheck_command=(
                list(explicit_toolchain.typecheck_command) if explicit_toolchain.typecheck_command else None
            ),
            build_command=list(explicit_toolchain.build_command) if explicit_toolchain.build_command else None,
            lint_command=list(explicit_toolchain.lint_command) if explicit_toolchain.lint_command else None,
            format_command=list(explicit_toolchain.format_command) if explicit_toolchain.format_command else None,
            install_command=list(explicit_toolchain.install_command) if explicit_toolchain.install_command else None,
            required_binaries=list(explicit_toolchain.required_binaries),
        )
    if isinstance(explicit_toolchain, dict):
        d = explicit_toolchain
    elif isinstance(explicit_toolchain, Mapping):
        d = dict(explicit_toolchain)
    else:
        raise ValueError("explicit_toolchain must be a ToolchainProfile or a mapping")

    lang = d.get("language")
    pm = d.get("package_manager")
    return _ToolchainPatch(
        language=_normalize_language(lang) if isinstance(lang, str) and lang.strip() else None,
        package_manager=str(pm).strip() if pm is not None and str(pm).strip() else None,
        test_command=_maybe_coerce_command(d.get("test_command")),
        typecheck_command=_maybe_coerce_command(d.get("typecheck_command")),
        build_command=_maybe_coerce_command(d.get("build_command")),
        lint_command=_maybe_coerce_command(d.get("lint_command")),
        format_command=_maybe_coerce_command(d.get("format_command")),
        install_command=_maybe_coerce_command(d.get("install_command")),
        required_binaries=_maybe_coerce_command(d.get("required_binaries")) if "required_binaries" in d else None,
    )


def _dominant_language(profile: ProjectProfile | None) -> str | None:
    if profile is None or not profile.languages:
        return None
    # `detect_project_profile()` emits languages in descending evidence/bytes order.
    return _normalize_language(profile.languages[0].language)


def _package_manager_from_evidence(*, language: str, package_managers: list[str]) -> str | None:
    pm = set(package_managers)

    if language in {"python"}:
        if "uv" in pm:
            return "uv"
        if "pip_or_py" in pm:
            return "pip"
        return None

    if language in {"javascript", "typescript"}:
        if "pnpm" in pm:
            return "pnpm"
        if "yarn" in pm:
            return "yarn"
        if "npm" in pm:
            return "npm"
        if "npm_or_node" in pm:
            return "npm"
        return None

    if language in {"rust"}:
        if "cargo" in pm:
            return "cargo"
        return None

    if language in {"go"}:
        if "go" in pm:
            return "go"
        return None

    return None


def _first_command_by_kind(profile: ProjectProfile, *, kind: str) -> list[str] | None:
    for bc in profile.build_commands:
        if bc.kind == kind:
            return list(bc.command)
    return None


def _install_command_for(*, root: Path, language: str, package_manager: str | None) -> list[str] | None:
    if package_manager is None:
        return None

    if language == "python":
        if package_manager == "uv":
            # `uv sync` typically follows `uv.lock` + `pyproject.toml`.
            return ["uv", "sync"]
        if package_manager == "pip":
            req = root / "requirements.txt"
            if req.is_file():
                return ["pip", "install", "-r", "requirements.txt"]
            # Fallback for projects using `pyproject.toml` / editable deps.
            if (root / "pyproject.toml").is_file() or (root / "setup.py").is_file():
                return ["pip", "install", "."]
            return None

    if language in {"javascript", "typescript"}:
        if package_manager == "npm":
            if (root / "package-lock.json").is_file():
                return ["npm", "ci"]
            return ["npm", "install"]
        if package_manager == "pnpm":
            if (root / "pnpm-lock.yaml").is_file():
                return ["pnpm", "install", "--frozen-lockfile"]
            return ["pnpm", "install"]
        if package_manager == "yarn":
            if (root / "yarn.lock").is_file():
                return ["yarn", "install", "--frozen-lockfile"]
            return ["yarn", "install"]

    if language == "rust" and package_manager == "cargo":
        return ["cargo", "fetch"]

    if language == "go" and package_manager == "go":
        return None

    return None


def _required_binaries_from_profile(profile: ToolchainProfile) -> list[str]:
    # Preflight gate uses this list to check executables with `shutil.which`.
    # It is intentionally best-effort: we include the language runtime + the
    # command entrypoints we plan to shell out to.
    bins: set[str] = set()

    lang = _normalize_language(profile.language)
    if lang == "python":
        bins.add("python3")
    elif lang in {"javascript", "typescript"}:
        bins.add("node")
    elif lang == "rust":
        bins.add("cargo")
    elif lang == "go":
        bins.add("go")

    if profile.package_manager:
        bins.add(profile.package_manager)

    for cmd in (
        profile.test_command,
        profile.build_command,
        profile.lint_command,
        profile.format_command,
        profile.install_command,
    ):
        if cmd:
            exe = str(cmd[0]).strip()
            if exe:
                bins.add(exe)

    return sorted(bins)


def _probe_binary_version(*, binary: str, timeout_s: float) -> str | None:
    """Best-effort version probe for a CLI binary.

    Returns a single-line version string when available, else None.
    """

    candidates: tuple[tuple[str, ...], ...] = (
        (binary, "--version"),
        (binary, "-V"),
        (binary, "-v"),
        (binary, "version"),
    )
    for argv in candidates:
        try:
            proc = subprocess.run(
                list(argv),
                check=False,
                capture_output=True,
                text=True,
                timeout=float(timeout_s),
            )
        except (OSError, subprocess.TimeoutExpired):
            continue
        out = (proc.stdout or proc.stderr or "").strip()
        if proc.returncode == 0 and out:
            return out.splitlines()[0].strip()
        # Some CLIs print versions to stderr with non-zero return codes; accept
        # if we got any output at all.
        if out:
            return out.splitlines()[0].strip()
    return None


def preflight_toolchain(profile: ToolchainProfile, *, timeout_s: float = 2.0) -> PreflightResult:
    """Check every required binary exists and reports version."""

    missing: list[str] = []
    versions: dict[str, str] = {}
    version_errors: list[str] = []

    for binary in profile.required_binaries:
        b = str(binary).strip()
        if not b:
            continue
        if not shutil.which(b):
            missing.append(b)
            continue
        v = _probe_binary_version(binary=b, timeout_s=float(timeout_s))
        if v is None:
            version_errors.append(b)
            continue
        versions[b] = v

    if missing or version_errors:
        return PreflightResult(
            ok=False,
            missing=tuple(sorted(set(missing))),
            versions=versions if versions else None,
            version_errors=tuple(sorted(set(version_errors))),
        )
    return PreflightResult(ok=True, versions=versions)


def _conventional_defaults(language: str, *, root: Path | None = None) -> ToolchainProfile:
    lang = _normalize_language(language)
    if lang == "python":
        pm = "pip"
        test = ["pytest", "-x"]
        lint: list[str] | None = ["ruff", "check", "."]
        fmt: list[str] | None = ["ruff", "format", "--check", "."]
        install: list[str] | None = None
        if root is not None:
            install = _install_command_for(root=root, language=lang, package_manager=pm)
        return ToolchainProfile(
            language=lang,
            package_manager=pm,
            test_command=test,
            typecheck_command=None,
            build_command=None,
            lint_command=lint,
            format_command=fmt,
            install_command=install,
            required_binaries=[],
        )

    if lang in {"javascript", "typescript"}:
        pm = "npm"
        test = ["jest"]
        lint = ["eslint", "."]
        fmt = ["prettier", "--check", "."]
        install = _install_command_for(root=root or Path("."), language=lang, package_manager=pm)
        return ToolchainProfile(
            language=lang,
            package_manager=pm,
            test_command=test,
            typecheck_command=(["tsc", "--noEmit"] if lang == "typescript" else None),
            build_command=None,
            lint_command=lint,
            format_command=fmt,
            install_command=install,
            required_binaries=[],
        )

    if lang == "rust":
        return ToolchainProfile(
            language="rust",
            package_manager="cargo",
            test_command=["cargo", "test"],
            typecheck_command=["cargo", "check"],
            build_command=["cargo", "build"],
            lint_command=None,
            format_command=None,
            install_command=["cargo", "fetch"] if root is None or root.exists() else ["cargo", "fetch"],
            required_binaries=[],
        )

    if lang == "go":
        return ToolchainProfile(
            language="go",
            package_manager="go",
            test_command=["go", "test", "./..."],
            typecheck_command=None,
            build_command=None,
            lint_command=None,
            format_command=None,
            install_command=None,
            required_binaries=[],
        )

    # Defensive fallback.
    return ToolchainProfile(
        language=lang,
        package_manager=None,
        test_command=["pytest", "-x"],
        typecheck_command=None,
        build_command=None,
        lint_command=None,
        format_command=None,
        install_command=None,
        required_binaries=[],
    )


def _with_changes(base: ToolchainProfile, **changes: Any) -> ToolchainProfile:
    """Slots-safe helper: return a new ToolchainProfile with overrides."""
    return replace(base, **changes)


def resolve_toolchain_profile(
    *,
    extracted_profile: ProjectProfile | None,
    explicit_toolchain: ToolchainProfile | Mapping[str, Any] | None = None,
) -> ToolchainProfile:
    """Resolve a concrete `ToolchainProfile`.

    Precedence (best-effort overlay):
    1. Explicit (from `.akc/project.json` `toolchain` key or CLI-derived dict)
    2. Extracted (from `ProjectProfile` manifest analysis)
    3. Conventional (language-specific defaults)
    """
    explicit_patch = _coerce_optional_patch(explicit_toolchain)
    extracted_lang = _dominant_language(extracted_profile)
    resolved_lang = (
        explicit_patch.language
        if explicit_patch is not None and explicit_patch.language is not None
        else (extracted_lang if extracted_lang is not None else "python")
    )

    root = extracted_profile.root if extracted_profile is not None else None
    base = _conventional_defaults(resolved_lang, root=root)

    # Extracted overlay: fill fields when extraction provides them.
    if extracted_profile is not None:
        pm = _package_manager_from_evidence(
            language=_normalize_language(resolved_lang),
            package_managers=extracted_profile.package_managers,
        )
        if pm is not None:
            base = _with_changes(base, package_manager=pm)

        test = _first_command_by_kind(extracted_profile, kind="test")
        if test:
            base = _with_changes(base, test_command=test)

        typecheck = _first_command_by_kind(extracted_profile, kind="typecheck")
        if typecheck:
            base = _with_changes(base, typecheck_command=typecheck)

        build = _first_command_by_kind(extracted_profile, kind="build")
        if build:
            base = _with_changes(base, build_command=build)

        lint = _first_command_by_kind(extracted_profile, kind="lint")
        if lint:
            base = _with_changes(base, lint_command=lint)

        fmt = _first_command_by_kind(extracted_profile, kind="format")
        if fmt:
            base = _with_changes(base, format_command=fmt)

        install = _install_command_for(
            root=extracted_profile.root,
            language=_normalize_language(resolved_lang),
            package_manager=base.package_manager,
        )
        if install is not None:
            base = _with_changes(base, install_command=install)

    # Explicit overlay (per-field).
    if explicit_patch is not None:
        # Apply only explicit fields that were present; `None` values mean "not set".
        changes: dict[str, Any] = {}
        if explicit_patch.language is not None:
            changes["language"] = explicit_patch.language
        if explicit_patch.package_manager is not None:
            changes["package_manager"] = explicit_patch.package_manager
        if explicit_patch.test_command is not None:
            if len(explicit_patch.test_command) == 0:
                raise ValueError("explicit_toolchain.test_command must be non-empty")
            changes["test_command"] = explicit_patch.test_command
        if explicit_patch.typecheck_command is not None:
            changes["typecheck_command"] = explicit_patch.typecheck_command
        if explicit_patch.build_command is not None:
            changes["build_command"] = explicit_patch.build_command
        if explicit_patch.lint_command is not None:
            changes["lint_command"] = explicit_patch.lint_command
        if explicit_patch.format_command is not None:
            changes["format_command"] = explicit_patch.format_command
        if explicit_patch.install_command is not None:
            changes["install_command"] = explicit_patch.install_command
        if explicit_patch.required_binaries is not None:
            if len(explicit_patch.required_binaries) == 0:
                raise ValueError("explicit_toolchain.required_binaries must be non-empty when set")
            changes["required_binaries"] = explicit_patch.required_binaries

        if changes:
            base = _with_changes(base, **changes)

    # Always recompute required_binaries unless explicit set it.
    if explicit_patch is not None and explicit_patch.required_binaries is not None:
        return base
    recomputed = _required_binaries_from_profile(base)
    return _with_changes(base, required_binaries=recomputed)
