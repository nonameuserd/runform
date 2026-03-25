from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DataFileInclude:
    """A single file copied into the compiled distribution."""

    src: Path
    dst_rel: str


@dataclass(frozen=True, slots=True)
class DataDirInclude:
    """A directory copied (recursively) into the compiled distribution."""

    src_dir: Path
    dst_rel_dir: str


def akc_nuitka_data_includes(*, repo_root: Path) -> tuple[tuple[DataFileInclude, ...], tuple[DataDirInclude, ...]]:
    """Explicit non-Python assets that must exist at runtime in a Nuitka build.

    Why explicit?
    - `hatch` ships these via `force-include` for wheels/sdists.
    - Nuitka needs equivalent `--include-data-*` rules or `importlib.resources` /
      `Path(__file__)` lookups will fail in the packaged binary.
    """

    root = repo_root.expanduser().resolve()

    # Keep these in sync with `pyproject.toml` `[tool.hatch.build.targets.wheel.force-include]`.
    files: tuple[DataFileInclude, ...] = (
        DataFileInclude(
            src=root / "src/akc/coordination/static/coordination_sdk.ts",
            dst_rel="akc/coordination/static/coordination_sdk.ts",
        ),
        DataFileInclude(
            src=root / "src/akc/control/operator_dashboard/index.html",
            dst_rel="akc/control/operator_dashboard/index.html",
        ),
        DataFileInclude(
            src=root / "src/akc/control/operator_dashboard/app.js",
            dst_rel="akc/control/operator_dashboard/app.js",
        ),
        DataFileInclude(
            src=root / "src/akc/control/operator_dashboard/README.md",
            dst_rel="akc/control/operator_dashboard/README.md",
        ),
        DataFileInclude(
            src=root / "src/akc/cli/compile_tools_policy_stub.rego",
            dst_rel="akc/cli/compile_tools_policy_stub.rego",
        ),
        DataFileInclude(
            src=root / "src/akc/viewer/static/index.html",
            dst_rel="akc/viewer/static/index.html",
        ),
        DataFileInclude(
            src=root / "src/akc/viewer/static/viewer.css",
            dst_rel="akc/viewer/static/viewer.css",
        ),
        DataFileInclude(
            src=root / "src/akc/viewer/static/viewer.js",
            dst_rel="akc/viewer/static/viewer.js",
        ),
        DataFileInclude(
            src=root / "src/akc/compile/skills/bundled/akc_default/SKILL.md",
            dst_rel="akc/compile/skills/bundled/akc_default/SKILL.md",
        ),
    )

    # Directories used via `Path(__file__).../schemas/...` lookups.
    # These are intentionally directory-level includes to avoid missing new schema
    # additions when the package grows.
    dirs: tuple[DataDirInclude, ...] = (
        DataDirInclude(
            src_dir=root / "src/akc/control/schemas",
            dst_rel_dir="akc/control/schemas",
        ),
        DataDirInclude(
            src_dir=root / "src/akc/artifacts/schemas",
            dst_rel_dir="akc/artifacts/schemas",
        ),
        DataDirInclude(
            src_dir=root / "src/akc/coordination/schemas",
            dst_rel_dir="akc/coordination/schemas",
        ),
    )

    return files, dirs


def verify_akc_nuitka_data_includes(*, repo_root: Path) -> list[str]:
    """Return human-readable errors for missing include sources."""

    file_includes, dir_includes = akc_nuitka_data_includes(repo_root=repo_root)
    errors: list[str] = []
    for item in file_includes:
        if not item.src.is_file():
            errors.append(f"missing file include source: {item.src}")
    for item in dir_includes:
        if not item.src_dir.is_dir():
            errors.append(f"missing dir include source: {item.src_dir}")
    return errors
