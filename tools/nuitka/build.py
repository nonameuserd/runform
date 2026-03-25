from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from tools.nuitka.nuitka_includes import akc_nuitka_data_includes, verify_akc_nuitka_data_includes


def standalone_output_filename(output_name: str) -> str:
    """Path segment(s) for Nuitka ``--output-filename`` (relative to the ``*.dist`` folder).

    Package data is bundled alongside the executable under a top-level ``akc/`` tree.
    On POSIX, an executable named ``akc`` would collide with that directory; use
    ``bin/<name>`` instead. Windows emits ``akc.exe``, which does not collide.
    """

    if sys.platform in {"win32", "cygwin"}:
        return output_name
    normalized = output_name.replace("\\", "/").strip("/")
    if "/" in normalized:
        return output_name
    return f"bin/{normalized}"


def nuitka_base_args(*, output_name: str) -> list[str]:
    """Conservative defaults for shipping `akc` as a standalone terminal executable."""

    out = standalone_output_filename(output_name)
    args: list[str] = [
        sys.executable,
        "-m",
        "nuitka",
        "--standalone",
        "--assume-yes-for-downloads",
        "--output-filename=" + out,
        "--follow-imports",
        "--warn-implicit-exceptions",
        "--warn-unusual-code",
        "--noinclude-setuptools-mode=nofollow",
        # Keep Python environment influence low in a packaged CLI.
        "--python-flag=no_site",
    ]

    # Windows CI runners frequently lack a working MSVC setup for Nuitka. Prefer
    # MinGW64 so builds succeed in a stock GitHub Actions environment.
    if sys.platform in {"win32", "cygwin"}:
        args.append("--mingw64")

    return args


def nuitka_data_args(*, repo_root: Path) -> list[str]:
    file_includes, dir_includes = akc_nuitka_data_includes(repo_root=repo_root)
    args: list[str] = []
    for inc in file_includes:
        args.append(f"--include-data-files={inc.src}={inc.dst_rel}")
    for inc in dir_includes:
        args.append(f"--include-data-dir={inc.src_dir}={inc.dst_rel_dir}")
    return args


def nuitka_akc_args(*, repo_root: Path, output_name: str) -> list[str]:
    """Full Nuitka args list for building the `akc` CLI."""

    base = [*nuitka_base_args(output_name=output_name), "--python-flag=-m"]
    data = nuitka_data_args(repo_root=repo_root)

    # Nuitka 4.x rejects a trailing `-m akc.cli` (parses `-m` as an invalid option). Use the
    # package directory plus `--python-flag=-m` so `__main__.py` is the entry (see Nuitka
    # warning when pointing `--main` at `__main__.py` directly).
    entry = (repo_root / "src/akc/cli").resolve()
    target = [f"--main={entry}"]
    return [*base, *data, *target]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Print deterministic Nuitka args for building `akc`.")
    p.add_argument(
        "--repo-root",
        default=".",
        help="Repository root (default: current working directory).",
    )
    p.add_argument(
        "--output-name",
        default="akc",
        help="Output executable name (default: akc).",
    )
    p.add_argument(
        "--print",
        dest="do_print",
        action="store_true",
        default=True,
        help="Print args to stdout (default: true).",
    )
    args = p.parse_args(argv)

    repo_root = Path(str(args.repo_root)).expanduser().resolve()
    errors = verify_akc_nuitka_data_includes(repo_root=repo_root)
    if errors:
        for e in errors:
            print(f"error: {e}", file=sys.stderr)
        return 2

    cmd = nuitka_akc_args(repo_root=repo_root, output_name=str(args.output_name))
    if bool(args.do_print):
        # Print as a shell-escaped-ish single line that is still readable in CI logs.
        # (Callers can also import `nuitka_akc_args` and run subprocess with the list.)
        print(" ".join(_quote(x) for x in cmd))
    return 0


def _quote(s: str) -> str:
    if not s:
        return "''"
    if any(ch.isspace() for ch in s) or any(ch in s for ch in ("'", '"', "$", "=", ":", ";", "(", ")", "&", "|")):
        return "'" + s.replace("'", "'\"'\"'") + "'"
    return s


if __name__ == "__main__":
    # Avoid leaking env into builds when CI calls this; it only prints args.
    os.environ.pop("PYTHONPATH", None)
    raise SystemExit(main())
