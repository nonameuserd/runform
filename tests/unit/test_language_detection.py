from __future__ import annotations

from pathlib import Path

from akc.adopt.detect import detect_project_profile


def _langs(profile) -> set[str]:
    return {e.language for e in profile.languages}


def _test_cmds(profile) -> set[tuple[str, tuple[str, ...]]]:
    return {(c.kind, c.command) for c in profile.build_commands}


def test_language_manifest_first_ts_refines_js(tmp_path: Path) -> None:
    (tmp_path / "package.json").write_text('{"name":"x","version":"0.0.0"}', encoding="utf-8")
    (tmp_path / "tsconfig.json").write_text("{}", encoding="utf-8")

    profile = detect_project_profile(root=tmp_path)
    langs = _langs(profile)

    assert "typescript" in langs
    assert "javascript" not in langs


def test_language_manifest_python_without_py_files(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "x"
version = "0.1.0"
""".lstrip(),
        encoding="utf-8",
    )

    profile = detect_project_profile(root=tmp_path)
    assert "python" in _langs(profile)


def test_extension_census_detects_python(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)
    (src / "app.py").write_text("VALUE = 1\n", encoding="utf-8")

    profile = detect_project_profile(root=tmp_path)
    assert "python" in _langs(profile)


def test_makefile_test_runner_extraction(tmp_path: Path) -> None:
    (tmp_path / "Makefile").write_text(
        "test:\n\tpytest -q\n",
        encoding="utf-8",
    )

    profile = detect_project_profile(root=tmp_path)
    assert ("test", ("pytest", "-q")) in _test_cmds(profile)


def test_pyproject_poetry_scripts_extracts_pytest_command(tmp_path: Path) -> None:
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "x"
version = "0.1.0"

[tool.poetry.scripts]
tests = "pytest -q"
""".lstrip(),
        encoding="utf-8",
    )

    profile = detect_project_profile(root=tmp_path)
    assert ("test", ("pytest", "-q")) in _test_cmds(profile)
