from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.adopt.detect import detect_project_profile
from akc.adopt.tree_sitter_conventions import (
    extract_import_and_test_conventions_from_tree_sitter,
    tree_sitter_available,
)
from akc.compile.project_conventions import build_conventions_system_preamble, render_conventions_system_preamble


def test_conventions_naming_snake_vs_camel(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)

    (src / "my_module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (src / "myModule.py").write_text("VALUE = 2\n", encoding="utf-8")

    profile = detect_project_profile(root=tmp_path)
    conventions = profile.conventions

    assert conventions.naming["snake_case_ratio"] == "1/2"
    assert conventions.naming["camelcase_ratio"] == "1/2"


def test_conventions_import_style_relative_absolute_and_aliasing(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir(parents=True, exist_ok=True)

    (src / "a.py").write_text(
        "\n".join(
            [
                "from .b import x",
                "from .c import y as z",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    (src / "d.py").write_text("from pkg.b import x\n", encoding="utf-8")

    profile = detect_project_profile(root=tmp_path)
    conventions = profile.conventions

    assert conventions.imports["relative_import_ratio"] == "2/3"
    assert conventions.imports["absolute_import_ratio"] == "1/3"
    assert conventions.imports["aliasing_ratio"] == "1/3"


def test_conventions_directory_structure_and_tests_org(tmp_path: Path) -> None:
    lib = tmp_path / "lib"
    tests = tmp_path / "tests"
    (lib / "pkg" / "sub").mkdir(parents=True, exist_ok=True)
    tests.mkdir(parents=True, exist_ok=True)

    (lib / "pkg" / "__init__.py").write_text("", encoding="utf-8")
    (lib / "pkg" / "sub" / "__init__.py").write_text("", encoding="utf-8")
    (lib / "pkg" / "test_widget.py").write_text("def test_x():\n    assert True\n", encoding="utf-8")
    (tests / "test_mod.py").write_text("def test_y():\n    assert True\n", encoding="utf-8")

    profile = detect_project_profile(root=tmp_path)
    conventions = profile.conventions

    assert conventions.layout["has_src"] == "no"
    assert conventions.layout["has_lib"] == "yes"
    assert conventions.layout["primary_code_dir"] == "lib"
    assert conventions.layout["python_package_depth_mode"] == "nested"
    assert conventions.layout["python_package_depth_ratio"] == "1/2"

    assert conventions.tests["tests_separate_count"] == "1"
    assert conventions.tests["tests_colocated_count"] == "1"
    assert conventions.tests["test_org_preference"] == "colocated"


def test_conventions_system_preamble_formatting(tmp_path: Path) -> None:
    project_root = tmp_path / "repo"
    (project_root / ".akc").mkdir(parents=True, exist_ok=True)

    conventions = {
        "layout": {
            "primary_code_dir": "src",
            "has_src": "yes",
            "has_lib": "no",
            "has_tests_dir": "yes",
        },
        "naming": {
            "snake_case_ratio": "3/10",
            "camelcase_ratio": "1/10",
        },
        "imports": {
            "import_preference": "relative",
            "relative_import_ratio": "5/8",
            "absolute_import_ratio": "3/8",
            "aliasing_ratio": "2/8",
        },
        "tests": {
            "test_org_preference": "separate",
            "tests_separate_count": "10",
            "tests_colocated_count": "2",
            "tests_colocated_ratio": "2/12",
        },
    }

    (project_root / ".akc" / "project_profile.json").write_text(
        json.dumps({"conventions": conventions}),
        encoding="utf-8",
    )

    sys_preamble = build_conventions_system_preamble(project_root=project_root)
    assert sys_preamble is not None
    assert "Coding conventions snapshot" in sys_preamble
    assert "Directory structure:" in sys_preamble
    assert "- import_preference: relative" in sys_preamble

    # Direct formatter should also be stable.
    sys2 = render_conventions_system_preamble(conventions)
    assert "Test organization:" in sys2


def test_tree_sitter_extractor_smoke_python(tmp_path: Path) -> None:
    if not tree_sitter_available():
        pytest.skip("tree-sitter not installed in this environment")

    code_root = tmp_path / "src"
    tests_dir = tmp_path / "tests"
    code_root.mkdir(parents=True, exist_ok=True)
    tests_dir.mkdir(parents=True, exist_ok=True)

    (code_root / "a.py").write_text("from .b import x\n", encoding="utf-8")
    (code_root / "b.py").write_text("from pkg.c import y as z\n", encoding="utf-8")
    (code_root / "test_widget.py").write_text("def test_x():\n    assert True\n", encoding="utf-8")
    (tests_dir / "test_mod.py").write_text("def test_y():\n    assert True\n", encoding="utf-8")

    imports, tests, naming = extract_import_and_test_conventions_from_tree_sitter(
        code_root=code_root,
        tests_dir=tests_dir,
        max_files_per_language=50,
        max_bytes_per_file=50_000,
    )

    assert "relative_import_ratio" in imports
    assert "absolute_import_ratio" in imports

    rel_num_s, rel_den_s = imports["relative_import_ratio"].split("/")
    abs_num_s, abs_den_s = imports["absolute_import_ratio"].split("/")
    assert int(rel_den_s) == int(abs_den_s)
    assert int(rel_num_s) + int(abs_num_s) == int(rel_den_s)

    assert "tests_total_count" in tests
    assert int(tests["tests_total_count"]) >= 2
    assert int(tests.get("tests_separate_count", "0")) >= 1
    assert int(tests.get("tests_colocated_count", "0")) >= 1

    if naming:
        assert "identifier_snake_case_ratio" in naming or "identifier_camelcase_ratio" in naming
