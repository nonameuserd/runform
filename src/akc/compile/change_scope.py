from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Literal

ChangeScopeCategory = Literal["code", "config", "ci", "infra", "dependency", "docs", "other"]


@dataclass(frozen=True, slots=True)
class ChangeScopeSummary:
    categories: tuple[ChangeScopeCategory, ...]
    counts_by_category: dict[ChangeScopeCategory, int]

    def to_json_obj(self) -> dict[str, object]:
        return {
            "categories": list(self.categories),
            "counts_by_category": {str(k): int(v) for k, v in sorted(self.counts_by_category.items())},
        }


def _is_dependency_path(rel: str) -> bool:
    p = rel.lower().strip()
    base = p.rsplit("/", 1)[-1]
    return base in {
        "pyproject.toml",
        "poetry.lock",
        "uv.lock",
        "requirements.txt",
        "requirements-dev.txt",
        "pipfile",
        "pipfile.lock",
        "setup.py",
        "setup.cfg",
        "package.json",
        "package-lock.json",
        "pnpm-lock.yaml",
        "yarn.lock",
        "bun.lockb",
        "cargo.toml",
        "cargo.lock",
        "go.mod",
        "go.sum",
        "composer.json",
        "composer.lock",
        "gemfile",
        "gemfile.lock",
    }


def _is_ci_path(rel: str) -> bool:
    p = rel.lower().strip()
    if p.startswith(".github/workflows/"):
        return True
    if p in {".gitlab-ci.yml", "azure-pipelines.yml", ".circleci/config.yml"}:
        return True
    return p.startswith(".circleci/")


def _is_infra_path(rel: str) -> bool:
    p = rel.lower().strip()
    base = p.rsplit("/", 1)[-1]
    if base in {"dockerfile", "docker-compose.yml", "docker-compose.yaml"}:
        return True
    if p.startswith("infra/") or p.startswith("k8s/") or p.startswith("helm/") or p.startswith("terraform/"):
        return True
    return bool(p.endswith(".tf") or p.endswith(".tfvars"))


def _is_docs_path(rel: str) -> bool:
    p = rel.lower().strip()
    if p.startswith("docs/"):
        return True
    return bool(p.endswith(".md") or p.endswith(".mdx"))


def _is_config_path(rel: str) -> bool:
    p = rel.lower().strip()
    base = p.rsplit("/", 1)[-1]
    if base in {".env", ".env.example", ".env.local"}:
        return True
    if base in {
        ".editorconfig",
        ".prettierrc",
        ".prettierrc.json",
        ".prettierrc.yml",
        ".prettierrc.yaml",
        ".prettierignore",
        ".ruff.toml",
        "ruff.toml",
        ".flake8",
        "mypy.ini",
        "pytest.ini",
        "tox.ini",
        "tsconfig.json",
        "eslint.config.js",
        ".eslintrc",
        ".eslintrc.json",
        ".eslintrc.yml",
        ".eslintrc.yaml",
        ".eslintignore",
    }:
        return True
    return p.startswith(".vscode/")


def _is_code_path(rel: str) -> bool:
    p = rel.lower().strip()
    if p.startswith("src/") or p.startswith("tests/"):
        return True
    return p.endswith((".py", ".ts", ".tsx", ".js", ".jsx", ".rs", ".go", ".java", ".kt"))


def categorize_touched_paths(paths: Iterable[str]) -> ChangeScopeSummary:
    counts: dict[ChangeScopeCategory, int] = {}
    for rel in paths:
        s = str(rel).strip()
        if not s:
            continue
        cat: ChangeScopeCategory
        if _is_dependency_path(s):
            cat = "dependency"
        elif _is_ci_path(s):
            cat = "ci"
        elif _is_infra_path(s):
            cat = "infra"
        elif _is_docs_path(s):
            cat = "docs"
        elif _is_config_path(s):
            cat = "config"
        elif _is_code_path(s):
            cat = "code"
        else:
            cat = "other"
        counts[cat] = int(counts.get(cat, 0)) + 1
    categories = tuple(sorted(counts.keys()))
    return ChangeScopeSummary(categories=categories, counts_by_category=counts)
