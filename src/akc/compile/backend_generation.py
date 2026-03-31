from __future__ import annotations

import json
import os
import re
import tomllib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Final, cast

from akc.adopt.detect import detect_project_profile
from akc.adopt.profile import BuildCommand, ProjectProfile
from akc.adopt.toolchain import resolve_toolchain_profile
from akc.ir import IRDocument, IRNode
from akc.memory.models import JSONValue
from akc.utils.fingerprint import stable_json_fingerprint

if TYPE_CHECKING:
    from akc.intent.models import IntentSpecV1


_SKIP_DIRS: frozenset[str] = frozenset(
    {
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
)

_ANCHOR_RULES: tuple[tuple[str, re.Pattern[str], float], ...] = (
    ("route", re.compile(r"(route|router|endpoint|controller|handler|api)", re.I), 0.92),
    ("service", re.compile(r"(service|usecase|workflow|job|worker)", re.I), 0.84),
    ("repository", re.compile(r"(repo|repository|dao|store|query)", re.I), 0.88),
    ("model", re.compile(r"(model|schema|entity|dto|serializer)", re.I), 0.76),
    ("migration", re.compile(r"(migration|migrate|alembic|prisma|drizzle|sql)", re.I), 0.86),
    ("test", re.compile(r"(test|spec|smoke|integration)", re.I), 0.8),
    ("config", re.compile(r"(config|settings|env|telemetry|observ)", re.I), 0.72),
)

_FRAMEWORK_MARKERS: tuple[tuple[str, str, str], ...] = (
    ("fastapi", "python", "fastapi"),
    ("flask", "python", "flask"),
    ("django", "python", "django"),
    ("express", "typescript", "express"),
    ("fastify", "typescript", "fastify"),
    ("nestjs", "typescript", "nest"),
    ("@nestjs", "typescript", "nest"),
    ("koa", "typescript", "koa"),
    ("gin", "go", "gin"),
    ("echo", "go", "echo"),
    ("fiber", "go", "fiber"),
    ("axum", "rust", "axum"),
    ("actix-web", "rust", "actix_web"),
    ("spring-boot", "java", "spring_boot"),
    ("spring-web", "java", "spring_boot"),
)

_PERSISTENCE_MARKERS: tuple[tuple[str, str], ...] = (
    ("sqlalchemy", "orm"),
    ("alembic", "migration"),
    ("django.db", "orm"),
    ("psycopg", "sql_driver"),
    ("asyncpg", "sql_driver"),
    ("prisma", "orm"),
    ("drizzle", "orm"),
    ("typeorm", "orm"),
    ("sequelize", "orm"),
    ("knex", "query_builder"),
    ("gorm", "orm"),
    ("sqlx", "sql_driver"),
    ("diesel", "orm"),
    ("hibernate", "orm"),
    ("jooq", "query_builder"),
    ("flyway", "migration"),
    ("liquibase", "migration"),
)

_OBSERVABILITY_MARKERS: tuple[tuple[str, str], ...] = (
    ("opentelemetry", "tracing"),
    ("otel", "tracing"),
    ("prometheus", "metrics"),
    ("statsd", "metrics"),
    ("structlog", "logging"),
    ("pino", "logging"),
    ("winston", "logging"),
    ("loguru", "logging"),
    ("slog", "logging"),
    ("micrometer", "metrics"),
)

_TRANSPORT_MARKERS: tuple[tuple[str, str], ...] = (
    ("openapi", "rest"),
    ("swagger", "rest"),
    ("grpc", "rpc"),
    ("queue", "queue"),
    ("worker", "queue"),
    ("cron", "scheduler"),
    ("celery", "job_runner"),
    ("rq", "job_runner"),
    ("bullmq", "job_runner"),
)


@dataclass(frozen=True, slots=True)
class PracticalBackendHandoff:
    backend_generation_profile: dict[str, Any]
    backend_ir: dict[str, Any]
    backend_api_contract_index: dict[str, Any]
    api_contracts: dict[str, dict[str, Any]]
    implementation_plan: dict[str, Any]
    implementation_acceptance_contract: dict[str, Any]
    practical_generation_result: dict[str, Any]
    runtime_plugin_decision: dict[str, Any]

    @property
    def adoption_readiness(self) -> str:
        raw = self.backend_generation_profile.get("adoption_readiness")
        return str(raw).strip() if isinstance(raw, str) and str(raw).strip() else "blocked"

    @property
    def practical_status(self) -> str:
        raw = self.practical_generation_result.get("status")
        return str(raw).strip() if isinstance(raw, str) and str(raw).strip() else self.adoption_readiness

    @property
    def execution_workspace_role(self) -> str:
        raw = self.practical_generation_result.get("execution_workspace_role")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        return "fallback_debug_reference"

    @property
    def authoritative_workspace_ready(self) -> bool:
        return (
            self.execution_workspace_role == "authoritative_generated_workspace"
            and self.practical_status in {"ready", "succeeded"}
            and not self.blocked_reasons
        )

    @property
    def blocked_reasons(self) -> list[str]:
        raw = self.practical_generation_result.get("blocked_reasons")
        if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
            return []
        return [str(item).strip() for item in raw if str(item).strip()]


def practical_backend_handoff_from_context(ctx: Mapping[str, Any]) -> PracticalBackendHandoff:
    return PracticalBackendHandoff(
        backend_generation_profile=dict(cast(dict[str, Any], ctx["backend_generation_profile"])),
        backend_ir=dict(cast(dict[str, Any], ctx["backend_ir"])),
        backend_api_contract_index=dict(cast(dict[str, Any], ctx.get("backend_api_contract_index") or {})),
        api_contracts={
            str(key): dict(cast(dict[str, Any], value))
            for key, value in cast(dict[str, Any], ctx.get("api_contracts") or {}).items()
            if isinstance(key, str) and isinstance(value, Mapping)
        },
        implementation_plan=dict(cast(dict[str, Any], ctx["implementation_plan"])),
        implementation_acceptance_contract=dict(cast(dict[str, Any], ctx["implementation_acceptance_contract"])),
        practical_generation_result=dict(cast(dict[str, Any], ctx["practical_generation_result"])),
        runtime_plugin_decision=dict(cast(dict[str, Any], ctx["runtime_plugin_decision"])),
    )


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _read_toml(path: Path) -> dict[str, Any] | None:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _walk_project_files(root: Path, *, max_files: int = 400) -> list[Path]:
    out: list[Path] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for name in sorted(filenames):
            path = Path(dirpath) / name
            if path.is_symlink():
                continue
            out.append(path)
            if len(out) >= max_files:
                return out
    return out


def _project_text_markers(project_root: Path | None) -> list[tuple[str, str]]:
    if project_root is None:
        return []
    markers: list[tuple[str, str]] = []
    pkg = _read_json(project_root / "package.json")
    if pkg is not None:
        deps = {}
        for section in ("dependencies", "devDependencies", "peerDependencies"):
            raw = pkg.get(section)
            if isinstance(raw, Mapping):
                deps.update({str(k): str(v) for k, v in raw.items()})
        for key in sorted(deps):
            markers.append((key.lower(), "package.json"))
        scripts = pkg.get("scripts")
        if isinstance(scripts, Mapping):
            for key, value in scripts.items():
                if isinstance(value, str):
                    markers.append((f"{key}:{value}".lower(), "package.json"))
    pyproject = _read_toml(project_root / "pyproject.toml")
    if pyproject is not None:
        project = pyproject.get("project")
        if isinstance(project, Mapping):
            project_deps = project.get("dependencies")
            if isinstance(project_deps, Sequence) and not isinstance(project_deps, (str, bytes)):
                for dep in project_deps:
                    markers.append((str(dep).lower(), "pyproject.toml"))
        tool = pyproject.get("tool")
        if isinstance(tool, Mapping):
            for key, value in tool.items():
                markers.append((str(key).lower(), "pyproject.toml"))
                if isinstance(value, Mapping):
                    for child_key in value:
                        markers.append((f"{key}.{child_key}".lower(), "pyproject.toml"))
    for rel in ("go.mod", "Cargo.toml", "pom.xml", "build.gradle", "build.gradle.kts"):
        path = project_root / rel
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace").lower()
        except OSError:
            continue
        markers.extend((line.strip(), rel) for line in text.splitlines()[:250] if line.strip())
    return markers


def _slug(value: str) -> str:
    out = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value))
    while "--" in out:
        out = out.replace("--", "-")
    return out.strip("-") or "backend"


def _build_command_to_json(cmd: BuildCommand) -> dict[str, JSONValue]:
    return {
        "kind": cmd.kind,
        "command": cast(JSONValue, [str(x) for x in cmd.command]),
        "source": cmd.source,
    }


def _normalize_language_name(language: str) -> str:
    raw = str(language).strip().lower()
    if raw in {"py", "python3", "python"}:
        return "python"
    if raw in {"ts", "typescript"}:
        return "typescript"
    if raw in {"js", "javascript", "node"}:
        return "javascript"
    if raw in {"rs", "rust"}:
        return "rust"
    if raw in {"go", "golang"}:
        return "go"
    if raw in {"java", "jdk"}:
        return "java"
    return raw


def _policy_flag(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _detected_language_names(project_profile: ProjectProfile | None) -> tuple[str, ...]:
    if project_profile is None:
        return ()
    out: list[str] = []
    for row in project_profile.languages:
        normalized = _normalize_language_name(row.language)
        if normalized and normalized not in out:
            out.append(normalized)
    return tuple(out)


def _effective_native_command_rows(project_profile: ProjectProfile | None) -> list[dict[str, JSONValue]]:
    rows: list[dict[str, JSONValue]] = []
    seen: set[tuple[str, tuple[str, ...]]] = set()
    if project_profile is not None:
        for cmd in project_profile.build_commands:
            command = tuple(str(part).strip() for part in cmd.command if str(part).strip())
            if not command:
                continue
            key = (str(cmd.kind).strip(), command)
            if key in seen:
                continue
            seen.add(key)
            rows.append(_build_command_to_json(cmd))
        if project_profile.languages or project_profile.package_managers or project_profile.build_commands:
            try:
                resolved = resolve_toolchain_profile(extracted_profile=project_profile, explicit_toolchain=None)
            except Exception:
                resolved = None
            if resolved is not None:
                for kind, command in (
                    ("test", resolved.test_command),
                    ("typecheck", resolved.typecheck_command),
                    ("build", resolved.build_command),
                    ("lint", resolved.lint_command),
                    ("format", resolved.format_command),
                ):
                    if not command:
                        continue
                    normalized_command = tuple(str(part).strip() for part in command if str(part).strip())
                    if not normalized_command:
                        continue
                    key = (kind, normalized_command)
                    if key in seen:
                        continue
                    seen.add(key)
                    rows.append(
                        {
                            "kind": kind,
                            "command": cast(JSONValue, list(normalized_command)),
                            "source": "resolved_toolchain",
                        }
                    )
    return rows


def _required_native_command_kinds(
    *,
    plugin: BackendRuntimePlugin,
    detected_languages: Sequence[str],
) -> tuple[str, ...]:
    required = [str(kind).strip() for kind in plugin.required_native_command_kinds if str(kind).strip()]
    language_set = {str(language).strip() for language in detected_languages if str(language).strip()}
    if plugin.plugin_id == "typescript_node" and "typescript" not in language_set and "javascript" in language_set:
        required = [kind for kind in required if kind != "typecheck"]
    return tuple(dict.fromkeys(required))


def load_backend_generator_policy(*, project_root: Path | None) -> dict[str, Any]:
    default = {
        "runtime_preferences": [],
        "allowed_target_runtimes": [],
        "disallowed_target_runtimes": [],
        "allow_runtime_language_override": False,
        "eligible_repo_paths": [],
        "ignored_repo_paths": [],
        "plugin_manifest_paths": [],
        "plugin_manifest_dirs": [".akc/backend_generators"],
        "minimum_adoption_confidence": 0.45,
        "fallback_mode": "blocked",
    }
    if project_root is None:
        return default
    for rel in (".akc/backend_generator_policy.json", ".akc/generator_policy.json"):
        loaded = _read_json(project_root / rel)
        if isinstance(loaded, dict):
            merged = dict(default)
            merged.update(loaded)
            return merged
    return default


@dataclass(frozen=True, slots=True)
class BackendRuntimePlugin:
    plugin_id: str
    runtime_family: str
    maturity: str
    supported_frameworks: tuple[str, ...]
    supported_persistence_modes: tuple[str, ...]
    supported_auth_modes: tuple[str, ...]
    supported_job_patterns: tuple[str, ...]
    supported_observability_libraries: tuple[str, ...]
    required_native_command_kinds: tuple[str, ...]
    supported_languages: tuple[str, ...] = ()
    unsupported_behavior: str = "blocked"

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "plugin_id": self.plugin_id,
            "runtime_family": self.runtime_family,
            "maturity": self.maturity,
            "supported_frameworks": cast(JSONValue, list(self.supported_frameworks)),
            "supported_persistence_modes": cast(JSONValue, list(self.supported_persistence_modes)),
            "supported_auth_modes": cast(JSONValue, list(self.supported_auth_modes)),
            "supported_job_patterns": cast(JSONValue, list(self.supported_job_patterns)),
            "supported_observability_libraries": cast(JSONValue, list(self.supported_observability_libraries)),
            "required_native_command_kinds": cast(JSONValue, list(self.required_native_command_kinds)),
            "supported_languages": cast(JSONValue, list(self.supported_languages)),
            "unsupported_behavior": self.unsupported_behavior,
        }


@dataclass(frozen=True, slots=True)
class BackendGeneratorPluginManifest:
    plugin_id: str
    interface_version: int
    runtime_family: str
    source: str
    materializer_kind: str
    supports_authoritative_workspace: bool
    supported_frameworks: tuple[str, ...]
    supported_languages: tuple[str, ...]
    required_native_command_kinds: tuple[str, ...]
    unsupported_behavior: str
    command: tuple[str, ...]
    maturity: str = "external"
    supported_persistence_modes: tuple[str, ...] = ()
    supported_auth_modes: tuple[str, ...] = ()
    supported_job_patterns: tuple[str, ...] = ()
    supported_observability_libraries: tuple[str, ...] = ()
    notes: str | None = None
    manifest_path: str | None = None

    def to_runtime_plugin(self) -> BackendRuntimePlugin:
        return BackendRuntimePlugin(
            plugin_id=self.plugin_id,
            runtime_family=self.runtime_family,
            maturity=self.maturity,
            supported_frameworks=self.supported_frameworks,
            supported_persistence_modes=self.supported_persistence_modes,
            supported_auth_modes=self.supported_auth_modes,
            supported_job_patterns=self.supported_job_patterns,
            supported_observability_libraries=self.supported_observability_libraries,
            required_native_command_kinds=self.required_native_command_kinds,
            supported_languages=self.supported_languages,
            unsupported_behavior=self.unsupported_behavior,
        )


@dataclass(frozen=True, slots=True)
class BackendMaterializerResolution:
    plugin: BackendRuntimePlugin
    plugin_source: str
    materializer_kind: str
    supports_authoritative_workspace: bool
    availability: str
    requested_by_policy: bool
    blocked_stage: str | None
    command: tuple[str, ...] | None = None
    manifest_path: str | None = None
    diagnostics: tuple[str, ...] = ()

    @property
    def available(self) -> bool:
        return self.availability in {"builtin", "external_manifest"}

    def to_json_obj(self) -> dict[str, JSONValue]:
        return {
            "plugin_source": self.plugin_source,
            "materializer_kind": self.materializer_kind,
            "supports_authoritative_workspace": self.supports_authoritative_workspace,
            "availability": self.availability,
            "requested_by_policy": self.requested_by_policy,
            "blocked_stage": self.blocked_stage,
            "command": cast(JSONValue, list(self.command)) if self.command is not None else None,
            "manifest_path": self.manifest_path,
            "diagnostics": cast(JSONValue, list(self.diagnostics)),
        }


BUILTIN_AUTHORITATIVE_RUNTIME_PLUGINS: Final[frozenset[str]] = frozenset(
    {"typescript_node", "python_fastapi", "go", "rust", "java"}
)


_PLUGIN_REGISTRY: tuple[BackendRuntimePlugin, ...] = (
    BackendRuntimePlugin(
        plugin_id="typescript_node",
        runtime_family="typescript",
        maturity="ga",
        supported_frameworks=("express", "fastify", "nest", "koa"),
        supported_persistence_modes=("repo_native", "sql_first"),
        supported_auth_modes=("conditional",),
        supported_job_patterns=("worker", "queue", "cron"),
        supported_observability_libraries=("opentelemetry", "pino", "winston", "prometheus"),
        required_native_command_kinds=("test", "lint", "typecheck"),
        supported_languages=("typescript", "javascript"),
    ),
    BackendRuntimePlugin(
        plugin_id="python_fastapi",
        runtime_family="python",
        maturity="ga",
        supported_frameworks=("fastapi", "flask", "django"),
        supported_persistence_modes=("repo_native", "sql_first"),
        supported_auth_modes=("conditional",),
        supported_job_patterns=("worker", "queue", "cron"),
        supported_observability_libraries=("opentelemetry", "structlog", "prometheus"),
        required_native_command_kinds=("test",),
        supported_languages=("python",),
    ),
    BackendRuntimePlugin(
        plugin_id="go",
        runtime_family="go",
        maturity="beta",
        supported_frameworks=("gin", "echo", "fiber"),
        supported_persistence_modes=("repo_native",),
        supported_auth_modes=("conditional",),
        supported_job_patterns=("worker", "queue"),
        supported_observability_libraries=("opentelemetry", "prometheus", "slog"),
        required_native_command_kinds=("test", "build"),
        supported_languages=("go",),
    ),
    BackendRuntimePlugin(
        plugin_id="rust",
        runtime_family="rust",
        maturity="beta",
        supported_frameworks=("axum", "actix_web"),
        supported_persistence_modes=("repo_native",),
        supported_auth_modes=("conditional",),
        supported_job_patterns=("worker", "queue"),
        supported_observability_libraries=("opentelemetry", "prometheus"),
        required_native_command_kinds=("test", "build"),
        supported_languages=("rust",),
    ),
    BackendRuntimePlugin(
        plugin_id="java",
        runtime_family="java",
        maturity="beta",
        supported_frameworks=("spring_boot",),
        supported_persistence_modes=("repo_native",),
        supported_auth_modes=("conditional",),
        supported_job_patterns=("worker", "queue"),
        supported_observability_libraries=("opentelemetry", "micrometer"),
        required_native_command_kinds=("test", "build"),
        supported_languages=("java",),
    ),
)


def _non_empty_str_seq(raw: Sequence[Any] | None) -> tuple[str, ...]:
    out: list[str] = []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return ()
    for item in raw:
        text = str(item).strip()
        if text:
            out.append(text)
    return tuple(out)


def _candidate_manifest_paths(*, project_root: Path | None, policy: Mapping[str, Any]) -> list[Path]:
    if project_root is None:
        return []
    root = project_root.expanduser().resolve()
    out: list[Path] = []
    seen: set[Path] = set()
    raw_paths = _non_empty_str_seq(cast(Sequence[Any], policy.get("plugin_manifest_paths") or []))
    raw_dirs = _non_empty_str_seq(cast(Sequence[Any], policy.get("plugin_manifest_dirs") or []))
    for rel in raw_paths:
        candidate = Path(rel)
        path = candidate if candidate.is_absolute() else root / candidate
        resolved = path.expanduser().resolve()
        if resolved in seen or not resolved.is_file():
            continue
        seen.add(resolved)
        out.append(resolved)
    for rel in raw_dirs:
        candidate = Path(rel)
        dir_path = candidate if candidate.is_absolute() else root / candidate
        resolved_dir = dir_path.expanduser().resolve()
        if not resolved_dir.is_dir():
            continue
        for path in sorted(resolved_dir.glob("*.json")):
            resolved = path.expanduser().resolve()
            if resolved in seen or not resolved.is_file():
                continue
            seen.add(resolved)
            out.append(resolved)
    return out


def _parse_backend_generator_plugin_manifest(path: Path) -> tuple[BackendGeneratorPluginManifest | None, str | None]:
    raw = _read_json(path)
    if raw is None:
        return None, f"plugin_manifest_invalid:{path.name}:not_a_json_object"

    def _required_str(name: str) -> tuple[str | None, str | None]:
        value = raw.get(name)
        if isinstance(value, str) and value.strip():
            return value.strip(), None
        return None, f"plugin_manifest_invalid:{path.name}:missing_{name}"

    plugin_id, err = _required_str("plugin_id")
    if err is not None:
        return None, err
    runtime_family, err = _required_str("runtime_family")
    if err is not None:
        return None, err
    source, err = _required_str("source")
    if err is not None:
        return None, err
    unsupported_behavior, err = _required_str("unsupported_behavior")
    if err is not None:
        return None, err
    interface_version = raw.get("interface_version")
    if not isinstance(interface_version, int) or interface_version != 1:
        return None, f"plugin_manifest_invalid:{path.name}:unsupported_interface_version"
    materializer_kind = str(raw.get("materializer_kind") or "").strip()
    if materializer_kind != "command":
        return None, f"plugin_manifest_invalid:{path.name}:unsupported_materializer_kind"
    supports_authoritative_workspace = raw.get("supports_authoritative_workspace")
    if not isinstance(supports_authoritative_workspace, bool):
        return None, f"plugin_manifest_invalid:{path.name}:missing_supports_authoritative_workspace"
    command = _non_empty_str_seq(cast(Sequence[Any], raw.get("command") or []))
    if not command:
        return None, f"plugin_manifest_invalid:{path.name}:missing_command"
    supported_frameworks = _non_empty_str_seq(cast(Sequence[Any], raw.get("supported_frameworks") or []))
    if not supported_frameworks:
        return None, f"plugin_manifest_invalid:{path.name}:missing_supported_frameworks"
    supported_languages = _non_empty_str_seq(cast(Sequence[Any], raw.get("supported_languages") or []))
    if not supported_languages:
        return None, f"plugin_manifest_invalid:{path.name}:missing_supported_languages"
    required_native_command_kinds = _non_empty_str_seq(
        cast(Sequence[Any], raw.get("required_native_command_kinds") or [])
    )
    if not required_native_command_kinds:
        return None, f"plugin_manifest_invalid:{path.name}:missing_required_native_command_kinds"
    notes = raw.get("notes")
    notes_s = str(notes).strip() if isinstance(notes, str) and notes.strip() else None
    return (
        BackendGeneratorPluginManifest(
            plugin_id=cast(str, plugin_id),
            interface_version=interface_version,
            runtime_family=cast(str, runtime_family),
            source=cast(str, source),
            materializer_kind=materializer_kind,
            supports_authoritative_workspace=supports_authoritative_workspace,
            supported_frameworks=supported_frameworks,
            supported_languages=supported_languages,
            required_native_command_kinds=required_native_command_kinds,
            unsupported_behavior=cast(str, unsupported_behavior),
            command=command,
            maturity=str(raw.get("maturity") or "external").strip() or "external",
            supported_persistence_modes=_non_empty_str_seq(
                cast(Sequence[Any], raw.get("supported_persistence_modes") or [])
            ),
            supported_auth_modes=_non_empty_str_seq(cast(Sequence[Any], raw.get("supported_auth_modes") or [])),
            supported_job_patterns=_non_empty_str_seq(cast(Sequence[Any], raw.get("supported_job_patterns") or [])),
            supported_observability_libraries=_non_empty_str_seq(
                cast(Sequence[Any], raw.get("supported_observability_libraries") or [])
            ),
            notes=notes_s,
            manifest_path=str(path),
        ),
        None,
    )


def load_backend_generator_plugin_manifests(
    *,
    project_root: Path | None,
    policy: Mapping[str, Any],
) -> tuple[dict[str, BackendGeneratorPluginManifest], tuple[str, ...]]:
    manifests: dict[str, BackendGeneratorPluginManifest] = {}
    diagnostics: list[str] = []
    for path in _candidate_manifest_paths(project_root=project_root, policy=policy):
        parsed, err = _parse_backend_generator_plugin_manifest(path)
        if err is not None:
            diagnostics.append(err)
            continue
        assert parsed is not None
        manifests[parsed.plugin_id] = parsed
    return manifests, tuple(sorted(set(diagnostics)))


def get_backend_runtime_plugin(plugin_id: str) -> BackendRuntimePlugin | None:
    lookup = str(plugin_id).strip()
    if not lookup:
        return None
    return next((plugin for plugin in _PLUGIN_REGISTRY if plugin.plugin_id == lookup), None)


def resolve_backend_runtime_materializer(
    *,
    selected_plugin: BackendRuntimePlugin,
    project_root: Path | None,
    policy: Mapping[str, Any],
    requested_by_policy: bool,
) -> BackendMaterializerResolution:
    manifests, diagnostics = load_backend_generator_plugin_manifests(project_root=project_root, policy=policy)
    manifest = manifests.get(selected_plugin.plugin_id)
    if manifest is not None:
        return BackendMaterializerResolution(
            plugin=manifest.to_runtime_plugin(),
            plugin_source=manifest.source,
            materializer_kind=manifest.materializer_kind,
            supports_authoritative_workspace=manifest.supports_authoritative_workspace,
            availability="external_manifest",
            requested_by_policy=requested_by_policy,
            blocked_stage=None if manifest.supports_authoritative_workspace else "materializer_resolution",
            command=manifest.command,
            manifest_path=manifest.manifest_path,
            diagnostics=diagnostics,
        )
    if selected_plugin.plugin_id in BUILTIN_AUTHORITATIVE_RUNTIME_PLUGINS:
        return BackendMaterializerResolution(
            plugin=selected_plugin,
            plugin_source="builtin",
            materializer_kind="builtin",
            supports_authoritative_workspace=True,
            availability="builtin",
            requested_by_policy=requested_by_policy,
            blocked_stage=None,
            diagnostics=diagnostics,
        )
    diag = list(diagnostics)
    diag.append(f"runtime_materializer_unavailable:{selected_plugin.plugin_id}")
    return BackendMaterializerResolution(
        plugin=selected_plugin,
        plugin_source="builtin_registry",
        materializer_kind="command",
        supports_authoritative_workspace=False,
        availability="missing",
        requested_by_policy=requested_by_policy,
        blocked_stage="materializer_resolution",
        diagnostics=tuple(sorted(set(diag))),
    )


def _framework_rows(*, markers: Sequence[tuple[str, str]]) -> list[dict[str, JSONValue]]:
    out: dict[str, dict[str, JSONValue]] = {}
    for text, source in markers:
        for needle, runtime_family, normalized in _FRAMEWORK_MARKERS:
            if needle not in text:
                continue
            row = out.setdefault(
                normalized,
                {
                    "name": normalized,
                    "runtime_family": runtime_family,
                    "evidence": cast(JSONValue, []),
                },
            )
            cast(list[JSONValue], row["evidence"]).append(f"{source}:{needle}")
    return [out[key] for key in sorted(out)]


def _marker_rows(
    *, markers: Sequence[tuple[str, str]], source_markers: Sequence[tuple[str, str]]
) -> list[dict[str, JSONValue]]:
    out: dict[str, dict[str, JSONValue]] = {}
    for text, source in markers:
        for needle, kind in source_markers:
            if needle not in text:
                continue
            row = out.setdefault(
                needle,
                {
                    "name": needle,
                    "kind": kind,
                    "evidence": cast(JSONValue, []),
                },
            )
            cast(list[JSONValue], row["evidence"]).append(f"{source}:{needle}")
    return [out[key] for key in sorted(out)]


def _repo_anchor_candidates(*, project_root: Path | None, policy: Mapping[str, Any]) -> list[dict[str, JSONValue]]:
    if project_root is None:
        return []
    ignored = {str(x).strip() for x in cast(Sequence[Any], policy.get("ignored_repo_paths") or []) if str(x).strip()}
    eligible = [str(x).strip() for x in cast(Sequence[Any], policy.get("eligible_repo_paths") or []) if str(x).strip()]
    out: list[dict[str, JSONValue]] = []
    for path in _walk_project_files(project_root):
        try:
            rel = path.relative_to(project_root).as_posix()
        except ValueError:
            continue
        if any(rel.startswith(prefix) for prefix in ignored):
            continue
        if eligible and not any(rel.startswith(prefix) for prefix in eligible):
            continue
        stem = path.stem
        for kind, rx, confidence in _ANCHOR_RULES:
            if not rx.search(rel) and not rx.search(stem):
                continue
            out.append(
                {
                    "path": rel,
                    "kind": kind,
                    "confidence": round(confidence, 2),
                }
            )
            break
        if len(out) >= 32:
            break
    return out


def _test_topology(*, project_root: Path | None, project_profile: ProjectProfile | None) -> dict[str, JSONValue]:
    smoke_files: list[str] = []
    integration_files: list[str] = []
    api_test_files: list[str] = []
    if project_root is not None:
        for path in _walk_project_files(project_root, max_files=250):
            try:
                rel = path.relative_to(project_root).as_posix()
            except ValueError:
                continue
            lrel = rel.lower()
            if "smoke" in lrel:
                smoke_files.append(rel)
            if "integration" in lrel:
                integration_files.append(rel)
            if "test" in lrel and any(token in lrel for token in ("api", "http", "endpoint", "route")):
                api_test_files.append(rel)
    native_commands = []
    if project_profile is not None:
        native_commands = [_build_command_to_json(cmd) for cmd in project_profile.build_commands]
    return {
        "native_commands": cast(JSONValue, native_commands),
        "smoke_test_files": cast(JSONValue, sorted(smoke_files)[:16]),
        "integration_test_files": cast(JSONValue, sorted(integration_files)[:16]),
        "api_test_files": cast(JSONValue, sorted(api_test_files)[:16]),
    }


def _select_runtime_plugin(
    *,
    project_profile: ProjectProfile | None,
    frameworks: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
) -> tuple[BackendRuntimePlugin, bool]:
    allowed = {
        str(x).strip() for x in cast(Sequence[Any], policy.get("allowed_target_runtimes") or []) if str(x).strip()
    }
    disallowed = {
        str(x).strip() for x in cast(Sequence[Any], policy.get("disallowed_target_runtimes") or []) if str(x).strip()
    }
    preferred = [str(x).strip() for x in cast(Sequence[Any], policy.get("runtime_preferences") or []) if str(x).strip()]
    registry = [plugin for plugin in _PLUGIN_REGISTRY if plugin.plugin_id not in disallowed]
    if allowed:
        registry = [plugin for plugin in registry if plugin.plugin_id in allowed]
    by_id = {plugin.plugin_id: plugin for plugin in registry}
    for plugin_id in preferred:
        if plugin_id in by_id:
            return by_id[plugin_id], True
    framework_names = {str(row.get("name", "")).strip() for row in frameworks}
    for plugin in registry:
        if framework_names.intersection(plugin.supported_frameworks):
            return plugin, False
    language_order = []
    if project_profile is not None:
        language_order = [row.language for row in project_profile.languages]
    for lang in language_order:
        lang_l = str(lang).strip().lower()
        if lang_l in {"typescript", "javascript"} and "typescript_node" in by_id:
            return by_id["typescript_node"], False
        if lang_l == "python" and "python_fastapi" in by_id:
            return by_id["python_fastapi"], False
        if lang_l == "go" and "go" in by_id:
            return by_id["go"], False
        if lang_l == "rust" and "rust" in by_id:
            return by_id["rust"], False
        if lang_l == "java" and "java" in by_id:
            return by_id["java"], False
    return (
        (registry[0], bool(allowed and registry and registry[0].plugin_id in allowed))
        if registry
        else (
            _PLUGIN_REGISTRY[0],
            False,
        )
    )


def _persistence_strategy(
    *,
    plugin: BackendRuntimePlugin,
    persistence_markers: Sequence[Mapping[str, Any]],
) -> tuple[str, list[str]]:
    reasons: list[str] = []
    if persistence_markers:
        reasons.append("existing persistence layer detected")
        return "repo_native", reasons
    if plugin.maturity == "ga" and "sql_first" in plugin.supported_persistence_modes:
        reasons.append("no repo-native persistence detected; constrained SQL-first fallback allowed")
        return "sql_first", reasons
    reasons.append("no persistence markers detected; stateless service baseline selected")
    return "stateless", reasons


def _confidence_score(
    *,
    project_profile: ProjectProfile | None,
    frameworks: Sequence[Mapping[str, Any]],
    anchors: Sequence[Mapping[str, Any]],
    plugin: BackendRuntimePlugin,
    persistence_strategy: str,
) -> float:
    score = 0.2
    if project_profile is not None and project_profile.build_commands:
        score += 0.15
    if frameworks:
        score += 0.2
    if anchors:
        score += 0.25
    if plugin.maturity == "ga":
        score += 0.15
    else:
        score += 0.05
    if persistence_strategy == "repo_native":
        score += 0.15
    elif persistence_strategy == "sql_first":
        score += 0.05
    elif persistence_strategy == "stateless":
        score += 0.1
    return min(round(score, 2), 0.98)


def _goal_statement(intent_spec: IntentSpecV1 | None) -> str | None:
    if intent_spec is None:
        return None
    if intent_spec.goal_statement is not None and str(intent_spec.goal_statement).strip():
        return str(intent_spec.goal_statement).strip()
    if intent_spec.objectives:
        return str(intent_spec.objectives[0].statement).strip()
    return None


def _source_kind_from_provenance(node: IRNode) -> list[dict[str, JSONValue]]:
    rows: dict[str, dict[str, JSONValue]] = {}
    for pointer in node.provenance:
        kind = str(pointer.kind).strip().lower()
        source_kind = (
            "openapi"
            if kind == "openapi_operation"
            else "messaging"
            if kind == "message"
            else "docs"
            if kind == "doc_chunk"
            else "codebase"
            if kind == "file"
            else "other"
        )
        row = rows.setdefault(
            source_kind,
            {
                "source_kind": source_kind,
                "confidence": 0.75 if source_kind in {"openapi", "codebase"} else 0.6,
                "evidence": cast(JSONValue, []),
            },
        )
        locator = str(pointer.locator or "").strip()
        evidence = f"{pointer.source_id}:{locator}" if locator else pointer.source_id
        cast(list[JSONValue], row["evidence"]).append(evidence)
    return [rows[key] for key in sorted(rows)]


def _contract_sources_for_node(
    *,
    node: IRNode,
    project_root: Path | None,
    transport_markers: Sequence[Mapping[str, Any]],
    anchors: Sequence[Mapping[str, Any]],
) -> list[dict[str, JSONValue]]:
    rows = _source_kind_from_provenance(node)
    seen = {str(row.get("source_kind", "")).strip() for row in rows}
    if project_root is not None and "codebase" not in seen:
        rows.append(
            {
                "source_kind": "codebase",
                "confidence": 0.82 if anchors else 0.64,
                "evidence": cast(
                    JSONValue,
                    [str(row.get("path", "")).strip() for row in anchors[:6] if str(row.get("path", "")).strip()],
                ),
            }
        )
        seen.add("codebase")
    transport_names = {str(row.get("name", "")).strip().lower() for row in transport_markers}
    if "openapi" in transport_names and "openapi" not in seen:
        rows.append({"source_kind": "openapi", "confidence": 0.7, "evidence": cast(JSONValue, ["project markers"])})
        seen.add("openapi")
    if "docs" not in seen:
        rows.append(
            {"source_kind": "docs", "confidence": 0.45, "evidence": cast(JSONValue, ["inferred contract narrative"])}
        )
    return sorted(rows, key=lambda item: str(item.get("source_kind", "")))


def _schema_component_name(*, target_name: str, suffix: str) -> str:
    base = "".join(ch for ch in str(target_name).title() if ch.isalnum()) or "Backend"
    end = "".join(ch for ch in str(suffix).title() if ch.isalnum()) or "Payload"
    return f"{base}{end}"


def _schema_from_hint(*, hint: Any, fallback_name: str) -> tuple[dict[str, Any], dict[str, Any]]:
    if isinstance(hint, Mapping):
        return dict(cast(dict[str, Any], hint)), {}
    if isinstance(hint, str) and hint.strip():
        name = _schema_component_name(target_name=hint, suffix=fallback_name)
        return {"$ref": f"#/components/schemas/{name}"}, {
            name: {
                "type": "object",
                "title": hint.strip(),
                "additionalProperties": True,
                "properties": {
                    "kind": {"type": "string", "default": hint.strip()},
                },
            }
        }
    return (
        {"type": "object", "additionalProperties": True, "properties": {"status": {"type": "string"}}},
        {},
    )


def _path_parameters(*, path: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for segment in re.findall(r"\{([^{}]+)\}", str(path)):
        name = str(segment).strip()
        if not name:
            continue
        out.append(
            {
                "name": name,
                "in": "path",
                "required": True,
                "schema": {"type": "string"},
            }
        )
    return out


def _normalize_parameter_list(raw_params: Any, *, location: str | None = None) -> list[dict[str, Any]]:
    if not isinstance(raw_params, Sequence) or isinstance(raw_params, (str, bytes)):
        return []
    out: list[dict[str, Any]] = []
    for item in raw_params:
        if not isinstance(item, Mapping):
            continue
        row = dict(cast(dict[str, Any], item))
        row_location = str(row.get("in") or "").strip()
        if location is not None and row_location != location:
            continue
        if location is not None and not row_location:
            row["in"] = location
        if str(row.get("name") or "").strip():
            out.append(row)
    return out


def _extract_parameter_groups(
    raw_params: Any, *, path: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    combined = _normalize_parameter_list(raw_params)
    path_params = [row for row in combined if str(row.get("in") or "").strip() == "path"] or _path_parameters(path=path)
    query_params = [row for row in combined if str(row.get("in") or "").strip() == "query"]
    header_params = [row for row in combined if str(row.get("in") or "").strip() == "header"]
    return path_params, query_params, header_params


def _normalize_request_body(raw_body: Any) -> dict[str, Any] | None:
    if not isinstance(raw_body, Mapping):
        return None
    if "content" in raw_body:
        content = raw_body.get("content")
        if isinstance(content, Mapping):
            for content_type, payload in content.items():
                if isinstance(content_type, str) and isinstance(payload, Mapping):
                    return {
                        "required": bool(raw_body.get("required", False)),
                        "content_type": content_type.strip() or "application/json",
                        "schema": dict(cast(dict[str, Any], payload.get("schema") or {"type": "object"})),
                    }
    content_type = str(raw_body.get("content_type", "application/json")).strip() or "application/json"
    return {
        "required": bool(raw_body.get("required", False)),
        "content_type": content_type,
        "schema": dict(cast(dict[str, Any], raw_body.get("schema") or {"type": "object"})),
    }


def _normalize_responses(raw_responses: Any) -> dict[str, Any]:
    if not isinstance(raw_responses, Mapping):
        return {}
    out: dict[str, Any] = {}
    for status, payload in raw_responses.items():
        if not isinstance(payload, Mapping):
            continue
        if "content" in payload and isinstance(payload.get("content"), Mapping):
            content = cast(Mapping[str, Any], payload.get("content"))
            selected_type = next((str(key).strip() for key in content if str(key).strip()), "application/json")
            selected_payload = content.get(selected_type)
            out[str(status)] = {
                "description": str(payload.get("description", "response")).strip() or "response",
                "content_type": selected_type,
                "schema": (
                    dict(cast(dict[str, Any], selected_payload.get("schema") or {"type": "object"}))
                    if isinstance(selected_payload, Mapping)
                    else {"type": "object"}
                ),
            }
            continue
        out[str(status)] = {
            "description": str(payload.get("description", "response")).strip() or "response",
            "content_type": str(payload.get("content_type", "application/json")).strip() or "application/json",
            "schema": dict(cast(dict[str, Any], payload.get("schema") or {"type": "object"})),
        }
    return out


def _feature_group_from_operation(*, path: str, tags: Sequence[Any]) -> str:
    for raw_tag in tags:
        tag = _slug(str(raw_tag))
        if tag and tag != "health":
            return tag
    for segment in str(path).split("/"):
        normalized = _slug(segment)
        if normalized and normalized not in {"api", "v1", "v2", "v3"} and not normalized.startswith("{"):
            return normalized
    return "default"


def _is_health_operation(operation: Mapping[str, Any]) -> bool:
    operation_id = _slug(str(operation.get("operation_id") or operation.get("operationId") or ""))
    path = str(operation.get("path") or "").strip().lower()
    tags = {_slug(str(tag)) for tag in cast(Sequence[Any], operation.get("tags") or []) if str(tag).strip()}
    return (
        operation_id.endswith(("healthz", "ready", "readiness"))
        or path in {"/healthz", "/ready", "/readiness"}
        or "health" in path
        or "health" in tags
    )


def _auth_mode_for_scheme(scheme: Mapping[str, Any]) -> str:
    scheme_type = str(scheme.get("type") or "").strip().lower()
    if scheme_type == "apiKey".lower():
        return "session" if str(scheme.get("in") or "").strip().lower() == "cookie" else "apiKey"
    if scheme_type == "http":
        http_scheme = str(scheme.get("scheme") or "").strip().lower()
        if http_scheme == "bearer":
            return "bearer"
        if http_scheme == "basic":
            return "basic"
    if scheme_type in {"oauth2", "openidconnect"}:
        return "oauth2"
    return "custom"


def _auth_modes_for_security(
    *,
    security: Any,
    security_schemes: Mapping[str, Any],
) -> list[str]:
    if not isinstance(security, Sequence) or isinstance(security, (str, bytes)):
        return []
    modes: set[str] = set()
    for requirement in security:
        if not isinstance(requirement, Mapping):
            continue
        for scheme_name in requirement:
            scheme = security_schemes.get(str(scheme_name))
            if isinstance(scheme, Mapping):
                modes.add(_auth_mode_for_scheme(scheme))
    return sorted(modes)


def _summarize_api_surface(surface: Mapping[str, Any]) -> dict[str, Any]:
    operations = [
        dict(cast(dict[str, Any], item))
        for item in cast(Sequence[Any], surface.get("operations") or [])
        if isinstance(item, Mapping)
    ]
    security_schemes = (
        dict(cast(dict[str, Any], surface.get("security_schemes") or {}))
        if isinstance(surface.get("security_schemes"), Mapping)
        else {}
    )
    global_security = list(cast(Sequence[Any], surface.get("security") or []))
    health_path = next(
        (str(op.get("path") or "/healthz") for op in operations if _is_health_operation(op)),
        "/healthz",
    )
    feature_groups = sorted(
        {
            _feature_group_from_operation(
                path=str(op.get("path") or "/"), tags=cast(Sequence[Any], op.get("tags") or [])
            )
            for op in operations
            if not _is_health_operation(op)
        }
    )
    auth_modes = sorted(
        {
            mode
            for op in operations
            for mode in (
                _auth_modes_for_security(
                    security=op.get("security") if op.get("security") is not None else global_security,
                    security_schemes=security_schemes,
                )
                or []
            )
        }
    )
    summarized = dict(cast(dict[str, Any], surface))
    summarized["operations"] = operations
    summarized["feature_groups"] = feature_groups
    summarized["auth_modes"] = auth_modes
    summarized["operation_count"] = len(operations)
    summarized["non_health_operation_count"] = sum(1 for op in operations if not _is_health_operation(op))
    summarized["health_path"] = health_path
    return summarized


def _api_surface_from_openapi_contract(
    *,
    node: IRNode,
    target_class: str,
    contract_obj: Mapping[str, Any],
    contract_sources: Sequence[Mapping[str, Any]],
    health_path: str,
) -> dict[str, Any] | None:
    if target_class not in {"backend_service", "integration"}:
        return None
    paths = contract_obj.get("paths")
    if not isinstance(paths, Mapping):
        return None
    operations: list[dict[str, Any]] = []
    component_schemas = (
        dict(cast(dict[str, Any], cast(Mapping[str, Any], contract_obj.get("components") or {}).get("schemas") or {}))
        if isinstance(contract_obj.get("components"), Mapping)
        else {}
    )
    for path, raw_methods in sorted(paths.items(), key=lambda item: str(item[0])):
        if not isinstance(path, str) or not isinstance(raw_methods, Mapping):
            continue
        for method, raw_op in sorted(raw_methods.items(), key=lambda item: str(item[0])):
            if not isinstance(method, str) or not isinstance(raw_op, Mapping):
                continue
            lower_method = method.lower().strip()
            if lower_method not in {"get", "post", "put", "patch", "delete", "head", "options"}:
                continue
            path_params, query_params, header_params = _extract_parameter_groups(raw_op.get("parameters"), path=path)
            operations.append(
                {
                    "operation_id": str(raw_op.get("operationId") or "").strip() or f"{lower_method}_{_slug(path)}",
                    "method": lower_method,
                    "path": path,
                    "summary": str(raw_op.get("summary") or "").strip(),
                    "description": str(raw_op.get("description") or "").strip(),
                    "tags": [
                        str(tag).strip() for tag in cast(Sequence[Any], raw_op.get("tags") or []) if str(tag).strip()
                    ],
                    "path_params": path_params,
                    "query_params": query_params,
                    "header_params": header_params,
                    "request_body": _normalize_request_body(raw_op.get("requestBody")),
                    "responses": _normalize_responses(raw_op.get("responses")),
                    "security": list(cast(Sequence[Any], raw_op.get("security") or [])),
                    "examples": [],
                }
            )
    if not any(_is_health_operation(op) for op in operations):
        health_component_name = _schema_component_name(target_name=node.name, suffix="HealthResponse")
        component_schemas.setdefault(
            health_component_name,
            {
                "type": "object",
                "additionalProperties": True,
                "required": ["status", "service"],
                "properties": {
                    "status": {"type": "string"},
                    "service": {"type": "string"},
                    "run_id": {"type": "string"},
                },
            },
        )
        operations.insert(
            0,
            {
                "operation_id": f"{_slug(node.name)}_healthz",
                "method": "get",
                "path": health_path,
                "summary": f"{node.name} health probe",
                "description": "Generated readiness contract.",
                "tags": [_slug(node.name), "health"],
                "path_params": [],
                "query_params": [],
                "header_params": [],
                "request_body": None,
                "responses": {
                    "200": {
                        "description": "Health status",
                        "content_type": "application/json",
                        "schema": {"$ref": f"#/components/schemas/{health_component_name}"},
                    }
                },
                "security": [],
                "examples": [],
            },
        )
    surface = {
        "contract_format": "openapi_3_1",
        "service_title": str(cast(dict[str, Any], contract_obj.get("info") or {}).get("title") or node.name),
        "base_path": health_path,
        "server_env_var": f"{_slug(node.name).replace('-', '_').upper()}_BASE_URL",
        "operations": operations,
        "component_schemas": component_schemas,
        "security_schemes": (
            dict(
                cast(
                    dict[str, Any],
                    cast(Mapping[str, Any], contract_obj.get("components") or {}).get("securitySchemes") or {},
                )
            )
            if isinstance(contract_obj.get("components"), Mapping)
            else {}
        ),
        "known_error_responses": sorted(
            {
                str(status)
                for op in operations
                for status in cast(dict[str, Any], op.get("responses") or {})
                if str(status).startswith(("4", "5"))
            }
        ),
        "contract_source_summary": [
            str(row.get("source_kind", "")).strip()
            for row in contract_sources
            if str(row.get("source_kind", "")).strip()
        ],
        "servers": list(cast(Sequence[Any], contract_obj.get("servers") or [])),
        "security": list(cast(Sequence[Any], contract_obj.get("security") or [])),
        "contract_depth": "full",
    }
    return _summarize_api_surface(surface)


def _api_surface_for_resource(
    *,
    node: IRNode,
    target_class: str,
    resource_path: str,
    request_schema_hint: Any,
    response_schema_hint: Any,
    contract_sources: Sequence[Mapping[str, Any]],
    node_properties: Mapping[str, Any] | None = None,
) -> dict[str, Any] | None:
    if target_class not in {"backend_service", "integration"}:
        return None
    props = dict(cast(dict[str, Any], node_properties or {}))
    slug = _slug(node.name)
    normalized_path = resource_path if resource_path.startswith("/") else f"/{resource_path}"
    normalized_health_path = str(props.get("health_path", "")).strip() or "/healthz"
    explicit_openapi = props.get("openapi_contract")
    if isinstance(explicit_openapi, Mapping):
        explicit_surface = _api_surface_from_openapi_contract(
            node=node,
            target_class=target_class,
            contract_obj=cast(Mapping[str, Any], explicit_openapi),
            contract_sources=contract_sources,
            health_path=normalized_health_path,
        )
        if explicit_surface is not None:
            return explicit_surface
    explicit_surface_raw = props.get("api_surface")
    if isinstance(explicit_surface_raw, Mapping):
        explicit_surface_candidate = dict(cast(dict[str, Any], explicit_surface_raw))
        if isinstance(explicit_surface_candidate.get("paths"), Mapping):
            explicit_surface = _api_surface_from_openapi_contract(
                node=node,
                target_class=target_class,
                contract_obj=explicit_surface_candidate,
                contract_sources=contract_sources,
                health_path=normalized_health_path,
            )
            if explicit_surface is not None:
                return explicit_surface
        operations = [
            dict(cast(dict[str, Any], item))
            for item in cast(Sequence[Any], explicit_surface_candidate.get("operations") or [])
            if isinstance(item, Mapping)
        ]
        if operations:
            explicit_surface_candidate.setdefault("contract_format", "openapi_3_1")
            explicit_surface_candidate.setdefault("service_title", node.name)
            explicit_surface_candidate.setdefault("base_path", normalized_path)
            explicit_surface_candidate.setdefault(
                "server_env_var", f"{_slug(node.name).replace('-', '_').upper()}_BASE_URL"
            )
            explicit_surface_candidate.setdefault("component_schemas", {})
            explicit_surface_candidate.setdefault("security_schemes", {})
            explicit_surface_candidate.setdefault(
                "contract_source_summary",
                [
                    str(row.get("source_kind", "")).strip()
                    for row in contract_sources
                    if str(row.get("source_kind", "")).strip()
                ],
            )
            explicit_surface_candidate.setdefault("servers", [{"url": "/"}])
            explicit_surface_candidate.setdefault("security", [])
            explicit_surface_candidate.setdefault("contract_depth", "full")
            return _summarize_api_surface(explicit_surface_candidate)
    explicit_operations = props.get("api_operations")
    if isinstance(explicit_operations, Sequence) and not isinstance(explicit_operations, (str, bytes)):
        operations = [dict(cast(dict[str, Any], item)) for item in explicit_operations if isinstance(item, Mapping)]
        if operations:
            return _summarize_api_surface(
                {
                    "contract_format": "openapi_3_1",
                    "service_title": node.name,
                    "base_path": normalized_path,
                    "server_env_var": f"{_slug(node.name).replace('-', '_').upper()}_BASE_URL",
                    "operations": operations,
                    "component_schemas": dict(cast(dict[str, Any], props.get("component_schemas") or {}))
                    if isinstance(props.get("component_schemas"), Mapping)
                    else {},
                    "security_schemes": dict(
                        cast(
                            dict[str, Any],
                            props.get("security_schemes") or props.get("securitySchemes") or {},
                        )
                    )
                    if isinstance(props.get("security_schemes") or props.get("securitySchemes"), Mapping)
                    else {},
                    "known_error_responses": ["400", "401", "403", "404", "500"],
                    "contract_source_summary": [
                        str(row.get("source_kind", "")).strip()
                        for row in contract_sources
                        if str(row.get("source_kind", "")).strip()
                    ],
                    "servers": [{"url": "/"}],
                    "security": list(cast(Sequence[Any], props.get("security") or [])),
                    "contract_depth": "full",
                }
            )
    request_schema, request_components = _schema_from_hint(
        hint=request_schema_hint,
        fallback_name="Request",
    )
    response_schema, response_components = _schema_from_hint(
        hint=response_schema_hint,
        fallback_name="Response",
    )
    health_component_name = _schema_component_name(target_name=node.name, suffix="HealthResponse")
    components = {
        **request_components,
        **response_components,
        health_component_name: {
            "type": "object",
            "additionalProperties": True,
            "required": ["status", "service"],
            "properties": {
                "status": {"type": "string"},
                "service": {"type": "string"},
                "run_id": {"type": "string"},
            },
        },
        "ErrorEnvelope": {
            "type": "object",
            "additionalProperties": True,
            "required": ["error"],
            "properties": {
                "error": {"type": "string"},
                "code": {"type": "string"},
                "details": {"type": "object", "additionalProperties": True},
            },
        },
    }
    primary_method = "post" if request_schema_hint is not None else "get"
    primary_operation = {
        "operation_id": f"{slug}_{primary_method}",
        "method": primary_method,
        "path": normalized_path,
        "summary": f"{node.name} primary endpoint",
        "description": f"Derived HTTP contract for {node.name}.",
        "tags": [slug],
        "path_params": _path_parameters(path=normalized_path),
        "query_params": [],
        "header_params": [],
        "request_body": (
            {
                "required": True,
                "content_type": "application/json",
                "schema": request_schema,
            }
            if request_schema_hint is not None
            else None
        ),
        "responses": {
            "200": {
                "description": "Successful response",
                "content_type": "application/json",
                "schema": response_schema,
            },
            "400": {
                "description": "Invalid request",
                "content_type": "application/json",
                "schema": {"$ref": "#/components/schemas/ErrorEnvelope"},
            },
            "500": {
                "description": "Unexpected error",
                "content_type": "application/json",
                "schema": {"$ref": "#/components/schemas/ErrorEnvelope"},
            },
        },
        "security": [],
        "examples": [],
    }
    health_operation = {
        "operation_id": f"{slug}_healthz",
        "method": "get",
        "path": normalized_health_path,
        "summary": f"{node.name} health probe",
        "description": "Generated readiness contract.",
        "tags": [slug, "health"],
        "path_params": [],
        "query_params": [],
        "header_params": [],
        "request_body": None,
        "responses": {
            "200": {
                "description": "Health status",
                "content_type": "application/json",
                "schema": {"$ref": f"#/components/schemas/{health_component_name}"},
            }
        },
        "security": [],
        "examples": [],
    }
    return _summarize_api_surface(
        {
            "contract_format": "openapi_3_1",
            "service_title": node.name,
            "base_path": normalized_path,
            "server_env_var": f"{_slug(node.name).replace('-', '_').upper()}_BASE_URL",
            "operations": [health_operation, primary_operation],
            "component_schemas": components,
            "security_schemes": {},
            "known_error_responses": ["400", "500"],
            "contract_source_summary": [
                str(row.get("source_kind", "")).strip()
                for row in contract_sources
                if str(row.get("source_kind", "")).strip()
            ],
            "servers": [{"url": "/"}],
            "security": [],
            "contract_depth": "minimal",
        }
    )


def _openapi_contract_rel_path(*, run_id: str, target_id: str) -> str:
    return f".akc/backend/{run_id}.{_slug(target_id)}.openapi.json"


def _build_openapi_contracts(
    *,
    run_id: str,
    tenant_id: str,
    repo_id: str,
    selected_runtime_plugin: str,
    resources: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    refs: list[dict[str, Any]] = []
    contracts: dict[str, dict[str, Any]] = {}
    for row in resources:
        target_id = str(row.get("target_id", "")).strip()
        api_surface = row.get("api_surface")
        if not target_id or not isinstance(api_surface, Mapping):
            continue
        paths: dict[str, dict[str, Any]] = {}
        for raw_op in cast(Sequence[Any], api_surface.get("operations") or []):
            if not isinstance(raw_op, Mapping):
                continue
            op_path = str(raw_op.get("path", "")).strip() or "/"
            method = str(raw_op.get("method", "get")).strip().lower() or "get"
            params: list[dict[str, Any]] = []
            for key in ("path_params", "query_params", "header_params"):
                raw_params = raw_op.get(key)
                if isinstance(raw_params, Sequence) and not isinstance(raw_params, (str, bytes)):
                    params.extend(
                        [dict(cast(dict[str, Any], item)) for item in raw_params if isinstance(item, Mapping)]
                    )
            request_body = raw_op.get("request_body")
            request_body_obj = None
            if isinstance(request_body, Mapping):
                content_type = str(request_body.get("content_type", "application/json")).strip() or "application/json"
                request_body_obj = {
                    "required": bool(request_body.get("required", False)),
                    "content": {
                        content_type: {
                            "schema": dict(cast(dict[str, Any], request_body.get("schema") or {"type": "object"}))
                        }
                    },
                }
            responses: dict[str, Any] = {}
            for status, payload in cast(dict[str, Any], raw_op.get("responses") or {}).items():
                if not isinstance(payload, Mapping):
                    continue
                content_type = str(payload.get("content_type", "application/json")).strip() or "application/json"
                responses[str(status)] = {
                    "description": str(payload.get("description", "response")).strip() or "response",
                    "content": {
                        content_type: {
                            "schema": dict(cast(dict[str, Any], payload.get("schema") or {"type": "object"}))
                        }
                    },
                }
            path_item = paths.setdefault(op_path, {})
            path_item[method] = {
                "operationId": str(raw_op.get("operation_id", "")).strip(),
                "summary": str(raw_op.get("summary", "")).strip(),
                "description": str(raw_op.get("description", "")).strip(),
                "tags": [str(tag).strip() for tag in cast(Sequence[Any], raw_op.get("tags") or []) if str(tag).strip()],
                "parameters": params,
                "requestBody": request_body_obj,
                "responses": responses,
                "security": list(cast(Sequence[Any], raw_op.get("security") or [])),
                "x-akc-contract-sources": list(cast(Sequence[Any], row.get("contract_sources") or [])),
            }
        contract_obj = {
            "openapi": "3.1.0",
            "info": {
                "title": str(api_surface.get("service_title") or row.get("name") or target_id),
                "version": "1.0.0",
                "description": "Derived from AKC backend IR.",
            },
            "servers": [
                dict(cast(dict[str, Any], item))
                for item in cast(Sequence[Any], api_surface.get("servers") or [])
                if isinstance(item, Mapping)
            ]
            or [{"url": "/"}],
            "paths": paths,
            "components": {
                "schemas": dict(cast(dict[str, Any], api_surface.get("component_schemas") or {})),
                "securitySchemes": dict(cast(dict[str, Any], api_surface.get("security_schemes") or {})),
            },
            "security": list(cast(Sequence[Any], api_surface.get("security") or [])),
            "x-akc-run-id": run_id,
            "x-akc-target-id": target_id,
            "x-akc-runtime-profile": selected_runtime_plugin,
            "x-akc-contract-sources": list(cast(Sequence[Any], row.get("contract_sources") or [])),
            "x-akc-contract-depth": api_surface.get("contract_depth"),
            "x-akc-feature-groups": list(cast(Sequence[Any], api_surface.get("feature_groups") or [])),
            "x-akc-auth-modes": list(cast(Sequence[Any], api_surface.get("auth_modes") or [])),
        }
        contracts[target_id] = contract_obj
        refs.append(
            {
                "target_id": target_id,
                "target_class": row.get("target_class"),
                "runtime_profile": selected_runtime_plugin,
                "openapi_rel_path": _openapi_contract_rel_path(run_id=run_id, target_id=target_id),
                "fingerprint": stable_json_fingerprint(contract_obj),
                "server_env_var": cast(dict[str, Any], api_surface).get("server_env_var"),
                "base_url_env_var": (
                    f"EXPO_PUBLIC_{_slug(str(row.get('name') or target_id)).replace('-', '_').upper()}_BASE_URL"
                ),
                "contract_depth": api_surface.get("contract_depth"),
                "operation_count": api_surface.get("operation_count"),
                "feature_groups": list(cast(Sequence[Any], api_surface.get("feature_groups") or [])),
                "auth_modes": list(cast(Sequence[Any], api_surface.get("auth_modes") or [])),
            }
        )
    index_obj = {
        "run_id": run_id,
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "selected_runtime_plugin": selected_runtime_plugin,
        "contract_format": "openapi_3_1",
        "contract_refs": sorted(refs, key=lambda item: str(item.get("target_id", ""))),
    }
    return index_obj, contracts


def build_practical_backend_context(
    *,
    run_id: str,
    ir_document: IRDocument,
    intent_spec: IntentSpecV1 | None,
    project_root: Path | None,
    delivery_plan_obj: Mapping[str, Any] | None = None,
    compile_succeeded: bool | None = None,
) -> dict[str, Any]:
    project_profile = detect_project_profile(root=project_root) if project_root is not None else None
    policy = load_backend_generator_policy(project_root=project_root)
    detected_languages = _detected_language_names(project_profile)
    allow_runtime_language_override = _policy_flag(policy.get("allow_runtime_language_override"))
    effective_native_commands = _effective_native_command_rows(project_profile)
    text_markers = _project_text_markers(project_root)
    frameworks = _framework_rows(markers=text_markers)
    persistence = _marker_rows(markers=text_markers, source_markers=_PERSISTENCE_MARKERS)
    observability = _marker_rows(markers=text_markers, source_markers=_OBSERVABILITY_MARKERS)
    transport = _marker_rows(markers=text_markers, source_markers=_TRANSPORT_MARKERS)
    anchors = _repo_anchor_candidates(project_root=project_root, policy=policy)
    topology = _test_topology(project_root=project_root, project_profile=project_profile)
    topology["native_commands"] = cast(JSONValue, effective_native_commands)
    selected_plugin, requested_by_policy = _select_runtime_plugin(
        project_profile=project_profile,
        frameworks=frameworks,
        policy=policy,
    )
    materializer = resolve_backend_runtime_materializer(
        selected_plugin=selected_plugin,
        project_root=project_root,
        policy=policy,
        requested_by_policy=requested_by_policy,
    )
    plugin = materializer.plugin
    persistence_strategy, persistence_reasons = _persistence_strategy(
        plugin=plugin,
        persistence_markers=persistence,
    )
    confidence = _confidence_score(
        project_profile=project_profile,
        frameworks=frameworks,
        anchors=anchors,
        plugin=plugin,
        persistence_strategy=persistence_strategy,
    )
    required_native_command_kinds = _required_native_command_kinds(
        plugin=plugin,
        detected_languages=detected_languages,
    )
    available_native_command_kinds = {
        str(row.get("kind", "")).strip()
        for row in effective_native_commands
        if isinstance(row, Mapping) and str(row.get("kind", "")).strip()
    }
    missing_required_native_command_kinds = [
        kind for kind in required_native_command_kinds if kind not in available_native_command_kinds
    ]
    min_conf = float(policy.get("minimum_adoption_confidence", 0.45) or 0.45)
    blocked_reasons: list[str] = []
    if plugin.supported_languages:
        supported_languages = {_normalize_language_name(language) for language in plugin.supported_languages}
        if detected_languages and not supported_languages.intersection(detected_languages):
            if allow_runtime_language_override and requested_by_policy:
                pass
            else:
                blocked_reasons.append(
                    "selected runtime plugin does not support detected project languages: "
                    + ", ".join(detected_languages)
                )
    if not anchors:
        blocked_reasons.append("no safe repo anchors detected")
    if persistence_strategy == "blocked":
        blocked_reasons.append("selected runtime plugin does not support fallback persistence for this repo")
    if not materializer.available:
        blocked_reasons.append(f"selected runtime plugin has no installed materializer: {selected_plugin.plugin_id}")
    elif not materializer.supports_authoritative_workspace:
        blocked_reasons.append(
            f"selected runtime plugin cannot emit authoritative workspaces: {selected_plugin.plugin_id}"
        )
    if missing_required_native_command_kinds:
        blocked_reasons.append(
            "missing native validation commands required for authoritative materialization: "
            + ", ".join(missing_required_native_command_kinds)
        )
    blocked_reasons.extend(materializer.diagnostics)
    if confidence < min_conf:
        blocked_reasons.append("repo adoption confidence below configured minimum")
    blocked_reasons = list(dict.fromkeys(reason for reason in blocked_reasons if reason))
    readiness = "ready" if not blocked_reasons else "blocked"
    why_this_target = [*persistence_reasons]
    if requested_by_policy:
        why_this_target.append("selected runtime plugin requested by policy")
    if allow_runtime_language_override and requested_by_policy and plugin.supported_languages:
        supported_languages = {_normalize_language_name(language) for language in plugin.supported_languages}
        if detected_languages and not supported_languages.intersection(detected_languages):
            why_this_target.append("policy allowed runtime/language override for authoritative materialization review")
    if frameworks:
        why_this_target.append("framework markers align with selected runtime plugin")
    if anchors:
        why_this_target.append("repo anchor candidates detected for patch-based realization")
    profile_obj: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "goal_statement": _goal_statement(intent_spec),
        "project_root": str(project_root) if project_root is not None else None,
        "detected_language_set": list(detected_languages),
        "detected_languages": (
            [
                {
                    "language": row.language,
                    "percent": float(row.percent),
                    "bytes": int(row.bytes),
                    "files": int(row.files),
                    "evidence": list(row.evidence),
                }
                for row in (project_profile.languages if project_profile is not None else [])
            ]
        ),
        "native_build_commands": effective_native_commands,
        "detected_frameworks": frameworks,
        "persistence_markers": persistence,
        "observability_markers": observability,
        "transport_markers": transport,
        "test_topology": topology,
        "repo_anchor_candidates": anchors,
        "selected_runtime_plugin": plugin.plugin_id,
        "selected_runtime_maturity": plugin.maturity,
        "persistence_strategy": persistence_strategy,
        "adoption_confidence_score": confidence,
        "adoption_readiness": readiness,
        "blocked_reasons": blocked_reasons,
        "required_native_command_kinds": list(required_native_command_kinds),
        "missing_required_native_command_kinds": missing_required_native_command_kinds,
        "why_this_target": why_this_target,
        "generator_policy": dict(policy),
        "plugin_manifest_diagnostics": list(materializer.diagnostics),
    }

    deployable_kinds = {"service", "integration", "agent", "workflow", "infrastructure"}
    delivery_targets_by_id: dict[str, Mapping[str, Any]] = {}
    if isinstance(delivery_plan_obj, Mapping):
        raw_targets = delivery_plan_obj.get("targets")
        if isinstance(raw_targets, Sequence) and not isinstance(raw_targets, (str, bytes)):
            for item in raw_targets:
                if not isinstance(item, Mapping):
                    continue
                target_id = item.get("target_id")
                if isinstance(target_id, str) and target_id.strip():
                    delivery_targets_by_id[target_id.strip()] = item
    resources: list[dict[str, Any]] = []
    anchor_paths = [str(row.get("path", "")).strip() for row in anchors if str(row.get("path", "")).strip()]
    for node in sorted((n for n in ir_document.nodes if n.kind in deployable_kinds), key=lambda item: item.id):
        projected = delivery_targets_by_id.get(node.id, {})
        props = dict(node.properties)
        target_name = _slug(node.name)
        contract_sources = _contract_sources_for_node(
            node=node,
            project_root=project_root,
            transport_markers=transport,
            anchors=anchors,
        )
        api_surface = _api_surface_for_resource(
            node=node,
            target_class=str(projected.get("target_class", ""))
            or ("backend_service" if node.kind in {"service", "agent"} else "integration"),
            resource_path=str(props.get("path", "")) or f"/api/{target_name}",
            request_schema_hint=props.get("request_schema") or props.get("input_schema"),
            response_schema_hint=props.get("response_schema") or props.get("output_schema"),
            contract_sources=contract_sources,
            node_properties=props,
        )
        api_surface_dict = api_surface or {}
        resources.append(
            {
                "target_id": node.id,
                "name": node.name,
                "kind": node.kind,
                "target_class": str(projected.get("target_class", ""))
                or ("backend_service" if node.kind in {"service", "agent"} else "integration"),
                "depends_on": list(node.depends_on),
                "resource_path": str(props.get("path", "")) or f"/api/{target_name}",
                "request_schema_hint": props.get("request_schema") or props.get("input_schema"),
                "response_schema_hint": props.get("response_schema") or props.get("output_schema"),
                "persistence_intent": {
                    "strategy": persistence_strategy,
                    "existing_markers": [str(row.get("name", "")) for row in persistence],
                    "requires_migration": persistence_strategy in {"repo_native", "sql_first"},
                },
                "background_jobs": (
                    [node.name] if any(token in node.name.lower() for token in ("worker", "job", "queue")) else []
                ),
                "config_env_keys": sorted(
                    {str(x).strip() for x in cast(Sequence[Any], props.get("env", []) or []) if str(x).strip()}
                ),
                "secret_keys": sorted(
                    {str(x).strip() for x in cast(Sequence[Any], props.get("secrets", []) or []) if str(x).strip()}
                ),
                "health_contract": {
                    "path": str(props.get("health_path", "")) or "/healthz",
                    "requires_readiness_contract": True,
                },
                "observability_contract": {
                    "logging": True,
                    "metrics": True,
                    "tracing": True,
                    "repo_markers": [str(row.get("name", "")) for row in observability],
                },
                "repo_anchor_paths": anchor_paths[:12],
                "api_surface": api_surface,
                "contract_depth": str(api_surface_dict.get("contract_depth") or "minimal"),
                "api_feature_groups": list(cast(Sequence[Any], api_surface_dict.get("feature_groups") or [])),
                "api_auth_modes": list(cast(Sequence[Any], api_surface_dict.get("auth_modes") or [])),
                "operation_count": int(api_surface_dict.get("operation_count") or 0),
                "contract_sources": contract_sources,
                "contract_confidence": round(min(0.98, confidence + 0.05), 2) if api_surface is not None else 0.0,
                "contract_evidence": contract_sources,
            }
        )
    backend_api_contract_index_obj, api_contracts = _build_openapi_contracts(
        run_id=run_id,
        tenant_id=ir_document.tenant_id,
        repo_id=ir_document.repo_id,
        selected_runtime_plugin=plugin.plugin_id,
        resources=resources,
    )
    acceptance_scenarios = [
        {
            "scenario_id": f"{_slug(plugin.plugin_id)}-native-validation",
            "mode": "native_commands",
            "required": True,
        },
        {
            "scenario_id": f"{_slug(plugin.plugin_id)}-contract-proof",
            "mode": "contract_tests",
            "required": True,
        },
    ]
    backend_ir_obj: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "goal_statement": _goal_statement(intent_spec),
        "selected_runtime_plugin": plugin.plugin_id,
        "resources": resources,
        "contract_layers": {
            "evidence_sources": [
                "codebase",
                "docs",
                "messaging",
                "openapi",
            ],
            "canonical_ir_layer": "backend_ir",
            "derived_http_contract_format": "openapi_3_1",
        },
        "cross_cut_requirements": [
            "request_validation",
            "config_env_wiring",
            "health_readiness_contract",
            "structured_logging",
            "metrics_tracing_hooks",
            "native_tests",
            "runnable_contract_proof",
            "shared_http_contract",
        ],
        "repo_anchor_plan": anchors[:16],
        "acceptance_scenarios": acceptance_scenarios,
    }

    seed_steps = [
        {
            "id": "step_backend_repo_analysis",
            "title": "Confirm repo anchors and backend boundaries",
            "phase": "repo_adoption_analysis",
        },
        {
            "id": "step_backend_domain_persistence",
            "title": "Implement models and persistence wiring",
            "phase": "implementation",
        },
        {
            "id": "step_backend_transport_observability",
            "title": "Implement transport, config, and observability wiring",
            "phase": "implementation",
        },
        {
            "id": "step_backend_tests_acceptance",
            "title": "Add native tests and contract proof coverage",
            "phase": "acceptance",
        },
    ]
    implementation_plan_obj: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "selected_runtime_plugin": plugin.plugin_id,
        "phases": [
            {"phase": "repo_adoption_analysis", "status": readiness, "required_outputs": ["repo_anchor_candidates"]},
            {
                "phase": "backend_ir_synthesis",
                "status": "ready",
                "required_outputs": ["resources", "repo_anchor_plan", "acceptance_scenarios"],
            },
            {"phase": "implementation", "status": readiness, "required_outputs": ["scoped_patch", "tests"]},
            {"phase": "acceptance", "status": readiness, "required_outputs": ["native_validation", "contract_proof"]},
        ],
        "seed_steps": seed_steps,
    }

    native_commands = cast(list[dict[str, Any]], topology.get("native_commands", []))
    required_commands = [
        row
        for row in native_commands
        if str(row.get("kind", "")).strip() in set(required_native_command_kinds).union({"test"})
    ]
    proof_command: list[str] | None = None
    if required_commands:
        proof_command = [str(x) for x in cast(list[Any], required_commands[0].get("command", []))]
    acceptance_contract_obj: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "selected_runtime_plugin": plugin.plugin_id,
        "required_native_commands": required_commands,
        "proof_strategy": "contract_tests" if proof_command is not None else "artifact_only_review",
        "proof_command": proof_command,
        "repo_anchor_paths": anchor_paths[:12],
        "required_cross_cuts": backend_ir_obj["cross_cut_requirements"],
        "persistence_expectation": persistence_strategy,
        "unsupported_feature_behavior": plugin.unsupported_behavior,
        "api_contract_refs": list(cast(Sequence[Any], backend_api_contract_index_obj.get("contract_refs") or [])),
    }

    runtime_plugin_decision_obj: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        **plugin.to_json_obj(),
        **materializer.to_json_obj(),
        "required_native_command_kinds": list(required_native_command_kinds),
        "selected_because": why_this_target,
        "adoption_confidence_score": confidence,
        "adoption_readiness": readiness,
        "blocked_reasons": blocked_reasons,
    }

    execution_workspace_role = (
        "authoritative_generated_workspace" if readiness == "ready" else "fallback_debug_reference"
    )
    practical_generation_result_obj: dict[str, Any] = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "status": readiness
        if compile_succeeded is None
        else ("succeeded" if compile_succeeded and readiness == "ready" else readiness),
        "selected_runtime_plugin": plugin.plugin_id,
        "selected_runtime_maturity": plugin.maturity,
        "plugin_source": materializer.plugin_source,
        "materializer_kind": materializer.materializer_kind,
        "supports_authoritative_workspace": materializer.supports_authoritative_workspace,
        "requested_by_policy": requested_by_policy,
        "availability": materializer.availability,
        "blocked_stage": materializer.blocked_stage,
        "adoption_confidence_score": confidence,
        "blocked_reasons": blocked_reasons,
        "practical_success_requires": [
            "scoped_patch_applied",
            "native_validation_passed",
            "contract_proof_passed",
            "cross_cuts_present",
            "no_blocked_feature_downgrade",
            "shared_http_contract_emitted",
        ],
        "execution_workspace_role": execution_workspace_role,
        "fallback_mode": str(policy.get("fallback_mode", "blocked") or "blocked"),
    }

    handoff = practical_backend_handoff_from_context(
        {
            "backend_generation_profile": profile_obj,
            "backend_ir": backend_ir_obj,
            "backend_api_contract_index": backend_api_contract_index_obj,
            "api_contracts": api_contracts,
            "implementation_plan": implementation_plan_obj,
            "implementation_acceptance_contract": acceptance_contract_obj,
            "practical_generation_result": practical_generation_result_obj,
            "runtime_plugin_decision": runtime_plugin_decision_obj,
        }
    )
    return {
        "backend_generation_profile": profile_obj,
        "backend_ir": backend_ir_obj,
        "backend_api_contract_index": backend_api_contract_index_obj,
        "api_contracts": api_contracts,
        "implementation_plan": implementation_plan_obj,
        "implementation_acceptance_contract": acceptance_contract_obj,
        "runtime_plugin_decision": runtime_plugin_decision_obj,
        "practical_generation_result": practical_generation_result_obj,
        "handoff": handoff,
        "seed_steps": seed_steps,
        "prompt_context": {
            "backend_generation_profile": profile_obj,
            "backend_ir": backend_ir_obj,
            "backend_api_contract_index": backend_api_contract_index_obj,
            "implementation_acceptance_contract": acceptance_contract_obj,
            "runtime_plugin_decision": runtime_plugin_decision_obj,
        },
    }
