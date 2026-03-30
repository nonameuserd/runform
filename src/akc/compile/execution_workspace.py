"""Compile-time materialization of generated app/service trees for packaging/delivery.

This is **not** the same tree as the operator's working copy used by the Phase 3
compile loop (``apply_scope_root`` / tenant-scoped executor cwd): the controller
validates patches and runs native commands there; this module emits a separate
product workspace under ``.akc/execution/<run_id>/`` for downstream delivery
(EAS/Expo, generated services)."""

from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, cast

from akc.artifacts.contracts import apply_schema_envelope
from akc.compile.backend_generation import (
    BUILTIN_AUTHORITATIVE_RUNTIME_PLUGINS,
    PracticalBackendHandoff,
    get_backend_runtime_plugin,
    load_backend_generator_policy,
    resolve_backend_runtime_materializer,
)
from akc.ir import IRDocument
from akc.memory.models import JSONValue
from akc.outputs.models import OutputArtifact
from akc.utils.fingerprint import stable_json_fingerprint


def _slug(value: str) -> str:
    out = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value))
    while "--" in out:
        out = out.replace("--", "-")
    return out.strip("-") or "app"


def _pascal_identifier(value: str) -> str:
    parts = [part for part in re.split(r"[^A-Za-z0-9]+", str(value)) if part]
    if not parts:
        return "Generated"
    out = "".join(part[:1].upper() + part[1:] for part in parts)
    return f"Generated{out}" if out[:1].isdigit() else out


def _camel_identifier(value: str) -> str:
    pascal = _pascal_identifier(value)
    return pascal[:1].lower() + pascal[1:]


def _read_json(path: Path) -> dict[str, Any] | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _project_profile(project_root: Path | None) -> dict[str, Any] | None:
    if project_root is None:
        return None
    return _read_json(project_root / ".akc" / "project_profile.json")


def _operator_prereqs(project_root: Path | None) -> dict[str, Any]:
    if project_root is None:
        return {}
    return _read_json(project_root / ".akc" / "delivery" / "operator_prereqs.json") or {}


def _client_codegen_command(*, project_root: Path | None) -> list[str] | None:
    env_cmd = str(os.environ.get("AKC_OPENAPI_CLIENT_GENERATOR_CMD", "")).strip()
    if env_cmd:
        return shlex.split(env_cmd)
    op = _operator_prereqs(project_root)
    execution = op.get("execution")
    if isinstance(execution, dict):
        raw = execution.get("openapi_client_generator_cmd")
        if isinstance(raw, str) and raw.strip():
            return shlex.split(raw.strip())
    for candidate in ("openapi-generator-cli", "openapi-generator"):
        resolved = shutil.which(candidate)
        if resolved:
            return [resolved]
    return None


def infer_backend_runtime_profile(*, project_root: Path | None) -> str:
    op = _operator_prereqs(project_root)
    override = (
        cast(dict[str, Any], op.get("execution") or {}).get("backend_runtime_override")
        if isinstance(op.get("execution"), dict)
        else None
    )
    if isinstance(override, str) and override.strip():
        override_s = override.strip()
        if get_backend_runtime_plugin(override_s) is not None:
            return override_s

    profile = _project_profile(project_root)
    languages = profile.get("languages") if isinstance(profile, dict) else None
    if isinstance(languages, list):
        for row in languages:
            if not isinstance(row, dict):
                continue
            language = str(row.get("language") or "").strip().lower()
            if language == "python":
                return "python_fastapi"
            if language in {"javascript", "typescript"}:
                return "typescript_node"
            if language == "go":
                return "go"
            if language == "rust":
                return "rust"
            if language == "java":
                return "java"
    return "typescript_node"


def infer_package_manager(*, project_root: Path | None) -> str:
    profile = _project_profile(project_root)
    package_managers = profile.get("package_managers") if isinstance(profile, dict) else None
    if isinstance(package_managers, list):
        ordered = [str(x).strip().lower() for x in package_managers if str(x).strip()]
        for candidate in ("pnpm", "yarn", "npm"):
            if candidate in ordered:
                return candidate
    return "npm"


def _expo_package_json(*, package_manager: str) -> dict[str, Any]:
    return {
        "name": "akc-universal-app",
        "private": True,
        "version": "1.0.0",
        "main": "index.js",
        "scripts": {
            "start": "expo start",
            "web": "expo start --web",
            "export:web": "expo export --platform web",
            "ios": "eas build --platform ios --profile preview",
            "android": "eas build --platform android --profile preview",
        },
        "dependencies": {
            "@tanstack/react-query": "^5.66.0",
            "@tanstack/react-query-persist-client": "^5.66.0",
            "expo": "^53.0.0",
            "expo-secure-store": "~14.2.3",
            "react": "^19.0.0",
            "react-native": "^0.79.0",
        },
        "packageManager": (
            "pnpm@10" if package_manager == "pnpm" else "yarn@1.22.22" if package_manager == "yarn" else "npm@10"
        ),
    }


def _expo_app_json(*, ir_document: IRDocument, app_slug: str, expo_project_id: str | None) -> dict[str, Any]:
    repo_slug = _slug(ir_document.repo_id)
    app_obj: dict[str, Any] = {
        "expo": {
            "name": ir_document.repo_id,
            "slug": app_slug,
            "scheme": app_slug.replace("-", ""),
            "version": "1.0.0",
            "orientation": "portrait",
            "platforms": ["ios", "android", "web"],
            "web": {"bundler": "metro", "output": "static"},
            "ios": {"bundleIdentifier": f"com.akc.{repo_slug}"},
            "android": {"package": f"com.akc.{repo_slug}"},
            "extra": {"akc": {"tenant_id": ir_document.tenant_id, "repo_id": ir_document.repo_id}},
        }
    }
    if expo_project_id:
        cast(dict[str, Any], cast(dict[str, Any], app_obj["expo"])["extra"])["eas"] = {"projectId": expo_project_id}
    return app_obj


def _expo_eas_json() -> dict[str, Any]:
    return {
        "cli": {"version": ">= 14.0.0", "appVersionSource": "remote"},
        "build": {
            "preview": {"distribution": "internal"},
            "production": {"autoIncrement": True},
        },
        "submit": {"preview": {}, "production": {}},
    }


def _contract_operations(contract_obj: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not isinstance(contract_obj, Mapping):
        return []
    paths = contract_obj.get("paths")
    if not isinstance(paths, Mapping):
        return []
    ops: list[dict[str, Any]] = []
    for path, raw_methods in sorted(paths.items(), key=lambda item: str(item[0])):
        if not isinstance(path, str) or not isinstance(raw_methods, Mapping):
            continue
        for method, raw_op in sorted(raw_methods.items(), key=lambda item: str(item[0])):
            if not isinstance(method, str) or not isinstance(raw_op, Mapping):
                continue
            ops.append(
                {
                    "path": path,
                    "method": method.lower(),
                    "operationId": str(raw_op.get("operationId") or "").strip() or f"{method}_{_slug(path)}",
                    "summary": str(raw_op.get("summary") or "").strip(),
                    "tags": [
                        str(tag).strip() for tag in cast(Sequence[Any], raw_op.get("tags") or []) if str(tag).strip()
                    ],
                    "security": list(cast(Sequence[Any], raw_op.get("security") or [])),
                    "parameters": [
                        dict(cast(dict[str, Any], item))
                        for item in cast(Sequence[Any], raw_op.get("parameters") or [])
                        if isinstance(item, Mapping)
                    ],
                    "requestBody": dict(cast(dict[str, Any], raw_op.get("requestBody") or {}))
                    if isinstance(raw_op.get("requestBody"), Mapping)
                    else None,
                }
            )
    return ops


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
    operation_id = _slug(str(operation.get("operationId") or operation.get("operation_id") or ""))
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
    if scheme_type == "apikey":
        return "session" if str(scheme.get("in") or "").strip().lower() == "cookie" else "apiKey"
    if scheme_type == "http":
        http_scheme = str(scheme.get("scheme") or "").strip().lower()
        if http_scheme == "bearer":
            return "bearer"
        if http_scheme == "basic":
            return "basic"
    if scheme_type in {"oauth2", "openidconnect"}:
        return "oauth2"
    return "none"


def _auth_mode_for_operation(*, security: Sequence[Any], security_schemes: Mapping[str, Any]) -> str:
    for requirement in security:
        if not isinstance(requirement, Mapping):
            continue
        for scheme_name in requirement:
            scheme = security_schemes.get(str(scheme_name))
            if isinstance(scheme, Mapping):
                return _auth_mode_for_scheme(scheme)
    return "none"


def _contract_security_schemes(contract_obj: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(contract_obj, Mapping):
        return {}
    components = contract_obj.get("components")
    if not isinstance(components, Mapping):
        return {}
    security_schemes = components.get("securitySchemes")
    return dict(cast(dict[str, Any], security_schemes)) if isinstance(security_schemes, Mapping) else {}


def _contract_depth_for_target(*, target: Mapping[str, JSONValue], contract_obj: Mapping[str, Any] | None) -> str:
    contract_ref = target.get("api_contract_ref")
    if isinstance(contract_ref, Mapping):
        raw = contract_ref.get("contract_depth")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    if isinstance(contract_obj, Mapping):
        raw = contract_obj.get("x-akc-contract-depth")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
        non_health = [op for op in _contract_operations(contract_obj) if not _is_health_operation(op)]
        return "full" if len(non_health) > 1 else "minimal"
    return "minimal"


def _operation_registry_rows(
    *,
    frontend_targets: list[dict[str, JSONValue]],
    contracts_by_target: Mapping[str, Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    feature_modules: list[dict[str, Any]] = []
    summary_targets: list[dict[str, Any]] = []
    for target in frontend_targets:
        target_id = str(target["target_id"])
        contract_obj = contracts_by_target.get(target_id)
        operations = _contract_operations(contract_obj)
        security_schemes = _contract_security_schemes(contract_obj)
        contract_fingerprint = stable_json_fingerprint(dict(contract_obj or {}))
        contract_depth = _contract_depth_for_target(target=target, contract_obj=contract_obj)
        feature_groups = sorted(
            {
                _feature_group_from_operation(path=str(op["path"]), tags=cast(Sequence[Any], op.get("tags") or []))
                for op in operations
                if not _is_health_operation(op)
            }
        )
        auth_modes = sorted(
            {
                _auth_mode_for_operation(
                    security=cast(Sequence[Any], op.get("security") or []),
                    security_schemes=security_schemes,
                )
                for op in operations
                if not _is_health_operation(op)
            }
            - {"none"}
        )
        module_map: dict[str, list[dict[str, Any]]] = {}
        for op in operations:
            tags = cast(Sequence[Any], op.get("tags") or [])
            feature_group = (
                "diagnostics"
                if _is_health_operation(op)
                else _feature_group_from_operation(path=str(op["path"]), tags=tags)
            )
            registry_row = {
                "targetId": target_id,
                "targetName": str(target["name"]),
                "targetClass": str(target["target_class"]),
                "operationId": str(op["operationId"]),
                "method": str(op["method"]).upper(),
                "path": str(op["path"]),
                "featureGroup": feature_group,
                "authMode": _auth_mode_for_operation(
                    security=cast(Sequence[Any], op.get("security") or []),
                    security_schemes=security_schemes,
                ),
                "cacheKind": "query" if str(op["method"]).lower() in {"get", "head"} else "mutation",
                "contractFingerprint": contract_fingerprint,
                "hasRequestBody": bool(op.get("requestBody")),
                "pathParamNames": [
                    str(row.get("name"))
                    for row in cast(Sequence[Any], op.get("parameters") or [])
                    if isinstance(row, Mapping)
                    and str(row.get("in") or "").strip() == "path"
                    and str(row.get("name") or "").strip()
                ],
                "queryParamNames": [
                    str(row.get("name"))
                    for row in cast(Sequence[Any], op.get("parameters") or [])
                    if isinstance(row, Mapping)
                    and str(row.get("in") or "").strip() == "query"
                    and str(row.get("name") or "").strip()
                ],
                "headerParamNames": [
                    str(row.get("name"))
                    for row in cast(Sequence[Any], op.get("parameters") or [])
                    if isinstance(row, Mapping)
                    and str(row.get("in") or "").strip() == "header"
                    and str(row.get("name") or "").strip()
                ],
                "isHealth": _is_health_operation(op),
            }
            rows.append(registry_row)
            if not registry_row["isHealth"] and contract_depth == "full":
                module_map.setdefault(feature_group, []).append(registry_row)
        module_slugs: list[str] = []
        for feature_group, module_ops in sorted(module_map.items()):
            module_slug = f"{_slug(target_id)}-{_slug(feature_group)}"
            module_slugs.append(module_slug)
            feature_modules.append(
                {
                    "moduleSlug": module_slug,
                    "targetId": target_id,
                    "targetName": str(target["name"]),
                    "featureGroup": feature_group,
                    "integrationMode": "feature_modules",
                    "operationIds": [str(item["operationId"]) for item in module_ops],
                    "authModes": sorted(
                        {str(item["authMode"]) for item in module_ops if str(item["authMode"]) != "none"}
                    ),
                    "operationCount": len(module_ops),
                }
            )
        summary_targets.append(
            {
                "targetId": target_id,
                "name": str(target["name"]),
                "contractDepth": contract_depth,
                "integrationMode": "feature_modules" if module_slugs else "diagnostics_only",
                "eligibleForFeatures": bool(module_slugs),
                "featureGroups": feature_groups,
                "authModes": auth_modes,
                "operationCount": len([op for op in operations if not _is_health_operation(op)]),
                "generatedModuleSlugs": module_slugs,
                "contractFingerprint": contract_fingerprint,
                "baseUrlEnvVar": str(target["frontend_base_url_env_var"]),
            }
        )
    summary = {
        "integrationFingerprint": stable_json_fingerprint(
            {"targets": summary_targets, "modules": feature_modules, "operations": rows}
        ),
        "targets": summary_targets,
        "generatedFeatureModules": feature_modules,
        "generatedOperationCount": len([row for row in rows if not row["isHealth"]]),
        "generatedFeatureModuleCount": len(feature_modules),
        "eligibleTargetCount": sum(1 for row in summary_targets if row["eligibleForFeatures"]),
        "diagnosticOnlyTargetCount": sum(1 for row in summary_targets if not row["eligibleForFeatures"]),
    }
    return rows, feature_modules, summary


def _js_path_for_express(path: str) -> str:
    return re.sub(r"\{([^{}]+)\}", r":\1", str(path))


def _python_response_dict(*, service_name: str, run_id: str, target_class: str, operation_id: str) -> str:
    payload = {
        "status": "ok",
        "service": service_name,
        "run_id": run_id,
        "target_class": target_class,
        "operation_id": operation_id,
    }
    return json.dumps(payload, sort_keys=True)


def _typescript_service_files(
    *,
    service_name: str,
    run_id: str,
    target_class: str,
    contract_obj: Mapping[str, Any] | None,
) -> dict[str, str]:
    operations = _contract_operations(contract_obj)
    route_lines = [
        "import express from 'express';\n",
        "import contract from './openapi.contract.json' assert { type: 'json' };\n\n",
        "const app = express();\n",
        "app.use(express.json());\n\n",
    ]
    for op in operations:
        route_path = _js_path_for_express(str(op["path"]))
        op_id = str(op["operationId"])
        route_lines.append(
            f"app.{str(op['method'])}('{route_path}', (_req, res) => res.json("
            + json.dumps(
                {
                    "status": "ok",
                    "service": service_name,
                    "run_id": run_id,
                    "target_class": target_class,
                    "operation_id": op_id,
                },
                sort_keys=True,
            )
            + "));\n"
        )
    route_lines.extend(
        [
            "\nconst port = Number(process.env.PORT || 8080);\n",
            "app.listen(port, () => {\n",
            (
                f"  console.log('{service_name} listening on', port, "
                "'operations', Object.keys(contract.paths || {}).length);\n"
            ),
            "});\n",
        ]
    )
    return {
        "package.json": json.dumps(
            {
                "name": service_name,
                "private": True,
                "type": "module",
                "scripts": {"start": "node src/index.js"},
                "dependencies": {"express": "^4.19.2"},
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        "src/index.js": "".join(route_lines),
        "src/openapi.contract.json": json.dumps(dict(contract_obj or {}), indent=2, sort_keys=True) + "\n",
        "Dockerfile": (
            "FROM node:20-slim\n"
            "WORKDIR /app\n"
            "COPY package.json ./\n"
            "RUN npm install --omit=dev\n"
            "COPY src ./src\n"
            'CMD ["node", "src/index.js"]\n'
        ),
    }


def _python_service_files(
    *,
    service_name: str,
    run_id: str,
    target_class: str,
    contract_obj: Mapping[str, Any] | None,
) -> dict[str, str]:
    operations = _contract_operations(contract_obj)
    route_lines = ["from fastapi import FastAPI\n\n", "app = FastAPI()\n\n"]
    for op in operations:
        decorator = str(op["method"]).lower()
        op_id = str(op["operationId"])
        route_lines.append(f"@app.{decorator}({json.dumps(str(op['path']))})\n")
        route_lines.append(f"def {op_id}() -> dict[str, str]:\n")
        route_lines.append(
            "    return "
            + _python_response_dict(
                service_name=service_name,
                run_id=run_id,
                target_class=target_class,
                operation_id=op_id,
            )
            + "\n\n"
        )
    return {
        "requirements.txt": "fastapi>=0.111.0\nuvicorn>=0.30.0\n",
        "app.py": "".join(route_lines),
        "openapi.contract.json": json.dumps(dict(contract_obj or {}), indent=2, sort_keys=True) + "\n",
        "Dockerfile": (
            "FROM python:3.12-slim\n"
            "WORKDIR /app\n"
            "COPY requirements.txt ./\n"
            "RUN pip install --no-cache-dir -r requirements.txt\n"
            "COPY app.py ./app.py\n"
            "COPY openapi.contract.json ./openapi.contract.json\n"
            'CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]\n'
        ),
    }


def _go_service_files(
    *,
    service_name: str,
    run_id: str,
    target_class: str,
    contract_obj: Mapping[str, Any] | None,
) -> dict[str, str]:
    operations = _contract_operations(contract_obj)
    route_lines = [
        "package main\n\n",
        "import (\n",
        '    "encoding/json"\n',
        '    "log"\n',
        '    "net/http"\n',
        '    "os"\n',
        ")\n\n",
        "func writeJSON(w http.ResponseWriter, payload any) {\n",
        '    w.Header().Set("Content-Type", "application/json")\n',
        "    _ = json.NewEncoder(w).Encode(payload)\n",
        "}\n\n",
        "func main() {\n",
        "    mux := http.NewServeMux()\n",
    ]
    for op in operations:
        route_lines.extend(
            [
                f"    mux.HandleFunc({json.dumps(str(op['path']))}, func(w http.ResponseWriter, r *http.Request) {{\n",
                f"        if r.Method != {json.dumps(str(op['method']).upper())} {{\n",
                "            w.WriteHeader(http.StatusMethodNotAllowed)\n",
                '            writeJSON(w, map[string]any{"status": "error", "error": "method_not_allowed"})\n',
                "            return\n",
                "        }\n",
                "        writeJSON(w, map[string]any{\n",
                f'            "status": {json.dumps("ok")},\n',
                f'            "service": {json.dumps(service_name)},\n',
                f'            "run_id": {json.dumps(run_id)},\n',
                f'            "target_class": {json.dumps(target_class)},\n',
                f'            "operation_id": {json.dumps(str(op["operationId"]))},\n',
                "        })\n",
                "    })\n",
            ]
        )
    route_lines.extend(
        [
            '    port := os.Getenv("PORT")\n',
            '    if port == "" {\n',
            '        port = "8080"\n',
            "    }\n",
            f'    log.Printf("{service_name} listening on %s with %d operations", port, {len(operations)})\n',
            '    log.Fatal(http.ListenAndServe(":"+port, mux))\n',
            "}\n",
        ]
    )
    return {
        "go.mod": f"module generated/{service_name}\n\ngo 1.22\n",
        "main.go": "".join(route_lines),
        "openapi.contract.json": json.dumps(dict(contract_obj or {}), indent=2, sort_keys=True) + "\n",
        "Dockerfile": (
            "FROM golang:1.22-alpine AS build\n"
            "WORKDIR /app\n"
            "COPY go.mod ./\n"
            "COPY main.go ./main.go\n"
            "RUN go build -o service ./main.go\n\n"
            "FROM alpine:3.20\n"
            "WORKDIR /app\n"
            "COPY --from=build /app/service ./service\n"
            "COPY openapi.contract.json ./openapi.contract.json\n"
            'CMD ["./service"]\n'
        ),
    }


def _rust_service_files(
    *,
    service_name: str,
    run_id: str,
    target_class: str,
    contract_obj: Mapping[str, Any] | None,
) -> dict[str, str]:
    operations = _contract_operations(contract_obj)
    handler_lines: list[str] = []
    route_lines = ["    let app = Router::new()\n"]
    method_map = {"get": "get", "post": "post", "put": "put", "patch": "patch", "delete": "delete"}
    for op in operations:
        operation_id = str(op["operationId"])
        handler_name = _camel_identifier(operation_id)
        method = method_map.get(str(op["method"]).lower(), "get")
        handler_lines.append(
            f"async fn {handler_name}() -> Json<Value> {{\n"
            "    Json(json!({\n"
            f'        "status": {json.dumps("ok")},\n'
            f'        "service": {json.dumps(service_name)},\n'
            f'        "run_id": {json.dumps(run_id)},\n'
            f'        "target_class": {json.dumps(target_class)},\n'
            f'        "operation_id": {json.dumps(operation_id)}\n'
            "    }))\n"
            "}\n\n"
        )
        route_lines.append(f"        .route({json.dumps(str(op['path']))}, routing::{method}({handler_name}))\n")
    route_lines.append("    ;\n")
    main_rs = (
        "use axum::{routing, Json, Router};\n"
        "use serde_json::{json, Value};\n"
        "use std::net::SocketAddr;\n\n"
        + "".join(handler_lines)
        + "#[tokio::main]\n"
        + "async fn main() {\n"
        + "".join(route_lines)
        + '    let port = std::env::var("PORT")\n'
        + "        .ok()\n"
        + "        .and_then(|value| value.parse::<u16>().ok())\n"
        + "        .unwrap_or(8080);\n"
        + "    let addr = SocketAddr::from(([0, 0, 0, 0], port));\n"
        + f'    println!("{service_name} listening on {{}} with {len(operations)} operations", port);\n'
        + '    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind listener");\n'
        + '    axum::serve(listener, app).await.expect("serve axum app");\n'
        + "}\n"
    )
    return {
        "Cargo.toml": (
            "[package]\n"
            f'name = "{service_name}"\n'
            'version = "0.1.0"\n'
            'edition = "2021"\n\n'
            "[dependencies]\n"
            'axum = "0.7"\n'
            'serde_json = "1"\n'
            'tokio = { version = "1", features = ["macros", "rt-multi-thread", "net"] }\n'
        ),
        "src/main.rs": main_rs,
        "openapi.contract.json": json.dumps(dict(contract_obj or {}), indent=2, sort_keys=True) + "\n",
        "Dockerfile": (
            "FROM rust:1.78-slim AS build\n"
            "WORKDIR /app\n"
            "COPY Cargo.toml ./Cargo.toml\n"
            "RUN mkdir -p src && printf 'fn main() {}\\n' > src/main.rs\n"
            "RUN cargo build --release || true\n"
            "COPY src ./src\n"
            "RUN cargo build --release\n\n"
            "FROM debian:bookworm-slim\n"
            "WORKDIR /app\n"
            f"COPY --from=build /app/target/release/{service_name} ./service\n"
            "COPY openapi.contract.json ./openapi.contract.json\n"
            'CMD ["./service"]\n'
        ),
    }


def _java_service_files(
    *,
    service_name: str,
    run_id: str,
    target_class: str,
    contract_obj: Mapping[str, Any] | None,
) -> dict[str, str]:
    operations = _contract_operations(contract_obj)
    package_suffix = re.sub(r"[^a-z0-9]+", "_", service_name.lower()).strip("_") or "generated"
    package_name = f"com.akc.generated.{package_suffix}"
    package_path = package_name.replace(".", "/")
    class_name = _pascal_identifier(service_name)
    application_name = f"{class_name}Application"
    controller_name = f"{class_name}Controller"
    annotation_map = {
        "get": "@GetMapping",
        "post": "@PostMapping",
        "put": "@PutMapping",
        "patch": "@PatchMapping",
        "delete": "@DeleteMapping",
    }
    method_lines = []
    for op in operations:
        operation_id = str(op["operationId"])
        handler_name = _camel_identifier(operation_id)
        annotation = annotation_map.get(str(op["method"]).lower(), "@GetMapping")
        method_lines.append(
            f"    {annotation}({json.dumps(str(op['path']))})\n"
            f"    public Map<String, Object> {handler_name}() {{\n"
            "        return Map.of(\n"
            f'            "status", {json.dumps("ok")},\n'
            f'            "service", {json.dumps(service_name)},\n'
            f'            "run_id", {json.dumps(run_id)},\n'
            f'            "target_class", {json.dumps(target_class)},\n'
            f'            "operation_id", {json.dumps(operation_id)}\n'
            "        );\n"
            "    }\n\n"
        )
    application_java = (
        f"package {package_name};\n\n"
        "import org.springframework.boot.SpringApplication;\n"
        "import org.springframework.boot.autoconfigure.SpringBootApplication;\n\n"
        "@SpringBootApplication\n"
        f"public class {application_name} {{\n"
        "    public static void main(String[] args) {\n"
        f"        SpringApplication.run({application_name}.class, args);\n"
        "    }\n"
        "}\n"
    )
    controller_java = (
        f"package {package_name};\n\n"
        "import java.util.Map;\n"
        "import org.springframework.web.bind.annotation.DeleteMapping;\n"
        "import org.springframework.web.bind.annotation.GetMapping;\n"
        "import org.springframework.web.bind.annotation.PatchMapping;\n"
        "import org.springframework.web.bind.annotation.PostMapping;\n"
        "import org.springframework.web.bind.annotation.PutMapping;\n"
        "import org.springframework.web.bind.annotation.RestController;\n\n"
        "@RestController\n"
        f"public class {controller_name} {{\n\n" + "".join(method_lines) + "}\n"
    )
    pom_xml = (
        '<project xmlns="http://maven.apache.org/POM/4.0.0" '
        'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" '
        'xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 '
        'https://maven.apache.org/xsd/maven-4.0.0.xsd">\n'
        "  <modelVersion>4.0.0</modelVersion>\n"
        "  <parent>\n"
        "    <groupId>org.springframework.boot</groupId>\n"
        "    <artifactId>spring-boot-starter-parent</artifactId>\n"
        "    <version>3.3.2</version>\n"
        "    <relativePath/>\n"
        "  </parent>\n"
        f"  <groupId>{package_name}</groupId>\n"
        f"  <artifactId>{service_name}</artifactId>\n"
        "  <version>0.1.0</version>\n"
        "  <properties>\n"
        "    <java.version>21</java.version>\n"
        "  </properties>\n"
        "  <dependencies>\n"
        "    <dependency>\n"
        "      <groupId>org.springframework.boot</groupId>\n"
        "      <artifactId>spring-boot-starter-web</artifactId>\n"
        "    </dependency>\n"
        "  </dependencies>\n"
        "  <build>\n"
        "    <plugins>\n"
        "      <plugin>\n"
        "        <groupId>org.springframework.boot</groupId>\n"
        "        <artifactId>spring-boot-maven-plugin</artifactId>\n"
        "      </plugin>\n"
        "    </plugins>\n"
        "  </build>\n"
        "</project>\n"
    )
    return {
        "pom.xml": pom_xml,
        f"src/main/java/{package_path}/{application_name}.java": application_java,
        f"src/main/java/{package_path}/{controller_name}.java": controller_java,
        "src/main/resources/application.properties": "server.port=${PORT:8080}\n",
        "openapi.contract.json": json.dumps(dict(contract_obj or {}), indent=2, sort_keys=True) + "\n",
        "Dockerfile": (
            "FROM maven:3.9.8-eclipse-temurin-21 AS build\n"
            "WORKDIR /app\n"
            "COPY pom.xml ./pom.xml\n"
            "COPY src ./src\n"
            "RUN mvn -q -DskipTests package\n\n"
            "FROM eclipse-temurin:21-jre\n"
            "WORKDIR /app\n"
            "COPY --from=build /app/target/*.jar ./app.jar\n"
            "COPY openapi.contract.json ./openapi.contract.json\n"
            'CMD ["java", "-jar", "/app/app.jar"]\n'
        ),
    }


def _builtin_service_files(
    *,
    runtime_profile: str,
    service_name: str,
    run_id: str,
    target_class: str,
    contract_obj: Mapping[str, Any] | None,
) -> dict[str, str]:
    if runtime_profile == "typescript_node":
        return _typescript_service_files(
            service_name=service_name,
            run_id=run_id,
            target_class=target_class,
            contract_obj=contract_obj,
        )
    if runtime_profile == "python_fastapi":
        return _python_service_files(
            service_name=service_name,
            run_id=run_id,
            target_class=target_class,
            contract_obj=contract_obj,
        )
    if runtime_profile == "go":
        return _go_service_files(
            service_name=service_name,
            run_id=run_id,
            target_class=target_class,
            contract_obj=contract_obj,
        )
    if runtime_profile == "rust":
        return _rust_service_files(
            service_name=service_name,
            run_id=run_id,
            target_class=target_class,
            contract_obj=contract_obj,
        )
    if runtime_profile == "java":
        return _java_service_files(
            service_name=service_name,
            run_id=run_id,
            target_class=target_class,
            contract_obj=contract_obj,
        )
    return {}


def _builtin_service_entry_rel(*, runtime_profile: str, rel_dir: str) -> str | None:
    if runtime_profile == "typescript_node":
        return f"{rel_dir}/src/index.js"
    if runtime_profile == "python_fastapi":
        return f"{rel_dir}/app.py"
    if runtime_profile == "go":
        return f"{rel_dir}/main.go"
    if runtime_profile == "rust":
        return f"{rel_dir}/src/main.rs"
    if runtime_profile == "java":
        return f"{rel_dir}/pom.xml"
    return None


def _target_rel_dir(*, target_class: str, name: str) -> str:
    if target_class == "worker":
        return f"workers/{_slug(name)}"
    if target_class in {"backend_service", "integration"}:
        return f"services/{_slug(name)}"
    if target_class in {"web_app", "mobile_client"}:
        return "apps/universal"
    return ""


def _practical_backend_resources(
    practical_backend_handoff: PracticalBackendHandoff | None,
) -> dict[str, Mapping[str, Any]]:
    if practical_backend_handoff is None:
        return {}
    raw = practical_backend_handoff.backend_ir.get("resources")
    if not isinstance(raw, list):
        return {}
    out: dict[str, Mapping[str, Any]] = {}
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        target_id = row.get("target_id")
        if isinstance(target_id, str) and target_id.strip():
            out[target_id.strip()] = row
    return out


def _practical_api_contracts(
    practical_backend_handoff: PracticalBackendHandoff | None,
) -> dict[str, Mapping[str, Any]]:
    if practical_backend_handoff is None:
        return {}
    return {
        str(target_id): contract
        for target_id, contract in practical_backend_handoff.api_contracts.items()
        if isinstance(target_id, str) and target_id.strip() and isinstance(contract, Mapping)
    }


def _practical_api_contract_refs(
    practical_backend_handoff: PracticalBackendHandoff | None,
) -> dict[str, Mapping[str, Any]]:
    if practical_backend_handoff is None:
        return {}
    raw = practical_backend_handoff.backend_api_contract_index.get("contract_refs")
    if not isinstance(raw, list):
        return {}
    out: dict[str, Mapping[str, Any]] = {}
    for row in raw:
        if not isinstance(row, Mapping):
            continue
        target_id = row.get("target_id")
        if isinstance(target_id, str) and target_id.strip():
            out[target_id.strip()] = row
    return out


def _practical_backend_manifest_meta(
    *,
    run_id: str,
    practical_backend_handoff: PracticalBackendHandoff | None,
) -> dict[str, Any] | None:
    if practical_backend_handoff is None:
        return None
    refs = {
        "backend_generation_profile_ref": {
            "path": f".akc/backend/{run_id}.generation_profile.json",
            "fingerprint": stable_json_fingerprint(practical_backend_handoff.backend_generation_profile),
        },
        "backend_ir_ref": {
            "path": f".akc/backend/{run_id}.backend_ir.json",
            "fingerprint": stable_json_fingerprint(practical_backend_handoff.backend_ir),
        },
        "backend_api_contract_index_ref": {
            "path": f".akc/backend/{run_id}.backend_api_contract_index.json",
            "fingerprint": stable_json_fingerprint(practical_backend_handoff.backend_api_contract_index),
        },
        "implementation_plan_ref": {
            "path": f".akc/backend/{run_id}.implementation_plan.json",
            "fingerprint": stable_json_fingerprint(practical_backend_handoff.implementation_plan),
        },
        "implementation_acceptance_contract_ref": {
            "path": f".akc/backend/{run_id}.implementation_acceptance_contract.json",
            "fingerprint": stable_json_fingerprint(practical_backend_handoff.implementation_acceptance_contract),
        },
        "practical_generation_result_ref": {
            "path": f".akc/backend/{run_id}.practical_generation_result.json",
            "fingerprint": stable_json_fingerprint(practical_backend_handoff.practical_generation_result),
        },
        "runtime_plugin_decision_ref": {
            "path": f".akc/backend/{run_id}.runtime_plugin_decision.json",
            "fingerprint": stable_json_fingerprint(practical_backend_handoff.runtime_plugin_decision),
        },
    }
    return {
        "selected_runtime_plugin": practical_backend_handoff.runtime_plugin_decision.get("plugin_id"),
        "selected_runtime_maturity": practical_backend_handoff.runtime_plugin_decision.get("maturity"),
        "plugin_source": practical_backend_handoff.runtime_plugin_decision.get("plugin_source"),
        "materializer_kind": practical_backend_handoff.runtime_plugin_decision.get("materializer_kind"),
        "supports_authoritative_workspace": practical_backend_handoff.runtime_plugin_decision.get(
            "supports_authoritative_workspace"
        ),
        "availability": practical_backend_handoff.runtime_plugin_decision.get("availability"),
        "requested_by_policy": practical_backend_handoff.runtime_plugin_decision.get("requested_by_policy"),
        "blocked_stage": practical_backend_handoff.runtime_plugin_decision.get("blocked_stage"),
        "adoption_readiness": practical_backend_handoff.adoption_readiness,
        "adoption_confidence_score": practical_backend_handoff.backend_generation_profile.get(
            "adoption_confidence_score"
        ),
        "status": practical_backend_handoff.practical_status,
        "blocked_reasons": list(practical_backend_handoff.blocked_reasons),
        "api_contract_refs": list(
            cast(Sequence[Any], practical_backend_handoff.backend_api_contract_index.get("contract_refs") or [])
        ),
        **refs,
    }


def _requested_runtime_plugin(
    *,
    project_root: Path | None,
    practical_backend_handoff: PracticalBackendHandoff | None,
) -> str:
    if practical_backend_handoff is not None:
        raw = practical_backend_handoff.runtime_plugin_decision.get("plugin_id")
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return infer_backend_runtime_profile(project_root=project_root)


def _guess_media_type(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".json"}:
        return "application/json; charset=utf-8"
    if suffix in {".js", ".mjs", ".cjs"}:
        return "application/javascript; charset=utf-8"
    if suffix in {".ts", ".tsx"}:
        return "application/typescript; charset=utf-8"
    if suffix in {".py", ".sh", ".md", ".txt", ".toml", ".yaml", ".yml", ".env"}:
        return "text/plain; charset=utf-8"
    return "application/octet-stream"


def _external_workspace_files(root: Path) -> tuple[list[tuple[str, bytes]], str | None]:
    out: list[tuple[str, bytes]] = []
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            return [], "external_plugin_emitted_symlink"
        if not path.is_file():
            continue
        try:
            rel = path.relative_to(root).as_posix()
        except ValueError:
            return [], "external_plugin_output_escaped_root"
        resolved = path.resolve()
        try:
            resolved.relative_to(root.resolve())
        except ValueError:
            return [], "external_plugin_output_escaped_root"
        out.append((rel, path.read_bytes()))
    return out, None


def _validate_external_materialization_response(
    *,
    selected_plugin_id: str,
    raw: object,
) -> tuple[dict[str, Any] | None, str | None]:
    if not isinstance(raw, Mapping):
        return None, "external_plugin_invalid_response:not_object"
    obj = dict(cast(dict[str, Any], raw))
    for key in (
        "status",
        "requested_runtime_plugin",
        "materialized_runtime_profile",
        "artifact_role",
        "generation_mode",
    ):
        value = obj.get(key)
        if not isinstance(value, str) or not value.strip():
            return None, f"external_plugin_invalid_response:missing_{key}"
    if str(obj.get("requested_runtime_plugin")).strip() != selected_plugin_id:
        return None, "external_plugin_invalid_response:requested_runtime_plugin_mismatch"
    targets = obj.get("targets")
    toolchain = obj.get("toolchain")
    if not isinstance(targets, list):
        return None, "external_plugin_invalid_response:missing_targets"
    if not isinstance(toolchain, Mapping):
        return None, "external_plugin_invalid_response:missing_toolchain"
    return obj, None


def _materialize_external_backend_workspace(
    *,
    run_id: str,
    ir_document: IRDocument,
    project_root: Path | None,
    selected_plugin_id: str,
    delivery_plan_obj: Mapping[str, Any] | None,
    practical_backend_handoff: PracticalBackendHandoff | None,
    command: Sequence[str],
) -> tuple[dict[str, Any] | None, list[tuple[str, bytes]] | None, str | None]:
    with tempfile.TemporaryDirectory(prefix=f"akc-exec-plugin-{_slug(selected_plugin_id)}-") as tmp_dir:
        output_root = Path(tmp_dir).resolve()
        request_obj = {
            "run_id": run_id,
            "tenant_id": ir_document.tenant_id,
            "repo_id": ir_document.repo_id,
            "project_root": str(project_root.resolve()) if project_root is not None else None,
            "requested_runtime_plugin": selected_plugin_id,
            "selected_runtime_plugin": selected_plugin_id,
            "output_root": str(output_root),
            "project_profile": _project_profile(project_root),
            "delivery_plan": dict(cast(dict[str, Any], delivery_plan_obj or {})),
            "backend_generation_profile": (
                dict(practical_backend_handoff.backend_generation_profile)
                if practical_backend_handoff is not None
                else {}
            ),
            "backend_ir": dict(practical_backend_handoff.backend_ir) if practical_backend_handoff is not None else {},
            "api_contracts": (
                {str(k): dict(v) for k, v in practical_backend_handoff.api_contracts.items()}
                if practical_backend_handoff is not None
                else {}
            ),
        }
        proc = subprocess.run(
            [str(x) for x in command],
            input=json.dumps(request_obj, sort_keys=True),
            capture_output=True,
            text=True,
            check=False,
            cwd=str(project_root.resolve()) if project_root is not None else None,
        )
        if proc.returncode != 0:
            return (
                None,
                None,
                (proc.stderr.strip() or proc.stdout.strip() or f"external_plugin_exit_code:{proc.returncode}"),
            )
        try:
            response_raw = json.loads(proc.stdout or "{}")
        except json.JSONDecodeError:
            return None, None, "external_plugin_invalid_response:not_json"
        response_obj, err = _validate_external_materialization_response(
            selected_plugin_id=selected_plugin_id,
            raw=response_raw,
        )
        if err is not None:
            return None, None, err
        files, files_err = _external_workspace_files(output_root)
        if files_err is not None:
            return None, None, files_err
        return response_obj, files, None


def _frontend_base_url_env_var(*, target_name: str) -> str:
    return f"EXPO_PUBLIC_{_slug(target_name).replace('-', '_').upper()}_BASE_URL"


def _target_client_module(*, target: Mapping[str, JSONValue], contract_obj: Mapping[str, Any] | None) -> str:
    operations = _contract_operations(contract_obj)
    health_path = next(
        (str(op.get("path", "/healthz")) for op in operations if str(op.get("operationId", "")).endswith("healthz")),
        "/healthz",
    )
    return (
        "const joinUrl = (baseUrl, path) => {\n"
        "  const base = String(baseUrl || '').replace(/\\/+$/, '');\n"
        "  const suffix = String(path || '/');\n"
        "  const normalized = suffix.startsWith('/') ? suffix : `/${suffix}`;\n"
        "  return `${base}${normalized}`;\n"
        "};\n\n"
        f"export const contractFingerprint = {json.dumps(stable_json_fingerprint(dict(contract_obj or {})))};\n"
        f"export const operationIds = {json.dumps([str(op['operationId']) for op in operations])};\n"
        f"export const healthPath = {json.dumps(health_path)};\n\n"
        "const readJson = async (response) => {\n"
        "  try {\n"
        "    return await response.json();\n"
        "  } catch {\n"
        "    return null;\n"
        "  }\n"
        "};\n\n"
        "export async function fetchHealth(baseUrl, fetchImpl = fetch) {\n"
        "  const url = joinUrl(baseUrl, healthPath);\n"
        "  const response = await fetchImpl(url);\n"
        "  return { response, payload: await readJson(response), url };\n"
        "}\n"
    )


def _relative_file_map(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not root.is_dir():
        return out
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        out[path.relative_to(root).as_posix()] = path.read_text(encoding="utf-8", errors="replace")
    return out


def _external_client_binding_files(
    *,
    target_id: str,
    contract_obj: Mapping[str, Any] | None,
    generator_cmd: Sequence[str] | None,
) -> tuple[dict[str, str], dict[str, Any]]:
    if not generator_cmd or not isinstance(contract_obj, Mapping):
        return {}, {"mode": "internal_fallback", "generated_files": []}

    with tempfile.TemporaryDirectory(prefix=f"akc-openapi-{_slug(target_id)}-") as tmp_dir:
        root = Path(tmp_dir)
        spec_path = root / "openapi.json"
        out_dir = root / "generated"
        spec_path.write_text(json.dumps(dict(contract_obj), indent=2, sort_keys=True) + "\n", encoding="utf-8")
        argv = [
            *[str(x) for x in generator_cmd],
            "generate",
            "-g",
            "typescript-fetch",
            "-i",
            str(spec_path),
            "-o",
            str(out_dir),
            "--additional-properties",
            ",".join(
                [
                    "supportsES6=true",
                    "useSingleRequestParameter=true",
                    "withInterfaces=true",
                    "prefixParameterInterfaces=true",
                    "stringEnums=true",
                ]
            ),
        ]
        proc = subprocess.run(argv, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            return {}, {
                "mode": "internal_fallback",
                "command": argv,
                "error": proc.stderr.strip() or proc.stdout.strip() or f"exit_code={proc.returncode}",
                "generated_files": [],
            }
        files = _relative_file_map(out_dir)
        if not files:
            return {}, {
                "mode": "internal_fallback",
                "command": argv,
                "error": "external_generator_emitted_no_files",
                "generated_files": [],
            }
        return files, {
            "mode": "external_openapi_generator",
            "command": argv,
            "generated_files": sorted(files),
        }


def _generated_backend_runtime_module(
    *,
    frontend_targets: list[dict[str, JSONValue]],
    operation_rows: list[dict[str, Any]],
    feature_modules: list[dict[str, Any]],
    frontend_integration_summary: Mapping[str, Any],
) -> str:
    rows: list[str] = []
    for target in frontend_targets:
        env_var = str(target["frontend_base_url_env_var"])
        global_fallback = str(target["frontend_global_base_url_env_var"])
        rows.append(
            "  {\n"
            f"    targetId: {json.dumps(str(target['target_id']))},\n"
            f"    name: {json.dumps(str(target['name']))},\n"
            f"    targetClass: {json.dumps(str(target['target_class']))},\n"
            f"    resourcePath: {json.dumps(str(target.get('backend_resource_path') or ''))},\n"
            f"    healthPath: {json.dumps(str(target.get('health_path') or '/healthz'))},\n"
            f"    contractDepth: {json.dumps(str(target.get('contract_depth') or 'minimal'))},\n"
            f"    featureGroups: {json.dumps(list(cast(Sequence[Any], target.get('api_feature_groups') or [])))},\n"
            f"    authModes: {json.dumps(list(cast(Sequence[Any], target.get('api_auth_modes') or [])))},\n"
            f"    baseUrlEnvVar: {json.dumps(env_var)},\n"
            "    resolveBaseUrl() {\n"
            f"      return process.env.{env_var} || process.env.{global_fallback} || null;\n"
            "    },\n"
            "  },"
        )
    target_rows = "\n".join(rows)
    return (
        "import { Platform } from 'react-native';\n"
        "import { QueryClient } from '@tanstack/react-query';\n"
        "import { authHeadersForOperation } from './auth';\n\n"
        f"export const operationRegistry = {json.dumps(operation_rows, indent=2)};\n\n"
        f"export const featureModules = {json.dumps(feature_modules, indent=2)};\n\n"
        f"export const frontendIntegrationSummary = {json.dumps(dict(frontend_integration_summary), indent=2)};\n\n"
        "export const backendTargets = [\n"
        f"{target_rows}\n"
        "];\n\n"
        "export const QUERY_CACHE_MAX_AGE_MS = 1000 * 60 * 60 * 24;\n"
        "const QUERY_CACHE_KEY = 'akc-query-cache-v1';\n\n"
        "const createMemoryStore = () => {\n"
        "  let value = null;\n"
        "  return {\n"
        "    async getItem() {\n"
        "      return value;\n"
        "    },\n"
        "    async setItem(_key, nextValue) {\n"
        "      value = nextValue;\n"
        "    },\n"
        "    async removeItem() {\n"
        "      value = null;\n"
        "    },\n"
        "  };\n"
        "};\n\n"
        "const memoryStore = createMemoryStore();\n"
        "const webStore =\n"
        "  Platform.OS === 'web' && typeof window !== 'undefined' && window.localStorage\n"
        "    ? {\n"
        "        async getItem(key) {\n"
        "          return window.localStorage.getItem(key);\n"
        "        },\n"
        "        async setItem(key, value) {\n"
        "          window.localStorage.setItem(key, value);\n"
        "        },\n"
        "        async removeItem(key) {\n"
        "          window.localStorage.removeItem(key);\n"
        "        },\n"
        "      }\n"
        "    : memoryStore;\n\n"
        "const cacheStore = Platform.OS === 'web' ? webStore : memoryStore;\n\n"
        "export const queryClient = new QueryClient({\n"
        "  defaultOptions: {\n"
        "    queries: {\n"
        "      gcTime: QUERY_CACHE_MAX_AGE_MS,\n"
        "      retry: 1,\n"
        "    },\n"
        "  },\n"
        "});\n\n"
        "export const queryPersister = {\n"
        "  async persistClient(client) {\n"
        "    await cacheStore.setItem(QUERY_CACHE_KEY, JSON.stringify(client));\n"
        "  },\n"
        "  async restoreClient() {\n"
        "    const raw = await cacheStore.getItem(QUERY_CACHE_KEY);\n"
        "    if (!raw) {\n"
        "      return undefined;\n"
        "    }\n"
        "    try {\n"
        "      return JSON.parse(raw);\n"
        "    } catch {\n"
        "      return undefined;\n"
        "    }\n"
        "  },\n"
        "  async removeClient() {\n"
        "    await cacheStore.removeItem(QUERY_CACHE_KEY);\n"
        "  },\n"
        "};\n\n"
        "const operationMap = new Map(operationRegistry.map((row) => [`${row.targetId}:${row.operationId}`, row]));\n\n"
        "const joinUrl = (baseUrl, path) => {\n"
        "  const base = String(baseUrl || '').replace(/\\/+$/, '');\n"
        "  const suffix = String(path || '/');\n"
        "  const normalized = suffix.startsWith('/') ? suffix : `/${suffix}`;\n"
        "  return `${base}${normalized}`;\n"
        "};\n\n"
        "const fillPathParams = (path, params) =>\n"
        "  String(path || '/').replace(\n"
        "    /\\{([^{}]+)\\}/g,\n"
        "    (_match, key) => encodeURIComponent(String((params || {})[key] ?? '')),\n"
        "  );\n\n"
        "const appendQueryParams = (url, query) => {\n"
        "  const pairs = [];\n"
        "  for (const [key, value] of Object.entries(query || {})) {\n"
        "    if (value === undefined || value === null) {\n"
        "      continue;\n"
        "    }\n"
        "    if (Array.isArray(value)) {\n"
        "      for (const item of value) {\n"
        "        pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(item))}`);\n"
        "      }\n"
        "      continue;\n"
        "    }\n"
        "    pairs.push(`${encodeURIComponent(key)}=${encodeURIComponent(String(value))}`);\n"
        "  }\n"
        "  if (pairs.length === 0) {\n"
        "    return url;\n"
        "  }\n"
        "  return `${url}${url.includes('?') ? '&' : '?'}${pairs.join('&')}`;\n"
        "};\n\n"
        "const readPayload = async (response) => {\n"
        "  const text = await response.text();\n"
        "  if (!text) {\n"
        "    return null;\n"
        "  }\n"
        "  try {\n"
        "    return JSON.parse(text);\n"
        "  } catch {\n"
        "    return text;\n"
        "  }\n"
        "};\n\n"
        "export function getBackendTarget(targetId) {\n"
        "  return backendTargets.find((row) => row.targetId === targetId) || null;\n"
        "}\n\n"
        "export function getOperation(targetId, operationId) {\n"
        "  return operationMap.get(`${targetId}:${operationId}`) || null;\n"
        "}\n\n"
        "export function queryKeyForOperation(operation, args = {}) {\n"
        "  return ['backend', operation.targetId, operation.featureGroup, operation.operationId, args];\n"
        "}\n\n"
        "export function featureQueryKey(targetId, featureGroup) {\n"
        "  return ['backend', targetId, featureGroup];\n"
        "}\n\n"
        "export function normalizeHttpError({ operation, target, response, payload, url }) {\n"
        "  const error = new Error(\n"
        "    payload && typeof payload === 'object' && payload.error\n"
        "      ? String(payload.error)\n"
        "      : `HTTP ${response.status}`,\n"
        "  );\n"
        "  error.name = 'BackendRequestError';\n"
        "  error.status = response.status;\n"
        "  error.payload = payload;\n"
        "  error.url = url;\n"
        "  error.operation = operation;\n"
        "  error.target = target;\n"
        "  return error;\n"
        "}\n\n"
        "export async function executeOperation(targetId, operationId, args = {}, fetchImpl = fetch) {\n"
        "  const target = getBackendTarget(targetId);\n"
        "  const operation = getOperation(targetId, operationId);\n"
        "  if (!target || !operation) {\n"
        "    throw new Error(`Unknown backend operation ${targetId}:${operationId}`);\n"
        "  }\n"
        "  const baseUrl = target.resolveBaseUrl();\n"
        "  if (!baseUrl) {\n"
        "    throw new Error(`Set ${target.baseUrlEnvVar} or EXPO_PUBLIC_API_BASE_URL`);\n"
        "  }\n"
        "  const path = fillPathParams(operation.path, args.path || args.params || {});\n"
        "  const url = appendQueryParams(joinUrl(baseUrl, path), args.query || {});\n"
        "  const authHeaders = await authHeadersForOperation(operation);\n"
        "  const headers = {\n"
        "    Accept: 'application/json',\n"
        "    ...authHeaders,\n"
        "    ...(args.headers || {}),\n"
        "  };\n"
        "  let body;\n"
        "  if (operation.hasRequestBody) {\n"
        "    body = args.body !== undefined ? args.body : args.data !== undefined ? args.data : null;\n"
        "    headers['Content-Type'] = 'application/json';\n"
        "  }\n"
        "  const response = await fetchImpl(url, {\n"
        "    method: operation.method,\n"
        "    headers,\n"
        "    body: body === undefined || body === null ? undefined : JSON.stringify(body),\n"
        "    signal: args.signal,\n"
        "    ...(args.init || {}),\n"
        "  });\n"
        "  const payload = await readPayload(response);\n"
        "  if (!response.ok) {\n"
        "    throw normalizeHttpError({ operation, target, response, payload, url });\n"
        "  }\n"
        "  return payload;\n"
        "}\n\n"
        "export async function invalidateOperationQueries(queryClientInstance, operation) {\n"
        "  await queryClientInstance.invalidateQueries({\n"
        "    queryKey: ['backend', operation.targetId, operation.featureGroup, operation.operationId],\n"
        "  });\n"
        "  await queryClientInstance.invalidateQueries({\n"
        "    queryKey: ['backend', operation.targetId, operation.featureGroup],\n"
        "  });\n"
        "}\n"
    )


def _generated_backend_auth_module() -> str:
    return (
        "import { createContext, useContext, useEffect, useMemo, useState } from 'react';\n"
        "import { Platform } from 'react-native';\n"
        "import * as SecureStore from 'expo-secure-store';\n\n"
        "const AUTH_STORAGE_KEY = 'akc-auth-credentials-v1';\n"
        "const defaultCredentials = { bearerToken: null, apiKeys: {} };\n"
        "const clone = (value) => JSON.parse(JSON.stringify(value));\n"
        "let currentCredentials = clone(defaultCredentials);\n"
        "let bootstrapPromise = null;\n"
        "let webMemoryCredentials = null;\n\n"
        "async function readStoredCredentials() {\n"
        "  if (Platform.OS === 'web') {\n"
        "    return clone(webMemoryCredentials || defaultCredentials);\n"
        "  }\n"
        "  try {\n"
        "    const raw = await SecureStore.getItemAsync(AUTH_STORAGE_KEY);\n"
        "    if (!raw) {\n"
        "      return clone(defaultCredentials);\n"
        "    }\n"
        "    return { ...clone(defaultCredentials), ...JSON.parse(raw) };\n"
        "  } catch {\n"
        "    return clone(defaultCredentials);\n"
        "  }\n"
        "}\n\n"
        "async function writeStoredCredentials(nextValue) {\n"
        "  currentCredentials = { ...clone(defaultCredentials), ...clone(nextValue) };\n"
        "  if (Platform.OS === 'web') {\n"
        "    webMemoryCredentials = clone(currentCredentials);\n"
        "    return clone(currentCredentials);\n"
        "  }\n"
        "  try {\n"
        "    await SecureStore.setItemAsync(AUTH_STORAGE_KEY, JSON.stringify(currentCredentials));\n"
        "  } catch {\n"
        "    currentCredentials = clone(defaultCredentials);\n"
        "  }\n"
        "  return clone(currentCredentials);\n"
        "}\n\n"
        "export async function ensureAuthStateLoaded() {\n"
        "  if (!bootstrapPromise) {\n"
        "    bootstrapPromise = readStoredCredentials().then((value) => {\n"
        "      currentCredentials = clone(value);\n"
        "      return clone(currentCredentials);\n"
        "    });\n"
        "  }\n"
        "  return bootstrapPromise;\n"
        "}\n\n"
        "export function getAuthSnapshot() {\n"
        "  return clone(currentCredentials);\n"
        "}\n\n"
        "export async function storeBearerToken(token) {\n"
        "  const base = await ensureAuthStateLoaded();\n"
        "  return writeStoredCredentials({ ...base, bearerToken: token || null });\n"
        "}\n\n"
        "export async function storeApiKey(name, value) {\n"
        "  const base = await ensureAuthStateLoaded();\n"
        "  const apiKeys = { ...(base.apiKeys || {}) };\n"
        "  if (value) {\n"
        "    apiKeys[name] = value;\n"
        "  } else {\n"
        "    delete apiKeys[name];\n"
        "  }\n"
        "  return writeStoredCredentials({ ...base, apiKeys });\n"
        "}\n\n"
        "export async function clearAuthCredentials() {\n"
        "  return writeStoredCredentials(defaultCredentials);\n"
        "}\n\n"
        "export async function authHeadersForOperation(operation) {\n"
        "  const credentials = await ensureAuthStateLoaded();\n"
        "  const headers = {};\n"
        "  if (operation.authMode === 'bearer' && credentials.bearerToken) {\n"
        "    headers.Authorization = `Bearer ${credentials.bearerToken}`;\n"
        "  }\n"
        "  if (operation.authMode === 'apiKey') {\n"
        "    const headerName = Array.isArray(operation.headerParamNames) && operation.headerParamNames.length > 0\n"
        "      ? operation.headerParamNames[0]\n"
        "      : 'X-API-Key';\n"
        "    const apiKey = credentials.apiKeys ? credentials.apiKeys[headerName] : null;\n"
        "    if (apiKey) {\n"
        "      headers[headerName] = apiKey;\n"
        "    }\n"
        "  }\n"
        "  return headers;\n"
        "}\n\n"
        "const AuthContext = createContext({\n"
        "  loading: true,\n"
        "  credentials: clone(defaultCredentials),\n"
        "  setBearerToken: async () => clone(defaultCredentials),\n"
        "  setApiKey: async () => clone(defaultCredentials),\n"
        "  clear: async () => clone(defaultCredentials),\n"
        "});\n\n"
        "export function AuthProvider({ children }) {\n"
        "  const [loading, setLoading] = useState(true);\n"
        "  const [credentials, setCredentials] = useState(clone(defaultCredentials));\n\n"
        "  useEffect(() => {\n"
        "    let active = true;\n"
        "    ensureAuthStateLoaded().then((value) => {\n"
        "      if (active) {\n"
        "        setCredentials(value);\n"
        "        setLoading(false);\n"
        "      }\n"
        "    });\n"
        "    return () => {\n"
        "      active = false;\n"
        "    };\n"
        "  }, []);\n\n"
        "  const value = useMemo(\n"
        "    () => ({\n"
        "      loading,\n"
        "      credentials,\n"
        "      setBearerToken: async (token) => {\n"
        "        const nextValue = await storeBearerToken(token);\n"
        "        setCredentials(nextValue);\n"
        "        return nextValue;\n"
        "      },\n"
        "      setApiKey: async (name, apiKey) => {\n"
        "        const nextValue = await storeApiKey(name, apiKey);\n"
        "        setCredentials(nextValue);\n"
        "        return nextValue;\n"
        "      },\n"
        "      clear: async () => {\n"
        "        const nextValue = await clearAuthCredentials();\n"
        "        setCredentials(nextValue);\n"
        "        return nextValue;\n"
        "      },\n"
        "    }),\n"
        "    [loading, credentials],\n"
        "  );\n\n"
        "  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;\n"
        "}\n\n"
        "export function useAuth() {\n"
        "  return useContext(AuthContext);\n"
        "}\n"
    )


def _generated_backend_diagnostics_module() -> str:
    return (
        "import { useQuery } from '@tanstack/react-query';\n"
        "import { backendTargets, getBackendTarget } from './runtime';\n\n"
        "const joinUrl = (baseUrl, path) => {\n"
        "  const base = String(baseUrl || '').replace(/\\/+$/, '');\n"
        "  const suffix = String(path || '/healthz');\n"
        "  const normalized = suffix.startsWith('/') ? suffix : `/${suffix}`;\n"
        "  return `${base}${normalized}`;\n"
        "};\n\n"
        "const readPayload = async (response) => {\n"
        "  try {\n"
        "    return await response.json();\n"
        "  } catch {\n"
        "    return null;\n"
        "  }\n"
        "};\n\n"
        "export async function fetchBackendHealth(target, fetchImpl = fetch) {\n"
        "  const resolvedTarget = typeof target === 'string' ? getBackendTarget(target) : target;\n"
        "  if (!resolvedTarget) {\n"
        "    return { ok: false, configured: false, targetId: null, error: 'Unknown backend target' };\n"
        "  }\n"
        "  const baseUrl = resolvedTarget.resolveBaseUrl();\n"
        "  if (!baseUrl) {\n"
        "    return {\n"
        "      targetId: resolvedTarget.targetId,\n"
        "      configured: false,\n"
        "      ok: false,\n"
        "      status: null,\n"
        "      url: null,\n"
        "      payload: null,\n"
        "      error: `Set ${resolvedTarget.baseUrlEnvVar} or EXPO_PUBLIC_API_BASE_URL`,\n"
        "    };\n"
        "  }\n"
        "  const url = joinUrl(baseUrl, resolvedTarget.healthPath);\n"
        "  try {\n"
        "    const response = await fetchImpl(url);\n"
        "    const payload = await readPayload(response);\n"
        "    return {\n"
        "      targetId: resolvedTarget.targetId,\n"
        "      configured: true,\n"
        "      ok: response.ok,\n"
        "      status: response.status,\n"
        "      url,\n"
        "      payload,\n"
        "      error: response.ok ? null : `HTTP ${response.status}`,\n"
        "    };\n"
        "  } catch (error) {\n"
        "    return {\n"
        "      targetId: resolvedTarget.targetId,\n"
        "      configured: true,\n"
        "      ok: false,\n"
        "      status: null,\n"
        "      url,\n"
        "      payload: null,\n"
        "      error: error instanceof Error ? error.message : String(error),\n"
        "    };\n"
        "  }\n"
        "}\n\n"
        "export async function fetchAllBackendHealth(fetchImpl = fetch) {\n"
        "  return Promise.all(backendTargets.map((target) => fetchBackendHealth(target, fetchImpl)));\n"
        "}\n\n"
        "export function backendDiagnosticsQueryOptions(options = {}) {\n"
        "  const { fetchImpl, ...queryOptions } = options;\n"
        "  return {\n"
        "    queryKey: ['backend', 'diagnostics', 'health'],\n"
        "    queryFn: () => fetchAllBackendHealth(fetchImpl),\n"
        "    staleTime: 30000,\n"
        "    ...queryOptions,\n"
        "  };\n"
        "}\n\n"
        "export function useBackendDiagnosticsQuery(options = {}) {\n"
        "  return useQuery(backendDiagnosticsQueryOptions(options));\n"
        "}\n"
    )


def _generated_feature_module(*, module_meta: Mapping[str, Any], module_operations: Sequence[Mapping[str, Any]]) -> str:
    lines = [
        "import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query';\n",
        (
            "import { executeOperation, getOperation, invalidateOperationQueries, "
            "queryKeyForOperation } from '../runtime';\n\n"
        ),
        f"export const featureModule = {json.dumps(dict(module_meta), indent=2)};\n\n",
        "export const featureOperations = [\n",
    ]
    for row in module_operations:
        lines.append(f"  getOperation({json.dumps(str(row['targetId']))}, {json.dumps(str(row['operationId']))}),\n")
    lines.append("].filter(Boolean);\n\n")
    for row in module_operations:
        operation_id = str(row["operationId"])
        op_const = f"{_camel_identifier(operation_id)}Operation"
        lines.append(
            f"const {op_const} = getOperation({json.dumps(str(row['targetId']))}, {json.dumps(operation_id)});\n"
        )
        pascal = _pascal_identifier(operation_id)
        if str(row["cacheKind"]) == "query":
            lines.append(
                f"export function {op_const}QueryKey(args = {{}}) {{\n"
                f"  return queryKeyForOperation({op_const}, args);\n"
                "}\n\n"
            )
            lines.append(
                f"export function {op_const}QueryOptions(args = {{}}, options = {{}}) {{\n"
                "  const { fetchImpl, ...queryOptions } = options;\n"
                "  return {\n"
                f"    queryKey: {op_const}QueryKey(args),\n"
                "    queryFn: (context) =>\n"
                f"      executeOperation({json.dumps(str(row['targetId']))}, {json.dumps(operation_id)},\n"
                "        { ...args, signal: context.signal },\n"
                "        fetchImpl,\n"
                "      ),\n"
                "    ...queryOptions,\n"
                "  };\n"
                "}\n\n"
            )
            lines.append(
                f"export function use{pascal}Query(args = {{}}, options = {{}}) {{\n"
                f"  return useQuery({op_const}QueryOptions(args, options));\n"
                "}\n\n"
            )
        else:
            lines.append(
                f"export function use{pascal}Mutation(options = {{}}) {{\n"
                "  const queryClient = useQueryClient();\n"
                "  const { fetchImpl, onSuccess, ...mutationOptions } = options;\n"
                "  return useMutation({\n"
                "    mutationFn: (args = {}) =>\n"
                f"      executeOperation({json.dumps(str(row['targetId']))}, {json.dumps(operation_id)},\n"
                "        args,\n"
                "        fetchImpl,\n"
                "      ),\n"
                "    ...mutationOptions,\n"
                "    onSuccess: async (data, variables, context) => {\n"
                f"      await invalidateOperationQueries(queryClient, {op_const});\n"
                "      if (typeof onSuccess === 'function') {\n"
                "        return onSuccess(data, variables, context);\n"
                "      }\n"
                "      return data;\n"
                "    },\n"
                "  });\n"
                "}\n\n"
            )
    return "".join(lines)


def _generated_feature_index_module(feature_modules: Sequence[Mapping[str, Any]]) -> str:
    import_lines: list[str] = []
    rows: list[str] = []
    for module_meta in feature_modules:
        module_slug = str(module_meta["moduleSlug"])
        alias = _camel_identifier(f"{module_slug}-module")
        import_lines.append(f"import {{ featureModule as {alias} }} from './{module_slug}.js';\n")
        rows.append(f"  {alias},\n")
    return "".join(import_lines) + "\nexport const featureModuleRegistry = [\n" + "".join(rows) + "];\n"


def _generated_backend_client_shim() -> str:
    return (
        "export {\n"
        "  backendTargets,\n"
        "  fetchBackendHealth,\n"
        "  fetchAllBackendHealth,\n"
        "  useBackendDiagnosticsQuery,\n"
        "} from './backend/diagnostics';\n"
        "export { featureModules, frontendIntegrationSummary, operationRegistry } from './backend/runtime';\n"
    )


def _generated_app_shell_module(*, repo_id: str, tenant_id: str, run_id: str) -> str:
    return (
        "import { useState } from 'react';\n"
        "import { Pressable, ScrollView, Text, View } from 'react-native';\n"
        "import { PersistQueryClientProvider } from '@tanstack/react-query-persist-client';\n"
        "import { AuthProvider, useAuth } from './src/generated/backend/auth';\n"
        "import { useBackendDiagnosticsQuery } from './src/generated/backend/diagnostics';\n"
        "import {\n"
        "  QUERY_CACHE_MAX_AGE_MS,\n"
        "  featureModules,\n"
        "  frontendIntegrationSummary,\n"
        "  queryClient,\n"
        "  queryPersister,\n"
        "} from './src/generated/backend/runtime';\n"
        "import { featureModuleRegistry } from './src/generated/backend/features/index';\n\n"
        "function SectionCard({ title, children }) {\n"
        "  return (\n"
        "    <View style={{ padding: 16, borderWidth: 1, borderColor: '#d1d5db', borderRadius: 12, gap: 8 }}>\n"
        "      <Text style={{ fontSize: 18, fontWeight: '600' }}>{title}</Text>\n"
        "      {children}\n"
        "    </View>\n"
        "  );\n"
        "}\n\n"
        "function HomeScreen() {\n"
        "  return (\n"
        "    <View style={{ gap: 16 }}>\n"
        '      <SectionCard title="Generated Feature Modules">\n'
        "        <Text>{featureModuleRegistry.length} modules generated from backend contracts.</Text>\n"
        "        {featureModuleRegistry.length === 0 ? (\n"
        "          <Text>No targets met the contract depth required for feature generation.</Text>\n"
        "        ) : (\n"
        "          featureModuleRegistry.map((module) => (\n"
        "            <View key={module.moduleSlug} style={{ gap: 4 }}>\n"
        "              <Text style={{ fontWeight: '600' }}>{module.targetName}: {module.featureGroup}</Text>\n"
        "              <Text>operations={module.operationCount} auth={module.authModes.join(', ') || 'none'}</Text>\n"
        "            </View>\n"
        "          ))\n"
        "        )}\n"
        "      </SectionCard>\n"
        '      <SectionCard title="Target Summary">\n'
        "        {frontendIntegrationSummary.targets.map((target) => (\n"
        "          <View key={target.targetId} style={{ gap: 4 }}>\n"
        "            <Text style={{ fontWeight: '600' }}>{target.name}</Text>\n"
        "            <Text>depth={target.contractDepth} mode={target.integrationMode}</Text>\n"
        "            <Text>features={target.featureGroups.join(', ') || 'diagnostics only'}</Text>\n"
        "          </View>\n"
        "        ))}\n"
        "      </SectionCard>\n"
        "    </View>\n"
        "  );\n"
        "}\n\n"
        "function DiagnosticsScreen() {\n"
        "  const diagnostics = useBackendDiagnosticsQuery();\n"
        "  const results = diagnostics.data || [];\n"
        "  return (\n"
        '    <SectionCard title="Backend Diagnostics">\n'
        "      <Text>{diagnostics.isLoading ? 'Checking backend health...' : 'Health probe results'}</Text>\n"
        "      {results.map((result) => (\n"
        "        <View key={result.targetId} style={{ gap: 4 }}>\n"
        "          <Text style={{ fontWeight: '600' }}>{result.targetId}</Text>\n"
        "          <Text>{result.ok ? `Healthy (${result.status})` : result.error || 'Unavailable'}</Text>\n"
        "          <Text>{result.url || 'No base URL configured'}</Text>\n"
        "        </View>\n"
        "      ))}\n"
        "    </SectionCard>\n"
        "  );\n"
        "}\n\n"
        "function GeneratedShell() {\n"
        "  const auth = useAuth();\n"
        "  const [screen, setScreen] = useState('home');\n"
        "  if (auth.loading) {\n"
        "    return (\n"
        "      <View style={{ flex: 1, alignItems: 'center', justifyContent: 'center', padding: 24 }}>\n"
        "        <Text style={{ fontSize: 20, fontWeight: '600' }}>Loading generated backend integration…</Text>\n"
        "      </View>\n"
        "    );\n"
        "  }\n"
        "  return (\n"
        "    <ScrollView contentContainerStyle={{ padding: 24, gap: 16 }}>\n"
        f"      <Text style={{ fontSize: 24, fontWeight: '600' }}>AKC execution workspace for {repo_id}</Text>\n"
        f"      <Text>tenant={tenant_id} run={run_id}</Text>\n"
        "      <Text>\n"
        "        targets={frontendIntegrationSummary.targets.length} "
        "featureModules={featureModules.length} "
        "generatedOperations={frontendIntegrationSummary.generatedOperationCount}\n"
        "      </Text>\n"
        "      <View style={{ flexDirection: 'row', gap: 12 }}>\n"
        "        <Pressable\n"
        "          onPress={() => setScreen('home')}\n"
        "          style={{\n"
        "            paddingVertical: 10,\n"
        "            paddingHorizontal: 14,\n"
        "            borderRadius: 999,\n"
        "            backgroundColor: screen === 'home' ? '#111827' : '#e5e7eb',\n"
        "          }}\n"
        "        >\n"
        "          <Text style={{ color: screen === 'home' ? '#ffffff' : '#111827', fontWeight: '600' }}>Home</Text>\n"
        "        </Pressable>\n"
        "        <Pressable\n"
        "          onPress={() => setScreen('diagnostics')}\n"
        "          style={{\n"
        "            paddingVertical: 10,\n"
        "            paddingHorizontal: 14,\n"
        "            borderRadius: 999,\n"
        "            backgroundColor: screen === 'diagnostics' ? '#111827' : '#e5e7eb',\n"
        "          }}\n"
        "        >\n"
        "          <Text style={{ color: screen === 'diagnostics' ? '#ffffff' : '#111827', fontWeight: '600' }}>\n"
        "            Diagnostics\n"
        "          </Text>\n"
        "        </Pressable>\n"
        "      </View>\n"
        "      {screen === 'home' ? <HomeScreen /> : <DiagnosticsScreen />}\n"
        "    </ScrollView>\n"
        "  );\n"
        "}\n\n"
        "export default function App() {\n"
        "  return (\n"
        "    <PersistQueryClientProvider\n"
        "      client={queryClient}\n"
        "      persistOptions={{\n"
        "        persister: queryPersister,\n"
        "        maxAge: QUERY_CACHE_MAX_AGE_MS,\n"
        "        buster: frontendIntegrationSummary.integrationFingerprint,\n"
        "      }}\n"
        "    >\n"
        "      <AuthProvider>\n"
        "        <GeneratedShell />\n"
        "      </AuthProvider>\n"
        "    </PersistQueryClientProvider>\n"
        "  );\n"
        "}\n"
    )


def _generated_backend_client_module(*, frontend_targets: list[dict[str, JSONValue]]) -> str:
    if not frontend_targets:
        return (
            "export const backendTargets = [];\n\n"
            "export async function fetchBackendHealth() {\n"
            "  return { ok: false, configured: false, error: 'No backend targets generated' };\n"
            "}\n\n"
            "export async function fetchAllBackendHealth() {\n"
            "  return [];\n"
            "}\n"
        )

    import_lines: list[str] = []
    rows: list[str] = []
    for target in frontend_targets:
        env_var = str(target["frontend_base_url_env_var"])
        global_fallback = str(target["frontend_global_base_url_env_var"])
        module_slug = _slug(str(target["target_id"]))
        alias = module_slug.replace("-", "_")
        import_lines.append(
            f"import {{ fetchHealth as fetchHealth_{alias}, "
            f"contractFingerprint as contractFingerprint_{alias}, "
            f"operationIds as operationIds_{alias}, "
            f"healthPath as healthPath_{alias} }} from './api/{module_slug}/client.js';\n"
        )
        rows.append(
            "  {\n"
            f"    targetId: {json.dumps(str(target['target_id']))},\n"
            f"    name: {json.dumps(str(target['name']))},\n"
            f"    targetClass: {json.dumps(str(target['target_class']))},\n"
            f"    resourcePath: {json.dumps(str(target.get('backend_resource_path') or ''))},\n"
            "    healthPath: healthPath_" + alias + ",\n"
            "    operationIds: operationIds_" + alias + ",\n"
            "    contractFingerprint: contractFingerprint_" + alias + ",\n"
            "    fetchHealth: fetchHealth_" + alias + ",\n"
            f"    baseUrlEnvVar: {json.dumps(env_var)},\n"
            "    resolveBaseUrl() {\n"
            f"      return process.env.{env_var} || process.env.{global_fallback} || null;\n"
            "    },\n"
            "  },"
        )
    rows_text = "\n".join(rows)
    return (
        "".join(import_lines) + "\n"
        "const joinUrl = (baseUrl, path) => {\n"
        "  const base = String(baseUrl || '').replace(/\\/+$/, '');\n"
        "  const suffix = String(path || '/healthz');\n"
        "  const normalized = suffix.startsWith('/') ? suffix : `/${suffix}`;\n"
        "  return `${base}${normalized}`;\n"
        "};\n\n"
        "const readJson = async (response) => {\n"
        "  try {\n"
        "    return await response.json();\n"
        "  } catch {\n"
        "    return null;\n"
        "  }\n"
        "};\n\n"
        "export const backendTargets = [\n"
        f"{rows_text}\n"
        "];\n\n"
        "export async function fetchBackendHealth(target, fetchImpl = fetch) {\n"
        "  const baseUrl = target.resolveBaseUrl();\n"
        "  if (!baseUrl) {\n"
        "    return {\n"
        "      targetId: target.targetId,\n"
        "      configured: false,\n"
        "      ok: false,\n"
        "      status: null,\n"
        "      url: null,\n"
        "      payload: null,\n"
        "      error: `Set ${target.baseUrlEnvVar} or EXPO_PUBLIC_API_BASE_URL`,\n"
        "    };\n"
        "  }\n"
        "  try {\n"
        "    const { response, payload, url } = await target.fetchHealth(baseUrl, fetchImpl);\n"
        "    return {\n"
        "      targetId: target.targetId,\n"
        "      configured: true,\n"
        "      ok: response.ok,\n"
        "      status: response.status,\n"
        "      url,\n"
        "      payload,\n"
        "      operationIds: target.operationIds,\n"
        "      contractFingerprint: target.contractFingerprint,\n"
        "      error: response.ok ? null : `HTTP ${response.status}`,\n"
        "    };\n"
        "  } catch (error) {\n"
        "    return {\n"
        "      targetId: target.targetId,\n"
        "      configured: true,\n"
        "      ok: false,\n"
        "      status: null,\n"
        "      url: joinUrl(baseUrl, target.healthPath),\n"
        "      payload: null,\n"
        "      operationIds: target.operationIds,\n"
        "      contractFingerprint: target.contractFingerprint,\n"
        "      error: error instanceof Error ? error.message : String(error),\n"
        "    };\n"
        "  }\n"
        "}\n\n"
        "export async function fetchAllBackendHealth(fetchImpl = fetch) {\n"
        "  return Promise.all(backendTargets.map((target) => fetchBackendHealth(target, fetchImpl)));\n"
        "}\n"
    )


def _materialize_client_workspace(
    *,
    add_json: Any,
    add_text: Any,
    package_manager: str,
    ir_document: IRDocument,
    app_slug: str,
    expo_project_id: str | None,
    run_id: str,
    frontend_targets: list[dict[str, JSONValue]],
    contracts_by_target: Mapping[str, Mapping[str, Any]],
    client_generator_cmd: Sequence[str] | None,
) -> dict[str, Any]:
    external_codegen_summary: dict[str, Any] = {
        "mode": "internal_fallback",
        "targets": {},
    }
    add_json(rel_path="apps/universal/package.json", obj=_expo_package_json(package_manager=package_manager))
    add_json(
        rel_path="apps/universal/app.json",
        obj=_expo_app_json(
            ir_document=ir_document,
            app_slug=app_slug,
            expo_project_id=expo_project_id,
        ),
    )
    add_json(rel_path="apps/universal/eas.json", obj=_expo_eas_json())
    add_text(
        rel_path="apps/universal/index.js",
        text="import { registerRootComponent } from 'expo';\nimport App from './App';\nregisterRootComponent(App);\n",
    )
    for target in frontend_targets:
        target_id = str(target["target_id"])
        module_slug = _slug(target_id)
        external_files, generation_meta = _external_client_binding_files(
            target_id=target_id,
            contract_obj=contracts_by_target.get(target_id),
            generator_cmd=client_generator_cmd,
        )
        external_codegen_summary["targets"][target_id] = generation_meta
        if generation_meta.get("mode") == "external_openapi_generator":
            external_codegen_summary["mode"] = "external_openapi_generator"
        for rel_name, text in external_files.items():
            add_text(
                rel_path=f"apps/universal/src/generated/api/{module_slug}/external/{rel_name}",
                text=text,
                media_type="text/plain; charset=utf-8",
            )
        add_text(
            rel_path=f"apps/universal/src/generated/api/{module_slug}/client.js",
            text=_target_client_module(target=target, contract_obj=contracts_by_target.get(target_id)),
            media_type="application/javascript; charset=utf-8",
        )
    operation_rows, feature_modules, frontend_integration_summary = _operation_registry_rows(
        frontend_targets=frontend_targets,
        contracts_by_target=contracts_by_target,
    )
    add_text(
        rel_path="apps/universal/src/generated/backend/runtime.js",
        text=_generated_backend_runtime_module(
            frontend_targets=frontend_targets,
            operation_rows=operation_rows,
            feature_modules=feature_modules,
            frontend_integration_summary=frontend_integration_summary,
        ),
        media_type="application/javascript; charset=utf-8",
    )
    add_text(
        rel_path="apps/universal/src/generated/backend/auth.js",
        text=_generated_backend_auth_module(),
        media_type="application/javascript; charset=utf-8",
    )
    add_text(
        rel_path="apps/universal/src/generated/backend/diagnostics.js",
        text=_generated_backend_diagnostics_module(),
        media_type="application/javascript; charset=utf-8",
    )
    module_ops_by_slug = {
        str(module_meta["moduleSlug"]): [
            row
            for row in operation_rows
            if str(row["targetId"]) == str(module_meta["targetId"])
            and str(row["featureGroup"]) == str(module_meta["featureGroup"])
            and not bool(row["isHealth"])
        ]
        for module_meta in feature_modules
    }
    for module_meta in feature_modules:
        module_slug = str(module_meta["moduleSlug"])
        add_text(
            rel_path=f"apps/universal/src/generated/backend/features/{module_slug}.js",
            text=_generated_feature_module(
                module_meta=module_meta,
                module_operations=module_ops_by_slug[module_slug],
            ),
            media_type="application/javascript; charset=utf-8",
        )
    add_text(
        rel_path="apps/universal/src/generated/backend/features/index.js",
        text=_generated_feature_index_module(feature_modules),
        media_type="application/javascript; charset=utf-8",
    )
    add_text(
        rel_path="apps/universal/src/generated/backend-client.js",
        text=_generated_backend_client_shim(),
        media_type="application/javascript; charset=utf-8",
    )
    add_text(
        rel_path="apps/universal/App.js",
        text=_generated_app_shell_module(
            repo_id=ir_document.repo_id,
            tenant_id=ir_document.tenant_id,
            run_id=run_id,
        ),
        media_type="application/javascript; charset=utf-8",
    )
    add_text(
        rel_path="apps/universal/web/index.html",
        text=(
            "<!doctype html>\n"
            '<html><head><meta charset="utf-8" /><title>AKC Universal App</title></head>'
            "<body><main><h1>AKC Universal App</h1>"
            f"<p>repo={ir_document.repo_id}</p><p>run_id={run_id}</p></main></body></html>\n"
        ),
        media_type="text/html; charset=utf-8",
    )
    external_codegen_summary["frontend_integration_summary"] = frontend_integration_summary
    return external_codegen_summary


def _backend_target_row(
    *,
    node: Any,
    target_class: str,
    rel_dir: str,
    runtime_profile: str | None,
    entry_rel: str | None,
    dockerfile_rel: str | None,
    projected: Mapping[str, Any] | None,
    backend_resource: Mapping[str, Any] | None,
    api_contract_ref: Mapping[str, Any] | None,
    api_contract: Mapping[str, Any] | None,
    practical_backend_source: bool,
) -> dict[str, JSONValue]:
    health_contract = (
        dict(cast(dict[str, Any], backend_resource.get("health_contract") or {}))
        if isinstance(backend_resource, Mapping)
        else {}
    )
    readiness_path = str(health_contract.get("path", "/healthz"))
    config_env_keys = (
        sorted({str(x).strip() for x in cast(list[Any], backend_resource.get("config_env_keys", [])) if str(x).strip()})
        if isinstance(backend_resource, Mapping)
        else []
    )
    secret_keys = (
        sorted({str(x).strip() for x in cast(list[Any], backend_resource.get("secret_keys", [])) if str(x).strip()})
        if isinstance(backend_resource, Mapping)
        else []
    )
    observability_contract = (
        dict(cast(dict[str, Any], backend_resource.get("observability_contract") or {}))
        if isinstance(backend_resource, Mapping)
        else {}
    )
    persistence_intent = (
        dict(cast(dict[str, Any], backend_resource.get("persistence_intent") or {}))
        if isinstance(backend_resource, Mapping)
        else {}
    )
    repo_anchor_paths = (
        [str(x).strip() for x in cast(list[Any], backend_resource.get("repo_anchor_paths", [])) if str(x).strip()]
        if isinstance(backend_resource, Mapping)
        else []
    )
    workspace_role = (
        "worker_lane"
        if rel_dir.startswith("workers/")
        else "backend_service_lane"
        if rel_dir.startswith("services/")
        else "client_packaging_lane"
    )
    target_row: dict[str, JSONValue] = {
        "target_id": node.id,
        "name": node.name,
        "kind": node.kind,
        "target_class": target_class or "unknown",
        "runtime_profile": runtime_profile,
        "workspace_rel_dir": rel_dir,
        "entry_rel_path": entry_rel,
        "dockerfile_rel_path": dockerfile_rel,
        "depends_on": cast(JSONValue, list(node.depends_on)),
        "workspace_role": workspace_role,
        "generation_source": (
            "practical_backend_ir"
            if practical_backend_source and rel_dir.startswith(("services/", "workers/"))
            else "delivery_target_mapping"
            if rel_dir.startswith(("services/", "workers/"))
            else "client_packaging_lane"
        ),
    }
    if rel_dir.startswith(("services/", "workers/")):
        target_row["health_path"] = "/healthz"
        target_row["health_contract"] = {
            "readiness_path": "/healthz",
            "health_endpoint_known": True,
        }
        target_row["config_env_keys"] = cast(JSONValue, [])
        target_row["secret_keys"] = cast(JSONValue, [])
        target_row["config_secrets_contract"] = {
            "required_env": cast(JSONValue, []),
            "required_secrets": cast(JSONValue, []),
        }
        target_row["runtime_contract"] = {"port": 8080}
    if backend_resource is not None:
        target_row["backend_resource_path"] = str(backend_resource.get("resource_path") or "")
        target_row["health_path"] = readiness_path
        target_row["health_contract"] = {
            "readiness_path": readiness_path,
            "health_endpoint_known": bool(health_contract.get("requires_readiness_contract", True)),
        }
        target_row["config_env_keys"] = cast(JSONValue, config_env_keys)
        target_row["secret_keys"] = cast(JSONValue, secret_keys)
        target_row["config_secrets_contract"] = {
            "required_env": cast(JSONValue, config_env_keys),
            "required_secrets": cast(JSONValue, secret_keys),
        }
        target_row["observability_contract"] = cast(JSONValue, observability_contract)
        target_row["operational_config"] = {
            "observability_toggles": {
                "logging_enabled": bool(observability_contract.get("logging", True)),
                "metrics_enabled": bool(observability_contract.get("metrics", True)),
                "tracing_enabled": bool(observability_contract.get("tracing", True)),
            }
        }
        target_row["persistence_intent"] = cast(JSONValue, persistence_intent)
        target_row["repo_anchor_paths"] = cast(JSONValue, repo_anchor_paths)
        target_row["runtime_contract"] = {"port": 8080}
        contract_sources = backend_resource.get("contract_sources")
        if isinstance(contract_sources, list):
            target_row["contract_sources"] = cast(JSONValue, contract_sources)
        contract_confidence = backend_resource.get("contract_confidence")
        if isinstance(contract_confidence, (int, float)) and not isinstance(contract_confidence, bool):
            target_row["contract_confidence"] = float(contract_confidence)
    if isinstance(api_contract_ref, Mapping):
        target_row["api_contract_ref"] = cast(JSONValue, dict(api_contract_ref))
        contract_depth = api_contract_ref.get("contract_depth")
        if isinstance(contract_depth, str) and contract_depth.strip():
            target_row["contract_depth"] = contract_depth.strip()
        feature_groups = api_contract_ref.get("feature_groups")
        if isinstance(feature_groups, list):
            target_row["api_feature_groups"] = cast(
                JSONValue, [str(item) for item in feature_groups if str(item).strip()]
            )
        auth_modes = api_contract_ref.get("auth_modes")
        if isinstance(auth_modes, list):
            target_row["api_auth_modes"] = cast(JSONValue, [str(item) for item in auth_modes if str(item).strip()])
        operation_count = api_contract_ref.get("operation_count")
        if isinstance(operation_count, int):
            target_row["operation_count"] = operation_count
    if isinstance(api_contract, Mapping):
        operation_ids = [str(op.get("operationId", "")).strip() for op in _contract_operations(api_contract)]
        target_row["api_operation_ids"] = cast(JSONValue, [op for op in operation_ids if op])
    if isinstance(projected, Mapping):
        for key in (
            "domain",
            "supported_delivery_paths",
            "rollout_recovery_policy",
            "config_secrets_contract",
            "operational_config",
            "scaling_resources",
            "runtime_contract",
            "health_contract",
            "build_contract",
        ):
            value = projected.get(key)
            if value is not None:
                target_row[key] = cast(JSONValue, value)
    return target_row


def build_execution_workspace(
    *,
    run_id: str,
    ir_document: IRDocument,
    delivery_plan_obj: Mapping[str, Any] | None,
    project_root: Path | None,
    practical_backend_handoff: PracticalBackendHandoff | None = None,
) -> tuple[dict[str, Any], tuple[OutputArtifact, ...]]:
    package_manager = infer_package_manager(project_root=project_root)
    requested_runtime_plugin = _requested_runtime_plugin(
        project_root=project_root,
        practical_backend_handoff=practical_backend_handoff,
    )
    requested_runtime_source = (
        str(practical_backend_handoff.runtime_plugin_decision.get("plugin_source", "")).strip()
        if practical_backend_handoff is not None
        else "inferred"
    ) or "inferred"
    runtime_profile = requested_runtime_plugin
    selected_runtime_plugin = requested_runtime_plugin
    policy = load_backend_generator_policy(project_root=project_root)
    materializer = None
    selected_plugin = get_backend_runtime_plugin(selected_runtime_plugin)
    if selected_plugin is not None:
        requested_by_policy = bool(
            practical_backend_handoff is not None
            and practical_backend_handoff.runtime_plugin_decision.get("requested_by_policy") is True
        )
        materializer = resolve_backend_runtime_materializer(
            selected_plugin=selected_plugin,
            project_root=project_root,
            policy=policy,
            requested_by_policy=requested_by_policy,
        )
    op = _operator_prereqs(project_root)
    expo_project_id = None
    expo_cfg = op.get("expo")
    if isinstance(expo_cfg, dict):
        raw_project_id = expo_cfg.get("project_id")
        if isinstance(raw_project_id, str) and raw_project_id.strip():
            expo_project_id = raw_project_id.strip()
    client_generator_cmd = _client_codegen_command(project_root=project_root)

    app_slug = _slug(ir_document.repo_id)
    workspace_root = f".akc/execution/{run_id}/workspace"
    manifest_path = f".akc/execution/{run_id}.execution_workspace_manifest.json"
    generated: list[OutputArtifact] = []
    generated_files: list[dict[str, JSONValue]] = []
    target_rows: list[dict[str, JSONValue]] = []
    expected_practical_generation_proof = bool(
        practical_backend_handoff is not None and practical_backend_handoff.authoritative_workspace_ready
    )
    practical_generation_proof = expected_practical_generation_proof
    artifact_role = "authoritative_generated_workspace" if practical_generation_proof else "fallback_debug_reference"
    generation_mode = "backend_ir_materialized_workspace" if practical_generation_proof else "side_workspace_reference"
    materializer_kind = materializer.materializer_kind if materializer is not None else "unknown"
    materialization_status = "pending" if practical_generation_proof else "fallback_debug_reference"
    backend_resources = _practical_backend_resources(practical_backend_handoff)
    api_contracts_by_target = _practical_api_contracts(practical_backend_handoff)
    api_contract_refs_by_target = _practical_api_contract_refs(practical_backend_handoff)
    practical_backend_generation = _practical_backend_manifest_meta(
        run_id=run_id,
        practical_backend_handoff=practical_backend_handoff,
    )

    def _add_text(*, rel_path: str, text: str, media_type: str = "text/plain; charset=utf-8") -> None:
        art = OutputArtifact.from_text(path=f"{workspace_root}/{rel_path}", text=text, media_type=media_type)
        generated.append(art)
        generated_files.append(
            {
                "path": art.path,
                "sha256": art.sha256_hex(),
                "size_bytes": art.size_bytes(),
            }
        )

    def _add_json(*, rel_path: str, obj: Mapping[str, Any]) -> None:
        art = OutputArtifact.from_json(path=f"{workspace_root}/{rel_path}", obj=obj)
        generated.append(art)
        generated_files.append(
            {
                "path": art.path,
                "sha256": art.sha256_hex(),
                "size_bytes": art.size_bytes(),
            }
        )

    def _add_bytes(*, rel_path: str, content: bytes, media_type: str) -> None:
        art = OutputArtifact(path=f"{workspace_root}/{rel_path}", content=content, media_type=media_type)
        generated.append(art)
        generated_files.append(
            {
                "path": art.path,
                "sha256": art.sha256_hex(),
                "size_bytes": art.size_bytes(),
            }
        )

    targets_by_id: dict[str, Mapping[str, Any]] = {}
    if isinstance(delivery_plan_obj, Mapping):
        raw_targets = delivery_plan_obj.get("targets")
        if isinstance(raw_targets, list):
            for row in raw_targets:
                if not isinstance(row, Mapping):
                    continue
                target_id = row.get("target_id")
                if isinstance(target_id, str) and target_id.strip():
                    targets_by_id[target_id.strip()] = row

    backend_stub_source = "practical_backend_ir" if practical_generation_proof else "delivery_target_mapping"
    external_materialization_error: str | None = None
    external_client_codegen_summary: dict[str, Any] | None = None
    external_frontend_integration_summary: dict[str, Any] | None = None
    external_toolchain: dict[str, Any] | None = None
    external_build_profiles: dict[str, Any] | None = None
    external_build_entrypoints: dict[str, Any] | None = None
    external_expected_outputs: dict[str, Any] | None = None
    external_expo: dict[str, Any] | None = None
    external_api_contract_refs: list[dict[str, Any]] | None = None

    if (
        materializer is not None
        and materializer.available
        and materializer.materializer_kind == "command"
        and materializer.command is not None
    ):
        response_obj, files, external_materialization_error = _materialize_external_backend_workspace(
            run_id=run_id,
            ir_document=ir_document,
            project_root=project_root,
            selected_plugin_id=selected_runtime_plugin,
            delivery_plan_obj=delivery_plan_obj,
            practical_backend_handoff=practical_backend_handoff,
            command=materializer.command,
        )
        if external_materialization_error is None and response_obj is not None and files is not None:
            artifact_role = str(response_obj["artifact_role"]).strip()
            generation_mode = str(response_obj["generation_mode"]).strip()
            runtime_profile = str(response_obj["materialized_runtime_profile"]).strip()
            practical_generation_proof = (
                str(response_obj["status"]).strip().lower() in {"ready", "succeeded"}
                and artifact_role == "authoritative_generated_workspace"
            )
            materialization_status = str(response_obj["status"]).strip()
            target_rows = [
                dict(cast(dict[str, Any], row))
                for row in cast(list[Any], response_obj.get("targets") or [])
                if isinstance(row, Mapping)
            ]
            external_toolchain = dict(cast(dict[str, Any], response_obj.get("toolchain") or {}))
            external_client_codegen_summary = (
                dict(cast(dict[str, Any], response_obj.get("client_codegen_summary") or {}))
                if isinstance(response_obj.get("client_codegen_summary"), Mapping)
                else {}
            )
            external_frontend_integration_summary = (
                dict(cast(dict[str, Any], response_obj.get("frontend_integration_summary") or {}))
                if isinstance(response_obj.get("frontend_integration_summary"), Mapping)
                else {"targets": [], "generatedFeatureModules": [], "generatedOperationCount": 0}
            )
            external_build_profiles = (
                dict(cast(dict[str, Any], response_obj.get("build_profiles") or {}))
                if isinstance(response_obj.get("build_profiles"), Mapping)
                else {}
            )
            external_build_entrypoints = (
                dict(cast(dict[str, Any], response_obj.get("build_entrypoints") or {}))
                if isinstance(response_obj.get("build_entrypoints"), Mapping)
                else {}
            )
            external_expected_outputs = (
                dict(cast(dict[str, Any], response_obj.get("expected_outputs") or {}))
                if isinstance(response_obj.get("expected_outputs"), Mapping)
                else {}
            )
            external_expo = (
                dict(cast(dict[str, Any], response_obj.get("expo") or {}))
                if isinstance(response_obj.get("expo"), Mapping)
                else {}
            )
            external_api_contract_refs = [
                dict(cast(dict[str, Any], row))
                for row in cast(list[Any], response_obj.get("api_contract_refs") or [])
                if isinstance(row, Mapping)
            ]
            for rel_path, content in files:
                _add_bytes(rel_path=rel_path, content=content, media_type=_guess_media_type(Path(rel_path)))
        else:
            practical_generation_proof = False
            artifact_role = "fallback_debug_reference"
            generation_mode = "side_workspace_reference"
            runtime_profile = "fallback_debug_reference"
            materialization_status = "blocked"
    else:
        if materializer is None or not materializer.available:
            practical_generation_proof = False
            artifact_role = "fallback_debug_reference"
            generation_mode = "side_workspace_reference"
            materialization_status = "blocked"
            if selected_runtime_plugin not in BUILTIN_AUTHORITATIVE_RUNTIME_PLUGINS:
                runtime_profile = "fallback_debug_reference"
        elif materializer.plugin.plugin_id in BUILTIN_AUTHORITATIVE_RUNTIME_PLUGINS:
            runtime_profile = materializer.plugin.plugin_id
            materialization_status = "builtin_authoritative"
        for node in sorted(ir_document.nodes, key=lambda item: item.id):
            projected = targets_by_id.get(node.id)
            backend_resource = backend_resources.get(node.id)
            api_contract = api_contracts_by_target.get(node.id)
            api_contract_ref = api_contract_refs_by_target.get(node.id)
            target_class = (
                str(projected.get("target_class")).strip()
                if isinstance(projected, Mapping) and projected.get("target_class")
                else (
                    str(backend_resource.get("target_class")).strip()
                    if isinstance(backend_resource, Mapping) and backend_resource.get("target_class")
                    else ""
                )
            )
            rel_dir = _target_rel_dir(target_class=target_class, name=node.name)
            entry_rel = None
            dockerfile_rel = None
            effective_profile: str | None = None

            if rel_dir.startswith("services/") or rel_dir.startswith("workers/"):
                if materializer is not None and materializer.plugin.plugin_id in BUILTIN_AUTHORITATIVE_RUNTIME_PLUGINS:
                    effective_profile = runtime_profile
                    built_in_files = _builtin_service_files(
                        runtime_profile=runtime_profile,
                        service_name=_slug(node.name),
                        run_id=run_id,
                        target_class=target_class,
                        contract_obj=api_contract,
                    )
                    for rel_name, text in built_in_files.items():
                        _add_text(rel_path=f"{rel_dir}/{rel_name}", text=text)
                    entry_rel = _builtin_service_entry_rel(runtime_profile=runtime_profile, rel_dir=rel_dir)
                    dockerfile_rel = f"{rel_dir}/Dockerfile"
                    if practical_generation_proof and backend_resource is None:
                        backend_stub_source = "mixed_authoritative_and_delivery_mapping"
                elif practical_generation_proof and backend_resource is None:
                    backend_stub_source = "mixed_authoritative_and_delivery_mapping"
            elif rel_dir == "apps/universal":
                effective_profile = "expo_universal"
                entry_rel = "apps/universal/App.js"

            if rel_dir:
                target_rows.append(
                    _backend_target_row(
                        node=node,
                        target_class=target_class,
                        rel_dir=rel_dir,
                        runtime_profile=effective_profile,
                        entry_rel=entry_rel,
                        dockerfile_rel=dockerfile_rel,
                        projected=projected,
                        backend_resource=backend_resource,
                        api_contract_ref=api_contract_ref,
                        api_contract=api_contract,
                        practical_backend_source=practical_generation_proof and backend_resource is not None,
                    )
                )

    frontend_targets = [
        {
            **row,
            "frontend_base_url_env_var": _frontend_base_url_env_var(target_name=str(row.get("name") or "service")),
            "frontend_global_base_url_env_var": "EXPO_PUBLIC_API_BASE_URL",
        }
        for row in target_rows
        if str(row.get("target_class")) in {"backend_service", "integration"}
    ]
    if external_toolchain is not None:
        client_codegen_summary = external_client_codegen_summary or {}
        frontend_integration_summary = external_frontend_integration_summary or {
            "targets": [],
            "generatedFeatureModules": [],
            "generatedOperationCount": 0,
        }
    else:
        client_codegen_summary = _materialize_client_workspace(
            add_json=_add_json,
            add_text=_add_text,
            package_manager=package_manager,
            ir_document=ir_document,
            app_slug=app_slug,
            expo_project_id=expo_project_id,
            run_id=run_id,
            frontend_targets=frontend_targets,
            contracts_by_target=api_contracts_by_target,
            client_generator_cmd=client_generator_cmd,
        )
        frontend_integration_summary = (
            dict(cast(dict[str, Any], client_codegen_summary.get("frontend_integration_summary")))
            if isinstance(client_codegen_summary.get("frontend_integration_summary"), Mapping)
            else {"targets": [], "generatedFeatureModules": [], "generatedOperationCount": 0}
        )

    api_contract_refs_for_manifest = (
        external_api_contract_refs
        if external_api_contract_refs is not None
        else [dict(cast(dict[str, Any], row)) for row in api_contract_refs_by_target.values()]
    )

    shared_contract = {
        "run_id": run_id,
        "tenant_id": ir_document.tenant_id,
        "repo_id": ir_document.repo_id,
        "artifact_role": artifact_role,
        "generation_mode": generation_mode,
        "practical_generation_proof": practical_generation_proof,
        "contract_format": "openapi_3_1",
        "api_contract_refs": api_contract_refs_for_manifest,
        "frontend_integration_summary": frontend_integration_summary,
        "targets": target_rows,
    }
    _add_json(rel_path="shared/targets.contract.json", obj=shared_contract)
    _add_text(
        rel_path=".env.example",
        text=(
            "AKC_TENANT_ID="
            f"{ir_document.tenant_id}\nAKC_REPO_ID={ir_document.repo_id}\nAKC_RUN_ID={run_id}\n"
            "EXPO_PUBLIC_API_BASE_URL=https://api.example.com\n"
            + "".join(
                f"{row['frontend_base_url_env_var']}=https://{_slug(str(row['name']))}.example.com\n"
                for row in frontend_targets
            )
        ),
    )

    default_toolchain = {
        "node": "node",
        "package_manager": package_manager,
        "eas_cli": "eas",
        "http_contract_format": "openapi_3_1",
        "client_binding_generator": {
            "mode": client_codegen_summary.get("mode"),
            "command": list(client_generator_cmd) if client_generator_cmd else None,
            "generator": "typescript-fetch",
            "additional_properties": {
                "supportsES6": True,
                "useSingleRequestParameter": True,
                "withInterfaces": True,
                "prefixParameterInterfaces": True,
                "stringEnums": True,
            },
        },
    }
    default_expo = {
        "app_dir": f"{workspace_root}/apps/universal",
        "slug": app_slug,
        "project_id": expo_project_id,
        "ios_bundle_identifier": f"com.akc.{_slug(ir_document.repo_id)}",
        "android_package": f"com.akc.{_slug(ir_document.repo_id)}",
    }
    default_build_entrypoints = {
        "web": {
            "cwd": f"{workspace_root}/apps/universal",
            "install": [package_manager, "install"] if package_manager != "yarn" else ["yarn", "install"],
            "command": ["npx", "expo", "export", "--platform", "web"],
            "output_dir": ".akc/delivery/<delivery_id>/packaging/web/exported",
        },
        "ios": {
            "cwd": f"{workspace_root}/apps/universal",
            "command": ["eas", "build", "--platform", "ios", "--profile", "preview", "--non-interactive"],
        },
        "android": {
            "cwd": f"{workspace_root}/apps/universal",
            "command": ["eas", "build", "--platform", "android", "--profile", "preview", "--non-interactive"],
        },
    }
    default_expected_outputs = {
        "web_export_dir": ".akc/delivery/<delivery_id>/packaging/web/exported",
        "ios_ipa_path": ".akc/delivery/<delivery_id>/packaging/ios/App.ipa",
        "android_aab_path": ".akc/delivery/<delivery_id>/packaging/android/app-release.aab",
    }

    if practical_backend_generation is not None:
        practical_backend_generation = dict(practical_backend_generation)
        practical_backend_generation["materialization_status"] = materialization_status
        practical_backend_generation["requested_runtime_plugin"] = requested_runtime_plugin
        practical_backend_generation["requested_runtime_source"] = requested_runtime_source
        if external_materialization_error is not None:
            practical_backend_generation["materialization_error"] = external_materialization_error

    manifest_obj = apply_schema_envelope(
        obj={
            "run_id": run_id,
            "tenant_id": ir_document.tenant_id,
            "repo_id": ir_document.repo_id,
            "artifact_role": artifact_role,
            "practical_generation_proof": practical_generation_proof,
            "generation_mode": generation_mode,
            "workspace_root": workspace_root,
            "package_manager": package_manager,
            "toolchain": external_toolchain if external_toolchain is not None else default_toolchain,
            "runtime_profile": runtime_profile,
            "requested_runtime_plugin": requested_runtime_plugin,
            "requested_runtime_source": requested_runtime_source,
            "materialization_status": materialization_status,
            "materializer_kind": materializer_kind,
            "expo": external_expo if external_expo is not None else default_expo,
            "build_profiles": (
                external_build_profiles
                if external_build_profiles is not None
                else {"beta": "preview", "store": "production"}
            ),
            "build_entrypoints": (
                external_build_entrypoints if external_build_entrypoints is not None else default_build_entrypoints
            ),
            "expected_outputs": (
                external_expected_outputs if external_expected_outputs is not None else default_expected_outputs
            ),
            "practical_backend_generation": practical_backend_generation,
            "backend_stub_source": backend_stub_source,
            "api_contract_refs": api_contract_refs_for_manifest,
            "frontend_integration_summary": frontend_integration_summary,
            "client_codegen_summary": client_codegen_summary,
            "targets": target_rows,
            "generated_files": generated_files,
            "workspace_fingerprint": stable_json_fingerprint(
                {"workspace_root": workspace_root, "generated_files": generated_files}
            ),
        },
        kind="execution_workspace_manifest",
        version=1,
    )
    manifest_artifact = OutputArtifact.from_json(
        path=manifest_path,
        obj=manifest_obj,
        metadata={"run_id": run_id, "kind": "execution_workspace"},
    )
    return manifest_obj, (manifest_artifact, *generated)
