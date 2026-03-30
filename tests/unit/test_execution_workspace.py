from __future__ import annotations

import json
import stat
from pathlib import Path

from akc.artifacts.validate import validate_obj
from akc.compile.backend_generation import build_practical_backend_context
from akc.compile.execution_workspace import (
    build_execution_workspace,
    infer_backend_runtime_profile,
    infer_package_manager,
)
from akc.ir import IRDocument, IRNode


def _ir() -> IRDocument:
    return IRDocument(
        tenant_id="t1",
        repo_id="repo1",
        nodes=(
            IRNode(id="api1", tenant_id="t1", kind="service", name="api backend", properties={}),
            IRNode(id="worker1", tenant_id="t1", kind="service", name="background worker", properties={}),
            IRNode(id="web1", tenant_id="t1", kind="service", name="web frontend", properties={}),
        ),
    )


def _ir_with_full_contract() -> IRDocument:
    return IRDocument(
        tenant_id="t1",
        repo_id="repo1",
        nodes=(
            IRNode(
                id="api1",
                tenant_id="t1",
                kind="service",
                name="api backend",
                properties={
                    "openapi_contract": {
                        "openapi": "3.1.0",
                        "info": {"title": "Accounts API", "version": "1.0.0"},
                        "paths": {
                            "/healthz": {
                                "get": {
                                    "operationId": "accountsHealthz",
                                    "tags": ["health"],
                                    "responses": {
                                        "200": {
                                            "description": "ok",
                                            "content": {"application/json": {"schema": {"type": "object"}}},
                                        }
                                    },
                                }
                            },
                            "/users": {
                                "get": {
                                    "operationId": "listUsers",
                                    "tags": ["users"],
                                    "parameters": [{"name": "page", "in": "query", "schema": {"type": "integer"}}],
                                    "responses": {
                                        "200": {
                                            "description": "ok",
                                            "content": {"application/json": {"schema": {"type": "array"}}},
                                        }
                                    },
                                },
                                "post": {
                                    "operationId": "createUser",
                                    "tags": ["users"],
                                    "security": [{"BearerAuth": []}],
                                    "requestBody": {
                                        "required": True,
                                        "content": {"application/json": {"schema": {"type": "object"}}},
                                    },
                                    "responses": {
                                        "200": {
                                            "description": "ok",
                                            "content": {"application/json": {"schema": {"type": "object"}}},
                                        }
                                    },
                                },
                            },
                            "/users/{userId}": {
                                "get": {
                                    "operationId": "getUser",
                                    "tags": ["users"],
                                    "parameters": [
                                        {"name": "userId", "in": "path", "required": True, "schema": {"type": "string"}}
                                    ],
                                    "responses": {
                                        "200": {
                                            "description": "ok",
                                            "content": {"application/json": {"schema": {"type": "object"}}},
                                        }
                                    },
                                }
                            },
                        },
                        "components": {"securitySchemes": {"BearerAuth": {"type": "http", "scheme": "bearer"}}},
                    }
                },
            ),
            IRNode(id="web1", tenant_id="t1", kind="service", name="web frontend", properties={}),
        ),
    )


def test_infer_runtime_profile_and_package_manager_from_project_profile(tmp_path: Path) -> None:
    profile_dir = tmp_path / ".akc"
    profile_dir.mkdir(parents=True)
    (profile_dir / "project_profile.json").write_text(
        json.dumps(
            {
                "languages": [{"language": "python", "percent": 90.0, "bytes": 10, "files": 1}],
                "package_managers": ["pnpm"],
            }
        ),
        encoding="utf-8",
    )
    assert infer_backend_runtime_profile(project_root=tmp_path) == "python_fastapi"
    assert infer_package_manager(project_root=tmp_path) == "pnpm"


def test_infer_runtime_profile_supports_go_from_project_profile(tmp_path: Path) -> None:
    profile_dir = tmp_path / ".akc"
    profile_dir.mkdir(parents=True)
    (profile_dir / "project_profile.json").write_text(
        json.dumps(
            {
                "languages": [{"language": "go", "percent": 100.0, "bytes": 10, "files": 1}],
                "package_managers": ["npm"],
            }
        ),
        encoding="utf-8",
    )
    assert infer_backend_runtime_profile(project_root=tmp_path) == "go"


def test_build_execution_workspace_emits_manifest_and_generated_files(tmp_path: Path) -> None:
    manifest, artifacts = build_execution_workspace(
        run_id="run-1",
        ir_document=_ir(),
        delivery_plan_obj={
            "targets": [
                {"target_id": "api1", "target_class": "backend_service"},
                {"target_id": "worker1", "target_class": "worker"},
                {"target_id": "web1", "target_class": "web_app"},
            ]
        },
        project_root=tmp_path,
    )
    assert validate_obj(obj=manifest, kind="execution_workspace_manifest", version=1) == []
    assert manifest["runtime_profile"] == "typescript_node"
    assert manifest["artifact_role"] == "fallback_debug_reference"
    assert manifest["practical_generation_proof"] is False
    assert manifest["generation_mode"] == "side_workspace_reference"
    assert any(a.path.endswith(".execution_workspace_manifest.json") for a in artifacts)
    assert any(a.path.endswith("apps/universal/app.json") for a in artifacts)
    assert any(a.path.endswith("apps/universal/src/generated/api/api1/client.js") for a in artifacts)
    assert any(a.path.endswith("apps/universal/src/generated/backend-client.js") for a in artifacts)
    assert any(a.path.endswith("services/api-backend/Dockerfile") for a in artifacts)
    assert any(a.path.endswith("workers/background-worker/Dockerfile") for a in artifacts)
    env_artifact = next(a for a in artifacts if a.path.endswith("/workspace/.env.example"))
    assert "EXPO_PUBLIC_API_BACKEND_BASE_URL" in env_artifact.text()


def test_build_execution_workspace_authoritative_mode_enriches_targets_and_contract(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "userController.ts").write_text("export const x = 1;\n", encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "api_smoke.test.ts").write_text("it('smoke', () => {});\n", encoding="utf-8")
    (tmp_path / "package.json").write_text(
        json.dumps(
            {
                "name": "svc",
                "scripts": {"test": "pnpm test", "lint": "pnpm lint"},
                "dependencies": {"express": "^4.0.0", "prisma": "^5.0.0", "pino": "^9.0.0"},
            }
        ),
        encoding="utf-8",
    )
    ctx = build_practical_backend_context(
        run_id="run-1",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={
            "targets": [
                {"target_id": "api1", "target_class": "backend_service"},
                {"target_id": "worker1", "target_class": "worker"},
                {"target_id": "web1", "target_class": "web_app"},
            ]
        },
        compile_succeeded=True,
    )
    manifest, artifacts = build_execution_workspace(
        run_id="run-1",
        ir_document=_ir(),
        delivery_plan_obj={
            "targets": [
                {"target_id": "api1", "target_class": "backend_service"},
                {"target_id": "worker1", "target_class": "worker"},
                {"target_id": "web1", "target_class": "web_app"},
            ]
        },
        project_root=tmp_path,
        practical_backend_handoff=ctx["handoff"],
    )
    assert manifest["artifact_role"] == "authoritative_generated_workspace"
    assert manifest["practical_generation_proof"] is True
    assert manifest["generation_mode"] == "backend_ir_materialized_workspace"
    practical = manifest["practical_backend_generation"]
    assert practical["selected_runtime_plugin"] == "typescript_node"
    assert practical["backend_ir_ref"]["path"].endswith(".backend_ir.json")
    assert practical["backend_api_contract_index_ref"]["path"].endswith(".backend_api_contract_index.json")
    assert practical["api_contract_refs"][0]["openapi_rel_path"].endswith(".api1.openapi.json")
    api_target = next(row for row in manifest["targets"] if row["target_id"] == "api1")
    assert api_target["workspace_rel_dir"] == "services/api-backend"
    assert api_target["generation_source"] == "practical_backend_ir"
    assert api_target["health_contract"]["readiness_path"] == "/healthz"
    assert api_target["api_contract_ref"]["openapi_rel_path"].endswith(".api1.openapi.json")
    contract_artifact = next(a for a in artifacts if a.path.endswith("shared/targets.contract.json"))
    contract_obj = json.loads(contract_artifact.text())
    assert contract_obj["artifact_role"] == "authoritative_generated_workspace"
    assert contract_obj["api_contract_refs"][0]["openapi_rel_path"].endswith(".api1.openapi.json")
    assert contract_obj["targets"] == manifest["targets"]
    assert manifest["frontend_integration_summary"]["targets"][0]["integrationMode"] == "diagnostics_only"
    backend_client = next(a for a in artifacts if a.path.endswith("apps/universal/src/generated/backend-client.js"))
    assert "fetchAllBackendHealth" in backend_client.text()
    assert "frontendIntegrationSummary" in backend_client.text()


def test_build_execution_workspace_generates_feature_modules_for_full_contracts(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "userController.ts").write_text("export const x = 1;\n", encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "api_smoke.test.ts").write_text("it('smoke', () => {});\n", encoding="utf-8")
    (tmp_path / "package.json").write_text(
        json.dumps(
            {
                "name": "svc",
                "scripts": {"test": "pnpm test"},
                "dependencies": {"express": "^4.0.0"},
            }
        ),
        encoding="utf-8",
    )
    ctx = build_practical_backend_context(
        run_id="run-full",
        ir_document=_ir_with_full_contract(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={
            "targets": [
                {"target_id": "api1", "target_class": "backend_service"},
                {"target_id": "web1", "target_class": "web_app"},
            ]
        },
        compile_succeeded=True,
    )
    manifest, artifacts = build_execution_workspace(
        run_id="run-full",
        ir_document=_ir_with_full_contract(),
        delivery_plan_obj={
            "targets": [
                {"target_id": "api1", "target_class": "backend_service"},
                {"target_id": "web1", "target_class": "web_app"},
            ]
        },
        project_root=tmp_path,
        practical_backend_handoff=ctx["handoff"],
    )
    contract_ref = manifest["api_contract_refs"][0]
    assert contract_ref["contract_depth"] == "full"
    assert contract_ref["feature_groups"] == ["users"]
    assert contract_ref["auth_modes"] == ["bearer"]
    assert contract_ref["operation_count"] == 4
    target_summary = manifest["frontend_integration_summary"]["targets"][0]
    assert target_summary["integrationMode"] == "feature_modules"
    assert target_summary["featureGroups"] == ["users"]
    assert manifest["frontend_integration_summary"]["generatedFeatureModuleCount"] == 1
    assert any(a.path.endswith("apps/universal/src/generated/backend/runtime.js") for a in artifacts)
    assert any(a.path.endswith("apps/universal/src/generated/backend/auth.js") for a in artifacts)
    assert any(a.path.endswith("apps/universal/src/generated/backend/diagnostics.js") for a in artifacts)
    assert any(a.path.endswith("apps/universal/src/generated/backend/features/api1-users.js") for a in artifacts)
    runtime_artifact = next(a for a in artifacts if a.path.endswith("apps/universal/src/generated/backend/runtime.js"))
    assert "executeOperation" in runtime_artifact.text()
    assert "QueryClient" in runtime_artifact.text()
    feature_artifact = next(
        a for a in artifacts if a.path.endswith("apps/universal/src/generated/backend/features/api1-users.js")
    )
    assert "useListUsersQuery" in feature_artifact.text()
    assert "useCreateUserMutation" in feature_artifact.text()
    auth_artifact = next(a for a in artifacts if a.path.endswith("apps/universal/src/generated/backend/auth.js"))
    assert "SecureStore" in auth_artifact.text()
    assert "Platform.OS === 'web'" in auth_artifact.text()


def test_build_execution_workspace_uses_external_client_generator_when_configured(
    tmp_path: Path,
    monkeypatch,
) -> None:
    generator = tmp_path / "fake-openapi-generator"
    generator.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import pathlib, sys",
                "args = sys.argv[1:]",
                "out_dir = pathlib.Path(args[args.index('-o') + 1])",
                "out_dir.mkdir(parents=True, exist_ok=True)",
                "(out_dir / 'sdk.ts').write_text('export const generated = true\\n', encoding='utf-8')",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    generator.chmod(generator.stat().st_mode | stat.S_IXUSR)
    monkeypatch.setenv("AKC_OPENAPI_CLIENT_GENERATOR_CMD", str(generator))

    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "userController.ts").write_text("export const x = 1;\n", encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "api_smoke.test.ts").write_text("it('smoke', () => {});\n", encoding="utf-8")
    (tmp_path / "package.json").write_text(
        json.dumps(
            {
                "name": "svc",
                "scripts": {"test": "pnpm test"},
                "dependencies": {"express": "^4.0.0"},
            }
        ),
        encoding="utf-8",
    )
    ctx = build_practical_backend_context(
        run_id="run-ext",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        compile_succeeded=True,
    )
    manifest, artifacts = build_execution_workspace(
        run_id="run-ext",
        ir_document=_ir(),
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        project_root=tmp_path,
        practical_backend_handoff=ctx["handoff"],
    )
    assert manifest["client_codegen_summary"]["mode"] == "external_openapi_generator"
    assert manifest["toolchain"]["client_binding_generator"]["mode"] == "external_openapi_generator"
    assert (
        manifest["toolchain"]["client_binding_generator"]["additional_properties"]["useSingleRequestParameter"] is True
    )
    assert any(a.path.endswith("apps/universal/src/generated/api/api1/external/sdk.ts") for a in artifacts)


def test_practical_backend_context_selects_go_and_blocks_without_plugin(tmp_path: Path) -> None:
    (tmp_path / "cmd").mkdir()
    (tmp_path / "cmd" / "api.go").write_text("package main\nfunc main() {}\n", encoding="utf-8")
    (tmp_path / "go.mod").write_text("module example.com/svc\n\ngo 1.22\n", encoding="utf-8")

    ctx = build_practical_backend_context(
        run_id="run-go",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        compile_succeeded=True,
    )
    decision = ctx["runtime_plugin_decision"]
    result = ctx["practical_generation_result"]
    assert decision["plugin_id"] == "go"
    assert decision["availability"] == "builtin"
    assert result["selected_runtime_plugin"] == "go"
    assert result["blocked_stage"] is None
    assert result["status"] == "succeeded"
    assert result["execution_workspace_role"] == "authoritative_generated_workspace"


def test_build_execution_workspace_materializes_builtin_go_runtime(tmp_path: Path) -> None:
    (tmp_path / "cmd").mkdir()
    (tmp_path / "cmd" / "api.go").write_text("package main\nfunc main() {}\n", encoding="utf-8")
    (tmp_path / "go.mod").write_text("module example.com/svc\n\ngo 1.22\n", encoding="utf-8")

    ctx = build_practical_backend_context(
        run_id="run-go-blocked",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        compile_succeeded=True,
    )
    manifest, artifacts = build_execution_workspace(
        run_id="run-go-blocked",
        ir_document=_ir(),
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        project_root=tmp_path,
        practical_backend_handoff=ctx["handoff"],
    )
    assert manifest["requested_runtime_plugin"] == "go"
    assert manifest["runtime_profile"] == "go"
    assert manifest["materialization_status"] == "builtin_authoritative"
    assert manifest["artifact_role"] == "authoritative_generated_workspace"
    assert any(a.path.endswith("services/api-backend/Dockerfile") for a in artifacts)
    assert any(a.path.endswith("services/api-backend/main.go") for a in artifacts)


def test_build_execution_workspace_materializes_builtin_rust_runtime(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "api.rs").write_text("fn main() {}\n", encoding="utf-8")
    (tmp_path / "Cargo.toml").write_text(
        '[package]\nname = "svc"\nversion = "0.1.0"\nedition = "2021"\n',
        encoding="utf-8",
    )

    ctx = build_practical_backend_context(
        run_id="run-rust",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        compile_succeeded=True,
    )
    manifest, artifacts = build_execution_workspace(
        run_id="run-rust",
        ir_document=_ir(),
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        project_root=tmp_path,
        practical_backend_handoff=ctx["handoff"],
    )
    assert manifest["requested_runtime_plugin"] == "rust"
    assert manifest["runtime_profile"] == "rust"
    assert manifest["materialization_status"] == "builtin_authoritative"
    assert manifest["artifact_role"] == "authoritative_generated_workspace"
    assert any(a.path.endswith("services/api-backend/Cargo.toml") for a in artifacts)
    assert any(a.path.endswith("services/api-backend/src/main.rs") for a in artifacts)


def test_build_execution_workspace_materializes_builtin_java_runtime(tmp_path: Path) -> None:
    java_dir = tmp_path / "src" / "main" / "java" / "com" / "example"
    java_dir.mkdir(parents=True)
    (java_dir / "UserController.java").write_text("class UserController {}\n", encoding="utf-8")
    (tmp_path / "pom.xml").write_text(
        "\n".join(
            [
                "<project>",
                "  <modelVersion>4.0.0</modelVersion>",
                "  <groupId>com.example</groupId>",
                "  <artifactId>svc</artifactId>",
                "  <version>0.1.0</version>",
                "  <dependencies>",
                "    <dependency>",
                "      <groupId>org.springframework.boot</groupId>",
                "      <artifactId>spring-boot-starter-web</artifactId>",
                "      <version>3.3.2</version>",
                "    </dependency>",
                "  </dependencies>",
                "</project>",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    ctx = build_practical_backend_context(
        run_id="run-java",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        compile_succeeded=True,
    )
    manifest, artifacts = build_execution_workspace(
        run_id="run-java",
        ir_document=_ir(),
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        project_root=tmp_path,
        practical_backend_handoff=ctx["handoff"],
    )
    assert manifest["requested_runtime_plugin"] == "java"
    assert manifest["runtime_profile"] == "java"
    assert manifest["materialization_status"] == "builtin_authoritative"
    assert manifest["artifact_role"] == "authoritative_generated_workspace"
    assert any(a.path.endswith("services/api-backend/pom.xml") for a in artifacts)
    assert any(a.path.endswith("services/api-backend/Dockerfile") for a in artifacts)


def test_practical_backend_context_surfaces_invalid_plugin_manifest(tmp_path: Path) -> None:
    generator_dir = tmp_path / ".akc" / "backend_generators"
    generator_dir.mkdir(parents=True)
    (generator_dir / "go.json").write_text(
        json.dumps(
            {
                "plugin_id": "go",
                "interface_version": 1,
                "runtime_family": "go",
                "source": "repo_local",
                "materializer_kind": "command",
                "supports_authoritative_workspace": True,
                "supported_frameworks": ["gin"],
                "supported_languages": ["go"],
                "required_native_command_kinds": ["test", "build"],
                "unsupported_behavior": "blocked",
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "cmd").mkdir()
    (tmp_path / "cmd" / "api.go").write_text("package main\nfunc main() {}\n", encoding="utf-8")
    (tmp_path / "go.mod").write_text("module example.com/svc\n\ngo 1.22\n", encoding="utf-8")

    ctx = build_practical_backend_context(
        run_id="run-go-invalid-plugin",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        compile_succeeded=True,
    )
    assert any(
        "plugin_manifest_invalid:go.json:missing_command" in reason
        for reason in ctx["practical_generation_result"]["blocked_reasons"]
    )


def test_build_execution_workspace_materializes_external_plugin(tmp_path: Path) -> None:
    generator_dir = tmp_path / ".akc" / "backend_generators"
    generator_dir.mkdir(parents=True)
    generator = tmp_path / "fake-go-generator"
    generator.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json, pathlib, sys",
                "req = json.loads(sys.stdin.read())",
                "out_root = pathlib.Path(req['output_root'])",
                "(out_root / 'services' / 'api-backend').mkdir(parents=True, exist_ok=True)",
                "(out_root / 'services' / 'api-backend' / 'main.go').write_text('package main\\n', encoding='utf-8')",
                "json.dump({",
                "  'status': 'succeeded',",
                "  'requested_runtime_plugin': req['requested_runtime_plugin'],",
                "  'materialized_runtime_profile': 'go',",
                "  'artifact_role': 'authoritative_generated_workspace',",
                "  'generation_mode': 'backend_ir_materialized_workspace',",
                "  'targets': [",
                "    {",
                "      'target_id': 'api1',",
                "      'name': 'api backend',",
                "      'kind': 'service',",
                "      'target_class': 'backend_service',",
                "      'runtime_profile': 'go',",
                "      'workspace_rel_dir': 'services/api-backend',",
                "      'entry_rel_path': 'services/api-backend/main.go',",
                "      'workspace_role': 'backend_service_lane',",
                "      'generation_source': 'external_plugin'",
                "    }",
                "  ],",
                "  'toolchain': {'go': 'go', 'http_contract_format': 'openapi_3_1'},",
                "  'build_profiles': {},",
                "  'build_entrypoints': {},",
                "  'expected_outputs': {},",
                "  'client_codegen_summary': {},",
                "  'frontend_integration_summary': {",
                "    'targets': [],",
                "    'generatedFeatureModules': [],",
                "    'generatedOperationCount': 0",
                "  }",
                "}, sys.stdout)",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    generator.chmod(generator.stat().st_mode | stat.S_IXUSR)
    (generator_dir / "go.json").write_text(
        json.dumps(
            {
                "plugin_id": "go",
                "interface_version": 1,
                "runtime_family": "go",
                "source": "repo_local",
                "materializer_kind": "command",
                "supports_authoritative_workspace": True,
                "supported_frameworks": ["gin"],
                "supported_languages": ["go"],
                "required_native_command_kinds": ["test", "build"],
                "unsupported_behavior": "blocked",
                "command": [str(generator)],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "cmd").mkdir()
    (tmp_path / "cmd" / "api.go").write_text("package main\nfunc main() {}\n", encoding="utf-8")
    (tmp_path / "go.mod").write_text("module example.com/svc\n\ngo 1.22\n", encoding="utf-8")

    ctx = build_practical_backend_context(
        run_id="run-go-plugin",
        ir_document=_ir(),
        intent_spec=None,
        project_root=tmp_path,
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        compile_succeeded=True,
    )
    manifest, artifacts = build_execution_workspace(
        run_id="run-go-plugin",
        ir_document=_ir(),
        delivery_plan_obj={"targets": [{"target_id": "api1", "target_class": "backend_service"}]},
        project_root=tmp_path,
        practical_backend_handoff=ctx["handoff"],
    )
    assert manifest["requested_runtime_plugin"] == "go"
    assert manifest["runtime_profile"] == "go"
    assert manifest["materialization_status"] == "succeeded"
    assert manifest["artifact_role"] == "authoritative_generated_workspace"
    assert manifest["toolchain"]["go"] == "go"
    assert any(a.path.endswith("services/api-backend/main.go") for a in artifacts)
