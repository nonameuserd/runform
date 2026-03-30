"""Concrete packaging lanes backed by generated execution workspace artifacts."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from hashlib import sha256
from pathlib import Path
from typing import Any, Final, cast

from akc.delivery.ingest import load_operator_prereqs_manifest, probe_web_hosting_endpoint
from akc.delivery.packaging_adapter import PackagingAdapter, PackagingResult
from akc.delivery.types import DeliveryPlatform, PackagingMode, PlatformBuildSpec, ReleaseMode, StoreSubmitMode
from akc.delivery.versioning import PlatformProviderVersions

_PREFLIGHT_STRICT_ENV: Final[str] = "AKC_PACKAGING_ENFORCE_PREFLIGHT"
_EXECUTE_PACKAGING_ENV: Final[str] = "AKC_DELIVERY_EXECUTE_PACKAGING"
_EAS_AUTO_SUBMIT_ENV: Final[str] = "AKC_DELIVERY_EAS_AUTO_SUBMIT"
_TRUTHY: Final[frozenset[str]] = frozenset({"1", "true", "yes", "on"})
_FALSY: Final[frozenset[str]] = frozenset({"0", "false", "no", "off"})


def _env_truthy(name: str, *, default: bool = False) -> bool:
    raw = os.environ.get(name, "").strip().lower()
    if not raw:
        return default
    return raw in _TRUTHY


def execute_packaging_requested() -> bool:
    """Deprecated env-only packaging toggle kept for compatibility with older callers."""

    return _env_truthy(_EXECUTE_PACKAGING_ENV, default=False)


def packaging_mode_for_spec(spec: PlatformBuildSpec | None = None) -> PackagingMode:
    raw = ""
    if spec is not None:
        raw = str(spec.metadata.get("packaging_mode") or "").strip().lower()
    if raw in {"execute", "plan"}:
        return cast(PackagingMode, raw)
    env = str(os.environ.get(_EXECUTE_PACKAGING_ENV, "") or "").strip().lower()
    if env in _TRUTHY:
        return "execute"
    if env in _FALSY:
        return "plan"
    return "execute"


def store_submit_mode_for_spec(spec: PlatformBuildSpec) -> StoreSubmitMode:
    if "store" not in spec.release_lanes:
        return "manual"
    raw = str(spec.metadata.get("store_submit_mode") or "").strip().lower()
    if raw in {"auto", "manual"}:
        return cast(StoreSubmitMode, raw)
    env = str(os.environ.get(_EAS_AUTO_SUBMIT_ENV, "") or "").strip().lower()
    if env in _TRUTHY:
        return "auto"
    if env in _FALSY:
        return "manual"
    return "auto"


def enforce_packaging_preflight(*, release_mode: ReleaseMode | str | None = None) -> bool:
    """Return True when packaging should fail closed for missing execution prerequisites."""

    v = os.environ.get(_PREFLIGHT_STRICT_ENV, "").strip().lower()
    if v in _TRUTHY:
        return True
    if v in _FALSY:
        return False
    rm = str(release_mode or "beta").strip().lower()
    return rm in {"store", "both"}


def _release_mode_for_spec(spec: PlatformBuildSpec) -> ReleaseMode:
    lanes = tuple(str(x).strip().lower() for x in spec.release_lanes if str(x).strip())
    lane_set = set(lanes)
    if lane_set == {"store"}:
        return "store"
    if lane_set == {"beta"}:
        return "beta"
    return "both"


def _metadata_mapping(spec: PlatformBuildSpec, key: str) -> dict[str, Any]:
    raw = spec.metadata.get(key)
    return dict(raw) if isinstance(raw, dict) else {}


def _execution_manifest_ref(spec: PlatformBuildSpec) -> str | None:
    raw = spec.metadata.get("execution_workspace_ref")
    if isinstance(raw, dict):
        path = raw.get("path")
        if isinstance(path, str) and path.strip():
            return path.strip()
    compile_handoff = _metadata_mapping(spec, "compile_handoff")
    raw2 = compile_handoff.get("execution_workspace_ref")
    if isinstance(raw2, dict):
        path = raw2.get("path")
        if isinstance(path, str) and path.strip():
            return path.strip()
    return None


def _load_execution_manifest(*, project_dir: Path, spec: PlatformBuildSpec) -> dict[str, Any] | None:
    rel = _execution_manifest_ref(spec)
    if not rel:
        return None
    path = (project_dir.resolve() / rel).resolve()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return raw if isinstance(raw, dict) else None


def _resolve_workspace_root(*, project_dir: Path, manifest: dict[str, Any]) -> Path:
    rel = str(manifest.get("workspace_root") or "").strip()
    if not rel:
        raise RuntimeError("execution workspace manifest missing workspace_root")
    return (project_dir.resolve() / rel).resolve()


def _package_manager_install_argv(package_manager: str) -> list[str]:
    pm = package_manager.strip().lower()
    if pm == "pnpm":
        return ["pnpm", "install", "--frozen-lockfile=false"]
    if pm == "yarn":
        return ["yarn", "install", "--non-interactive"]
    return ["npm", "install"]


def _last_json(stdout: str) -> dict[str, Any] | None:
    for line in reversed([ln.strip() for ln in stdout.splitlines() if ln.strip()]):
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    try:
        parsed = json.loads(stdout)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _run_packaging_command(*, argv: list[str], cwd: Path, env: dict[str, str] | None = None) -> dict[str, Any]:
    proc = subprocess.run(
        argv,
        cwd=str(cwd),
        env=dict(os.environ, **(env or {})),
        capture_output=True,
        text=True,
        check=False,
    )
    return {
        "argv": list(argv),
        "cwd": str(cwd),
        "exit_code": int(proc.returncode),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }


def _hash_tree(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    if not root.is_dir():
        return out
    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        out[str(path.relative_to(root))] = sha256(path.read_bytes()).hexdigest()
    return out


def _hash_file(path: Path) -> str | None:
    if not path.is_file():
        return None
    return sha256(path.read_bytes()).hexdigest()


def _string_or_none(raw: Any) -> str | None:
    if raw is None:
        return None
    s = str(raw).strip()
    return s or None


def _distribution_inputs_for_outputs(*, platform: DeliveryPlatform, outputs: dict[str, Any]) -> dict[str, Any]:
    execution_mode = str(outputs.get("execution_mode") or "").strip().lower()
    authoritative = str(outputs.get("artifact_authority") or "").strip().lower() == "authoritative"
    artifact_paths = dict(cast(dict[str, Any], outputs.get("artifact_paths") or {}))
    artifact_urls = dict(cast(dict[str, Any], outputs.get("artifact_urls") or {}))
    common = {
        "execution_workspace_root": _string_or_none(artifact_paths.get("execution_workspace_root")),
        "build_profile": _string_or_none(outputs.get("build_profile")),
    }

    def _row(*, provider_kind: str, ready: bool, extra: dict[str, Any]) -> dict[str, Any]:
        row = {
            "provider_kind": provider_kind,
            "ready": bool(ready) and execution_mode == "execute" and authoritative,
            **common,
        }
        for key in ("hosting_url", "eas_build_id", "build_url", "ipa_path", "aab_path", "apk_path"):
            row[key] = None
        row.update(extra)
        return row

    if platform == "web":
        hosting_url = _string_or_none(outputs.get("hosting_url")) or _string_or_none(artifact_urls.get("hosting_url"))
        ready = hosting_url is not None
        return {
            "beta": _row(
                provider_kind="web_invite",
                ready=ready,
                extra={"hosting_url": hosting_url},
            ),
            "store": _row(
                provider_kind="web_invite",
                ready=ready,
                extra={"hosting_url": hosting_url},
            ),
        }

    eas_build_id = _string_or_none(outputs.get("eas_build_id")) or _string_or_none(
        dict(cast(dict[str, Any], outputs.get("build_result") or {})).get("build_id")
    )
    build_url = _string_or_none(artifact_urls.get("build_url")) or _string_or_none(
        dict(cast(dict[str, Any], outputs.get("build_result") or {})).get("artifact_url")
    )
    ipa_path = _string_or_none(outputs.get("ipa_path")) or _string_or_none(artifact_paths.get("ipa_path"))
    aab_path = _string_or_none(outputs.get("aab_path")) or _string_or_none(artifact_paths.get("aab_path"))
    apk_path = _string_or_none(outputs.get("apk_path")) or _string_or_none(artifact_paths.get("apk_path"))

    if platform == "ios":
        return {
            "beta": _row(
                provider_kind="testflight",
                ready=bool(eas_build_id or build_url),
                extra={
                    "eas_build_id": eas_build_id,
                    "build_url": build_url,
                    "ipa_path": ipa_path,
                },
            ),
            "store": _row(
                provider_kind="app_store_release",
                ready=bool(eas_build_id or build_url or ipa_path),
                extra={
                    "eas_build_id": eas_build_id,
                    "build_url": build_url,
                    "ipa_path": ipa_path,
                },
            ),
        }

    return {
        "beta": _row(
            provider_kind="firebase_app_distribution",
            ready=bool(apk_path or aab_path),
            extra={
                "eas_build_id": eas_build_id,
                "build_url": build_url,
                "aab_path": aab_path,
                "apk_path": apk_path,
            },
        ),
        "store": _row(
            provider_kind="google_play_release",
            ready=bool(eas_build_id or build_url or aab_path),
            extra={
                "eas_build_id": eas_build_id,
                "build_url": build_url,
                "aab_path": aab_path,
                "apk_path": apk_path,
            },
        ),
    }


def _attach_distribution_contract(*, platform: DeliveryPlatform, outputs: dict[str, Any]) -> None:
    outputs["distribution_inputs"] = _distribution_inputs_for_outputs(platform=platform, outputs=outputs)
    outputs.setdefault("distribution_results", {})


def _configured_web_base_url(*, project_dir: Path, spec: PlatformBuildSpec) -> str | None:
    raw = spec.metadata.get("web_invite_base_url")
    if isinstance(raw, str) and raw.strip():
        return raw.strip()
    env = os.environ.get("AKC_DELIVERY_WEB_INVITE_BASE_URL", "").strip()
    if env:
        return env
    op = load_operator_prereqs_manifest(project_dir)
    web = op.get("web")
    if isinstance(web, dict):
        for key in ("hosting_endpoint", "invite_base_url"):
            val = web.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return None


def _configured_web_hosting_provider(*, project_dir: Path, spec: PlatformBuildSpec) -> str | None:
    raw = spec.metadata.get("web_hosting_provider")
    if isinstance(raw, str) and raw.strip():
        return raw.strip().lower()
    env = os.environ.get("AKC_DELIVERY_WEB_HOSTING_PROVIDER", "").strip().lower()
    if env:
        return env
    op = load_operator_prereqs_manifest(project_dir)
    web = op.get("web")
    if isinstance(web, dict):
        val = web.get("hosting_provider")
        if isinstance(val, str) and val.strip():
            return val.strip().lower()
    detected = probe_web_hosting_endpoint(project_dir)
    if not detected:
        return None
    suffix = detected.removeprefix("detected:").strip().lower()
    if suffix == "vercel.json":
        return "vercel"
    if suffix == "netlify.toml":
        return "netlify"
    if suffix == "wrangler.toml":
        return "wrangler"
    if suffix == "fly.toml":
        return "fly"
    return None


def _coerce_command_tokens(raw: Any) -> list[str] | None:
    if isinstance(raw, list):
        tokens = [str(x).strip() for x in raw if str(x).strip()]
        return tokens or None
    if isinstance(raw, str) and raw.strip():
        tokens = [tok.strip() for tok in shlex.split(raw) if tok.strip()]
        return tokens or None
    return None


def _render_command_template(tokens: list[str], *, export_dir: Path, base_url: str | None) -> list[str]:
    rendered: list[str] = []
    for token in tokens:
        rendered.append(token.replace("{export_dir}", str(export_dir)).replace("{base_url}", str(base_url or "")))
    return rendered


def _configured_web_deploy_command(
    *,
    project_dir: Path,
    spec: PlatformBuildSpec,
    export_dir: Path,
    base_url: str | None,
) -> tuple[str | None, list[str] | None]:
    direct = _coerce_command_tokens(spec.metadata.get("web_deploy_command"))
    if direct is not None:
        provider = _configured_web_hosting_provider(project_dir=project_dir, spec=spec) or "custom"
        return provider, _render_command_template(direct, export_dir=export_dir, base_url=base_url)
    env = _coerce_command_tokens(os.environ.get("AKC_DELIVERY_WEB_DEPLOY_COMMAND"))
    if env is not None:
        provider = _configured_web_hosting_provider(project_dir=project_dir, spec=spec) or "custom"
        return provider, _render_command_template(env, export_dir=export_dir, base_url=base_url)
    op = load_operator_prereqs_manifest(project_dir)
    web = op.get("web")
    if isinstance(web, dict):
        op_cmd = _coerce_command_tokens(web.get("deploy_command"))
        if op_cmd is not None:
            provider = _configured_web_hosting_provider(project_dir=project_dir, spec=spec) or "custom"
            return provider, _render_command_template(op_cmd, export_dir=export_dir, base_url=base_url)
    hosting_provider = _configured_web_hosting_provider(project_dir=project_dir, spec=spec)
    if hosting_provider == "vercel":
        return hosting_provider, ["vercel", "deploy", str(export_dir), "--yes"]
    if hosting_provider == "netlify":
        return hosting_provider, ["netlify", "deploy", "--dir", str(export_dir), "--prod"]
    if hosting_provider == "wrangler":
        return hosting_provider, ["wrangler", "pages", "deploy", str(export_dir)]
    return hosting_provider, None


def _extract_deploy_id(parsed: dict[str, Any] | None) -> str | None:
    if not isinstance(parsed, dict):
        return None
    for key in ("deploymentId", "deployId", "id"):
        val = parsed.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    deployment = parsed.get("deployment")
    if isinstance(deployment, dict):
        val = deployment.get("id")
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _extract_deploy_url(parsed: dict[str, Any] | None) -> str | None:
    if not isinstance(parsed, dict):
        return None
    for key in ("hosting_url", "url", "deploy_url", "deployment_url"):
        val = parsed.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    deployment = parsed.get("deployment")
    if isinstance(deployment, dict):
        val = deployment.get("url")
        if isinstance(val, str) and val.strip():
            return val.strip()
    return None


def _expo_auth_configured(project_dir: Path) -> bool:
    if os.environ.get("EXPO_TOKEN", "").strip():
        return True
    op = load_operator_prereqs_manifest(project_dir)
    expo = op.get("expo")
    return bool(isinstance(expo, dict) and expo.get("access_token_configured"))


def _expo_project_configured(project_dir: Path, manifest: dict[str, Any]) -> bool:
    expo = manifest.get("expo")
    if isinstance(expo, dict):
        project_id = expo.get("project_id")
        if isinstance(project_id, str) and project_id.strip():
            return True
    op = load_operator_prereqs_manifest(project_dir)
    expo_cfg = op.get("expo")
    return bool(isinstance(expo_cfg, dict) and expo_cfg.get("project_id"))


def _platform_identifier_present(platform: DeliveryPlatform, manifest: dict[str, Any]) -> bool:
    expo = manifest.get("expo")
    if not isinstance(expo, dict):
        return False
    if platform == "ios":
        value = expo.get("ios_bundle_identifier")
        return isinstance(value, str) and value.strip() != ""
    if platform == "android":
        value = expo.get("android_package")
        return isinstance(value, str) and value.strip() != ""
    return True


class _ExecutionWorkspacePackagingAdapter(PackagingAdapter):
    def preflight(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
    ) -> list[str]:
        if tenant_id.strip() != tenant_id or not tenant_id.strip():
            return ["invalid tenant_id for scoped packaging preflight"]
        if repo_id.strip() != repo_id or not repo_id.strip():
            return ["invalid repo_id for scoped packaging preflight"]
        if spec.tenant_id != tenant_id or spec.repo_id != repo_id:
            return ["PlatformBuildSpec tenant/repo mismatch (isolation check failed)"]

        manifest = _load_execution_manifest(project_dir=project_dir, spec=spec)
        if manifest is None:
            return ["execution workspace manifest missing from compile handoff"]

        if packaging_mode_for_spec(spec) == "plan":
            # Plan mode is artifact-first: compile handoff must exist, but execute-only
            # credentials and provider wiring should not block plan generation.
            return []

        issues = self._strict_issues(project_dir=project_dir.resolve(), spec=spec, manifest=manifest)
        if not enforce_packaging_preflight(release_mode=_release_mode_for_spec(spec)):
            return []
        return issues

    def _strict_issues(
        self,
        *,
        project_dir: Path,
        spec: PlatformBuildSpec,
        manifest: dict[str, Any],
    ) -> list[str]:
        raise NotImplementedError


class WebBundlePackagingAdapter(_ExecutionWorkspacePackagingAdapter):
    @property
    def lane(self) -> str:
        return "web_bundle"

    @property
    def platform(self) -> DeliveryPlatform:
        return "web"

    def _strict_issues(
        self,
        *,
        project_dir: Path,
        spec: PlatformBuildSpec,
        manifest: dict[str, Any],
    ) -> list[str]:
        issues: list[str] = []
        if not _expo_project_configured(project_dir, manifest=manifest):
            issues.append("web packaging: Expo project id not configured in execution workspace or operator prereqs")
        base_url = _configured_web_base_url(project_dir=project_dir, spec=spec)
        if base_url is None:
            issues.append("web packaging: hosting target/base URL is not configured")
        if packaging_mode_for_spec(spec) == "execute":
            provider, deploy_cmd = _configured_web_deploy_command(
                project_dir=project_dir,
                spec=spec,
                export_dir=project_dir / ".akc" / "delivery" / spec.delivery_id / "packaging" / "web" / "exported",
                base_url=base_url,
            )
            if not provider:
                issues.append("web packaging: hosting provider is not configured or detectable")
            if deploy_cmd is None:
                issues.append("web packaging: deploy command is not configured for the resolved hosting provider")
        return issues

    def package(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
        compile_run_id: str | None,
        provider_versions: PlatformProviderVersions,
    ) -> PackagingResult:
        _ = (tenant_id, repo_id)
        manifest = _load_execution_manifest(project_dir=project_dir, spec=spec)
        if manifest is None:
            return PackagingResult(ok=False, lane=self.lane, error="execution workspace manifest missing", outputs={})
        workspace_root = _resolve_workspace_root(project_dir=project_dir, manifest=manifest)
        app_dir = workspace_root / "apps" / "universal"
        staging = project_dir.resolve() / ".akc" / "delivery" / spec.delivery_id / "packaging" / "web"
        export_dir = staging / "exported"
        export_dir.mkdir(parents=True, exist_ok=True)
        package_manager = str(manifest.get("package_manager") or "npm")
        execution_mode = packaging_mode_for_spec(spec)
        base_url = _configured_web_base_url(project_dir=project_dir, spec=spec)
        provider, deploy_command = _configured_web_deploy_command(
            project_dir=project_dir,
            spec=spec,
            export_dir=export_dir,
            base_url=base_url,
        )
        command_plan = [
            _package_manager_install_argv(package_manager),
            ["npx", "expo", "export", "--platform", "web", "--output-dir", str(export_dir)],
        ]
        if deploy_command is not None:
            command_plan.append(list(deploy_command))
        command_logs: list[dict[str, Any]] = []
        outputs: dict[str, Any] = {
            "workspace_ref": _execution_manifest_ref(spec),
            "execution_mode": execution_mode,
            "artifact_authority": "planned" if execution_mode == "plan" else "authoritative",
            "distribution_ready": False,
            "artifact_paths": {
                "web_export_dir": str(export_dir),
                "execution_workspace_root": str(workspace_root),
            },
            "artifact_urls": {},
            "checksums": {},
            "planned_commands": command_plan,
            "command_logs": command_logs,
            "compile_run_id": compile_run_id,
            "provider_versions": {
                "delivery_version": provider_versions.delivery_version,
                "web_pwa_version": provider_versions.web_pwa_version,
            },
            "delivery_plan_bound": bool((spec.metadata.get("compile_handoff") or {}).get("delivery_plan_loaded")),
            "deploy_result": {
                "provider": provider,
                "deployment_id": None,
                "hosting_url": None,
                "planned": execution_mode == "plan",
                "expected_hosting_url": base_url,
            },
        }
        if execution_mode == "execute" and deploy_command is None:
            return PackagingResult(
                ok=False,
                lane=self.lane,
                error="web packaging requires a configured deploy command in execute mode",
                outputs=outputs,
            )

        if execution_mode == "execute":
            for argv in command_plan:
                log = _run_packaging_command(argv=argv, cwd=app_dir)
                command_logs.append(log)
                if int(log.get("exit_code", 1)) != 0:
                    outputs["failed_command"] = list(argv)
                    outputs["failed_command_index"] = len(command_logs) - 1
                    return PackagingResult(
                        ok=False,
                        lane=self.lane,
                        error=f"web packaging command failed: {' '.join(argv)}",
                        outputs=outputs,
                    )
            parsed = _last_json(str(command_logs[-1].get("stdout") or "")) if command_logs else None
            deploy_url = _extract_deploy_url(parsed) or base_url
            deploy_id = _extract_deploy_id(parsed)
            checksums = _hash_tree(export_dir)
            if not checksums:
                return PackagingResult(
                    ok=False,
                    lane=self.lane,
                    error="web packaging did not produce an exported bundle",
                    outputs=outputs,
                )
            if not deploy_url:
                return PackagingResult(
                    ok=False,
                    lane=self.lane,
                    error="web packaging did not resolve a deployed hosting URL",
                    outputs=outputs,
                )
            outputs["checksums"] = checksums
            outputs["artifact_urls"] = {"hosting_url": deploy_url}
            outputs["hosting_url"] = deploy_url
            outputs["distribution_ready"] = True
            outputs["deploy_result"] = {
                "provider": provider,
                "deployment_id": deploy_id,
                "hosting_url": deploy_url,
                "planned": False,
            }
        else:
            source_dir = app_dir / "web"
            if source_dir.is_dir():
                for src in source_dir.rglob("*"):
                    if not src.is_file():
                        continue
                    rel = src.relative_to(source_dir)
                    dst = export_dir / rel
                    dst.parent.mkdir(parents=True, exist_ok=True)
                    dst.write_bytes(src.read_bytes())
            outputs["checksums"] = _hash_tree(export_dir)
        _attach_distribution_contract(platform=self.platform, outputs=outputs)
        return PackagingResult(
            ok=True,
            lane=self.lane,
            error=None,
            outputs=outputs,
        )


class _EasBuildPackagingAdapter(_ExecutionWorkspacePackagingAdapter):
    platform_name: DeliveryPlatform

    def _strict_issues(
        self,
        *,
        project_dir: Path,
        spec: PlatformBuildSpec,
        manifest: dict[str, Any],
    ) -> list[str]:
        _ = spec
        issues: list[str] = []
        if not _expo_auth_configured(project_dir):
            issues.append(f"{self.platform_name} packaging: EXPO_TOKEN / expo access token is not configured")
        if not _expo_project_configured(project_dir, manifest=manifest):
            issues.append(f"{self.platform_name} packaging: Expo project id not configured")
        if not _platform_identifier_present(self.platform_name, manifest):
            issues.append(f"{self.platform_name} packaging: bundle/package identifier missing in execution manifest")
        return issues

    def _profile_name(self, spec: PlatformBuildSpec) -> str:
        return "production" if "store" in spec.release_lanes else "preview"

    def _base_outputs(
        self,
        *,
        spec: PlatformBuildSpec,
        workspace_root: Path,
        staging: Path,
        provider_versions: PlatformProviderVersions,
        manifest: dict[str, Any],
        execution_mode: PackagingMode,
        planned_commands: list[list[str]],
        command_logs: list[dict[str, Any]],
        eas_build: dict[str, Any] | None,
    ) -> dict[str, Any]:
        outputs: dict[str, Any] = {
            "workspace_ref": _execution_manifest_ref(spec),
            "execution_mode": execution_mode,
            "artifact_authority": "planned" if execution_mode == "plan" else "authoritative",
            "distribution_ready": False,
            "artifact_paths": {"execution_workspace_root": str(workspace_root), "staging_dir": str(staging)},
            "artifact_urls": {},
            "checksums": {},
            "planned_commands": planned_commands,
            "command_logs": command_logs,
            "toolchain": manifest.get("toolchain"),
            "build_profile": self._profile_name(spec),
            "compile_run_id": spec.compile_run_id,
            "build_result": {
                "platform": self.platform_name,
                "profile": self._profile_name(spec),
                "planned": execution_mode == "plan",
                "build_id": None,
                "artifact_url": None,
            },
            "store_submit_mode": store_submit_mode_for_spec(spec),
        }
        if self.platform_name == "ios":
            outputs["resolved_versions"] = {
                "delivery_version": provider_versions.delivery_version,
                "ios_marketing_version": provider_versions.ios_marketing_version,
                "ios_build_number": provider_versions.ios_build_number,
            }
        else:
            outputs["resolved_versions"] = {
                "delivery_version": provider_versions.delivery_version,
                "android_version_name": provider_versions.android_version_name,
                "android_version_code": provider_versions.android_version_code,
            }
        if isinstance(eas_build, dict):
            build_id = eas_build.get("id") or eas_build.get("buildId")
            artifacts = eas_build.get("artifacts")
            artifact_url = artifacts.get("buildUrl") if isinstance(artifacts, dict) else eas_build.get("artifactUrl")
            if build_id:
                outputs["eas_build_id"] = str(build_id)
                outputs["build_result"]["build_id"] = str(build_id)
            if artifact_url:
                outputs["artifact_urls"] = {"build_url": str(artifact_url)}
                outputs["build_result"]["artifact_url"] = str(artifact_url)
            if self.platform_name == "ios":
                local = eas_build.get("localIpaPath") or eas_build.get("ipaPath")
                if local:
                    outputs["ipa_path"] = str(local)
                    outputs["artifact_paths"]["ipa_path"] = str(local)
                    hashed = _hash_file(Path(str(local)))
                    if hashed:
                        outputs["checksums"][str(local)] = hashed
            else:
                local_aab = eas_build.get("localAabPath") or eas_build.get("aabPath")
                local_apk = eas_build.get("localApkPath") or eas_build.get("apkPath")
                if local_aab:
                    outputs["aab_path"] = str(local_aab)
                    outputs["artifact_paths"]["aab_path"] = str(local_aab)
                    hashed = _hash_file(Path(str(local_aab)))
                    if hashed:
                        outputs["checksums"][str(local_aab)] = hashed
                if local_apk:
                    outputs["apk_path"] = str(local_apk)
                    outputs["artifact_paths"]["apk_path"] = str(local_apk)
                    hashed = _hash_file(Path(str(local_apk)))
                    if hashed:
                        outputs["checksums"][str(local_apk)] = hashed
        outputs["distribution_ready"] = (
            bool(
                outputs.get("eas_build_id")
                or outputs["artifact_urls"].get("build_url")
                or outputs["artifact_paths"].get("ipa_path")
                or outputs["artifact_paths"].get("aab_path")
                or outputs["artifact_paths"].get("apk_path")
            )
            and execution_mode == "execute"
        )
        _attach_distribution_contract(platform=self.platform_name, outputs=outputs)
        return outputs

    def package(
        self,
        *,
        project_dir: Path,
        tenant_id: str,
        repo_id: str,
        spec: PlatformBuildSpec,
        compile_run_id: str | None,
        provider_versions: PlatformProviderVersions,
    ) -> PackagingResult:
        _ = (tenant_id, repo_id, compile_run_id)
        manifest = _load_execution_manifest(project_dir=project_dir, spec=spec)
        if manifest is None:
            return PackagingResult(ok=False, lane=self.lane, error="execution workspace manifest missing", outputs={})
        workspace_root = _resolve_workspace_root(project_dir=project_dir, manifest=manifest)
        app_dir = workspace_root / "apps" / "universal"
        staging = project_dir.resolve() / ".akc" / "delivery" / spec.delivery_id / "packaging" / self.platform_name
        staging.mkdir(parents=True, exist_ok=True)
        package_manager = str(manifest.get("package_manager") or "npm")
        profile = self._profile_name(spec)
        execution_mode = packaging_mode_for_spec(spec)
        planned_commands: list[list[str]] = [
            _package_manager_install_argv(package_manager),
            ["eas", "build", "--platform", self.platform_name, "--profile", profile, "--non-interactive", "--json"],
        ]

        command_logs: list[dict[str, Any]] = []
        eas_build: dict[str, Any] | None = None
        outputs = self._base_outputs(
            spec=spec,
            workspace_root=workspace_root,
            staging=staging,
            provider_versions=provider_versions,
            manifest=manifest,
            execution_mode=execution_mode,
            planned_commands=planned_commands,
            command_logs=command_logs,
            eas_build=None,
        )
        if execution_mode == "execute":
            for idx, argv in enumerate(planned_commands):
                log = _run_packaging_command(argv=argv, cwd=app_dir)
                command_logs.append(log)
                if int(log.get("exit_code", 1)) != 0:
                    outputs["failed_command"] = list(argv)
                    outputs["failed_command_index"] = idx
                    return PackagingResult(
                        ok=False,
                        lane=self.lane,
                        error=f"{self.platform_name} packaging command failed: {' '.join(argv)}",
                        outputs=outputs,
                    )
                parsed = _last_json(str(log.get("stdout") or ""))
                if idx == 1:
                    eas_build = parsed

        outputs = self._base_outputs(
            spec=spec,
            workspace_root=workspace_root,
            staging=staging,
            provider_versions=provider_versions,
            manifest=manifest,
            execution_mode=execution_mode,
            planned_commands=planned_commands,
            command_logs=command_logs,
            eas_build=eas_build,
        )
        if execution_mode == "execute" and not bool(outputs.get("distribution_ready")):
            return PackagingResult(
                ok=False,
                lane=self.lane,
                error=f"{self.platform_name} packaging completed without authoritative build outputs",
                outputs=outputs,
            )
        return PackagingResult(ok=True, lane=self.lane, error=None, outputs=outputs)


class IosBuildPackagingAdapter(_EasBuildPackagingAdapter):
    platform_name: DeliveryPlatform = "ios"

    @property
    def lane(self) -> str:
        return "ios_build"

    @property
    def platform(self) -> DeliveryPlatform:
        return "ios"


class AndroidBuildPackagingAdapter(_EasBuildPackagingAdapter):
    platform_name: DeliveryPlatform = "android"

    @property
    def lane(self) -> str:
        return "android_build"

    @property
    def platform(self) -> DeliveryPlatform:
        return "android"


_WEB = WebBundlePackagingAdapter()
_IOS = IosBuildPackagingAdapter()
_ANDROID = AndroidBuildPackagingAdapter()

_PACKAGING_BY_PLATFORM: Final[dict[DeliveryPlatform, PackagingAdapter]] = {
    "web": _WEB,
    "ios": _IOS,
    "android": _ANDROID,
}

ALL_PACKAGING_ADAPTERS: Final[tuple[PackagingAdapter, ...]] = (_WEB, _IOS, _ANDROID)


def packaging_adapter_for(platform: DeliveryPlatform) -> PackagingAdapter:
    return _PACKAGING_BY_PLATFORM[platform]


def collect_packaging_preflight_issues(
    *,
    project_dir: Path,
    tenant_id: str,
    repo_id: str,
    delivery_id: str,
    delivery_version: str,
    platforms: list[str],
    release_mode: str,
    platform_metadata: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    from akc.delivery import adapters as distribution_adapters

    issues: list[dict[str, Any]] = []
    if release_mode not in ("beta", "store", "both"):
        raise ValueError(f"invalid release_mode: {release_mode!r}")
    lanes = distribution_adapters.release_lanes_for_mode(cast(ReleaseMode, release_mode))
    for plat_raw in platforms:
        if plat_raw not in ("web", "ios", "android"):
            continue
        platform = cast(DeliveryPlatform, plat_raw)
        adapter = packaging_adapter_for(platform)
        spec = PlatformBuildSpec(
            tenant_id=tenant_id,
            repo_id=repo_id,
            delivery_id=delivery_id,
            platform=platform,
            delivery_version=delivery_version,
            release_lanes=lanes,
            metadata=dict(platform_metadata or {}),
        )
        for reason in adapter.preflight(
            project_dir=project_dir,
            tenant_id=tenant_id,
            repo_id=repo_id,
            spec=spec,
        ):
            issues.append({"platform": platform, "lane": adapter.lane, "reason": reason})
    return issues
