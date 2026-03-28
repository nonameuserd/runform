from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.compile.controller_config import Budget
from akc.intent.models import OperationalValidityParams
from akc.validation import (
    SUPPORTED_VALIDATOR_ADAPTER_ID,
    ValidatorBindingsConfigError,
    execute_validator_bindings,
    load_validator_bindings,
    resolve_validator_bindings_path,
)

# Import graph: touch compile config before ``akc.intent`` package init (avoids circular import on collect).
_: type[Budget] = Budget


def _write_bindings(path: Path, bindings: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "schema_kind": "validator_bindings",
                "bindings": bindings,
            }
        ),
        encoding="utf-8",
    )
    return path


def _specs_for(
    binding_id: str,
    *,
    evidence_type: str = "akc_observability_query_result",
) -> list[tuple[str, OperationalValidityParams]]:
    return [
        (
            "sc-1",
            OperationalValidityParams.from_mapping(
                {
                    "spec_version": 1,
                    "window": "single_run",
                    "predicate_kind": "presence",
                    "expected_evidence_types": [evidence_type],
                    "signals": [{"evidence_type": evidence_type, "validator_stub": binding_id}],
                }
            ),
        )
    ]


def test_load_validator_bindings_rejects_unknown_kind(tmp_path: Path) -> None:
    path = _write_bindings(tmp_path / "bindings.json", {"bad": {"kind": "shell"}})
    with pytest.raises(ValidatorBindingsConfigError, match="schema invalid"):
        load_validator_bindings(path=path)


def test_resolve_validator_bindings_path_prefers_project_override(tmp_path: Path) -> None:
    from akc.cli.project_config import AkcProjectConfig

    custom = tmp_path / "custom" / "validators.json"
    custom.parent.mkdir(parents=True, exist_ok=True)
    custom.write_text("{}", encoding="utf-8")
    resolved = resolve_validator_bindings_path(
        cwd=tmp_path,
        project=AkcProjectConfig(validation_bindings_path="custom/validators.json"),
        cli_value=None,
    )
    assert resolved == custom.resolve()


def test_resolve_validator_bindings_path_cli_overrides_project(tmp_path: Path) -> None:
    from akc.cli.project_config import AkcProjectConfig

    cli_file = tmp_path / "cli_only.json"
    project_file = tmp_path / "from_project.json"
    _write_bindings(cli_file, {})
    _write_bindings(project_file, {})
    resolved = resolve_validator_bindings_path(
        cwd=tmp_path,
        project=AkcProjectConfig(validation_bindings_path="from_project.json"),
        cli_value="cli_only.json",
    )
    assert resolved == cli_file.resolve()


def test_resolve_validator_bindings_path_discovers_default_file(tmp_path: Path) -> None:
    default = tmp_path / "configs" / "validation" / "validator_bindings.v1.yaml"
    default.parent.mkdir(parents=True, exist_ok=True)
    default.write_text(
        '{"schema_version": 1, "schema_kind": "validator_bindings", "bindings": {}}',
        encoding="utf-8",
    )
    resolved = resolve_validator_bindings_path(cwd=tmp_path, project=None, cli_value=None)
    assert resolved == default.resolve()


def test_execute_validator_bindings_observability_normalizes_http_response(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bindings_path = _write_bindings(
        tmp_path / "bindings.json",
        {
            "obs.login": {
                "kind": "logql_query",
                "url": "https://example.test/query",
                "query": '{app="mobile"}',
                "target": "loki-main",
            }
        },
    )

    class _Headers:
        @staticmethod
        def get_content_type() -> str:
            return "application/json"

    class _Response:
        headers = _Headers()

        def __enter__(self) -> _Response:
            return self

        def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
            return None

        @staticmethod
        def read() -> bytes:
            return b'{"count": 3, "healthy": true}'

    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: _Response())
    result = execute_validator_bindings(
        scope_root=tmp_path,
        run_id="run-1",
        runtime_run_id="runtime-1",
        specs=_specs_for("obs.login"),
        bindings_path=bindings_path,
        adapter_id=SUPPORTED_VALIDATOR_ADAPTER_ID,
    )
    assert len(result.evidence) == 1
    rec = result.evidence[0]
    assert rec.evidence_type == "akc_observability_query_result"
    assert rec.payload["binding_id"] == "obs.login"
    assert rec.payload["summary"] == {"count": 3, "healthy": True}


def test_execute_validator_bindings_missing_device_stub_emits_device_capture_error_artifact(
    tmp_path: Path,
) -> None:
    result = execute_validator_bindings(
        scope_root=tmp_path,
        run_id="run-1",
        runtime_run_id="runtime-1",
        specs=_specs_for("missing.capture", evidence_type="akc_device_capture_result"),
        bindings_path=_write_bindings(tmp_path / "b.json", {}),
        adapter_id=SUPPORTED_VALIDATOR_ADAPTER_ID,
    )
    assert result.evidence[0].evidence_type == "akc_device_capture_result"
    assert result.evidence[0].payload.get("capture_kind") == "blocked"


def test_execute_validator_bindings_blocks_under_native_adapter(tmp_path: Path) -> None:
    bindings_path = _write_bindings(
        tmp_path / "bindings.json",
        {
            "mobile.login.android": {
                "kind": "maestro_flow",
                "platform": "android",
                "journey_id": "login",
                "flow_path": "flows/login.yaml",
            }
        },
    )
    (tmp_path / "flows").mkdir()
    (tmp_path / "flows" / "login.yaml").write_text("appId: com.example\n", encoding="utf-8")
    result = execute_validator_bindings(
        scope_root=tmp_path,
        run_id="run-1",
        runtime_run_id="runtime-1",
        specs=_specs_for("mobile.login.android", evidence_type="akc_mobile_journey_result"),
        bindings_path=bindings_path,
        adapter_id="native",
    )
    assert result.binding_results[0]["status"] == "blocked"
    assert result.evidence[0].payload["status"] == "error"


def test_execute_validator_bindings_rejects_flow_path_outside_scope_root(tmp_path: Path) -> None:
    scope = tmp_path / "tenant-x" / "repo-y"
    scope.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    flow = outside / "flow.yaml"
    flow.write_text("appId: com.example\n", encoding="utf-8")
    bindings_path = _write_bindings(
        tmp_path / "registry.json",
        {
            "m.flow": {
                "kind": "maestro_flow",
                "platform": "android",
                "journey_id": "j1",
                "flow_path": str(flow.resolve()),
            }
        },
    )
    with pytest.raises(ValidatorBindingsConfigError, match="must be under scope_root"):
        execute_validator_bindings(
            scope_root=scope,
            run_id="run-1",
            runtime_run_id="runtime-1",
            specs=_specs_for("m.flow", evidence_type="akc_mobile_journey_result"),
            bindings_path=bindings_path,
            adapter_id=SUPPORTED_VALIDATOR_ADAPTER_ID,
        )


def test_load_validator_bindings_rejects_screenshot_artifact_name_with_path_segments(tmp_path: Path) -> None:
    path = _write_bindings(
        tmp_path / "bindings.json",
        {
            "bad.name": {
                "kind": "android_helper",
                "operation": "screenshot",
                "artifact_name": "../escape",
            }
        },
    )
    with pytest.raises(ValidatorBindingsConfigError, match="artifact_name"):
        load_validator_bindings(path=path)


def test_execute_validator_bindings_maestro_failure_still_writes_normalized_artifact(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bindings_path = _write_bindings(
        tmp_path / "bindings.json",
        {
            "mobile.login.android": {
                "kind": "maestro_flow",
                "platform": "android",
                "journey_id": "login",
                "flow_path": "flows/login.yaml",
            }
        },
    )
    (tmp_path / "flows").mkdir()
    (tmp_path / "flows" / "login.yaml").write_text("appId: com.example\n", encoding="utf-8")

    class _Failed:
        returncode = 1
        stdout = "err"
        stderr = "maestro failed"

    monkeypatch.setattr("subprocess.run", lambda *_args, **_kwargs: _Failed())
    result = execute_validator_bindings(
        scope_root=tmp_path,
        run_id="run-1",
        runtime_run_id="runtime-1",
        specs=_specs_for("mobile.login.android", evidence_type="akc_mobile_journey_result"),
        bindings_path=bindings_path,
        adapter_id=SUPPORTED_VALIDATOR_ADAPTER_ID,
    )
    assert result.evidence[0].payload["status"] == "failed"
    assert result.evidence[0].payload["assertions_failed"] == 1
    art_dir = tmp_path / ".akc" / "verification" / "validators" / "run-1"
    assert any(art_dir.glob("*.mobile_journey_result.json"))


def test_execute_validator_bindings_runs_maestro_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bindings_path = _write_bindings(
        tmp_path / "bindings.json",
        {
            "mobile.login.android": {
                "kind": "maestro_flow",
                "platform": "android",
                "journey_id": "login",
                "flow_path": "flows/login.yaml",
                "device_id": "emulator-5554",
            }
        },
    )
    (tmp_path / "flows").mkdir()
    (tmp_path / "flows" / "login.yaml").write_text("appId: com.example\n", encoding="utf-8")

    class _Completed:
        returncode = 0
        stdout = "passed"
        stderr = ""

    monkeypatch.setattr("subprocess.run", lambda *_args, **_kwargs: _Completed())
    result = execute_validator_bindings(
        scope_root=tmp_path,
        run_id="run-1",
        runtime_run_id="runtime-1",
        specs=_specs_for("mobile.login.android", evidence_type="akc_mobile_journey_result"),
        bindings_path=bindings_path,
        adapter_id=SUPPORTED_VALIDATOR_ADAPTER_ID,
    )
    assert result.evidence[0].evidence_type == "akc_mobile_journey_result"
    assert result.evidence[0].payload["status"] == "passed"


def test_execute_validator_bindings_runs_android_helper_capture(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    bindings_path = _write_bindings(
        tmp_path / "bindings.json",
        {
            "android.failure.screenshot": {
                "kind": "android_helper",
                "operation": "screenshot",
                "artifact_name": "final",
            }
        },
    )

    class _Completed:
        returncode = 0
        stdout = b"png-bytes"
        stderr = b""

    monkeypatch.setattr("subprocess.run", lambda *_args, **_kwargs: _Completed())
    result = execute_validator_bindings(
        scope_root=tmp_path,
        run_id="run-1",
        runtime_run_id="runtime-1",
        specs=_specs_for("android.failure.screenshot", evidence_type="akc_device_capture_result"),
        bindings_path=bindings_path,
        adapter_id=SUPPORTED_VALIDATOR_ADAPTER_ID,
    )
    assert result.evidence[0].evidence_type == "akc_device_capture_result"
    assert result.evidence[0].payload["status"] == "ok"
    assert str(result.evidence[0].payload["artifact_path"]).endswith(".final.png")


def test_execute_validator_bindings_runs_ios_helper_capture(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    bindings_path = _write_bindings(
        tmp_path / "bindings.json",
        {
            "ios.failure.screenshot": {
                "kind": "ios_simulator_helper",
                "operation": "screenshot",
                "device_id": "booted",
                "artifact_name": "final",
            }
        },
    )

    class _Completed:
        returncode = 0
        stdout = ""
        stderr = ""

    def _fake_run(
        argv: list[str],
        cwd: str,
        capture_output: bool,
        text: bool,
        timeout: float,
        env: dict[str, str],
        check: bool,
    ) -> _Completed:
        Path(argv[-1]).write_bytes(b"png")
        return _Completed()

    monkeypatch.setattr("subprocess.run", _fake_run)
    result = execute_validator_bindings(
        scope_root=tmp_path,
        run_id="run-1",
        runtime_run_id="runtime-1",
        specs=_specs_for("ios.failure.screenshot", evidence_type="akc_device_capture_result"),
        bindings_path=bindings_path,
        adapter_id=SUPPORTED_VALIDATOR_ADAPTER_ID,
    )
    assert result.evidence[0].payload["platform"] == "ios"
    assert result.evidence[0].payload["status"] == "ok"
