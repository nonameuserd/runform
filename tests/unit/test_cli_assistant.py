from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.assistant.engine import CommandExecutionResult
from akc.cli import main
from akc.compile.interfaces import LLMBackend, LLMRequest, LLMResponse, TenantRepoScope


def _run_main(argv: list[str], *, capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as excinfo:
        main(argv)
    io = capsys.readouterr()
    out = io.out
    err = io.err
    return int(excinfo.value.code), out, err


def _last_json(stdout: str) -> dict[str, object]:
    text = str(stdout).strip()
    assert text
    return json.loads(text)


def test_help_includes_assistant(capsys: pytest.CaptureFixture[str]) -> None:
    code, out, _ = _run_main(["--help"], capsys=capsys)
    assert code == 0
    assert "assistant" in out


def test_assistant_programmatic_plan_mode_json(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, out, _ = _run_main(
        [
            "assistant",
            "-p",
            "list recent runs",
            "--tenant-id",
            "tenant-a",
            "--outputs-root",
            str(tmp_path / "out"),
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code == 0
    payload = _last_json(out)
    assert payload["status"] == "planned"
    cmd = payload["suggested_command"]
    assert isinstance(cmd, list)
    assert cmd[:3] == ["control", "runs", "list"]


def test_assistant_uses_project_scope_defaults_when_cli_scope_unset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    akc_dir = tmp_path / ".akc"
    akc_dir.mkdir(parents=True)
    (akc_dir / "project.json").write_text(
        json.dumps(
            {
                "tenant_id": "t-file",
                "repo_id": "r-file",
                "outputs_root": str(tmp_path / "out-file"),
                "assistant_default_format": "json",
            }
        ),
        encoding="utf-8",
    )

    code, out, _ = _run_main(
        [
            "assistant",
            "-p",
            "list recent runs",
        ],
        capsys=capsys,
    )
    assert code == 0
    payload = _last_json(out)
    assert payload["status"] == "planned"
    cmd = payload["suggested_command"]
    assert isinstance(cmd, list)
    assert cmd[:3] == ["control", "runs", "list"]
    assert "--tenant-id" in cmd and cmd[cmd.index("--tenant-id") + 1] == "t-file"
    assert "--repo-id" in cmd and cmd[cmd.index("--repo-id") + 1] == "r-file"
    assert "--outputs-root" in cmd and cmd[cmd.index("--outputs-root") + 1] == str(tmp_path / "out-file")


def test_assistant_execute_mutation_requires_approval_then_approve_executes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    def _fake_exec(argv: object) -> CommandExecutionResult:
        toks = tuple(str(x) for x in (argv or ()))
        return CommandExecutionResult(argv=toks, exit_code=0, stdout="ok\n", stderr="")

    monkeypatch.setattr("akc.cli.assistant.execute_cli_command", _fake_exec)

    code1, out1, _ = _run_main(
        [
            "assistant",
            "--mode",
            "execute",
            "-p",
            "akc compile --tenant-id t1 --repo-id r1 --outputs-root out --mode quick",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code1 == 0
    payload1 = _last_json(out1)
    assert payload1["status"] == "approval_required"
    request_id = str(payload1["request_id"])
    session_id = str(payload1["session_id"])
    assert request_id
    assert session_id

    code2, out2, _ = _run_main(
        [
            "assistant",
            "--resume",
            session_id,
            "-p",
            f"/approve {request_id}",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code2 == 0
    payload2 = _last_json(out2)
    assert payload2["status"] == "executed"
    assert payload2["command_exit_code"] == 0


def test_assistant_execute_mutation_can_be_denied(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code1, out1, _ = _run_main(
        [
            "assistant",
            "--mode",
            "execute",
            "-p",
            "akc compile --tenant-id t1 --repo-id r1 --outputs-root out --mode quick",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code1 == 0
    payload1 = _last_json(out1)
    assert payload1["status"] == "approval_required"
    request_id = str(payload1["request_id"])
    session_id = str(payload1["session_id"])

    code2, out2, _ = _run_main(
        [
            "assistant",
            "--resume",
            session_id,
            "-p",
            f"/deny {request_id}",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code2 == 0
    payload2 = _last_json(out2)
    assert payload2["status"] == "session_updated"
    assert payload2["request_id"] == request_id


def test_assistant_execute_drift_is_read_only_and_does_not_require_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    def _fake_exec(argv: object) -> CommandExecutionResult:
        toks = tuple(str(x) for x in (argv or ()))
        return CommandExecutionResult(argv=toks, exit_code=0, stdout="ok\n", stderr="")

    monkeypatch.setattr("akc.cli.assistant.execute_cli_command", _fake_exec)

    code, out, _ = _run_main(
        [
            "assistant",
            "--mode",
            "execute",
            "-p",
            "akc drift --tenant-id t1 --repo-id r1 --outputs-root out --format json",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code == 0
    payload = _last_json(out)
    assert payload["status"] == "executed"
    assert payload["command_exit_code"] == 0


def test_assistant_execute_drift_update_baseline_requires_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, out, _ = _run_main(
        [
            "assistant",
            "--mode",
            "execute",
            "-p",
            "akc drift --tenant-id t1 --repo-id r1 --outputs-root out --update-baseline",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code == 0
    payload = _last_json(out)
    assert payload["status"] == "approval_required"


def test_assistant_memory_commands_and_trace(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code1, out1, _ = _run_main(
        [
            "assistant",
            "-p",
            "list recent runs",
            "--format",
            "json",
            "--memory-budget-tokens",
            "32",
        ],
        capsys=capsys,
    )
    assert code1 == 0
    payload1 = _last_json(out1)
    assert isinstance(payload1.get("memory_trace"), dict)
    session_id = str(payload1["session_id"])

    code2, out2, _ = _run_main(
        [
            "assistant",
            "--resume",
            session_id,
            "-p",
            "/memory",
            "--format",
            "json",
            "--memory-budget-tokens",
            "32",
        ],
        capsys=capsys,
    )
    assert code2 == 0
    payload2 = _last_json(out2)
    assert payload2["status"] == "session_updated"
    assert "Memory entries" in str(payload2["message"])


def test_assistant_referential_recall_applies_thorough_hint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code1, out1, _ = _run_main(
        [
            "assistant",
            "-p",
            "akc compile --tenant-id t1 --repo-id r1 --outputs-root out --mode quick",
            "--format",
            "json",
            "--memory-budget-tokens",
            "256",
        ],
        capsys=capsys,
    )
    assert code1 == 0
    payload1 = _last_json(out1)
    session_id = str(payload1["session_id"])
    assert payload1["status"] == "planned"

    code2, out2, _ = _run_main(
        [
            "assistant",
            "--resume",
            session_id,
            "-p",
            "run that again but thorough",
            "--format",
            "json",
            "--memory-budget-tokens",
            "256",
        ],
        capsys=capsys,
    )
    assert code2 == 0
    payload2 = _last_json(out2)
    assert payload2["status"] == "planned"
    cmd = payload2.get("suggested_command")
    assert isinstance(cmd, list)
    assert "--mode" in cmd
    assert cmd[cmd.index("--mode") + 1] == "thorough"


def test_assistant_session_v1_loads_and_is_rewritten_as_v2(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    session_id = "11111111-1111-1111-1111-111111111111"
    session_dir = tmp_path / ".akc" / "assistant" / "sessions"
    session_dir.mkdir(parents=True, exist_ok=True)
    session_path = session_dir / f"{session_id}.json"
    session_path.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "session_id": session_id,
                "created_at_ms": 1,
                "updated_at_ms": 1,
                "mode": "plan",
                "scope": {},
                "turns": [],
                "pending_actions": {},
                "last_suggested_command": None,
            }
        ),
        encoding="utf-8",
    )

    code, out, _ = _run_main(
        [
            "assistant",
            "--resume",
            session_id,
            "-p",
            "/memory",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code == 0
    payload = _last_json(out)
    assert payload["status"] == "session_updated"
    raw = json.loads(session_path.read_text(encoding="utf-8"))
    assert int(raw["schema_version"]) == 2


class _FakeHostedPlanner(LLMBackend):
    def complete(self, *, scope: TenantRepoScope, stage: str, request: LLMRequest) -> LLMResponse:
        _ = (scope, stage, request)
        return LLMResponse(
            text='{"argv":["control","runs","list","--tenant-id","t-hosted"],"message":"Hosted planner mapping."}'
        )


def test_assistant_hosted_planner_returns_suggested_command(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    def _stub_build_llm(*, config: object) -> _FakeHostedPlanner:  # noqa: ARG001
        return _FakeHostedPlanner()

    monkeypatch.setattr("akc.cli.assistant.build_llm_backend", _stub_build_llm)

    code, out, _ = _run_main(
        [
            "assistant",
            "-p",
            "show recent runs",
            "--llm-backend",
            "openai",
            "--llm-model",
            "gpt-test",
            "--llm-allow-network",
            "--format",
            "json",
        ],
        capsys=capsys,
    )
    assert code == 0
    payload = _last_json(out)
    assert payload["status"] == "planned"
    assert payload["llm_mode"] == "openai"
    assert payload["suggested_command"] == ["control", "runs", "list", "--tenant-id", "t-hosted"]
