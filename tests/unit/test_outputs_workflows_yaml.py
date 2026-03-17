from __future__ import annotations

import pytest

from akc.outputs.workflows import GithubActionsWorkflow, WorkflowJob, WorkflowStep, dump_yaml


def test_dump_yaml_is_deterministic_and_quotes_when_needed() -> None:
    payload = {
        "b": 2,
        "a": "hello world",
        "nested": {"z": "ok", "y": "needs:quote"},
        "list": ["plain", "two words", True, None, 3],
    }
    y = dump_yaml(payload)
    # Sorted keys: a then b then list then nested.
    assert y.splitlines()[0].startswith("a: hello world")
    assert 'y: "needs:quote"' in y
    assert "- true" in y
    assert "- null" in y


def test_workflow_renders_expected_keys_and_step_fields() -> None:
    wf = GithubActionsWorkflow(
        name="CI",
        on={"push": {"branches": ["main"]}, "pull_request": {}},
        jobs={
            "test": WorkflowJob(
                name="Run tests",
                runs_on="ubuntu-latest",
                steps=[
                    WorkflowStep(uses="actions/checkout@v4"),
                    WorkflowStep(
                        name="Run pytest",
                        run="pytest -q",
                        env={"PYTHONUNBUFFERED": "1"},
                        working_directory=".",
                    ),
                ],
            )
        },
    )
    y = wf.render_yaml()
    assert "name: CI\n" in y
    assert "on:\n" in y
    assert "jobs:\n" in y
    assert "test:\n" in y
    assert "runs-on: ubuntu-latest\n" in y
    assert "uses: actions/checkout@v4\n" in y
    assert "run: pytest -q\n" in y
    assert "working-directory: .\n" in y


def test_step_requires_uses_or_run() -> None:
    with pytest.raises(ValueError, match="either uses or run"):
        WorkflowStep()


def test_job_requires_steps_non_empty() -> None:
    with pytest.raises(ValueError, match="steps must be non-empty"):
        WorkflowJob(runs_on="ubuntu-latest", steps=[])


def test_workflow_to_artifact_defaults_to_github_workflows_dir_and_yml() -> None:
    wf = GithubActionsWorkflow(
        name="CI",
        on=["push"],
        jobs={
            "noop": WorkflowJob(runs_on="ubuntu-latest", steps=[WorkflowStep(run="echo ok")]),
        },
    )
    art = wf.to_artifact(filename="ci")
    assert art.path == ".github/workflows/ci.yml"
    assert art.media_type.startswith("application/yaml")
    assert "name: CI\n" in art.text()

