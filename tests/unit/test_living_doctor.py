from __future__ import annotations

import json
from pathlib import Path

from akc.cli.living_doctor import (
    eval_suite_has_living_hooks,
    lease_single_writer_warnings,
    run_living_unattended_checks,
)
from akc.cli.project_config import AkcProjectConfig


def test_eval_suite_hooks_detects_keys(tmp_path: Path) -> None:
    p = tmp_path / "suite.json"
    p.write_text(json.dumps({"suite_version": "x"}), encoding="utf-8")
    ok, msg = eval_suite_has_living_hooks(p)
    assert ok is False
    assert "missing" in msg.lower()

    p.write_text(
        json.dumps(
            {
                "living_recompile_policy": {"granular_acceptance_triggers": True},
                "runtime_canary_thresholds": {"max_total_impacts": 1},
            }
        ),
        encoding="utf-8",
    )
    ok2, msg2 = eval_suite_has_living_hooks(p)
    assert ok2 is True
    assert msg2 == "ok"


def test_unattended_claim_requires_profile(tmp_path: Path) -> None:
    ok, lines = run_living_unattended_checks(
        cwd=tmp_path,
        env={"AKC_LIVING_UNATTENDED_CLAIM": "1"},
        project=None,
        tenant_id="t",
        repo_id="r",
        outputs_root=tmp_path,
        eval_suite_path=None,
        ingest_state_path=None,
        relaxed_baseline=True,
        living_automation_profile_cli=None,
    )
    assert ok is False
    assert any("not living_loop_unattended_v1" in line for line in lines)


def test_unattended_profile_checks_paths(tmp_path: Path) -> None:
    out = tmp_path / "out"
    scope = out / "t" / "r"
    (scope / ".akc" / "living").mkdir(parents=True)
    (scope / ".akc" / "living" / "baseline.json").write_text("{}", encoding="utf-8")
    ingest = tmp_path / "ingest.json"
    ingest.write_text("{}", encoding="utf-8")
    suite = tmp_path / "suite.json"
    suite.write_text(
        json.dumps(
            {
                "living_recompile_policy": {"x": 1},
                "runtime_canary_thresholds": {"y": 2},
            }
        ),
        encoding="utf-8",
    )

    ok, lines = run_living_unattended_checks(
        cwd=tmp_path,
        env={"AKC_LIVING_AUTOMATION_PROFILE": "living_loop_unattended_v1"},
        project=None,
        tenant_id="t",
        repo_id="r",
        outputs_root=out,
        eval_suite_path=suite,
        ingest_state_path=ingest,
        relaxed_baseline=False,
        living_automation_profile_cli=None,
    )
    assert ok is True
    assert any("eval suite hooks: ok" in line for line in lines)


def test_lease_single_writer_warns_k8s_pod_filesystem(tmp_path: Path) -> None:
    w = lease_single_writer_warnings(
        env={"KUBERNETES_SERVICE_HOST": "10.0.0.1"},
        lease_backend="filesystem",
        lease_namespace=None,
        expect_replicas=None,
    )
    assert any("KUBERNETES_SERVICE_HOST" in x for x in w)


def test_lease_single_writer_warns_multi_replica_filesystem(tmp_path: Path) -> None:
    w = lease_single_writer_warnings(
        env={},
        lease_backend="filesystem",
        lease_namespace=None,
        expect_replicas=2,
    )
    assert any("expect_replicas" in x.lower() for x in w)


def test_lease_single_writer_warns_k8s_without_namespace(tmp_path: Path) -> None:
    w = lease_single_writer_warnings(
        env={},
        lease_backend="k8s",
        lease_namespace=None,
        expect_replicas=1,
    )
    assert any("lease_namespace" in x.lower() for x in w)


def test_unattended_profile_emits_lease_warnings(tmp_path: Path) -> None:
    out = tmp_path / "out"
    scope = out / "t" / "r"
    (scope / ".akc" / "living").mkdir(parents=True)
    (scope / ".akc" / "living" / "baseline.json").write_text("{}", encoding="utf-8")
    ingest = tmp_path / "ingest.json"
    ingest.write_text("{}", encoding="utf-8")
    suite = tmp_path / "suite.json"
    suite.write_text(
        json.dumps(
            {
                "living_recompile_policy": {"x": 1},
                "runtime_canary_thresholds": {"y": 2},
            }
        ),
        encoding="utf-8",
    )

    ok, lines = run_living_unattended_checks(
        cwd=tmp_path,
        env={"KUBERNETES_SERVICE_HOST": "10.0.0.1"},
        project=None,
        tenant_id="t",
        repo_id="r",
        outputs_root=out,
        eval_suite_path=suite,
        ingest_state_path=ingest,
        relaxed_baseline=False,
        living_automation_profile_cli="living_loop_unattended_v1",
        lease_backend="filesystem",
    )
    assert ok is True
    assert any("KUBERNETES_SERVICE_HOST" in line for line in lines)


def test_project_claim_alignment(tmp_path: Path) -> None:
    proj = AkcProjectConfig(living_unattended_claim=True, living_automation_profile="living_loop_v1")
    ok, lines = run_living_unattended_checks(
        cwd=tmp_path,
        env={},
        project=proj,
        tenant_id="t",
        repo_id="r",
        outputs_root=tmp_path,
        eval_suite_path=None,
        ingest_state_path=None,
        relaxed_baseline=True,
        living_automation_profile_cli=None,
    )
    assert ok is False
    assert any("not living_loop_unattended_v1" in line for line in lines)
