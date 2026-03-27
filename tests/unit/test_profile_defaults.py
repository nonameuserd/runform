from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from akc.cli.profile_defaults import (
    ResolvedValue,
    normalize_developer_role_profile,
    resolve_compile_profile_defaults,
    resolve_developer_role_profile,
    resolve_optional_project_string,
)
from akc.cli.project_config import AkcProjectConfig, load_akc_project_config


@dataclass(frozen=True, slots=True)
class _Gov:
    rollout_stage: str | None = None
    compile_defaults: tuple[tuple[str, object], ...] = ()


def test_normalize_developer_role_profile_defaults_to_classic() -> None:
    assert normalize_developer_role_profile(None) == "classic"
    assert normalize_developer_role_profile("classic") == "classic"
    assert normalize_developer_role_profile("emerging") == "emerging"
    assert normalize_developer_role_profile("unknown") == "classic"


def test_resolve_developer_role_profile_cli_over_env_over_file(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps({"developer_role_profile": "emerging"}),
        encoding="utf-8",
    )
    assert (
        resolve_developer_role_profile(
            cli_value="classic",
            cwd=tmp_path,
            env={"AKC_DEVELOPER_ROLE_PROFILE": "emerging"},
        ).value
        == "classic"
    )
    assert (
        resolve_developer_role_profile(
            cli_value=None,
            cwd=tmp_path,
            env={"AKC_DEVELOPER_ROLE_PROFILE": "classic"},
        ).source
        == "env"
    )
    assert (
        resolve_developer_role_profile(
            cli_value=None,
            cwd=tmp_path,
            env={},
        ).value
        == "emerging"
    )
    assert (
        resolve_developer_role_profile(
            cli_value=None,
            cwd=tmp_path,
            env={},
            project=AkcProjectConfig(developer_role_profile="emerging"),
        ).value
        == "emerging"
    )


def test_resolve_developer_role_profile_legacy_when_unset(tmp_path: Path) -> None:
    assert resolve_developer_role_profile(cli_value=None, cwd=tmp_path, env={}).value == "classic"
    assert resolve_developer_role_profile(cli_value=None, cwd=tmp_path, env={}).source == "legacy_default"


def test_resolve_developer_role_profile_defaults_to_emerging_for_adoption_level_compiler(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "project.json").write_text(json.dumps({"adoption_level": "compiler"}), encoding="utf-8")
    r = resolve_developer_role_profile(cli_value=None, cwd=tmp_path, env={})
    assert r.value == "emerging"
    assert r.source == "profile_default"


def test_resolve_optional_project_string_precedence() -> None:
    assert resolve_optional_project_string(
        cli_value="a",
        env_key="AKC_TENANT_ID",
        file_value="b",
        env={"AKC_TENANT_ID": "c"},
    ) == ResolvedValue("a", "cli")
    r = resolve_optional_project_string(
        cli_value=None,
        env_key="AKC_TENANT_ID",
        file_value="b",
        env={"AKC_TENANT_ID": "c"},
    )
    assert r.value == "c" and r.source == "env"
    r2 = resolve_optional_project_string(
        cli_value=None,
        env_key="AKC_TENANT_ID",
        file_value="b",
        env={},
    )
    assert r2.value == "b" and r2.source == "project_file"


def test_load_akc_project_config_json(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps(
            {
                "developer_role_profile": "emerging",
                "tenant_id": "t-env",
                "repo_id": "r1",
                "outputs_root": "/tmp/out",
            }
        ),
        encoding="utf-8",
    )
    cfg = load_akc_project_config(tmp_path)
    assert cfg is not None
    assert cfg.developer_role_profile == "emerging"
    assert cfg.tenant_id == "t-env"
    assert cfg.repo_id == "r1"
    assert cfg.outputs_root == "/tmp/out"
    assert cfg.opa_policy_path is None
    assert cfg.opa_decision_path is None


def test_load_akc_project_config_compile_skill_byte_caps(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps(
            {
                "tenant_id": "t",
                "repo_id": "r",
                "outputs_root": "/tmp/out",
                "compile_skill_max_file_bytes": 120_000,
                "compile_skill_max_total_bytes": 30_000,
            }
        ),
        encoding="utf-8",
    )
    cfg = load_akc_project_config(tmp_path)
    assert cfg is not None
    assert cfg.compile_skill_max_file_bytes == 120_000
    assert cfg.compile_skill_max_total_bytes == 30_000


def test_load_akc_project_config_native_and_change_scope(tmp_path: Path) -> None:
    akc = tmp_path / ".akc"
    akc.mkdir(parents=True)
    (akc / "project.json").write_text(
        '{"native_test_mode": true, "change_scope_deny_categories": ["ci", "infra"]}\n',
        encoding="utf-8",
    )
    cfg = load_akc_project_config(tmp_path)
    assert cfg is not None
    assert cfg.native_test_mode is True
    assert cfg.change_scope_deny_categories == ("ci", "infra")


def test_load_akc_project_config_json_opa_fields(tmp_path: Path) -> None:
    (tmp_path / ".akc").mkdir(parents=True)
    (tmp_path / ".akc" / "project.json").write_text(
        json.dumps(
            {
                "developer_role_profile": "classic",
                "tenant_id": "t-env",
                "repo_id": "r1",
                "outputs_root": "/tmp/out",
                "opa_policy_path": ".akc/policy/compile_tools.rego",
                "opa_decision_path": "data.akc.allow",
            }
        ),
        encoding="utf-8",
    )
    cfg = load_akc_project_config(tmp_path)
    assert cfg is not None
    assert cfg.opa_policy_path == ".akc/policy/compile_tools.rego"
    assert cfg.opa_decision_path == "data.akc.allow"


def test_resolve_compile_profile_precedence_cli_over_governance_over_profile() -> None:
    resolved = resolve_compile_profile_defaults(
        profile="emerging",
        governance_profile=_Gov(rollout_stage="prod"),
        sandbox="dev",
        strong_lane_preference="docker",
        policy_mode="audit_only",
        replay_mode="live",
        promotion_mode=None,
        stored_assertion_index="off",
    )
    # policy_mode is explicit CLI override (non-legacy default)
    assert resolved["policy_mode"].value == "audit_only"
    assert resolved["policy_mode"].source == "cli"
    # sandbox inherits governance stage when CLI stayed at legacy default
    assert resolved["sandbox"].value == "strong"
    assert resolved["sandbox"].source == "governance"
    # stored assertion index falls to profile default under emerging
    assert resolved["stored_assertion_index"].value == "merge"
    assert resolved["stored_assertion_index"].source == "profile_default"


def test_resolve_compile_profile_governance_compile_defaults_overrides_rollout_stage() -> None:
    resolved = resolve_compile_profile_defaults(
        profile="emerging",
        governance_profile=_Gov(
            rollout_stage="prod",
            compile_defaults=(("sandbox", "dev"), ("replay_mode", "record")),
        ),
        sandbox="dev",
        strong_lane_preference="docker",
        policy_mode="enforce",
        replay_mode="live",
        promotion_mode=None,
        stored_assertion_index="off",
    )
    # prod would set sandbox strong; explicit compile_defaults wins
    assert resolved["sandbox"].value == "dev"
    assert resolved["sandbox"].source == "governance"
    assert resolved["replay_mode"].value == "record"
    assert resolved["replay_mode"].source == "governance"
    # promotion_mode still from prod stage (not overridden)
    assert resolved["promotion_mode"].value == "staged_apply"
    assert resolved["promotion_mode"].source == "governance"


def test_resolve_compile_profile_governance_compile_defaults_without_rollout_stage() -> None:
    resolved = resolve_compile_profile_defaults(
        profile="classic",
        governance_profile=_Gov(
            rollout_stage=None,
            compile_defaults=(
                ("promotion_mode", "artifact_only"),
                ("replay_mode", "vcr"),
            ),
        ),
        sandbox="dev",
        strong_lane_preference="docker",
        policy_mode="enforce",
        replay_mode="live",
        promotion_mode=None,
        stored_assertion_index="off",
    )
    assert resolved["replay_mode"].value == "vcr"
    assert resolved["replay_mode"].source == "governance"
    assert resolved["promotion_mode"].value == "artifact_only"
    assert resolved["promotion_mode"].source == "governance"


def test_resolve_compile_profile_quality_rollout_defaults_to_advisory() -> None:
    resolved = resolve_compile_profile_defaults(
        profile="classic",
        governance_profile=_Gov(rollout_stage=None, compile_defaults=()),
        sandbox="dev",
        strong_lane_preference="docker",
        policy_mode="enforce",
        replay_mode="live",
        promotion_mode=None,
        stored_assertion_index="off",
    )
    assert resolved["quality_contract_rollout_stage"].value == "advisory"
    assert resolved["quality_contract_rollout_stage"].source == "profile_default"


def test_resolve_compile_profile_governance_quality_rollout_stage_override() -> None:
    resolved = resolve_compile_profile_defaults(
        profile="emerging",
        governance_profile=_Gov(
            rollout_stage="prod",
            compile_defaults=(("quality_contract_rollout_stage", "phase_b"),),
        ),
        sandbox="dev",
        strong_lane_preference="docker",
        policy_mode="enforce",
        replay_mode="live",
        promotion_mode=None,
        stored_assertion_index="off",
    )
    assert resolved["quality_contract_rollout_stage"].value == "phase_b"
    assert resolved["quality_contract_rollout_stage"].source == "governance"


def test_resolve_compile_profile_governance_quality_evidence_expectations_mapping() -> None:
    expectations = {
        "engineering_discipline": ["tests_touched", "execution_passed"],
        "judgment": ["policy_decisions"],
    }
    resolved = resolve_compile_profile_defaults(
        profile="emerging",
        governance_profile=_Gov(
            rollout_stage="prod",
            compile_defaults=(("quality_evidence_expectations", expectations),),
        ),
        sandbox="dev",
        strong_lane_preference="docker",
        policy_mode="enforce",
        replay_mode="live",
        promotion_mode=None,
        stored_assertion_index="off",
    )
    assert resolved["quality_evidence_expectations"].source == "governance"
    assert resolved["quality_evidence_expectations"].value == expectations


def test_resolve_compile_profile_governance_quality_domain_fields() -> None:
    resolved = resolve_compile_profile_defaults(
        profile="classic",
        governance_profile=_Gov(
            rollout_stage=None,
            compile_defaults=(
                ("quality_domain_id", "security_network_secrets"),
                ("quality_domain_matrix_path", "tests/fixtures/knowledge_domains/domain_coverage_matrix.json"),
            ),
        ),
        sandbox="dev",
        strong_lane_preference="docker",
        policy_mode="enforce",
        replay_mode="live",
        promotion_mode=None,
        stored_assertion_index="off",
    )
    assert resolved["quality_domain_id"].value == "security_network_secrets"
    assert resolved["quality_domain_id"].source == "governance"
    assert (
        resolved["quality_domain_matrix_path"].value == "tests/fixtures/knowledge_domains/domain_coverage_matrix.json"
    )
    assert resolved["quality_domain_matrix_path"].source == "governance"
