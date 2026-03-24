from __future__ import annotations

from akc.living.automation_profile import (
    PROFILE_LIVING_LOOP_UNATTENDED_V1,
    PROFILE_LIVING_LOOP_V1,
    PROFILE_OFF,
    living_automation_includes_runtime_bridge,
    parse_living_automation_profile_token,
    resolve_living_automation_profile,
)


def test_resolve_living_automation_profile_precedence_cli_wins() -> None:
    p = resolve_living_automation_profile(
        cli_value="off",
        env={"AKC_LIVING_AUTOMATION_PROFILE": "living_loop_v1"},
        project_value="living_loop_v1",
    )
    assert p.id == "off"


def test_resolve_living_automation_profile_env_when_no_cli() -> None:
    p = resolve_living_automation_profile(
        cli_value=None,
        env={"AKC_LIVING_AUTOMATION_PROFILE": "living_loop_v1"},
        project_value="off",
    )
    assert p.id == "living_loop_v1"


def test_resolve_living_automation_profile_project_when_no_cli_env() -> None:
    p = resolve_living_automation_profile(
        cli_value=None,
        env={},
        project_value="living_loop_v1",
    )
    assert p.id == "living_loop_v1"


def test_resolve_living_automation_profile_defaults_off() -> None:
    p = resolve_living_automation_profile(cli_value=None, env={}, project_value=None)
    assert p == PROFILE_OFF


def test_parse_unknown_token_is_off() -> None:
    assert parse_living_automation_profile_token("nope") == "off"


def test_living_loop_v1_profile_fields() -> None:
    assert PROFILE_LIVING_LOOP_V1.granular_acceptance_default is True
    assert PROFILE_LIVING_LOOP_V1.baseline_duration_hours == 8.0
    assert living_automation_includes_runtime_bridge(profile=PROFILE_LIVING_LOOP_V1) is True
    assert living_automation_includes_runtime_bridge(profile=PROFILE_OFF) is False


def test_living_loop_unattended_v1_token_and_bridge() -> None:
    assert parse_living_automation_profile_token("living_loop_unattended_v1") == "living_loop_unattended_v1"
    assert living_automation_includes_runtime_bridge(profile=PROFILE_LIVING_LOOP_UNATTENDED_V1) is True


def test_resolve_living_loop_unattended_v1() -> None:
    p = resolve_living_automation_profile(
        cli_value=None,
        env={"AKC_LIVING_AUTOMATION_PROFILE": "living_loop_unattended_v1"},
        project_value=None,
    )
    assert p.id == "living_loop_unattended_v1"
