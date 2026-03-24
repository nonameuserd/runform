from __future__ import annotations

from akc.living.unattended_defaults import unattended_autopilot_defaults_for_env


def test_unattended_matrix_prod_vs_staging_intervals() -> None:
    st = unattended_autopilot_defaults_for_env("staging")
    pr = unattended_autopilot_defaults_for_env("prod")
    assert st.living_check_interval_s < pr.living_check_interval_s
    assert st.budgets.max_mutations_per_day > pr.budgets.max_mutations_per_day


def test_unattended_matrix_dev_is_aggressive() -> None:
    d = unattended_autopilot_defaults_for_env("dev")
    assert d.living_check_interval_s <= 300.0
