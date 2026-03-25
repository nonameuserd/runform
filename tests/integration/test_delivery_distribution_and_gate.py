"""Integration-style tests: distribution dispatch + release_mode=both human gate (no real provider HTTP)."""

from __future__ import annotations

from pathlib import Path

import pytest

from akc.delivery import store as delivery_store
from akc.delivery.distribution_dispatch import run_delivery_distribution


def test_both_mode_store_promotion_blocked_until_gate_passed(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="ship",
        recipients=["x@example.com"],
        platforms=["ios"],
        release_mode="both",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    with pytest.raises(ValueError, match="human readiness gate"):
        delivery_store.record_promote(project_dir=tmp_path, delivery_id=did, lane="store")

    delivery_store.record_human_readiness_gate_pass(project_dir=tmp_path, delivery_id=did)
    row = delivery_store.record_promote(project_dir=tmp_path, delivery_id=did, lane="store")
    assert row.get("event_type") == "delivery.store.promotion_requested"


def test_distribution_beta_wave_sets_human_gate_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    monkeypatch.setenv("AKC_DELIVERY_EXECUTE_PROVIDERS", "false")

    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="beta",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="both",
        delivery_version="1.0.0",
        tenant_id="t1",
        repo_id="r1",
        skip_distribution_preflight=True,
    )
    did = str(summary["delivery_id"])
    delivery_store.update_session_compile_stage(
        project_dir=tmp_path,
        delivery_id=did,
        run_id="r1",
        succeeded=True,
    )
    delivery_store.update_session_pipeline_stage(
        project_dir=tmp_path,
        delivery_id=did,
        stage_name="package",
        status="completed",
        outputs={
            "per_platform": {
                "web": {
                    "ok": True,
                    "outputs": {"deployed_url": "https://example.org"},
                },
            },
        },
    )

    dist_result = run_delivery_distribution(
        project_dir=tmp_path,
        delivery_id=did,
        tenant_id="t1",
        repo_id="r1",
        platforms=["web"],
        release_mode="both",
        delivery_version="1.0.0",
        compile_run_id="r1",
        lanes=("beta",),
    )
    assert dist_result.get("ok") is True
    assert "web:beta" in (dist_result.get("jobs") or {}), (
        "beta wave must record at least one job; empty jobs means the dispatch loop did no work"
    )

    sess = delivery_store.load_session(tmp_path, did)
    dp = sess.get("distribution_plan")
    assert isinstance(dp, dict)
    assert dp.get("current_phase") == "human_readiness_gate"
    assert str(sess.get("human_readiness_gate", {}).get("status")) == "pending"
