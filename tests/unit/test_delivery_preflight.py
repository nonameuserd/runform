from __future__ import annotations

import json
from pathlib import Path

import pytest

from akc.delivery import adapters as distribution_adapters
from akc.delivery import store as delivery_store


def test_iter_distribution_jobs_matches_matrix() -> None:
    jobs = list(
        distribution_adapters.iter_distribution_jobs(
            platforms=["web", "ios"],
            release_mode="beta",
        ),
    )
    kinds = {(j[0], j[1], j[2].kind) for j in jobs}
    assert ("web", "beta", "web_invite") in kinds
    assert ("ios", "beta", "testflight") in kinds
    assert len(jobs) == 2


def test_iter_distribution_jobs_both_orders_beta_before_store() -> None:
    jobs = list(
        distribution_adapters.iter_distribution_jobs(
            platforms=["web", "android", "ios"],
            release_mode="both",
        ),
    )
    lanes_order = [j[1] for j in jobs]
    first_store_idx = next((i for i, lane in enumerate(lanes_order) if lane == "store"), len(lanes_order))
    assert all(lanes_order[i] == "beta" for i in range(first_store_idx))
    assert lanes_order.count("beta") == 3 and lanes_order.count("store") == 3
    assert jobs[0][1] == "beta" and jobs[0][0] == "web"
    assert jobs[2][0] == "ios" and jobs[2][1] == "beta"
    assert jobs[3][1] == "store" and jobs[3][0] == "web"


def test_both_mode_phases_constant() -> None:
    assert distribution_adapters.BOTH_MODE_DISTRIBUTION_PHASES == (
        "beta_delivery",
        "human_readiness_gate",
        "store_promotion",
    )


def test_preflight_default_passes_with_stubs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", "1")
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
    )
    assert summary["preflight_issues"] == []
    sess = summary["session"]
    assert sess["session_phase"] == "accepted"
    ps = delivery_store.load_provider_state_sidecar(tmp_path, summary["delivery_id"])
    assert ps["platforms"]["web"]["details"]["preflight"]["issues"] == []
    assert ps["platforms"]["web"]["status"] == "not_started"


def test_preflight_strict_blocks_session(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web", "ios"],
        release_mode="both",
        tenant_id="t1",
        repo_id="r1",
    )
    assert len(summary["preflight_issues"]) >= 1
    sess = summary["session"]
    assert sess["session_phase"] == "blocked"
    assert sess["pipeline"]["distribution"]["status"] == "blocked"
    ev = delivery_store.load_events(tmp_path, summary["delivery_id"])
    types = [e.get("event_type") for e in ev]
    assert "delivery.preflight.completed" in types
    assert any(e.get("event_type") == "delivery.preflight.completed" and not e["payload"]["ok"] for e in ev)
    pre_ev = next(e for e in ev if e.get("event_type") == "delivery.preflight.completed")
    plat_pay = pre_ev["payload"].get("platforms") or {}
    assert "web" in plat_pay and "ios" in plat_pay
    assert plat_pay["web"]["lanes"]["beta"]["applicable"] is True
    assert plat_pay["ios"]["lanes"]["beta"]["applicable"] is True


def test_skip_distribution_preflight_skips_sidecar_details_enrichment(tmp_path: Path) -> None:
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web"],
        release_mode="beta",
        skip_distribution_preflight=True,
    )
    assert summary["preflight_issues"] == []
    ps = delivery_store.load_provider_state_sidecar(tmp_path, summary["delivery_id"])
    assert "preflight" not in (ps["platforms"]["web"].get("details") or {})


def test_legacy_enforce_env_zero_relaxes_preflight(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AKC_DELIVERY_ENFORCE_ADAPTER_PREFLIGHT", "0")
    monkeypatch.delenv("AKC_DELIVERY_RELAX_ADAPTER_PREFLIGHT", raising=False)
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web", "ios"],
        release_mode="both",
        tenant_id="t1",
        repo_id="r1",
    )
    assert summary["preflight_issues"] == []
    assert summary["session"]["session_phase"] == "accepted"


def test_preflight_per_lane_independent_visibility(tmp_path: Path) -> None:
    prereq = tmp_path / ".akc" / "delivery" / "operator_prereqs.json"
    prereq.parent.mkdir(parents=True, exist_ok=True)
    prereq.write_text(
        json.dumps(
            {"web": {"hosting_endpoint": True, "invite_email_configured": True}},
            indent=2,
        ),
        encoding="utf-8",
    )
    summary = delivery_store.create_delivery_session(
        project_dir=tmp_path,
        request_text="x",
        recipients=["a@example.com"],
        platforms=["web", "ios"],
        release_mode="beta",
        tenant_id="t1",
        repo_id="r1",
    )
    assert summary["session"]["session_phase"] == "blocked"
    web_beta = summary["session"]["per_platform"]["web"]["channels"]["beta"]
    assert web_beta["status"] == "not_started"
    assert web_beta["details"]["preflight"]["ok"] is True
    ios_beta = summary["session"]["per_platform"]["ios"]["channels"]["beta"]
    assert ios_beta["status"] == "blocked"
    assert ios_beta["details"]["preflight"]["ok"] is False
    assert ios_beta["details"]["preflight"]["issues"]
