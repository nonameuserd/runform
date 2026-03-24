from __future__ import annotations

from pathlib import Path

import pytest

from akc.runtime.coordination.external_identity import (
    stub_external_identity_metadata,
    validate_external_identity_metadata_shape,
)
from akc.runtime.coordination.isolation import (
    effective_coordination_execution_allow_network,
    normalize_role_id_for_path,
    resolve_read_only_root_paths,
    subprocess_cwd_for_runtime_action,
    validate_role_profiles_network_vs_bundle,
)
from akc.runtime.coordination.models import (
    CoordinationGovernance,
    FilesystemScopeSpec,
    RoleIsolationProfile,
    parse_coordination_obj,
)
from akc.runtime.coordination.step_resolve import resolve_step_to_role_name
from akc.runtime.models import RuntimeContext


def test_stub_external_identity_metadata_shape() -> None:
    meta = stub_external_identity_metadata(
        spiffe_id="spiffe://example.org/ns/default/sa/writer",
        opa_policy_bundle_version="1.2.3",
    )
    assert meta.get("integration") == "stub"
    assert meta.get("spiffe_id") == "spiffe://example.org/ns/default/sa/writer"
    assert meta.get("opa_policy_bundle_version") == "1.2.3"
    assert validate_external_identity_metadata_shape(meta) == ()


def test_validate_external_identity_metadata_rejects_bad_types() -> None:
    issues = validate_external_identity_metadata_shape({"spiffe_id": 123})  # type: ignore[arg-type]
    assert issues


def test_effective_network_and_tightening() -> None:
    gov = CoordinationGovernance(
        max_steps=1,
        allowed_capabilities=(),
        execution_allow_network=True,
        role_profiles={
            "a": RoleIsolationProfile(
                filesystem_scope=FilesystemScopeSpec(read_only_roots=(), scratch_subdir="s"),
                allowed_tools=(),
                execution_allow_network=False,
            )
        },
    )
    prof = gov.role_profiles["a"]
    assert not effective_coordination_execution_allow_network(
        bundle_allow_network=True,
        governance=gov,
        role_profile=prof,
    )
    assert effective_coordination_execution_allow_network(
        bundle_allow_network=True,
        governance=gov,
        role_profile=None,
    )


def test_validate_role_profiles_network_vs_bundle() -> None:
    issues = validate_role_profiles_network_vs_bundle(
        bundle_allow_network=False,
        governance={
            "role_profiles": {"x": {"execution_allow_network": True}},
        },
    )
    assert issues


def test_resolve_step_to_role_name() -> None:
    orch = {
        "steps": [
            {"step_id": "workflow_000", "role": "writer"},
        ]
    }
    assert resolve_step_to_role_name(step_id="workflow_000", orchestration_obj=orch) == "writer"


def test_subprocess_cwd_uses_role_path(tmp_path: Path) -> None:
    ctx = RuntimeContext(
        tenant_id="t1",
        repo_id="r1",
        run_id="run1",
        runtime_run_id="rt1",
        policy_mode="enforce",
        adapter_id="local_depth",
    )
    pc = {
        "coordination_role_id": "writer",
        "coordination_filesystem_scope": {"read_only_roots": [], "scratch_subdir": "scratch"},
    }
    cwd = subprocess_cwd_for_runtime_action(
        context=ctx,
        outputs_root=tmp_path,
        action_policy_context=pc,
    )
    assert cwd.name == "scratch"
    assert "roles" in cwd.parts
    assert "writer" in cwd.parts


def test_normalize_role_id_rejects_path_injection() -> None:
    with pytest.raises(ValueError):
        normalize_role_id_for_path("../x")


def test_parse_coordination_role_profiles() -> None:
    spec = parse_coordination_obj(
        {
            "spec_version": 1,
            "run_id": "r1",
            "tenant_id": "t1",
            "repo_id": "p1",
            "coordination_graph": {"nodes": [], "edges": []},
            "governance": {
                "role_profiles": {
                    "planner": {
                        "filesystem_scope": {"read_only_roots": ["."], "scratch_subdir": "scratch"},
                        "allowed_tools": ["llm.complete"],
                        "execution_allow_network": False,
                    }
                },
            },
        }
    )
    assert spec.governance is not None
    assert "planner" in spec.governance.role_profiles
    assert spec.governance.role_profiles["planner"].allowed_tools == ("llm.complete",)


def test_resolve_read_only_root_paths_contained(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    p = resolve_read_only_root_paths(repo_root=repo, read_only_roots=("src",))
    assert p[0] == (repo / "src").resolve()
