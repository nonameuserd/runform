from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from akc.cli.compile import _merge_compile_skills_from_sources
from akc.cli.project_config import AkcProjectConfig
from akc.compile.artifact_passes import build_patch_artifact_prompt_envelope
from akc.compile.controller_config import Budget, ControllerConfig, TierConfig
from akc.compile.controller_types import Candidate, ControllerResult
from akc.compile.session import CompileSession
from akc.compile.skills.discovery import build_skill_catalog, split_yaml_frontmatter
from akc.compile.skills.models import SkillManifest
from akc.compile.skills.pipeline import build_compile_skill_system_append
from akc.compile.skills.prompt import format_skill_system_preamble
from akc.compile.skills.selection import select_activated_skills
from akc.intent import compile_intent_spec
from akc.memory.models import PlanState, PlanStep


def test_builtin_default_skill_packaged_and_loadable() -> None:
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="default_only",
    )
    cat = build_skill_catalog(config=cfg, project_root=None)
    assert "akc-default" in cat.by_name
    m = cat.by_name["akc-default"]
    assert m.path_kind == "builtin"
    assert "unified diff" in m.body_text.lower()
    assert "tests/unit" in m.body_text or "tests/integration" in m.body_text
    assert "tenant" in m.body_text.lower() and "repository" in m.body_text.lower()
    assert len(m.content_sha256) == 64


def test_split_yaml_frontmatter_basic() -> None:
    raw = "---\nname: foo\ndescription: Bar skill\n---\n\nHello **body**.\n"
    meta, body = split_yaml_frontmatter(raw)
    assert meta["name"] == "foo"
    assert meta["description"] == "Bar skill"
    assert "Hello **body**." in body


def test_split_yaml_frontmatter_no_opening_fence_is_plain_document() -> None:
    raw = "name: looks-like-yaml\n---\nNot frontmatter.\n"
    meta, body = split_yaml_frontmatter(raw)
    assert meta == {}
    assert body == raw


def test_split_yaml_frontmatter_unclosed_fence_yields_empty_meta() -> None:
    raw = "---\nname: orphan\n"
    meta, body = split_yaml_frontmatter(raw)
    assert meta == {}
    assert body == raw


def test_split_yaml_frontmatter_strips_quotes_and_skips_comments() -> None:
    raw = (
        "---\n"
        'name: "quoted-name"\n'
        "description: 'single-quoted'\n"
        "# ignored: true\n"
        "disable-model-invocation: 'on'\n"
        "---\n\nBody.\n"
    )
    meta, body = split_yaml_frontmatter(raw)
    assert meta["name"] == "quoted-name"
    assert meta["description"] == "single-quoted"
    assert meta["disable-model-invocation"] == "on"
    assert body.strip() == "Body."


def test_split_yaml_frontmatter_duplicate_keys_last_wins() -> None:
    raw = "---\nname: first\nname: second\n---\n\n"
    meta, body = split_yaml_frontmatter(raw)
    assert meta["name"] == "second"
    assert not body.strip()


def test_split_yaml_frontmatter_ignores_lines_without_colon() -> None:
    raw = "---\nnot_a_key_value_line\nname: ok\n---\n\n"
    meta, body = split_yaml_frontmatter(raw)
    assert meta == {"name": "ok"}


def test_fingerprint_stable_for_identical_bytes(tmp_path: Path) -> None:
    text = "---\nname: fp-skill\ndescription: x\n---\n\nSame bytes.\n"
    p = tmp_path / ".cursor" / "skills" / "fp" / "SKILL.md"
    p.parent.mkdir(parents=True)
    p.write_text(text, encoding="utf-8")
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    cat1 = build_skill_catalog(config=cfg, project_root=tmp_path)
    cat2 = build_skill_catalog(config=cfg, project_root=tmp_path)
    m1 = cat1.by_name["fp-skill"]
    m2 = cat2.by_name["fp-skill"]
    assert m1.content_sha256 == m2.content_sha256
    assert m1.content_sha256 == hashlib.sha256(text.encode("utf-8")).hexdigest()


def test_fingerprint_changes_when_file_body_changes(tmp_path: Path) -> None:
    def write(body: str) -> None:
        p = tmp_path / ".cursor" / "skills" / "fp2" / "SKILL.md"
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            f"---\nname: fp2\ndescription: x\n---\n\n{body}\n",
            encoding="utf-8",
        )

    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    write("v1")
    h1 = build_skill_catalog(config=cfg, project_root=tmp_path).by_name["fp2"].content_sha256
    write("v2")
    h2 = build_skill_catalog(config=cfg, project_root=tmp_path).by_name["fp2"].content_sha256
    assert h1 != h2


def test_discovery_order_builtin_then_akc_cursor_sorted_dirs(tmp_path: Path) -> None:
    """Builtin first; each root scans immediate children sorted by directory name."""

    def write_skill(rel: Path, skill_name: str) -> None:
        d = tmp_path / rel
        d.mkdir(parents=True)
        (d / "SKILL.md").write_text(
            f"---\nname: {skill_name}\ndescription: x\n---\n\n",
            encoding="utf-8",
        )

    write_skill(Path(".akc") / "skills" / "z_dir", "z-from-akc")
    write_skill(Path(".akc") / "skills" / "a_dir", "a-from-akc")
    write_skill(Path(".cursor") / "skills" / "m_dir", "m-from-cursor")
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert cat.discovery_order[0] == "akc-default"
    assert list(cat.discovery_order[1:]) == ["a-from-akc", "z-from-akc", "m-from-cursor"]


def test_discovery_skips_unsafe_skill_directory_names(tmp_path: Path) -> None:
    skills = tmp_path / ".cursor" / "skills"
    skills.mkdir(parents=True)
    bad = skills / "bad!name"
    bad.mkdir(parents=True)
    (bad / "SKILL.md").write_text("---\nname: bad\n---\n\n", encoding="utf-8")
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert "bad" not in cat.by_name


def test_compile_skill_relative_roots_dotdot_prefix_skipped(tmp_path: Path) -> None:
    """``compile_skill_relative_roots`` entries starting with ``..`` are ignored (traversal guard)."""
    neighbor = tmp_path.parent / f"_akc_neighbor_skills_{tmp_path.name}"
    root = neighbor / "skills"
    pkg = root / "evil"
    pkg.mkdir(parents=True)
    (pkg / "SKILL.md").write_text(
        "---\nname: neighbor-skill\ndescription: x\n---\n\n",
        encoding="utf-8",
    )
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
        compile_skill_relative_roots=(f"../{neighbor.name}/skills",),
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert "neighbor-skill" not in cat.by_name


def test_selection_explicit_does_not_include_disable_model_invocation_without_allowlist(
    tmp_path: Path,
) -> None:
    skill_dir = tmp_path / ".cursor" / "skills" / "manual_skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\n"
        "name: manual-x\n"
        "description: overlap token pytesthere\n"
        "disable-model-invocation: true\n"
        "---\n\nOnly manual.\n",
        encoding="utf-8",
    )
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
        compile_skill_allowlist=(),
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t",
        repo_id="r",
        goal_statement="pytesthere automation",
        controller_budget=Budget(),
    )
    sel, _exp = select_activated_skills(catalog=cat, config=cfg, goal="pytesthere", intent_spec=intent)
    assert [m.name for m in sel] == ["akc-default"]


def test_relative_skill_root_symlink_outside_project_is_ignored(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"_akc_skill_rel_{tmp_path.name}"
    ext_skills = outside / "skills_root"
    pkg = ext_skills / "evil_pkg"
    pkg.mkdir(parents=True)
    (pkg / "SKILL.md").write_text(
        "---\nname: escaped-rel\ndescription: x\n---\n\nNope.\n",
        encoding="utf-8",
    )
    link_name = tmp_path / "skill_link_out"
    try:
        link_name.symlink_to(ext_skills, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation not supported in this environment")
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
        compile_skill_relative_roots=("skill_link_out",),
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert "escaped-rel" not in cat.by_name


def test_discovery_finds_skill_under_compile_skill_extra_roots(tmp_path: Path) -> None:
    extra = tmp_path / "global_skills"
    skill_dir = extra / "from_extra"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: extra-root-skill\ndescription: from extra root\n---\n\nExtra body.\n",
        encoding="utf-8",
    )
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
        compile_skill_extra_roots=(extra.resolve(),),
    )
    cat = build_skill_catalog(config=cfg, project_root=None)
    assert "extra-root-skill" in cat.by_name
    assert cat.by_name["extra-root-skill"].path_kind == "extra"
    assert "Extra body." in cat.by_name["extra-root-skill"].body_text


def test_discovery_finds_immediate_child_skill(tmp_path: Path) -> None:
    skill_dir = tmp_path / ".cursor" / "skills" / "demo_skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: demo-skill\ndescription: unit test skill\n---\n\nBody line.\n",
        encoding="utf-8",
    )
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert "demo-skill" in cat.by_name
    m = cat.by_name["demo-skill"]
    assert m.path_kind == "project"
    assert "Body line." in m.body_text


def test_selection_default_only_builtin_only() -> None:
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="default_only",
    )
    cat = build_skill_catalog(config=cfg, project_root=None)
    intent = compile_intent_spec(
        tenant_id="t",
        repo_id="r",
        goal_statement="Ship a feature",
        controller_budget=Budget(),
    )
    sel, _exp = select_activated_skills(catalog=cat, config=cfg, goal="g", intent_spec=intent)
    assert len(sel) >= 1
    assert sel[0].path_kind == "builtin"


def test_selection_auto_respects_disable_model_invocation(tmp_path: Path) -> None:
    skill_dir = tmp_path / ".akc" / "skills" / "manual_only"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\n"
        "name: manual-only\n"
        "description: testing compile loop pytest automation\n"
        "disable-model-invocation: true\n"
        "---\n\nBody.\n",
        encoding="utf-8",
    )
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="auto",
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    intent = compile_intent_spec(
        tenant_id="t",
        repo_id="r",
        goal_statement="pytest automation for the compile loop",
        controller_budget=Budget(),
    )
    sel, _exp = select_activated_skills(catalog=cat, config=cfg, goal=intent.goal_statement or "", intent_spec=intent)
    names = [m.name for m in sel]
    assert "manual-only" not in names


def test_selection_auto_includes_when_explicitly_allowlisted(tmp_path: Path) -> None:
    skill_dir = tmp_path / ".akc" / "skills" / "manual_only"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: manual-only\ndescription: x\ndisable-model-invocation: true\n---\n\nBody.\n",
        encoding="utf-8",
    )
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="auto",
        compile_skill_allowlist=("manual-only",),
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    intent = compile_intent_spec(tenant_id="t", repo_id="r", goal_statement="unrelated", controller_budget=Budget())
    sel, _exp = select_activated_skills(catalog=cat, config=cfg, goal="g", intent_spec=intent)
    names = [m.name for m in sel]
    assert "manual-only" in names


def test_prompt_envelope_prompt_key_changes_with_skills() -> None:
    base = build_patch_artifact_prompt_envelope(
        user_prompt="u",
        tier_name="small",
        tier_model="m",
        plan_id="p",
        step_id="s",
        replay_mode="live",
        temperature=0.0,
        max_output_tokens=None,
    )
    with_skill_a = build_patch_artifact_prompt_envelope(
        user_prompt="u",
        tier_name="small",
        tier_model="m",
        plan_id="p",
        step_id="s",
        replay_mode="live",
        temperature=0.0,
        max_output_tokens=None,
        skill_system_append="### SKILL: x\nextra-alpha",
    )
    with_skill_b = build_patch_artifact_prompt_envelope(
        user_prompt="u",
        tier_name="small",
        tier_model="m",
        plan_id="p",
        step_id="s",
        replay_mode="live",
        temperature=0.0,
        max_output_tokens=None,
        skill_system_append="### SKILL: x\nextra-beta",
    )
    assert base.prompt_key != with_skill_a.prompt_key
    assert base.prompt_key != with_skill_b.prompt_key
    assert with_skill_a.prompt_key != with_skill_b.prompt_key
    assert "extra-alpha" in with_skill_a.llm_request.messages[0].content
    assert "extra-beta" in with_skill_b.llm_request.messages[0].content


def test_prompt_envelope_skill_blocks_join_system() -> None:
    env = build_patch_artifact_prompt_envelope(
        user_prompt="u",
        tier_name="small",
        tier_model="m",
        plan_id="p",
        step_id="s",
        replay_mode="live",
        temperature=0.0,
        max_output_tokens=None,
        skill_blocks=("### A", "", "  ### B  "),
    )
    system = env.llm_request.messages[0].content
    assert system.startswith("You are an AKC compile loop assistant.")
    assert "### A" in system and "### B" in system


def test_prompt_envelope_metadata_includes_skill_audit() -> None:
    env = build_patch_artifact_prompt_envelope(
        user_prompt="u",
        tier_name="small",
        tier_model="m",
        plan_id="p",
        step_id="s",
        replay_mode="live",
        temperature=0.0,
        max_output_tokens=None,
        compile_skills_active=({"name": "n", "sha256": "0" * 64, "path_kind": "builtin"},),
        compile_skills_mode="default_only",
    )
    md = env.llm_request.metadata or {}
    assert md.get("compile_skills_mode") == "default_only"
    assert isinstance(md.get("compile_skills_active"), list)


def test_format_skill_system_preamble_token_cap() -> None:
    m = SkillManifest(
        name="big",
        description="d",
        disable_model_invocation=False,
        body_text="x" * 5000,
        skill_root="/r",
        skill_md_path="/r/SKILL.md",
        content_sha256="0" * 64,
        path_kind="project",
    )
    out = format_skill_system_preamble(manifests=(m,), max_total_bytes=1_000_000, max_input_tokens=100)
    assert len(out.encode("utf-8")) <= 100 * 16


def test_discovery_ignores_symlinked_skill_dir_outside_project(tmp_path: Path) -> None:
    outside = tmp_path.parent / f"_akc_evil_outside_{tmp_path.name}"
    real_skill = outside / "evil"
    real_skill.mkdir(parents=True)
    (real_skill / "SKILL.md").write_text(
        "---\nname: evil-skill\ndescription: x\n---\n\nNope.\n",
        encoding="utf-8",
    )
    skills_root = tmp_path / ".cursor" / "skills"
    skills_root.mkdir(parents=True)
    try:
        (skills_root / "link_skill").symlink_to(real_skill, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation not supported in this environment")

    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert "evil-skill" not in cat.by_name


def test_discovery_follows_symlink_while_staying_inside_root(tmp_path: Path) -> None:
    skills_root = tmp_path / ".akc" / "skills"
    skills_root.mkdir(parents=True)
    real = tmp_path / "real_skill_home" / "linked_skill"
    real.mkdir(parents=True)
    (real / "SKILL.md").write_text(
        "---\nname: linked-ok\ndescription: in tree\n---\n\nOK.\n",
        encoding="utf-8",
    )
    try:
        (skills_root / "linked_skill").symlink_to(real, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation not supported in this environment")

    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert "linked-ok" in cat.by_name
    assert "OK." in cat.by_name["linked-ok"].body_text


def test_discovery_rejects_cursor_skills_anchor_that_escapes_project(tmp_path: Path) -> None:
    """If ``.cursor`` is a symlink outside the project, do not scan that external tree."""
    cursor_link = tmp_path / ".cursor"
    external = tmp_path.parent / f"_akc_ext_cursor_{tmp_path.name}"
    external.mkdir(parents=True)
    skills = external / "skills" / "bad"
    skills.mkdir(parents=True)
    (skills / "SKILL.md").write_text(
        "---\nname: escaped\ndescription: x\n---\n\nBad.\n",
        encoding="utf-8",
    )
    try:
        cursor_link.symlink_to(external, target_is_directory=True)
    except OSError:
        pytest.skip("symlink creation not supported in this environment")

    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="explicit",
    )
    cat = build_skill_catalog(config=cfg, project_root=tmp_path)
    assert "escaped" not in cat.by_name


def test_merge_compile_skills_applies_project_byte_caps() -> None:
    base = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="default_only",
        compile_skill_max_file_bytes=393_216,
        compile_skill_max_total_bytes=98_304,
    )
    proj = AkcProjectConfig(
        compile_skill_max_file_bytes=50_000,
        compile_skill_max_total_bytes=10_000,
    )
    out = _merge_compile_skills_from_sources(
        config=base,
        proj=proj,
        cli_skill_names=[],
        cli_mode=None,
        cli_extra_skill_roots=[],
        cli_max_file_bytes=None,
        cli_max_total_bytes=None,
        project_dir=Path("/tmp"),
    )
    assert out.compile_skill_max_file_bytes == 50_000
    assert out.compile_skill_max_total_bytes == 10_000


def test_merge_compile_skills_cli_byte_caps_override_project() -> None:
    base = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="default_only",
    )
    proj = AkcProjectConfig(
        compile_skill_max_file_bytes=50_000,
        compile_skill_max_total_bytes=10_000,
    )
    out = _merge_compile_skills_from_sources(
        config=base,
        proj=proj,
        cli_skill_names=[],
        cli_mode=None,
        cli_extra_skill_roots=[],
        cli_max_file_bytes=80_000,
        cli_max_total_bytes=20_000,
        project_dir=Path("/tmp"),
    )
    assert out.compile_skill_max_file_bytes == 80_000
    assert out.compile_skill_max_total_bytes == 20_000


def test_pipeline_off_mode() -> None:
    cfg = ControllerConfig(
        tiers={"small": TierConfig(name="small", llm_model="m")},
        compile_skills_mode="off",
    )
    intent = compile_intent_spec(tenant_id="t", repo_id="r", goal_statement="g", controller_budget=Budget())
    text, meta = build_compile_skill_system_append(
        config=cfg,
        project_root=None,
        intent_spec=intent,
        goal="g",
        effective_max_input_tokens=None,
    )
    assert text is None
    assert meta["compile_skills_mode"] == "off"
    assert meta["compile_skills_active"] == []


def test_compile_session_generate_pass_merges_compile_skills_into_metadata() -> None:
    ts = 1
    plan = PlanState(
        id="plan1",
        tenant_id="t",
        repo_id="r",
        goal="g",
        status="active",
        created_at_ms=ts,
        updated_at_ms=ts,
        steps=(PlanStep(id="s1", title="step", status="pending", order_idx=0),),
        next_step_id="s1",
    )
    patch = "\n".join(
        [
            "--- a/src/x.py",
            "+++ b/src/x.py",
            "@@",
            "+1",
            "",
        ]
    )
    cand = Candidate(
        tier="small",
        stage="generate",
        llm_text=patch,
        touched_paths=("src/x.py",),
        test_paths=(),
        execution=None,
        execution_stage=None,
        execution_command=None,
        score=1,
        attempt_idx=0,
        created_at_ms=ts,
    )
    result = ControllerResult(
        status="succeeded",
        plan=plan,
        best_candidate=cand,
        accounting={
            "llm_calls": 1,
            "compile_skills_active": [{"name": "akc-default", "sha256": "a" * 64, "path_kind": "builtin"}],
            "compile_skills_mode": "default_only",
        },
        compile_succeeded=True,
        intent_satisfied=True,
    )
    session = CompileSession.from_memory(tenant_id="t", repo_id="r")
    recs = session._build_pass_records(result=result, step_outputs={"last_prompt_key": "pk1"})
    gen = next(p for p in recs if p.name == "generate")
    assert gen.metadata is not None
    assert gen.metadata.get("prompt_key") == "pk1"
    assert gen.metadata.get("compile_skills_mode") == "default_only"
    assert gen.metadata.get("compile_skills_active") == [
        {"name": "akc-default", "sha256": "a" * 64, "path_kind": "builtin"}
    ]
