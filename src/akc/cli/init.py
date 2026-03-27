from __future__ import annotations

import argparse
import json
import re
from importlib import resources
from pathlib import Path
from typing import Any, Literal

from .profile_defaults import normalize_developer_role_profile

_POLICY_STUB_RESOURCE = "compile_tools_policy_stub.rego"
_POLICY_REL_IN_PROJECT = ".akc/policy/compile_tools.rego"


def _read_policy_stub_bytes() -> bytes:
    pkg = "akc.cli"
    try:
        return resources.files(pkg).joinpath(_POLICY_STUB_RESOURCE).read_bytes()
    except (FileNotFoundError, ModuleNotFoundError, OSError):
        here = Path(__file__).resolve().parent / _POLICY_STUB_RESOURCE
        return here.read_bytes()


def _slug(s: str) -> str:
    raw = str(s).strip().lower().replace("_", "-")
    raw = re.sub(r"[^a-z0-9-]+", "-", raw)
    raw = re.sub(r"-+", "-", raw).strip("-")
    return raw or "repo"


def _default_repo_id_for_path(root: Path) -> str:
    try:
        resolved = root.resolve()
    except OSError:
        resolved = root
    name = resolved.name.strip()
    if not name or name == ".":
        return "repo"
    return _slug(name)


def _write_policy_stub(*, akc_dir: Path) -> Path:
    policy_dir = akc_dir / "policy"
    policy_dir.mkdir(parents=True, exist_ok=True)
    dest = policy_dir / "compile_tools.rego"
    dest.write_bytes(_read_policy_stub_bytes())
    return dest


def cmd_init(args: argparse.Namespace) -> int:
    """Create ``.akc/project.json`` (and optional local OPA policy stub).

    When `--detect` is set, also emit a lightweight `.akc/project_profile.json`.
    """

    root = Path(str(args.directory)).expanduser()
    try:
        root.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        print(f"akc init: cannot create directory {root}: {exc}")
        return 2

    akc_dir = root / ".akc"
    project_path = akc_dir / "project.json"
    if project_path.is_file() and not bool(getattr(args, "force", False)):
        print(
            f"akc init: {project_path} already exists (use --force to overwrite)",
        )
        return 2

    tenant_id = str(getattr(args, "tenant_id", "") or "").strip() or "local"
    repo_id = str(getattr(args, "repo_id", "") or "").strip() or _default_repo_id_for_path(root)
    outputs_root = str(getattr(args, "outputs_root", "") or "").strip() or "out"
    profile: Literal["classic", "emerging"] = normalize_developer_role_profile(
        getattr(args, "developer_role_profile", None),
    )
    policy_stub = bool(getattr(args, "policy_stub", True))

    if policy_stub:
        try:
            _write_policy_stub(akc_dir=akc_dir)
        except (OSError, FileNotFoundError) as exc:
            print(f"akc init: failed to write policy stub under {akc_dir}: {exc}")
            return 2

    payload: dict[str, Any] = {
        "developer_role_profile": profile,
        "tenant_id": tenant_id,
        "repo_id": repo_id,
        "outputs_root": outputs_root,
    }
    adoption_level = str(getattr(args, "adoption_level", "") or "").strip()
    if adoption_level:
        payload["adoption_level"] = adoption_level
    if policy_stub:
        payload["opa_policy_path"] = _POLICY_REL_IN_PROJECT
        payload["opa_decision_path"] = "data.akc.allow"

    akc_dir.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    try:
        project_path.write_text(text, encoding="utf-8")
    except OSError as exc:
        print(f"akc init: cannot write {project_path}: {exc}")
        return 2

    if bool(getattr(args, "detect", False)):
        try:
            from akc.adopt.detect import detect_project_profile

            detected_profile = detect_project_profile(root=root)
            profile_path = akc_dir / "project_profile.json"
            profile_text = detected_profile.to_json_str(indent=2)
            profile_path.write_text(profile_text, encoding="utf-8")
            print(f"  project_profile: {profile_path}")
        except Exception as exc:  # pragma: no cover (defensive; detection should be best-effort)
            print(f"akc init: project detection failed (continuing without profile): {exc}")

    print(f"Wrote {project_path}")
    if policy_stub:
        print(f"  policy stub: {root / _POLICY_REL_IN_PROJECT}")
    print("  tenant_id:", tenant_id)
    print("  repo_id:", repo_id)
    print("  outputs_root:", outputs_root)
    print("  developer_role_profile:", profile)
    return 0


def register_init_parser(sub: Any) -> None:
    init = sub.add_parser(
        "init",
        help="Create .akc/project.json with repo-scoped defaults (and optional local OPA policy stub)",
    )
    init.add_argument(
        "--directory",
        "-C",
        default=".",
        help="Directory to initialize (default: current directory)",
    )
    init.add_argument(
        "--force",
        action="store_true",
        help="Overwrite an existing .akc/project.json",
    )
    init.add_argument(
        "--tenant-id",
        default=None,
        help="Tenant identifier (default: local)",
    )
    init.add_argument(
        "--repo-id",
        default=None,
        help="Repo identifier (default: slug of the directory name)",
    )
    init.add_argument(
        "--outputs-root",
        default=None,
        help='Outputs root (default: "out", relative to --directory)',
    )
    init.add_argument(
        "--developer-role-profile",
        choices=["classic", "emerging"],
        default="emerging",
        help=(
            "Developer-role UX profile recorded in project.json (default: emerging). "
            "Does not change the global CLI default when this file is absent."
        ),
    )
    init.add_argument(
        "--adoption-level",
        default=None,
        help=(
            "Progressive takeover ladder level recorded in project.json (informational): "
            "observer|advisor|copilot|compiler|autonomy (or numeric 0..4)."
        ),
    )
    init.add_argument(
        "--no-policy-stub",
        dest="policy_stub",
        action="store_false",
        default=True,
        help="Do not copy the bundled OPA policy stub or set opa_* keys in project.json",
    )
    init.add_argument(
        "--detect",
        action="store_true",
        help="Analyze the repository and emit .akc/project_profile.json",
    )
    init.set_defaults(func=cmd_init)
