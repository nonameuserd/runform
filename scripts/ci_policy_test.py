"""
CI policy tests for OSPS-aligned hygiene.

This is intentionally lightweight and dependency-free:
- It runs on untrusted `pull_request` events.
- It performs static checks over workflow YAML files.

What it checks (best-effort):
- No `pull_request_target` usage (common privileged workflow anti-pattern).
- No `secrets.*` usage in workflows that run on `pull_request`.
- No artifact uploads in PR workflows unless a provenance/attestation mechanism
  is present in the workflow file.
- Pull-request workflows remain low privilege (no write permissions, no OIDC).
"""

from __future__ import annotations

import pathlib
import re
import sys

WORKFLOWS_DIR = pathlib.Path(".github/workflows")


SECRETS_RE = re.compile(r"\bsecrets\.")
PULL_REQUEST_TARGET_RE = re.compile(r"\bpull_request_target\s*:")
PULL_REQUEST_RE = re.compile(r"^\s*pull_request\s*:\s*$", re.MULTILINE)

UPLOAD_ARTIFACT_RE = re.compile(
    r"actions/upload-artifact|upload-artifact|upload-release-assets\s*:\s*true|upload-assets\s*:\s*true",
    re.IGNORECASE,
)

PROVENANCE_RE = re.compile(
    r"slsa-framework/slsa-github-generator|attest-build-provenance|cosign|sigstore",
    re.IGNORECASE,
)


def read_text(path: pathlib.Path) -> str:
    return path.read_text(encoding="utf-8")


def workflow_triggers_pull_request(text: str) -> bool:
    # We only need a best-effort signal; this is a CI policy test, not a YAML parser.
    return bool(PULL_REQUEST_RE.search(text))


def pr_workflow_has_write_permissions(text: str) -> bool:
    # If any write-privilege appears in PR workflows, fail.
    return bool(
        re.search(
            r"\b(contents|id-token|actions|security-events|packages|pull-requests)\s*:\s*write\b",
            text,
        )
    )


def pr_workflow_has_prohibited_secrets(text: str) -> bool:
    # If secrets are referenced in PR workflows, fail. GitHub may block secrets on forks,
    # but we still want to prevent accidental privileged routing patterns.
    return bool(SECRETS_RE.search(text))


def pr_workflow_has_artifact_upload(text: str) -> bool:
    return bool(UPLOAD_ARTIFACT_RE.search(text))


def assert_policy() -> None:
    if not WORKFLOWS_DIR.exists():
        raise SystemExit(f"Missing directory: {WORKFLOWS_DIR}")

    workflow_files = sorted(WORKFLOWS_DIR.glob("*.yml")) + sorted(WORKFLOWS_DIR.glob("*.yaml"))
    if not workflow_files:
        raise SystemExit(f"No workflow YAML files found under {WORKFLOWS_DIR}")

    errors: list[str] = []

    for wf in workflow_files:
        text = read_text(wf)
        if PULL_REQUEST_TARGET_RE.search(text):
            errors.append(
                f"{wf}: contains `pull_request_target` (privileged untrusted input anti-pattern)"
            )

        if not workflow_triggers_pull_request(text):
            # The remaining checks are for untrusted PR contexts only.
            continue

        if pr_workflow_has_prohibited_secrets(text):
            errors.append(f"{wf}: references `secrets.*` in a `pull_request` workflow")

        if pr_workflow_has_write_permissions(text):
            errors.append(f"{wf}: grants write-privilege permissions in a `pull_request` workflow")

        if pr_workflow_has_artifact_upload(text) and not PROVENANCE_RE.search(text):
            errors.append(
                f"{wf}: uploads artifacts in PR context but lacks "
                f"provenance/attestation tooling signals "
                f"(expected `slsa-github-generator` / attest / cosign / sigstore)"
            )

    if errors:
        for e in errors:
            print(f"[FAIL] {e}")
        sys.exit(1)

    print("[PASS] CI policy checks succeeded for untrusted PR contexts.")


if __name__ == "__main__":
    assert_policy()
