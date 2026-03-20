from __future__ import annotations


def extract_touched_paths(patch_text: str) -> list[str]:
    """Extract touched file paths from a unified diff.

    Best-effort and deterministic: returns stable sorted unique paths.
    """

    paths: set[str] = set()
    for raw in (patch_text or "").splitlines():
        line = raw.strip()
        # Common forms:
        # --- a/foo.py
        # +++ b/foo.py
        if line.startswith("+++ "):
            p = line[4:].strip()
            if p.startswith("b/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
        elif line.startswith("--- "):
            p = line[4:].strip()
            if p.startswith("a/"):
                p = p[2:]
            if p and p != "/dev/null":
                paths.add(p)
    return sorted(paths)
