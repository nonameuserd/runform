from __future__ import annotations

import subprocess
from pathlib import Path


def run_checked(
    argv: list[str],
    *,
    cwd: Path,
    timeout_sec: float,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        cwd=str(cwd),
        capture_output=True,
        text=True,
        timeout=timeout_sec,
        check=False,
    )
