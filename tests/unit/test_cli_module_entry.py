from __future__ import annotations

import subprocess
import sys


def test_python_m_akc_cli_invokes_cli() -> None:
    """``python -m akc.cli`` must work (wheel/console_scripts use :func:`akc.cli.main`; Nuitka uses the same entry)."""

    proc = subprocess.run(
        [sys.executable, "-m", "akc.cli", "--help"],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "Agentic Knowledge Compiler" in proc.stdout
