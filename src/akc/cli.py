"""Minimal CLI entry point; full compile command will be added in Phase 3."""

from akc import __version__


def main() -> None:
    """Entry point for the akc command."""
    print(f"akc {__version__} — Agentic Knowledge Compiler")
    print("Run 'akc compile --input ./docs' once the compile loop is implemented (Phase 3).")
