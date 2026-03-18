"""Backward-compatible shim for `akc.cli` module-level entrypoint.

The real CLI implementation now lives under the `akc.cli` package
(`akc/cli/__init__.py`). This module simply re-exports `main` so that
existing entrypoints such as `akc.cli:main` continue to work.
"""

from __future__ import annotations
