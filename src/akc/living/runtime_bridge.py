"""Living runtime bridge factory (Phase E).

The default implementation lives in :mod:`akc.runtime.living_bridge`. This module
provides a single import path for automation that wires the bridge together
with :mod:`akc.living.automation_profile` documentation.
"""

from __future__ import annotations

from akc.runtime.living_bridge import DefaultLivingRuntimeBridge, LivingRuntimeBridge


def default_living_runtime_bridge() -> LivingRuntimeBridge:
    """Return the OSS default bridge used when ``living_loop_v1`` is enabled."""

    return DefaultLivingRuntimeBridge()
