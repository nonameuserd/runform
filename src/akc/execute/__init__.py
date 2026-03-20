"""Secure sandboxed execution (Phase C).

This package provides a stable abstraction for selecting and configuring
execution sandboxes (development vs strong boundaries), including:
- resource limits (memory, stdout/stderr caps)
- IO policy (network allowed/denied)
- tenant isolation (per-tenant run namespaces + per-tenant secrets scoping)

The compile loop still targets the existing `akc.compile.interfaces.Executor`
protocol; sandboxes here are implemented as `Executor` wrappers.
"""

from .factory import (
    SandboxDevConfig,
    SandboxFactoryConfig,
    SandboxStrongConfig,
    create_sandbox_executor,
)
from .secrets import SecretsScopeConfig

__all__ = [
    "SecretsScopeConfig",
    "SandboxDevConfig",
    "SandboxStrongConfig",
    "SandboxFactoryConfig",
    "create_sandbox_executor",
]
