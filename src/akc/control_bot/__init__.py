"""Control-bot control plane (multi-channel operator gateway).

This subsystem keeps channel-specific logic at the edges (ingress/outbound adapters)
and routes all actions through a deterministic command engine with policy/approval
gates. Tenant isolation is enforced at module boundaries by requiring explicit IDs.
"""

from __future__ import annotations

__all__ = [
    "audit",
    "approval_workflow",
    "command_engine",
    "event_store",
    "ingress_adapters",
    "outbound_response_adapters",
]
