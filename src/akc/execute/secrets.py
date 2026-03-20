from __future__ import annotations

import os
from dataclasses import dataclass

from akc.compile.interfaces import TenantRepoScope


@dataclass(frozen=True, slots=True)
class SecretsScopeConfig:
    """Tenant-scoped secrets injector.

    Host environment convention (configurable by prefix):
    - secrets are expected to exist in `os.environ` as
      `"{host_env_prefix}{tenant_id}_{secret_name}"`.
    - only keys that match the active `tenant_id` are injected.

    Injected sandbox env convention:
    - injected as `"{sandbox_env_prefix}{secret_name}"`.
    """

    host_env_prefix: str = "AKC_SECRET_"
    sandbox_env_prefix: str = "AKC_SECRET_"
    allowed_secret_names: tuple[str, ...] = ()

    def resolve_env_for_scope(self, *, scope: TenantRepoScope) -> dict[str, str]:
        """Resolve secrets to inject for the given tenant scope."""
        tenant_id = str(scope.tenant_id).strip()
        prefix = f"{self.host_env_prefix}{tenant_id}_"
        out: dict[str, str] = {}
        allowed = set(self.allowed_secret_names)
        for k, v in os.environ.items():
            k2 = str(k)
            if not k2.startswith(prefix):
                continue
            secret_name = k2[len(prefix) :]
            if not secret_name:
                continue
            if allowed and secret_name not in allowed:
                continue
            sandbox_key = f"{self.sandbox_env_prefix}{secret_name}"
            out[sandbox_key] = str(v)
        return out
