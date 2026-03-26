from __future__ import annotations

import argparse
import json
from pathlib import Path

import pytest

from akc.cli.control_bot import cmd_control_bot_validate_config


def _write(p: Path, obj: object) -> None:
    p.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def test_control_bot_validate_config_ok(tmp_path: Path) -> None:
    cfg = {
        "schema": "control_bot_config.v1",
        "server": {"bind": "127.0.0.1", "port": 9999},
        "channels": {
            "slack": {"enabled": False},
            "discord": {"enabled": False},
            "telegram": {"enabled": False},
            "whatsapp": {"enabled": False},
        },
        "routing": {"tenants": ["t1"], "workspaces": []},
        "identity": {"principal_roles": {"u1": {"tenant_id": "t1", "roles": ["operator"]}}},
        "policy": {
            "mode": "enforce",
            "opa": {"decision_path": "data.akc.allow"},
            "role_allowlist": {"operator": ["status.*", "approval.*", "incident.*", "mutate.*"]},
        },
        "approval": {"default_ttl_ms": 60_000},
        "storage": {
            "state_dir": str(tmp_path / "state"),
            "sqlite_path": str(tmp_path / "state" / "control_bot.sqlite"),
        },
    }
    p = tmp_path / "control-bot.json"
    _write(p, cfg)
    args = argparse.Namespace(config=str(p), verbose=False, print_json=False)
    assert cmd_control_bot_validate_config(args) == 0


def test_control_bot_validate_config_rejects_unknown_tenant(tmp_path: Path) -> None:
    cfg = {
        "schema": "control_bot_config.v1",
        "server": {"bind": "127.0.0.1", "port": 9999},
        "channels": {
            "slack": {"enabled": False},
            "discord": {"enabled": False},
            "telegram": {"enabled": False},
            "whatsapp": {"enabled": False},
        },
        "routing": {"tenants": ["t1"], "workspaces": []},
        "identity": {"principal_roles": {"u1": {"tenant_id": "t2", "roles": ["operator"]}}},
        "policy": {"mode": "enforce"},
        "approval": {"default_ttl_ms": 60_000},
        "storage": {
            "state_dir": str(tmp_path / "state"),
            "sqlite_path": str(tmp_path / "state" / "control_bot.sqlite"),
        },
    }
    p = tmp_path / "control-bot.json"
    _write(p, cfg)
    args = argparse.Namespace(config=str(p), verbose=False, print_json=False)
    with pytest.raises(SystemExit, match="routing\\.tenants"):
        cmd_control_bot_validate_config(args)


def test_control_bot_validate_config_rejects_workspace_route_to_unknown_tenant(tmp_path: Path) -> None:
    cfg = {
        "schema": "control_bot_config.v1",
        "server": {"bind": "127.0.0.1", "port": 9999},
        "channels": {
            "slack": {"enabled": False},
            "discord": {"enabled": False},
            "telegram": {"enabled": False},
            "whatsapp": {"enabled": False},
        },
        "routing": {
            "tenants": ["t1"],
            "workspaces": [{"channel": "slack", "workspace_id": "T123", "tenant_id": "t2"}],
        },
        "identity": {"principal_roles": {"u1": {"tenant_id": "t1", "roles": ["operator"]}}},
        "policy": {"mode": "enforce"},
        "approval": {"default_ttl_ms": 60_000},
        "storage": {
            "state_dir": str(tmp_path / "state"),
            "sqlite_path": str(tmp_path / "state" / "control_bot.sqlite"),
        },
    }
    p = tmp_path / "control-bot.json"
    _write(p, cfg)
    args = argparse.Namespace(config=str(p), verbose=False, print_json=False)
    with pytest.raises(SystemExit, match="routing\\.workspaces"):
        cmd_control_bot_validate_config(args)
