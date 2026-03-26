from __future__ import annotations

import argparse
import json
from pathlib import Path

from akc.cli.common import configure_logging
from akc.control_bot.config import (
    ControlBotConfigError,
    LoadedControlBotConfig,
    load_control_bot_config,
    load_control_bot_config_file,
)
from akc.control_bot.server import ControlBotServerConfig, run_control_bot_server


def cmd_control_bot_validate_config(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    cfg_path = Path(str(getattr(args, "config", "") or "")).expanduser().resolve()
    if not str(cfg_path).strip():
        raise SystemExit("--config is required")
    try:
        data = load_control_bot_config_file(cfg_path)
        loaded = load_control_bot_config(cfg_path)
    except ControlBotConfigError as e:
        raise SystemExit(str(e)) from e
    if bool(getattr(args, "print_json", False)):
        print(json.dumps(data, indent=2, sort_keys=True))
    # typed cross-field checks (tenant routing, principal map)
    _ = loaded.model.principal_identities()
    return 0


def cmd_control_bot_serve(args: argparse.Namespace) -> int:
    configure_logging(verbose=bool(getattr(args, "verbose", False)))
    cfg_path = Path(str(getattr(args, "config", "") or "")).expanduser().resolve()
    if not str(cfg_path).strip():
        raise SystemExit("--config is required")
    try:
        loaded = load_control_bot_config(cfg_path)
    except ControlBotConfigError as e:
        raise SystemExit(str(e)) from e

    # CLI overrides (useful for local dev without editing config).
    bind = str(getattr(args, "bind", "") or "").strip()
    port = getattr(args, "port", None)
    if bind:
        loaded = LoadedControlBotConfig(
            raw=loaded.raw,
            model=loaded.model.model_copy(update={"server": {**loaded.model.server.model_dump(), "bind": bind}}),
            path=loaded.path,
        )
    if port is not None:
        loaded = LoadedControlBotConfig(
            raw=loaded.raw,
            model=loaded.model.model_copy(update={"server": {**loaded.model.server.model_dump(), "port": int(port)}}),
            path=loaded.path,
        )

    cfg = ControlBotServerConfig(loaded=loaded)
    try:
        run_control_bot_server(cfg)
    except KeyboardInterrupt:
        return 0
    return 0
