"""MkDocs hooks for this repository.

MkDocs always prepends the builtin ``fenced_code`` extension before entries from
``markdown_extensions``. ``pymdownx.superfences`` must be registered *before*
``fenced_code``; the reverse order breaks triple-backtick fences (``#`` shell
comments become ATX headings and fence backticks show up as literal text).

We move ``fenced_code`` to immediately after ``pymdownx.superfences``. Both
extensions remain enabled so nested fences and highlighting keep working.

See: https://github.com/facelessuser/pymdown-extensions/issues/1056
"""

from __future__ import annotations

from typing import Any

_FENCED_CODE = frozenset(("fenced_code", "markdown.extensions.fenced_code"))


def on_config(config: Any) -> Any:
    exts = list(config.markdown_extensions)
    if "pymdownx.superfences" not in exts:
        return config
    if not any(e in _FENCED_CODE for e in exts):
        return config

    without_fc = [e for e in exts if e not in _FENCED_CODE]
    try:
        pos = without_fc.index("pymdownx.superfences") + 1
    except ValueError:
        return config
    reordered = without_fc[:pos] + ["fenced_code"] + without_fc[pos:]
    if reordered != exts:
        config.markdown_extensions = reordered
    return config
