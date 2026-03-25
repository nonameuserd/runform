"""Allow ``python -m akc.cli`` (and Nuitka ``--main`` on this file).

Mirrors the setuptools console entry ``akc = akc.cli:main``.
"""

from __future__ import annotations

from akc.cli import main

if __name__ == "__main__":
    main()
