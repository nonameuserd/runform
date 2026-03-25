"""Local-only HTTP helper for static viewer bundles (developer convenience).

This is not a production server: bind address is fixed to loopback. See
``docs/viewer-trust-boundary.md``.
"""

from __future__ import annotations

import sys
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Final

LOCAL_VIEWER_HOST: Final[str] = "127.0.0.1"


class ViewerBundleHTTPRequestHandler(SimpleHTTPRequestHandler):
    """Serves one directory with ``nosniff`` and path confinement to the bundle root."""

    protocol_version = "HTTP/1.1"

    def translate_path(self, path: str) -> str:
        translated = super().translate_path(path)
        root = Path(self.directory).resolve()
        try:
            resolved = Path(translated).resolve()
            resolved.relative_to(root)
        except ValueError:
            return str(root / "__akc_viewer_path_outside_bundle__")
        return translated

    def end_headers(self) -> None:
        self.send_header("X-Content-Type-Options", "nosniff")
        super().end_headers()


def serve_viewer_bundle(root: Path, *, port: int | None = None) -> int:
    """Serve *root* on ``127.0.0.1`` until interrupted.

    :param root: Directory containing ``index.html`` and bundle assets (resolved, must exist).
    :param port: TCP port, or ``None`` for an ephemeral port chosen by the OS.
    :returns: ``0`` on clean shutdown, ``2`` if the directory is invalid or bind fails.
    """

    bundle_root = root.expanduser().resolve()
    if not bundle_root.is_dir():
        print(f"ERROR: viewer bundle directory does not exist: {bundle_root}", file=sys.stderr)
        return 2

    bind_port = 0 if port is None else int(port)
    handler_factory = partial(ViewerBundleHTTPRequestHandler, directory=str(bundle_root))

    try:
        httpd = ThreadingHTTPServer((LOCAL_VIEWER_HOST, bind_port), handler_factory)
    except OSError as exc:
        print(
            f"ERROR: could not bind HTTP server to {LOCAL_VIEWER_HOST}:{bind_port}: {exc}",
            file=sys.stderr,
        )
        return 2

    chosen_port = int(httpd.server_address[1])
    index_url = bundle_root / "index.html"
    file_url = index_url.as_uri()
    http_url = f"http://{LOCAL_VIEWER_HOST}:{chosen_port}/index.html"

    print(
        "Note: opening the bundle via file:// works for static HTML, but some browsers "
        "block fetch() and similar APIs on file:// pages; use the HTTP URL below when the "
        "viewer needs those APIs.",
        file=sys.stderr,
    )
    print(f"  file://  {file_url}", file=sys.stderr)
    print(f"  http://  {http_url}", file=sys.stderr)
    print(f"Serving {bundle_root} (Ctrl+C to stop)", file=sys.stderr)

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.", file=sys.stderr)
    finally:
        httpd.server_close()
    return 0
