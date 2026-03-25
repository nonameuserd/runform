from __future__ import annotations

import threading
from functools import partial
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from akc.viewer.serve import LOCAL_VIEWER_HOST, ViewerBundleHTTPRequestHandler, serve_viewer_bundle


def _start_server(directory: Path) -> ThreadingHTTPServer:
    handler = partial(ViewerBundleHTTPRequestHandler, directory=str(directory))
    httpd = ThreadingHTTPServer((LOCAL_VIEWER_HOST, 0), handler)
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd


def test_viewer_bundle_handler_sends_nosniff(tmp_path: Path) -> None:
    (tmp_path / "index.html").write_text("<!doctype html><title>x</title>", encoding="utf-8")
    httpd = _start_server(tmp_path)
    try:
        port = httpd.server_address[1]
        req = Request(f"http://{LOCAL_VIEWER_HOST}:{port}/index.html")
        with urlopen(req, timeout=5.0) as resp:
            assert resp.status == 200
            assert resp.headers.get("X-Content-Type-Options") == "nosniff"
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_viewer_bundle_handler_blocks_parent_path(tmp_path: Path) -> None:
    (tmp_path / "index.html").write_text("ok", encoding="utf-8")
    httpd = _start_server(tmp_path)
    try:
        port = httpd.server_address[1]
        req = Request(f"http://{LOCAL_VIEWER_HOST}:{port}/../../../etc/passwd")
        with pytest.raises(HTTPError) as exc_info:
            urlopen(req, timeout=5.0)
        assert exc_info.value.code == 404
    finally:
        httpd.shutdown()
        httpd.server_close()


def test_serve_viewer_bundle_missing_dir_returns_2(tmp_path: Path) -> None:
    missing = tmp_path / "nope"
    assert serve_viewer_bundle(missing) == 2
