from __future__ import annotations

from akc.runtime.observe_probes import evaluate_observe_probes, parse_observe_probe_specs


def test_parse_observe_probe_specs_skips_non_objects() -> None:
    assert parse_observe_probe_specs(None) == ()
    assert parse_observe_probe_specs("x") == ()
    assert parse_observe_probe_specs([{"kind": "tcp", "port": 1, "host": "127.0.0.1"}]) != ()


def test_tcp_probe_closed_port_is_false() -> None:
    specs = ({"kind": "tcp", "host": "127.0.0.1", "port": 65530, "timeout_ms": 200},)
    rows = evaluate_observe_probes(specs)
    assert len(rows) == 1
    assert rows[0].type == "ProbeTcp"
    assert rows[0].status == "false"


def test_http_probe_connection_refused_is_false() -> None:
    specs = ({"kind": "http", "url": "http://127.0.0.1:65530/health", "timeout_ms": 500},)
    rows = evaluate_observe_probes(specs)
    assert rows[0].type == "ProbeHttp"
    assert rows[0].status == "false"
