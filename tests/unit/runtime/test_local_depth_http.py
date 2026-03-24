from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.runtime.init import create_local_depth_runtime
from akc.runtime.models import (
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeContext,
    RuntimeNodeRef,
)


class _OkHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"akc_http_ok")

    def log_message(self, _format: str, *_args: object) -> None:  # noqa: A003
        return


def test_local_depth_http_hits_allowlisted_local_server(tmp_path) -> None:
    server = HTTPServer(("127.0.0.1", 0), _OkHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        url = f"http://127.0.0.1:{port}/ping"
        context = RuntimeContext(
            tenant_id="tenant-a",
            repo_id="repo-a",
            run_id="compile-1",
            runtime_run_id="runtime-1",
            policy_mode="enforce",
            adapter_id="local_depth",
        )
        out_keys = (
            "action_id",
            "action_type",
            "adapter_id",
            "route",
            "http_status_code",
            "http_latency_ms",
            "http_url_redacted",
            "http_response_snippet",
        )
        contract = OperationalContract(
            contract_id="contract-1",
            contract_category="runtime",
            triggers=(
                ContractTrigger(
                    trigger_id="kernel_started",
                    source="runtime.kernel.started",
                    details={"event_type": "runtime.kernel.started"},
                ),
            ),
            io_contract=IOContract(input_keys=("runtime_run_id",), output_keys=out_keys),
        )
        bundle = RuntimeBundle(
            context=context,
            ref=RuntimeBundleRef(
                bundle_path=str(tmp_path / "runtime_bundle.json"),
                manifest_hash="a" * 64,
                created_at=1,
                source_compile_run_id="compile-1",
            ),
            nodes=(RuntimeNodeRef(node_id="node-1", kind="workflow", contract_id="contract-1"),),
            contract_ids=("contract-1",),
            policy_envelope={},
            metadata={
                "runtime_execution": {
                    "allow_http": True,
                    "http_allowlist": [f"http://127.0.0.1:{port}"],
                    "http_method_allowlist": ["GET"],
                },
                "referenced_ir_nodes": [
                    {
                        "id": "node-1",
                        "tenant_id": context.tenant_id,
                        "kind": "workflow",
                        "name": "Workflow 1",
                        "properties": {
                            "runtime_execution": {
                                "route": "http",
                                "http": {"url": url, "method": "GET", "timeout_ms": 5000},
                            }
                        },
                        "depends_on": [],
                        "contract": contract.to_json_obj(),
                    }
                ],
                "referenced_contracts": [contract.to_json_obj()],
            },
        )
        kernel = create_local_depth_runtime(bundle, outputs_root=tmp_path)
        result = kernel.run_until_terminal(max_iterations=10)
        assert result.status == "terminal"
        completed = [e for e in result.emitted_events if e.event_type == "runtime.action.completed"]
        assert completed
        out = completed[-1].payload["result"]["outputs"]
        assert out.get("http_status_code") == 200
        assert "akc_http_ok" in str(out.get("http_response_snippet", ""))
        assert str(out.get("http_url_redacted", "")).startswith(f"http://127.0.0.1:{port}")
    finally:
        server.shutdown()
        thread.join(timeout=2)
