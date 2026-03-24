from __future__ import annotations

import sys
from pathlib import Path

from akc.ir import ContractTrigger, IOContract, OperationalContract
from akc.runtime.init import create_local_depth_runtime
from akc.runtime.models import (
    RuntimeBundle,
    RuntimeBundleRef,
    RuntimeContext,
    RuntimeNodeRef,
)


def _subprocess_bundle(tmp_path: Path) -> RuntimeBundle:
    context = RuntimeContext(
        tenant_id="tenant-a",
        repo_id="repo-a",
        run_id="compile-1",
        runtime_run_id="runtime-1",
        policy_mode="enforce",
        adapter_id="local_depth",
    )
    exe_base = Path(sys.executable).name
    out_keys = ("action_id", "action_type", "adapter_id", "exit_code", "stdout", "stderr")
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
        io_contract=IOContract(
            input_keys=("runtime_run_id",),
            output_keys=out_keys,
        ),
    )
    return RuntimeBundle(
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
                "allow_subprocess": True,
                "subprocess_allowlist": [exe_base],
            },
            "referenced_ir_nodes": [
                {
                    "id": "node-1",
                    "tenant_id": context.tenant_id,
                    "kind": "workflow",
                    "name": "Workflow 1",
                    "properties": {
                        "runtime_execution": {
                            "route": "subprocess",
                            "subprocess": {
                                "argv": [sys.executable, "-c", "print('akc_subprocess_ok')"],
                                "timeout_ms": 5000,
                            },
                        }
                    },
                    "depends_on": [],
                    "contract": contract.to_json_obj(),
                }
            ],
            "referenced_contracts": [contract.to_json_obj()],
        },
    )


def test_local_depth_subprocess_runs_under_tenant_scoped_cwd(tmp_path: Path) -> None:
    bundle = _subprocess_bundle(tmp_path)
    kernel = create_local_depth_runtime(bundle, outputs_root=tmp_path)

    result = kernel.run_until_terminal(max_iterations=10)

    assert result.status == "terminal"
    assert result.last_checkpoint.node_states["node-1"]["state"] == "completed"
    cwd = (
        tmp_path
        / bundle.context.tenant_id
        / "repo-a"
        / ".akc"
        / "runtime"
        / bundle.context.run_id
        / bundle.context.runtime_run_id
    )
    assert cwd.is_dir()
    completed = [e for e in result.emitted_events if e.event_type == "runtime.action.completed"]
    assert completed
    last_out = completed[-1].payload["result"]["outputs"]
    assert last_out["exit_code"] == 0
    assert "akc_subprocess_ok" in str(last_out.get("stdout", ""))
