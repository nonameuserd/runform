"""
Typing stub for the optional `akc_rust` PyO3 module.

The real module is produced by the Rust crates and is loaded at runtime
when available. In this repo we treat it as an optional dependency, so we
provide this stub to keep static analyzers (and IDEs) happy.
"""

def run_exec_json(payload: str) -> str: ...
def ingest_json(payload: str) -> str: ...
