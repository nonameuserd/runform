"""Compilation (Phase 2/3): plan → retrieve → generate → execute → repair."""

from akc.compile.controller import ControllerResult, run_compile_loop
from akc.compile.planner import advance_plan, create_or_resume_plan
from akc.compile.retriever import retrieve_context
from akc.compile.session import CompileSession
from akc.compile.controller_config import Budget, ControllerConfig, TierConfig
from akc.compile.executors import DockerExecutor, SubprocessExecutor
from akc.compile.interfaces import (
    Executor,
    Index,
    IndexDocument,
    IndexQuery,
    LLMBackend,
    LLMMessage,
    LLMRequest,
    LLMResponse,
    TenantRepoScope,
)
from akc.compile.repair import FailureSummary, build_repair_prompt, parse_execution_failure
from akc.compile.verifier import DeterministicVerifier, VerifierPolicy, VerifierResult
from akc.compile.vectorstore_index_adapter import VectorStoreIndexAdapter
from akc.compile.execute.rust_executor import RustExecutor

__all__ = [
    "Budget",
    "CompileSession",
    "ControllerConfig",
    "ControllerResult",
    "DockerExecutor",
    "Executor",
    "Index",
    "IndexDocument",
    "IndexQuery",
    "LLMBackend",
    "LLMMessage",
    "LLMRequest",
    "LLMResponse",
    "TenantRepoScope",
    "TierConfig",
    "SubprocessExecutor",
    "RustExecutor",
    "DeterministicVerifier",
    "VerifierPolicy",
    "VerifierResult",
    "VectorStoreIndexAdapter",
    "advance_plan",
    "FailureSummary",
    "build_repair_prompt",
    "parse_execution_failure",
    "run_compile_loop",
    "create_or_resume_plan",
    "retrieve_context",
]
