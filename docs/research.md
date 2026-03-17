# Research grounding

The Agentic Knowledge Compiler is designed in line with recent research on agentic code synthesis, memory, and correctness. This page summarizes the main papers and how they map to AKC’s components.

## Core synthesis and repair

### DeepCode (arXiv:2512.07921)

- **Focus:** Document-to-codebase synthesis with “channel optimization” via blueprint distillation, **stateful code memory**, RAG, and **closed-loop error correction**.
- **Relevance to AKC:** Code memory in `memory/` and retrieval-before-generation in the compile loop; repair step in Plan → Generate → Execute → Repair. PaperBench-style evaluation can inform future benchmarks.

### ARCS (arXiv:2504.20434)

- **Focus:** **Synthesize–execute–repair** over a frozen LLM; retrieval-before-generation; **provable** termination, monotonic improvement, and bounded cost; tiered controller (Small/Medium/Large).
- **Relevance to AKC:** The compile loop (Plan → Retrieve → Generate → Execute → Repair) and optional tiered controller for latency/quality; emphasis on retrieval and repair for correctness.

### DocAgent (Meta, arXiv:2504.08725)

- **Focus:** Multi-agent roles (Reader, Searcher, Writer, Verifier, Orchestrator); **topological** processing of code and dependencies.
- **Relevance to AKC:** Ingest and compile can adopt role-based or topological processing where useful; verification as an explicit step.

## Reasoning and acting

### ReAct (arXiv:2210.03629)

- **Focus:** Interleaved **reasoning** and **acting**; foundation for tool use and iterative refinement.
- **Relevance to AKC:** Plan state and the iterative compile loop (plan → act → observe → repair) align with ReAct-style reasoning and acting.

### ActMem (arXiv:2603.00026)

- **Focus:** **Memory and reasoning**; causal/semantic graphs over dialogue; conflict detection.
- **Relevance to AKC:** Optional knowledge/causal graph in `memory/` for “why” and conflict detection; plan state as a form of working memory.

## Formal verification (optional later phase)

- **AlphaVerus:** Self-improving verified code generation.
- **AlgoVeri:** Benchmarks for Dafny/Veris/Lean.
- **ProofWright:** Agentic verification.

These can support a future “correctness guarantees” phase (e.g. Dafny/Verus or agentic verifiers for critical paths).

## Inputs in practice

- **Messaging (Slack, Discord, Teams, Matrix):** Auth (OAuth or platform-specific); structure as **Q&A from threads** (Snyk-style) rather than raw dumps; channel/date/user filters. Connectors should implement a common messaging abstraction.
- **Docs/APIs:** Chunking, embedding, retrieval; optional schema extraction (OpenAPI) for API-derived workflows.
- **Living docs:** Bidirectional sync and validation (e.g. SpecWeave-style) can be a later “living system” feature.

## Ingestion-specific choices

- **Chunking:** Default to recursive/structure-aware chunking with overlap to preserve coherence and improve retrieval quality. This aligns with common RAG chunking guidance (e.g. Glukhov/Firecrawl surveys of chunking strategies).
- **OpenAPI ingestion:** Prefer operation-level chunks (“`GET /users`”) plus an endpoint inventory document, matching the “discover relevant endpoints, then retrieve details” pattern described in recent OpenAPI-for-agents work.

When implementing features, we align with and cite these works where relevant.
