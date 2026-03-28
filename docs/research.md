# Research grounding

The Agentic Knowledge Compiler is designed in line with recent research on agentic code synthesis, memory, and correctness. This page summarizes the main papers and external references to anchor design choices.

## Core synthesis and repair

### DeepCode ([arXiv:2512.07921](https://arxiv.org/abs/2512.07921))

- **Focus:** Document-to-codebase synthesis with “channel optimization” via blueprint distillation, **stateful code memory**, RAG, and **closed-loop error correction**.
- **Relevance to AKC:** Code memory in `memory/` and retrieval-before-generation in the compile loop; repair step in Plan → Generate → Execute → Repair. PaperBench-style evaluation can inform future benchmarks.
- **Implementation reference:** [HKUDS/DeepCode](https://github.com/HKUDS/DeepCode) (also cited in OSS direction planning).

### ARCS ([arXiv:2504.20434](https://arxiv.org/abs/2504.20434))

- **Focus:** **Synthesize–execute–repair** over a frozen LLM; retrieval-before-generation; **provable** termination, monotonic improvement, and bounded cost; tiered controller (Small/Medium/Large).
- **Relevance to AKC:** The compile loop (Plan → Retrieve → Generate → Execute → Repair) and optional tiered controller for latency/quality; emphasis on retrieval and repair for correctness.

### DocAgent (Meta, [arXiv:2504.08725](https://arxiv.org/abs/2504.08725))

- **Focus:** Multi-agent roles (Reader, Searcher, Writer, Verifier, Orchestrator); **topological** processing of code and dependencies.
- **Relevance to AKC:** Ingest and compile can adopt role-based or topological processing where useful; verification as an explicit step.

## Reasoning and acting

### ReAct ([arXiv:2210.03629](https://arxiv.org/abs/2210.03629))

- **Focus:** Interleaved **reasoning** and **acting**; foundation for tool use and iterative refinement.
- **Relevance to AKC:** Plan state and the iterative compile loop (plan → act → observe → repair) align with ReAct-style reasoning and acting.

### ActMem ([arXiv:2603.00026](https://arxiv.org/abs/2603.00026))

- **Focus:** **Memory and reasoning**; causal/semantic graphs over dialogue; conflict detection.
- **Relevance to AKC:** Optional knowledge/causal graph in `memory/` for “why” and conflict detection; plan state as a form of working memory.

## Comparable agents, benchmarks, and eval caveats

- **SWE-agent:** Cited in OSS direction planning as a comparable framework for how open-source agent stacks approach engineering tasks and sandboxing; project home [princeton-nlp/SWE-agent](https://github.com/princeton-nlp/SWE-agent).
- **Coding-agent / benchmark caveats:** [OpenAI note on SWE-bench Verified (Feb 2026)](https://openai.com/index/why-we-no-longer-evaluate-swe-bench-verified/) — useful context when interpreting leaderboard-style results (contamination, methodology).

## OSS security, supply chain, and CI (planning anchors)

These appear in OSS direction and hardening plans as **patterns**, not prescriptions to copy blindly; AKC keeps tenant-scoped, artifact-local truth.

- [OpenSSF OSPS Baseline](https://baseline.openssf.org/) and [maintainer guidance](https://baseline.openssf.org/maintainers.html).
- [OpenSSF Scorecard](https://github.com/ossf/scorecard) — automated checks and remediation patterns.
- [SLSA build provenance (v1.2)](https://slsa.dev/spec/v1.2/build-provenance) and [slsa-github-generator](https://github.com/slsa-framework/slsa-github-generator).
- [GitHub Actions security hardening](https://docs.github.com/en/actions/how-tos/security-for-github-actions/security-guides/security-hardening-for-github-actions).
- [CII OpenSSF Best Practices badge](https://bestpractices.dev) — criteria alignment for dependencies, versioning, and docs.

## Observability and telemetry contracts

- **LLM spans:** [OpenTelemetry GenAI semantic conventions](https://opentelemetry.io/docs/specs/semconv/gen-ai/gen-ai-spans/) (`gen_ai.`*, token usage where available) — cited for control-plane / trace alignment.
- **Distributed correlation:** [W3C Trace Context](https://www.w3.org/TR/trace-context/) — optional propagation for cross-service use; compile run id as correlation id is already a reasonable default.

## Multi-agent coordination and workflow graphs

Industry and framework references used when reasoning about coordination semantics, fork/join, and dynamic parallelism (see coordination plans):

- [Microsoft AutoGen — GraphFlow](https://microsoft.github.io/autogen/stable//user-guide/agentchat-user-guide/graph-flow.html).
- [LangGraph persistence](https://docs.langchain.com/oss/python/langgraph/persistence) — checkpointing / state persistence patterns.
- **Fork/join:** [Orkes Conductor fork/join operator](https://orkes.io/content/reference-docs/operators/fork-join) — classical parallel-branch aggregation pattern.
- **Dynamic parallelism:** [LangGraph `Send()` / map-reduce style branches](https://medium.com/@astropomeai/implementing-map-reduce-with-langgraph-creating-flexible-branches-for-parallel-execution-b6dc44327c0e), [Send API overview](https://medium.com/@vishy2k5/langgraph-send-api-7aaab56bc6b8) — variable fan-out at plan time.
- **Advanced (optional):** [HyperFlow process-network model](https://www.sciencedirect.com/science/article/pii/S0167739X15002770) — hypergraph-style workflows; only if coordination needs outgrow DAGs.

## Identity, policy, and audit trails

- **Workload identity:** [SPIFFE workload endpoint](https://spiffe.io/docs/latest/spiffe-specs/spiffe_workload_endpoint/); [SPIFFE + OPA with Envoy](https://spiffe.io/docs/latest/microservices/envoy-jwt-opa/readme/) — common patterns for mutual authentication and authorization between autonomous workloads; AKC maps this to cryptographic binding of role identity to bundle hash + tenant scope in phased designs.
- **Policy engines:** [Open Policy Agent](https://www.openpolicyagent.org/docs/latest) — reference for policy-as-code patterns.
- **Audit narrative:** [Multi-agent systems and synchronized audit trails](https://dev.to/custodiaadmin/multi-agent-systems-need-synchronized-audit-trails-5da5) — correlating decisions across handoffs; AKC emphasizes immutable artifact hashes + replay.

## Reconciliation, operators, and progressive delivery

References from runtime and “recompile” planning for operator-style control loops, leases, and progressive analysis:

- **Kubernetes:** [Controller](https://kubernetes.io/docs/concepts/architecture/controller/), [Operator pattern](https://kubernetes.io/docs/concepts/extend-kubernetes/operator/), [Leases](https://kubernetes.io/docs/concepts/architecture/leases), [Liveness/readiness/startup probes](https://kubernetes.io/docs/concepts/configuration/liveness-readiness-startup-probes/), [Deployments](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/), [Pod Security Standards](https://kubernetes.io/docs/concepts/security/pod-security-standards/), [Recommended labels](https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/).
- **Controller-runtime:** [FAQ (reconciliation semantics)](https://github.com/kubernetes-sigs/controller-runtime/blob/main/FAQ.md).
- **GitOps / progressive:** [Flux Kustomization reconciliation](https://fluxcd.io/flux/components/kustomize/kustomizations/), [Argo Rollouts — analysis](https://argo-rollouts.readthedocs.io/en/stable/features/analysis/), [Argo Rollouts — canary](https://argo-rollouts.readthedocs.io/en/stable/features/canary/), [Argo CD application spec](https://argo-cd.readthedocs.io/en/release-3.0/user-guide/application-specification/) (declarative desired state).
- **SRE:** [Canarying releases](https://sre.google/workbook/canarying-releases/), [Error budgets](https://sre.google/workbook/error-budget-policy/).
- **SLO vocabulary (patterns):** [OpenSLO](https://openslo.com/), [Harness SLO-as-code](https://developer.harness.io/docs/service-reliability-management/slo/slo-as-code) — declarative SLO targets; AKC evaluates exported evidence bundles, not a hosted TSDB.

## RAG, retrieval quality, and evaluation

- [RAGAS evaluation framework](https://arxiv.org/abs/2309.15217) — grounding-focused RAG evaluation dimensions.
- [Google Cloud — optimizing RAG retrieval](https://cloud.google.com/blog/products/ai-machine-learning/optimizing-rag-retrieval) — retrieval quality patterns.
- [Evidently AI — RAG evaluation guide](https://evidentlyai.com/llm-guide/rag-evaluation) — offline eval dimensions for RAG systems.

## Human quality, UX, and governance anchors

References used to ground quality dimensions such as `taste`, `judgment`, and `user_empathy`:

- [ISO 9241-210:2019](https://www.iso.org/standard/77520.html) — human-centered design principles.
- [NN/g — 10 Usability Heuristics](https://www.nngroup.com/articles/ten-usability-heuristics/) — user control, error prevention, and recovery heuristics.
- [NN/g — Aesthetic-Usability Effect](https://www.nngroup.com/articles/aesthetic-usability-effect/) — perceived usability impact from visual quality.
- [NIST AI RMF 1.0](https://doi.org/10.6028/NIST.AI.100-1) — AI risk/governance framing for judgment and controls.
- [Data Mesh Principles](https://martinfowler.com/articles/data-mesh-principles.html) — domain ownership and bounded-context framing.
- [ACM Software Engineering Code of Ethics](https://www.acm.org/code-of-ethics/software-engineering-code) — engineering-discipline and professional obligations.

## Lead time, DORA, and “time compression” measurement

Used in time-compression benchmark planning to anchor metric semantics (lead time, anti-gaming, caveats for AI-assist speedup claims):

- [DORA metrics guide](https://dora.dev/guides/dora-metrics/) and [history](https://dora.dev/guides/dora-metrics/history).
- [Apache DevLake — lead time for changes](https://devlake.apache.org/docs/Metrics/LeadTimeForChanges/).
- [METR — exploratory transcript analysis for coding-agent time savings](https://metr.org/notes/2026-02-17-exploratory-transcript-analysis-for-estimating-time-savings-from-coding-agents/) — methodology caveats for upper-bound factors.

## Sandboxing and container security

From Rust/Docker sandbox hardening plans:

- [Docker — seccomp](https://docs.docker.com/engine/security/seccomp/), `[docker run` reference](https://docs.docker.com/reference/cli/docker/container/run/).
- [OWASP Docker Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Docker_Security_Cheat_Sheet.html).

## Compiler IR and system representation (background)

- [Intermediate representation (IR) for compilers](https://thelinuxcode.com/intermediate-representation-ir-for-working-compilers/) — general background for IR-as-spine discussions.

## Formal verification (optional later phase)

- **AlphaVerus:** Self-improving verified code generation.
- **AlgoVeri:** Benchmarks for Dafny/Veris/Lean.
- **ProofWright:** Agentic verification.

These can support a future “correctness guarantees” phase (e.g. Dafny/Verus or agentic verifiers for critical paths). Add stable public links here when the project standardizes on specific versions or papers.

## Inputs in practice

- **Messaging (current and future):** Today the repo ships Slack, Discord, Telegram, and WhatsApp-oriented messaging ingest paths. The common design goal is still the same: structure messages as **Q&A from threads** rather than raw dumps, with auth and channel/date/user filters where the platform supports them.
- **Docs/APIs:** Chunking, embedding, retrieval; optional schema extraction (OpenAPI) for API-derived workflows.
- **Living docs:** Bidirectional sync and validation (e.g. SpecWeave-style) can be a later “living system” feature.

## Ingestion-specific choices

- **Chunking:** Default to recursive/structure-aware chunking with overlap to preserve coherence and improve retrieval quality. This aligns with common RAG chunking guidance (e.g. Glukhov/Firecrawl surveys of chunking strategies).
- **OpenAPI ingestion:** Prefer operation-level chunks (“`GET /users`”) plus an endpoint inventory document, matching the “discover relevant endpoints, then retrieve details” pattern described in recent OpenAPI-for-agents work.

## Platform and delivery (supplementary)

We occasionally cite **golden paths** and developer-portal patterns; these are product/delivery context, not core AKC research:

- [Backstage — software templates](https://backstage.io/docs/next/features/software-templates/), [what is Backstage](https://backstage.io/docs/overview/what-is-backstage).
- [Glen Thomas — golden paths / opinionated platforms](https://blog.glen-thomas.com/platform%20engineering/2024/03/01/building-golden-paths-designing-opinionated-platforms.html), [Jellyfish — golden paths](https://jellyfish.co/library/platform-engineering/golden-paths/).

Mobile and store-specific distribution links (TestFlight, Firebase App Distribution, Play Developer API, PWA installability) appear in delivery plans; see [delivery-architecture.md](delivery-architecture.md) and [getting-started.md](getting-started.md) for product-facing consolidation rather than duplicating them here.

When implementing features, we align with and cite these works where relevant.
