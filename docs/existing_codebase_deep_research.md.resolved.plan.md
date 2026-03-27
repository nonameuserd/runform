# Deep Research: AKC Critical Path — Existing Codebases

## Executive Summary

This document synthesizes research from the AKC codebase (docs, source, tests) and external prior art (SWE-agent, OpenHands, Aider, GitHub Linguist, AST-aware tooling) to recommend how AKC should implement the four pillars: **Adopt don't restart**, **Language-aware execution**, **Safe realization**, and **Progressive takeover**.

> [!IMPORTANT]
> AKC already has strong foundations for pillars 3 and 4 via [scoped_apply](file:///Users/dami/Documents/Runform/src/akc/compile/scoped_apply.py#52-74), `artifact_only`, policy bundles, and the `emerging` developer profile. The main gaps are in pillars 1 and 2: **project detection/adoption** and **language-aware toolchain execution**.

---

## 1. Adopt, Don't Restart

### What exists today

- `akc init` creates `.akc/project.json` with tenant/repo scope, policy stubs, and developer profile — but **no project analysis**.
- `akc compile` works from a compile plan (Plan → Retrieve → Generate → Execute → Repair), producing **unified diffs**, not greenfield scaffolds.
- [scoped_apply](file:///Users/dami/Documents/Runform/src/akc/compile/scoped_apply.py#52-74) already applies patches against an existing working tree under strict path confinement.
- Code memory (`src/akc/memory/`) persists generated/existing code artifacts to prevent hallucination.

### What's missing

| Gap | Description |
|-----|-------------|
| **Project fingerprinting** | No automated detection of project structure, conventions, or architecture patterns |
| **Convention extraction** | No analysis of naming conventions, file organization patterns, import styles |
| **Existing code ingestion** | The `docs` ingest connector handles docs; there's no first-class "codebase" ingest connector |
| **Architecture graph** | No automated extraction of module boundaries, dependency DAGs, or layer patterns |

### Recommended approach

#### 1a. New `ProjectProfile` model (extends `akc init`)

Introduce a **`ProjectProfile`** data class emitted by `akc init --detect` (or `akc adopt`):

```python
@dataclass
class ProjectProfile:
    root: Path
    languages: list[LanguageEntry]        # detected languages + percentages
    package_managers: list[str]           # npm, cargo, uv/pip, etc.
    build_commands: list[BuildCommand]    # detected build/test/lint commands
    ci_systems: list[CISystem]           # GitHub Actions, GitLab CI, etc.
    conventions: ConventionSnapshot       # extracted naming/structure patterns
    entry_points: list[str]              # main files, CLI entries
    architecture_hints: dict[str, Any]   # monorepo, workspace, etc.
```

#### 1b. Language detection (GitHub Linguist-style)

Use a **deterministic, heuristic-first** approach (no ML needed):

```
Detection order (parallel where possible):
1. Manifest files → definitive stack signals
   • package.json          → Node/JS/TS
   • Cargo.toml            → Rust
   • pyproject.toml/setup.py → Python
   • go.mod                → Go
   • pom.xml/build.gradle  → Java/Kotlin
   • tsconfig.json         → TypeScript (refines JS)
   
2. File extension census → language percentages
   • .py, .rs, .ts, .js, .go, .java (weighted by LOC or bytes)
   
3. CI/build file detection
   • .github/workflows/   → GitHub Actions
   • .gitlab-ci.yml       → GitLab CI
   • Jenkinsfile          → Jenkins
   • Makefile             → make-based
   
4. Build/test command extraction
   • Parse scripts from package.json, Makefile, pyproject.toml
   • Extract test runners: pytest, jest, cargo test, go test
```

**Prior art alignment:**
- **GitHub Linguist**: Uses manifest files → extensions → heuristics → Bayesian classification. We only need the first two layers since we're not classifying ambiguous files but detecting the *project stack*.
- **Aider**: Builds a "repo map" by indexing the codebase with tree-sitter for function/class signatures. We should adopt a similar approach for convention extraction.

#### 1c. Convention extraction

Leverage tree-sitter (already used in many code analysis tools) or simpler AST parsing:

- **Naming conventions**: camelCase vs snake_case, file naming patterns
- **Import style**: relative vs absolute, aliasing patterns
- **Directory structure**: src/ vs lib/, flat vs nested modules
- **Test organization**: colocated vs separate `tests/` directory

Store as part of `ProjectProfile` and inject into compile prompts so generated code matches existing conventions.

#### 1d. Codebase ingest connector

Add a new ingest connector `codebase` under `src/akc/ingest/connectors/`:

```
akc ingest codebase --project-dir . --languages python,typescript
```

This connector would:
- Walk the source tree (respecting `.gitignore`)
- Chunk by file and function/class boundaries (tree-sitter aware)
- Build embeddings for retrieval during compile
- Populate code memory with existing code

---

## 2. Language-Aware Execution

### What exists today

- The executor layer (`src/akc/execute/`, `src/akc/compile/executors.py`) supports sandbox lanes (WASM, OS process, Docker).
- The compile controller runs tests via `ControllerConfig` with configurable commands.
- The Rust bridge (`src/akc/compile/rust_bridge.py`) already handles cross-language execution.

### What's missing

| Gap | Description |
|-----|-------------|
| **Toolchain resolution** | No auto-detection of which test/build/lint commands to run |
| **Environment validation** | No preflight check that required tools (node, cargo, python, etc.) exist |
| **Multi-language orchestration** | No coordination when a repo has Python + Rust + TS |
| **Fail-closed missing env** | Currently silent if environment tools are absent |

### Recommended approach

#### 2a. Toolchain resolver

```python
@dataclass
class ToolchainProfile:
    language: str
    package_manager: str | None         # npm, cargo, pip/uv
    test_command: list[str]             # ["pytest", "-x"]
    build_command: list[str] | None     # ["cargo", "build"]
    lint_command: list[str] | None      # ["ruff", "check", "."]
    format_command: list[str] | None    # ["prettier", "--check", "."]
    install_command: list[str] | None   # ["npm", "ci"]
    required_binaries: list[str]        # ["python3", "node", "cargo"]
```

Resolution order:
1. **Explicit** (from `.akc/project.json` `toolchain` key or CLI flags)
2. **Extracted** (from `ProjectProfile` manifest analysis)
3. **Conventional** (language-specific defaults: Python → pytest, JS → jest, Rust → cargo test)

#### 2b. Environment preflight gate

Before any compile run against an existing codebase, run a **fail-closed preflight**:

```python
def preflight_toolchain(profile: ToolchainProfile) -> PreflightResult:
    """Check every required binary exists and reports version."""
    missing = []
    for binary in profile.required_binaries:
        if not shutil.which(binary):
            missing.append(binary)
    if missing:
        return PreflightResult(ok=False, missing=missing)
    return PreflightResult(ok=True, versions={...})
```

This integrates with `ControllerConfig` — compile refuses to start if preflight fails (fail-closed).

#### 2c. Native validation command execution

The compile loop should delegate to the project's **native** test/build commands instead of synthetic tests:

- **Smoke**: Run the project's lint + type-check (`ruff check .`, `tsc --noEmit`, `cargo check`)
- **Full**: Run the project's full test suite (`pytest`, `npm test`, `cargo test`)

The existing `ControllerConfig.test_mode` can be extended with a `native` option that shells out to extracted commands.

**Prior art:**
- **SWE-agent**: Uses an Agent-Computer Interface (ACI) that detects and interacts with the repo's native tooling. Commands are validated/sandboxed but use the real project tools.
- **OpenHands**: Runs within Docker sandboxes but executes the project's actual build/test commands.
- **Aider**: Runs `pytest`, `npm test`, etc. directly using the repo's existing test infrastructure.

---

## 3. Safe Realization

### What exists today — **Strong foundation**

| Component | Status |
|-----------|--------|
| `scoped_apply` | ✅ Fail-closed, path-confined patch application with SHA-256 verification |
| `artifact_only` mode | ✅ Zero working-tree writes; patches as audit artifacts only |
| Policy bundles (OPA/Rego) | ✅ Default-deny policy for `compile.patch.apply` |
| Tenant isolation | ✅ All paths tenant+repo scoped; cross-tenant writes prevented |
| Auditable mutation history | ✅ Run manifests, patch SHA-256, trace spans, cost accounting |
| Verifier gate | ✅ Can veto unsafe patches even when tests pass |

### What to add for existing codebase safety

| Enhancement | Description |
|-------------|-------------|
| **Path allowlist from project config** | Let `.akc/project.json` declare `mutation_paths` (e.g. `["src/", "tests/"]`) so patches to `package.json`, CI configs, or infra are denied by default |
| **Change-scope categorization** | Classify diffs as `code`, `config`, `infra`, `ci`, `dependency` and apply different policy gates per category |
| **Rollback snapshots** | Before scoped_apply, snapshot affected files (already partially implemented via staging in `_stage_patch_inputs`) |
| **Branch-aware apply** | When operating against a git repo, optionally create a branch for each compile run for easy revert |

### Prior art alignment

- **SWE-agent**: Rejects syntax-breaking edits, uses Docker isolation, comprehensive audit logs.
- **OpenHands**: Event-driven state management, sandboxed runtime, developer control over scope.
- **Aider**: Auto-creates Git commits for reversibility. AKC should support `--git-commit` mode.

---

## 4. Progressive Takeover

### What exists today — **Strong primitives**

- `emerging` developer profile defaults toward higher automation.
- Living bridge (`src/akc/living/`) supports drift → recompile triggers.
- Autopilot (`src/akc/runtime/autopilot.py`) with lease-based single-writer concurrency.
- Compile skills (`src/akc/compile/skills/`) inject project-specific guidance into LLM prompts.

### Recommended progressive adoption ladder

```
Level 0: Observer (read-only)
├─ akc init --detect          → Analyze project, produce ProjectProfile
├─ akc ingest codebase        → Index existing code for retrieval
└─ akc view                   → Browse analysis results

Level 1: Advisor (artifact-only)
├─ akc compile --artifact-only → Generate patches as artifacts
├─ Human reviews diffs        → Operator applies manually
└─ Evidence accumulates       → Trust builds over time

Level 2: Co-pilot (scoped apply)
├─ akc compile                → Scoped apply under policy gates
├─ Path allowlists            → Mutation confined to approved dirs
├─ Git integration            → Branch-per-compile for safe rollback
└─ Native test validation     → Project's own tests gate acceptance

Level 3: Compiler (larger slices)
├─ akc compile + runtime      → Compile services + deploy workflows
├─ Coordination graphs        → Multi-agent orchestration
├─ Living recompile           → Drift-triggered re-synthesis
└─ Reliability SLO gates      → Measured before expansion

Level 4: Full autonomy (intent → system)
├─ akc runtime autopilot      → Continuous reconciliation
├─ Fleet automation           → Cross-repo coordination
└─ Policy-only human role     → Humans define boundaries only
```

### Trust accumulation model

Each level should require **evidence** before unlocking the next:

| Transition | Required Evidence |
|------------|-------------------|
| L0 → L1 | `ProjectProfile` validated, codebase indexed |
| L1 → L2 | N consecutive artifact-only runs with human-approved diffs |
| L2 → L3 | Reliability scoreboard passes (policy compliance ≥ 0.95, zero rollbacks) |
| L3 → L4 | Consecutive SLO window passes with full test coverage |

This maps naturally to the existing `reliability_scoreboard` and `convergence_certificate` infrastructure.

---

## 5. Implementation Priority (What to Build First)

### Phase 1: Project Detection & Adoption (Pillar 1 + 2)

New packages/modules:

| Module | Purpose |
|--------|---------|
| `src/akc/adopt/` | New package for project analysis |
| `src/akc/adopt/detect.py` | Language/stack detection (manifest + extension heuristics) |
| `src/akc/adopt/profile.py` | `ProjectProfile` model |
| `src/akc/adopt/toolchain.py` | `ToolchainProfile` resolution + preflight |
| `src/akc/adopt/conventions.py` | Convention extraction (naming, imports, structure) |
| `src/akc/ingest/connectors/codebase.py` | Codebase ingest connector |
| `src/akc/cli/adopt.py` | CLI commands (`akc adopt` or `akc init --detect`) |

Extend existing:

| File | Changes |
|------|---------|
| `src/akc/cli/init.py` | Add `--detect` flag to run project analysis |
| `src/akc/compile/controller_config.py` | Add `toolchain_profile` and `native_test_mode` |
| `src/akc/compile/controller.py` | Integrate native test commands from toolchain profile |
| `.akc/project.json` schema | Add `toolchain`, `mutation_paths`, `adoption_level` keys |

### Phase 2: Enhanced Safety for Existing Repos (Pillar 3)

| File | Changes |
|------|---------|
| `src/akc/compile/scoped_apply.py` | Path allowlist from project config, change-scope categorization |
| `src/akc/compile/session.py` | Git-aware branching mode |
| Policy stubs | Category-specific policy rules |

### Phase 3: Progressive Takeover Infrastructure (Pillar 4)

| Module | Purpose |
|--------|---------|
| `src/akc/adopt/trust_ladder.py` | Adoption level tracking + evidence requirements |
| Extend `ProjectProfile` | Track current adoption level + evidence accumulation |
| Extend reliability scoreboard | Include adoption-level promotion in SLO gates |

---

## 6. Key Design Decisions

### Decision 1: `akc adopt` vs extending `akc init`

**Recommendation: New `akc adopt` command** that internally calls `akc init` if needed.

Rationale: `akc init` is lightweight (write project.json). `akc adopt` implies *analysis* of an existing repo — heavier, optional, and conceptually distinct. The adopt command can produce a richer `project.json` informed by detection.

### Decision 2: Tree-sitter vs simple parsing for convention extraction

**Recommendation: Start with simple heuristics, add tree-sitter later.**

Rationale: For convention extraction (naming styles, import patterns), regex + file-system analysis covers 80% of value. Tree-sitter adds precise AST awareness but also a dependency. Phase 1 should use simple heuristics; Phase 2 can add tree-sitter for deeper analysis.

### Decision 3: Native test execution vs synthetic tests

**Recommendation: Support both, prefer native when available.**

The compile controller should:
1. Use native commands when `ToolchainProfile` is available
2. Fall back to synthetic test generation (existing behavior) when no native tests exist
3. Allow operators to force one mode via `ControllerConfig`

### Decision 4: Git integration for rollback

**Recommendation: Opt-in `--git-commit` / `--git-branch` flags on `akc compile`.**

When enabled:
- Create a branch `akc/compile/<run_id>` before apply
- Commit applied patches with structured commit messages
- Link branch/commit SHA in the run manifest

This gives operators git-native rollback without changing the core patch application model.

---

## 7. Research Sources

### Codebase (internal)

| Document | Key Insights |
|----------|-------------|
| [architecture.md](file:///Users/dami/Documents/Runform/docs/architecture.md) | Full pipeline layout, package roles, compile/runtime contracts |
| [ir-schema.md](file:///Users/dami/Documents/Runform/docs/ir-schema.md) | IR node kinds, operational contracts, provenance pointers |
| [runtime-execution.md](file:///Users/dami/Documents/Runform/docs/runtime-execution.md) | Action routing, checkpoints, reconciliation, autopilot |
| [compile-skills.md](file:///Users/dami/Documents/Runform/docs/compile-skills.md) | Skill injection for project-aware prompts |
| [akc-alignment.md](file:///Users/dami/Documents/Runform/docs/akc-alignment.md) | Current alignment status, honest gaps |
| [scoped_apply.py](file:///Users/dami/Documents/Runform/src/akc/compile/scoped_apply.py) | Existing safe patch application |
| [controller.py](file:///Users/dami/Documents/Runform/src/akc/compile/controller.py) | ARCS-style compile loop |
| [init.py](file:///Users/dami/Documents/Runform/src/akc/cli/init.py) | Current `akc init` implementation |

### External prior art

| Source | Key Insights |
|--------|-------------|
| **SWE-agent** | ACI for LLM agents, Docker isolation, syntax-reject edits, audit logs |
| **OpenHands** | Event-driven state, sandboxed runtime, autonomous CVE patching |
| **Aider** | Repo map via tree-sitter, Git auto-commits, coding conventions, chat modes |
| **GitHub Linguist** | Manifest → extension → heuristics → Bayesian classification pipeline |
| **DeepCode** (paper) | Code memory, retrieval-before-generation, closed-loop error correction |
| **ARCS** (paper) | Synthesize–execute–repair, tiered controller, provable termination |
