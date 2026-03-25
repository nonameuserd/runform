# Compile-time Agent Skills

AKC can load [Agent Skills](https://agentskills.io/specification) packages—directories with a `SKILL.md` file (YAML front matter plus Markdown body)—and inject selected skill bodies into the **patch-generation** LLM system prompt. Skills are **untrusted text**: they are read from disk under configured roots, size-capped, and included for the model to follow; AKC does **not** execute `scripts/` or other assets.

## Skill directories (on-disk layout)

Each skill is an **immediate child directory** of a scan root, with a **literal** `SKILL.md` file:

```text
<scan-root>/
  my-skill/
    SKILL.md
```

Directory names must match `[A-Za-z0-9][A-Za-z0-9_.-]*`. Only `SKILL.md` is loaded for injection; optional `scripts/`, `references/`, and `assets/` from the spec are ignored by the compiler unless you document them for humans or future tooling.

Front matter is parsed with a small subset (scalar `key: value` lines). Supported keys used by AKC include:

- `name` — logical id (falls back to the directory name if omitted).
- `description` — used in **`auto`** mode for keyword overlap scoring against the compile goal and intent text.
- `disable-model-invocation` (or `disable_model_invocation`) — when true, the skill is excluded from **`auto`** unless it is explicitly listed in the allowlist.

## Where AKC looks (discovery order)

Discovery builds a catalog in this order; **the first occurrence of a given skill `name` wins** if the same name appears twice.

1. **Bundled default** — packaged `akc-default` skill (always available unless discovery fails).
2. **Project-relative roots** (only when a project directory is known), each scanned if it exists:
   - `.akc/skills/`
   - `.cursor/skills/`
   - Each path in `skill_roots` from `.akc/project.json` / `.akc/project.yaml` (must be **relative** to the project root, not `..`, not absolute).
3. **`AKC_SKILLS_ROOT`** — optional environment variable pointing at **one** directory to treat as a scan root (useful for headless automation).
4. **Extra roots** — absolute paths from `ControllerConfig.compile_skill_extra_roots` and from the compile CLI `--compile-skill-extra-root` (relative CLI paths are resolved from the current working directory).

Every resolved skill directory and `SKILL.md` path must stay under its boundary root (project root, env root, or extra root) after resolution.

## Parity with Cursor

Cursor discovers skills under paths such as `.cursor/skills/` (see [Cursor Agent Skills](https://www.cursor.com/docs/context/skills)). AKC scans the same **Agent Skills** layout under `.cursor/skills/`, so one repo can share skills between the editor and `akc compile`.

Differences worth noting:

- AKC also scans **`.akc/skills/`** for project-local skills that need not live under `.cursor/`.
- Additional project-relative directories are configured with **`skill_roots`** in project config, not only fixed Cursor paths.
- User-global skill directories (for example under a home directory) are not mirrored automatically; use **`AKC_SKILLS_ROOT`** or **`--compile-skill-extra-root`** with an absolute path when you want that.

## `.akc/project.json` keys

The same keys apply to **`.akc/project.yaml`** when you use YAML instead of JSON (PyYAML required). Optional fields (all can be overridden or supplemented by compile CLI flags where noted):

| Key | Type | Meaning |
| --- | --- | --- |
| `compile_skills` | array of strings | Skill **names** to activate (merged with `--compile-skill`). |
| `compile_skills_mode` | string | One of `off`, `default_only`, `explicit`, `auto` (see below). |
| `skill_roots` | array of strings | Extra **project-relative** directories (each is a scan root for child skill folders). |
| `compile_skill_max_file_bytes` | positive integer | Max bytes read per `SKILL.md`. |
| `compile_skill_max_total_bytes` | positive integer | Max UTF-8 bytes for the combined injected skills preamble. |

If `compile_skills_mode` is omitted and there is **no** allowlist from project or CLI, the effective mode defaults to **`default_only`** (bundled skill only). If there **is** an allowlist, the CLI layer defaults the mode to **`explicit`** unless you set the mode explicitly.

### Modes

- **`off`** — No skills injected (including the bundled default).
- **`default_only`** — Only the bundled AKC default skill.
- **`explicit`** — Bundled default plus every skill named in `compile_skills` / `--compile-skill` that exists in the catalog.
- **`auto`** — Same as **explicit**, then adds other discovered skills whose descriptions score positively against the goal and intent; skills with `disable-model-invocation: true` are skipped unless explicitly allowlisted.

Controller defaults for byte caps (when not set in project or CLI) are **393,216** bytes per file and **98,304** bytes total injected preamble.

## Compile CLI (summary)

- `--compile-skills-mode {off,default_only,explicit,auto}`
- `--compile-skill NAME` (repeatable)
- `--compile-skill-extra-root PATH` (repeatable; absolute or cwd-relative)
- `--compile-skill-max-file-bytes N`
- `--compile-skill-max-total-bytes N`

## Replay and audit

When skills affect the system prompt, VCR / replay keys incorporate that content so recordings do not silently drift when skill files change. Run manifests record which skills were active and content fingerprints for auditability.

## Further reading

- [Agent Skills specification](https://agentskills.io/specification)
- [Cursor: Agent Skills](https://www.cursor.com/docs/context/skills)
