## Platform migrations + deletes (fixture)

### Purpose

Migration runbooks often contain destructive steps; this fixture ensures the compiler/runtime
projects those constraints as deny rules from knowledge.

### Requirements (normative)

- **Destructive operations are forbidden by default**.
  - Operators MUST NOT drop databases, wipe storage, or purge production namespaces without explicit approval.
  - `rm -rf` is considered destructive.
- **Network constraints**: Outbound network egress should be treated as restricted in migration tooling.
  - Internet egress requires approval.

