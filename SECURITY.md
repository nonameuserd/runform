# Security

## Reporting a vulnerability

We take security seriously. If you believe you have found a security vulnerability, please report it privately so we can address it before it is disclosed publicly.

**Do not** open a public GitHub issue for security-sensitive bugs.

### How to report

- **Email:** nonameuserd007@outlook.com
- **Primary security contact:** the project team will route this inbox to the maintainers responsible for security triage.
- **Private disclosure:** Describe the issue and steps to reproduce. We will acknowledge receipt and work with you to understand and fix the issue.
- **What we ask:** Give us reasonable time to address the report before any public disclosure. We will keep you updated and credit you in advisories if you wish.

### What to expect

- We will acknowledge your report promptly.
- We will investigate and confirm the issue, then work on a fix and release plan.
- We may ask for additional information. We will not share your report publicly without your agreement (except as needed for an advisory after a fix).

### Scope

- Security issues in this repository (agentic knowledge compiler core, ingestion, memory, compile loop, outputs) and its dependencies as used by the project.
- Out of scope: issues in third-party services or connectors you run yourself (e.g. Slack, Discord) unless the vulnerability is in our connector code or configuration guidance.

Thank you for helping keep the project and its users safe.

## Execution sandbox incidents

If you believe you have found a vulnerability that could let generated code escape the execution sandbox (e.g., file system, network, environment variables, or cross-tenant access), please report it as a security incident using the private disclosure process above. Include:

- what capability/limits were expected to prevent the impact
- the exact tenant/run identifiers involved (if applicable)
- reproduction steps and logs (sanitize secrets)
