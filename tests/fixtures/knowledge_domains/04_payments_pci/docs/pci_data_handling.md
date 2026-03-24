## Payments + PCI data handling (fixture)

### Scope

This document defines non-negotiable constraints for payment and billing systems.

### Requirements (normative)

1. **PII and secrets**: Personally identifiable information (PII), cardholder data, and API keys are secrets.
   - Systems **MUST NOT** log secrets or PII to external logs.
   - Systems **MUST NOT** export API keys to third-party analytics.
2. **Network / egress**: Public internet egress is restricted.
   - Outbound HTTPS calls to external APIs require explicit approval.
3. **Destructive operations**: Destructive delete/purge actions are forbidden by default.
   - Operators **MUST NOT** perform `rm -rf`, purge, or wipe operations on production billing volumes without a signed change record.

