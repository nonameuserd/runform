# Sample product constraints (demo)

This document is intentionally small so `akc ingest --connector docs --input ./examples/docs-sample` stays fast.

## API behavior

- Clients **MUST** use HTTPS for all requests to the public API.
- Request bodies **MUST NOT** include primary account numbers in plain text.

## Operations

- Breaking API changes **SHOULD** be announced at least seven days before deployment.
