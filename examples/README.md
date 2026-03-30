# Examples

Small fixtures and command recipes for AKC. For a **no-run** compile viewer snapshot, see [`golden-viewer-demo/`](./golden-viewer-demo/).

## `akc ingest` — every supported connector

Install optional pieces as needed (from repo root):

```bash
uv sync --extra ingest-docs        # nicer Markdown/HTML for docs connector
uv sync --extra ingest-openapi     # YAML OpenAPI specs (JSON specs work without this)
uv sync --extra ingest-messaging    # official Slack SDK (stdlib fallback exists for some paths)
uv sync --extra ingest-mcp         # MCP client for mcp connector
uv sync --extra ingest-all         # all of the above + vector/embed/MCP server extras
```

Shared flags used below: persist chunks in SQLite and stay offline for embeddings:

```bash
EMBED="--embedder hash --index-backend sqlite"
```

### `docs`

Local directory of Markdown/HTML and common doc formats.

```bash
akc ingest --tenant-id demo --connector docs --input ./examples/docs-sample $EMBED
```

You can also point at this repository’s `docs/` tree (larger corpus).

### `codebase`

Repository-aware filesystem crawl from a root directory.

```bash
akc ingest --tenant-id demo --connector codebase --input ./examples/codebase-sample $EMBED
# (or index the whole repo with --input .)
```

### `openapi`

OpenAPI **3.x** spec as a file path or URL. JSON works out of the box; **YAML** needs `akc[ingest-openapi]` (PyYAML).

```bash
akc ingest --tenant-id demo --connector openapi --input ./examples/openapi/petstore.json $EMBED
# akc ingest --tenant-id demo --connector openapi --input https://example.com/openapi.json $EMBED
```

### `slack`

`--input` is a **channel id** (e.g. `C01234567`). Requires a bot or user token (`--slack-token` or `AKC_SLACK_TOKEN`).

```bash
export AKC_SLACK_TOKEN='xoxb-...'
akc ingest --tenant-id demo --connector slack --input C01234567 $EMBED
```

See [`messaging/README.md`](./messaging/README.md) for history bounds and thread limits.

### `discord`

`--input` is a **channel id** (snowflake). Token via `--discord-token` or `AKC_DISCORD_TOKEN`.

```bash
export AKC_DISCORD_TOKEN='...'
akc ingest --tenant-id demo --connector discord --input 123456789012345678 $EMBED
```

### `telegram`

Long-polls **`getUpdates`** and drains new bot events (no historic backfill). **`--input` must be the literal string `updates`**. Token: `--telegram-token` / `AKC_TELEGRAM_TOKEN`. Optional: `--telegram-chat-ids`, `--telegram-allowed-updates`, `--telegram-offset-state-path`, etc. (`akc ingest --help`).

```bash
export AKC_TELEGRAM_TOKEN='...'
akc ingest --tenant-id demo --connector telegram --input updates $EMBED
```

### `whatsapp`

**WhatsApp Cloud API** webhook capture payloads: `--input` is a **comma-separated** list of JSON file paths or directories.

```bash
akc ingest --tenant-id demo --connector whatsapp --input ./examples/messaging/whatsapp-captures/payloads.jsonl $EMBED
# Signed envelope demo: add --whatsapp-verify-signatures --whatsapp-app-secret demo-secret --no-incremental
```

### `mcp`

Model Context Protocol resources. Requires `akc[ingest-mcp]`.

- **`--input`:** either a **server name** defined under `servers` in `--mcp-config`, or a path to a **standalone single-server JSON** file (see `akc.ingest.connectors.mcp.config`).
- **`--mcp-config`:** multi-server JSON (default: `.akc/mcp-ingest.json`). A checked-in template lives at [`mcp/mcp-ingest.example.json`](./mcp/mcp-ingest.example.json) — copy it, replace the command, then point `--mcp-config` at your file.

```bash
akc ingest --tenant-id demo --connector mcp --input stdio_fixture \
  --mcp-config examples/mcp/mcp-ingest.stdio-fixture.json $EMBED
# (then see examples/mcp/README.md for configuring real servers)
```

More detail: [`mcp/README.md`](./mcp/README.md).

## Other folders

| Directory | Purpose |
| --- | --- |
| [`docs-sample/`](./docs-sample/) | Tiny Markdown corpus for `docs` connector demos |
| [`codebase-sample/`](./codebase-sample/) | Tiny fixture repo for `codebase` connector demos |
| [`openapi/`](./openapi/) | Petstore-style OpenAPI 3 sample JSON |
| [`messaging/`](./messaging/) | Slack/Discord/Telegram notes + WhatsApp offline captures |
| [`mcp/`](./mcp/) | MCP ingest template + offline stdio fixture server |
| [`wasm/`](./wasm/) | WAT samples for executor / runtime experiments |
| [`llm_backends/`](./llm_backends/) | Example HTTP LLM backend module wiring |
| [`infrastructure/`](./infrastructure/) | Full-stack `delivery_plan` example (API + web + mobile + cache), not backend-only |
| [`golden-delivery-demo/`](./golden-delivery-demo/) | Checked-in `.akc/delivery/<id>/` session sidecars for `akc view` (no deliver run) |
| [`golden-viewer-demo/`](./golden-viewer-demo/) | Checked-in plan + `manifest.json` for `akc view` without compile |
