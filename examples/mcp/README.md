# MCP ingest example

The **`mcp`** connector reads resources exposed by MCP servers. It expects the **`ingest-mcp`** extra (`mcp` + `httpx` in published packages).

```bash
uv sync --extra ingest-mcp
```

**`--mcp-config`** points at a **multi-server** JSON file (default: `.akc/mcp-ingest.json`). See [`mcp-ingest.example.json`](./mcp-ingest.example.json) for the expected `servers` / `default_server` shape.

**`--input`** selects which server to use:

- A **server name** present in that config (e.g. `example_stdio` in the template), or  
- A path to an **inline single-server** JSON file (one server object, no `servers` wrapper).

```bash
cp mcp-ingest.example.json "$PWD/.akc/mcp-ingest.json"
# Edit .akc/mcp-ingest.json: real stdio command or http URL

akc ingest --tenant-id demo --connector mcp --input example_stdio \
  --mcp-config .akc/mcp-ingest.json \
  --embedder hash --index-backend sqlite
```

Optional: `--mcp-uri-prefix`, `--mcp-static-prompt`, `--mcp-timeout-s`. Do not commit secrets; use `${ENV_VAR}` expansion in `env` maps as documented in code, or keep configs gitignored.

## Offline stdio fixture (no network)

This repo includes a runnable MCP stdio server and matching config:

- Server: `fixtures/stdio_static_server.py`
- Config: `mcp-ingest.stdio-fixture.json`

```bash
uv sync --extra ingest-mcp
akc ingest --tenant-id demo --connector mcp --input stdio_fixture \
  --mcp-config examples/mcp/mcp-ingest.stdio-fixture.json \
  --embedder hash --index-backend sqlite
```

