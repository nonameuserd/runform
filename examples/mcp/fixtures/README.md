# MCP fixture servers

These are small **local MCP servers** intended for offline demos.

- `stdio_static_server.py` exposes a couple of static text resources over **stdio**.

Run it directly:

```bash
python -m examples.mcp.fixtures.stdio_static_server
```

Or ingest via the checked-in config at `../mcp-ingest.stdio-fixture.json`:

```bash
uv sync --extra ingest-mcp
akc ingest --tenant-id demo --connector mcp --input stdio_fixture   --mcp-config examples/mcp/mcp-ingest.stdio-fixture.json   --embedder hash --index-backend sqlite
```
