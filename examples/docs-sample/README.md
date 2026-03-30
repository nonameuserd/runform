# Tiny docs sample

Use with the **`docs`** connector when you want a small, fast corpus instead of indexing the whole repository:

```bash
akc ingest --tenant-id demo --connector docs --input ./examples/docs-sample \
  --embedder hash --index-backend sqlite
```

`constraints.md` carries normative language on purpose so knowledge / policy fixtures can pick it up in tests and demos.
