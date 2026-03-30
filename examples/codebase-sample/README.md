# Tiny codebase sample

Use this when you want a **small, fast** `codebase` connector demo instead of indexing the entire repository.

```bash
akc ingest --tenant-id demo --connector codebase --input ./examples/codebase-sample \
  --embedder hash --index-backend sqlite
```

The `codebase` connector indexes files by extension and skips common build/output folders (see `akc.ingest.connectors.codebase`).
