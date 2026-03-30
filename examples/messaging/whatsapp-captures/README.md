# WhatsApp Cloud API capture fixtures (sanitized)

These files are **offline fixtures** for the `whatsapp` ingest connector.

## Files

- `payloads.jsonl`: two minimal webhook bodies (each line is a JSON object)
- `payload.signed.envelope.json`: an envelope with `headers` + `raw_body` + `body` that exercises signature verification

## Run (offline)

Plain JSONL:

```bash
akc ingest --tenant-id demo --connector whatsapp \
  --input ./examples/messaging/whatsapp-captures/payloads.jsonl \
  --embedder hash --index-backend sqlite
```

Signed envelope (bypass incremental skipping so you can run repeatedly):

```bash
akc ingest --tenant-id demo --connector whatsapp \
  --input ./examples/messaging/whatsapp-captures/payload.signed.envelope.json \
  --whatsapp-verify-signatures \
  --whatsapp-app-secret demo-secret \
  --no-incremental \
  --embedder hash --index-backend sqlite
```

Notes:

- This uses the **demo secret** `demo-secret` only because the fixture signature was generated from it.
- Real deployments should never commit app secrets; pass them via env (`AKC_WHATSAPP_APP_SECRET`) or secret managers.
