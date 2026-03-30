# Messaging ingest examples

Connectors: **`slack`**, **`discord`**, **`telegram`**, **`whatsapp`** (Cloud API webhook JSON).

## Slack

- **Input:** channel id (`C…`).
- **Token:** `--slack-token` or `AKC_SLACK_TOKEN`.
- **Optional bounds:** `--slack-oldest`, `--slack-latest`, `--slack-history-limit`, `--slack-max-threads`, `--slack-max-answers`, `--slack-include-bot-answers`.

```bash
export AKC_SLACK_TOKEN='xoxb-...'
akc ingest --tenant-id demo --connector slack --input C01234567 \
  --embedder hash --index-backend sqlite
```

List channels (separate utility): `akc slack list-channels …`

## Discord

- **Input:** channel id (numeric snowflake).
- **Token:** `--discord-token` or `AKC_DISCORD_TOKEN`.
- **Optional:** `--discord-oldest`, `--discord-latest`, `--discord-history-limit`, `--discord-max-threads`, `--discord-max-answers`, `--discord-timeout-s`, `--discord-max-retries`.

```bash
export AKC_DISCORD_TOKEN='...'
akc ingest --tenant-id demo --connector discord --input 123456789012345678 \
  --embedder hash --index-backend sqlite
```

## Telegram

- **Input:** literal **`updates`** (Bot API `getUpdates` long-poll drain; not a chat id).
- **Token:** `--telegram-token` or `AKC_TELEGRAM_TOKEN`.
- **Optional:** `--telegram-chat-ids`, `--telegram-allowed-updates`, `--telegram-offset-state-path`, … — see `akc ingest --help`.

```bash
export AKC_TELEGRAM_TOKEN='...'
akc ingest --tenant-id demo --connector telegram --input updates \
  --embedder hash --index-backend sqlite
```

## WhatsApp (Cloud API)

- **Input:** comma-separated paths to **webhook JSON** files or directories of captures.
- Configure Cloud API credentials per connector options in `akc ingest --help` (WhatsApp-specific flags).

```bash
# Offline capture fixtures live under examples/messaging/whatsapp-captures
akc ingest --tenant-id demo --connector whatsapp \
  --input ./examples/messaging/whatsapp-captures/payloads.jsonl \
  --embedder hash --index-backend sqlite

# Signed envelope demo (repeatable)
akc ingest --tenant-id demo --connector whatsapp \
  --input ./examples/messaging/whatsapp-captures/payload.signed.envelope.json \
  --whatsapp-verify-signatures \
  --whatsapp-app-secret demo-secret \
  --no-incremental \
  --embedder hash --index-backend sqlite
```

Install optional messaging dependencies: `uv sync --extra ingest-messaging` (see root `pyproject.toml`).

