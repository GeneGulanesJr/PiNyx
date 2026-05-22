# PiNyx

Standalone model gateway that routes LLM requests through a local proxy, logs usage, and can optionally integrate with Pi via extension.

## Quick Start

### 1. Build

```bash
cargo build --release
```

### 2. Configure

Copy and edit the example config:

```bash
mkdir -p ~/.pinyx
cp pinyx.json.example ~/.pinyx/pinyx.json
```

Edit `~/.pinyx/pinyx.json` and set your API keys. Use `$ENV_VAR_NAME` to reference environment variables:

```json
{
  "providers": {
    "anthropic": {
      "api": "anthropic-messages",
      "baseUrl": "https://api.anthropic.com",
      "apiKey": "$ANTHROPIC_API_KEY",
      "models": [
        {
          "id": "claude-sonnet-4-20250514",
          "name": "Claude Sonnet 4",
          "reasoning": true
        }
      ]
    }
  }
}
```
Minimal model config only needs `id` (plus provider settings). Fields like `name`, `input`, `reasoning`, `contextWindow`, `maxTokens`, and `cost` are optional metadata for `/v1/models` output and do not mutate the incoming request payload.

### 3. Start PiNyx

```bash
./target/release/pinyx
```

### 4. Open onboarding UI (standalone)

```
http://127.0.0.1:7331/
```

Use the web UI to configure providers (API key, base URL, model list, cost) and choose default thinking/coding models.

### 5. Optional: Install the Pi extension

```bash
# Copy or symlink the extension
cp -r extensions/pi-pinyx ~/.pi/agent/extensions/pi-pinyx
```

Or install as a Pi package (from this repo):

```bash
pi install git:github.com/your-org/pinyx
```

### 6. Optional: Connect from Pi

```
pi
/login
# Select "PiNyx (local)"
/model
# Pick any model from your configured providers
```

## CLI

```bash
pinyx                          # Start gateway (reads ~/.pinyx/pinyx.json)
pinyx --config ./custom.json   # Custom config path
pinyx --port 8080              # Override port
pinyx --host 0.0.0.0          # Override host
pinyx --check                  # Validate config without starting
pinyx --verbose                # Debug logging
```

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/v1/chat/completions` | POST | OpenAI-compatible proxy (streaming SSE) |
| `/anthropic/v1/messages` | POST | Anthropic-compatible proxy (streaming SSE) |
| `/v1/models` | GET | Model registry (OpenAI-compatible format) |
| `/api/config` | GET/PUT | Read or save gateway config JSON |
| `/api/settings` | GET/PUT | Read or save thinking/coding model preferences |
| `/` | GET | Web onboarding/configuration UI |
| `/health` | GET | Gateway health + provider status |

## Model ID Format

PiNyx uses `provider/model-id` format:

```
anthropic/claude-sonnet-4-20250514
openai/gpt-4o
deepseek/deepseek-chat
```

When sending requests, use the full `provider/model-id` as the `model` field.

## How It Works

```
Client (Pi or any OpenAI-compatible caller) → PiNyx standalone gateway
Pi → prompt → model request → http://localhost:7331 → PiNyx
                                                    → routes to real provider
                                                    → logs request
                                                    → streams response back
```

- **API keys** live in `~/.pinyx/pinyx.json`. Pi never sees real provider keys.
- **No auth** between Pi and PiNyx (localhost only, `127.0.0.1` binding).
- **Request logs** written to `~/.pinyx/logs/YYYY-MM-DD.jsonl`.

## Architecture

```
pinyx/
├── src/
│   ├── main.rs          # CLI entry point, Axum server
│   ├── config.rs        # JSON config loading + API key resolution
│   ├── server/mod.rs    # HTTP endpoints + request routing
│   ├── proxy/mod.rs     # Provider adapters (OpenAI, Anthropic, Google)
│   └── logging/mod.rs   # JSONL request logging
├── extensions/
│   └── pi-pinyx/        # Pi extension for /login + model discovery
│       ├── package.json
│       └── index.ts
└── pinyx.json.example   # Example configuration
```

## Roadmap

- [x] Phase 1: Core proxy + Pi extension (MVP)
- [ ] Phase 2: Intent classification + rule engine
- [ ] Phase 3: Budget, fallback, and retry
- [ ] Phase 4: Risk flagging + permission hooks
- [ ] Phase 5: Dashboard + observability

## License

MIT
