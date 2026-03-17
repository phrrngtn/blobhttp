# Development Environment Setup

This document describes the services that blobhttp integrates with and how
to configure them for local development.

## Services

blobhttp is an HTTP client library. It makes requests on behalf of SQL
queries and needs external services for authentication (OpenBao), traffic
inspection (mitmproxy), and optionally rate limiting diagnostics.

```
DuckDB/SQLite (blobhttp extension)
  │
  │  bh_http_get / bh_http_post / ...
  │
  ├─── mitmproxy (optional, :8443) ──── target API
  │    Inspect, record, replay
  │
  └─── OpenBao (:8200)
       Fetch API keys at request time
       (vault_path in bh_http_config)
```

## OpenBao (Secret Management)

[OpenBao](https://openbao.org) is an open-source fork of HashiCorp Vault
with an identical HTTP API. blobhttp fetches API keys from it at request
time when `vault_path` is set in a `bh_http_config` scope.

### Install

```bash
brew install openbao
```

### Start (dev mode)

```bash
bao server -dev \
    -dev-root-token-id=dev-blobapi-token \
    -dev-listen-address=127.0.0.1:8200
```

Or as a background service:

```bash
brew services start openbao
```

Dev mode uses an in-memory backend — all secrets are lost on restart.
The token `dev-blobapi-token` is a convention used across the blob*
projects; production would use AppRole or Kubernetes auth.

### Verify

```bash
curl -s -H "X-Vault-Token: dev-blobapi-token" \
    'http://127.0.0.1:8200/v1/sys/health' | python3 -m json.tool
```

### Write secrets

Secrets are stored under `secret/blobapi/{service_name}` (KV v2 engine):

```bash
# Anthropic API key (used by Bifrost, not blobhttp directly)
curl -X POST \
    -H "X-Vault-Token: dev-blobapi-token" \
    -H "Content-Type: application/json" \
    'http://127.0.0.1:8200/v1/secret/data/blobapi/anthropic' \
    -d '{"data": {"api_key": "sk-ant-..."}}'

# Geocodio
curl -X POST \
    -H "X-Vault-Token: dev-blobapi-token" \
    -H "Content-Type: application/json" \
    'http://127.0.0.1:8200/v1/secret/data/blobapi/geocodio' \
    -d '{"data": {"api_key": "...", "base_url": "https://api.geocod.io/v1.7"}}'

# Visual Crossing weather
curl -X POST \
    -H "X-Vault-Token: dev-blobapi-token" \
    -H "Content-Type: application/json" \
    'http://127.0.0.1:8200/v1/secret/data/blobapi/visualcrossing' \
    -d '{"data": {"api_key": "..."}}'
```

### List secrets

```bash
curl -s -H "X-Vault-Token: dev-blobapi-token" \
    'http://127.0.0.1:8200/v1/secret/metadata/blobapi?list=true'
```

### Configure blobhttp to use OpenBao

```sql
SET VARIABLE bh_http_config = MAP {
    -- Default scope: vault address + token inherited by all scopes
    'default': '{"vault_addr": "http://127.0.0.1:8200",
                 "vault_token": "dev-blobapi-token"}',

    -- Per-service: vault_path triggers automatic secret fetch
    'https://api.geocod.io/': '{"vault_path": "secret/blobapi/geocodio",
                                "auth_type": "bearer"}',

    'https://weather.visualcrossing.com/': '{
        "vault_path": "secret/blobapi/visualcrossing",
        "auth_type": "query_param",
        "vault_param_name": "key"
    }'
};

-- Now API calls just work — no keys in the SQL
SELECT bh_http_get('https://api.geocod.io/v1.7/geocode',
    params := '{"q": "1600 Pennsylvania Ave NW, Washington DC"}');
```

### How it works internally

When blobhttp resolves a config scope that has `vault_path`:

1. Makes a bare HTTP GET to `{vault_addr}/v1/secret/data/{vault_path}`
   with the `X-Vault-Token` header (not routed through blobhttp's own
   config/rate-limit/proxy stack — avoids recursion).
2. Extracts `data.data.{vault_field}` from the KV v2 response
   (default field: `api_key`).
3. Injects the secret per `auth_type`:
   - `bearer` → `Authorization: Bearer {secret}` header
   - `query_param` → `?{vault_param_name}={secret}` query parameter
4. Caches the secret in-process for 5 minutes.

## mitmproxy (HTTP Inspection)

[mitmproxy](https://mitmproxy.org) is an interactive HTTPS proxy for
inspecting, recording, and replaying HTTP traffic. It's invaluable for
debugging blobhttp requests — you can see exactly what's being sent to
external APIs, inspect response headers, and replay requests.

### Install

```bash
brew install mitmproxy
```

### Start

```bash
# Interactive TUI (terminal UI)
mitmproxy --listen-port 8443

# Web interface (browser-based)
mitmweb --listen-port 8443 --web-port 8081

# Dump mode (log to stdout, no UI)
mitmdump --listen-port 8443
```

### Configure blobhttp to route through mitmproxy

Route specific scopes through the proxy:

```sql
SET VARIABLE bh_http_config = MAP {
    'default': '{"vault_addr": "http://127.0.0.1:8200",
                 "vault_token": "dev-blobapi-token"}',

    -- Route weather API calls through mitmproxy
    'https://weather.visualcrossing.com/': '{
        "vault_path": "secret/blobapi/visualcrossing",
        "auth_type": "query_param",
        "vault_param_name": "key",
        "proxy": "http://localhost:8443"
    }'
};
```

Route all traffic through mitmproxy (useful for debugging):

```sql
SET VARIABLE bh_http_config = MAP {
    'default': '{"proxy": "http://localhost:8443"}'
};
```

### TLS considerations

mitmproxy generates its own CA certificate for HTTPS interception. By
default, blobhttp will reject the proxy's certificate. Two options:

1. **Disable verification** (dev only):
   ```sql
   'https://api.example.com/': '{"proxy": "http://localhost:8443",
                                  "verify_ssl": false}'
   ```

2. **Trust the mitmproxy CA** (better):
   ```sql
   'https://api.example.com/': '{"proxy": "http://localhost:8443",
                                  "ca_bundle": "~/.mitmproxy/mitmproxy-ca-cert.pem"}'
   ```

   mitmproxy generates its CA at `~/.mitmproxy/` on first run.

### What you can see in mitmproxy

- Full request/response bodies (JSON payloads, API responses)
- Headers including `Authorization: Bearer ...` (after Vault injection)
- Query parameters (including API keys injected via `query_param` auth)
- Timing (latency, TLS handshake)
- Rate limit response headers (`X-RateLimit-*`, `Retry-After`)

### Recording and replay

```bash
# Record all traffic to a file
mitmdump --listen-port 8443 -w traffic.flow

# Replay recorded traffic (for testing without hitting real APIs)
mitmdump --listen-port 8443 --server-replay traffic.flow
```

This is useful for developing and testing adapter JMESPath expressions
without burning API quota.

## Typical development session

```bash
# Terminal 1: OpenBao
bao server -dev -dev-root-token-id=dev-blobapi-token \
    -dev-listen-address=127.0.0.1:8200

# Terminal 2: mitmproxy (optional)
mitmweb --listen-port 8443 --web-port 8081

# Terminal 3: DuckDB
duckdb -unsigned
```

Then in DuckDB:

```sql
LOAD 'build/release/extension/bhttp/bhttp.duckdb_extension';

-- Configure vault + proxy
SET VARIABLE bh_http_config = MAP {
    'default': '{"vault_addr": "http://127.0.0.1:8200",
                 "vault_token": "dev-blobapi-token",
                 "proxy": "http://localhost:8443"}',
    'https://api.geocod.io/': '{"vault_path": "secret/blobapi/geocodio",
                                "auth_type": "bearer"}'
};

-- This request routes through mitmproxy with the API key from vault
SELECT bh_http_get('https://api.geocod.io/v1.7/geocode',
    params := '{"q": "02458"}').response_body;
```

Open `http://localhost:8081` to see the request in mitmproxy's web UI.
