# Enterprise Feature Ideas

Ideas that might be worth pursuing. These tend toward the "enterprisey" end of
the spectrum — governance, observability, operational controls — and are
recorded here so they don't get lost.

None of these are committed work. They're notes on what the architecture could
support if the need arises.

## Telemetry sink

Push request/response statistics to an external metrics collector (OTEL
collector, Prometheus pushgateway, Splunk HEC, or any HTTP endpoint) so that
DuckDB HTTP activity can participate in existing dashboards, alerts, and
capacity planning.

The extension already has:
- Per-host and global counters (requests, bytes, elapsed, errors, 429s, pacing)
- An HTTP client capable of POSTing JSON

So the implementation would use the extension's own HTTP machinery to push its
own statistics — which means telemetry requests must be excluded from the
counters they report, or you get infinite regress.

Configuration would fit naturally into `bh_http_config`:

```sql
SET VARIABLE bh_http_config = bh_http_config_set(
    'default',
    json_object('telemetry_sink', 'http://otel-collector:4318/v1/metrics')
);
```

Push triggers (in order of complexity):
1. Explicit `bh_http_flush_stats(sink_url)` — user controls when
2. On extension unload — automatic, best-effort, no threads
3. Every N requests or T seconds — needs a timer or piggyback mechanism

Start with (1) and (2). If someone wants continuous push, they can call
`bh_http_flush_stats()` on an interval from SQL.

## Bandwidth limiting

The extension controls request *count* but not bytes-on-the-wire. Options:

- **In-extension**: cpr exposes `LimitRate{downrate, uprate}` (maps to
  `CURLOPT_MAX_RECV_SPEED_LARGE`). Per-connection, so aggregate =
  `max_concurrent * per_connection_limit`. Approximate but functional.
- **Proxy-based** (preferred): delegate bandwidth control to a throttling
  proxy (Squid `delay_pools`, Charles Proxy). The extension already supports
  `proxy` in config. This delegates both control and responsibility to IT.
- **OS-level**: macOS `dnctl`/`pfctl`, Linux `tc`. Requires root, system-wide.

The proxy approach is architecturally cleaner: the extension owns request-rate
governance, infrastructure owns bandwidth governance.

## Per-query request ceiling

A `max_requests_per_query` config field that hard-caps the total number of HTTP
requests a single query can make, regardless of rate. Guards against
`FROM range(1000000)` scenarios where rate limiting merely slows the avalanche
rather than stopping it.

## Request audit log

Write a structured log (JSON lines or Parquet) of every HTTP request: URL,
method, status code, elapsed, response size, timestamp. Useful for compliance
and post-incident forensics. The sink could be a local file, an S3 path, or
an HTTP endpoint (same telemetry sink pattern).

## Circuit breaker

If a host returns N consecutive errors (5xx or timeout), stop sending requests
to that host for a cooldown period. Prevents the extension from hammering a
service that's already in trouble. Classic Hystrix/resilience4j pattern.

## Retry with backoff

Configurable retry for transient failures (429, 503, network errors) with
exponential backoff and jitter. The rate limiter already handles 429 feedback
by pushing the TAT forward; explicit retry would complement this for cases
where the request should actually be re-sent.

## Mutual TLS (mTLS)

**Implemented.** The `client_cert` and `client_key` config fields are now
supported. See README for usage.

## Expiring bearer tokens

**Implemented.** Two complementary mechanisms:

### 1. OpenBao vault integration (preferred for production)

`vault_path` + `auth_type=bearer` fetches secrets from OpenBao at request
time with a 5-minute in-process cache. For OAuth-bearing APIs like Google
Sheets/Drive, OpenBao's GCP secrets engine can mint short-lived access
tokens on demand — no refresh logic in blobhttp or the hosting application.

```sql
SET VARIABLE bh_http_config = bh_http_config_set(
    'https://sheets.googleapis.com/',
    json_object('auth_type', 'bearer',
                'vault_path', 'gcp/token/sheets-reader',
                'vault_addr', 'http://127.0.0.1:8200',
                'vault_token', current_setting('vault_token'))
);
-- OpenBao mints a fresh Google access token; blobhttp caches it for 5 min
```

### 2. Manual bearer token with expiry (for ad-hoc / notebook use)

`bh_http_config_set_bearer` lets the hosting application push a token with
an explicit expiry. The extension checks `bearer_token_expires_at` before
each request and fails fast with a clear error.

```sql
SET VARIABLE bh_http_config = bh_http_config_set_bearer(
    'https://api.corp.com/', 'eyJ...', expires_at := 1741564800
);
```

### ~~Extension-level automatic token refresh~~ (not planned)

Previously this document proposed `token_endpoint`, `token_auth_type`, and
`token_expiry_field` config fields that would let blobhttp itself call a
token endpoint and cache the result. **This approach is superseded by the
OpenBao integration** for the following reasons:

- **OpenBao already does this.** Its secrets engines (GCP, AWS, Azure,
  LDAP, database, etc.) handle credential minting, rotation, TTL, and
  lease management. Reimplementing token refresh inside blobhttp would
  duplicate what a purpose-built secrets manager already provides.
- **Violates side-effect-free.** Automatic token refresh is hidden state
  that affects whether a request succeeds. The extension's behavior would
  depend on timing, cache state, and reachability of an external token
  endpoint — none of which are visible to the SQL caller.
- **Scope creep.** Supporting OAuth2 client_credentials, Negotiate/SPNEGO
  token exchange, and arbitrary token response formats pulls the extension
  toward being an auth framework rather than an HTTP client.
- **Two mechanisms suffice.** Vault for automated/production pipelines,
  manual bearer for interactive sessions. There is no gap that extension-
  level refresh would fill.

## OpenBao / Vault integration

**Implemented.** See `vault_path`, `vault_addr`, `vault_token`, `vault_field`,
`vault_param_name`, `vault_kv_version` in `bh_http_config`. The extension
fetches secrets via direct HTTP GET to OpenBao/Vault, with a 5-minute
process-global cache (thread-safe, keyed by addr+path+field).

Supports KV v1 and v2 secrets engines. Auth injection modes: `bearer`
(Authorization header) and `query_param`.

**Not yet implemented:**
- Vault PKI backend for mTLS certificates (issue short-lived client certs
  from Vault's PKI secrets engine, eliminating cert file management)
- Non-token auth to Vault itself (Kubernetes auth, LDAP/Kerberos)

## Request tagging / correlation IDs

Inject a configurable header (e.g. `X-Request-ID`, `X-Correlation-ID`) into
every outbound request, with a value that traces back to the DuckDB query.
Helps service owners correlate their logs with the DuckDB workload that
generated the traffic.
