# blobhttp

> **Note on authorship:** The code and documentation in this repository were
> generated entirely by Claude Opus 4.6 (Anthropic), under close human
> supervision. This is a research project — a vehicle for experimentation with
> designs, implementation techniques, and the boundaries of AI-assisted software
> development, not a production-ready artifact. Architectural decisions, API
> shape, and overall direction were guided by the human; implementation was
> performed by the model.

blobhttp is a member of the [BLOB extension family](https://github.com/phrrngtn/rule4/blob/main/BLOB_EXTENSIONS.md).
It provides HTTP client functions as composable SQL primitives for both
**DuckDB** and **SQLite**, with enterprise features: SPNEGO/Kerberos
authentication, mutual TLS, Vault/OpenBao secret injection, scoped
configuration, GCRA rate limiting, and parallel execution via libcurl's multi
interface.

The DuckDB extension is built on the [DuckDB C Extension API](https://github.com/duckdb/extension-template-c)
for binary compatibility across DuckDB versions. The SQLite extension follows
the standard loadable extension pattern.

Inspired by Alex Garcia's excellent [sqlite-http](https://github.com/asg017/sqlite-http)
(`http0`) extension for SQLite, which demonstrated how natural and powerful
HTTP-in-SQL can be when done as explicit table-valued and scalar functions
rather than as a transparent filesystem layer.

### Relation to the community `http_client` extension

The DuckDB community extensions repository includes
[http_client](https://github.com/Query-farm/httpclient) by Query-farm, which
provides basic `http_get`/`http_post`/`http_head` functions returning JSON.
blobhttp is a separate, ground-up implementation targeting different use
cases. Key differences:

| | [http_client](https://github.com/Query-farm/httpclient) | blobhttp |
|---|---|---|
| HTTP methods | GET, POST, HEAD | GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS + generic `bh_http_request()` |
| Return type | JSON (access via `->>`) | Native STRUCT (access via `.field`) |
| Volatility | Not specified | Correct per-verb: GET/HEAD/OPTIONS idempotent; POST/PATCH volatile |
| Rate limiting | None | GCRA per-host + global, 429 backoff, `bh_http_rate_limit_stats()` diagnostics |
| Parallel execution | None | libcurl multi interface, configurable `max_concurrent` |
| Authentication | Manual headers | SPNEGO/Kerberos, Bearer with expiry checking, mutual TLS |
| Configuration | Per-call only | Scoped config via `bh_http_config` variable with URL prefix + domain-suffix matching |
| SSL/TLS | Basic | Client certs (mTLS), custom CA bundles, `verify_ssl` toggle |
| Proxy support | None | Configurable per-scope |
| Extension API | C++ internal API | C Extension API (binary-compatible across DuckDB versions) |

**Important:** blobhttp uses `bh_` prefixed function names (`bh_http_get`,
`bh_http_post`, etc.) to avoid conflicts with http_client. The two extensions
have different signatures and return types; SQL written for one will not work
with the other without modification.

## Loading

### DuckDB

```sql
LOAD 'path/to/bhttp.duckdb_extension';
```

Or, if loading an unsigned extension:

```bash
duckdb -unsigned -cmd "LOAD 'build/release/bhttp.duckdb_extension';"
```

### SQLite

```sql
.load path/to/bhttp
```

Both DuckDB and SQLite functions use the `bh_` prefix (`bh_http_get`, etc.).
In DuckDB, these are SQL macros that wrap the underlying `_bh_http_raw_request`
C function. In SQLite, they are direct scalar/table-valued functions.

| DuckDB | SQLite | Notes |
|---|---|---|
| `bh_http_get(url, ...)` | `bh_http_get(url, ...)` | Named params (DuckDB) vs positional (SQLite) |
| `bh_http_post(url, ...)` | `bh_http_post(url, ...)` | |
| `bh_http_request(method, url, ...)` | `bh_http_request(method, url, ...)` | Generic, all verbs |
| Returns STRUCT | Returns JSON string | SQLite has no STRUCT type |

## HTTP Functions

### Per-verb scalar functions

Each returns a STRUCT with request and response details. Use a subquery or CTE
to access individual fields via dot notation.

`bh_http_get`, `bh_http_head`, `bh_http_options`, `bh_http_put`, and `bh_http_delete` are
idempotent — DuckDB may safely deduplicate identical calls within a query.
`bh_http_post` and `bh_http_patch` are volatile — every call fires regardless.

```sql
-- Simple GET with struct field access
SELECT r.response_status_code, r.response_body
FROM (SELECT bh_http_get('https://httpbin.org/get') AS r);
```

```sql
-- GET with custom headers
SELECT r.response_body
FROM (SELECT bh_http_get('https://httpbin.org/get',
    headers := MAP {'X-Api-Key': 'secret123'}) AS r);
```

```sql
-- POST with JSON body (volatile — always fires)
SELECT r.response_status_code
FROM (SELECT bh_http_post('https://httpbin.org/post',
    body := '{"name": "duckdb"}',
    content_type := 'application/json') AS r);
```

```sql
-- PUT with explicit content type
SELECT r.response_status_code
FROM (SELECT bh_http_put('https://httpbin.org/put',
    body := '<item><name>test</name></item>',
    content_type := 'application/xml') AS r);
```

```sql
-- Data-driven batch: fetch from a list of URLs
SELECT url, r.response_status_code AS status, round(r.elapsed, 3) AS seconds
FROM (
    SELECT url, bh_http_get(url) AS r
    FROM (VALUES ('https://httpbin.org/get'), ('https://httpbin.org/ip')) AS t(url)
)
ORDER BY url;
```

```sql
-- Batch API calls driven by table data
SELECT e.endpoint_url, r.response_status_code AS status
FROM (
    SELECT e.endpoint_url, bh_http_get(e.endpoint_url) AS r
    FROM endpoints AS e
    LEFT OUTER JOIN health_checks AS h ON h.url = e.endpoint_url
    WHERE h.url IS NULL
);
```

### Generic scalar function: `bh_http_request(method, url, ...)`

For dynamic methods or when the verb isn't known at query-writing time. Always
volatile (every call fires).

```sql
SELECT r.response_status_code
FROM (SELECT bh_http_request('GET', 'https://httpbin.org/get') AS r);
```

### JSON variant: `bh_http_request_json(method, url, ...)`

Returns the same result as `bh_http_request` but serialized as a JSON string via
DuckDB's `to_json()`.

```sql
SELECT bh_http_request_json('GET', 'https://httpbin.org/ip');
```

### STRUCT fields

All scalar functions return a STRUCT with the same fields:

| Field | Type | Description |
|-------|------|-------------|
| `request_url` | VARCHAR | The URL as sent |
| `request_method` | VARCHAR | HTTP method used |
| `request_headers` | MAP(VARCHAR, VARCHAR) | Headers sent |
| `request_body` | VARCHAR | Request body, if any |
| `response_status_code` | INTEGER | HTTP status code (200, 404, etc.) |
| `response_status` | VARCHAR | Status line (e.g. `HTTP/1.1 200 OK`) |
| `response_headers` | MAP(VARCHAR, VARCHAR) | Response headers (keys are lowercase, as normalized by libcurl) |
| `response_body` | VARCHAR | Response body |
| `response_url` | VARCHAR | Final URL after redirects |
| `elapsed` | DOUBLE | Request duration in seconds |
| `redirect_count` | INTEGER | Number of redirects followed |

### Function parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | VARCHAR | (required) | Request URL |
| `headers` | MAP(VARCHAR, VARCHAR) | NULL | Request headers — MAP literals are cast to JSON internally. Also accepts JSON strings for dynamic composition via `json_object()`. |
| `params` | VARCHAR (JSON) | NULL | Query parameters as a JSON object (`{"key": "value"}` → `?key=value`). Compose with `json_object()`, `json_merge_patch()`, or vault-derived values. |
| `body` | VARCHAR | NULL | Request body (POST, PUT, PATCH only) |
| `content_type` | VARCHAR | NULL | Content-Type (defaults to `application/json` if body is set) |

The generic `bh_http_request` also takes `method` (VARCHAR) as the first
parameter.

```sql
-- Query params as a JSON object (no string concatenation)
SELECT r.response_body
FROM (SELECT bh_http_get('https://api.example.com/search',
    params := '{"q": "duckdb", "limit": "10"}') AS r);

-- Dynamic params via json_object()
SELECT r.response_body
FROM (SELECT bh_http_get('https://api.example.com/search',
    params := json_object('q', search_term, 'limit', '10')) AS r)
FROM search_terms;

-- Compose params from multiple sources with json_merge_patch
SELECT r.response_body
FROM (SELECT bh_http_get(base_url,
    params := json_merge_patch(
        '{"format": "json", "units": "metric"}',  -- base params
        json_object('lat', lat, 'lng', lng)         -- per-row params
    )) AS r)
FROM locations;
```

### Recommended pattern: subquery or CTE

Always assign the scalar function result to an alias in a subquery or CTE,
then access fields from the alias. This ensures the HTTP request fires exactly
once per row, regardless of how many fields you reference.

```sql
-- Good: one request, access multiple fields
WITH api_calls AS (
    SELECT id, bh_http_get('https://api.example.com/item/' || id) AS r
    FROM items
)
SELECT id, r.response_status_code, r.response_body, r.elapsed
FROM api_calls;

-- Bad: fires two requests per row (DuckDB evaluates each expression separately)
SELECT
    bh_http_get(url).response_status_code,
    bh_http_get(url).elapsed
FROM urls;
```

This is consistent with how SQL handles any expensive expression — factor it
into a subquery and reference the result by name.

## Configuration

Configuration is managed via a DuckDB variable (`bh_http_config`) containing a
`MAP(VARCHAR, VARCHAR)`. Keys are URL prefixes (scopes); values are JSON
objects with configuration fields. The longest matching prefix wins, with
`'default'` as the fallback.

```sql
SET VARIABLE bh_http_config = MAP {
    'default': '{"timeout": 30, "rate_limit": "20/s"}',
    'https://api.example.com/': '{"auth_type": "bearer", "bearer_token": "sk-abc123", "rate_limit": "5/s"}',
    'https://internal.corp.com/': '{"auth_type": "negotiate", "verify_ssl": false}'
};
```

### Configuration fields

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `timeout` | integer | 30 | Request timeout in seconds |
| `rate_limit` | string | `"20/s"` | Rate limit (`"10/s"`, `"100/m"`, `"3600/h"`, `"none"` to disable) |
| `burst` | number | 5.0 | Burst capacity for rate limiter |
| `verify_ssl` | boolean | true | Verify SSL certificates |
| `proxy` | string | | HTTP/HTTPS proxy URL |
| `ca_bundle` | string | | Path to CA certificate bundle |
| `auth_type` | string | | `"negotiate"` or `"bearer"` |
| `bearer_token` | string | | Token for Bearer authentication |
| `bearer_token_expires_at` | integer | 0 | Unix epoch seconds; request fails with a clear error if the token has expired. Set to 0 to disable expiry checking. |
| `client_cert` | string | | Path to client certificate file (PEM) for mutual TLS |
| `client_key` | string | | Path to client private key file (PEM) for mutual TLS |
| `max_concurrent` | integer | 10 | Max parallel requests per scalar function chunk |
| `global_rate_limit` | string | | Aggregate rate limit across all hosts (e.g. `"50/s"`) |
| `global_burst` | number | 10.0 | Burst capacity for the global rate limiter |

### How configuration flows

The user-facing functions (`bh_http_get`, `bh_http_post`, etc.) are SQL macros that
read `bh_http_config` from the caller's connection via `getvariable()`, then pass
it to the underlying C functions. This means configuration set via
`SET VARIABLE` is correctly visible during function execution.

### Scope resolution example

```sql
SET VARIABLE bh_http_config = MAP {
    'default':                    '{"timeout": 30}',
    'https://api.example.com/':   '{"bearer_token": "abc", "rate_limit": "5/s"}',
    'https://api.example.com/v2/':'{"bearer_token": "xyz"}'
};

-- Uses default config (timeout=30, no auth)
SELECT r.response_status_code
FROM (SELECT bh_http_get('https://other-site.com/data') AS r);

-- Matches 'https://api.example.com/' scope (bearer_token=abc, rate_limit=5/s)
SELECT r.response_status_code
FROM (SELECT bh_http_get('https://api.example.com/v1/users') AS r);

-- Matches 'https://api.example.com/v2/' scope (bearer_token=xyz)
-- Also inherits timeout=30 from default
SELECT r.response_status_code
FROM (SELECT bh_http_get('https://api.example.com/v2/users') AS r);
```

### Configuration helpers

The extension provides helper macros for safely updating individual scopes
without clobbering the rest of `bh_http_config`.

| Macro | Description |
|-------|-------------|
| `bh_http_config_set(scope, config_json)` | Merge a scope's JSON config into the existing config, preserving all other scopes. Returns the new MAP. |
| `bh_http_config_set_bearer(scope, token, expires_at := 0)` | Convenience for setting a bearer token. Uses `json_object()` internally for safe JSON construction. |
| `bh_http_config_get(scope)` | Read a single scope's JSON config string (or NULL). |
| `bh_http_config_remove(scope)` | Remove a scope. Returns the new MAP. |

```sql
-- Set a scope's config (merges, doesn't clobber)
SET VARIABLE bh_http_config = bh_http_config_set(
    'https://secure-api.corp.com/',
    json_object('client_cert', '/path/to/client.pem',
                'client_key', '/path/to/client-key.pem',
                'ca_bundle', '/path/to/ca-chain.pem')
);

-- Set a bearer token with expiry (shorthand)
SET VARIABLE bh_http_config = bh_http_config_set_bearer(
    'https://api.vendor.com/', 'eyJ...', expires_at := 1741564800
);

-- Refresh a token later — other scopes are preserved
SET VARIABLE bh_http_config = bh_http_config_set_bearer(
    'https://api.vendor.com/', 'eyJnew...', expires_at := 1741571400
);

-- Inspect what's configured for a scope
SELECT bh_http_config_get('https://api.vendor.com/');

-- Remove a scope entirely
SET VARIABLE bh_http_config = bh_http_config_remove('https://api.vendor.com/');
```

### Vault / OpenBao integration

API keys can be fetched automatically from [HashiCorp Vault](https://www.vaultproject.io/)
or [OpenBao](https://openbao.org) (open-source fork, same API). When a scope
has a `vault_path`, blobhttp fetches the secret before making the request and
injects it per `auth_type` — no vault CTEs or manual key handling in the query.

```sql
SET VARIABLE bh_http_config = MAP {
    'default': '{"vault_addr": "http://127.0.0.1:8200", "vault_token": "dev-token"}',
    'https://api.geocod.io/': '{"vault_path": "secret/blobapi/geocodio", "auth_type": "bearer"}',
    'https://weather.visualcrossing.com/': '{"vault_path": "secret/blobapi/visualcrossing", "auth_type": "query_param", "vault_param_name": "key"}'
};

-- No API keys anywhere in the query — vault handles it
SELECT json_extract_string(r.response_body, '$.results[0].formatted_address')
FROM (SELECT bh_http_get('https://api.geocod.io/v1.11/geocode',
    params := '{"q": "02458"}') AS r);
```

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `vault_path` | string | | KV secret path (e.g. `secret/blobapi/geocodio`) |
| `vault_addr` | string | `http://127.0.0.1:8200` | Vault/OpenBao address |
| `vault_token` | string | | Vault auth token |
| `vault_field` | string | `api_key` | Field to extract from the secret |
| `vault_param_name` | string | | For `auth_type=query_param`: the query param name |
| `vault_kv_version` | integer | 2 | KV secrets engine version (1 or 2) |

The `vault_addr` and `vault_token` are typically set in the `default` scope
and inherited by all service-specific scopes. The `vault_path` is per-scope.

Secrets are cached in-process for 5 minutes to avoid repeated vault calls.
The vault fetch itself is a bare HTTP GET with the token header — it does not
go through blobhttp's config resolution, rate limiting, or proxy settings.
Works with both Vault and OpenBao (identical HTTP API).

### Mutual TLS (mTLS)

To authenticate with a client certificate:

```sql
SET VARIABLE bh_http_config = bh_http_config_set(
    'https://secure-api.corp.com/',
    json_object('client_cert', '/path/to/client.pem',
                'client_key', '/path/to/client-key.pem')
);

SELECT r.response_status_code
FROM (SELECT bh_http_get('https://secure-api.corp.com/endpoint') AS r);
```

Combine with `ca_bundle` if the server uses a private CA:

```sql
SET VARIABLE bh_http_config = bh_http_config_set(
    'https://secure-api.corp.com/',
    json_object('client_cert', '/path/to/client.pem',
                'client_key', '/path/to/client-key.pem',
                'ca_bundle', '/path/to/ca-chain.pem')
);
```

### Bearer token with expiry

When tokens have a known expiry time, set `bearer_token_expires_at` so the
extension fails fast with a clear error rather than making a request that will
be rejected:

```sql
SET VARIABLE bh_http_config = bh_http_config_set_bearer(
    'https://api.vendor.com/', 'eyJ...', expires_at := 1741564800
);
```

If the token has expired, the extension raises an error with ISO 8601
timestamps:

```
Bearer token for api.vendor.com expired at 2025-03-10T00:00:00Z (1741564800)
(current time: 2025-03-10T01:30:00Z (1741570200)).
Refresh the token via your application and update bh_http_config.
```

Refresh by calling `bh_http_config_set_bearer` again — existing config for other
scopes is preserved:

```sql
SET VARIABLE bh_http_config = bh_http_config_set_bearer(
    'https://api.vendor.com/', 'eyJnew...', expires_at := 1741571400
);
```

### Rate limiting

Rate limiting uses the GCRA (Generic Cell Rate Algorithm) and is applied
per-host automatically. The default is 20 requests/second with a burst of 5.
Override per-scope via configuration:

```sql
SET VARIABLE bh_http_config = MAP {
    'default': '{"rate_limit": "20/s"}',
    'https://rate-limited-api.com/': '{"rate_limit": "2/s"}'
};
```

### Parallel execution

The scalar functions execute requests in parallel using
libcurl's multi interface (via cpr's `MultiPerform`). When DuckDB passes a
chunk of rows to the scalar function, the extension fires up to
`max_concurrent` requests simultaneously, then moves to the next batch.

```sql
-- Default: up to 10 concurrent requests per chunk
SELECT json_extract(
    bh_http_request('GET', 'http://api.example.com/item/' || id::VARCHAR, NULL, NULL, NULL),
    '$.response_status_code')::INTEGER AS status
FROM range(100) AS t(id);
```

```sql
-- Throttle to 3 concurrent requests
SET VARIABLE bh_http_config = MAP {
    'default': '{"max_concurrent": 3}'
};

SELECT json_extract(
    bh_http_request('GET', 'http://api.example.com/item/' || id::VARCHAR, NULL, NULL, NULL),
    '$.response_status_code')::INTEGER AS status
FROM range(100) AS t(id);
```

DuckDB's vectorized engine passes rows to scalar functions in chunks (up to
2048 rows). Within each chunk, the extension:

1. **Parses** all rows — resolves config, builds sessions, acquires rate limit tokens
2. **Executes** in sub-batches of `max_concurrent` via `MultiPerform` (libcurl event loop — no threads)
3. **Writes** all results back to the output vector

Rate limiting is enforced *before* each batch: the extension acquires one rate
limit token per request, sleeping if necessary, then fires the batch. If a
server responds with 429, the rate limiter's TAT (Theoretical Arrival Time) is
pushed forward by the `Retry-After` value, automatically slowing subsequent
batches.

Parallelism is automatic for data-driven workloads where the scalar function
is applied across multiple rows.

### Rate limiter diagnostics

The `bh_http_rate_limit_stats()` table function returns a snapshot of per-host
rate limiter state. Call it after running requests to see how the rate limiter
behaved.

```sql
SELECT * FROM bh_http_rate_limit_stats();
```

| Column | Type | Description |
|--------|------|-------------|
| `host` | VARCHAR | Hostname key |
| `rate_limit` | VARCHAR | Configured rate spec (e.g. `20/s`) |
| `rate_rps` | DOUBLE | Requests per second (parsed) |
| `burst` | DOUBLE | Burst capacity |
| `requests` | BIGINT | Total requests recorded |
| `paced` | BIGINT | Times the caller had to sleep before sending |
| `total_wait_seconds` | DOUBLE | Cumulative time spent waiting for rate limit tokens |
| `throttled_429` | BIGINT | Times a 429 response pushed back the rate limiter |
| `backlog_seconds` | DOUBLE | How far ahead the TAT is from now (positive = backlogged) |
| `total_responses` | BIGINT | Total HTTP responses received |
| `total_response_bytes` | BIGINT | Total response body bytes received |
| `total_elapsed` | DOUBLE | Sum of all request durations (seconds) |
| `min_elapsed` | DOUBLE | Fastest request (seconds) |
| `max_elapsed` | DOUBLE | Slowest request (seconds) |
| `errors` | BIGINT | Responses with non-2xx status codes |

When a `global_rate_limit` is configured, a `(global)` row appears with
aggregate counts across all hosts.

Example workflow:

```sql
-- Fire some requests
SELECT count(*) FROM (
    SELECT bh_http_request('GET', 'http://localhost:8444/fast', NULL, NULL, NULL) AS r
    FROM range(50) AS t(id)
);

-- Inspect rate limiter and request stats
SELECT host, requests, total_responses, total_response_bytes,
       round(total_elapsed, 3) AS total_s,
       round(min_elapsed, 4) AS min_s,
       round(max_elapsed, 4) AS max_s,
       errors, paced, throttled_429
FROM bh_http_rate_limit_stats();
```

Example with a global rate limiter:

```sql
SET VARIABLE bh_http_config = MAP {
    'default': '{"global_rate_limit": "10/s", "rate_limit": "100/s", "max_concurrent": 5}'
};

SELECT json_extract(
    bh_http_request('GET', 'http://api.example.com/item/' || id::VARCHAR, NULL, NULL, NULL),
    '$.response_status_code')::INTEGER AS status
FROM range(15) AS t(id);

SELECT * FROM bh_http_rate_limit_stats();
-- (global)   | 15 requests | 1955 bytes | paced=1 | pacing_s=0.986
-- localhost  | 15 requests | 1955 bytes | paced=0
```

## Reified Functions

blobhttp provides infrastructure for **reified functions** — domain-specific
SQL functions whose definitions are stored as data in the `llm_adapter` table
rather than as code. Each reified function has a typed interface (a SQL macro),
but the prompt template, output schema, and response reshaping are all
configuration rows, not compiled logic.

The term "reified" reflects what's happening: an abstract capability (latent
knowledge in an LLM) is made concrete as a callable, schema-bound function
with a declared interface.

All reified functions target the OpenAI `/v1/chat/completions` protocol. A
local gateway like [Bifrost](https://github.com/maximhq/bifrost) translates
this to 20+ LLM providers (Anthropic, Google, Mistral, etc.), so the SQL
never contains vendor-specific logic.

### Architecture

```
Domain macro                         llm_adapter table
physical_properties(                 ┌──────────────────────────┐
  ['water','ethanol'],               │ prompt_template (inja)   │
  ['boiling point']                  │ output_schema (JSON)     │
)                                    │ response_jmespath        │
  │                                  └────────────┬─────────────┘
  │ maps typed args to JSON                       │
  ▼                                               │
llm_adapt('physical_properties', params)          │
  │                                               │
  │ looks up adapter row ◄────────────────────────┘
  │ renders prompt via bt_template_render() (inja)
  │ merges session defaults (endpoint, model, bh_http_config)
  │ merges caller overrides
  ▼
_llm_adapt_raw(config_json)
  │
  │ C++ scalar function:
  │   1. POST to gateway (via cpr, rate-limited)
  │   2. Continuation loop (finish_reason == "length")
  │   3. Schema validation (jsoncons Draft 2020-12)
  │   4. Retry with error feedback on validation failure
  │   5. Response reshaping (JMESPath via jsoncons)
  ▼
JSON result
```

Three layers, each with a single responsibility:

| Layer | What | Where logic lives |
|---|---|---|
| Domain macro | Typed interface for callers | One-line SQL macro |
| `llm_adapt` | Adapter lookup, template rendering, config merge | SQL table macro + `bt_template_render()` from blobtemplates |
| `_llm_adapt_raw` | HTTP, continuation, validation, reshaping | C++ scalar function |

Two template/reshaping languages, each used where it fits:

| | Input (prompt) | Output (result) |
|---|---|---|
| **Nature** | Unstructured text from structured data | Structured data from structured data |
| **Tool** | [Inja](https://github.com/pantor/inja) (Jinja2-style) | JMESPath |
| **Why** | Natural language composition — loops, conditionals, formatting | Projection, filtering, flattening of a JSON tree with a known shape |

### The `llm_adapter` table

```sql
CREATE TABLE llm_adapter (
    name              VARCHAR PRIMARY KEY,
    prompt_template   VARCHAR NOT NULL,   -- Inja/Jinja2 template
    output_schema     VARCHAR,            -- JSON Schema for validation
    response_jmespath VARCHAR,            -- JMESPath to reshape the result
    max_tokens        INTEGER DEFAULT 4096
);
```

Each row is a function definition. Adding a new reified function = one INSERT
+ one `CREATE MACRO`.

### Example: `physical_properties`

Adapter row:

```sql
INSERT INTO llm_adapter VALUES (
    'physical_properties',
    'For each of these substances: {{ join(substances, ", ") }}. '
    'Return the following measurements in SI units: '
    '{{ join(metrics, ", ") }}. '
    'Return one row per substance-metric combination.',
    '{ ... output_schema ... }',
    'measurements',
    4096
);
```

Domain macro:

```sql
CREATE OR REPLACE MACRO physical_properties(substances, metrics) AS TABLE (
    SELECT result FROM llm_adapt('physical_properties',
        json_object('substances', substances, 'metrics', metrics))
);
```

Call:

```sql
SELECT * FROM physical_properties(
    ['water', 'ethanol', 'mercury'],
    ['boiling point', 'melting point', 'density at 25°C']
);
```

Returns a JSON list-of-dicts with `substance`, `metric`, `value`, and
`unit_of_measure` fields, schema-validated.

### Example: `domain_inference`

A more complex reified function that takes a header (column names) and body
(sample rows) — the same header+body shape used by blobodbc — and returns
semantic domain classifications and inferred functional dependencies.

Adapter row (abbreviated):

```sql
INSERT INTO llm_adapter VALUES (
    'domain_inference',
    'Given a table with these columns and sample data, infer the semantic
     domain of each column and any likely functional dependencies...
     Columns: {{ join(header, ", ") }}
     Sample data:
     {% for row in body %}  {{ row }}
     {% endfor %}',
    '{ "type": "object", "required": ["columns", "functional_dependencies"],
       "properties": {
         "columns": { "type": "array", "items": { ... "domain", "confidence", "reasoning" ... }},
         "functional_dependencies": { "type": "array", "items": { ... "determinant", "dependent" ... }}
       }}',
    '',
    4096
);
```

Domain macro:

```sql
CREATE OR REPLACE MACRO domain_inference(header, body) AS TABLE (
    SELECT result FROM llm_adapt('domain_inference',
        json_object('header', header, 'body', body))
);
```

Call:

```sql
SELECT * FROM domain_inference(
    ['code', 'name', 'country', 'ccy', 'exchange', 'mkt_cap'],
    [['AAPL', 'Apple Inc.', 'US', 'USD', 'NASDAQ', '2890000000000'],
     ['NESN', 'Nestle S.A.', 'CH', 'CHF', 'SIX', '270000000000'],
     ['7203', 'Toyota Motor Corp.', 'JP', 'JPY', 'TSE', '35000000000000']]
);
```

Returns a single JSON object with two arrays:

- `columns`: `[{column_name, domain, confidence, reasoning}, ...]`
  — e.g. `{column_name: "ccy", domain: "currency_code", confidence: 0.98}`
- `functional_dependencies`: `[{determinant, dependent, confidence, reasoning}, ...]`
  — e.g. `{determinant: "country", dependent: "ccy", confidence: 0.75,
    reasoning: "Country code typically determines currency, but eurozone..."}`

The LLM uses world knowledge (ISO 4217, stock exchanges, dual listings) that
no statistical FD test can replicate. These inferred priors can weight or
prune the mechanical FD search done by blobfilters.

### Shredding the result

Reified functions return JSON. Use `UNNEST`/`from_json` to shred into rows.
When referencing the result multiple times, use `AS MATERIALIZED` to avoid
re-executing the LLM call:

```sql
WITH RAW AS MATERIALIZED (
    SELECT result::JSON AS j FROM domain_inference(
        ['code', 'name', 'country', 'ccy'],
        [['AAPL', 'Apple Inc.', 'US', 'USD'], ...])
)
SELECT c->>'$.column_name' AS col, c->>'$.domain' AS domain,
       CAST(c->>'$.confidence' AS DOUBLE) AS conf
FROM RAW, LATERAL UNNEST(from_json(j->'$.columns', '["json"]')) AS t(c)
UNION ALL
SELECT f->>'$.determinant', f->>'$.dependent',
       CAST(f->>'$.confidence' AS DOUBLE)
FROM RAW, LATERAL UNNEST(from_json(j->'$.functional_dependencies', '["json"]')) AS t(f);
```

Without `MATERIALIZED`, DuckDB may inline the CTE and evaluate it twice —
resulting in two LLM calls. The `MATERIALIZED` keyword forces single
evaluation. This matters because `_llm_adapt_raw` is volatile.

### Override anything per-call

The `params` JSON passed to `llm_adapt` is merged on top of the adapter row,
so any well-known key can be overridden:

```sql
-- Use a different model
SELECT * FROM llm_adapt('physical_properties',
    json_object('substances', ['water'], 'metrics', ['boiling point'],
                'model', 'anthropic/claude-sonnet-4-20250514'));

-- Override max_tokens
SELECT * FROM llm_adapt('physical_properties',
    json_object('substances', ['water'], 'metrics', ['boiling point'],
                'max_tokens', 8192));
```

### Low-level: `llm_complete()`

Direct chat completion call without the adapter system. For one-off queries,
testing, or custom pipelines:

```sql
SELECT llm_complete(
    'http://localhost:8080/v1/chat/completions',
    body := json_object(
        'model', 'anthropic/claude-haiku-4-5-20251001',
        'max_tokens', 256,
        'messages', json_array(
            json_object('role', 'user', 'content', 'What is the capital of France?')
        )
    ),
    output_schema := '{ ... }'   -- optional
) AS response;
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `url` | VARCHAR | (required) | Chat completions endpoint URL |
| `body` | VARCHAR | (required) | Request body JSON |
| `headers` | MAP(VARCHAR, VARCHAR) | NULL | Extra headers |
| `output_schema` | VARCHAR | NULL | JSON Schema for output validation |
| `max_continuations` | INTEGER | 10 | Max continuations on `finish_reason == "length"` |
| `max_retries` | INTEGER | 3 | Max retries on schema validation failure |

### Schema validation

1. The `output_schema` is wrapped as a tool definition with `tool_choice: forced`
2. The response is extracted from `choices[0].message.tool_calls[0].function.arguments`
3. jsoncons validates against the schema (JSON Schema Draft 2020-12)
4. If valid: return the JSON string
5. If invalid: send validation errors back to the model as a tool result, retry

The top-level schema must be `"type": "object"` (required by the tool call
protocol). To return arrays, wrap them in an object property.

### Session configuration

| Variable | Default | Description |
|---|---|---|
| `llm_endpoint` | `http://localhost:8080/v1/chat/completions` | Chat completions URL |
| `llm_model` | `anthropic/claude-haiku-4-5-20251001` | Default model |
| `bh_http_config` | `MAP {}` | Auth, rate limiting, Vault config |

### Gateway setup (Bifrost)

```bash
docker run -d --name bifrost -p 8080:8080 maximhq/bifrost

API_KEY=$(bao kv get -field=api_key secret/blobapi/anthropic)
curl -X POST http://localhost:8080/api/providers \
  -H 'Content-Type: application/json' \
  -d "{\"name\": \"anthropic\", \"provider\": \"anthropic\",
       \"keys\": [{\"name\": \"default\", \"value\": \"$API_KEY\",
                   \"enabled\": true, \"weight\": 1}]}"
```

Model names use `provider/model` format: `anthropic/claude-haiku-4-5-20251001`,
`openai/gpt-4o`, etc.

### Dependencies

- [jsoncons](https://github.com/danielaparker/jsoncons) v1.1.0 (header-only) —
  JSON Schema validation and JMESPath for response reshaping.
- [blobtemplates](../blobtemplates) extension (runtime) — provides
  `bt_template_render()` (Inja/Jinja2) for prompt construction. Must be loaded
  alongside blobhttp when using `llm_adapt`.

## Negotiate (SPNEGO/Kerberos) Authentication

### `bh_negotiate_auth_header(url)`

Returns the `Authorization` header value for SPNEGO/Kerberos authentication.
Requires a valid Kerberos ticket (`kinit` or OS-level SSO). The URL must be
HTTPS.

```sql
SELECT bh_negotiate_auth_header('https://intranet.example.com/api/data');
-- Returns: 'Negotiate YIIGhgYJKoZI...'
```

Use it to authenticate HTTP requests to Kerberos-protected services:

```sql
SELECT r.response_status_code, r.response_body
FROM (SELECT bh_http_get('https://intranet.example.com/api/data',
    headers := MAP {'Authorization': bh_negotiate_auth_header('https://intranet.example.com/api/data')}) AS r);
```

Or configure it globally so all requests to a host auto-authenticate:

```sql
SET VARIABLE bh_http_config = MAP {
    'https://intranet.example.com/': '{"auth_type": "negotiate"}'
};

-- No explicit headers needed — the token is generated and injected automatically
SELECT r.response_status_code
FROM (SELECT bh_http_get('https://intranet.example.com/api/data') AS r);
```

### `bh_negotiate_auth_header_json(url)`

Returns a JSON object with the token and debugging metadata about the
authentication process. Useful for diagnosing Kerberos issues.

```sql
SELECT bh_negotiate_auth_header_json('https://intranet.example.com/api/data');
```

Returns:

```json
{
    "token": "YIIGhgYJKoZIhvcSAQICAQBuggY...",
    "header": "Negotiate YIIGhgYJKoZIhvcSAQICAQBuggY...",
    "url": "https://intranet.example.com/api/data",
    "hostname": "intranet.example.com",
    "spn": "HTTP@intranet.example.com",
    "provider": "GSS-API",
    "library": "/System/Library/Frameworks/GSS.framework/Versions/Current/GSS"
}
```

The fields:

| Field | Description |
|-------|-------------|
| `token` | Base64-encoded SPNEGO token |
| `header` | Complete `Authorization` header value (`Negotiate <token>`) |
| `url` | The URL the token was generated for |
| `hostname` | Hostname extracted from the URL |
| `spn` | Service Principal Name used (`HTTP@hostname`) |
| `provider` | Authentication provider (`GSS-API` on macOS/Linux, `SSPI` on Windows) |
| `library` | Path to the loaded security library |

This is particularly useful for verifying that the correct SPN is being
constructed, that the right security library is loaded, and that the hostname
extraction is working as expected.

## Troubleshooting

### Negotiate auth returns an error

- **"Negotiate authentication requires HTTPS"** — SPNEGO tokens are only
  generated for HTTPS URLs. This prevents accidental credential leakage over
  plaintext HTTP.

- **GSS-API / SSPI errors** — Ensure you have a valid Kerberos ticket. On
  macOS/Linux, run `klist` to check and `kinit` to obtain one. On Windows,
  tickets are managed by the OS via domain login.

- **Wrong SPN** — The extension constructs the SPN as `HTTP@hostname`. If
  your service is registered under a different SPN, you'll get an authentication
  failure. Use `bh_negotiate_auth_header_json(url)` to inspect the SPN being used.

### Bearer token expired

The extension checks `bearer_token_expires_at` before each request. If your
token has expired, you'll see an error with both ISO 8601 and Unix timestamps.
Refresh the token in your hosting application and call
`bh_http_config_set_bearer` (see [Bearer token with expiry](#bearer-token-with-expiry)).

### mTLS handshake failures

- Verify that `client_cert` and `client_key` paths are absolute and readable
  by the DuckDB process.
- Ensure the certificate and key match (they must be from the same keypair).
- If the server uses a private CA, set `ca_bundle` to the CA chain file.

### Dead-column elimination

`SELECT count(*) FROM (SELECT bh_http_get(url) FROM urls)` fires zero requests
because DuckDB's optimizer eliminates unused columns. Reference a field from
the result to force evaluation:

```sql
SELECT count(*) FROM (
    SELECT (bh_http_get(url)).response_status_code AS status FROM urls
);
```

## Building

```bash
make
```

This configures cmake, builds the extension, and stamps the metadata for
DuckDB to load it.

## Testing

### Automated tests

```bash
make test_release
```

The sqllogictest suite (`test/sql/`) covers error cases for
Negotiate auth, table functions against httpbin.org, and scalar function usage
including data-driven queries.

### Concurrency testing with the Flask server

A Flask server (`test/flask_concurrency_server.py`) instruments concurrent
connections to verify parallel execution behavior. It tracks per-request
arrival/departure times, thread identity, and peak concurrency.

```bash
# Terminal 1: start the concurrency test server
python3 test/flask_concurrency_server.py
# Listens on http://localhost:8444
```

Endpoints:

| Endpoint | Description |
|----------|-------------|
| `GET /slow/<path>?delay=0.5` | Responds after a configurable delay (default 0.5s). Tracks concurrency. |
| `GET /fast` | Responds immediately. For throughput measurement. |
| `GET /stats` | Returns JSON with `total_requests`, `peak_concurrent_connections`, and per-request log. |
| `GET /reset` | Resets all counters and logs. |
| `GET /health` | Health check. |

#### Verify parallel execution

```bash
# Terminal 2: reset and run 10 requests with 0.3s delay each
curl -s http://localhost:8444/reset > /dev/null

duckdb -unsigned -cmd "LOAD 'build/release/bhttp.duckdb_extension';" -c "
SELECT id,
       json_extract(bh_http_request('GET',
           'http://localhost:8444/slow/' || id::VARCHAR || '?delay=0.3',
           NULL, NULL, NULL), '\$.response_status_code')::INTEGER AS status
FROM range(10) AS t(id);
"

# Check what the server saw
curl -s http://localhost:8444/stats | python3 -m json.tool
```

With the default `max_concurrent=10`, all 10 requests arrive within
milliseconds of each other (wall-clock time ~0.3s, not 3.0s). The server
reports `peak_concurrent_connections: 10`.

#### Verify batching with max_concurrent

```bash
curl -s http://localhost:8444/reset > /dev/null

duckdb -unsigned -cmd "LOAD 'build/release/bhttp.duckdb_extension';" -c "
SET VARIABLE bh_http_config = MAP {
    'default': '{\"max_concurrent\": 3, \"rate_limit\": \"100/s\"}'
};

SELECT id,
       json_extract(bh_http_request('GET',
           'http://localhost:8444/slow/' || id::VARCHAR || '?delay=0.3',
           NULL, NULL, NULL), '\$.response_status_code')::INTEGER AS status
FROM range(10) AS t(id);
"

curl -s http://localhost:8444/stats | python3 -m json.tool
```

The server reports `peak_concurrent_connections: 3`. Requests arrive in 4
batches of sizes [3, 3, 3, 1], with ~0.3s between batches (total wall-clock
~1.2s). This confirms that `max_concurrent` correctly limits parallelism.

#### Analyze batch timing

A quick script to summarize arrival batches from the server stats:

```bash
curl -s http://localhost:8444/stats | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(f'Total requests: {data[\"total_requests\"]}')
print(f'Peak concurrent: {data[\"peak_concurrent_connections\"]}')
arrivals = sorted(r['arrived'] for r in data['request_log'])
print(f'Arrival span: {arrivals[-1] - arrivals[0]:.3f}s')
batches, batch = [], [arrivals[0]]
for a in arrivals[1:]:
    if a - batch[0] < 0.05:
        batch.append(a)
    else:
        batches.append(batch)
        batch = [a]
batches.append(batch)
print(f'Batches: {len(batches)} (sizes: {[len(b) for b in batches]})')
"
```

#### Verify rate limiter diagnostics after testing

```bash
duckdb -unsigned -cmd "LOAD 'build/release/bhttp.duckdb_extension';" -c "
-- Run some requests first
SELECT count(*) FROM (
    SELECT bh_http_request('GET', 'http://localhost:8444/fast', NULL, NULL, NULL)
    FROM range(20) AS t(id)
);

-- Inspect rate limiter
SELECT host, rate_limit, requests, paced, throttled_429,
       round(total_wait_seconds, 3) AS wait_s,
       round(backlog_seconds, 3) AS backlog_s
FROM bh_http_rate_limit_stats();
"
```

### Manual testing with the Flask negotiate server

A test server is included for end-to-end Negotiate authentication testing:

```bash
# Start the test server (auto-generates self-signed cert)
python3 test/flask_negotiate_server.py

# In another terminal
duckdb -unsigned -cmd "LOAD 'build/release/bhttp.duckdb_extension';" -c "
    -- Health check (no auth required)
    SELECT r.response_status_code
    FROM (SELECT bh_http_get('https://localhost:8443/health') AS r);

    -- Authenticated request
    SELECT r.response_status_code, r.response_body
    FROM (SELECT bh_http_get('https://localhost:8443/data.json',
        headers := MAP {'Authorization': bh_negotiate_auth_header('https://localhost:8443/data.json')}) AS r);
"
```

## License

MIT

## Acknowledgments

- [sqlite-http](https://github.com/asg017/sqlite-http) by Alex Garcia — the
  model for HTTP-as-SQL-functions
- [pyspnego](https://github.com/jborean93/pyspnego) — used to validate the
  pre-flight Negotiate technique
- Richard E. Silverman — for suggesting the pre-flight Negotiate approach
