# Design Review: duckdb-http-client v0

Notes from a review of the current API surface, responsibility boundaries,
correctness concerns, and gaps. Written before any external users.

## API surface

The public API is:

| Function | Kind | Purpose |
|----------|------|---------|
| `bh_http_get`, `bh_http_head`, `bh_http_options`, `bh_http_delete` | Scalar macro (idempotent) | Per-verb GET/HEAD/OPTIONS/DELETE |
| `bh_http_put` | Scalar macro (idempotent) | PUT with body |
| `bh_http_post`, `bh_http_patch` | Scalar macro (volatile) | POST/PATCH — every call fires |
| `bh_http_request(method, url, ...)` | Scalar macro (volatile) | Generic method variant |
| `bh_http_request_json(method, url, ...)` | Scalar macro (volatile) | JSON variant via `to_json()` |
| `bh_negotiate_auth_header(url)` | Scalar | SPNEGO token generation |
| `bh_negotiate_auth_header_json(url)` | Scalar | SPNEGO token + debug metadata |
| `bh_http_rate_limit_stats()` | Table | Rate limiter and request diagnostics |

Plus two internal C functions (`_bh_http_raw_request`, `_bh_http_raw_request_volatile`)
and one helper macro (`_bh_http_config`) that are not intended for direct use.

All scalar functions return a STRUCT with the same fields (request_url,
response_status_code, response_body, response_headers, etc.). Access fields
via the CTE/subquery pattern: `SELECT r.field FROM (SELECT bh_http_get(url) AS r)`.

The Negotiate helpers exist because SPNEGO token generation can't be folded
into a config flag without a pre-flight step.

### Assessment

The surface feels right-sized. Per-verb macros route to the appropriate C
function variant based on HTTP method semantics — idempotent verbs (GET, HEAD,
OPTIONS, PUT, DELETE) allow the optimizer to deduplicate identical calls, while
non-idempotent verbs (POST, PATCH) are marked volatile so every call fires.
Named parameters with defaults eliminate the need for trailing NULLs.

## Responsibility boundaries

| Concern | Owner | Mechanism |
|---------|-------|-----------|
| Context resolution (variables, config) | SQL macros | `getvariable()` in caller's context |
| HTTP execution | C functions | cpr/libcurl |
| Request-rate governance | Extension (GCRA) | Per-host + global rate limiters |
| Bandwidth governance | Infrastructure | Proxy (`delay_pools`, QoS) |
| Concurrency control | Extension | `max_concurrent` + `MultiPerform` |
| Authentication | Extension + OS | Config-driven; Negotiate via GSS-API/SSPI |
| Observability | Extension | `bh_http_rate_limit_stats()` with raw counters |

### Assessment

The joints are clean. The macro/C split is forced by the C API's lack of
context access, but it turned out to be a reasonable separation: macros do
context, C does compute. Bandwidth is explicitly not our problem — we provide
the `proxy` config field and leave shaping to infrastructure that understands
bytes-on-the-wire.

The one joint worth watching is rate limiting. The extension always applies a
default (20 req/s) which is conservative and correct for most cases, but
environments that already have API gateways or load balancers doing rate
limiting may want to disable ours entirely. The `"rate_limit": "none"` option
now supports this.

## Correctness concerns

### Dead-column elimination

DuckDB's optimizer will skip evaluating a scalar function if its result is not
used. `SELECT count(*) FROM (SELECT bh_http_request(...))` fires zero HTTP
requests because `count(*)` only needs the row count, not the column value.

This is correct optimizer behavior — scalar functions should not have side
effects. HTTP requests are inherently side-effecting, so this is a fundamental
tension. The `VOLATILE` flag (now set) prevents deduplication of identical
arguments but does not prevent dead-column elimination.

**Mitigation**: document the pattern. Users should extract a value from the
result to force evaluation:

```sql
-- Wrong: fires 0 requests
SELECT count(*) FROM (SELECT bh_http_request('GET', url) FROM urls);

-- Right: forces evaluation
SELECT count(*) FROM (
    SELECT json_extract(bh_http_request('GET', url), '$.response_status_code') AS s
    FROM urls
);
```

### Rate limiter state scope

Rate limiters are process-global singletons. If two connections in the same
DuckDB process configure different rates for the same host, the first
connection's rate wins — `GetOrCreate` returns the existing limiter. This is
arguably correct (one rate per host, globally) but could surprise users who
expect per-connection isolation.

### Response bodies in memory

All response bodies are held as strings in memory during chunk processing. A
batch of `max_concurrent` responses to large-payload endpoints (multi-MB)
could consume significant memory. There is no streaming, no response size cap,
and no way for the user to limit it except by reducing `max_concurrent`.

### Error handling in MultiPerform

If one request in a batch fails (DNS error, timeout), the other requests in
the same `MultiPerform` batch may still succeed. The failed request returns
with status code 0 and the error in the response body. If `Perform()` itself
throws, the entire chunk fails with a single error.

## Gaps

### No retry logic

A transient 503 or network timeout fails the request permanently. The rate
limiter adapts to 429s by slowing down future requests, but it does not
re-send the failed one. For batch workloads against flaky APIs, this means
partial results with errors mixed in.

### No response size limit

There is no way to cap the size of a response body. A `SELECT bh_http_request(
'GET', url)` against an endpoint that returns a 1GB file will attempt to load
the entire body into a VARCHAR.

### Scalar function VOLATILE vs dead-column elimination

As noted above, `VOLATILE` solves argument deduplication but not dead-column
elimination. There is no mechanism in DuckDB's C API to mark a function as
"must always evaluate" regardless of whether its output is consumed. This is a
documentation issue, not a code issue.

### CI

~~There is no GitHub Actions workflow.~~ A GitHub Actions workflow now runs on
Ubuntu and macOS, building the extension and running a Python test suite against
a local Flask server. The sqllogictest suite still depends on httpbin.org for
some tests and is not yet run in CI.

## What we would not change

- **The macro-over-C-function pattern.** Forced by the C API, but it's a
  clean separation of context vs compute. If `duckdb_function_get_variable`
  is ever added, the macros become optional sugar.

- **STRUCT return type for the scalar function.** The primary functions
  return a typed STRUCT with native MAP headers; `bh_http_request_json` is
  available as a JSON variant for callers that prefer a single string.

- **Per-host rate limiting by default.** 20 req/s is conservative enough
  to prevent accidental damage, permissive enough not to interfere with
  normal use. The `"none"` escape hatch exists for environments that don't
  want it.

- **Delegating bandwidth to infrastructure.** The extension should not try
  to be a traffic shaper. The `proxy` config field is the right integration
  point.
