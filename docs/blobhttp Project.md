# blobhttp Project

HTTP as a first-class SQL citizen, with the enterprise concerns that usually
force you out of SQL handled in the extension: authentication, rate limiting,
secret retrieval, connection reuse.

## What it does

`bh_http_get`, `bh_http_post` and the rest return the full request/response
envelope, so a response is a value you can join against. Landing an API result
in a table is `INSERT INTO staging AS SELECT bh_http_get(url) FROM urls` — no
host code moving bytes.

## The parts that are not just curl

| concern | how |
| --- | --- |
| **Kerberos SSO** | `bh_negotiate_auth_header` produces SPNEGO tokens; `bh_negotiate_available` says whether GSS-API is usable at all |
| **OIDC** | `bh_sso_jwt` turns a Kerberos ticket into a JWT — no password, no stored key |
| **Secrets** | scope config can name a Vault/OpenBao path instead of carrying a token, and authenticate to it *by SSO* |
| **Rate limiting** | GCRA, in the core, so all three hosts share one limiter |
| **Connection reuse** | a shared `CURLSH` — this was measured, and before the fix 10 requests opened 10 connections |
| **LLM** | `llm_complete` runs the completion loop with schema validation and continuation, in the core rather than in SQL |

## Worked example

`examples/sso_to_minio.sql` runs the whole chain in SQL: Kerberos ticket → OIDC
JWT → MinIO STS credentials → DuckDB secret manager → `read_parquet('s3://...')`.
Nothing on disk is a long-lived credential.

## Building

`zig build`. One prerequisite: Zig 0.16.0 — no CMake, no Make, no `configure`.
See [[Building the Blob Family]] for the full instructions, cross-compilation,
testing, and how to verify an extension actually loads.

## The family pattern

blob* extensions share one shape: a **C ABI core** carrying the behaviour, with
thin per-host shims over it — DuckDB, SQLite, and Python through ctypes. The
core is where fixes go, so all three hosts benefit at once; the shims are
deliberately boring. Build scaffolding is shared through
[[blobzig Project|blobzig]].

## Related

- [[SSO Layering — spnego-token, blobsso, blobhttp]] — why the OIDC flow is
  duplicated deliberately while the SPNEGO token code is shared
- `docs/design-notes-http-layering.md` — the layering rationale
