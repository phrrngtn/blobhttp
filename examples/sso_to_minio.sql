-- Kerberos SSO -> OIDC JWT -> MinIO STS -> DuckDB secret -> read s3://
--
-- The whole chain in SQL. Nothing here holds a long-lived credential: the only
-- thing on disk is *where* the IdP and the object store are. Everything else
-- derives from the ambient Kerberos ticket and expires with the STS lease.
--
--   1. bh_sso_jwt          Kerberos ticket  -> OIDC JWT           (blobhttp)
--   2. bh_http_post        JWT              -> STS credentials    (MinIO)
--   3. CREATE SECRET       STS credentials  -> DuckDB secret manager
--   4. read_parquet        secret           -> s3:// access       (httpfs)
--
-- Run, from the repo root:
--   KRB5CCNAME=FILE:/tmp/lakecc.verify \
--   KEYCLOAK_CLIENT_SECRET=... \
--   duckdb -unsigned < examples/sso_to_minio.sql
--
-- Piped rather than -init: dot-commands and COPY interleave correctly that
-- way, and step 3 depends on the file COPY writes existing before .read.
--
-- Step 3 is the one place SQL alone is not enough, and the workaround is
-- honest rather than hidden: DuckDB's CREATE SECRET takes literal values and
-- has no `AS SELECT` form, so the statement is *generated* into a file and
-- read back. That is two extra lines of the CLI's own dot-commands, not a host
-- language.

.bail on

-- Path is relative to the repo root; see the run line above.
LOAD 'zig-out/lib/bhttp.duckdb_extension';
INSTALL httpfs;
LOAD httpfs;

-- DuckDB has no quote_literal, and the STS secret routinely contains '/'
-- and '+'. Single-quote, doubling any embedded quote.
CREATE OR REPLACE MACRO sql_quote(v) AS '''' || replace(v, '''', '''''') || '''';

-- ── parameters ──────────────────────────────────────────────────────
-- Everything environment-specific in one place. The client secret is a
-- Keycloak *client* credential, not a user credential — it identifies the
-- application, and on its own grants nothing without a Kerberos ticket.
CREATE OR REPLACE TEMP TABLE cfg AS SELECT
    'https://keycloak.phrrngtn.arpa:8443/realms/lake' AS issuer,
    'minio'                                          AS client_id,
    getenv('KEYCLOAK_CLIENT_SECRET')                 AS client_secret,
    '/tmp/kc_ca.pem'                                 AS ca_bundle,
    'http://127.0.0.1:9000'                          AS minio_endpoint,
    's3://lake/demo.parquet'                         AS target;

-- ── 1. Kerberos ticket -> OIDC JWT ──────────────────────────────────
-- bh_sso_jwt does the SPNEGO dance against Keycloak's authorization endpoint
-- and exchanges the resulting code for a token. No password anywhere.
CREATE OR REPLACE TEMP TABLE jwt AS
SELECT bh_sso_jwt(json_object(
           'issuer',        issuer,
           'client_id',     client_id,
           'client_secret', client_secret,
           'ca_bundle',     ca_bundle)) AS token
FROM cfg;

SELECT 'step 1: got a JWT of ' || length(token) || ' chars' AS status FROM jwt;

-- ── 2. JWT -> MinIO STS temporary credentials ───────────────────────
-- AssumeRoleWithWebIdentity trades the OIDC token for short-lived S3
-- credentials. MinIO answers in XML, so the fields come out by regex — there
-- is no XML parser in DuckDB and pulling one in for four captures is not worth
-- it. The response is machine-generated and its shape is stable.
CREATE OR REPLACE TEMP TABLE sts AS
WITH RESPONSE AS (
    -- Named arguments: the macro is bh_http_post(url, headers:=, params:=,
    -- body:=, content_type:=), so the body cannot be passed positionally.
    -- The result is a STRUCT, not JSON — response_body is a field.
    SELECT bh_http_post(
               cfg.minio_endpoint || '/',
               body := 'Action=AssumeRoleWithWebIdentity'
                       || '&Version=2011-06-15'
                       || '&DurationSeconds=3600'
                       || '&WebIdentityToken=' || jwt.token,
               content_type := 'application/x-www-form-urlencoded'
           ) AS r
    FROM cfg, jwt
)
SELECT
    regexp_extract(r.response_body, '<AccessKeyId>([^<]*)</AccessKeyId>', 1)         AS key_id,
    regexp_extract(r.response_body, '<SecretAccessKey>([^<]*)</SecretAccessKey>', 1) AS secret,
    regexp_extract(r.response_body, '<SessionToken>([^<]*)</SessionToken>', 1)       AS session_token,
    regexp_extract(r.response_body, '<Expiration>([^<]*)</Expiration>', 1)           AS expires,
    r.response_status_code                                                            AS status_code,
    r.response_body                                                                   AS xml
FROM RESPONSE;

-- Fail loudly rather than creating an empty secret and puzzling over a 403
-- three steps later.
SELECT CASE
         WHEN key_id = '' THEN error(
             'STS returned no credentials (HTTP ' || status_code || '): ' || xml)
         ELSE 'step 2: STS credentials expire at ' || expires
       END AS status
FROM sts;

-- ── 3. STS credentials -> the secret manager ────────────────────────
-- CREATE SECRET has no `AS SELECT` form, so generate the DDL and read it back.
COPY (
    SELECT 'CREATE OR REPLACE SECRET minio_sso ('
        || ' TYPE s3,'
        || ' PROVIDER config,'
        || ' KEY_ID ' || sql_quote(key_id) || ','
        || ' SECRET ' || sql_quote(secret) || ','
        || ' SESSION_TOKEN ' || sql_quote(session_token) || ','
        || ' ENDPOINT ' || sql_quote(replace(cfg.minio_endpoint, 'http://', '')) || ','
        || ' URL_STYLE ''path'','
        || ' USE_SSL false'
        || ');'
    FROM sts, cfg
) TO '/tmp/minio_sso_secret.sql' (FORMAT csv, HEADER false, QUOTE '');

.read /tmp/minio_sso_secret.sql

-- The generated file holds live credentials for an hour. Remove it now that
-- the secret manager has them; leaving it around would undo the point of
-- deriving them per session.
.shell rm -f /tmp/minio_sso_secret.sql

SELECT 'step 3: secret registered -> ' || name || ' (' || type || ')' AS status
FROM duckdb_secrets() WHERE name = 'minio_sso';

-- ── 4. Read through httpfs, using the secret ────────────────────────
-- No credentials named here. httpfs resolves the s3:// scheme against the
-- secret manager, which is holding the STS lease we just derived.
SELECT 'step 4: reading ' || target AS status FROM cfg;
SELECT * FROM read_parquet((SELECT target FROM cfg)) LIMIT 10;
