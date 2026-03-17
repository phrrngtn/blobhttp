-- Per-verb scalar macros route to the idempotent or volatile C function
-- based on HTTP method semantics. All return STRUCT.
--
-- The C function takes all VARCHAR (JSON strings) for headers, params,
-- and config. The macros accept MAP literals for headers and convert
-- via CAST(... AS JSON) so both forms work:
--
--   headers := MAP {'Authorization': 'Bearer xyz'}     -- literal
--   headers := json_object('Authorization', token)     -- dynamic
--   headers := vault_response_json                     -- from CTE
--
-- Params is a JSON object whose keys/values become URL query parameters.
-- Compose layers with json_merge_patch(catalog_params, app_params).

CREATE OR REPLACE MACRO bh_http_get(url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR) AS
    _bh_http_raw_request('GET', url,
        CAST(headers AS JSON), params, NULL, NULL,
        CAST(_bh_http_config() AS JSON));

CREATE OR REPLACE MACRO bh_http_head(url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR) AS
    _bh_http_raw_request('HEAD', url,
        CAST(headers AS JSON), params, NULL, NULL,
        CAST(_bh_http_config() AS JSON));

CREATE OR REPLACE MACRO bh_http_options(url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR) AS
    _bh_http_raw_request('OPTIONS', url,
        CAST(headers AS JSON), params, NULL, NULL,
        CAST(_bh_http_config() AS JSON));

CREATE OR REPLACE MACRO bh_http_put(url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR,
    body := NULL::VARCHAR,
    content_type := NULL::VARCHAR) AS
    _bh_http_raw_request('PUT', url,
        CAST(headers AS JSON), params, body, content_type,
        CAST(_bh_http_config() AS JSON));

CREATE OR REPLACE MACRO bh_http_delete(url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR) AS
    _bh_http_raw_request('DELETE', url,
        CAST(headers AS JSON), params, NULL, NULL,
        CAST(_bh_http_config() AS JSON));

-- Non-idempotent verbs: volatile, every call fires.

CREATE OR REPLACE MACRO bh_http_post(url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR,
    body := NULL::VARCHAR,
    content_type := NULL::VARCHAR) AS
    _bh_http_raw_request_volatile('POST', url,
        CAST(headers AS JSON), params, body, content_type,
        CAST(_bh_http_config() AS JSON));

CREATE OR REPLACE MACRO bh_http_patch(url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR,
    body := NULL::VARCHAR,
    content_type := NULL::VARCHAR) AS
    _bh_http_raw_request_volatile('PATCH', url,
        CAST(headers AS JSON), params, body, content_type,
        CAST(_bh_http_config() AS JSON));

-- Generic scalar: method is a runtime parameter, so must be volatile
-- (we can't know at compile time whether it's idempotent).

CREATE OR REPLACE MACRO bh_http_request(method, url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR,
    body := NULL::VARCHAR,
    content_type := NULL::VARCHAR) AS
    _bh_http_raw_request_volatile(method, url,
        CAST(headers AS JSON), params, body, content_type,
        CAST(_bh_http_config() AS JSON));

-- JSON variant: wraps the STRUCT with to_json().

CREATE OR REPLACE MACRO bh_http_request_json(method, url,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    params := NULL::VARCHAR,
    body := NULL::VARCHAR,
    content_type := NULL::VARCHAR) AS
    to_json(_bh_http_raw_request_volatile(method, url,
        CAST(headers AS JSON), params, body, content_type,
        CAST(_bh_http_config() AS JSON)));
