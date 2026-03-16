-- llm_complete: schema-validated LLM completion with automatic continuation.
--
-- Sends a chat completion request to an OpenAI-compatible endpoint (direct
-- or via a gateway like Bifrost). If the model hits max_tokens, automatically
-- continues until the response is complete. If output_schema is provided,
-- validates the response against it and retries with error feedback on failure.
--
-- Returns VARCHAR: the completed text, or validated JSON string.
--
-- The function reads http_config for auth, rate limiting, Vault secrets, etc.
-- Configure the gateway scope in http_config like any other endpoint:
--
--   SET VARIABLE http_config = MAP {
--       'http://localhost:8080/': '{"vault_path": "secret/blobapi/bifrost",
--                                   "auth_type": "bearer", "rate_limit": "5/s"}'
--   };

CREATE OR REPLACE MACRO llm_complete(url, body,
    headers := NULL::MAP(VARCHAR, VARCHAR),
    output_schema := NULL::VARCHAR,
    max_continuations := 10,
    max_retries := 3) AS
    _llm_complete_raw(url, body,
        COALESCE(CAST(headers AS JSON), '{}'),
        COALESCE(CAST(_http_config() AS JSON), '{}'),
        COALESCE(output_schema, ''),
        max_continuations,
        max_retries);
