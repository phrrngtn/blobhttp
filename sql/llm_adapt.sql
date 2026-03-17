-- llm_adapt: table-driven adapter for LLM-backed functions.
--
-- Looks up an adapter by name from the llm_adapter table, renders the
-- prompt template via bt_render() (inja/Jinja2, from blobtemplates),
-- merges caller params + session defaults, and passes the resolved config
-- to _llm_adapt_raw() which handles the LLM call, schema validation,
-- and response JMESPath reshaping.
--
-- Requires: bhttp extension (this file), blobtemplates extension (bt_render).
--
-- The llm_adapter table must exist with columns:
--   name, prompt_template, output_schema, response_jmespath, max_tokens
--
-- Usage:
--   SELECT * FROM llm_adapt('physical_properties',
--       json_object('substances', ['water', 'ethanol'],
--                   'metrics', ['boiling point', 'melting point']));

CREATE OR REPLACE MACRO llm_adapt(adapter_name, params) AS TABLE (
    WITH ADAPTER_ROW AS (
        SELECT prompt_template, output_schema, response_jmespath,
               CAST(max_tokens AS VARCHAR) AS max_tokens_str
        FROM llm_adapter
        WHERE name = adapter_name
    ),
    RENDERED AS (
        SELECT
            bt_render(prompt_template, params) AS prompt_text,
            output_schema,
            response_jmespath,
            max_tokens_str
        FROM ADAPTER_ROW
    ),
    CONFIG AS (
        SELECT json_merge_patch(
            json_object(
                'prompt_text',        prompt_text,
                'output_schema',      output_schema,
                'response_jmespath',  response_jmespath,
                'max_tokens',         max_tokens_str,
                'endpoint',           COALESCE(
                    TRY_CAST(getvariable('llm_endpoint') AS VARCHAR),
                    'http://localhost:8080/v1/chat/completions'),
                'model',              COALESCE(
                    TRY_CAST(getvariable('llm_model') AS VARCHAR),
                    'anthropic/claude-haiku-4-5-20251001'),
                'http_config',        CAST(IFNULL(
                    TRY_CAST(getvariable('bh_http_config') AS MAP(VARCHAR, VARCHAR)),
                    MAP {}) AS JSON)::VARCHAR::JSON
            ),
            params::JSON
        ) AS config_json
        FROM RENDERED
    )
    SELECT _llm_adapt_raw(CAST(config_json AS VARCHAR)) AS result
    FROM CONFIG
);
