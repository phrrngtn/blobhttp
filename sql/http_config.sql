-- Internal helper: safely read the bh_http_config variable.
-- Returns an empty MAP if the variable is not set.
CREATE OR REPLACE MACRO _bh_http_config() AS
    IFNULL(TRY_CAST(getvariable('bh_http_config') AS MAP(VARCHAR, VARCHAR)), MAP {});
