#!/bin/sh
# bh_sso_jwt from SQL, in both hosts, against the live Keycloak.
#
# Run on dc1 with a lake-realm ticket:
#   KRB5CCNAME=FILE:/tmp/lakecc.verify sh sso_sql_test.sh <client_secret>
set -eu
SECRET="$1"

CFG=$(python3 - "$SECRET" <<'PY'
import json, sys
print(json.dumps({
    "issuer": "https://keycloak.phrrngtn.arpa:8443/realms/lake",
    "client_id": "minio",
    "client_secret": sys.argv[1],
    "http_config": {
        "https://keycloak.phrrngtn.arpa:8443/": json.dumps({"ca_bundle": "/tmp/kc_ca.pem"})
    },
}))
PY
)

# SQL string literal: double any single quotes.
SQLCFG=$(printf '%s' "$CFG" | sed "s/'/''/g")

echo "=== DuckDB ==="
duckdb -unsigned -c "
LOAD '/tmp/bhttp.duckdb_extension';
SELECT length(bh_sso_jwt('$SQLCFG')) AS jwt_len,
       json_extract_string(bh_sso_jwt_json('$SQLCFG'), '\$.token_type') AS token_type;
"

echo "=== SQLite ==="
sqlite3 :memory: ".load /tmp/bhttp" "
SELECT length(bh_sso_jwt('$SQLCFG')) AS jwt_len,
       json_extract(bh_sso_jwt_json('$SQLCFG'), '\$.token_type') AS token_type;
"
