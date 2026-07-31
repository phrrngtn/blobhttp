#!/bin/sh
# The full SSO chain **at the SQL level**, in both hosts.
#
# The existing probes leave a gap. `sso_sql_probe.sh` and `sso_sqlite_probe.py`
# demonstrate `bh_sso_jwt` from SQL — that is *token acquisition* only.
# `vault_jwt_probe.py` demonstrates the whole chain, but through ctypes. So the
# thing that actually matters has never been shown from SQL:
#
#     a SQL HTTP request whose bearer token was fetched from OpenBao using a
#     JWT obtained by Kerberos SSO, with no secret anywhere in the config
#
# That is what this proves. The scope config below carries no vault_token and no
# API key; everything derives from the ambient Kerberos ticket.
#
# Run on dc1, with a lake-realm ticket:
#     KRB5CCNAME=FILE:/tmp/lakecc.verify sh test/vault_sql_probe.sh <client_secret>
#
# Prerequisites, all of which the probe checks:
#   - a valid TGT for lakeuser@PHRRNGTN.ARPA
#   - Keycloak reachable at keycloak.phrrngtn.arpa:8443, realm `lake`
#   - OpenBao at 127.0.0.1:8200 with the `blobhttp` JWT role bound to that realm
set -eu

[ $# -ge 1 ] || { echo "usage: $0 <keycloak-client-secret>" >&2; exit 2; }
SECRET="$1"
EXT="${EXT:-$PWD/zig-out/lib/bhttp}"
TARGET="https://httpbin.org/headers"   # echoes request headers back to us

# ── preflight ────────────────────────────────────────────────────────
klist -s 2>/dev/null || { echo "no Kerberos ticket — set KRB5CCNAME" >&2; exit 1; }
echo "ticket    : $(klist 2>/dev/null | awk '/Default principal/{print $3}')"
curl -s -m 5 http://127.0.0.1:8200/v1/sys/health >/dev/null \
  || { echo "OpenBao unreachable at 127.0.0.1:8200" >&2; exit 1; }
echo "openbao   : up"

# ── the scope config: note what is NOT in it ─────────────────────────
# No vault_token. No api key. Only *where* the IdP and the vault are.
CFG=$(cat <<JSON
{"auth_type":"bearer",
 "vault_auth_method":"jwt",
 "vault_addr":"http://127.0.0.1:8200",
 "vault_path":"secret/blobapi/demo",
 "vault_field":"api_key",
 "vault_jwt_role":"blobhttp",
 "oidc_issuer":"https://keycloak.phrrngtn.arpa:8443/realms/lake",
 "oidc_client_id":"minio",
 "oidc_client_secret":"$SECRET",
 "ca_bundle":"/tmp/kc_ca.pem"}
JSON
)
case "$CFG" in *vault_token*) echo "config leaked a vault token" >&2; exit 1;; esac
echo "config    : carries no vault_token (the point of the exercise)"

# SQL string literal: double the single quotes.
SQLCFG=$(printf '%s' "$CFG" | tr -d '\n' | sed "s/'/''/g")

echo
echo "=== DuckDB ==="
duckdb -unsigned -noheader -list <<SQL
LOAD '${EXT}.duckdb_extension';
SELECT bh_http_config_set('${TARGET}', '${SQLCFG}');
-- httpbin echoes our headers back, so the Authorization it *received* is the
-- proof the secret arrived — without us printing it from this side.
SELECT 'authorization_scheme=' ||
       coalesce(split_part(json_extract_string(
         json_extract_string(bh_http_get('${TARGET}'), '\$.response_body'),
         '\$.headers.Authorization'), ' ', 1), '(absent)');
SQL

echo
echo "=== SQLite ==="
sqlite3 :memory: <<SQL
.load ${EXT}
SELECT bh_http_config_set('${TARGET}', '${SQLCFG}');
SELECT 'authorization_scheme=' ||
       coalesce(substr(json_extract(
         json_extract(bh_http_get('${TARGET}'), '\$.response_body'),
         '\$.headers.Authorization'), 1, 6), '(absent)');
SQL

echo
echo "If both print 'Bearer', the chain ran entirely inside SQL:"
echo "  Kerberos ticket -> Keycloak JWT -> OpenBao -> bearer header -> request"
