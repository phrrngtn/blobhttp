"""bh_sso_jwt through the SQLite extension, against the live Keycloak."""
import json, sqlite3, sys

cfg = json.dumps({
    "issuer": "https://keycloak.phrrngtn.arpa:8443/realms/lake",
    "client_id": "minio",
    "client_secret": sys.argv[1],
    "http_config": {
        "https://keycloak.phrrngtn.arpa:8443/": json.dumps({"ca_bundle": "/tmp/kc_ca.pem"})
    },
})

con = sqlite3.connect(":memory:")
con.enable_load_extension(True)
con.load_extension("/tmp/bhttp")

jwt_len, token_type = con.execute(
    "SELECT length(bh_sso_jwt(?)), json_extract(bh_sso_jwt_json(?), '$.token_type')",
    (cfg, cfg),
).fetchone()
print("jwt_len:", jwt_len, " token_type:", token_type)
