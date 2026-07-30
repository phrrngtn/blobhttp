"""End-to-end: a request whose bearer token comes from OpenBao, with no
plaintext vault token in the config.

The scope config carries no secret at all — only where the IdP and the vault
are. Everything else derives from the ambient Kerberos ticket.

Run on dc1 with a lake-realm ticket:
    KRB5CCNAME=FILE:/tmp/lakecc.verify python3 vault_jwt_test.py <client_secret>
"""
import ctypes
import json
import sys

lib = ctypes.CDLL("/tmp/libbhttp.so")
lib.bh_batch_new.argtypes = [ctypes.c_char_p]
lib.bh_batch_new.restype = ctypes.c_void_p
lib.bh_batch_add.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p,
                             ctypes.c_char_p, ctypes.c_char_p, ctypes.c_void_p,
                             ctypes.c_size_t, ctypes.c_char_p, ctypes.c_int, ctypes.c_int]
lib.bh_batch_add.restype = ctypes.c_int
lib.bh_batch_perform.argtypes = [ctypes.c_void_p]
lib.bh_batch_perform.restype = ctypes.c_int
lib.bh_batch_free.argtypes = [ctypes.c_void_p]
lib.bh_result_json.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
lib.bh_result_json.restype = ctypes.c_void_p
lib.bh_errmsg.restype = ctypes.c_char_p
lib.bh_free.argtypes = [ctypes.c_void_p]

# httpbin echoes the request headers back, so the Authorization header it
# received proves the secret arrived — without us printing it from our side.
TARGET = "https://httpbin.org/headers"

scope_config = {
    "auth_type": "bearer",
    # No vault_token. This is the whole point.
    "vault_auth_method": "jwt",
    "vault_addr": "http://127.0.0.1:8200",
    "vault_path": "secret/blobapi/demo",
    "vault_field": "api_key",
    "vault_jwt_role": "blobhttp",
    "oidc_issuer": "https://keycloak.phrrngtn.arpa:8443/realms/lake",
    "oidc_client_id": "minio",
    "oidc_client_secret": sys.argv[1],
    "ca_bundle": "/tmp/kc_ca.pem",
}
config = json.dumps({TARGET: json.dumps(scope_config)})

assert "vault_token" not in config, "config must carry no vault token"

b = lib.bh_batch_new(config.encode())
if not b:
    print("batch_new FAILED:", lib.bh_errmsg().decode()[:300])
    sys.exit(1)
if lib.bh_batch_add(b, b"GET", TARGET.encode(), None, None, None, 0, None, -1, -1) != 0:
    print("add FAILED:", lib.bh_errmsg().decode()[:300])
    sys.exit(1)
if lib.bh_batch_perform(b) != 0:
    print("perform FAILED:", lib.bh_errmsg().decode()[:500])
    lib.bh_batch_free(b)
    sys.exit(1)

p = lib.bh_result_json(b, 0)
res = json.loads(ctypes.cast(p, ctypes.c_char_p).value.decode())
lib.bh_free(p)
lib.bh_batch_free(b)

echoed = json.loads(res["response_body"])["headers"]
auth = echoed.get("Authorization", "")
print("status                :", res["response_status_code"])
print("Authorization arrived :", auth[:7] + "..." if auth else "(absent)")
print("secret matched OpenBao:", auth == "Bearer s3cr3t-from-openbao")
print()
print("config contained a vault token:", "vault_token" in config)
print("secret reached the request anyway — via Kerberos -> JWT -> OpenBao")
