"""Exercise bh_sso_jwt against the live Keycloak, then use the JWT on OpenBao.

Run on dc1, where the Kerberos ticket lives:
    KRB5CCNAME=FILE:/tmp/lakecc.verify python3 sso_test.py <client_secret>
"""
import base64
import ctypes
import json
import sys
import urllib.request

lib = ctypes.CDLL("/tmp/libbhttp.so")
lib.bh_sso_jwt.argtypes = [ctypes.c_char_p]
lib.bh_sso_jwt.restype = ctypes.c_void_p
lib.bh_errmsg.restype = ctypes.c_char_p
lib.bh_free.argtypes = [ctypes.c_void_p]

req = {
    "issuer": "https://keycloak.phrrngtn.arpa:8443/realms/lake",
    "client_id": "minio",
    "client_secret": sys.argv[1],
    "redirect_uri": "http://localhost/cb",
    # The issuer is behind a private CA; the scoped config carries the bundle.
    "http_config": {
        "https://keycloak.phrrngtn.arpa:8443/": json.dumps({"ca_bundle": "/tmp/kc_ca.pem"})
    },
}

p = lib.bh_sso_jwt(json.dumps(req).encode())
if not p:
    print("bh_sso_jwt FAILED:", lib.bh_errmsg().decode()[:500])
    sys.exit(1)
tok = json.loads(ctypes.cast(p, ctypes.c_char_p).value.decode())
lib.bh_free(p)

pad = lambda s: s + "=" * (-len(s) % 4)
claims = json.loads(base64.urlsafe_b64decode(pad(tok["access_token"].split(".")[1])))
print("1. bh_sso_jwt -> JWT")
print("   expires_in:", tok.get("expires_in"), "token_type:", tok.get("token_type"))
print("   iss :", claims.get("iss"))
print("   user:", claims.get("preferred_username"))

# The point of the exercise: that JWT authenticates to OpenBao with no
# plaintext vault token anywhere.
body = json.dumps({"role": "blobhttp", "jwt": tok["access_token"]}).encode()
r = urllib.request.Request("http://127.0.0.1:8200/v1/auth/jwt/login", data=body, method="POST")
auth = json.load(urllib.request.urlopen(r, timeout=15))["auth"]
print("2. that JWT -> OpenBao login")
print("   policies:", auth["token_policies"], "lease:", auth["lease_duration"], "s")

r = urllib.request.Request(
    "http://127.0.0.1:8200/v1/secret/data/blobapi/demo",
    headers={"X-Vault-Token": auth["client_token"]},
)
print("3. read secret:", json.load(urllib.request.urlopen(r, timeout=15))["data"]["data"])
print()
print("full chain through blobhttp's C ABI, no plaintext vault token")
