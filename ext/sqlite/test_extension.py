"""Smoke test: load the blobhttp SQLite extension and verify functions exist."""
import sqlite3
from blobhttp_sqlite import extension_path

EXPECTED_FUNCTIONS = [
    "bh_http_request",
    "bh_http_get",
    "bh_http_post",
    "bh_negotiate_auth_header",
    "bh_negotiate_auth_header_json",
    "bh_http_rate_limit_stats",
]

conn = sqlite3.connect(":memory:")
conn.enable_load_extension(True)
conn.load_extension(extension_path())

for func in EXPECTED_FUNCTIONS:
    try:
        conn.execute(f"SELECT {func}('dummy')")
    except sqlite3.OperationalError as e:
        if "no such function" in str(e).lower():
            raise AssertionError(f"Function {func} not registered") from e

print(f"OK: all {len(EXPECTED_FUNCTIONS)} expected functions present")
