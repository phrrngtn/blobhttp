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

registered = set(
    row[0]
    for row in conn.execute("SELECT name FROM pragma_function_list").fetchall()
)

missing = [f for f in EXPECTED_FUNCTIONS if f not in registered]
if missing:
    raise AssertionError(f"Missing functions: {missing}")

print(f"OK: all {len(EXPECTED_FUNCTIONS)} expected functions present in pragma_function_list")
