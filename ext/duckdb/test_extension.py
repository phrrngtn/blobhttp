"""Smoke test: load the blobhttp DuckDB extension and verify functions exist."""
import duckdb
from blobhttp_duckdb import extension_path

EXPECTED_FUNCTIONS = [
    "bh_negotiate_auth_header",
    "bh_negotiate_auth_header_json",
    "bh_http_rate_limit_stats",
]

conn = duckdb.connect(config={"allow_unsigned_extensions": "true"})
conn.execute(f"LOAD '{extension_path()}'")

registered = set(
    row[0]
    for row in conn.execute(
        "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name LIKE 'bh_%'"
    ).fetchall()
)

missing = [f for f in EXPECTED_FUNCTIONS if f not in registered]
if missing:
    raise AssertionError(f"Missing functions: {missing}")

print(f"OK: {len(registered)} bh_* functions registered, all {len(EXPECTED_FUNCTIONS)} expected functions present")
