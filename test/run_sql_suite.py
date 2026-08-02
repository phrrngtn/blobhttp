#!/usr/bin/env python3
"""Run the SQL-level test corpus against a built bhttp DuckDB extension.

This is the suite CI runs, and it deliberately tests the *consumer surface* —
SQL against the loaded extension — rather than the C ABI underneath it. The C
ABI has its own step (`test-core`); exercising it directly says little about
whether the extension works for anyone.

Two phases, because the corpus splits cleanly:

  http_client_local.test    self-contained. The example.com/api.example.com
                            URLs in it are header-construction and error-path
                            cases that never issue a request.
  http_client_server.test   needs test/flask_concurrency_server.py on :8444,
                            which this script starts and tears down.

Phase 2 is skipped (not failed) if Flask is unavailable, so the self-contained
half still runs in a stripped environment. Anything else is a failure — a suite
that silently skips is worse than no suite, which is exactly how
http_client.test sat dead for weeks requiring a `http_enterprise` extension
that no longer exists.

usage: run_sql_suite.py <extension.duckdb_extension>
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
RUNNER = HERE / "run_sqllogic.py"
SERVER = HERE / "flask_concurrency_server.py"
PORT = int(os.environ.get("PORT", "8444"))
STARTUP_TIMEOUT_S = 20.0


def run_sqllogic(ext: str, *test_files: Path) -> int:
    """Delegate to the existing runner so its pass/fail semantics stay the one
    source of truth. Returns its exit code."""
    cmd = [sys.executable, str(RUNNER), ext, *(str(f) for f in test_files)]
    return subprocess.call(cmd)


def wait_for_port(port: int, deadline: float) -> bool:
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("localhost", port), timeout=0.5):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def phase_server(ext: str) -> int:
    try:
        import flask  # noqa: F401
    except ImportError:
        print("SKIP http_client_server.test — flask not available", flush=True)
        return 0

    env = dict(os.environ, PORT=str(PORT))
    proc = subprocess.Popen(
        [sys.executable, str(SERVER)],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,  # own process group, so teardown cannot orphan it
    )
    try:
        if not wait_for_port(PORT, time.monotonic() + STARTUP_TIMEOUT_S):
            print(
                f"FAIL: concurrency server did not open :{PORT} "
                f"within {STARTUP_TIMEOUT_S:.0f}s",
                flush=True,
            )
            return 1
        return run_sqllogic(ext, HERE / "sql" / "http_client_server.test")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2

    ext = sys.argv[1]
    if not Path(ext).exists():
        print(f"FAIL: extension not found: {ext}", file=sys.stderr)
        return 2

    print("── self-contained ──", flush=True)
    rc_local = run_sqllogic(ext, HERE / "sql" / "http_client_local.test")

    print("── against local Flask server ──", flush=True)
    rc_server = phase_server(ext)

    rc = rc_local or rc_server
    print(f"\nSQL suite: {'PASS' if rc == 0 else 'FAIL'}", flush=True)
    return rc


if __name__ == "__main__":
    sys.exit(main())
