#!/usr/bin/env python3
"""A local stand-in for httpbin.org, for test_core_abi.c.

The tests used to call httpbin.org, which rate-limits: the transport would
succeed (so bh_batch_perform returned 0) while the body was a 503 page instead
of the bytes asked for. The suite passed and then failed on the identical
commit d94af86 — CI tasks 5335 and 5353.

Stdlib rather than a container or Flask: one of the three CI runners executes
on the macOS host and has no container, and PyPI's `httpbin` is unmaintained
and broken against modern Werkzeug. This runs anywhere python3 does, offline.

    python3 httpbin.py [port]     # port 0 (default) picks a free one

Prints "READY <port>" once accepting connections, so callers wait rather than
sleep a guessed interval.
"""

import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        pass  # the test prints its own results; request logs bury them

    def do_GET(self):
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        body, status = b"", 404
        if len(parts) == 2 and parts[0] == "bytes":
            # Deterministic, not random: a byte-for-byte assertion should not
            # depend on entropy, and this catches a body mangled mid-stream.
            n = max(0, min(int(parts[1]), 1 << 20))
            body, status = bytes(i % 256 for i in range(n)), 200
        elif len(parts) == 2 and parts[0] == "status":
            status = int(parts[1])

        self.send_response(status)
        self.send_header("Content-Type", "application/octet-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


srv = ThreadingHTTPServer(("127.0.0.1", int(sys.argv[1]) if len(sys.argv) > 1 else 0), Handler)
srv.daemon_threads = True
print(f"READY {srv.server_address[1]}", flush=True)  # flush: pipe is block-buffered
srv.serve_forever()
