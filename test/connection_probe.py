"""Count distinct client connections against a server that honours keep-alive.

The first version of this used Flask, which was useless for the purpose:
Werkzeug's dev server answers `Connection: close`, so every request opens a new
socket no matter what the client does — plain curl scored 3 connections for 3
requests too. Any client-side conclusion drawn from that would have been wrong.

http.server with protocol_version = "HTTP/1.1" does persist connections, as
long as every response carries a Content-Length.
"""
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

_lock = threading.Lock()
_ports: set = set()
_requests = [0]


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"  # without this, keep-alive is off

    def _send(self, body: bytes) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path == "/reset":
            with _lock:
                _ports.clear()
                _requests[0] = 0
            self._send(b"reset")
            return
        if self.path == "/stats":
            with _lock:
                body = f'{{"connections":{len(_ports)},"requests":{_requests[0]}}}'.encode()
            self._send(body)
            return
        with _lock:
            # client_address is (host, ephemeral_port) — one port per TCP connection
            _ports.add(self.client_address[1])
            _requests[0] += 1
        self._send(b"ok")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("127.0.0.1", 8478), Handler).serve_forever()
