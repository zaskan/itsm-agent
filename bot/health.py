"""Liveness / readiness HTTP server."""

from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

_ready = threading.Event()


def set_ready() -> None:
    _ready.set()


class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        path = self.path.split("?", 1)[0]
        if path == "/healthz":
            body, code = b'{"status":"ok"}', 200
        elif path == "/readyz":
            if _ready.is_set():
                body, code = b'{"status":"ready"}', 200
            else:
                body, code = b'{"status":"starting"}', 503
        else:
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt: str, *args: object) -> None:
        return


def run_health_server(port: int) -> None:
    HTTPServer(("0.0.0.0", port), _HealthHandler).serve_forever()
