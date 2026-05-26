#!/usr/bin/env python3
"""
webhook_stub.py  --  Day 4 deliverable

A placeholder webhook endpoint. Stands in for the AI agent, which does not
exist yet. Its only job: sit at a local web address, catch whatever a Splunk
saved-search alert POSTs to it, and print it to the terminal.

This proves the trip-wire pathway works -- saved search fires -> a message
arrives somewhere -- before the real agent is built in Week 2.

Usage:
  python webhook_stub.py            # listens on http://localhost:8000/
  python webhook_stub.py --port 9000

Leave it running in its own terminal window. Stop it with Ctrl+C.
"""

import argparse
import json
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer


class WebhookHandler(BaseHTTPRequestHandler):
    """Handles one incoming HTTP request: read the body, print it, say OK."""

    def do_POST(self):
        # Read the request body. Content-Length tells us how many bytes.
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8") if length else ""

        stamp = datetime.now().strftime("%H:%M:%S")
        print(f"\n=== webhook fired at {stamp} ===")
        print(f"  path: {self.path}")
        # Splunk sends JSON. Pretty-print it if we can; fall back to raw text.
        try:
            print(json.dumps(json.loads(body), indent=2))
        except (json.JSONDecodeError, ValueError):
            print(body or "  (empty body)")
        print("=== end ===", flush=True)

        # Tell Splunk we received it. Without a 200 reply Splunk logs an error.
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        # Silence the default per-request access log -- our own print is enough.
        pass


def main():
    ap = argparse.ArgumentParser(
        description="Local webhook stub for Splunk alerts.")
    ap.add_argument("--port", type=int, default=8000, help="Port to listen on")
    args = ap.parse_args()

    server = HTTPServer(("localhost", args.port), WebhookHandler)
    print(f"webhook stub listening on http://localhost:{args.port}/")
    print("waiting for a Splunk alert to fire... (Ctrl+C to stop)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped.")
        server.server_close()


if __name__ == "__main__":
    main()
