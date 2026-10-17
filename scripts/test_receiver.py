#!/usr/bin/env python3
"""Minimal webhook receiver for end-to-end tests: echoes the verification
challenge and checks HMAC signatures on deliveries.

Usage: test_receiver.py PORT LOGFILE   (secret is read from LOGFILE.secret when present)
"""
import hashlib
import hmac
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT, LOG = int(sys.argv[1]), sys.argv[2]


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("content-length", 0)))
        msg = json.loads(body)
        if msg.get("type") == "verification":
            out = json.dumps({"challenge": msg["challenge"]}).encode()
            note = "verification challenge echoed"
        else:
            try:
                secret = open(LOG + ".secret").read().strip()
            except OSError:
                secret = ""
            ts = self.headers.get("x-nem-timestamp", "")
            expected = "sha256=" + hmac.new(secret.encode(), (ts + ".").encode() + body, hashlib.sha256).hexdigest()
            ok = hmac.compare_digest(expected, self.headers.get("x-nem-signature", ""))
            note = f"{msg.get('type')} signature_valid={ok} title={msg['item']['title']!r}"
            out = b"ok"
        with open(LOG, "a") as f:
            f.write(note + "\n")
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.end_headers()
        self.wfile.write(out)


HTTPServer(("127.0.0.1", PORT), H).serve_forever()
